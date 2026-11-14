//! Windowed t-digest latency signal table — gives the P2C tie-break layer a distribution view
//! over p50/p95.
//!
//! Each routing unit (`unit_key`) keeps one [`WindowedDigest`] made of two
//! [`crate::tdigest::Tdigest`]s — `cur` accumulates the current window's samples, `prev` is the
//! snapshot after the previous window rotation. Queries read only `prev`'s p50/p95 (an O(1) data
//! source), avoiding a merge per query; rotation unconditionally does `merge(prev, cur)` and resets
//! `cur`.
//!
//! The EWMA peak-latency semantics of "10s downward decay" fall naturally to window rotation: a
//! spike from 30s ago survives at most one `prev` window and is then dropped — the tail-latency
//! signal therefore answers for "the distribution over the last 30s", not "a single scalar peak".
//!
//! ## Rotation is report-driven (a known boundary for silent channels)
//!
//! Rotation is decided only when [`LatencyDigestTable::report`] is called, so **when a channel
//! goes silent (upstream timeouts/traffic shedding stop producing latency samples) the window
//! never rotates, the `prev` snapshot stays frozen at the last rotation's value**, and `weight()`
//! still reports warm because it counts cumulatively. This is the same silent semantics as the
//! existing `latency_ewma_ms` ("no samples, no movement") and therefore not a new regression;
//! channel liveness is judged by the health-score channel (`HealthState::ewma_score` degraded by
//! `apply_outcome`), and the latency signal does not carry that responsibility. Once reporting
//! resumes, the next report triggers a rotation.
//!
//! On a clock rollback (`now < last_rotate_ms`), `saturating_sub` yields 0 and no rotation
//! happens that time; since `last_rotate_ms` only advances on rotation, the normal cadence
//! resumes once the clock recovers.
//!
//! The table shares [`crate::health::MemoryHealthTable`]'s lifetime and lock-poisoning policy
//! (`unwrap_or_else(into_inner)`). `report` never panics: even with a poisoned inner lock it
//! recovers the guard and keeps accounting; `quantiles` returns `None` for units never reported.
//!
//! Assembly (§3.3 of the design) injects this table into `MemoryHealthTable` behind the rollout
//! key `gateway.health.digest_enabled` (`with_digest`); when `digest_enabled = false` it is not
//! mounted, and the rollback path is byte-for-byte identical to the status quo.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use crate::tdigest::Tdigest;

/// Compression parameter δ (`Tdigest::new(delta)`): 100, the agreed design value.
const DIGEST_DELTA: f64 = 100.0;

/// Default rotation window (ms): 30s, aligned with `W` in §3.1 of the design.
pub const DEFAULT_WINDOW_MS: u64 = 30_000;

/// Windowed digest pair for one routing unit.
struct WindowedDigest {
    /// Samples accumulated for the current window (all weights are 1).
    cur: Tdigest,
    /// Snapshot from the previous rotation (the O(1) data source for queries). Empty until the
    /// first rotation.
    prev: Tdigest,
    /// Time of the most recent rotation (unix ms).
    last_rotate_ms: u64,
}

/// Windowed t-digest latency signal table: keeps a "current window + previous-window snapshot"
/// pair of t-digests per `unit_key`; queries read p50/p95 from the previous window.
///
/// The clock is injectable (`with_clock`) so rotation semantics are deterministic in tests;
/// production `new()` uses the `chrono::Utc` millisecond clock.
pub struct LatencyDigestTable {
    /// `unit_key → WindowedDigest`.
    windows: Mutex<HashMap<String, WindowedDigest>>,
    /// Rotation window length (ms).
    window_ms: u64,
    /// Clock (`chrono` by default / injected via `with_clock`).
    now_ms: Arc<dyn Fn() -> u64 + Send + Sync>,
}

impl LatencyDigestTable {
    /// Default clock (`chrono::Utc`), window [`DEFAULT_WINDOW_MS`], δ = 100.
    /// Shared as an `Arc` (assembly mounts it on `MemoryHealthTable` so both writers reuse it).
    pub fn new() -> Arc<Self> {
        let now_ms: Arc<dyn Fn() -> u64 + Send + Sync> = Arc::new(crate::utc_now_ms);
        Arc::new(Self::build(now_ms, DEFAULT_WINDOW_MS))
    }

    /// Inject a deterministic clock and window length (tests). The clock also decides when to
    /// rotate.
    pub fn with_clock(
        now_ms: impl Fn() -> u64 + Send + Sync + 'static,
        window_ms: u64,
    ) -> Arc<Self> {
        let now_ms = Arc::new(now_ms);
        Arc::new(Self::build(now_ms, window_ms))
    }

    fn build(now_ms: Arc<dyn Fn() -> u64 + Send + Sync>, window_ms: u64) -> Self {
        Self {
            windows: Mutex::new(HashMap::new()),
            window_ms,
            now_ms,
        }
    }

    /// Take the lock without panicking on poisoning (same rule as `MemoryHealthTable::lock`: one
    /// thread panicking must not take down every request).
    fn lock(&self) -> MutexGuard<'_, HashMap<String, WindowedDigest>> {
        self.windows
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Report one latency sample. When `now - last_rotate > window`, rotate first: `prev`
    /// unconditionally `merge`s `cur`, `cur` is reset to an empty digest, then `cur.add(latency)`.
    /// Every reported `unit_key` keeps a permanent record (never deleted). Never panics.
    pub fn report(&self, unit_key: &str, latency_ms: u32) {
        let now = (self.now_ms)();
        let mut windows = self.lock();
        let wd = windows
            .entry(unit_key.to_string())
            .or_insert_with(|| WindowedDigest {
                cur: Tdigest::new(DIGEST_DELTA),
                prev: Tdigest::new(DIGEST_DELTA),
                last_rotate_ms: now,
            });
        if now.saturating_sub(wd.last_rotate_ms) > self.window_ms {
            // Rotate: prev absorbs cur (unconditionally — cur is carried over even below 10
            // samples), and cur is reset. Queries read the new prev from here on.
            wd.prev.merge(&wd.cur);
            wd.cur = Tdigest::new(DIGEST_DELTA);
            wd.last_rotate_ms = now;
        }
        wd.cur.add(f64::from(latency_ms));
    }

    /// Query side in O(1): reads only `prev`'s p50/p95 (two `quantile` calls under the lock, no
    /// cloning of the table, no merge). An empty `prev` (first window, no rotation yet) → `None`.
    /// A `unit_key` never reported → `None`.
    pub fn quantiles(&self, unit_key: &str) -> Option<(f64, f64)> {
        self.quantiles_with_weight(unit_key)
            .map(|(p50, p95, _)| (p50, p95))
    }

    /// Total sample weight for the cold-start threshold: `prev.count + cur.count` (matching
    /// `HealthSetting.min_requests`). A `unit_key` never reported → `0.0`.
    pub fn weight(&self, unit_key: &str) -> f64 {
        let windows = self.lock();
        windows
            .get(unit_key)
            .map_or(0.0, |wd| wd.prev.count() + wd.cur.count())
    }

    /// Fetch p50/p95/sample weight under one lock (hot path; replaces calling `quantiles()` +
    /// `weight()`). Returns (p50, p95, weight); empty prev / never reported → None (same semantics
    /// as quantiles).
    pub fn quantiles_with_weight(&self, unit_key: &str) -> Option<(f64, f64, f64)> {
        let windows = self.lock();
        let wd = windows.get(unit_key)?;
        if wd.prev.is_empty() {
            return None;
        }
        Some((
            wd.prev.quantile(0.5)?,
            wd.prev.quantile(0.95)?,
            wd.prev.count() + wd.cur.count(),
        ))
    }
}
