//! Per-channel in-flight token accounting + a minute-level token sliding window — the
//! token-side signal source for the P2C load factor.
//!
//! ## Soft budget semantics
//!
//! This structure is a **soft budget**: `preclaim` only accounts and never rejects
//! (instantaneous oversell is allowed). The actual "rejection" comes from
//! [`TokenBudgetState::admit_window`]'s minute sliding-window check (reject only when the
//! window is full); the counters themselves never reject. Rejection lives in the
//! **window check**, not in the counters.
//!
//! ## Three signals
//!
//! - `inflight`: estimated in-flight tokens per channel (`preclaim` adds,
//!   `TokenHandle::drop` / `settle_actual` returns), RAII guarantees no path leaks;
//! - `ema`: exponential moving average of measured completion per channel (`settle_actual`
//!   calibrates with α=0.3), initial value [`COMPLETION_EMA_INIT`];
//! - `caps`: per-channel token cap (TPM), `0` = unlimited (the same assembly convention
//!   as `channel_max_concurrency`); a channel with no override falls back to
//!   `global_default`.
//!
//! Shape matches `gateway_gate::concurrency::ConcurrencyState` (`DashMap` + atomics) with no
//! background thread; the clock matches `ratelimit` (default `chrono`, tests inject
//! deterministic milliseconds via `with_clock`).
//!
//! Hot path avoids cloning shared state: `preclaim` / `inflight` / `load` go through a
//! `DashMap` entry + atomic reads; the RAII handle only holds an `Arc` handle to the shared
//! counter (mirroring `OwnedSemaphorePermit`).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use dashmap::DashMap;

use crate::ratelimit::{RateLimitSpec, SlidingWindow};

/// Initial completion EMA (used to estimate output-token volume when no measurement exists).
pub const COMPLETION_EMA_INIT: u64 = 512;

/// Token window fixed at 60s (TPM = tokens/minute).
const WINDOW_MS: u64 = 60_000;

/// Per-channel in-flight token counter + minute token sliding window. Shared across
/// requests (`new` returns `Arc<Self>`).
///
/// Every method takes `&self` (`DashMap` sharding + atomic counters internally); the hot
/// path takes no global lock and allocates only the key strings for the entry/handle.
pub struct TokenBudgetState {
    /// `channel_key` → token cap (TPM). `0` is not registered; unregistered = unlimited.
    caps: DashMap<String, u64>,
    /// `channel_key` → estimated in-flight tokens (`Arc` so RAII handles can return from other
    /// threads).
    inflight: DashMap<String, Arc<AtomicU64>>,
    /// `channel_key` → measured completion EMA (initial [`COMPLETION_EMA_INIT`]).
    ema: DashMap<String, Arc<AtomicU64>>,
    /// Minute-level token sliding window (used by `admit_window`; weight = estimated token count).
    window: SlidingWindow,
    /// Global default token cap (installed by `set_global_default`, `0` = unlimited).
    global_default: AtomicU64,
    /// Clock (default `chrono` / injected by `with_clock`).
    now_ms: Arc<dyn Fn() -> u64 + Send + Sync>,
}

impl TokenBudgetState {
    /// Default clock (`chrono::Utc`). `Arc`-shared; the default cap is `0`.
    pub fn new() -> Arc<Self> {
        let now_ms: Arc<dyn Fn() -> u64 + Send + Sync> = Arc::new(crate::utc_now_ms);
        let window = SlidingWindow::new();
        Arc::new(Self::build(now_ms, window))
    }

    /// Inject a deterministic clock (tests). The same clock also feeds the internal token
    /// sliding window.
    pub fn with_clock(now_ms: impl Fn() -> u64 + Send + Sync + 'static) -> Arc<Self> {
        let shared: Arc<dyn Fn() -> u64 + Send + Sync> = Arc::new(now_ms);
        let window = SlidingWindow::with_clock({
            let clock = Arc::clone(&shared);
            move || (clock)()
        });
        Arc::new(Self::build(shared, window))
    }

    fn build(now_ms: Arc<dyn Fn() -> u64 + Send + Sync>, window: SlidingWindow) -> Self {
        Self {
            caps: DashMap::new(),
            inflight: DashMap::new(),
            ema: DashMap::new(),
            window,
            global_default: AtomicU64::new(0),
            now_ms,
        }
    }

    /// Set the global default token cap (TPM). `0` = unlimited. Call it before the
    /// per-channel `register_channel` during assembly (channels without an override fall
    /// back to this value).
    pub fn set_global_default(&self, tpm: u64) {
        self.global_default.store(tpm, Ordering::Relaxed);
    }

    /// Current global default token cap (`0` = unlimited).
    pub fn global_default(&self) -> u64 {
        self.global_default.load(Ordering::Relaxed)
    }

    /// Register a channel token cap (TPM). `cap == 0` ⇒ not registered (the existing
    /// "0 = unlimited" convention). Repeated registration is idempotent (the same key is
    /// overwritten with the new cap, supporting hot-reload snapshots).
    pub fn register_channel(&self, channel_key: &str, cap: u64) {
        if cap == 0 {
            return;
        }
        self.caps.insert(channel_key.to_string(), cap);
    }

    /// Channel token cap: the per-channel override, else [`Self::global_default`];
    /// `0` = unlimited. An explicit `tpm_limit = 0` is not registered, so it also follows
    /// the global default (the catalog cannot express "override to unlimited").
    pub fn cap(&self, channel_key: &str) -> u64 {
        self.caps
            .get(channel_key)
            .map_or_else(|| self.global_default(), |v| *v)
    }

    /// Current estimated in-flight tokens for the channel (unregistered/no in-flight = `0`).
    pub fn inflight(&self, channel_key: &str) -> u64 {
        self.inflight
            .get(channel_key)
            .map_or(0, |c| c.load(Ordering::Relaxed))
    }

    /// Current in-flight token occupancy for the channel: `Some(min(1.0, inflight / cap))`;
    /// `cap == 0` (no override and no global default) ⇒ `None`. P2C discounts channels about to
    // In-flight tokens and the cap are u64 counters; the ratio is an f64 for P2C discounting
    // and channel token counts stay far below 2^53, so the conversion is exact in practice.
    #[allow(clippy::cast_precision_loss)]
    pub fn load(&self, channel_key: &str) -> Option<f64> {
        let cap = self.cap(channel_key);
        if cap == 0 {
            return None;
        }
        Some((self.inflight(channel_key) as f64 / cap as f64).min(1.0))
    }

    /// Measured completion EMA for the channel; no observation = [`COMPLETION_EMA_INIT`].
    pub fn completion_ema(&self, channel_key: &str) -> u64 {
        self.ema
            .get(channel_key)
            .map_or(COMPLETION_EMA_INIT, |e| e.load(Ordering::Relaxed))
    }

    /// Minute-level token window check: whether this channel still has room for the
    /// estimated tokens accumulated within 60s. `cap == 0` (no cap in effect) ⇒ pass
    /// immediately (`true`); otherwise delegates to [`SlidingWindow::admits_weighted`]
    /// (weight = estimated token count, strict side rejects over-limit).
    pub fn admit_window(&self, channel_key: &str, est_tokens: u64) -> bool {
        let cap = self.cap(channel_key);
        if cap == 0 {
            return true;
        }
        let spec = RateLimitSpec {
            requests: cap,
            window_ms: WINDOW_MS,
        };
        self.window
            .admits_weighted(channel_key, Some(&spec), est_tokens, (self.now_ms)())
    }

    /// Pre-claim estimated in-flight tokens for the channel and return an RAII handle. On
    /// drop the handle returns the "currently held" amount (estimate if not settled,
    /// measured total after settle); no path leaks.
    pub fn preclaim(&self, channel_key: &str, est_tokens: u64) -> TokenHandle {
        let inflight = shared_counter(&self.inflight, channel_key, 0);
        let ema = shared_counter(&self.ema, channel_key, COMPLETION_EMA_INIT);
        inflight.fetch_add(est_tokens, Ordering::Relaxed);
        TokenHandle {
            channel_key: channel_key.to_string(),
            inflight,
            ema,
            held: AtomicU64::new(est_tokens),
            settled: AtomicBool::new(false),
        }
    }
}

/// Get (or lazily create) the channel's shared counter; `init` only applies when the entry
/// is created.
fn shared_counter(
    counters: &DashMap<String, Arc<AtomicU64>>,
    channel_key: &str,
    init: u64,
) -> Arc<AtomicU64> {
    counters
        .entry(channel_key.to_string())
        .or_insert_with(|| Arc::new(AtomicU64::new(init)))
        .clone()
}

/// In-flight token pre-claim handle (RAII).
///
/// - `held = est` at `preclaim`; dropping the handle subtracts `est` from the channel's
///   in-flight counter;
/// - after a mid-flight `settle_actual` commits the measured total, `held = actual_total`
///   and the in-flight delta has been adjusted both ways (the subtraction is always ≤ this
///   handle's contributed share, so it never underflows); on drop it subtracts
///   `actual_total`, and the EMA is fed the measured completion with α=0.3;
/// - idempotent: a second `settle_actual` returns immediately without re-accounting.
///
/// `channel_key` is public (the forward layer attributes by it); the remaining fields are
/// private.
pub struct TokenHandle {
    /// Channel key this handle belongs to.
    pub channel_key: String,
    inflight: Arc<AtomicU64>,
    ema: Arc<AtomicU64>,
    /// Currently held amount: estimate at `preclaim`, measured total tokens after `settle_actual`.
    held: AtomicU64,
    /// Settle idempotency flag: once set, `settle_actual` / drop no longer re-feed the EMA or
    /// double-count.
    settled: AtomicBool,
}

impl TokenHandle {
    /// Commit measured values, calibrating the in-flight counter and completion EMA.
    /// Idempotent (a second call returns immediately).
    ///
    /// - In-flight delta adjusted both ways: `actual_total > held` adds, `< held` subtracts
    ///   (the subtraction is always ≤ this handle's share, so it never underflows); after
    ///   normalization this handle's contribution to the channel in-flight counter =
    ///   `actual_total` (subtracted again on drop, net zero);
    /// - completion EMA is fed `actual_completion` with α=0.3: `ema = 0.3·sample + 0.7·old`
    ///   (integer form `(old·7 + sample·3) / 10`, all saturating).
    pub fn settle_actual(&self, actual_total: u64, actual_completion: u64) {
        // Idempotent: first writer wins, later calls do not re-account.
        if self.settled.swap(true, Ordering::SeqCst) {
            return;
        }
        let held = self.held.load(Ordering::SeqCst);
        if actual_total >= held {
            self.inflight
                .fetch_add(actual_total - held, Ordering::Relaxed);
        } else {
            // The subtraction (held - actual_total) is necessarily ≤ held ≤ this handle's
            // contributed share, so it cannot underflow.
            self.inflight
                .fetch_sub(held - actual_total, Ordering::Relaxed);
        }
        self.held.store(actual_total, Ordering::SeqCst);

        let old = self.ema.load(Ordering::Relaxed);
        let ema_next = old
            .saturating_mul(7)
            .saturating_add(actual_completion.saturating_mul(3))
            / 10;
        self.ema.store(ema_next, Ordering::Relaxed);
    }
}

impl Drop for TokenHandle {
    fn drop(&mut self) {
        // Return the "currently held" amount (actual_total after settle) — no path leaks.
        let held = self.held.load(Ordering::SeqCst);
        if held > 0 {
            self.inflight.fetch_sub(held, Ordering::Relaxed);
        }
    }
}
