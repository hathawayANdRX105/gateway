//! Local health table — a port of the phase0 `track_health.go` model, adjusted
//! for p2c scheduling.
//!
//! Reference: new-api phase0 `internal/catalog/track_health.go` +
//! `bridge_channel_health.go` (outcome classification / EWMA continuous score /
//! cooldown / slow-start ramp).
//!
//! Model highlights:
//! - **Five outcome classes**: Success / Fatal / Throttled (429) / Neutral (other
//!   4xx) / UnauthorizedRun (401/403 escalated on consecutive hits, 3 → Fatal);
//! - **EWMA continuous score**: `score = α×obs + (1-α)×score`, obs ∈
//!   {1.0, 0.0, 0.7(429)}, not updated until MinRequests is exceeded, floored at
//!   MinScore.
//!   **No longer a weight multiplier**; it only feeds the P2C likelihood;
//! - **Peak-latency EWMA**: `latency_ewma_ms` records upstream latency; increases
//!   apply immediately, decreases are pulled toward the latest sample with a 10s
//!   time constant. It is the only signal through which P2C sees a "slow but
//!   successful" channel;
//! - **Immediate cooldown + increasing ladder**: any single fatal outcome enters
//!   cooldown at once; **no more "N consecutive hits" threshold**. The full Fatal
//!   penalty is carried by the duration ladder:
//!   `base_ms + (max_ms - base_ms)×(n/N)^p` (p=2, convex), climbing step by step
//!   toward the ceiling (default starts at 300ms, capped at 8s, topped out at
//!   N=12 steps). The power law **decouples the shape p from the horizon N** — a
//!   gentler start does not drag the saturation point along. A sub-second start
//!   keeps a failed channel from being reconnected immediately, and the
//!   increasing step size keeps a channel with more consecutive hits out longer.
//!   `cooldown_streak` is decremented window by window by `anneal_streak` while
//!   idle and also on a successful report; once a channel recovers, the ladder
//!   restarts from base — a flapping channel therefore stays at the 300ms rung.
//!   The outcome tier is still layered onto the curve: a 401-run escalation → max
//!   tier;
//! - **Separate 429 cooldown ladder**: Throttled uses a `throttle_streak` fully
//!   independent of Fatal (the two ledgers never contaminate each other: 429 does
//!   not write cooldown_streak, Fatal never touches throttle_streak), range
//!   `cooldown_base_ms → throttle_max_ms` (default 300ms→1s), same power-law
//!   shape (p=2) but a fixed 4-step horizon, ladder
//!   343→475→693→1000ms (truncated to whole ms). An upstream Retry-After header
//!   (when a hint exists) directly overrides the ladder and is clamped to
//!   `cooldown_max_ms`. A successful report and idle annealing decrement
//!   throttle_streak in the same way (reusing the same annealing window);
//! - **EWMA time decay**: before updating `ewma_score`, age it toward the default
//!   score with the time constant `score_decay_tau_ms` (default 15min):
//!   `prior' = 1 - (1-prior)×exp(-Δt/τ)` — an idle channel's old penalty expires
//!   over time, half-life T½ = τ·ln2; `score_updated_at_ms` records the baseline
//!   instant;
//! - **slow-start ramp**: after cooldown ends the channel enters ramp
//!   (RampPending); a real failure immediately RampExited. The ramp lives in the
//!   sampling layer: effective prior = weight × slow_start_factor
//!   (`request_count/min_requests`), consumed by the selector; this table only
//!   supplies the ramp state (ramp_pending / ramp_exited / request_count) and no
//!   longer owns the weight formula.
//!
//! - **Live config reload**: `set_config` replaces the whole config at runtime
//!   (TODO(#311) implementation): the hot path reads `get_config()` and clones
//!   the current value; only parameters are swapped, cooldown/streak/EWMA state
//!   is never reset (the states table stays as is).
//!
//! All state is in memory (Mutex<HashMap>) and is not synchronized across nodes —
//! center's health_observations is trend material and does not participate in
//! hot-path decisions.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::latency_digest::LatencyDigestTable;

mod cooldown;
mod outcome;

pub use cooldown::throttle_cooldown_duration_ms;
pub use gateway_contract::health::{
    ChannelOutcome, DEFAULT_SCORE, HealthSetting, HealthState, TPS_ALPHA_ESTIMATED,
    TPS_ALPHA_MEASURED, TpsSample,
};
pub use outcome::{AttemptReport, FailureClass};
pub use outcome::{THROTTLE_RAMP_STEPS, THROTTLED_OBSERVATION, UNAUTHORIZED_ESCALATION_THRESHOLD};

/// Health table trait — reads and writes health state keyed by the candidate key
/// (RouteUnitRecord::meta.key).
///
/// Engine-side trait: the hot path (record / selection gating) stays here. The
/// admin plane uses the narrower `gateway_contract::health::HealthHandle` port instead.
pub trait HealthTable: Send + Sync {
    /// Read the current health state; no record → Default (full health).
    fn get(&self, unit_key: &str) -> HealthState;

    /// Report one attempt result; classify and account for it internally by the
    /// phase0 rules.
    ///
    /// The payload is bundled in the [`AttemptReport`] struct (constructor
    /// `AttemptReport::new` + `with_tps` builder) so new signals no longer blow up
    /// the positional parameters.
    ///
    /// - `report.outcome`: `Ok(status)` is classified by status code (2xx→Success,
    ///   429→Throttled, 5xx→Fatal, 401/403→escalation counter, other 4xx→Neutral);
    ///   `Err(Retryable)` transport-layer failure → Fatal (the channel is
    ///   currently unavailable); `Err(Fatal)` client-side problem → Neutral (the
    ///   channel is not at fault);
    /// - `report.latency_ms`: total duration or time-to-first-token of a successful
    ///   request; `None` on failure or when unobserved;
    /// - `report.retry_after_ms`: wait duration (ms) parsed from an upstream 429
    ///   `Retry-After` header. Consumed only when outcome = `Ok(429)` (overrides
    ///   the separate 429 ladder and is clamped to `cooldown_max_ms`); the value is
    ///   dropped outright for other outcomes.
    /// - `report.tps`: generation throughput sample (tok/s). **tps is a late
    ///   report from the stream's end** — a streaming request only knows the status
    ///   code/time-to-first-byte when the attempt wraps up, and throughput can only
    ///   be computed once the stream is fully read. A sample carried along by this
    ///   report goes through this method into the EMA; the genuinely late path (at
    ///   stream-end time) goes through [`HealthTable::record_tps`].
    ///
    /// **Input contract (ingest boundary)**: an empty `unit_key` and non-physical
    /// throughput samples (NaN/Inf/≤0, including the one carried along by
    /// `report.tps` into the EMA through [`Self::record_tps`]) are silently
    /// dropped at the table boundary (debug log), creating no table entry and
    /// contaminating no existing entry; the hot path is local memory only (no
    /// IO/network, no panic).
    fn record(&self, unit_key: &str, report: AttemptReport);

    /// Report one **late** generation throughput sample (at stream end, see the
    /// `record` docs); it only merges into `HealthState::tps_ema` —
    /// cooldown/streak/EWMA/latency/digest are all left untouched, so a side
    /// channel cannot contaminate the existing health accounting.
    ///
    /// **Input contract (ingest boundary)**: an empty `unit_key` and non-physical
    /// throughput samples (NaN/Inf/≤0) are silently dropped at the table boundary
    /// (debug log), creating no table entry and not contaminating any existing
    /// entry's EMA; the hot path is local memory only.
    fn record_tps(&self, unit_key: &str, sample: TpsSample);

    /// Gate decision: whether the candidate may take part in this round of
    /// selection (cooling down → no).
    fn is_selectable(&self, unit_key: &str, now_ms: u64) -> bool;

    /// Remaining cooldown — `Some(ms)` means the channel is cooling down (not yet
    /// expired), `None` means it is not cooling or the cooldown already expired.
    /// The selector uses this to find the channel closest to recovery for an
    /// emergency recall when the pool runs dry.
    fn cooling_remaining_ms(&self, unit_key: &str, now_ms: u64) -> Option<u64>;

    /// Emergency recall — force the channel's cooldown to end (reusing
    /// finish_cooldown semantics: clear the cooldown, reset the request count, arm
    /// the slow-start ramp).
    /// **`cooldown_streak` is preserved**: a recall is not a pardon; the next
    /// failure still climbs from the existing rung.
    fn force_recall(&self, unit_key: &str);

    /// Read the current health config (live-reload convention: a `get_config`
    /// snapshot). The selector reads the config items it needs (`alpha` /
    /// `min_requests` / `slow_start_factor` input, etc.) through this port instead
    /// of reaching into `MemoryHealthTable`'s private slot.
    fn setting(&self) -> HealthSetting;
}

type StateMap = HashMap<String, HealthState>;

/// Pure in-memory health table — a Rust port of phase0 `HealthStore`.
/// The config is hot-reloadable at runtime (TODO(#311) implemented as set_config:
/// only parameter values are swapped, state is untouched).
pub struct MemoryHealthTable {
    states: Mutex<StateMap>,
    /// Config slot: hot updates go through [`Self::set_config`]; the hot path
    /// reads [`Self::get_config`] and clones the current value. Poison-tolerant
    /// lock, same rule as `states`.
    cfg: Mutex<HealthSetting>,
    now_ms: Box<dyn Fn() -> u64 + Send + Sync>,
    /// Windowed t-digest latency signal table (optional dual write). `None` = not
    /// attached (the default / gray flag off), in which case `record` is
    /// byte-for-byte identical to current behavior.
    digest: Option<Arc<LatencyDigestTable>>,
}

impl Default for MemoryHealthTable {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryHealthTable {
    pub fn new() -> Self {
        Self::with_config(HealthSetting::default())
    }

    pub fn with_config(cfg: HealthSetting) -> Self {
        Self::with_config_and_clock(cfg, crate::utc_now_ms)
    }

    /// Test/deterministic clock injection.
    pub fn with_config_and_clock(
        cfg: HealthSetting,
        now_ms: impl Fn() -> u64 + Send + Sync + 'static,
    ) -> Self {
        Self {
            states: Mutex::new(StateMap::new()),
            cfg: Mutex::new(cfg),
            now_ms: Box::new(now_ms),
            digest: None,
        }
    }

    /// Attach the windowed t-digest latency signal table and return self (dual
    /// write on). Consumes `self` — the assembly layer must call it before
    /// `Arc::new(...)` (`Arc<MemoryHealthTable>` is immutable, so it cannot be
    /// attached at runtime). Not attached by default; see
    /// [`crate::latency_digest`].
    pub fn with_digest(mut self, digest: Arc<LatencyDigestTable>) -> Self {
        self.digest = Some(digest);
        self
    }

    /// Take the lock; poisoning does not panic (one thread panicking must not take
    /// down every request).
    fn lock(&self) -> std::sync::MutexGuard<'_, StateMap> {
        self.states.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Take the config-slot lock (poison-tolerant like [`Self::lock`]; lock order
    /// is fixed as states → cfg, never nested the other way, so no deadlock).
    fn lock_cfg(&self) -> std::sync::MutexGuard<'_, HealthSetting> {
        self.cfg.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Live-reload config (TODO(#311)): replace only the config values, never
    /// reset cooldown/streak/EWMA state (the `states` table stays untouched). The
    /// caller (bin/api's live-reload bus) builds new values through the same
    /// `knobs::load_health_setting` path as boot.
    ///
    /// **Input contract (ingest boundary)**: the new value first passes
    /// [`HealthSetting::validate`]; illegal values (α / min_score non-finite or
    /// outside [0,1]; base above either ceiling) → the whole update is rejected.
    pub fn set_config(&self, cfg: HealthSetting) {
        if let Err(reason) = cfg.validate() {
            tracing::warn!(
                reason,
                ?cfg,
                "health set_config rejected; keeping current setting"
            );
            return;
        }
        *self.lock_cfg() = cfg;
    }

    /// Read the current config (the hot path clones the `Copy` value per request,
    /// so no lock is held after the snapshot).
    pub fn get_config(&self) -> HealthSetting {
        *self.lock_cfg()
    }

    /// Whole-table read port — returns a snapshot list of `(unit_key, state)` for
    /// every current health record.
    ///
    /// Semantics: clone the entire table inside the lock, then release it
    /// (`HealthState` is `Copy`, so cloning carries no heap-allocation risk; the
    /// caller gets a data copy that later reads cannot change through concurrent
    /// `record`). Read-only, without settling expired cooldowns — the query surface
    /// produces no side effects (unlike the lazy-settlement semantics of
    /// `is_selectable`).
    /// A unit that was never recorded is not in the table and does not appear in
    /// the result.
    /// Aimed at the admin query surface (`/api/gateway/health`); not called on the
    /// hot path (the whole table is cloned under the lock, so its size grows with
    /// the number of channels ever recorded).
    ///
    /// **Output contract (sanitization)**: returns only an aggregate numeric
    /// snapshot of `(unit_key, HealthState)` — no credentials, upstream addresses,
    /// or raw error text/payloads; a caller (admin view) can project it directly.
    pub fn entries(&self) -> Vec<(String, HealthState)> {
        let states = self.lock();
        states.iter().map(|(key, st)| (key.clone(), *st)).collect()
    }
}

impl HealthTable for MemoryHealthTable {
    fn get(&self, unit_key: &str) -> HealthState {
        self.lock().get(unit_key).copied().unwrap_or_default()
    }

    fn record(&self, unit_key: &str, report: AttemptReport) {
        // Ingest-boundary guard: an empty unit_key creates no table entry and
        // triggers neither the digest nor the tps side channel (silently dropped,
        // debug log).
        if unit_key.is_empty() {
            tracing::debug!("health record dropped: empty unit key");
            return;
        }
        let now = (self.now_ms)();
        {
            let mut states = self.lock();
            let mut st = states.get(unit_key).copied().unwrap_or_default();
            if let Some(latency_ms) = report.latency_ms {
                update_latency_ewma(&mut st, latency_ms, now);
            }
            outcome::apply_outcome(
                &mut st,
                report.outcome,
                &self.get_config(),
                now,
                report.retry_after_ms,
            );
            states.insert(unit_key.to_string(), st);
        }
        // Digest dual-write: report after the states lock is released. Lock order
        // is one-way (states → cfg) and digest has its own lock; never hold both
        // at once, to avoid deadlock (design §3.3).
        if let (Some(latency_ms), Some(digest)) = (report.latency_ms, self.digest.as_ref()) {
            digest.report(unit_key, latency_ms);
        }
        // tps late-arrival channel: same position as digest (after the digest
        // report, with locks released) and merged into the EMA; cooldown/streak/
        // EWMA/digest are never touched here.
        if let Some(tps) = report.tps {
            self.record_tps(unit_key, tps);
        }
    }

    fn record_tps(&self, unit_key: &str, sample: TpsSample) {
        // Ingest-boundary guard: an empty unit_key creates no table entry
        // (silently dropped, debug log).
        if unit_key.is_empty() {
            tracing::debug!("health record_tps dropped: empty unit key");
            return;
        }
        // Non-physical throughput samples (NaN/Inf/≤0): silently dropped **before**
        // the table entry is created — a bad sample must not create or contaminate
        // a table entry's EMA.
        let value = match sample {
            TpsSample::Measured(v) | TpsSample::Estimated(v) => v,
        };
        if !(value.is_finite() && value > 0.0) {
            tracing::debug!(
                unit_key,
                ?sample,
                "health record_tps dropped: non-physical sample"
            );
            return;
        }
        let mut states = self.lock();
        let mut st = states.get(unit_key).copied().unwrap_or_default();
        st.update_tps_ema(sample);
        states.insert(unit_key.to_string(), st);
    }

    fn is_selectable(&self, unit_key: &str, now_ms: u64) -> bool {
        // Lazily settle an expired cooldown (phase0 FilterCoolingChannels
        // semantics) — a channel whose cooldown expired re-enters the slow-start
        // ramp at this gate check.
        let mut states = self.lock();
        let Some(st) = states.get_mut(unit_key) else {
            return true; // no history = not cooling
        };
        if st.cooldown_until_ms != 0 && !st.is_cooling(now_ms) {
            cooldown::finish_cooldown(st, now_ms);
        }
        // Idle annealing: for a channel with no active traffic, cooldown streaks
        // decay by base-sized windows.
        cooldown::anneal_streak(st, &self.get_config(), now_ms);
        !st.is_cooling(now_ms)
    }

    fn setting(&self) -> HealthSetting {
        self.get_config()
    }

    fn cooling_remaining_ms(&self, unit_key: &str, now_ms: u64) -> Option<u64> {
        // Only a cooldown deadline strictly greater than the current instant counts
        // as cooling (same convention as is_cooling); unrecorded / never cooled
        // (cooldown_until_ms == 0) → None.
        self.lock()
            .get(unit_key)
            .and_then(|st| st.cooldown_until_ms.checked_sub(now_ms))
    }

    fn force_recall(&self, unit_key: &str) {
        // Use the table's injected clock: a recall means "the cooldown ends now",
        // and the annealing anchor also starts from this instant.
        let now = (self.now_ms)();
        if let Some(st) = self.lock().get_mut(unit_key) {
            cooldown::finish_cooldown(st, now);
        }
    }
}

/// Admin-plane port: the panel lists entries, the live-reload bus reads and replaces
/// the config. Both delegate to the inherent methods, so `set_config` keeps its
/// validate-then-swap contract.
impl gateway_contract::health::HealthHandle for MemoryHealthTable {
    fn entries(&self) -> Vec<(String, HealthState)> {
        MemoryHealthTable::entries(self)
    }

    fn config(&self) -> HealthSetting {
        MemoryHealthTable::get_config(self)
    }

    fn set_config(&self, cfg: HealthSetting) {
        MemoryHealthTable::set_config(self, cfg);
    }
}

/// Update the peak-sensitive latency EWMA (aligned with Finagle `p2cPeakEwma`
/// decay semantics).
///
/// Increases apply immediately: when `sample >= score` the new value is taken
/// directly, so a latency spike is visible to P2C in the same round. Decreases
/// pull the peak **toward the latest sample** with a 10-second time constant:
/// `score = sample + (score - sample) × e^(-Δ/τ)`.
/// Note this is not "decay toward 0" — `score` is always `>= sample`, so a
/// currently fast channel is not permanently depressed by a long-past spike. The
/// first sample establishes the baseline directly (`0.0` means no observation
/// yet).
fn update_latency_ewma(st: &mut HealthState, latency_ms: u32, now_ms: u64) {
    const DECAY_TIME_CONSTANT_MS: f64 = 10_000.0;

    let sample = f64::from(latency_ms);
    if st.latency_updated_at_ms == 0 || st.latency_ewma_ms == 0.0 || sample >= st.latency_ewma_ms {
        st.latency_ewma_ms = sample;
    } else {
        let elapsed_ms = now_ms.saturating_sub(st.latency_updated_at_ms) as f64;
        let decay = (-elapsed_ms / DECAY_TIME_CONSTANT_MS).exp();
        st.latency_ewma_ms = sample + (st.latency_ewma_ms - sample) * decay;
    }
    st.latency_updated_at_ms = now_ms;
}

/// EWMA score time aging — before an update, pull the score back toward the
/// default `DEFAULT_SCORE` with the time constant `score_decay_tau_ms`:
///
/// ```text
/// prior' = DEFAULT_SCORE - (DEFAULT_SCORE - prior) × exp(-Δt / τ)
/// ```
///
/// where `Δt = now_ms - score_updated_at_ms`. Half-life T½ = τ·ln2 (default
/// τ = 15min → about 10.4min). An idle channel's old penalty (low score) expires
/// over time; the high-score side converges toward 1.0 (identity).
/// `score_updated_at_ms == 0` (no baseline yet) or `score_decay_tau_ms == 0`
/// (disabled) returns the original value unchanged.
pub(super) fn age_ewma_score(st: &HealthState, cfg: &HealthSetting, now_ms: u64) -> f64 {
    let tau_ms = cfg.score_decay_tau_ms;
    if tau_ms == 0 || st.score_updated_at_ms == 0 {
        return st.ewma_score;
    }
    let dt_ms = now_ms.saturating_sub(st.score_updated_at_ms) as f64;
    let decay = (-dt_ms / (tau_ms as f64)).exp();
    DEFAULT_SCORE - (DEFAULT_SCORE - st.ewma_score) * decay
}
