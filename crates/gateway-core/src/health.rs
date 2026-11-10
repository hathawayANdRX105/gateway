//! Health vocabulary shared by the admin plane and the dispatch engine.
//!
//! The engine (`gateway-dispatch`) owns the state machine: outcome classification,
//! the cooldown ladders, idle annealing, and the hot-path table. What lives here is
//! the data both sides pass around — the per-unit health record, the config the
//! admin plane reads and hot-swaps, and the outcome class the observability surface
//! renders. Keeping them here is what lets `api-observe` and `api-gateway-state`
//! read or retune the health table without depending on `gateway-dispatch`.

/// Health outcome — the phase0 `ChannelOutcome` enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelOutcome {
    /// 2xx — fully trusted, EWMA obs = 1.0.
    Success,
    /// 5xx / transport layer / bad body — record streak, obs = 0.0, trigger cooldown.
    Fatal,
    /// 429 — channel healthy but rate-limited; light down-weight, obs = 0.7.
    Throttled,
    /// Other 4xx (400/404/422/isolated 401/403) — channel not at fault, no score change and no
    /// streak.
    Neutral,
}

impl ChannelOutcome {
    /// Whether it affects the EWMA score (Success/Fatal/Throttled; Neutral does not participate).
    #[must_use]
    pub fn affects_health(self) -> bool {
        !matches!(self, ChannelOutcome::Neutral)
    }
}

/// Default score for a channel with no history.
pub const DEFAULT_SCORE: f64 = 1.0;
/// tps EMA smoothing coefficient — split by source: 0.2 for measured samples (main EMA), 0.1 for
/// estimated fallback samples (noise isolation; see design §3.2: "estimated fallback samples
/// without usage: update but halve α").
pub const TPS_ALPHA_MEASURED: f64 = 0.2;
pub const TPS_ALPHA_ESTIMATED: f64 = 0.1;

/// Generated throughput sample (tok/s) — input to the channel-level EMA.
///
/// Validation (`>= 0.0`, non-finite → zero) is the responsibility of the forward-side calculation
/// point; this enum does no numeric validation — it only distinguishes two sources, and the EMA
/// picks a different α per source:
/// - [`Measured`](TpsSample::Measured): measured throughput from a normally completed stream
///   (`completion_tokens / generation-segment elapsed`), α = 0.2;
/// - [`Estimated`](TpsSample::Estimated): estimated fallback sample without usage (bytes/4),
///   noisier; α = 0.1 isolates it from the main EMA.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TpsSample {
    Measured(f64),
    Estimated(f64),
}

/// Health record of a single `RouteUnit` — phase0 `ChannelHealthState`.
#[derive(Debug, Clone, Copy)]
pub struct HealthState {
    /// EWMA continuous health score, range [`MinScore`, 1.0]; no history = 1.0.
    pub ewma_score: f64,
    /// Peak-sensitive EWMA of upstream latency (ms); 0.0 = no latency observation
    /// yet.
    pub latency_ewma_ms: f64,
    /// Instant of the latest latency observation (unix ms); used to time-decay
    /// the peak EWMA.
    pub latency_updated_at_ms: u64,
    /// Generation throughput EMA (tok/s): measured samples α=0.2, estimated
    /// fallback samples α=0.1 (noise isolated outside the main EMA); 0.0 = no
    /// observation yet (the selector treats it as the neutral factor 1.0).
    /// Scalar, so `Copy` semantics remain intact.
    pub tps_ema: f64,
    /// Observed request count; EWMA is not updated until `MinRequests` is exceeded
    /// (trusting new channels).
    pub request_count: u32,
    /// Consecutive 401/403 (credential-class) count; reaching
    /// `UnauthorizedEscalationThreshold` → Fatal.
    pub unauthorized_run: u32,
    /// Leave the slow-start ramp after a real failure (no more gradual recovery).
    pub ramp_exited: bool,
    /// Cooldown just ended; the first selection starts from the ramp floor.
    pub ramp_pending: bool,
    /// Number of consecutive cooldown activations; determines the Fatal cooldown
    /// duration (increasing curve, see the engine's cooldown ladder).
    pub cooldown_streak: u32,
    /// Number of consecutive 429 (Throttled) activations — a ledger fully
    /// independent of `cooldown_streak`: 429 does not write `cooldown_streak`, Fatal
    /// never touches `throttle_streak`. Determines the separate 429 ladder rung; a
    /// successful report and idle annealing decrement it in the same way.
    pub throttle_streak: u32,
    /// Cooldown deadline (unix ms); 0 = not cooling.
    pub cooldown_until_ms: u64,
    /// Instant of the latest EWMA health-score update (unix ms); 0 = no update
    /// baseline yet. Before the next EWMA update the score ages toward the default
    /// with the `score_decay_tau_ms` time constant.
    pub score_updated_at_ms: u64,
    /// Annealing anchor (unix ms): the instant the latest cooldown ended; 0 = no
    /// idle-annealing anchor. While idle, `cooldown_streak` and `throttle_streak`
    /// decay from here in identical base-sized windows.
    pub anneal_since_ms: u64,
    /// Outcome that triggered the latest cooldown (basis for differentiated
    /// cooldown: an escalated 401-run Fatal → max tier; 5xx streak → the regular
    /// curve; 429 → separate ladder/Retry-After tier).
    /// None = never cooled down.
    pub last_cooling_outcome: Option<ChannelOutcome>,
}

impl Default for HealthState {
    fn default() -> Self {
        Self {
            ewma_score: DEFAULT_SCORE,
            latency_ewma_ms: 0.0,
            latency_updated_at_ms: 0,
            request_count: 0,
            tps_ema: 0.0,
            unauthorized_run: 0,
            ramp_exited: false,
            ramp_pending: false,
            cooldown_streak: 0,
            throttle_streak: 0,
            cooldown_until_ms: 0,
            score_updated_at_ms: 0,
            anneal_since_ms: 0,
            last_cooling_outcome: None,
        }
    }
}

impl HealthState {
    /// Whether the channel is currently inside a cooldown window (and it has not
    /// expired yet).
    #[must_use]
    pub fn is_cooling(&self, now_ms: u64) -> bool {
        self.cooldown_until_ms > now_ms
    }

    /// Slow-start factor — phase0 `slowStartFactor`:
    /// - `RampExited` → 1.0 (a real failure; no more gradual recovery);
    /// - `RampPending` → `1/min_requests` (the ramp floor for the first pick after
    ///   cooldown);
    /// - below `min_requests` → `count/min_requests` (gradual; a fresh record with
    ///   `count == 0` stays at 1.0 because new channels are trusted);
    /// - at or above the threshold → 1.0.
    #[must_use]
    pub fn slow_start_factor(&self, min_requests: u32) -> f64 {
        if min_requests == 0 || self.ramp_exited {
            return 1.0;
        }
        if self.ramp_pending {
            return 1.0 / f64::from(min_requests);
        }
        if self.request_count == 0 || self.request_count >= min_requests {
            return 1.0;
        }
        f64::from(self.request_count) / f64::from(min_requests)
    }

    /// Merge a generation throughput EMA (design §3.2/§3.3): `α` depends on the
    /// sample source (`Measured` 0.2 / `Estimated` 0.1), formula
    /// `ema = α·sample + (1−α)·ema`; `ema == 0.0` (first sample) takes the sample
    /// value directly. Only `tps_ema` moves; cooldown/streak/EWMA/latency are never
    /// touched.
    pub fn update_tps_ema(&mut self, sample: TpsSample) {
        let (alpha, value) = match sample {
            TpsSample::Measured(v) => (TPS_ALPHA_MEASURED, v),
            TpsSample::Estimated(v) => (TPS_ALPHA_ESTIMATED, v),
        };
        self.tps_ema = if self.tps_ema == 0.0 {
            value
        } else {
            alpha * value + (1.0 - alpha) * self.tps_ema
        };
    }
}

/// Health config — phase0 `ChannelHealthSetting` defaults.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HealthSetting {
    pub enabled: bool,
    /// EWMA smoothing coefficient.
    pub alpha: f64,
    /// Lower bound of the health score.
    pub min_score: f64,
    /// Minimum request count before the EWMA is trusted.
    pub min_requests: u32,
    /// Cooldown start duration (ms) — the ladder's starting point; **any single
    /// failure enters cooldown immediately**.
    ///
    /// Sub-second (300ms): a failure removes the channel at once, and within
    /// 300ms it is neither reselected by the same request nor immediately
    /// reconnected by subsequent requests — this is the minimum interval that
    /// "prevents a direct reconnect".
    pub cooldown_base_ms: u64,
    /// Cooldown ceiling (ms) — the **upper bound** of the increasing curve, not a
    /// fixed value.
    pub cooldown_max_ms: u64,
    /// Horizon N (number of steps) of the slow-climb ladder: N increments above
    /// the base rung land exactly on the max ceiling (`(n/N)^p`, see the engine's
    /// cooldown ladder; the first failure starts at base, so the (N+1)-th
    /// consecutive hit is the first at max). A larger N means more, gentler steps.
    /// The shape exponent `p` is fixed in the engine, so N only controls "how many
    /// steps to the cap" and does not change the feel of the first few steps.
    pub cooldown_ramp_steps: u32,
    /// Reaching this many cooldown activations disables the model on the channel;
    /// no code path reads this field today (selection is gated by cooldown state).
    pub cooldown_disable_streak: u32,
    /// Ceiling (ms) of the separate 429 ladder — Throttled cooldown range
    /// `cooldown_base_ms → throttle_max_ms` (default 300ms → 1s), topped out at a
    /// fixed step count. Decoupled from Fatal's `cooldown_max_ms`: 429 is
    /// transient rate limiting, so the ceiling is deliberately an order of
    /// magnitude below the Fatal ceiling (8s). A Retry-After hint that overrides
    /// the ladder is still clamped to `cooldown_max_ms`.
    pub throttle_max_ms: u64,
    /// EWMA score time-decay constant τ (ms) — the time scale on which the score
    /// ages toward the default before an update
    /// (`prior' = 1 - (1-prior)×exp(-Δt/τ)`; the engine owns the aging step).
    /// Default 15min; half-life T½ = τ·ln2. `0` = off (old behavior: the score is
    /// frozen forever).
    pub score_decay_tau_ms: u64,
}

impl Default for HealthSetting {
    fn default() -> Self {
        Self {
            enabled: true,
            alpha: 0.3,
            min_score: 0.05,
            min_requests: 5,
            // Power-law ladder (n/N)^2 with a sub-second start, approaching the 8s
            // ceiling from 300ms and topping out at N=12 steps:
            // → 0.35 → 0.51 → 0.78 → 1.16 → 1.64 → 2.23 → 2.92 → 3.72 →
            //   4.63 → 5.65 → 6.77 → 8.00s.
            // Steps 53 / 160 / 267 / 374 / 481 / 588 / 695 / 802 / 909 / 1016 /
            // 1123 / 1230ms strictly increasing
            // (slow first, then fast); one success drops one level (see the Success
            // branch of apply_outcome), so a flapping channel stays at the 300ms
            // rung forever.
            cooldown_base_ms: 300,
            cooldown_max_ms: 8_000,
            cooldown_ramp_steps: 12,
            cooldown_disable_streak: 3,
            // Separate 429 ladder: starts at 300ms and tops out after 4 steps,
            // default ladder 343 → 475 → 693 → 1000ms (truncated to whole ms).
            throttle_max_ms: 1_000,
            // EWMA time decay with a 15min time constant (half-life ≈ 10.4min).
            score_decay_tau_ms: 900_000,
        }
    }
}

impl HealthSetting {
    /// Config ingest validation — returns the reason string of the first violating
    /// field; the config is validated and accepted as a whole, illegal values are
    /// never partially applied.
    /// - `alpha` / `min_score`: finite and ∈ [0, 1] (NaN/Inf and out-of-range are
    ///   rejected);
    /// - `cooldown_base_ms`: must not exceed either ceiling `cooldown_max_ms` /
    ///   `throttle_max_ms` (the ladder start must be ≤ the ceiling, or the duration
    ///   formula degenerates).
    ///
    /// Two callers share this single rule set: the live-reload entry
    /// (`MemoryHealthTable::set_config` in the dispatch engine) and the boot exit in
    /// `api-gateway-state` (per-key option reads only guarantee a default per key;
    /// cross-field violations are caught only here). The constructors
    /// (`with_config` / `with_config_and_clock`) **do not** call it — those entry
    /// points must support deterministic assembly and relaxed configs; validation
    /// closes only at the config ingest boundary.
    ///
    /// # Errors
    ///
    /// Returns a static message naming the first violated field: `alpha` or
    /// `min_score` outside `[0, 1]` (or non-finite), or `cooldown_base_ms`
    /// exceeding `cooldown_max_ms`.
    pub fn validate(&self) -> Result<(), &'static str> {
        if !self.alpha.is_finite() || !(0.0..=1.0).contains(&self.alpha) {
            return Err("alpha must be finite and in [0, 1]");
        }
        if !self.min_score.is_finite() || !(0.0..=1.0).contains(&self.min_score) {
            return Err("min_score must be finite and in [0, 1]");
        }
        if self.cooldown_base_ms > self.cooldown_max_ms {
            return Err("cooldown_base_ms must not exceed cooldown_max_ms");
        }
        if self.cooldown_base_ms > self.throttle_max_ms {
            return Err("cooldown_base_ms must not exceed throttle_max_ms");
        }
        Ok(())
    }
}

/// Admin-plane port over the running health table.
///
/// Only the operations the admin plane performs: list every record for the panel,
/// read the config that is in effect, replace it on a live reload. The engine keeps
/// its own richer hot-path trait (`gateway_dispatch::health::HealthTable` with
/// record / selection gating); this port deliberately does not expose them so the
/// admin plane cannot grow into the request path.
pub trait HealthHandle: Send + Sync {
    /// Whole-table read: `(unit_key, state)` for every current record. Read-only,
    /// without settling expired cooldowns — the query surface has no data-plane
    /// side effects.
    fn entries(&self) -> Vec<(String, HealthState)>;

    /// Config currently in effect (clone of the live value).
    fn config(&self) -> HealthSetting;

    /// Replace the whole config at runtime. Parameters only: cooldown / streak /
    /// EWMA state is never reset.
    fn set_config(&self, cfg: HealthSetting);
}
