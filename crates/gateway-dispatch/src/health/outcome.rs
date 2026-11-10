//! Outcome classification and accounting — the outcome submodule of the `health` parent module
//! (split in step 4; logic unchanged).
//!
//! Ownership: the engine-side payload [`FailureClass`] and [`AttemptReport`], the 429-ladder
//! constants, along with `classify` (status code → outcome, including 401/403 consecutive
//! escalation) and `apply_outcome` (accounting entry: EWMA observation, cooldown, slow-start).
//! The shared vocabulary ([`ChannelOutcome`] / [`TpsSample`] / the EWMA constants) lives on the
//! vocabulary crate (`gateway-core`) and is re-exported by the parent module. The ladder
//! durations themselves live in the [`cooldown`] submodule.

use super::cooldown::{finish_cooldown, start_cooldown};
use super::{ChannelOutcome, HealthSetting, HealthState, TpsSample, age_ewma_score};

/// Retryable error classification — decides whether the state machine backs off to the next
/// candidate. Health classification is independent of retry decisions: health looks at upstream
/// status codes, retries look at client recoverability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureClass {
    /// Transport-layer failure (timeout/connection) → switch candidate and retry.
    Retryable,
    /// Client or credential problem (4xx) → no retry.
    Fatal,
}

/// EWMA observation for 429 (light down-weight, not fatal).
pub const THROTTLED_OBSERVATION: f64 = 0.7;
/// Consecutive 401/403 at this count escalate to Fatal.
pub const UNAUTHORIZED_ESCALATION_THRESHOLD: u32 = 3;

/// Horizon N (steps) of the separate 429 ladder: the N-th consecutive 429 reaches
/// `throttle_max_ms` (see
/// [`throttle_cooldown_duration_ms`](super::cooldown::throttle_cooldown_duration_ms)).
/// 429 is transient rate limiting, so the ladder is deliberately short and steep: fixed at 4
/// steps (no config knob) — a much shorter horizon than the Fatal curve (N=12), avoiding long
/// cooldowns burned into the health table for a channel that stays rate-limited; a Retry-After
/// hint override is the main mechanism for the 429 duration.
pub const THROTTLE_RAMP_STEPS: u32 = 4;

/// Health report payload for one attempt — the unified structure for `HealthTable::record` /
/// `Dispatch::report` (no longer split between trait and mock after the old four-arg signature
/// was removed). The constructor does not clamp values: non-physical payloads (such as throughput
/// samples that are NaN/Inf/≤0) are discarded at the `HealthTable` ingest boundary
/// (`record` / `record_tps` guards); the payload only carries them.
#[derive(Debug, Clone, Copy)]
pub struct AttemptReport {
    /// `Ok(status)` classified by status code; `Err(FailureClass)` transport/client failure.
    pub outcome: Result<u16, FailureClass>,
    /// Upstream round-trip latency for this attempt (ms); failure or no observation = `None`.
    pub latency_ms: Option<u32>,
    /// Upstream 429 `Retry-After` hint (ms); only meaningful for Throttled, discarded otherwise.
    pub retry_after_ms: Option<u64>,
    /// Generated throughput sample (tok/s); non-streaming / interrupted stream / not computed =
    /// `None`.
    pub tps: Option<TpsSample>,
}

impl AttemptReport {
    /// Build a basic report (`tps` defaults to `None`); when a throughput sample is needed, chain
    /// the [`with_tps`](AttemptReport::with_tps) builder.
    pub fn new(
        outcome: Result<u16, FailureClass>,
        latency_ms: Option<u32>,
        retry_after_ms: Option<u64>,
    ) -> Self {
        Self {
            outcome,
            latency_ms,
            retry_after_ms,
            tps: None,
        }
    }

    /// Attach a generated throughput sample (consumes `self`, returns a new value with tps).
    pub fn with_tps(mut self, sample: TpsSample) -> Self {
        self.tps = Some(sample);
        self
    }
}

/// Classify by result and record it, preserving the existing outcome, health score, cooldown, and
/// slow-start semantics. `retry_after_ms`: upstream 429 `Retry-After` hint (ms) — passed to the
/// cooldown ladder as an override only when outcome is classified as Throttled (clamped to
/// `cooldown_max_ms`); discarded for other outcomes.
pub(super) fn apply_outcome(
    st: &mut HealthState,
    outcome: Result<u16, FailureClass>,
    cfg: &HealthSetting,
    now_ms: u64,
    retry_after_ms: Option<u64>,
) {
    if !cfg.enabled {
        return;
    }

    // Settle an expired cooldown first (post-expiry results cleanly re-enter the slow-start ramp).
    if st.cooldown_until_ms != 0 && !st.is_cooling(now_ms) {
        finish_cooldown(st, now_ms);
    }

    let outcome = classify(st, outcome);

    match outcome {
        ChannelOutcome::Success => {
            // Active event re-arms the period: clear the annealing anchor and recompute the idle
            // window from the start.
            st.anneal_since_ms = 0;
            // A clean success after cooldown expiry decrements the cooldown streak (the next
            // failure starts from a shorter duration).
            if st.cooldown_streak > 0 && st.cooldown_until_ms == 0 {
                st.cooldown_streak = st.cooldown_streak.saturating_sub(1);
            }
            // The separate 429 ledger decrements in the same way: success proves the rate limit has
            // eased.
            if st.throttle_streak > 0 && st.cooldown_until_ms == 0 {
                st.throttle_streak = st.throttle_streak.saturating_sub(1);
            }
        }
        // Neutral (400/404/isolated 401): channel not at fault — no score change, no request count,
        // no cooldown; early-return unchanged in the observation branch below.
        // All Fatal / Throttled penalties are carried by start_cooldown below, so no cumulative
        // counter is needed here.
        ChannelOutcome::Neutral | ChannelOutcome::Fatal | ChannelOutcome::Throttled => {}
    }

    // EWMA observation. Neutral returns early here (no score change / no request count / no
    // cooldown).
    let observation = match outcome {
        ChannelOutcome::Success => 1.0,
        ChannelOutcome::Fatal => 0.0,
        ChannelOutcome::Throttled => THROTTLED_OBSERVATION,
        ChannelOutcome::Neutral => return,
    };

    st.request_count += 1;
    st.ramp_pending = false;

    // A real failure exits the ramp immediately.
    if outcome == ChannelOutcome::Fatal {
        st.ramp_exited = true;
    }

    // Do not update EWMA before MinRequests (trust new channels).
    if st.request_count > cfg.min_requests {
        // Before updating, age the score toward the default by Δt (score_decay_tau_ms time
        // constant): an idle channel's old penalty expires over time; half-life T½ = τ·ln2.
        let prior = age_ewma_score(st, cfg, now_ms);
        st.ewma_score = cfg.alpha * observation + (1.0 - cfg.alpha) * prior;
        if st.ewma_score < cfg.min_score {
            st.ewma_score = cfg.min_score;
        }
        st.score_updated_at_ms = now_ms;
    }

    // Any fatal/throttled outcome enters cooldown immediately — no more "N consecutive" threshold.
    // The full penalty strength comes from the duration ladders (Fatal 300ms → 8s; 429 300ms → 1s):
    // the first failure removes the channel for only the base rung (300ms for Fatal, 343ms for the
    // first 429 rung; it comes back quickly); only repeated hits stretch the duration. If the
    // threshold were kept, a bad channel would still receive traffic
    // during the window while the count accumulates, and retries — guaranteed by mark_tried not to
    // reselect the same unit within a request — would never observe "sustained failure"; nobody
    // fills that window.
    if matches!(outcome, ChannelOutcome::Fatal | ChannelOutcome::Throttled) {
        start_cooldown(st, cfg, now_ms, outcome, retry_after_ms);
    }
}

/// Classification — phase0 `classifyChannelOutcomeUnlocked` + UnauthorizedRun escalation.
/// 401/403 have a separate branch (P1-A): an isolated credential error keeps Neutral semantics (the
/// channel is not at fault and its score does not change), but counts toward unauthorized_run;
/// reaching the threshold consecutively → Fatal. All other branches (success/429/5xx/other 4xx/
/// transport/client errors) reset it.
fn classify(st: &mut HealthState, outcome: Result<u16, FailureClass>) -> ChannelOutcome {
    match outcome {
        Ok(401 | 403) => {
            if st.unauthorized_run < UNAUTHORIZED_ESCALATION_THRESHOLD {
                st.unauthorized_run += 1;
            }
            if st.unauthorized_run >= UNAUTHORIZED_ESCALATION_THRESHOLD {
                ChannelOutcome::Fatal
            } else {
                ChannelOutcome::Neutral
            }
        }
        Ok(status) => {
            st.unauthorized_run = 0;
            classify_status(status)
        }
        Err(FailureClass::Retryable) => {
            // Transport-layer failure (timeout/connection) → the channel is currently unavailable.
            st.unauthorized_run = 0;
            ChannelOutcome::Fatal
        }
        Err(FailureClass::Fatal) => {
            // Client problem → the channel is not at fault.
            st.unauthorized_run = 0;
            ChannelOutcome::Neutral
        }
    }
}

/// Status code → outcome (consecutive escalation on 401/403 is handled by [`classify`]).
fn classify_status(status: u16) -> ChannelOutcome {
    match status {
        200..=299 => ChannelOutcome::Success,
        429 => ChannelOutcome::Throttled,
        500..=599 => ChannelOutcome::Fatal,
        // Other 4xx (400/404/422, etc.): channel not at fault, no score change.
        _ => ChannelOutcome::Neutral,
    }
}
