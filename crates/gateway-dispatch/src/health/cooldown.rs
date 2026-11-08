//! Cooldown ladder and annealing — the cooldown submodule of the parent `health` module
//! (split out in step 4, logic unchanged word for word).
//!
//! Ownership: the Fatal increasing ladder [`cooldown_duration_ms`] and the separate 429
//! ladder `throttle_cooldown_duration_ms` (power-law shape, sharing the private
//! `SHAPE_EXPONENT`), plus cooldown entry/exit (`start_cooldown` / `finish_cooldown`) and
//! idle annealing `anneal_streak`. Outcome-side accounting lives in the [`outcome`]
//! submodule.

use super::outcome::{THROTTLE_RAMP_STEPS, UNAUTHORIZED_ESCALATION_THRESHOLD};
use super::{ChannelOutcome, HealthSetting, HealthState};

/// Shape exponent `p` of the cooldown ladder (see [`cooldown_duration_ms`]).
///
/// `p > 1` convex = gentle start, steep later (slow then fast); `p < 1` concave = steep
/// start. With 2.0: at base=300ms / max=8s / N=12 the first step adds only 53ms (18% of
/// base) — a sub-second start requires "almost no penalty for the first failure", and a
/// power law is the simplest form that achieves it.
const SHAPE_EXPONENT: f64 = 2.0;

/// Shared shape of the power-law ladder ([`cooldown_duration_ms`] /
/// [`throttle_cooldown_duration_ms`]): maps `streak` onto a duration in `[base, max]`,
/// topping out at `steps` rungs.
fn power_law_duration_ms(base: u64, max: u64, steps: f64, streak: u32) -> u64 {
    let steps = steps.max(1.0);
    let n = (streak as f64).min(steps).max(0.0);
    let ramp = (n / steps).powf(SHAPE_EXPONENT);
    (base as f64 + (max - base) as f64 * ramp).clamp(base as f64, max as f64) as u64
}

/// Cooldown duration grows with the consecutive-failure count, approaching the
/// `cooldown_max_ms` ceiling. base/max are both milliseconds.
///
/// ```text
/// duration(n) = min(max_ms, base_ms + (max_ms - base_ms) × (n/N)^p)
/// where        N = cooldown_ramp_steps, p = SHAPE_EXPONENT
/// ```
///
/// **Why a power law**: with `p = 2` the function is strictly convex — going forward
/// (accumulating failures) the step size grows, slow then fast; going backward
/// (`anneal_streak` decrements streak window by window) walks the same curve back and the
/// step size shrinks — fast then slow. **One formula serves both directions**, so recovery
/// needs no separate decay law.
///
/// **Why not tan**: tan's `φ(n) = n·π/(2N)` uses the same `N` to decide both the early
/// slope and the top-out point — slowing the start requires a larger `N`, but a larger
/// `N` also pushes the top-out point back (with N=40 it takes 20 rungs to cap, and the
/// first 9 steps are almost constant, flattening the convexity into a straight line). The
/// power law **decouples shape (p) from horizon (N)**: p alone controls how gentle the
/// start is, N alone controls how many rungs before the cap.
///
/// Default ladder (300ms → 8s, N=12, p=2): 0.35 → 0.51 → 0.78 → 1.16 → 1.64 →
/// 2.23 → 2.92 → 3.72 → 4.63 → 5.65 → 6.77 → 8.00s, with step sizes
/// 53 / 160 / 267 / 374 / 481 / 588 / 695 / 802 / 909 / 1016 / 1123 / 1230ms, strictly
/// increasing. By contrast tan(N=12) starts at 1277ms (426% of base, under the old 10s
/// ceiling convention), while the power law's first rung adds only 53ms (18%).
///
/// For `n ≥ N`, `(n/N)^p ≥ 1` is pinned by `min(max, …)`, so the cooldown duration is
/// always ≤ `max`.
pub(super) fn cooldown_duration_ms(cfg: &HealthSetting, streak: u32) -> u64 {
    let base = cfg.cooldown_base_ms;
    let max = cfg.cooldown_max_ms;
    if base == 0 || max == 0 || max <= base {
        return base;
    }
    power_law_duration_ms(base, max, f64::from(cfg.cooldown_ramp_steps.max(1)), streak)
}

/// Separate 429 ladder — cooldown duration grows with `throttle_streak`, approaching the
/// `throttle_max_ms` ceiling (decoupled from Fatal's `cooldown_max_ms`).
///
/// ```text
/// duration(n) = min(throttle_max_ms,
///              cooldown_base_ms + (throttle_max_ms - cooldown_base_ms)
///              × (n/THROTTLE_RAMP_STEPS)^p)   p = SHAPE_EXPONENT
/// ```
///
/// Same shape as [`cooldown_duration_ms`] (reusing the power law and `SHAPE_EXPONENT`),
/// but the horizon is fixed at `THROTTLE_RAMP_STEPS` = 4 rungs to the cap — 429 is
/// transient throttling, so the ladder is short and steep; with the default 300ms → 1s the
/// ladder is 343 → 475 → 693 → 1000ms (truncated). For `n ≥ N`, `(n/N)^p ≥ 1` is pinned,
/// so the duration is always ≤ `throttle_max_ms`.
pub fn throttle_cooldown_duration_ms(cfg: &HealthSetting, streak: u32) -> u64 {
    let base = cfg.cooldown_base_ms;
    let max = cfg.throttle_max_ms;
    if base == 0 || max == 0 || max <= base {
        return base;
    }
    power_law_duration_ms(base, max, f64::from(THROTTLE_RAMP_STEPS.max(1)), streak)
}

/// Enter cooldown and cap every outcome class uniformly at `cooldown_max_ms`.
///
/// Consecutive 401/403 escalation and other fatal failures take the Fatal ladder
/// (`cooldown_streak` → [`cooldown_duration_ms`]); 429 takes the separate ladder
/// (`throttle_streak` → [`throttle_cooldown_duration_ms`]), and a Retry-After hint, when
/// present, overrides the ladder directly (clamped to `cooldown_max_ms`). No cooldown ever
/// exceeds the configured ceiling.
pub(super) fn start_cooldown(
    st: &mut HealthState,
    cfg: &HealthSetting,
    now_ms: u64,
    outcome: ChannelOutcome,
    retry_after_ms: Option<u64>,
) {
    let max_ms = cfg.cooldown_max_ms;
    let requested_ms = match outcome {
        ChannelOutcome::Fatal if st.unauthorized_run >= UNAUTHORIZED_ESCALATION_THRESHOLD => {
            st.cooldown_streak += 1;
            max_ms
        }
        ChannelOutcome::Throttled => {
            // 429 is upstream-initiated throttling, not a channel fault: separate ladder
            // 343→475→693→1000ms (default range), and does not write cooldown_streak. A
            // Retry-After hint, when present, overrides the ladder — upstream knows how long
            // to wait better than a local estimate.
            st.throttle_streak += 1;
            let ladder_ms = throttle_cooldown_duration_ms(cfg, st.throttle_streak);
            retry_after_ms.map_or(ladder_ms, |hint_ms| hint_ms.min(max_ms))
        }
        _ => {
            // Take the rung with the current streak first, then increment: first failure =
            // rung 0 = base, consistent with the power-law ladder docs (the k-th failure lands
            // on rung k-1).
            let ladder_ms = cooldown_duration_ms(cfg, st.cooldown_streak);
            st.cooldown_streak += 1;
            ladder_ms
        }
    };
    let d = requested_ms.min(max_ms);
    if d == 0 {
        return;
    }
    st.last_cooling_outcome = Some(outcome);
    st.request_count = 0;
    st.ramp_exited = false;
    st.cooldown_until_ms = now_ms + d;
}

/// End cooldown — phase0 `finishCooldownLocked`: clear the cooldown and arm the slow-start
/// ramp. `now_ms` becomes the annealing anchor: if the channel then stays idle, both streak
/// ledgers decay identically in base-sized windows.
pub(super) fn finish_cooldown(st: &mut HealthState, now_ms: u64) {
    st.cooldown_until_ms = 0;
    st.request_count = 0;
    st.ramp_exited = false;
    st.ramp_pending = true;
    st.anneal_since_ms = now_ms;
}

/// Idle annealing — after the cooldown expires, if traffic stays absent, `cooldown_streak`
/// and `throttle_streak` both fall linearly over time (reusing the same base window, no new
/// config).
///
/// The annealing window reuses `cooldown_base_ms`: each full window drops both ledgers by 1
/// (saturating at 0), and the remainder carries into the next settlement. Active traffic
/// itself decrements both ledgers and clears the anchor through apply_outcome's Success
/// branch, so annealing only handles "silent" channels; when the anchor is 0 (a success has
/// happened) it sleeps until the next cooldown end sets a new anchor.
///
/// - `st`: state to settle, decayed in place;
/// - `cfg`: provides the annealing window length;
/// - `now_ms`: current clock.
///
/// Called lazily only on the read path (is_selectable) — record itself is an active event
/// and needs no annealing.
pub(super) fn anneal_streak(st: &mut HealthState, cfg: &HealthSetting, now_ms: u64) {
    if st.cooldown_until_ms != 0
        || st.anneal_since_ms == 0
        || (st.cooldown_streak == 0 && st.throttle_streak == 0)
    {
        return;
    }
    let window_ms = cfg.cooldown_base_ms;
    if window_ms == 0 {
        return;
    }
    let steps = now_ms.saturating_sub(st.anneal_since_ms) / window_ms;
    if steps == 0 {
        return;
    }
    st.cooldown_streak = st.cooldown_streak.saturating_sub(steps as u32);
    st.throttle_streak = st.throttle_streak.saturating_sub(steps as u32);
    st.anneal_since_ms += steps * window_ms;
    if st.cooldown_streak == 0 && st.throttle_streak == 0 {
        st.anneal_since_ms = 0;
    }
}
