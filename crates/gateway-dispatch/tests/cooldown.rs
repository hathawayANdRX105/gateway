//! P1-A outcome-driven differentiated cooldown behavior tests (spec:
//! todo/gateway-resilience.md P1-A).
//!
//! What is tested — cooldown duration is tiered by the triggering outcome, all through the
//! public `MemoryHealthTable` API plus an injected clock asserting `cooldown_until_ms - now`
//! (externally observable duration; private functions are not tested):
//! - Fatal escalated from a 401-run → max tier, significantly longer than the first step
//!   of the 5xx streak curve;
//! - 429 → independent ladder 343→1000ms (`throttle_streak` ledger, never writes the fatal
//!   streak; the 5xx curve is meanwhile climbing toward max);
//! - Neutral (400 / isolated 401) never triggers cooldown or records a streak;
//! - `unauthorized_run` counting extends to 403 (mixed credential runs escalate the same
//!   way).

use gateway_dispatch::health::{
    AttemptReport, ChannelOutcome, FailureClass, HealthSetting, HealthState, HealthTable,
    MemoryHealthTable,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Default config: 300ms base / 8s max / N=12 power law. Cooldown has no threshold, and
/// the duration climbs with the streak toward the 8s ceiling — the 8s cap is an
/// established goal of the P2C rework.
const T0: u64 = 1_000_000;

fn clock_table(now: &Arc<AtomicU64>) -> MemoryHealthTable {
    let clock = {
        let now = now.clone();
        move || now.load(Ordering::Relaxed)
    };
    MemoryHealthTable::with_config_and_clock(HealthSetting::default(), clock)
}

fn cooldown_len_ms(st: &HealthState, now: u64) -> u64 {
    st.cooldown_until_ms.saturating_sub(now)
}

#[test]
fn unauthorized_run_cooldown_is_max_tier_and_beats_5xx_curve() {
    let now = Arc::new(AtomicU64::new(T0));
    let table = clock_table(&now);

    // 5xx: cooldown has no threshold; five failures walk five rungs (0 through 4), but
    // each rung's duration depends only on the current rung. The last one lands on rung 4 =
    // 300 + 7700·(4/12)² = 1155ms.
    for _ in 0..5 {
        table.record("five_xx", AttemptReport::new(Ok(500), None, None));
    }
    let st5 = table.get("five_xx");
    assert_eq!(
        cooldown_len_ms(&st5, T0),
        1_155,
        "5xx 5th rung should be rung 4"
    );
    assert_eq!(st5.last_cooling_outcome, Some(ChannelOutcome::Fatal));

    // 401 run: the first 2 attempts are Neutral, and from the 3rd onward the run escalates
    // to Fatal and enters the max tier immediately. A 401-run takes the max tier (8s), far
    // longer than the curve tier for the same streak count on 5xx — the tiering is
    // observable.
    for _ in 0..7 {
        table.record("unauthorized", AttemptReport::new(Ok(401), None, None));
    }
    let st4 = table.get("unauthorized");
    assert_eq!(
        cooldown_len_ms(&st4, T0),
        8_000,
        "Fatal escalated from 401-run should go straight to max tier (8s)",
    );
    assert_eq!(
        st4.last_cooling_outcome,
        Some(ChannelOutcome::Fatal),
        "401-run still accounted as Fatal classification",
    );
    assert_eq!(
        st4.cooldown_streak, 5,
        "Consecutive 5 activations starting from the 3rd attempt"
    );
}

#[test]
fn throttled_climbs_independent_ladder_without_touching_fatal_streak() {
    let now = Arc::new(AtomicU64::new(T0));
    let table = clock_table(&now);

    // Independent 429 ladder: streaks 1..=4 follow the 300→1000ms power law
    // (N=THROTTLE_RAMP_STEPS), and from streak 5 it is pinned at the 1s ceiling; it
    // **never writes** cooldown_streak (the two ledgers do not contaminate each other).
    let run_ladder = [343, 475, 693, 1_000, 1_000];
    for rung in run_ladder {
        let t = table.get("throttled").cooldown_until_ms + 1;
        now.store(t, Ordering::Relaxed);
        table.record("throttled", AttemptReport::new(Ok(429), None, None));
        let st = table.get("throttled");
        assert_eq!(
            cooldown_len_ms(&st, t),
            rung,
            "throttle_streak {} should fall in {rung}ms rung",
            st.throttle_streak
        );
        assert_eq!(st.cooldown_streak, 0, "429 must not write to Fatal ledger");
        assert_eq!(st.last_cooling_outcome, Some(ChannelOutcome::Throttled));
    }

    // After the 429 ladder, one Fatal: it uses the 5xx curve's own cooldown_streak and does
    // not affect throttle_streak=5.
    let t = table.get("throttled").cooldown_until_ms + 1;
    now.store(t, Ordering::Relaxed);
    table.record("throttled", AttemptReport::new(Ok(500), None, None));
    let st = table.get("throttled");
    assert_eq!(
        st.cooldown_streak, 1,
        "Fatal uses its own ledger first rung"
    );
    assert_eq!(st.throttle_streak, 5, "429 ledger keeps original value");
}

/// A Retry-After hint overrides the 429 ladder outright: a present hint is used directly
/// (the upstream knows how long to wait), clamped to the Fatal ceiling `cooldown_max_ms`;
/// with no hint the ladder applies. The two-ledger semantics match
/// [`throttled_climbs_independent_ladder_without_touching_fatal_streak`].
#[test]
fn retry_after_hint_overrides_ladder_and_clamps_to_fatal_cap() {
    let now = Arc::new(AtomicU64::new(T0));
    let table = clock_table(&now);

    // 1) hint 8_000ms (= the Fatal ceiling) is clamped: the effective duration is 8_000ms.
    table.record("hinted", AttemptReport::new(Ok(429), None, Some(8_000)));
    let st = table.get("hinted");
    assert_eq!(
        cooldown_len_ms(&st, T0),
        8_000,
        "hint above 1s ladder but ≤ Fatal cap",
    );
    assert_eq!(
        st.throttle_streak, 1,
        "hint override doesn't change ladder streak accounting"
    );
    assert_eq!(st.last_cooling_outcome, Some(ChannelOutcome::Throttled));

    // 2) hint 60s (above the Fatal cap 8s) → clamped to 8s.
    let t = st.cooldown_until_ms + 1;
    now.store(t, Ordering::Relaxed);
    table.record("hinted", AttemptReport::new(Ok(429), None, Some(60_000)));
    let st = table.get("hinted");
    assert_eq!(
        cooldown_len_ms(&st, t),
        8_000,
        "hint exceeding Fatal ceiling clamps to cooldown_max_ms",
    );
    assert_eq!(st.throttle_streak, 2);

    // 3) no hint → ladder rung (streak=3 → 693ms).
    let t2 = st.cooldown_until_ms + 1;
    now.store(t2, Ordering::Relaxed);
    table.record("hinted", AttemptReport::new(Ok(429), None, None));
    let st = table.get("hinted");
    assert_eq!(
        cooldown_len_ms(&st, t2),
        693,
        "No hint falls back to throttle ladder streak=3 rung",
    );

    // 4) hint=0 is a valid "retry immediately": the requested duration is 0 → start_cooldown
    //    returns early when d==0, arming no new cooldown round (the expired marker was
    //    already cleared), and the channel is immediately selectable.
    let t3 = st.cooldown_until_ms + 1;
    now.store(t3, Ordering::Relaxed);
    table.record("hinted", AttemptReport::new(Ok(429), None, Some(0)));
    let st = table.get("hinted");
    assert!(
        table.is_selectable("hinted", t3),
        "hint=0 → 0ms cooldown, channel immediately selectable",
    );
    assert!(
        st.cooldown_until_ms <= t3,
        "Expired old value must not re-arm cooldown window"
    );
}

#[test]
fn neutral_never_enters_cooldown() {
    let now = Arc::new(AtomicU64::new(T0));
    let table = clock_table(&now);

    // A bad request (400) and an isolated 401 (followed by success) are both not the
    // channel's fault.
    for _ in 0..20 {
        table.record("bad_request", AttemptReport::new(Ok(400), None, None));
        table.record("orphan_401", AttemptReport::new(Ok(401), None, None));
        table.record("orphan_401", AttemptReport::new(Ok(200), None, None));
    }
    for key in ["bad_request", "orphan_401"] {
        let st = table.get(key);
        assert_eq!(st.cooldown_until_ms, 0, "{key} should not enter cooldown");
        assert_eq!(st.last_cooling_outcome, None);
        assert!(table.is_selectable(key, T0));
    }
    // Neutral does not change the score: a channel with only 400s keeps a full EWMA.
    assert_eq!(table.get("bad_request").ewma_score, 1.0);
}

#[test]
fn unauthorized_run_counts_403_alongside_401() {
    let now = Arc::new(AtomicU64::new(T0));
    let table = clock_table(&now);

    // Mixed run: 401 and 403 are still Neutral in isolation (no score change), and the 3rd
    // reaches the threshold and escalates to Fatal.
    table.record("mixed", AttemptReport::new(Ok(401), None, None));
    table.record("mixed", AttemptReport::new(Ok(403), None, None));
    let st = table.get("mixed");
    assert_eq!(
        st.cooldown_until_ms, 0,
        "Isolated 401/403 should keep Neutral semantics, not enter cooldown",
    );
    table.record("mixed", AttemptReport::new(Ok(403), None, None));
    let st = table.get("mixed");
    assert_eq!(st.unauthorized_run, 3, "403 should count same as 401");
    assert!(
        table.get("mixed").cooldown_until_ms > T0,
        "run reaching threshold should escalate to Fatal and immediately enter cooldown",
    );

    // Success clears the run; 403 no longer goes through the old `_` branch's clear
    // semantics.
    table.record("mixed", AttemptReport::new(Ok(200), None, None));
    assert_eq!(table.get("mixed").unauthorized_run, 0);

    // A transport-layer failure (non-credential) takes the curve tier rather than the max
    // tier: accumulate a run first, then disconnect → cleared.
    table.record("transport", AttemptReport::new(Ok(401), None, None));
    table.record("transport", AttemptReport::new(Ok(401), None, None));
    table.record(
        "transport",
        AttemptReport::new(Err(FailureClass::Retryable), None, None),
    );
    assert_eq!(table.get("transport").unauthorized_run, 0);
}

// ---------- Config hot update: set_config swaps parameters without
// resetting state ----------

#[test]
fn set_config_hot_swaps_tier_cap_without_resetting_state() {
    let now = Arc::new(AtomicU64::new(T0));
    let table = clock_table(&now);

    // Before the hot update: default settings. A 429 takes the independent ladder's first
    // rung = 343ms (no longer pinned to the 300ms base).
    table.record("u", AttemptReport::new(Ok(429), None, None));
    let before = table.get("u");
    assert_eq!(
        cooldown_len_ms(&before, T0),
        343,
        "429 first rung = throttle_cooldown_duration_ms(streak=1) = 343ms",
    );

    // Hot update: ceiling 8s → 5s.
    table.set_config(HealthSetting {
        cooldown_max_ms: 5_000,
        ..HealthSetting::default()
    });
    assert_eq!(
        table.get_config().cooldown_max_ms,
        5_000,
        "get_config should read the hot-updated value",
    );

    // State is not reset: the cooldown streak and the cooldown deadline keep their
    // pre-update values.
    let after = table.get("u");
    assert_eq!(
        after.cooldown_streak, before.cooldown_streak,
        "Hot update must not reset cooldown streak",
    );
    assert_eq!(
        after.cooldown_until_ms, before.cooldown_until_ms,
        "Hot update must not reset cooldown deadline",
    );

    // New failures after the hot update use the new ceiling: after the expiry time, 13
    // consecutive failures (rung ≥ N=12 tops out) should yield the new cap of 5s, not the
    // 8s from boot.
    now.store(after.cooldown_until_ms + 1, Ordering::Relaxed);
    for _ in 0..13 {
        table.record("u", AttemptReport::new(Ok(500), None, None));
    }
    let capped = table.get("u");
    assert_eq!(
        cooldown_len_ms(&capped, now.load(Ordering::Relaxed)),
        5_000,
        "After hot update streak hitting cap should be new cap 5s",
    );
}
