//! Behavior tests for the peak-latency EWMA plus the boundaries of the cooldown escalation
//! ladder and outcome tiering.

use gateway_dispatch::health::{
    AttemptReport, FailureClass, HealthSetting, HealthTable, MemoryHealthTable,
    UNAUTHORIZED_ESCALATION_THRESHOLD,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Downward time constant of the peak EWMA, on the same order as Finagle's `p2cPeakEwma`.
const TAU_MS: f64 = 10_000.0;

fn clock_table(setting: HealthSetting) -> (Arc<MemoryHealthTable>, Arc<AtomicU64>) {
    let now = Arc::new(AtomicU64::new(1_000_000));
    let clock = Arc::clone(&now);
    let table = Arc::new(MemoryHealthTable::with_config_and_clock(
        setting,
        move || clock.load(Ordering::Relaxed),
    ));
    (table, now)
}

#[test]
fn first_latency_sample_establishes_baseline() {
    let (table, _) = clock_table(HealthSetting::default());
    table.record("u", AttemptReport::new(Ok(200), Some(250), None));
    assert_eq!(table.get("u").latency_ewma_ms, 250.0);
    assert_eq!(table.get("u").latency_updated_at_ms, 1_000_000);
}

#[test]
fn no_latency_observation_keeps_score_zero() {
    let (table, _) = clock_table(HealthSetting::default());
    table.record("u", AttemptReport::new(Ok(200), None, None));
    assert_eq!(table.get("u").latency_ewma_ms, 0.0);
}

#[test]
fn spike_immediately_raises_latency_score() {
    let (table, now) = clock_table(HealthSetting::default());
    table.record("u", AttemptReport::new(Ok(200), Some(100), None));
    now.store(1_001_000, Ordering::Relaxed);
    table.record("u", AttemptReport::new(Ok(200), Some(5_000), None));
    assert_eq!(
        table.get("u").latency_ewma_ms,
        5_000.0,
        "Peak sensitivity: latency spikes must immediately reflect in the score"
    );
}

/// EWMA time decay (`score_decay_tau_ms`, #8): before an update, the old score ages toward
/// the default score 1.0 by Δt — an idle channel's old penalty expires over time. The
/// half-life is τ·ln2 (default 15min → the penalty magnitude halves in about 10.4min).
///
/// `min_requests = 0` disables the "trust new channels" gate: every cooldown clears
/// `request_count` (slow-start ramp semantics), so under the default gate consecutive 429
/// score observations never reach the update threshold and would mask the decay mechanism
/// under test.
fn decay_setting() -> HealthSetting {
    HealthSetting {
        min_requests: 0,
        ..HealthSetting::default()
    }
}

#[test]
fn ewma_score_ages_toward_default_during_idle() {
    let (table, now) = clock_table(decay_setting());
    // Push the score down first: after ≥ min_requests observations, apply several rounds of
    // 429 (obs=0.7).
    for _ in 0..5 {
        table.record("u", AttemptReport::new(Ok(200), None, None));
    }
    for _ in 0..20 {
        table.record("u", AttemptReport::new(Ok(429), None, None));
    }
    let t1 = now.load(Ordering::Relaxed);
    let fresh = table.get("u").ewma_score;
    assert!(
        fresh < 0.8,
        "After 20 rounds of 429 the score should be suppressed: got {fresh}"
    );

    // After idling for a full τ (15min), record one more 200 (obs=1.0): the prior ages by
    // Δt=τ before the update → prior' = 1 - (1-prior)×e^-1 ≈ 1-(1-prior)/e, shrinking the
    // penalty magnitude to 1/e ≈ 36.8%; then the EWMA blends in obs=1.0.
    now.store(t1 + 900_000, Ordering::Relaxed);
    table.record("u", AttemptReport::new(Ok(200), None, None));
    let after_idle = table.get("u").ewma_score;

    // Control: same initial score, no idle time (Δt≈0), record 200 immediately → the prior
    // does not age.
    let (table2, _now2) = clock_table(decay_setting());
    for _ in 0..5 {
        table2.record("v", AttemptReport::new(Ok(200), None, None));
    }
    for _ in 0..20 {
        table2.record("v", AttemptReport::new(Ok(429), None, None));
    }
    table2.record("v", AttemptReport::new(Ok(200), None, None));
    let no_idle = table2.get("v").ewma_score;

    assert!(
        after_idle > no_idle,
        "After idling one τ, update should pull back toward default score: idle={after_idle} no_idle={no_idle}"
    );
}

#[test]
fn ewma_decay_disabled_when_tau_is_zero() {
    let mut s = decay_setting();
    s.score_decay_tau_ms = 0;
    let (table, now) = clock_table(s);
    for _ in 0..5 {
        table.record("u", AttemptReport::new(Ok(200), None, None));
    }
    for _ in 0..10 {
        table.record("u", AttemptReport::new(Ok(429), None, None));
    }
    let t1 = now.load(Ordering::Relaxed);
    now.store(t1 + 3_600_000, Ordering::Relaxed); // idle 1h
    table.record("u", AttemptReport::new(Ok(200), None, None));
    let decayed_off = table.get("u").ewma_score;

    // τ=0 disables decay: the control table enables decay but Δt=0 → both have the same
    // prior before updating, so the post-update scores must match (the old behavior froze
    // the score).
    let mut s2 = decay_setting();
    s2.score_decay_tau_ms = 900_000;
    let (table2, _) = clock_table(s2);
    for _ in 0..5 {
        table2.record("v", AttemptReport::new(Ok(200), None, None));
    }
    for _ in 0..10 {
        table2.record("v", AttemptReport::new(Ok(429), None, None));
    }
    table2.record("v", AttemptReport::new(Ok(200), None, None));
    let no_decay = table2.get("v").ewma_score;
    assert!(
        (decayed_off - no_decay).abs() < 1e-9,
        "τ=0 should be equivalent to no decay: off={decayed_off} on_Δt0={no_decay}"
    );
}
#[test]
fn fast_response_pulls_score_down_over_time_constant() {
    // The peak EWMA decays not "from the peak toward 0" but "pulls the peak toward the
    // newest sample by a time constant": `score = sample + (old - sample) × e^(-Δ/τ)`.
    // That keeps score ≥ the newest sample at all times, so a consistently fast channel is
    // not permanently dragged down to a distorted level by an old spike.
    let (table, now) = clock_table(HealthSetting::default());
    table.record("u", AttemptReport::new(Ok(200), Some(4_000), None));
    let peak = 4_000.0_f64;
    let sample = 1_000.0_f64;

    now.store(1_010_000, Ordering::Relaxed);
    table.record("u", AttemptReport::new(Ok(200), Some(1_000), None));
    let expected = sample + (peak - sample) * (-(10_000.0 / TAU_MS)).exp();
    let actual = table.get("u").latency_ewma_ms;
    assert!(
        (actual - expected).abs() < 1e-6,
        "After one time constant should equal sample + (peak-sample)×e⁻¹ = {expected:.1}, measured {actual:.1}"
    );
    assert!(
        actual > sample,
        "Score must remain higher than the latest sample (spike residual not yet dissipated)"
    );

    // After a long idle period it converges near the newest sample.
    now.store(1_100_000, Ordering::Relaxed);
    table.record("u", AttemptReport::new(Ok(200), Some(1_000), None));
    let settled = table.get("u").latency_ewma_ms;
    assert!(
        (settled - sample).abs() < 1.0,
        "After nine time constants should basically stick to the latest sample, measured {settled:.1}"
    );
}

#[test]
fn latency_update_does_not_disturb_health_classification() {
    let (table, _) = clock_table(HealthSetting::default());
    table.record("u", AttemptReport::new(Ok(200), Some(120), None));
    let state = table.get("u");
    assert_eq!(state.request_count, 1);
    assert_eq!(
        state.ewma_score, 1.0,
        "EWMA health score of successful requests is unaffected by latency"
    );
}

#[test]
fn neutral_outcome_with_latency_still_skips_health_accounting() {
    let (table, _) = clock_table(HealthSetting::default());
    table.record("u", AttemptReport::new(Ok(400), Some(300), None));
    let state = table.get("u");
    assert_eq!(state.request_count, 0, "Neutral does not count requests");
    assert_eq!(
        state.ewma_score, 1.0,
        "Neutral does not change health score"
    );
    // Latency is independent of classification: the duration of this 400 request must still
    // be recorded.
    assert_eq!(state.latency_ewma_ms, 300.0);
}

#[test]
fn cooldown_escalates_with_streak_and_stays_under_max() {
    let (table, now) = clock_table(HealthSetting::default());
    // Cooldown has no threshold: one failure = one tier up. The first round has streak=0 →
    // exactly base 300ms.
    table.record(
        "u",
        AttemptReport::new(Err(FailureClass::Retryable), None, None),
    );
    let mut last = table.get("u").cooldown_until_ms - now.load(Ordering::Relaxed);
    assert_eq!(last, 300, "First cooldown should be exactly base 300ms");
    // The previous step's increment, used to verify the step growth implied by power-law
    // convexity.
    let mut prev_step = 0u64;

    // Verify tier by tier: increasing and never above the max ceiling. N=12, so the 12th
    // tier lands exactly on the cap.
    for round in 1..=12u32 {
        let expiry = table.get("u").cooldown_until_ms;
        now.store(expiry + 1, Ordering::Relaxed);
        table.record(
            "u",
            AttemptReport::new(Err(FailureClass::Retryable), None, None),
        );
        let new_expiry = table.get("u").cooldown_until_ms;
        let current = now.load(Ordering::Relaxed);
        assert!(
            new_expiry > current,
            "Tier {round} should really enter a new cooldown (otherwise the duration assertion is meaningless)"
        );
        let duration = new_expiry - current;
        assert!(
            duration >= last,
            "Tier {round} cooldown should not be shorter than the previous tier: {duration} < {last}"
        );
        // Power law p=2 is strictly convex → step sizes grow (slow, then fast). The first
        // tier has no previous step to compare against; once the ceiling is hit the step is
        // 0 and growth is no longer required.
        if duration < 8_000 && round > 1 {
            assert!(
                duration - last > last - prev_step,
                "Before hitting the ceiling, duration must increase (power-law convexity): step {} -> {}",
                last - prev_step,
                duration - last
            );
        }
        assert!(
            duration <= 8_000,
            "Tier {round} must not exceed max ceiling, measured {duration}ms"
        );
        prev_step = last;
        last = duration;
    }
    assert_eq!(last, 8_000, "Tier N should exactly touch max ceiling");
    assert!(
        table.get("u").cooldown_streak >= 5,
        "streak continues accumulating for annealing use"
    );
}

#[test]
fn unauthorized_run_cooldown_takes_max_tier_regardless_of_streak() {
    // A 401-run is outcome tiering: invalid credentials are a persistent state, so it takes
    // the max tier regardless of streak.
    let (table, now) = clock_table(HealthSetting::default());
    for _ in 0..UNAUTHORIZED_ESCALATION_THRESHOLD {
        table.record("u", AttemptReport::new(Ok(401), None, None));
    }
    for _ in 0..5 {
        table.record("u", AttemptReport::new(Ok(401), None, None));
    }
    let duration = table.get("u").cooldown_until_ms - now.load(Ordering::Relaxed);
    assert_eq!(duration, 8_000, "401-run should directly take max tier");
}

#[test]
fn throttled_cooldown_climbs_independent_ladder_to_cap() {
    let (table, now) = clock_table(HealthSetting::default());
    // 429 follows its own ladder 343→475→693→1000ms and is then clamped by throttle_max_ms
    // (defaults base=300, throttle_max=1000, N=4 power law, truncated).
    for expect in [343_u64, 475, 693, 1_000, 1_000] {
        table.record("u", AttemptReport::new(Ok(429), None, None));
        assert_eq!(
            table.get("u").cooldown_until_ms - now.load(Ordering::Relaxed),
            expect,
            "Consecutive 429s should climb the independent ladder step by step and cap at throttle_max_ms"
        );
    }
}

#[test]
fn custom_config_scales_the_whole_ladder() {
    // base=2000ms / max=4000ms (keeping the default N=12, p=2): ladder 2000 → 2001 → 2006 →
    // 2018 → 2043 → 2086 → 2152 → 2247 → 2386 → 2574 → 2822 → 3144 → 4000ms.
    // Same shape as the default: the power law crawls toward the custom max=4000ms, the Nth
    // tier lands exactly on the cap without overshooting.
    let setting = HealthSetting {
        cooldown_base_ms: 2_000,
        cooldown_max_ms: 4_000,
        ..HealthSetting::default()
    };
    let (table, now) = clock_table(setting);

    // Cooldown has no threshold: the first failure enters cooldown, and the streak is 0 at
    // that moment → first tier base.
    table.record(
        "u",
        AttemptReport::new(Err(FailureClass::Retryable), None, None),
    );
    assert_eq!(
        table.get("u").cooldown_until_ms - now.load(Ordering::Relaxed),
        2_000,
        "First tier takes base"
    );

    // Each cooldown expiry plus one new failure = one tier up; running all N=12 tiers should
    // converge exactly on the custom cap.
    for _ in 0..12 {
        let expiry = table.get("u").cooldown_until_ms;
        now.store(expiry + 1, Ordering::Relaxed);
        table.record(
            "u",
            AttemptReport::new(Err(FailureClass::Retryable), None, None),
        );
    }
    let last = table.get("u").cooldown_until_ms - now.load(Ordering::Relaxed);
    assert_eq!(
        last, 4_000,
        "Tier N should converge to custom max=4000ms without exceeding, measured {last}ms"
    );
}
