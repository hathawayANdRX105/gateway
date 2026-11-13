//! Behavioral tests for the dispatch health table / global P2C selection / failover budget.

use gateway_core::records::{
    ChannelKey, ChannelRecord, ChannelSettings, RouteUnitRecord, SyncMeta,
};
use gateway_dispatch::Dispatch;
use gateway_dispatch::candidate::resolve_candidate;
use gateway_dispatch::health::{AttemptReport, FailureClass, HealthTable, MemoryHealthTable};
use gateway_dispatch::retry::{
    AttemptOutcome, Failover, FakeTimeControl, RetryPolicy, run_retry_loop,
};
use gateway_dispatch::selector::{DEFAULT_EXPLORE_RATE, P2CSelector, Selector};
use gateway_gate::concurrency::ConcurrencyState;
use rand::SeedableRng;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

fn unit(group: &str, model: &str, key: &str, weight: u32, status: u8) -> RouteUnitRecord {
    RouteUnitRecord {
        meta: SyncMeta {
            key: key.to_string(),
            schema_version: 1,
            logical_version: 1,
            origin: "test".to_string(),
            updated_at: chrono::Utc::now(),
        },
        group: group.to_string(),
        public_model: model.to_string(),
        channel_key: key.to_string(),
        key_index: 0,
        upstream_model: model.to_string(),
        weight,
        status,
    }
}

fn channel(key: &str, secret: &str, base_url: &str) -> ChannelRecord {
    ChannelRecord {
        meta: SyncMeta {
            key: key.to_string(),
            schema_version: 1,
            logical_version: 1,
            origin: "test".to_string(),
            updated_at: chrono::Utc::now(),
        },
        name: key.to_string(),
        provider_type: "openai".to_string(),
        base_url: base_url.to_string(),
        keys: vec![ChannelKey {
            index: 0,
            secret: secret.to_string(),
            rpm_limit: 0,
        }],
        max_concurrency: 10,
        status: 1,
        groups: vec!["default".to_string()],
        settings: ChannelSettings::default(),
    }
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().unwrap()
}

fn assert_pick(
    units: &[RouteUnitRecord],
    health: &dyn HealthTable,
    exclude: &[String],
) -> Option<RouteUnitRecord> {
    let refs: Vec<&RouteUnitRecord> = units.iter().collect();
    let mut rng = rand::rngs::StdRng::seed_from_u64(42);
    P2CSelector::new(
        Arc::new(ConcurrencyState::default()),
        2,
        DEFAULT_EXPLORE_RATE,
    )
    .pick(&refs, health, exclude, None, 0, &mut rng)
    .cloned()
}

// ---------- health (phase0 track_health.go model) ----------

/// Fixed-clock table + default config (cooldown base 300ms / max 8s, power-law N=12).
fn clock_table(now: &Arc<AtomicU64>) -> MemoryHealthTable {
    let clock = {
        let now = now.clone();
        move || now.load(Ordering::Relaxed)
    };
    MemoryHealthTable::with_config_and_clock(
        gateway_dispatch::health::HealthSetting::default(),
        clock,
    )
}

#[test]
fn fatal_streak_trips_cooldown_and_recovers_with_ramp() {
    let now = Arc::new(AtomicU64::new(1_000_000));
    let table = clock_table(&now);

    // No threshold on cooldown: one fatal-class failure enters it at the base rung of 300ms
    // (cooldown_streak 0→1).
    table.record(
        "u1",
        AttemptReport::new(Err(FailureClass::Retryable), None, None),
    );
    let st = table.get("u1");
    assert_eq!(st.cooldown_streak, 1);
    assert_eq!(
        st.cooldown_until_ms - 1_000_000,
        300,
        "First cooldown should be base 300ms",
    );
    assert!(!table.is_selectable("u1", now.load(Ordering::Relaxed)));

    // A second failure inside the cooldown window is **not a no-op**; it immediately climbs to the
    // next rung. Under the old semantics the threshold was not met and this failure was swallowed;
    // with a threshold-free cooldown, sustained failure keeps adding penalty — exactly the behavior
    // needed for "a bad channel is left alone for longer and longer".
    now.store(1_000_001, Ordering::Relaxed); // still inside the first 300ms window
    table.record(
        "u1",
        AttemptReport::new(Err(FailureClass::Retryable), None, None),
    );
    let st = table.get("u1");
    assert_eq!(
        st.cooldown_streak, 2,
        "Failure during cooldown continues up one level"
    );
    assert_eq!(
        st.cooldown_until_ms - 1_000_001,
        353,
        "Second rung = power law rung 1 = 353ms",
    );

    // Cooldown ends → selectable again and enters the slow-start ramp (ramp_pending armed,
    // request_count=0). Ramp weight calculation has moved out of the health table (the selector
    // consumes `weight × slow_start_factor`): this table exposes only ramp state fields + the pure
    // function [`HealthState::slow_start_factor`].
    let expiry = table.get("u1").cooldown_until_ms;
    now.store(expiry + 1, Ordering::Relaxed);
    assert!(table.is_selectable("u1", expiry + 1));
    let st = table.get("u1");
    assert!(st.ramp_pending, "Ramp should be armed after cooldown ends");
    assert_eq!(st.request_count, 0, "Ramp has no successful record yet");
    // ramp_pending floor = 1/min_requests (min_requests comes from table config):
    let min_requests = table.setting().min_requests;
    let factor = st.slow_start_factor(min_requests);
    assert!(
        (factor - 1.0 / f64::from(min_requests)).abs() < 1e-9,
        "Ramp floor factor = 1/min_requests: got {factor}",
    );

    // One success → ramp advances: request_count=1 → factor = 1/5
    table.record("u1", AttemptReport::new(Ok(200), None, None));
    let st = table.get("u1");
    assert!(!st.ramp_pending, "Success should exit ramp_pending");
    assert_eq!(st.request_count, 1);
    // request_count < min_requests → gradual interval count/min_requests (same value as the floor
    // at 1/5; the difference becomes observable only at request_count=2 → 2/5).
    let factor = st.slow_start_factor(min_requests);
    assert!(
        (factor - 1.0 / f64::from(min_requests)).abs() < 1e-9,
        "Ramp 1/5 factor: got {factor}",
    );
}

#[test]
fn outcome_classification_matches_phase0() {
    let table = MemoryHealthTable::new();

    // 2xx → Success: ewma rises
    for _ in 0..6 {
        table.record("u1", AttemptReport::new(Ok(200), None, None));
    }
    assert_eq!(
        table.get("u1").ewma_score,
        1.0,
        "All success → full health score"
    );

    // 429 → Throttled: obs=0.7, light down-weight, not fatal
    let table = MemoryHealthTable::new();
    for _ in 0..6 {
        table.record("u2", AttemptReport::new(Ok(200), None, None));
    }
    // note: 429 is Ok(status), so it is classified by status code
    table.record("u2", AttemptReport::new(Ok(429), None, None));
    let st = table.get("u2");
    assert!(
        (st.ewma_score - (0.3 * 0.7 + 0.7 * 1.0)).abs() < 1e-9,
        "429 → obs 0.7 mixed: got {}",
        st.ewma_score
    );

    // 400 → Neutral: no score change, no request count
    let table = MemoryHealthTable::new();
    table.record("u3", AttemptReport::new(Ok(400), None, None));
    let st = table.get("u3");
    assert_eq!(st.request_count, 0, "Neutral does not count requests");
    assert_eq!(st.ewma_score, 1.0, "Neutral does not change score");
}

#[test]
fn unauthorized_escalates_after_three_401() {
    let table = MemoryHealthTable::new();
    // Isolated 401 → Neutral (do not penalize the channel)
    table.record("u1", AttemptReport::new(Ok(401), None, None));
    assert_eq!(table.get("u1").request_count, 0);
    // Three consecutive 401s → Fatal
    for _ in 0..2 {
        table.record("u1", AttemptReport::new(Ok(401), None, None));
    }
    let st = table.get("u1");
    assert_eq!(st.unauthorized_run, 3);
    // Escalation to Fatal enters the max-tier cooldown immediately (start_cooldown resets
    // ramp_exited to false).
    assert!(
        st.cooldown_until_ms > 0,
        "Escalating to Fatal should immediately enter cooldown"
    );
}

#[test]
fn cooldown_escalates_with_streak() {
    let now = Arc::new(AtomicU64::new(1_000_000));
    let table = clock_table(&now);

    // No cooldown threshold: one failure = one rung. The power law (n/N)^2 makes steps grow
    // (slow first, then fast).
    table.record(
        "u1",
        AttemptReport::new(Err(FailureClass::Retryable), None, None),
    );
    let first = table.get("u1").cooldown_until_ms;
    assert_eq!(
        first - 1_000_000,
        300,
        "First cooldown should be base 300ms"
    );

    // rung 1 = 300 + 7700·(1/12)² = 353ms
    now.store(first + 1, Ordering::Relaxed);
    table.record(
        "u1",
        AttemptReport::new(Err(FailureClass::Retryable), None, None),
    );
    let second = table.get("u1").cooldown_until_ms;
    let second_dur = second - (first + 1);
    assert_eq!(
        second_dur, 353,
        "Second cooldown should be about 353ms, measured {second_dur}ms"
    );

    // rung 2 = 300 + 7700·(2/12)² = 513ms
    now.store(second + 1, Ordering::Relaxed);
    table.record(
        "u1",
        AttemptReport::new(Err(FailureClass::Retryable), None, None),
    );
    let third = table.get("u1").cooldown_until_ms;
    let third_dur = third - (second + 1);
    assert_eq!(
        third_dur, 513,
        "Third cooldown should be about 513ms, measured {third_dur}ms"
    );

    // Steps must increase (convexity with p=2 = slow first, then fast)
    assert!(
        second_dur - 300 < third_dur - second_dur,
        "Slow-start requires increasing steps: first jump {}ms second jump {}ms",
        second_dur - 300,
        third_dur - second_dur
    );
    assert_eq!(
        table.get("u1").cooldown_streak,
        3,
        "streak still continues accumulating"
    );
}
#[test]
fn ewma_score_floor_and_ramp_exit() {
    // min_requests=0 makes EWMA update on every failure. Cooldown is now threshold-free and
    // triggers on every failure, but it clears request_count rather than ewma_score — with a zero
    // threshold that reset does not affect the next update's eligibility, so all-fatal failures
    // can still drive the score down to the MinScore (0.05) floor.
    let cfg = gateway_dispatch::health::HealthSetting {
        min_requests: 0,
        ..Default::default()
    };
    let now = Arc::new(AtomicU64::new(0));
    let clock = {
        let now = now.clone();
        move || now.load(Ordering::Relaxed)
    };
    let table = MemoryHealthTable::with_config_and_clock(cfg, clock);
    for _ in 0..30 {
        table.record(
            "u1",
            AttemptReport::new(Err(FailureClass::Retryable), None, None),
        );
    }
    let st = table.get("u1");
    assert!(
        st.ewma_score >= 0.05 - 1e-9,
        "Score should not be lower than MinScore: {}",
        st.ewma_score
    );
    // ramp_exited is true only when a failure was recorded but no cooldown was entered. Cooldown
    // is threshold-free, so start_cooldown immediately resets it to false (cooling itself exits
    // the ramp); therefore this asserts **cooldown is active**, not ramp_exited. Note the table's
    // clock starts at 0, so the comparison baseline is 0 rather than 1_000_000.
    assert!(
        st.cooldown_until_ms > 0,
        "Fatal should immediately enter cooldown: cooldown_until_ms={}",
        st.cooldown_until_ms
    );
}

#[test]
fn cooling_gates_routing_weight() {
    // routing_weight has been removed from the health table: cooling candidates are excluded by
    // is_selectable / selector; this table only exposes cooldown state fields.
    let now = Arc::new(AtomicU64::new(1_000_000));
    let table = clock_table(&now);
    for _ in 0..5 {
        table.record(
            "u1",
            AttemptReport::new(Err(FailureClass::Retryable), None, None),
        );
    }
    let t = now.load(Ordering::Relaxed);
    let st = table.get("u1");
    assert!(
        st.cooldown_until_ms > t,
        "Still within cooldown window after 5 consecutive hits"
    );
    assert!(
        !table.is_selectable("u1", t),
        "Not selectable during cooldown"
    );
    assert!(
        !st.ramp_pending,
        "Ramp only armed when cooldown ends (finish_cooldown); ramp should not be suspended during cooldown",
    );
}

#[test]
fn retry_loop_succeeds_after_one_failover() {
    // First attempt Retryable → mark_tried → second attempt hits another candidate → Done.
    let mut attempts = 0u32;
    let result = rt().block_on(run_retry_loop(
        "g",
        "m",
        &RetryPolicy {
            max_attempts: 3,
            total_budget_ms: 0,
        },
        None,
        None,
        |_: &str, _: &str, exclude: &[String], _: Option<&str>, _: Option<u64>| {
            let key = if exclude.is_empty() { "ch1" } else { "ch2" };
            let mut u = unit("g", "m", key, 10, 1);
            u.channel_key = key.to_string();
            Ok(gateway_dispatch::candidate::Candidate {
                unit: u,
                secret: "s".into(),
                base_url: "https://u".into(),
                upstream_model: "m".into(),
                provider_type: "openai".into(),
                settings: ChannelSettings::default(),
            })
        },
        |_: &gateway_dispatch::candidate::Candidate| {
            attempts += 1;
            let n = attempts;
            async move {
                if n == 1 {
                    AttemptOutcome::Retryable(FailureClass::Retryable, None)
                } else {
                    AttemptOutcome::Done {
                        status: 200,
                        latency_ms: 1,
                    }
                }
            }
        },
        |_k, _report| {},
        &FakeTimeControl::default(),
    ));
    // The second attempt hits another candidate → Done; run_retry_loop returns
    // Ok((winning attempt, outcome)).
    let (attempt, outcome) =
        result.expect("After retryable failure, switching candidate should eventually succeed");
    assert!(
        matches!(
            outcome,
            AttemptOutcome::Done {
                status: 200,
                latency_ms: 1
            }
        ),
        "After switching candidate should successfully conclude, got: {outcome:?}",
    );
    assert_eq!(
        attempt.candidate.unit.meta.key, "ch2",
        "Winning context should carry the candidate selected on the second attempt (ch2)",
    );
    assert_eq!(attempt.attempt_no, 2, "The winner is the 2nd attempt");
    assert_eq!(
        attempts, 2,
        "After first retryable failure should switch candidate and retry"
    );
}

#[test]
fn retry_loop_exhausts_budget_with_retryable() {
    // All attempts fail retryably → budget exhausted → BudgetExhausted.
    let err = rt().block_on(run_retry_loop(
        "g",
        "m",
        &RetryPolicy {
            max_attempts: 2,
            total_budget_ms: 0,
        },
        None,
        None,
        |_: &str, _: &str, exclude: &[String], _: Option<&str>, _: Option<u64>| {
            let key = if exclude.is_empty() { "ch1" } else { "ch2" };
            let mut u = unit("g", "m", key, 10, 1);
            u.channel_key = key.to_string();
            Ok(gateway_dispatch::candidate::Candidate {
                unit: u,
                secret: "s".into(),
                base_url: "https://u".into(),
                upstream_model: "m".into(),
                provider_type: "openai".into(),
                settings: ChannelSettings::default(),
            })
        },
        |_: &gateway_dispatch::candidate::Candidate| async {
            AttemptOutcome::Retryable(FailureClass::Retryable, None)
        },
        |_k, _report| {},
        &FakeTimeControl::default(),
    ));
    assert!(matches!(
        err,
        Err(gateway_dispatch::DispatchError::BudgetExhausted { .. })
    ));
}

#[test]
fn retry_loop_fatal_stops_immediately() {
    // Fatal → no retry; one attempt returns immediately.
    let mut attempts = 0u32;
    let outcome = rt().block_on(run_retry_loop(
        "g",
        "m",
        &RetryPolicy {
            max_attempts: 3,
            total_budget_ms: 0,
        },
        None,
        None,
        |_: &str, _: &str, _: &[String], _: Option<&str>, _: Option<u64>| {
            let u = unit("g", "m", "ch1", 10, 1);
            Ok(gateway_dispatch::candidate::Candidate {
                unit: u,
                secret: "s".into(),
                base_url: "https://u".into(),
                upstream_model: "m".into(),
                provider_type: "openai".into(),
                settings: ChannelSettings::default(),
            })
        },
        |_: &gateway_dispatch::candidate::Candidate| {
            attempts += 1;
            async move { AttemptOutcome::Fatal(FailureClass::Fatal, None) }
        },
        |_k, _report| {},
        &FakeTimeControl::default(),
    ));
    assert_eq!(attempts, 1, "Fatal should not be retried");
    // A Fatal termination also counts as "the loop ended successfully": return the context of the
    // terminating attempt.
    let (attempt, outcome) = outcome.expect("Fatal should be returned as Ok((Attempt, outcome))");
    assert!(matches!(outcome, AttemptOutcome::Fatal(_, _)));
    assert_eq!(
        attempt.candidate.unit.meta.key, "ch1",
        "When Fatal terminates, the terminating candidate should also be returned",
    );
    assert_eq!(attempt.attempt_no, 1);
}

#[test]
fn retry_loop_select_error_propagates() {
    // select returns an error (for example ModelUnknown) → the loop propagates it immediately and
    // makes no attempt.
    let err = rt().block_on(run_retry_loop(
        "g",
        "m",
        &RetryPolicy {
            max_attempts: 3,
            total_budget_ms: 0,
        },
        None,
        None,
        |group: &str, model: &str, _: &[String], _: Option<&str>, _: Option<u64>| {
            Err(gateway_dispatch::DispatchError::ModelUnknown {
                group: group.into(),
                model: model.into(),
            })
        },
        |_: &gateway_dispatch::candidate::Candidate| async {
            AttemptOutcome::Done {
                status: 200,
                latency_ms: 1,
            }
        },
        |_k, _report| {},
        &FakeTimeControl::default(),
    ));
    assert!(matches!(
        err,
        Err(gateway_dispatch::DispatchError::ModelUnknown { .. })
    ));
}

// ---------- selector ----------

#[test]
fn global_pool_weight_dominates() {
    // Global duel pool: expected baseline share = w/Σw. High-weight "low" (w=100) must take an
    // overwhelming majority over low-weight "high" (w=1).
    let units = [unit("g", "m", "low", 100, 1), unit("g", "m", "high", 1, 1)];
    let health = MemoryHealthTable::new();
    let selector = P2CSelector::new(
        Arc::new(ConcurrencyState::default()),
        2,
        DEFAULT_EXPLORE_RATE,
    );
    let total = 12 * 300;
    let mut low_picks = 0usize;
    for seed in 1u64..=12 {
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        for _ in 0..300 {
            let refs: Vec<&RouteUnitRecord> = units.iter().collect();
            let picked = selector
                .pick(&refs, &health, &[], None, 0, &mut rng)
                .unwrap();
            if picked.meta.key == "low" {
                low_picks += 1;
            }
        }
    }
    // `low_picks ≤ 12 × 300 = 3 600` and `total` is the same fixed test scale,
    // so `low_picks as f64` is exact (well below f64's 2^53 mantissa); it only
    // computes a measured share.
    #[allow(clippy::cast_precision_loss)]
    let share = low_picks as f64 / f64::from(total);
    assert!(
        share > 0.9,
        "High-weight candidate should get dominant share in global pool (w/Σw≈0.99), measured {share:.3}",
    );
}

#[test]
fn floored_candidate_keeps_explore_traffic() {
    // A channel whose likelihood hits the floor can never win a normal duel (baseline traffic 0);
    // explore_rate reroutes to it uniformly with a small probability, preserving state observation
    // and the self-healing path.
    let units = [
        unit("g", "m", "light", 10, 1),
        unit("g", "m", "heavy", 10, 1),
    ];
    let health = MemoryHealthTable::new();
    // light succeeds every time but has very high latency (triggering the likelihood floor); heavy
    // is fast and healthy.
    health.record("light", AttemptReport::new(Ok(200), Some(9_000), None));
    health.record("heavy", AttemptReport::new(Ok(200), Some(10), None));
    let selector = P2CSelector::new(
        Arc::new(ConcurrencyState::default()),
        2,
        DEFAULT_EXPLORE_RATE,
    );
    let mut light_picks = 0usize;
    let total = 12 * 300;
    for seed in 1u64..=12 {
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        for _ in 0..300 {
            let refs: Vec<&RouteUnitRecord> = units.iter().collect();
            let picked = selector
                .pick(&refs, &health, &[], None, 0, &mut rng)
                .unwrap();
            if picked.meta.key == "light" {
                light_picks += 1;
            }
        }
    }
    assert!(
        light_picks > 0,
        "Channel whose likelihood drops to floor with positive prior must still retain exploration traffic",
    );
    // `light_picks ≤ 12 × 300 = 3 600` and `total` is the same fixed test scale,
    // so `light_picks as f64` is exact (well below f64's 2^53 mantissa); it only
    // computes a measured share.
    #[allow(clippy::cast_precision_loss)]
    let ratio = light_picks as f64 / f64::from(total);
    assert!(
        ratio < 0.5,
        "Downgraded channel share should be significantly lower: {ratio:.3} ({light_picks}/{total})",
    );
}

#[test]
fn weighted_distribution_matches_configured_ratio() {
    // share = sampling probability × win probability. A fixed seed injection makes this
    // deterministic; 8 seeds × 2000 draws = 16000 samples. Pin only observable properties, not
    // exact percentages.
    let units = [
        unit("g", "m", "light", 10, 1),
        unit("g", "m", "heavy", 30, 1),
    ];
    // When both candidates have equal likelihood, the posterior is determined strictly by the
    // prior, and the heavy one wins every duel → light gets 0 traffic → unobservable. So give light
    // a slight health-score disadvantage, letting it still obtain a non-zero share through the
    // sampling probability.
    //
    // Cooldown is now threshold-free, so any failure immediately removes light — what gets measured
    // then is "unselectable", not "low share". So after every failure push the clock past the
    // cooldown window so it is selectable again on the next pick. min_requests=0 has two effects:
    // EWMA updates on every failure (otherwise request_count is cleared by cooldown and the
    // threshold is never reached), and the ramp factor degenerates to 1.0 — leaving only ewma_score
    // to distinguish the two candidates, exactly the quantity this case measures.
    let now = Arc::new(AtomicU64::new(1_000_000));
    let clock = {
        let now = now.clone();
        move || now.load(Ordering::Relaxed)
    };
    let health = MemoryHealthTable::with_config_and_clock(
        gateway_dispatch::health::HealthSetting {
            min_requests: 0,
            ..gateway_dispatch::health::HealthSetting::default()
        },
        clock,
    );
    for _ in 0..20 {
        health.record(
            "light",
            AttemptReport::new(Err(FailureClass::Retryable), None, None),
        );
        let expiry = health.get("light").cooldown_until_ms;
        now.store(expiry + 1, Ordering::Relaxed);
        health.record("heavy", AttemptReport::new(Ok(200), None, None));
    }
    let selector = P2CSelector::new(
        Arc::new(ConcurrencyState::default()),
        2,
        DEFAULT_EXPLORE_RATE,
    );
    let n = 2000usize;
    let (mut light, mut heavy) = (0usize, 0usize);
    for seed in 1u64..=8 {
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        for _ in 0..n {
            let refs: Vec<&RouteUnitRecord> = units.iter().collect();
            let picked = selector
                .pick(
                    &refs,
                    &health,
                    &[],
                    None,
                    now.load(Ordering::Relaxed),
                    &mut rng,
                )
                .unwrap();
            if picked.meta.key == "heavy" {
                heavy += 1;
            } else {
                light += 1;
            }
        }
    }
    let total = n * 8;
    // `heavy` and `total = n * 8` are bounded by the test's fixed draw counts,
    // far below f64's 2^53 mantissa, so both `usize → f64` casts are exact;
    // this computes an observed heavy-channel ratio.
    #[allow(clippy::cast_precision_loss)]
    let heavy_ratio = heavy as f64 / total as f64;
    assert!(
        heavy > light,
        "Channel weight 30 share should be higher than weight 10: {heavy_ratio:.3} ({heavy}/{total})",
    );
    assert!(
        light > 0,
        "Low-weight channel must retain non-zero traffic to be observed: {light}/{total}",
    );
    assert!(
        heavy_ratio < 1.0,
        "Low-weight channel should not be completely squeezed out: {heavy_ratio:.3}",
    );
}

#[test]
fn exclude_and_cooldown_remove_candidates() {
    let units = vec![unit("g", "m", "a", 10, 1), unit("g", "m", "b", 10, 1)];
    let health = MemoryHealthTable::new();
    // exclude a → always pick b
    for _ in 0..5 {
        assert_eq!(
            assert_pick(&units, &health, &["a".to_string()])
                .unwrap()
                .meta
                .key,
            "b"
        );
    }
    // Cool both a and b → no selectable candidate
    let now = Arc::new(AtomicU64::new(0));
    let clock2 = {
        let now = now.clone();
        move || now.load(Ordering::Relaxed)
    };
    let health2 = MemoryHealthTable::with_config_and_clock(
        gateway_dispatch::health::HealthSetting::default(),
        clock2,
    );
    for _ in 0..5 {
        health2.record(
            "a",
            AttemptReport::new(Err(FailureClass::Retryable), None, None),
        );
        health2.record(
            "b",
            AttemptReport::new(Err(FailureClass::Retryable), None, None),
        );
    }
    // All candidates cooling → urgently recall the one with the shortest remaining cooldown (a and
    // b failed at the same clock instant, so their cooldown deadlines are equal; take the first in
    // units order = a). Recall preserves the streak: the next failure still climbs from the
    // existing rung, so this is not forgiveness.
    let recalled = assert_pick(&units, &health2, &[])
        .expect("All cooldown → recall earliest-expiring channel");
    assert_eq!(
        recalled.meta.key, "a",
        "Same remaining cooldown, take first in order"
    );
    assert!(
        health2.is_selectable("a", 0),
        "After recall channel immediately selectable (slow-start ramp armed)",
    );
    let st = health2.get("a");
    assert_eq!(
        st.cooldown_streak, 5,
        "Recall doesn't clear streak: 5 failures accumulated to rung 5, recall doesn't forgive",
    );
    assert!(st.ramp_pending, "Recall arms slow-start ramp");
}

#[test]
fn cooled_unit_is_filtered_globally() {
    // Cooldown gating is a global hard gate: cooling candidates are filtered out directly, and the
    // remaining candidates compete in the global pool — there is no concept of "layer fallthrough".
    let now = Arc::new(AtomicU64::new(0));
    let clock = {
        let now = now.clone();
        move || now.load(Ordering::Relaxed)
    };
    let health = MemoryHealthTable::with_config_and_clock(
        gateway_dispatch::health::HealthSetting::default(),
        clock,
    );
    let units = [unit("g", "m", "top", 100, 1), unit("g", "m", "low", 100, 1)];
    for _ in 0..5 {
        health.record(
            "top",
            AttemptReport::new(Err(FailureClass::Retryable), None, None),
        );
    }
    // top's cooldown starts at 0; five consecutive failures at the same clock instant push its
    // deadline to 1155ms, so now=500 is still inside the window.
    now.store(500, Ordering::Relaxed);
    assert!(
        !health.is_selectable("top", 500),
        "top should be in cooldown"
    );
    assert!(health.is_selectable("low", 500), "low should be available");
    // top is blocked by the global cooldown gate → only low remains in the pool
    assert_eq!(assert_pick(&units, &health, &[]).unwrap().meta.key, "low");
}

// ---------- retry ----------

#[test]
fn failover_budget_and_tried_set() {
    let mut f = Failover::new(
        RetryPolicy {
            max_attempts: 3,
            total_budget_ms: 0,
        },
        None,
    );
    assert_eq!(f.next_attempt(), Some(1));
    assert_eq!(f.next_attempt(), Some(2));
    assert_eq!(f.next_attempt(), Some(3));
    assert_eq!(f.next_attempt(), None, "Budget exhausted");
    assert!(f.is_exhausted());

    let mut f = Failover::new(RetryPolicy::default(), None);
    assert_eq!(f.next_attempt(), Some(1));
    f.mark_tried("k1");
    f.mark_tried("k1"); // deduplicate
    assert_eq!(f.exclude(), &["k1".to_string()]);
}

// ---------- candidate ----------

#[test]
fn resolve_candidate_picks_key_by_index() {
    let u = unit("g", "m", "ch1", 10, 1);
    let ch = channel("ch1", "sk-secret", "https://upstream.example");
    let c = resolve_candidate(&u, &ch).unwrap();
    assert_eq!(c.secret, "sk-secret");
    assert_eq!(c.base_url, "https://upstream.example");
    assert_eq!(c.upstream_model, "m");

    // key_index out of range → None
    let mut u2 = u.clone();
    u2.key_index = 7;
    assert!(resolve_candidate(&u2, &ch).is_none());
}

// ---------- dispatcher ----------

#[test]
fn dispatcher_select_resolves_full_candidate() {
    let units = vec![unit("g", "m", "ch1", 10, 1)];
    let mut channels: HashMap<String, ChannelRecord> = HashMap::new();
    channels.insert(
        "ch1".to_string(),
        channel("ch1", "sk-secret", "https://upstream.example"),
    );
    let snap = Arc::new(gateway_dispatch::Snapshot { units, channels });
    let dispatcher = gateway_dispatch::Dispatcher::new(
        Some(snap),
        Arc::new(MemoryHealthTable::new()),
        Arc::new(ConcurrencyState::default()),
    );
    let c = dispatcher.select("g", "m", &[], None, None).unwrap();
    assert_eq!(c.secret, "sk-secret");
    assert_eq!(c.base_url, "https://upstream.example");
}

#[test]
fn failover_end_to_end_switches_candidate() {
    // End to end: select A → failure → mark_tried(A) → next select excludes A and switches to B.
    let units = vec![unit("g", "m", "ch1", 10, 1), unit("g", "m", "ch2", 10, 1)];
    let mut channels: HashMap<String, ChannelRecord> = HashMap::new();
    channels.insert("ch1".to_string(), channel("ch1", "s1", "https://u1"));
    channels.insert("ch2".to_string(), channel("ch2", "s2", "https://u2"));
    let snap = Arc::new(gateway_dispatch::Snapshot { units, channels });
    let dispatcher = gateway_dispatch::Dispatcher::new(
        Some(snap),
        Arc::new(MemoryHealthTable::new()),
        Arc::new(ConcurrencyState::default()),
    );

    let mut failover = gateway_dispatch::retry::Failover::new(
        gateway_dispatch::retry::RetryPolicy::default(),
        None,
    );
    assert_eq!(failover.next_attempt(), Some(1));
    let first = dispatcher
        .select("g", "m", failover.exclude(), None, None)
        .unwrap();
    failover.mark_tried(first.unit.meta.key.clone());

    assert_eq!(failover.next_attempt(), Some(2));
    let second = dispatcher
        .select("g", "m", failover.exclude(), None, None)
        .unwrap();
    assert_ne!(
        second.unit.meta.key, first.unit.meta.key,
        "After excluding tried candidates must switch channel",
    );
    assert!(failover.exclude().contains(&first.unit.meta.key));
}

#[test]
fn single_candidate_is_selected() {
    // Single candidate: the only candidate in the pool is selected directly (new-api
    // selectByWeight semantics for a single candidate).
    let units = vec![unit("g", "m", "only", 0, 1)];
    let mut channels: HashMap<String, ChannelRecord> = HashMap::new();
    channels.insert("only".to_string(), channel("only", "s1", "https://u1"));
    let snap = Arc::new(gateway_dispatch::Snapshot { units, channels });
    let dispatcher = gateway_dispatch::Dispatcher::new(
        Some(snap),
        Arc::new(MemoryHealthTable::new()),
        Arc::new(ConcurrencyState::default()),
    );
    let c = dispatcher.select("g", "m", &[], None, None).unwrap();
    assert_eq!(c.unit.meta.key, "only");
}

#[test]
fn dispatcher_missing_key_index_returns_no_candidate() {
    let mut u = unit("g", "m", "ch1", 10, 1);
    u.key_index = 5; // channel only has the key at index=0
    let mut channels: HashMap<String, ChannelRecord> = HashMap::new();
    channels.insert(
        "ch1".to_string(),
        channel("ch1", "sk-secret", "https://upstream.example"),
    );
    let snap = Arc::new(gateway_dispatch::Snapshot {
        units: vec![u],
        channels,
    });
    let dispatcher = gateway_dispatch::Dispatcher::new(
        Some(snap),
        Arc::new(MemoryHealthTable::new()),
        Arc::new(ConcurrencyState::default()),
    );
    assert!(matches!(
        dispatcher.select("g", "m", &[], None, None),
        Err(gateway_dispatch::DispatchError::AllUnavailable { .. })
    ));
}
// ---------- fallback key selection (issue #11(a)) ----------

// fallback_key_index = Some(1): no route unit for the model → the fallback channel's unit
// carries key_index 1 and select resolves the key at index 1 ("sk-one").
#[test]
fn fallback_uses_settings_key_index_when_some() {
    let mut channels: HashMap<String, ChannelRecord> = HashMap::new();
    channels.insert(
        "fb".to_string(),
        ChannelRecord {
            keys: vec![
                ChannelKey {
                    index: 0,
                    secret: "sk-zero".to_string(),
                    rpm_limit: 0,
                },
                ChannelKey {
                    index: 1,
                    secret: "sk-one".to_string(),
                    rpm_limit: 0,
                },
            ],
            settings: ChannelSettings {
                fallback: true,
                fallback_key_index: Some(1),
                ..ChannelSettings::default()
            },
            ..channel("fb", "unused", "https://upstream.example")
        },
    );
    let snap = Arc::new(gateway_dispatch::Snapshot {
        units: vec![],
        channels,
    });
    let dispatcher = gateway_dispatch::Dispatcher::new(
        Some(snap),
        Arc::new(MemoryHealthTable::new()),
        Arc::new(ConcurrencyState::default()),
    );
    let c = dispatcher.select("default", "m", &[], None, None).unwrap();
    assert_eq!(c.secret, "sk-one");
    assert_eq!(c.unit.key_index, 1);
}

// fallback_key_index = None: the fallback unit keeps key_index 0 (old behavior).
#[test]
fn fallback_defaults_to_key_index_0_when_none() {
    let mut channels: HashMap<String, ChannelRecord> = HashMap::new();
    channels.insert(
        "fb".to_string(),
        ChannelRecord {
            keys: vec![ChannelKey {
                index: 0,
                secret: "sk-zero".to_string(),
                rpm_limit: 0,
            }],
            settings: ChannelSettings {
                fallback: true,
                fallback_key_index: None,
                ..ChannelSettings::default()
            },
            ..channel("fb", "unused", "https://upstream.example")
        },
    );
    let snap = Arc::new(gateway_dispatch::Snapshot {
        units: vec![],
        channels,
    });
    let dispatcher = gateway_dispatch::Dispatcher::new(
        Some(snap),
        Arc::new(MemoryHealthTable::new()),
        Arc::new(ConcurrencyState::default()),
    );
    let c = dispatcher.select("default", "m", &[], None, None).unwrap();
    assert_eq!(c.secret, "sk-zero");
    assert_eq!(c.unit.key_index, 0);
}

// fallback_key_index = Some(5) with no key at that index: resolve_candidate finds no
// candidate and select fails closed with AllUnavailable (channel skipped).
#[test]
fn fallback_out_of_range_key_index_yields_no_candidate() {
    let mut channels: HashMap<String, ChannelRecord> = HashMap::new();
    channels.insert(
        "fb".to_string(),
        ChannelRecord {
            keys: vec![ChannelKey {
                index: 0,
                secret: "sk-zero".to_string(),
                rpm_limit: 0,
            }],
            settings: ChannelSettings {
                fallback: true,
                fallback_key_index: Some(5),
                ..ChannelSettings::default()
            },
            ..channel("fb", "unused", "https://upstream.example")
        },
    );
    let snap = Arc::new(gateway_dispatch::Snapshot {
        units: vec![],
        channels,
    });
    let dispatcher = gateway_dispatch::Dispatcher::new(
        Some(snap),
        Arc::new(MemoryHealthTable::new()),
        Arc::new(ConcurrencyState::default()),
    );
    assert!(matches!(
        dispatcher.select("default", "m", &[], None, None),
        Err(gateway_dispatch::DispatchError::AllUnavailable { .. })
    ));
}

#[test]
fn dispatcher_skips_disabled_channel() {
    // channel.status = 2 (manually disabled) → the unit is not schedulable (new-api channel
    // status gating)
    let mut ch = channel("ch1", "sk-secret", "https://upstream.example");
    ch.status = 2;
    let mut channels: HashMap<String, ChannelRecord> = HashMap::new();
    channels.insert("ch1".to_string(), ch);
    let snap = Arc::new(gateway_dispatch::Snapshot {
        units: vec![unit("g", "m", "ch1", 10, 1)],
        channels,
    });
    let dispatcher = gateway_dispatch::Dispatcher::new(
        Some(snap),
        Arc::new(MemoryHealthTable::new()),
        Arc::new(ConcurrencyState::default()),
    );
    assert!(matches!(
        dispatcher.select("g", "m", &[], None, None),
        Err(gateway_dispatch::DispatchError::AllUnavailable { .. })
    ));
}

#[test]
fn dispatcher_fails_closed_before_snapshot() {
    let dispatcher = gateway_dispatch::Dispatcher::new(
        None,
        Arc::new(MemoryHealthTable::new()),
        Arc::new(ConcurrencyState::default()),
    );
    assert!(matches!(
        dispatcher.select("g", "m", &[], None, None),
        Err(gateway_dispatch::DispatchError::SnapshotNotReady)
    ));
}

// ---------- dispatch stage channel attribution ----------

/// After selecting a route, `DispatchStage` must write channel attribution into ctx: `channel_key`
/// comes from unit, and `channel_name` is looked up from the Dispatcher snapshot. Usage persistence
/// uses these two fields for per-channel statistics — if the stage omits them, the pipeline
/// packages empty attribution and every channel column in reports is blank (the original bug this
/// PR fixes).
#[test]
fn dispatch_stage_fills_selected_channel_fields() {
    use gateway_dispatch::stage::DispatchStage;
    use gateway_pipeline::Stage;
    use gateway_pipeline::{BodySource, ProtocolKind, RequestMeta};

    let units = vec![unit("g", "m", "ch1", 10, 1)];
    let mut channels: HashMap<String, ChannelRecord> = HashMap::new();
    channels.insert("ch1".to_string(), channel("ch1", "s1", "https://u1"));
    let dispatcher = gateway_dispatch::Dispatcher::new(
        Some(Arc::new(gateway_dispatch::Snapshot { units, channels })),
        Arc::new(MemoryHealthTable::new()),
        Arc::new(ConcurrencyState::default()),
    );

    let ctx = &mut gateway_pipeline::RequestCtx::new(RequestMeta {
        method: "POST".to_string(),
        path: "/v1/chat/completions".to_string(),
        headers: http::HeaderMap::new(),
        body: BodySource::InMemory(bytes::Bytes::new()),
        client_ip: "127.0.0.1".parse().unwrap(),
        request_id: uuid::Uuid::now_v7(),
        inbound_protocol: ProtocolKind::OpenAI,
    });
    ctx.token = Some(gateway_pipeline::TokenInfo {
        id: "tok-1".into(),
        user_key: "user-test".to_string(),
        group: "g".into(),
        enabled: true,
        allowed_models: None,
        auth_version: 1,
    });
    ctx.requested_model = Some("m".into());

    let stage = DispatchStage::new(Arc::new(dispatcher));
    let outcome = rt().block_on(stage.handle(ctx)).unwrap();
    assert!(matches!(outcome, gateway_pipeline::StageOutcome::Continue));
    assert_eq!(
        ctx.selected_channel_key.as_deref(),
        Some("ch1"),
        "Attribution key = route.unit.channel_key",
    );
    // In the channel() helper name = key, so the lookup should return the same name
    assert_eq!(ctx.selected_channel_name.as_deref(), Some("ch1"));
}

/// `channel_name` returns None for a key outside the snapshot (the basis for degrading to
/// key-only semantics).
#[test]
fn dispatcher_channel_name_unknown_key_is_none() {
    let dispatcher = gateway_dispatch::Dispatcher::new(
        None,
        Arc::new(MemoryHealthTable::new()),
        Arc::new(ConcurrencyState::default()),
    );
    assert!(dispatcher.channel_name("nope").is_none());

    let units = vec![unit("g", "m", "ch1", 10, 1)];
    let mut channels: HashMap<String, ChannelRecord> = HashMap::new();
    channels.insert("ch1".to_string(), channel("ch1", "s1", "https://u1"));
    let dispatcher = gateway_dispatch::Dispatcher::new(
        Some(Arc::new(gateway_dispatch::Snapshot { units, channels })),
        Arc::new(MemoryHealthTable::new()),
        Arc::new(ConcurrencyState::default()),
    );
    assert!(dispatcher.channel_name("ch1").is_some());
    assert!(dispatcher.channel_name("nope").is_none());
}
