//! Observable behavior of the P2C selector: prior×likelihood, sampling floor, prior
//! fallback, starvation prevention, slow-start ramp discount.

use app_contract::records::{RouteUnitRecord, SyncMeta};
use gateway_dispatch::health::{
    AttemptReport, FailureClass, HealthSetting, HealthTable, MemoryHealthTable,
};
use gateway_dispatch::selector::{DEFAULT_EXPLORE_RATE, P2CSelector, Selector};
use gateway_gate::concurrency::ConcurrencyState;
use rand::SeedableRng;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

fn unit(key: &str, weight: u32) -> RouteUnitRecord {
    RouteUnitRecord {
        meta: SyncMeta {
            key: key.to_string(),
            schema_version: 1,
            logical_version: 1,
            origin: "test".to_string(),
            updated_at: chrono::Utc::now(),
        },
        group: "g".to_string(),
        public_model: "m".to_string(),
        channel_key: key.to_string(),
        key_index: 0,
        upstream_model: "m".to_string(),
        weight,
        status: 1,
    }
}

const SEEDS: u64 = 12;
const DRAWS_PER_SEED: usize = 300;

fn share_of(
    selector: &P2CSelector,
    units: &[RouteUnitRecord],
    health: &MemoryHealthTable,
    key: &str,
) -> f64 {
    let refs: Vec<&RouteUnitRecord> = units.iter().collect();
    let mut hits = 0usize;
    for seed in 1..=SEEDS {
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        for _ in 0..DRAWS_PER_SEED {
            let picked = selector
                .pick(&refs, health, &[], None, 0, &mut rng)
                .unwrap();
            if picked.meta.key == key {
                hits += 1;
            }
        }
    }
    hits as f64 / (DRAWS_PER_SEED * SEEDS as usize) as f64
}

fn never_picks(
    selector: &P2CSelector,
    units: &[RouteUnitRecord],
    health: &MemoryHealthTable,
    key: &str,
) {
    let refs: Vec<&RouteUnitRecord> = units.iter().collect();
    for seed in 1..=8u64 {
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        for _ in 0..200 {
            let picked = selector
                .pick(&refs, health, &[], None, 0, &mut rng)
                .unwrap();
            assert_ne!(picked.meta.key, key, "{key} must not be selected");
        }
    }
}

#[test]
fn two_unit_pool_ties_split_by_sampling_probability() {
    // A two-candidate pool has no "third opponent" to draw, so the opponent back-fill rule
    // guarantees every pick is a real duel. Under scheme C, equal latency means the
    // likelihoods tie and the decision reduces to **sampling order** (the first draw wins),
    // so traffic matches the prior ratio exactly: weight 10:30 → prior 10:30 → 30/40 = 0.75.
    // The old semantics (weight also deciding the duel) gave heavy 100% — that is exactly
    // "counting the same configuration twice".
    let units = [unit("light", 10), unit("heavy", 30)];
    let health = MemoryHealthTable::new();
    let selector = P2CSelector::new(
        Arc::new(ConcurrencyState::default()),
        2,
        DEFAULT_EXPLORE_RATE,
    );

    let heavy = share_of(&selector, &units, &health, "heavy");
    assert!(
        (0.70..=0.78).contains(&heavy),
        "at equal latency shares should follow prior 30/40=0.75, measured {heavy:.3}"
    );
}

#[test]
fn slow_channel_loses_despite_higher_weight() {
    let units = [unit("slow-heavy", 30), unit("fast-light", 10)];
    let health = MemoryHealthTable::new();
    health.record("slow-heavy", AttemptReport::new(Ok(200), Some(5_000), None));
    health.record("fast-light", AttemptReport::new(Ok(200), Some(50), None));
    let selector = P2CSelector::new(
        Arc::new(ConcurrencyState::default()),
        2,
        DEFAULT_EXPLORE_RATE,
    );

    let slow = share_of(&selector, &units, &health, "slow-heavy");
    assert!(
        slow < 0.5,
        "slow channel should lose to fast channel even with 3× static weight: measured {slow:.3}"
    );
}

#[test]
fn slow_channel_keeps_exploration_traffic_at_sampling_floor() {
    let units = [unit("a", 1), unit("b", 1), unit("c", 1), unit("slow", 1)];
    let health = MemoryHealthTable::new();
    // Healthy channels need latency data too: the pool's fastest latency is the relative
    // baseline; if only the degraded channel had data it would become its own baseline and
    // the likelihood would lose all discrimination.
    for key in ["a", "b", "c"] {
        health.record(key, AttemptReport::new(Ok(200), Some(50), None));
    }
    health.record("slow", AttemptReport::new(Ok(200), Some(10_000), None));
    let selector = P2CSelector::new(
        Arc::new(ConcurrencyState::default()),
        2,
        DEFAULT_EXPLORE_RATE,
    );

    let slow = share_of(&selector, &units, &health, "slow");
    assert!(
        slow > 0.0,
        "degraded channel with positive prior must retain exploration traffic, or never recover"
    );
    assert!(
        slow < 0.2,
        "degraded channel share should be well below healthy channels: measured {slow:.3}"
    );
}

#[test]
fn two_unit_pool_lets_latency_decide_when_weights_differ() {
    // Core behavior of scheme C: weight only drives sampling frequency, **the duel is
    // decided by live state**. light has the lower weight (10) but far better latency than
    // heavy (30, 5s), so heavy loses most duels and takes a share far below its prior of
    // 0.75. Under the old semantics heavy's higher weight would simply win the duel.
    let units = [unit("heavy", 30), unit("light", 10)];
    let health = MemoryHealthTable::new();
    for _ in 0..8 {
        health.record("heavy", AttemptReport::new(Ok(200), Some(5_000), None));
        health.record("light", AttemptReport::new(Ok(200), Some(50), None));
    }
    let selector = P2CSelector::new(
        Arc::new(ConcurrencyState::default()),
        2,
        DEFAULT_EXPLORE_RATE,
    );
    let heavy = share_of(&selector, &units, &health, "heavy");
    assert!(
        heavy < 0.30,
        "channel 100× slower with higher weight should cede most traffic: measured {heavy:.3}"
    );
}

#[test]
fn more_choices_sharpen_selection() {
    let units = [unit("fast", 1), unit("slow", 1), unit("mid", 1)];
    let health = MemoryHealthTable::new();
    for key in ["fast", "mid"] {
        health.record(key, AttemptReport::new(Ok(200), Some(50), None));
    }
    health.record("slow", AttemptReport::new(Ok(200), Some(10_000), None));

    let weak = P2CSelector::new(
        Arc::new(ConcurrencyState::default()),
        2,
        DEFAULT_EXPLORE_RATE,
    );
    let strong = P2CSelector::new(
        Arc::new(ConcurrencyState::default()),
        8,
        DEFAULT_EXPLORE_RATE,
    );

    let weak_share = share_of(&weak, &units, &health, "slow");
    let strong_share = share_of(&strong, &units, &health, "slow");
    assert!(
        strong_share < weak_share,
        "more choices should increase filtering strength: weak={weak_share:.3} strong={strong_share:.3}"
    );
}

#[test]
fn saturated_channel_loses_to_channel_with_free_slots() {
    let units = [unit("saturated", 30), unit("free", 10)];
    let health = MemoryHealthTable::new();
    let concurrency = Arc::new(ConcurrencyState::default());
    concurrency.register_channel("saturated", 1);
    concurrency.register_channel("free", 8);
    // Consume saturated's only slot: 0 available; free keeps 8.
    let held = concurrency.try_acquire("saturated").unwrap().unwrap();

    let selector = P2CSelector::new(concurrency.clone(), 2, DEFAULT_EXPLORE_RATE);
    let saturated = share_of(&selector, &units, &health, "saturated");
    assert!(
        saturated < 0.3,
        "full-slot channel should be picked less even with higher weight: measured {saturated:.3}"
    );
    drop(held);
}

#[test]
fn zero_weight_candidate_gets_only_explore_traffic() {
    // Scheme C: prior = w/Σw — a weight=0 candidate has zero sampling weight and a baseline
    // share of 0 (the old semantics, prior = weight+1, gave it 1/7 ≈ 0.14; that is gone).
    // The explore path falls back to uniform sampling (not prior-weighted, otherwise a zero
    // weight could never be explored), so its state stays refreshable and it can self-heal,
    // but its traffic must stay far below a normal allocation.
    let units = [unit("zero", 0), unit("real", 5)];
    let health = MemoryHealthTable::new();
    let selector = P2CSelector::new(
        Arc::new(ConcurrencyState::default()),
        2,
        DEFAULT_EXPLORE_RATE,
    );
    let zero = share_of(&selector, &units, &health, "zero");
    assert!(
        zero > 0.0,
        "weight=0 channel must retain explore observation traffic"
    );
    assert!(
        zero < 0.02,
        "weight=0 baseline share should be 0 (only explore ≈ ε=0.005 traffic), measured {zero:.3}"
    );
}

#[test]
fn zero_weight_fast_candidate_only_gets_exploration_traffic() {
    let units = [unit("slow", 5), unit("fast-zero", 0)];
    let health = MemoryHealthTable::new();
    health.record("slow", AttemptReport::new(Ok(200), Some(5_000), None));
    health.record("fast-zero", AttemptReport::new(Ok(200), Some(50), None));

    let no_explore = P2CSelector::new(Arc::new(ConcurrencyState::default()), 2, 0.0);
    never_picks(&no_explore, &units, &health, "fast-zero");

    let explore = P2CSelector::new(
        Arc::new(ConcurrencyState::default()),
        2,
        DEFAULT_EXPLORE_RATE,
    );
    let fast_share = share_of(&explore, &units, &health, "fast-zero");
    assert!(
        fast_share > 0.0 && fast_share < 0.02,
        "weight=0 fast channel can only get traffic via ε exploration: measured {fast_share:.3}"
    );
}

#[test]
fn all_zero_weight_pool_splits_evenly() {
    // Σw=0 falls back to a uniform prior: routing still works and both candidates get close
    // to half the traffic.
    let units = [unit("a", 0), unit("b", 0)];
    let health = MemoryHealthTable::new();
    let selector = P2CSelector::new(
        Arc::new(ConcurrencyState::default()),
        2,
        DEFAULT_EXPLORE_RATE,
    );
    let a_share = share_of(&selector, &units, &health, "a");
    assert!(
        (0.44..=0.56).contains(&a_share),
        "all-zero-weight pool must split evenly, measured a share {a_share:.3}"
    );
}
#[test]
fn single_candidate_pool_returns_that_candidate() {
    let units = [unit("only", 7)];
    let health = MemoryHealthTable::new();
    let selector = P2CSelector::new(
        Arc::new(ConcurrencyState::default()),
        2,
        DEFAULT_EXPLORE_RATE,
    );
    let refs: Vec<&RouteUnitRecord> = units.iter().collect();
    let mut rng = rand::rngs::StdRng::seed_from_u64(1);
    assert_eq!(
        selector
            .pick(&refs, &health, &[], None, 0, &mut rng)
            .unwrap()
            .meta
            .key,
        "only"
    );
}

#[test]
fn all_saturated_pool_still_returns_a_candidate() {
    let units = [unit("a", 1), unit("b", 1)];
    let health = MemoryHealthTable::new();
    let concurrency = Arc::new(ConcurrencyState::default());
    concurrency.register_channel("a", 1);
    concurrency.register_channel("b", 1);
    let held_a = concurrency.try_acquire("a").unwrap().unwrap();
    let held_b = concurrency.try_acquire("b").unwrap().unwrap();

    let selector = P2CSelector::new(concurrency, 2, DEFAULT_EXPLORE_RATE);
    let refs: Vec<&RouteUnitRecord> = units.iter().collect();
    let mut rng = rand::rngs::StdRng::seed_from_u64(9);
    assert!(
        selector
            .pick(&refs, &health, &[], None, 0, &mut rng)
            .is_some(),
        "full slots must not make selector return None (429 rejected by concurrency gate, not the selector)"
    );
    drop(held_a);
    drop(held_b);
}

#[test]
fn cooldown_remains_hard_gate_before_sampling() {
    let units = [unit("cooling", 100), unit("ok", 1)];
    let health = MemoryHealthTable::new();
    for _ in 0..5 {
        health.record(
            "cooling",
            AttemptReport::new(Err(FailureClass::Retryable), None, None),
        );
    }
    let selector = P2CSelector::new(
        Arc::new(ConcurrencyState::default()),
        2,
        DEFAULT_EXPLORE_RATE,
    );
    never_picks(&selector, &units, &health, "cooling");
}

#[test]
fn exploration_rate_is_configurable_and_bounds_degraded_traffic() {
    // Exploration must be capped by an explicit ε, independent of pool size and choices.
    let units = [unit("fast", 1), unit("mid", 1), unit("slow", 1)];
    let health = MemoryHealthTable::new();
    for key in ["fast", "mid"] {
        health.record(key, AttemptReport::new(Ok(200), Some(50), None));
    }
    health.record("slow", AttemptReport::new(Ok(200), Some(10_000), None));

    for rate in [0.02_f64, 0.10_f64] {
        let selector = P2CSelector::new(Arc::new(ConcurrencyState::default()), 2, rate);
        let slow = share_of(&selector, &units, &health, "slow");
        // The measured share should track ε (slightly lower: exploration happens only when
        // ε hits and a floor candidate exists in the pool).
        assert!(
            (0.0..=rate * 1.15).contains(&slow),
            "at exploration rate {rate} slow channel share should not exceed this order: measured {slow:.3}"
        );
    }
}

#[test]
fn exploration_does_not_grow_with_choices() {
    // Key regression: a floor gate (unconditionally admitting the floor candidate) would let
    // exploration grow to 90%+ with choices; explicit ε exploration must stay constant.
    let units = [unit("a", 1), unit("b", 1), unit("c", 1), unit("slow", 1)];
    let health = MemoryHealthTable::new();
    for key in ["a", "b", "c"] {
        health.record(key, AttemptReport::new(Ok(200), Some(50), None));
    }
    health.record("slow", AttemptReport::new(Ok(200), Some(10_000), None));

    let weak = P2CSelector::new(Arc::new(ConcurrencyState::default()), 2, 0.05);
    let strong = P2CSelector::new(Arc::new(ConcurrencyState::default()), 8, 0.05);
    let weak_share = share_of(&weak, &units, &health, "slow");
    let strong_share = share_of(&strong, &units, &health, "slow");
    assert!(
        (weak_share - strong_share).abs() < 0.03,
        "exploration does not change with choices: choices=2 {weak_share:.3} vs choices=8 {strong_share:.3}"
    );
}

/// A health table with the cooldown just ended and the ramp armed: the `ramping` channel
/// returns to a selectable state via one failure + force_recall. A pinned clock (always 0)
/// guarantees no lazy settlement happens on `pick(now_ms=0)`.
fn ramp_armed_table() -> MemoryHealthTable {
    let now = Arc::new(AtomicU64::new(0));
    let table = MemoryHealthTable::with_config_and_clock(HealthSetting::default(), {
        let now = now.clone();
        move || now.load(Ordering::Relaxed)
    });
    table.record(
        "ramping",
        AttemptReport::new(Err(FailureClass::Retryable), None, None),
    );
    table.force_recall("ramping"); // end the cooldown immediately + arm the slow-start ramp
    table
}

#[test]
fn ramp_channel_keeps_positive_share_in_equal_weight_pool() {
    // Sampling-layer ramp discount (the core of change 1): the expected share of a
    // ramp_pending channel is 10×(1/5) / (10×(1/5) + 10×1.0) = 1/6 ≈ 0.167 — discounted but
    // always positive. A recovering channel need not wait for explore_rate to rescue it: the
    // ramp floor preserves observation traffic, sampling keeps refreshing its state, and
    // accumulated counts self-heal it (under the old dead semantics `routing_weight` had no
    // production consumer, so recovering channels got a full share, contradicting the
    // documented promise of gradual recovery).
    let units = [unit("ramping", 10), unit("normal", 10)];
    let health = ramp_armed_table();
    assert!(
        health.get("ramping").ramp_pending,
        "recovering channel should be in ramp-pending state"
    );
    let selector = P2CSelector::new(
        Arc::new(ConcurrencyState::default()),
        2,
        DEFAULT_EXPLORE_RATE,
    );
    let share = share_of(&selector, &units, &health, "ramping");
    assert!(
        (0.08..=0.25).contains(&share),
        "under ramp floor (1/5) expected share ≈ 1/6: measured {share:.3}"
    );
}

#[test]
fn ramp_share_grows_linearly_with_success_count() {
    // Linear ramp recovery (f = count/min_requests, min_requests defaults to 5):
    // after 2 successes the expected share is 10×(2/5) / (10×(2/5) + 10×1.0) = 2/7 ≈ 0.286,
    // strictly between the floor share 1/6 and the full share 1/2.
    let units = [unit("ramping", 10), unit("normal", 10)];
    let health = ramp_armed_table();
    for _ in 0..2 {
        health.record("ramping", AttemptReport::new(Ok(200), None, None));
    }
    let selector = P2CSelector::new(
        Arc::new(ConcurrencyState::default()),
        2,
        DEFAULT_EXPLORE_RATE,
    );
    let share = share_of(&selector, &units, &health, "ramping");
    assert!(
        (0.20..=0.37).contains(&share),
        "at count 2/5 expected share ≈ 2/7: measured {share:.3}"
    );
}

#[test]
fn ramp_share_recovers_to_even_split_after_min_requests() {
    // Count accumulation self-heals (acceptance b): after collecting min_requests (default
    // 5) successes, slow_start_factor returns to 1.0 and an equal-weight two-unit pool
    // returns to a 50/50 split.
    let units = [unit("ramping", 10), unit("normal", 10)];
    let health = ramp_armed_table();
    for _ in 0..5 {
        health.record("ramping", AttemptReport::new(Ok(200), None, None));
    }
    let st = health.get("ramping");
    assert!(
        !st.ramp_pending && st.request_count == 5,
        "after min_requests successes should exit ramp-pending"
    );
    let selector = P2CSelector::new(
        Arc::new(ConcurrencyState::default()),
        2,
        DEFAULT_EXPLORE_RATE,
    );
    let share = share_of(&selector, &units, &health, "ramping");
    assert!(
        (0.44..=0.56).contains(&share),
        "after ramp recovery share should be even split: measured {share:.3}"
    );
}
