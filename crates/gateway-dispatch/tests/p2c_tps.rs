//! Statistical cases for P2C's fourth factor (generation throughput `tps_ema`).
//!
//! Shaped after `tests/p2c_token_load.rs` (fake rng injection, `share_of` sampling):
//! - two channels with equal TTFT and 2:1 throughput ⇒ the low-throughput channel's
//!   share is clearly below the prior;
//! - both channels' `tps_ema` are 0 (no observations) ⇒ shares converge to the prior
//!   and the factor stays neutral;
//! - health system `enabled=false` ⇒ every factor is neutral (current-behavior
//!   assertion, pinning the gray-release semantics).

use std::sync::Arc;

use app_contract::records::{RouteUnitRecord, SyncMeta};
use gateway_dispatch::health::{HealthSetting, HealthTable, MemoryHealthTable, TpsSample};
use gateway_dispatch::selector::{DEFAULT_EXPLORE_RATE, P2CSelector, Selector};
use gateway_gate::concurrency::ConcurrencyState;
use rand::SeedableRng;

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

fn selector() -> P2CSelector {
    P2CSelector::new(
        Arc::new(ConcurrencyState::default()),
        2,
        DEFAULT_EXPLORE_RATE,
    )
}

#[test]
fn faster_tps_channel_wins_share() {
    // Two units with equal weight (10:10) and equal TTFT (neither feeds latency, so
    // the latency factor is always 1.0); throughput 2:1 (A 100 / B 50 tok/s, entering
    // the EMA through the late-arrival record_tps channel).
    // Low-throughput B has tps_factor = 50/100 = 0.5 and A = 1.0; A wins virtually
    // every duel, so B's share should sit clearly below the 0.5 prior (pinning
    // "throughput 2:1 ⇒ a visibly lower share for the low-throughput channel").
    // bug: if the fourth factor is missing from likelihood (multiplier not applied /
    // the wrong tps_ema read), B's share ≈ 0.5 → red.
    let units = [unit("A", 10), unit("B", 10)];
    let health = MemoryHealthTable::new();
    health.record_tps("A", TpsSample::Measured(100.0));
    health.record_tps("B", TpsSample::Measured(50.0));

    let sel = selector();
    let b_share = share_of(&sel, &units, &health, "B");
    assert!(
        b_share < 0.40,
        "low-throughput channel B share should be well below prior 0.5: measured {b_share:.3}"
    );
    let a_share = share_of(&sel, &units, &health, "A");
    assert!(
        a_share > 0.60,
        "high-throughput channel A should take excess share (> 0.6): measured {a_share:.3}"
    );
}

#[test]
fn no_observations_keeps_prior_split() {
    // Regression guard: when both channels' tps_ema are 0 (no throughput observation
    // at all), the fourth factor stays neutral at 1.0 and the equal-weight two-unit
    // pool splits exactly by the 0.5 prior, with no drift.
    // bug: if unobserved channels still enter the duel (treating 0 as a valid baseline
    // or leaking NaN into the product), the share drifts from 0.5 → red.
    let units = [unit("A", 10), unit("B", 10)];
    let health = MemoryHealthTable::new();

    let sel = selector();
    let b_share = share_of(&sel, &units, &health, "B");
    assert!(
        (0.45..=0.55).contains(&b_share),
        "without throughput observations B share should follow prior ≈ 0.5: measured {b_share:.3}"
    );
}

#[test]
fn disabled_health_system_keeps_factors_neutral() {
    // Health system enabled=false (gray release off) ⇒ the fourth factor stays neutral
    // at 1.0 even though the user-side record_tps channel did write tps_ema (the
    // health table keeps accounting; the selector just does not use it in the duel),
    // so shares still split by the prior (current-behavior assertion: the gray-release
    // switch causes zero disturbance).
    // bug: if the selector reads tps_ema but forgets the enabled gray key (enabled=false
    // still enters the duel), the share drifts from 0.5 → red.
    let units = [unit("A", 10), unit("B", 10)];
    let health = MemoryHealthTable::with_config(HealthSetting {
        enabled: false,
        ..HealthSetting::default()
    });
    // The user-side late-arrival channel still writes (not participating in selection
    // ≠ stopping accounting).
    health.record_tps("A", TpsSample::Measured(200.0));
    health.record_tps("B", TpsSample::Measured(50.0));

    let sel = selector();
    let b_share = share_of(&sel, &units, &health, "B");
    assert!(
        (0.45..=0.55).contains(&b_share),
        "with health system disabled B share should follow prior ≈ 0.5 (fourth factor neutral): measured {b_share:.3}"
    );
}
