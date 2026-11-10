//! P2C token-load discount — a channel's in-flight token occupancy is a decision-layer factor
//! that shrinks the share of a channel which has "eaten its TPM".
//!
//! A deterministic rng is injected (statistical shape mirrors `tests/p2c_selector.rs`). The token
//! budget is attached through `P2CSelector::with_token`; when it is not attached, when the channel
//! cap is 0, or when inflight is 0, the decision factor stays 1.0 (regression guard: the
//! distribution is byte-identical to the unattached case).

use std::sync::Arc;

use gateway_core::records::{RouteUnitRecord, SyncMeta};
use gateway_dispatch::health::MemoryHealthTable;
use gateway_dispatch::selector::{DEFAULT_EXPLORE_RATE, P2CSelector, Selector};
use gateway_dispatch::token_budget::TokenBudgetState;
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

// `hits ≤ DRAWS_PER_SEED * SEEDS = 3 600` and the divisor is that same fixed
// test constant, so both `usize → f64` casts are exact (well below f64's
// 2^53 mantissa); this computes a hit ratio, not big-integer bookkeeping.
#[allow(clippy::cast_precision_loss)]
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
    hits as f64 / (DRAWS_PER_SEED * usize::try_from(SEEDS).expect("SEEDS = 12, fits usize")) as f64
}

#[test]
fn token_saturated_channel_loses_to_unregistered() {
    // Two units with equal weight (10:10). A is registered with cap=1000 and inflight pinned at
    // the cap (load=1.0 → discount 0.0 → likelihood hits the floor 0.05); B is unregistered
    // (load=None → discount 1.0).
    // bug: if the token discount does not reach the decision layer (selector fails to read load /
    // the multiplier is dropped), A never hits the floor and B's share degrades to the prior
    // ≈ 0.5 instead of > 0.95 → red.
    let units = [unit("A", 10), unit("B", 10)];
    let health = MemoryHealthTable::new();
    let token = TokenBudgetState::new();
    token.register_channel("A", 1000);
    // inflight(A) pinned at the cap; the handle stays in scope, so load(A) stays 1.0 while
    // the shares are measured.
    let _held_full = token.preclaim("A", 1000);

    let selector = P2CSelector::new(
        Arc::new(ConcurrencyState::default()),
        2,
        DEFAULT_EXPLORE_RATE,
    )
    .with_token(Arc::clone(&token));
    let b_share = share_of(&selector, &units, &health, "B");
    assert!(
        b_share > 0.95,
        "token-maxed channel A should hit floor, B share > 0.95 (A only explore left), measured B={b_share:.3}"
    );
}

#[test]
fn token_none_matches_prior_split() {
    // Regression guard: with no token budget attached, an equal-weight two-unit pool splits
    // strictly by the prior (≈0.5).
    // bug: if token=None still changes prior sampling (the discount leaks into the sampling
    // layer), B ≠ 0.5 → red.
    let units = [unit("A", 10), unit("B", 10)];
    let health = MemoryHealthTable::new();
    let selector = P2CSelector::new(
        Arc::new(ConcurrencyState::default()),
        2,
        DEFAULT_EXPLORE_RATE,
    );
    let b_share = share_of(&selector, &units, &health, "B");
    assert!(
        (0.45..=0.55).contains(&b_share),
        "without token attached B share should follow prior ≈ 0.5, measured {b_share:.3}"
    );
}

#[test]
fn token_inflight_zero_is_neutral() {
    // Token attached but inflight=0 (A cap=1000, nothing preclaimed): load(A)=Some(0.0) →
    // discount 1.0, neutral like B (unregistered → discount 1.0), so the distribution must match
    // token=None (an even split).
    // bug: if inflight=0 still yields a discount < 1.0 (load misread as non-zero / wrong
    // denominator), B ≠ 0.5 → red.
    let units = [unit("A", 10), unit("B", 10)];
    let health = MemoryHealthTable::new();
    let token = TokenBudgetState::new();
    token.register_channel("A", 1000);
    // No preclaim → inflight(A)=0.

    let selector = P2CSelector::new(
        Arc::new(ConcurrencyState::default()),
        2,
        DEFAULT_EXPLORE_RATE,
    )
    .with_token(Arc::clone(&token));
    let b_share = share_of(&selector, &units, &health, "B");
    assert!(
        (0.45..=0.55).contains(&b_share),
        "at inflight=0 token discount should be neutral, B share ≈ 0.5, measured {b_share:.3}"
    );
}
