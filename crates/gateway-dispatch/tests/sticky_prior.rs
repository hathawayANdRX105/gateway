//! Statistics and regression coverage for the sticky prior boost (design
//! prompt-cache-sticky-routing §4/§5): the boost enters only the **prior/sampling**
//! layer — posterior likelihood and the veto power (cooldown/exclude/disabled) stay
//! untouched, stickiness never hard-pins a channel, and the prior is explicitly
//! renormalized after the boost (Σprior == 1).

use gateway_core::records::ChannelSettings;
use gateway_core::records::RouteUnitRecord;
use gateway_core::records::SyncMeta;
use gateway_dispatch::health::{AttemptReport, FailureClass, HealthTable, MemoryHealthTable};
use gateway_dispatch::retry::{AttemptOutcome, FakeTimeControl, RetryPolicy, run_retry_loop};
use gateway_dispatch::selector::{DEFAULT_EXPLORE_RATE, MAX_STICKY_BOOST, P2CSelector, Selector};
use gateway_dispatch::{Candidate, DispatchError};
use gateway_gate::concurrency::ConcurrencyState;
use rand::SeedableRng;
use std::sync::Arc;

const SEEDS: u64 = 12;
const DRAWS_PER_SEED: usize = 300;

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

fn cand(key: &str) -> Candidate {
    Candidate {
        unit: unit(key, 10),
        secret: "s".into(),
        base_url: "https://u".into(),
        upstream_model: "m".to_string(),
        provider_type: "openai".into(),
        settings: ChannelSettings::default(),
    }
}

fn selector() -> P2CSelector {
    P2CSelector::new(
        Arc::new(ConcurrencyState::default()),
        2,
        DEFAULT_EXPLORE_RATE,
    )
}

/// Accumulated share over multiple seeds. With equal latency (the two candidates'
/// likelihoods tie) traffic equals the prior exactly, so these statistics directly
/// measure how much the boost raised the prior.
// `hits ≤ DRAWS_PER_SEED * SEEDS = 3 600` and the divisor is that same fixed
// test constant, so both `usize → f64` casts are exact (well below f64's
// 2^53 mantissa); this computes a hit ratio, not big-integer bookkeeping.
#[allow(clippy::cast_precision_loss)]
fn share_of(
    selector: &P2CSelector,
    units: &[RouteUnitRecord],
    health: &dyn HealthTable,
    key: &str,
    sticky: Option<&str>,
) -> f64 {
    let refs: Vec<&RouteUnitRecord> = units.iter().collect();
    let mut hits = 0usize;
    for seed in 1..=SEEDS {
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        for _ in 0..DRAWS_PER_SEED {
            let picked = selector
                .pick(&refs, health, &[], sticky, 0, &mut rng)
                .unwrap();
            if picked.meta.key == key {
                hits += 1;
            }
        }
    }
    hits as f64 / (DRAWS_PER_SEED * usize::try_from(SEEDS).expect("SEEDS = 12, fits usize")) as f64
}

/// Two rounds with the same prefix: after the first-round binding, the sticky channel's
/// share rises significantly in the second round (design §5.3).
///
/// Expected: a 1:1 prior renormalized by boost=4 becomes 4:1 → 0.8 : 0.2 (3600 draws,
/// σ≈0.007, with an assertion band of ±0.1).
/// Which bug would make this red: the boost never enters sampling (the sticky argument
/// ignored by `pick` → still 0.5); the boost enters the **tie-break** instead of the
/// prior (the tie-break is polluted and the ratio is no longer the prior ratio).
#[test]
fn second_round_same_prefix_boosts_sticky_share_to_80_percent() {
    let units = [unit("a", 1), unit("b", 1)];
    let health = MemoryHealthTable::new();

    let share = share_of(&selector(), &units, &health, "a", Some("a"));
    assert!(
        (0.70..=0.90).contains(&share),
        "boost=4 with equal channel weights: prior 1:1 -> 4:1, sticky channel share should be ~0.8, measured {share:.3}"
    );
    let other = share_of(&selector(), &units, &health, "a", None);
    assert!(
        (0.44..=0.56).contains(&other),
        "control group: non-sticky traffic still splits evenly, measured {other:.3}"
    );
}

/// Veto power: when the sticky channel is cooling down (or already in the exclude set)
/// there is no boost, and selection degrades to plain P2C.
///
/// Which bug would make this red: any "hard pin" shape (`if sticky == k { return k }`, or
/// a boost that bypasses eligible filtering) — the vetoed sticky channel is still selected,
/// or the remaining pool's ratio is distorted by the boost.
#[test]
fn vetoed_sticky_channel_degrades_to_plain_p2c() {
    let units = [unit("a", 1), unit("b", 3), unit("c", 1)];

    // Cooldown veto: a is in the cooldown window, so sticky=a leaves nowhere to apply the
    // boost and b:c stays 3:1.
    let cooling = MemoryHealthTable::new();
    for _ in 0..5 {
        cooling.record(
            "a",
            AttemptReport::new(Err(FailureClass::Retryable), None, None),
        );
    }
    assert!(!cooling.is_selectable("a", 0), "a should be in cooldown");
    let b = share_of(&selector(), &units, &cooling, "b", Some("a"));
    let c = share_of(&selector(), &units, &cooling, "c", Some("a"));
    assert!(
        (0.70..=0.80).contains(&b) && (0.15..=0.30).contains(&c),
        "when the sticky channel is vetoed by cooldown it should degrade to plain P2C (b:c = 3:1), measured b={b:.3} c={c:.3}"
    );

    // Exclude veto: b is healthy and is the sticky channel, but it was already tried in
    // this request ⇒ never reselect it.
    let healthy = MemoryHealthTable::new();
    let refs: Vec<&RouteUnitRecord> = units.iter().collect();
    for seed in 1..=8u64 {
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        for _ in 0..200 {
            let picked = selector()
                .pick(&refs, &healthy, &["b".to_string()], Some("b"), 0, &mut rng)
                .unwrap();
            assert_ne!(
                picked.meta.key, "b",
                "channels in the exclude set must not be reselected due to stickiness (stickiness never hard-pins)"
            );
        }
    }
}

/// Unbound traffic conservation: with `sticky=None` shares match the current behavior
/// (the boost does not leak into the non-hit pool, design §5.4). Two selectors, one with
/// the extreme boost=99 and one with the default 4.0, are cross-checked:
/// Which bug would make this red: the boost/renormalization applied to all candidates (or
/// the boost folded into the total-sum analysis) makes the two shares diverge.
#[test]
fn unbound_traffic_conservation() {
    let units = [unit("a", 1), unit("b", 3)];
    let health = MemoryHealthTable::new();
    let plain = selector();
    let boosted = selector().with_sticky_boost(99.0);

    let baseline = share_of(&plain, &units, &health, "b", None);
    assert!(
        (0.65..=0.85).contains(&baseline),
        "baseline: prior 3:4 -> 0.75, measured {baseline:.3}"
    );
    let same = share_of(&boosted, &units, &health, "b", None);
    assert!(
        (baseline - same).abs() < 0.01,
        "with sticky=None the boost must not change the ratio: baseline {baseline:.3} vs boosted selector {same:.3}"
    );
}

/// Normalization: Σprior == 1 after the boost (floating-point tolerance 1e-9), and the
/// all-zero-weight degradation path stays uniform.
///
/// Which bug would make this red: forgetting to renormalize by the new total after the
/// boost (the prior drifts and diagnostics misreport, e.g. a=4 and b=3 used directly as
/// weights); or the all-zero degradation branch broken by the boost.
#[test]
fn priors_renormalize_to_one_after_boost() {
    let units = [unit("a", 1), unit("b", 3)];
    let health = MemoryHealthTable::new();
    let refs: Vec<&RouteUnitRecord> = units.iter().collect();

    let scored = selector().scored(&refs, &health, &[], Some("a"), 0);
    let sum: f64 = scored.iter().map(|(_, prior, _)| prior).sum();
    assert!(
        (sum - 1.0).abs() < 1e-9,
        "after the boost Σprior must be 1, measured {sum:.12}"
    );
    let a = scored.iter().find(|(u, _, _)| u.meta.key == "a").unwrap();
    let b = scored.iter().find(|(u, _, _)| u.meta.key == "b").unwrap();
    let a_prior = a.1;
    let b_prior = b.1;
    assert!(
        (a_prior - 4.0 / 7.0).abs() < 1e-9,
        "a's prior should be 4/7, measured {a_prior:.12}"
    );
    assert!(
        (b_prior - 3.0 / 7.0).abs() < 1e-9,
        "b's prior should be 3/7, measured {b_prior:.12}"
    );

    let plain_sum: f64 = selector()
        .scored(&refs, &health, &[], None, 0)
        .iter()
        .map(|(_, prior, _)| prior)
        .sum();
    assert!(
        (plain_sum - 1.0).abs() < 1e-9,
        "on a miss Σprior is also 1, measured {plain_sum:.12}"
    );

    let zeros = [unit("a", 0), unit("b", 0)];
    let zero_refs: Vec<&RouteUnitRecord> = zeros.iter().collect();
    let zero_scored = selector().scored(&zero_refs, &health, &[], Some("a"), 0);
    let zero_sum: f64 = zero_scored.iter().map(|(_, prior, _)| prior).sum();
    assert!(
        (zero_sum - 1.0).abs() < 1e-9,
        "the all-zero-weight degradation path Σprior must be 1, measured {zero_sum:.12}"
    );
    assert!(
        (zero_scored[0].1 - zero_scored[1].1).abs() < 1e-9,
        "the all-zero-weight degradation path must be uniform: {:?} vs {:?}",
        zero_scored[0].1,
        zero_scored[1].1
    );
}

/// Upper-bound clamping plus defense in depth: an out-of-range boost (`f64::MAX`,
/// non-finite, ≤0, or above the bound) is always clamped to [`MAX_STICKY_BOOST`], the
/// prior stays finite (never NaN), and `pick` neither panics nor returns a candidate
/// outside the pool.
///
/// Which bug would make this red: removing the clamp in `with_sticky_boost` (assigning the
/// value as-is) — `f64::MAX` passes through ⇒ `weight × ramp × boost = inf` ⇒ post-boost
/// Σw = inf ⇒ the prior becomes `inf/inf = NaN` ⇒ `pick_weighted_index`'s
/// `gen_range(0.0..NaN)` panics (rand empty range).
#[test]
fn out_of_range_boost_clamps_priors_stay_finite_and_pick_does_not_panic() {
    let units = [unit("a", 100), unit("b", 100)];
    let health = MemoryHealthTable::new();
    let refs: Vec<&RouteUnitRecord> = units.iter().collect();

    for boost in [
        f64::MAX,
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::NAN,
        0.0,
        -1.0,
    ] {
        let sel = selector().with_sticky_boost(boost);
        // Read the prior directly from scored (covers the normalization branch): it must
        // be finite and non-negative, never NaN/inf.
        let scored = sel.scored(&refs, &health, &[], Some("a"), 0);
        assert_eq!(scored.len(), 2, "eligible pool must be non-empty");
        for (u, prior, _) in &scored {
            assert!(
                prior.is_finite() && *prior >= 0.0,
                "boost={boost:?}: prior for {} must be finite and non-negative, measured {prior:?}",
                u.meta.key
            );
        }
        // pick must not panic (a NaN prior would blow up sampling), and the returned
        // candidate must be in the pool.
        let mut rng = rand::rngs::StdRng::seed_from_u64(7);
        let picked = sel
            .pick(&refs, &health, &[], Some("a"), 0, &mut rng)
            .expect("pick must not panic and must return a candidate");
        assert!(
            units.iter().any(|u| u.meta.key == picked.meta.key),
            "pick must return an in-pool candidate, measured {}",
            picked.meta.key
        );
    }

    // Self-verifying convergence target: out-of-range input is bit-identical to "pass the
    // upper bound directly" — proving the clamp lands on MAX_STICKY_BOOST and not just some
    // finite value (asserting "does not panic" is too weak; clamping to any finite value
    // would pass).
    let prior_a = |boost: f64| {
        selector()
            .with_sticky_boost(boost)
            .scored(&refs, &health, &[], Some("a"), 0)
            .iter()
            .find(|(u, _, _)| u.meta.key == "a")
            .unwrap()
            .1
    };
    for boost in [f64::MAX, f64::NAN, 0.0, -1.0] {
        assert_eq!(
            prior_a(boost),
            prior_a(MAX_STICKY_BOOST),
            "boost={boost:?}: out-of-range input must converge to the upper bound {MAX_STICKY_BOOST}"
        );
    }
    assert_eq!(
        prior_a(MAX_STICKY_BOOST * 2.0),
        prior_a(MAX_STICKY_BOOST),
        "values above the bound also converge to the upper bound"
    );
    // An ordinary valid value passes through unchanged (the clamp must not harm valid
    // config): two equally weighted channels with boost=4 → 4:1 → 0.8.
    assert_eq!(
        prior_a(4.0),
        4.0 / 5.0,
        "valid boost=4.0 must be preserved as-is"
    );
}

/// Retry switches candidates: after the first attempt on the sticky channel fails, switch
/// candidates; stickiness does not prevent the switch (exclude semantics).
///
/// Which bug would make this red: a sticky hard pin (the second round returns to the
/// already-tried channel, or removes it from exclude); the retry loop not passing exclude
/// to select.
#[test]
fn retry_switches_candidate_despite_sticky() {
    let units = [unit("a", 1), unit("b", 1)];
    let health = MemoryHealthTable::new();
    let refs: Vec<&RouteUnitRecord> = units.iter().collect();
    let mut rng = rand::rngs::StdRng::seed_from_u64(7);
    for _ in 0..100 {
        let picked = selector().pick(&refs, &health, &["a".to_string()], Some("a"), 0, &mut rng);
        assert_eq!(
            picked.unwrap().meta.key,
            "b",
            "when the sticky channel is already in exclude, must switch candidates (exclude semantics must not be bypassed by stickiness)"
        );
    }

    let mut calls: Vec<Vec<String>> = Vec::new();
    let result = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(run_retry_loop(
            "g",
            "m",
            &RetryPolicy {
                max_attempts: 3,
                total_budget_ms: 0,
            },
            Some("a"),
            None,
            |_: &str, _: &str, exclude: &[String], sticky: Option<&str>, _: Option<u64>| {
                assert_eq!(
                    sticky,
                    Some("a"),
                    "the sticky key must be passed through unchanged to select each round"
                );
                calls.push(exclude.to_vec());
                // Emulate the real semantics: prefer the untried sticky channel, and
                // switch naturally once it has been tried.
                let key = if exclude.iter().any(|k| k == "a") {
                    "b"
                } else {
                    "a"
                };
                Ok::<Candidate, DispatchError>(cand(key))
            },
            |candidate: &Candidate| {
                // Sticky channel a fails on the first attempt (Retryable); switching to
                // candidate b succeeds.
                let key = candidate.unit.meta.key.clone();
                async move {
                    if key == "a" {
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

    let (attempt, _) =
        result.expect("after the sticky channel fails, must switch candidates, retry, and succeed");
    assert_eq!(
        attempt.candidate.unit.meta.key, "b",
        "the winning candidate should be the switched-to b, not the already-tried a pulled back by stickiness"
    );
    assert_eq!(
        calls.first().map(Vec::as_slice),
        Some([].as_slice()),
        "round one's exclude should be empty (stickiness prefers channel a in round one)"
    );
    assert!(
        calls.get(1).is_some_and(|ex| ex.iter().any(|k| k == "a")),
        "round two's exclude should contain the already-tried sticky channel, measured {calls:?}"
    );
}
