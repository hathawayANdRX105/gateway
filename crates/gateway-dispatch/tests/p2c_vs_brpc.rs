//! Correspondence between the P2C selector and the brpc `p2c_ewma` proposal
//! (apache/brpc#3340, unmerged).
//!
//! Purpose: make "why we do not copy brpc line for line" an **executable assertion** rather than a
//! claim in documentation. Item-by-item comparison:
//!
//! | Dimension | brpc proposal | This implementation | Relationship |
//! |---|---|---|---|
//! | Latency stats | `ewma_us` peak-sensitive EWMA | `latency_ewma_ms` peak-sensitive EWMA | identical |
//! | Load signal | `inflight + 1` (in-flight count) | `available_permits + 1` (free slots) | affine-equivalent, see below |
//! | Static weight | `/ max(weight, 1)` denominator | `weight / Σweight` **sampling only** | **intentionally different** |
//! | Decision direction | pick lower `score` | pick higher likelihood | opposite wording, equivalent semantics |
//! | Floor exploration | none | `explore_rate` fallback exploration | **added here** |
//!
//! ## Why the load terms are equivalent
//!
//! brpc: `inflight` = requests that backend is currently processing and has not returned yet.
//! This implementation: `available_permits` = `max_concurrency - inflight` (the permit is held
//! during `forward`'s attempt and returned when the attempt ends). The two differ by a constant
//! `max_concurrency`, which is an affine transform.
//!
//! ## Why weight cannot go into the denominator or the tie-break (Option C)
//!
//! The brpc form `score = ewma × (inflight+1) / weight` **collapses when latencies are equal**:
//! the numerator is the same while the denominator differs, so a smaller `weight` produces a
//! larger score, i.e. "lower weight gets priority" — the opposite of the configured intent.
//! `denominator_form_collapses_when_latencies_match` pins this down.
//!
//! The other direction is also wrong: `posterior = prior × likelihood` puts weight into both
//! sampling and the duel, counting the same configuration twice and making the high-weight
//! channel's share grow superlinearly. Option C lets weight determine only the **long-run traffic
//! mix** (sampling probability), while the EWMA family determines only **who is better right now**
//! (the duel). `weight_expresses_sampling_share_not_duel_score` pins down that separation.

use gateway_core::records::{RouteUnitRecord, SyncMeta};
use gateway_dispatch::health::{
    AttemptReport, FailureClass, HealthSetting, HealthTable, MemoryHealthTable,
};
use gateway_dispatch::latency_digest::LatencyDigestTable;
use gateway_dispatch::selector::{DEFAULT_EXPLORE_RATE, LIKELIHOOD_FLOOR, P2CSelector, Selector};
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

// `hits ≤ n * 8 = 32 000` and the divisor `n * 8 = 32 000`, both well below
// f64's 2^53 mantissa, so the `usize → f64` casts are exact; hit-ratio only.
#[allow(clippy::cast_precision_loss)]
fn share_of(
    selector: &P2CSelector,
    units: &[RouteUnitRecord],
    health: &MemoryHealthTable,
    key: &str,
) -> f64 {
    let refs: Vec<&RouteUnitRecord> = units.iter().collect();
    let n = 4000usize;
    let mut hits = 0usize;
    for seed in 1u64..=8 {
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        for _ in 0..n {
            let picked = selector
                .pick(&refs, health, &[], None, 0, &mut rng)
                .unwrap();
            if picked.meta.key == key {
                hits += 1;
            }
        }
    }
    hits as f64 / (n * 8) as f64
}

#[test]
fn denominator_form_collapses_when_latencies_match() {
    // brpc's denominator form: score = ewma / max(weight, 1). When latencies are equal the
    // denominator directly determines order, and the direction is reversed — lower weight → smaller
    // denominator → higher score → more likely to be selected. Reproduce this collapse with 1:99 and
    // 10:30 weight pairs.
    for (light_w, heavy_w) in [(1u32, 99u32), (10, 30)] {
        let light = 100.0_f64 / f64::from(light_w);
        let heavy = 100.0_f64 / f64::from(heavy_w);
        assert!(
            light > heavy,
            "Denominator form gives lower weight higher score at equal latency (direction reversed): w={light_w} -> {light:.3}, w={heavy_w} -> {heavy:.3}"
        );
    }
}

#[test]
fn weight_expresses_sampling_share_not_duel_score() {
    // Option C: weight **only enters sampling** (P(select u) ∝ weight/Σweight), not the duel score.
    // With equal latency and equal health, the duel ties → order comes from sampling → traffic
    // exactly equals the prior: weight 1:99 → 1/100 = 1%. The old behavior (+1 smoothing) raised it
    // to 2/102 ≈ 1.96%; if weight also entered the duel, the same configuration would be counted
    // twice and the share would grow superlinearly.
    let units = [unit("light", 1), unit("heavy", 99)];
    let health = MemoryHealthTable::new();
    let selector = P2CSelector::new(
        Arc::new(ConcurrencyState::default()),
        2,
        DEFAULT_EXPLORE_RATE,
    );
    let light = share_of(&selector, &units, &health, "light");
    assert!(
        (0.005..=0.015).contains(&light),
        "weight 1:99 traffic share should be exactly 1/100=1% (tolerance covers sampling variance), measured {light:.4}"
    );
}

#[test]
fn latency_signal_overrides_weight_in_both_forms() {
    // When the latency difference is large enough, both forms make the slower one lose — that is the
    // real problem the brpc proposal addresses (shedding degraded backends quickly), independent of
    // whether weight is in the numerator or denominator. This case pins that property: weights 10:30,
    // but at 100× slower the slow channel must yield traffic.
    let units = [unit("1", 10), unit("3", 30)];
    let health = MemoryHealthTable::new();
    for _ in 0..10 {
        health.record("1", AttemptReport::new(Ok(200), Some(5_000), None)); // slow
        health.record("3", AttemptReport::new(Ok(200), Some(50), None)); // fast
    }
    let selector = P2CSelector::new(
        Arc::new(ConcurrencyState::default()),
        2,
        DEFAULT_EXPLORE_RATE,
    );
    let slow = share_of(&selector, &units, &health, "1");
    let fast = share_of(&selector, &units, &health, "3");
    assert!(
        fast > slow,
        "100x slower channel should cede traffic: fast={fast:.3} slow={slow:.3}"
    );
}

#[test]
fn available_permits_is_affine_in_inflight() {
    // Equivalence of the load terms: available_permits = max_concurrency - inflight. Verify that
    // "a channel with fewer free slots gets a lower concurrency_factor", i.e. "a channel with more
    // in-flight requests is down-weighted" — the same direction as brpc's (inflight+1).
    let state = ConcurrencyState::default();
    state.register_channel("busy", 8);
    state.register_channel("idle", 8);
    let _free = state.try_acquire("busy");
    let _free2 = state.try_acquire("busy");
    let _free3 = state.try_acquire("busy");

    let busy = state.available_permits("busy");
    let idle = state.available_permits("idle");
    assert_eq!(busy, Some(5), "busy should have 5 slots left");
    assert_eq!(idle, Some(8), "idle should have 8 slots left");
    assert!(
        busy < idle,
        "More in-flight means fewer free slots -> lower concurrency_factor -> lower posterior"
    );
}

#[test]
fn epsilon_exploration_is_an_addition_not_in_brpc() {
    // The brpc proposal has no floor exploration. A degraded channel's posterior stays low → loses
    // every duel → 0 traffic → state never refreshes → one jitter permanently takes it out. This
    // implementation uses explore_rate as the fallback.
    let units = [unit("1", 10), unit("2", 10)];
    let health = MemoryHealthTable::new();
    for _ in 0..10 {
        health.record("1", AttemptReport::new(Ok(200), Some(50), None));
        health.record("2", AttemptReport::new(Ok(200), Some(5_000), None));
    }
    // Disable exploration → degraded channel gets 0 traffic (reproduces brpc's behavior)
    let no_explore = P2CSelector::new(Arc::new(ConcurrencyState::default()), 2, 0.0);
    let degraded = share_of(&no_explore, &units, &health, "2");
    assert!(
        degraded < 0.001,
        "Without exploration, degraded channel gets almost no traffic (brpc behavior), measured {degraded:.5}"
    );
    // Enable exploration → non-zero observable traffic returns
    let explore = P2CSelector::new(
        Arc::new(ConcurrencyState::default()),
        2,
        DEFAULT_EXPLORE_RATE * 20.0,
    );
    let recovered = share_of(&explore, &units, &health, "2");
    assert!(
        recovered > 0.0,
        "With exploration, degraded channel must retain traffic to self-heal, measured {recovered:.5}"
    );
}

#[test]
fn likelihood_floor_is_the_clamp_ceiling() {
    // Floor exploration only applies to candidates whose likelihood is clamped to the floor; healthy
    // candidates that are not clamped are unaffected. The floor value is LIKELIHOOD_FLOOR.
    assert!(
        (LIKELIHOOD_FLOOR - 0.05).abs() < f64::EPSILON,
        "Floor constant should stay at 0.05"
    );
    // Cooldown changed to "trigger immediately on every failure", while start_cooldown clears
    // request_count. EWMA's threshold is request_count > min_requests — the default 5 means every
    // failure falls just short of the next update's eligibility (after the reset it must accumulate
    // 6 again), so the health score is repeatedly pulled back by successful samples and converges to
    // a fixed point 0.21/0.51 ≈ 0.41, never reaching the floor.
    //
    // min_requests = 0 lets every failure participate in the update; this is the real reachable path
    // for "sustained failure pushes the health score to the floor" (a jittery channel is handled by
    // the cooldown ladder, not by EWMA reaching the floor).
    let health = MemoryHealthTable::with_config(HealthSetting {
        min_requests: 0,
        ..HealthSetting::default()
    });
    for _ in 0..30 {
        health.record(
            "1",
            AttemptReport::new(Err(FailureClass::Retryable), None, None),
        );
    }
    let degraded = health.get("1").ewma_score;
    assert!(
        degraded <= LIKELIHOOD_FLOOR * 1.01,
        "After consecutive failures, health score should land near the floor, measured {degraded}"
    );
}

// ────────────────────────────────────────────────────────────────────
// t-digest attachment scenario (design §3.2): the latency_factor data source switches from the EWMA
// scalar to p50/p95 quantiles. Three cases pin down "which bug makes it red".

/// Rotation window (ms), aligned with the `LatencyDigestTable::new` default window.
const DIGEST_WINDOW_MS: u64 = 30_000;

/// Fake-clock digest table: `with_clock` makes rotation deterministic and reproducible in tests.
fn clocked_digest(now: &Arc<AtomicU64>) -> Arc<LatencyDigestTable> {
    let c = Arc::clone(now);
    LatencyDigestTable::with_clock(move || c.load(Ordering::Relaxed), DIGEST_WINDOW_MS)
}

/// Feed first-window samples for each key in `fills` (accumulating into cur), then advance the clock
/// past the window to trigger rotation, so each key's `prev` absorbs the first-window distribution —
/// `quantiles(key)` starts hitting from then on.
fn arm_digests(now: &AtomicU64, table: &LatencyDigestTable, fills: &[(&str, &[u32])]) {
    now.store(0, Ordering::Relaxed);
    for entry in fills {
        let (key, samples) = *entry;
        for &s in samples {
            table.report(key, s);
        }
    }
    // Cross the window boundary to trigger rotation: prev = merge(empty, cur); then add one sample to
    // the new cur (prev untouched).
    now.store(DIGEST_WINDOW_MS + 1, Ordering::Relaxed);
    for entry in fills {
        let (key, samples) = *entry;
        table.report(key, samples[0]);
    }
}

/// Fat-tailed distribution: p50≈100, p95≫p50 (180 low values + 20 spikes in 1k–5k).
fn fat_tail_samples() -> Vec<u32> {
    [100u32; 180]
        .into_iter()
        .chain((1000..=5000).step_by(1000).cycle().take(20))
        .collect()
}

/// Tight distribution: p50≈p95≈100 (all pool values equal), almost no tail penalty.
fn tight_samples() -> Vec<u32> {
    vec![100u32; 200]
}

#[test]
fn digest_tail_penalty_pushes_fat_tail_channel_down() {
    // Same p50, different p95: two equal-weight channels, A fat-tailed (p95≫p50), B tight
    // (p95≈p50) → B share > 0.6. Bugs that would make this red: the digest's p95 tail penalty is not
    // fed into likelihood (only speed / forgot with_latency_digest / dirty-data divide-by-zero) →
    // A's and B's latency_factor tie or fall back to the EWMA branch (constant 1.0 without EWMA
    // data) → B share drops back to the 0.5 prior and fails the 0.6 gate.
    let units = [unit("A", 10), unit("B", 10)];
    let health = MemoryHealthTable::new();
    let now = Arc::new(AtomicU64::new(0));
    let digest = clocked_digest(&now);
    let a = fat_tail_samples();
    let b = tight_samples();
    arm_digests(&now, &digest, &[("A", &a[..]), ("B", &b[..])]);

    let selector = P2CSelector::new(
        Arc::new(ConcurrencyState::default()),
        2,
        DEFAULT_EXPLORE_RATE,
    )
    .with_latency_digest(Arc::clone(&digest));

    let b_share = share_of(&selector, &units, &health, "B");
    assert!(
        b_share > 0.6,
        "Fat-tailed channel A should be suppressed by tail penalty, tight channel B share should be > 0.6, measured B={b_share:.4}"
    );
}

#[test]
fn digest_same_distribution_keeps_prior_share() {
    // Two channels with the same distribution ⇒ share converges to the prior (no drift). Feed A and
    // B the same fat-tailed distribution; p50/p95 are perfectly symmetric → latency_factor ties →
    // the duel falls back to sampling order → traffic exactly = prior (equal weights = 0.5).
    // Bugs that would make this red: quantile reads introduce a per-candidate asymmetric offset, the
    // digest factor is multiplied into the prior (same distribution counted twice), or dirty-data
    // divide-by-zero produces NaN and breaks the tie → share drifts away from 0.5.
    let units = [unit("A", 10), unit("B", 10)];
    let health = MemoryHealthTable::new();
    let now = Arc::new(AtomicU64::new(0));
    let digest = clocked_digest(&now);
    let a = fat_tail_samples();
    let b = fat_tail_samples();
    arm_digests(&now, &digest, &[("A", &a[..]), ("B", &b[..])]);

    let selector = P2CSelector::new(
        Arc::new(ConcurrencyState::default()),
        2,
        DEFAULT_EXPLORE_RATE,
    )
    .with_latency_digest(Arc::clone(&digest));

    let b_share = share_of(&selector, &units, &health, "B");
    assert!(
        (0.45..=0.55).contains(&b_share),
        "Two same-distribution channels should converge to prior 0.5 (digest introduces no drift), measured B={b_share:.4}"
    );
}

#[test]
fn digest_cold_channel_is_neutral_not_starved() {
    // A cold channel (fewer than min_requests samples) ⇒ neutral like an all-cold pool, share ≈
    // prior (not starved). A tight distribution is fed enough to pass the warm gate (200+ samples);
    // B gets only 2 samples (weight < min_requests=5) → B takes the cold path latency_factor=1.0.
    // Both A and B are neutral → share = prior 0.5. Bugs that would make this red: a cold channel is
    // clamped to LIKELIHOOD_FLOOR (0.05) instead of neutral 1.0 → B loses every duel → share << 0.5
    // (starved); or the warm predicate misses the weight gate (treating B as warm and using its
    // residual samples for the tail) → share drifts.
    let units = [unit("A", 10), unit("B", 10)];
    let health = MemoryHealthTable::new(); // default min_requests = 5
    let now = Arc::new(AtomicU64::new(0));
    let digest = clocked_digest(&now);
    let a = tight_samples();
    let b = [100u32; 2];
    arm_digests(&now, &digest, &[("A", &a[..]), ("B", &b[..])]);

    // Gate check: A warm (weight ≥ 5), B cold (weight < 5).
    assert!(
        digest.weight("A") >= 5.0,
        "A should fill the warm gate, measured weight={}",
        digest.weight("A")
    );
    assert!(
        digest.weight("B") < 5.0,
        "B should stay cold, measured weight={}",
        digest.weight("B")
    );

    let selector = P2CSelector::new(
        Arc::new(ConcurrencyState::default()),
        2,
        DEFAULT_EXPLORE_RATE,
    )
    .with_latency_digest(Arc::clone(&digest));

    let b_share = share_of(&selector, &units, &health, "B");
    assert!(
        (0.45..=0.55).contains(&b_share),
        "Cold channel B should stay neutral and not starve, share ~ prior 0.5, measured B={b_share:.4}"
    );
}
