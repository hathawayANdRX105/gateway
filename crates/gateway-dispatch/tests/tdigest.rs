//! Differential tests for the t-digest vendored into `dispatch` (copied verbatim from
//! `algorchemy` `tests/sketch_tdigest.rs`, upstream commit
//! `2caccf43ab30f683b6985f991f99c59898e11367`).
//!
//! Five `#[test]` functions match upstream verbatim except names (translated to English); the one
//! difference is that upstream's `proptest!` monotonicity case was rewritten to
//! `StdRng::seed_from_u64` with 4 fixed seeds × 300 rounds (the repo has zero
//! property-testing dependencies — a maintainer decision; fixed seeds are reproducible and
//! pin drift just the same). The minimal counterexample shrunk in proptest-regressions is
//! kept hard-coded as its own `#[test]` (the interpolation endpoint must not exceed the last
//! centroid's mean).

use gateway_dispatch::tdigest::Tdigest;
use rand::Rng;
use rand::SeedableRng;

/// Same PRNG as `benches/union_find.rs`: multiplier 6364136223846793005, increment
/// 1442695040888963407, right shift 33. Used only to generate deterministic sample
/// streams, so those samples stay reproducible.
struct RngSeed(u64);

impl RngSeed {
    fn next(&mut self) -> usize {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 33) as usize
    }
    /// Uniform [0,1).
    // `next()` returns `(self.0 >> 33) as usize`, always < 2^31, so the `usize → f64`
    // and `u64 → f64` casts below are lossless for every value this test PRNG emits.
    #[allow(clippy::cast_precision_loss)]
    fn unit(&mut self) -> f64 {
        self.next() as f64 / (1u64 << 31) as f64
    }
}

/// Box–Muller standard normal samples (two uniforms, taking the cos branch).
// `rng.next()` is always < 2^31 (it is `(self.0 >> 33)`), so the `usize → f64`
// and `u64 → f64` casts below only normalize a PRNG output to [0, 1]; they are
// lossless in practice.
#[allow(clippy::cast_precision_loss)]
fn normal_stream(rng: &mut RngSeed, n: usize) -> Vec<f64> {
    (0..n)
        .map(|_| {
            let u1 = (rng.next() + 1) as f64 / (1u64 << 31) as f64; // (0,1], avoids log(0)
            let u2 = rng.unit(); // [0,1)
            (-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos()
        })
        .collect()
}

fn digest_of(vals: &[f64], delta: f64) -> Tdigest {
    let mut td = Tdigest::new(delta);
    for &v in vals {
        td.add(v);
    }
    td
}

// ---------- 1. Differential check on known distributions ----------

#[test]
// `N` is a fixed test constant (100_000), far below f64's 53-bit mantissa,
// so `N as f64` is exact; it only feeds a comparison tolerance.
#[allow(clippy::cast_precision_loss)]
fn uniform_and_normal_large_sample_quantile_comparison() {
    const N: usize = 100_000;
    let mut rng = RngSeed(0x2545_F491_4F6C_DD1D);

    let uni: Vec<f64> = (0..N).map(|_| rng.unit()).collect();
    let td = digest_of(&uni, 100.0);
    assert_eq!(td.count(), N as f64);
    assert!(
        (td.median().unwrap() - 0.5).abs() < 0.01,
        "U(0,1) p50 estimate {}",
        td.median().unwrap()
    );
    assert!(
        (td.quantile(0.95).unwrap() - 0.95).abs() < 0.02,
        "U(0,1) p95 estimate {}",
        td.quantile(0.95).unwrap()
    );

    let norm = normal_stream(&mut rng, N);
    let td = digest_of(&norm, 100.0);
    assert!(
        td.median().unwrap().abs() < 0.01,
        "N(0,1) p50 estimate {}",
        td.median().unwrap()
    );
    assert!(
        (td.quantile(0.95).unwrap() - 1.6449).abs() < 0.02,
        "N(0,1) p95 estimate {}",
        td.quantile(0.95).unwrap()
    );
}

// ---------- 2. Boundaries ----------

#[test]
fn empty_digest_single_value_flat_zero_weight() {
    // Empty digest
    let td = Tdigest::new(100.0);
    assert!(td.is_empty());
    assert_eq!(td.count(), 0.0);
    assert!(td.quantile(0.0).is_none());
    assert!(td.quantile(0.5).is_none());
    assert!(td.quantile(1.0).is_none());
    assert!(td.median().is_none());

    // Single value: any q returns that value exactly (the q=0/q=1 boundaries included)
    let mut one = Tdigest::new(100.0);
    one.add(42.5);
    assert!(!one.is_empty());
    assert_eq!(one.count(), 1.0);
    assert_eq!(one.quantile(0.0), Some(42.5));
    assert_eq!(one.median(), Some(42.5));
    assert_eq!(one.quantile(1.0), Some(42.5));

    // All-identical values: the median is bit-exact (when the mean matches exactly, adding
    // weight leaves the mean untouched)
    let mut flat = Tdigest::new(100.0);
    for _ in 0..10_000 {
        flat.add(7.0);
    }
    assert_eq!(flat.count(), 10_000.0);
    assert_eq!(flat.median(), Some(7.0));
    assert_eq!(flat.quantile(0.0), Some(7.0));
    assert_eq!(flat.quantile(1.0), Some(7.0));

    // Weight 0 is a no-op; a weighted add counts toward the total weight
    let mut z = Tdigest::new(100.0);
    z.add_weighted(1.0, 0.0);
    assert!(z.is_empty());
    assert_eq!(z.quantile(0.5), None);
    z.add_weighted(2.0, 5.0);
    z.add_weighted(2.0, 0.0);
    assert_eq!(z.count(), 5.0);
    assert_eq!(z.median(), Some(2.0));
}

#[test]
#[should_panic(expected = "quantile requires q in [0,1]")]
fn out_of_range_q_panics() {
    let mut td = Tdigest::new(100.0);
    td.add(1.0);
    let _ = td.quantile(1.5);
}

// ---------- 3. merge differential ----------

#[test]
fn merge_halves_vs_full_digest_comparison() {
    let mut rng = RngSeed(0x853C_49E6_748F_EA9B);
    let all: Vec<f64> = (0..100_000).map(|_| rng.unit()).collect();
    let full = digest_of(&all, 100.0);

    let mut a = digest_of(&all[..50_000], 100.0);
    a.merge(&Tdigest::new(100.0)); // merging an empty digest is a no-op
    let b = digest_of(&all[50_000..], 100.0);
    a.merge(&b);

    assert!((a.count() - 100_000.0).abs() < 1e-9);
    // Both sides were compressed separately before the merge, so information is already
    // lost; the deviation thresholds are looser than the single-stream differential (see
    // the module doc).
    for (q, tol) in [(0.5, 0.02), (0.95, 0.02), (0.99, 0.03)] {
        let (ma, mf) = (a.quantile(q).unwrap(), full.quantile(q).unwrap());
        assert!(
            (ma - mf).abs() < tol,
            "q={q}: after merge {ma}, single stream {mf}, deviation exceeds {tol}"
        );
    }
}

// ---------- 4. Ordering ----------

/// The minimal counterexample shrunk by proptest (kept hard-coded): 5 points with delta=50
/// do not trigger compression, so quantile(0.9) lands on the interpolation endpoint with
/// frac=1.0, where `m_lo` + 1.0*(`m_hi` - `m_lo`) rounds one ulp above `m_hi` while quantile(1.0)
/// returns the exact `m_hi` — monotonicity is broken by 1 ulp. The fix clamps the
/// interpolation result inside quantile; this test pins that behaviour.
#[test]
fn interpolation_endpoint_must_not_exceed_last_centroid_mean() {
    let vals = [
        411_265.603_693_516_75,
        -451_454.645_496_239_2,
        -290_106.914_453_947_33,
        -499_286.731_691_501_74,
        -613_564.874_053_542_9,
    ];
    let mut td = Tdigest::new(50.0);
    for &v in &vals {
        td.add(v);
    }
    let q90 = td.quantile(0.9).unwrap();
    let q100 = td.quantile(1.0).unwrap();
    assert!(q90 <= q100, "q90={q90} > q100={q100}");
    assert_eq!(
        q90, q100,
        "frac=1.0 must land exactly on last centroid mean"
    );
}

// ---------- 5. Monotonicity (proptest → fixed-seed rewrite) ----------

/// Fixed-seed version of upstream's `proptest::collection::vec(-1e6..1e6, 0..300)`
/// monotonicity case: `StdRng::seed_from_u64` with 4 seeds × 300 rounds, each round drawing
/// 0..300 points at random. The repo has zero property-testing dependencies (a maintainer
/// decision) — fixed seeds are reproducible and pin drift just the same.
#[test]
fn quantile_estimate_monotonic_non_decreasing_in_q() {
    for seed in [1u64, 2, 3, 4] {
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        for _ in 0..300 {
            let len = rng.gen_range(0..300);
            let vals: Vec<f64> = (0..len).map(|_| rng.gen_range(-1e6f64..1e6f64)).collect();
            let mut td = Tdigest::new(50.0);
            for &v in &vals {
                td.add(v);
            }
            if let Some(q10) = td.quantile(0.1) {
                let q50 = td.quantile(0.5).unwrap();
                let q90 = td.quantile(0.9).unwrap();
                assert!(td.quantile(0.0).unwrap() <= q10, "q0 > q10");
                assert!(q10 <= q50, "q10={q10} > q50={q50}");
                assert!(q50 <= q90, "q50={q50} > q90={q90}");
                assert!(q90 <= td.quantile(1.0).unwrap(), "q90 > q100");
            } else {
                assert_eq!(vals, [] as [f64; 0]);
                assert!(td.is_empty());
            }
        }
    }
}
