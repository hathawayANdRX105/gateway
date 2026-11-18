//! Criterion benchmark for the proposed cheap prefilter in front of the response
//! cache key.
//!
//! Measures the three request paths the proposal creates, per body size, over the
//! same corpus `hash_candidates.rs` uses:
//!
//! - `sha256_only`: status quo, full digest on every request.
//! - `prefilter_miss`: fingerprint plus an index probe that misses, then the full
//!   digest anyway — the store path.
//! - `prefilter_hit`: fingerprint plus an index probe that hands back the stored
//!   full key, no digest at all.
//!
//! The prefilter cannot skip the digest on a miss: the cache is write-through, so
//! the new response is filed under the full key that miss still has to derive. Only
//! the hit path saves work, which is what the printed break-even conclusion
//! quantifies — nanoseconds saved per hit, nanoseconds paid per miss, and the hit
//! rate at which the two cancel.
//!
//! No throughput is reported: bytes per second would misreport the hit path, which
//! never reads the whole body. Per-request nanoseconds is the unit the conclusion
//! needs, and what Criterion reports by default.

#[path = "support/hash_candidates.rs"]
mod support;

#[path = "support/prefilter.rs"]
mod prefilter;

use std::hint::black_box;
use std::time::{Duration, Instant};

use criterion::{Criterion, criterion_group, criterion_main};
use prefilter::{FullKey, QuickIndex};
use support::{BODY_SIZES, RequestCase, corpus};

/// Requests per timed batch: batching keeps the clock read out of the per-request
/// cost, which matters because the cheap paths are tens of nanoseconds.
const CONCLUSION_BATCH: u32 = 200;

/// Timed batches per path and body size; the median batch is the reported cost.
const CONCLUSION_BATCHES: usize = 100;

/// The three request paths the prefilter proposal changes.
#[derive(Clone, Copy, Debug)]
enum Path {
    /// Status quo: derive the full key, both digests, every request.
    Sha256Only,
    /// Fingerprint plus a probe that misses, then the full key anyway.
    PrefilterMiss,
    /// Fingerprint plus a probe that hands back the stored full key.
    PrefilterHit,
}

impl Path {
    /// Every path, in reporting order (status quo first).
    const ALL: [Path; 3] = [Path::Sha256Only, Path::PrefilterMiss, Path::PrefilterHit];

    /// Benchmark id suffix for this path.
    fn label(self) -> &'static str {
        match self {
            Path::Sha256Only => "sha256_only",
            Path::PrefilterMiss => "prefilter_miss",
            Path::PrefilterHit => "prefilter_hit",
        }
    }

    /// The index this path probes. The status quo never looks, so it is handed the
    /// cold index to keep the call sites uniform.
    fn index(self, harness: &Harness) -> &QuickIndex {
        match self {
            Path::Sha256Only | Path::PrefilterMiss => &harness.cold,
            Path::PrefilterHit => &harness.warm,
        }
    }

    /// Run this path once for `case`.
    fn run(self, index: &QuickIndex, case: &RequestCase) -> FullKey {
        match self {
            Path::Sha256Only => prefilter::full_key(case),
            // Miss and hit are one lookup; only the index state differs.
            Path::PrefilterMiss | Path::PrefilterHit => prefilter::prefilter_lookup(index, case),
        }
    }
}

/// One measured lane: the corpus cases plus the two index states a lookup can find.
struct Harness {
    cases: Vec<RequestCase>,
    /// Index without the measured cases: what a first request probes (miss).
    cold: QuickIndex,
    /// Index with the measured cases stored: what a repeat request probes (hit).
    warm: QuickIndex,
}

fn build_harness() -> Harness {
    let cases = corpus(BODY_SIZES.len());
    let cold = QuickIndex::cold(prefilter::INDEX_FILL);
    let mut warm = QuickIndex::cold(prefilter::INDEX_FILL);
    for case in &cases {
        warm.store(case);
    }
    Harness { cases, cold, warm }
}

/// Time one batch of requests through `path` and return the per-request cost in
/// nanoseconds.
fn batch_ns(path: Path, index: &QuickIndex, case: &RequestCase) -> f64 {
    let start = Instant::now();
    for _ in 0..CONCLUSION_BATCH {
        black_box(path.run(index, case));
    }
    start.elapsed().as_secs_f64() * 1e9 / f64::from(CONCLUSION_BATCH)
}

/// Median cost of one request through `path`, in nanoseconds.
fn request_ns(harness: &Harness, path: Path, case: &RequestCase) -> f64 {
    let index = path.index(harness);
    let mut batches: Vec<f64> = (0..CONCLUSION_BATCHES)
        .map(|_| batch_ns(path, index, case))
        .collect();
    batches.sort_by(f64::total_cmp);
    batches[CONCLUSION_BATCHES / 2]
}

/// Hit rate, in percent, at which the prefilter stops costing more than it saves.
///
/// With `h` the hit rate, the per-request change is `h * saving - (1 - h) *
/// overhead`, which is zero at `overhead / (overhead + saving)`. Degenerate
/// measurements (no saving, or no miss overhead) clamp to the boundary so the
/// printed rate stays a percentage.
fn break_even_rate(overhead_ns: f64, saving_ns: f64) -> f64 {
    if saving_ns <= 0.0 {
        // Nothing to win: only a perfect hit rate would break even.
        return 100.0;
    }
    if overhead_ns <= 0.0 {
        // The miss path costs nothing extra: any hit rate wins.
        return 0.0;
    }
    100.0 * overhead_ns / (overhead_ns + saving_ns)
}

/// Print the break-even conclusion: per body size, what a hit saves, what a miss
/// pays, and the hit rate at which the two cancel.
fn print_conclusion(harness: &Harness) {
    println!("prefilter probe: median ns per request by body size");
    println!("  break-even hit rate = miss overhead / (miss overhead + hit saving)");
    for case in &harness.cases {
        let body_len = case.body.len();
        let baseline = request_ns(harness, Path::Sha256Only, case);
        let hit = request_ns(harness, Path::PrefilterHit, case);
        let miss = request_ns(harness, Path::PrefilterMiss, case);
        let saving = baseline - hit;
        let overhead = miss - baseline;
        let rate = break_even_rate(overhead, saving);
        println!(
            "  body_{body_len:<5} sha256_only {baseline:7.1} ns | prefilter_hit {hit:7.1} ns (saves {saving:6.1} ns) \
             | prefilter_miss {miss:7.1} ns (pays {overhead:6.1} ns) | break-even hit rate {rate:5.1}% \
             (minimum hit rate for net benefit; 预筛开始净收益所需的最低命中率)"
        );
    }
    println!(
        "  reading: below the break-even hit rate the prefilter costs more than it saves; above it, the hit \
         path's saved digests pay for the miss path's extra work"
    );
}

fn prefilter_probe_bench(c: &mut Criterion) {
    let harness = build_harness();
    print_conclusion(&harness);

    let mut group = c.benchmark_group("prefilter_probe");
    group.sample_size(40);
    group.measurement_time(Duration::from_secs(2));
    group.warm_up_time(Duration::from_secs(1));

    for case in &harness.cases {
        let body_len = case.body.len();
        for path in Path::ALL {
            let index = path.index(&harness);
            group.bench_function(format!("{}/body_{body_len}", path.label()), |b| {
                b.iter(|| black_box(path.run(index, case)));
            });
        }
    }

    group.finish();
}

criterion_group!(benches, prefilter_probe_bench);
criterion_main!(benches);
