//! Criterion benchmark for the hash-candidate comparison.
//!
//! Answers one question: for the byte layout `response_key` actually hashes, how do
//! SHA-256 and the plausible alternatives compare on throughput, and which of them
//! keep a stable, collision-safe 32/16/8-byte fingerprint?
//!
//! Inputs come from `support/hash_candidates.rs`, the same module the consistency
//! test uses, so speed is always measured over the exact bytes the key semantics
//! define. Throughput is reported in bytes per second, which is the only unit that
//! stays comparable across candidates and across machines.
//!
//! `is_cross_process_stable() == false` candidates (ahash/foldhash) are benchmarked
//! for reference only; they cannot serve as a shared cache key because their output
//! depends on build/platform.

#[path = "support/hash_candidates.rs"]
mod support;

use std::hint::black_box;
use std::time::Duration;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use support::{Algorithm, BODY_SIZES, RequestCase, corpus};

/// One encoded request per body size, reused across candidates so the comparison is
/// byte-for-byte identical.
fn cases() -> Vec<(usize, Vec<u8>)> {
    let samples = corpus(BODY_SIZES.len());
    samples
        .iter()
        .map(|case: &RequestCase| (case.body.len(), case.encode()))
        .collect()
}

fn hash_candidates_bench(c: &mut Criterion) {
    let inputs = cases();

    let mut group = c.benchmark_group("hash_candidates");
    group.sample_size(40);
    group.measurement_time(Duration::from_secs(2));
    group.warm_up_time(Duration::from_secs(1));

    for (body_len, encoded) in &inputs {
        // Report MB/s: the metric that does not change with machine class.
        group.throughput(Throughput::Bytes(encoded.len() as u64));
        for algorithm in Algorithm::ALL {
            group.bench_function(format!("{}/body_{body_len}", algorithm.label()), |b| {
                b.iter(|| {
                    let digest = algorithm.digest(black_box(encoded));
                    black_box(digest)
                });
            });
        }
    }

    group.finish();
}

criterion_group!(benches, hash_candidates_bench);
criterion_main!(benches);
