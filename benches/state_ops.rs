//! Dispatch state-primitive microbenchmarks: the in-memory state each `select` touches between
//! candidate scoring and the final pick.
//!
//! `selector_latency.rs` measures the whole `P2CSelector::pick` chain; this file isolates the
//! individual state primitives that chain reads and writes on every request, so their own cost
//! is visible without selector/health noise:
//! - `SlidingWindow::admits_weighted` — per-unit rate limit (mutex + bucket sum + account);
//! - `StickyTable::lookup` — prefix-cache sticky binding hit (index + TTL check);
//! - `Tdigest::add` / `Tdigest::quantile` — streaming p50/p95/p99 ingestion and query;
//! - `LatencyDigestTable::quantiles_with_weight` — the O(1) per-unit hot-path query;
//! - `TokenBudgetState::admit_window` — minute token-budget window check.
//!
//! These sit on the synchronous select path for every forwarded request. Every clock is injected
//! and fixed so window rotation and TTL decisions are deterministic, and every input/result is
//! black-boxed so the compiler cannot elide the measured work. Local only, not run in CI.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use gateway_dispatch::latency_digest::LatencyDigestTable;
use gateway_dispatch::ratelimit::{RateLimitSpec, SlidingWindow};
use gateway_dispatch::sticky::{StickyKey, StickyTable};
use gateway_dispatch::tdigest::Tdigest;
use gateway_dispatch::token_budget::TokenBudgetState;

const FIXED_NOW_MS: u64 = 1_700_000_000_000;
const RATE_WINDOW_MS: u64 = 60_000;
const LATENCY_WINDOW_MS: u64 = 30_000;
const TDIGEST_DELTA: f64 = 100.0;
// Large enough that repeated calls never flip from the account branch to the reject branch
// mid-measurement; that keeps the measured work constant across iterations.
const RATE_LIMIT_REQUESTS: u64 = 1_000_000_000;
const TOKEN_CAP: u64 = 1_000_000_000;

/// Build a fixed-clock rate limiter with a never-saturating spec and window.
fn rate_limit_harness() -> (SlidingWindow, RateLimitSpec) {
    (
        SlidingWindow::with_clock(|| FIXED_NOW_MS),
        RateLimitSpec {
            requests: RATE_LIMIT_REQUESTS,
            window_ms: RATE_WINDOW_MS,
        },
    )
}

/// Pre-bind session keys on a fixed-clock table so `lookup` always hits a live entry (the TTL
/// never elapses inside the measurement).
fn sticky_harness() -> (Arc<StickyTable>, Vec<StickyKey>, u64) {
    let table = StickyTable::with_clock(|| FIXED_NOW_MS, 300_000, 1024);
    let keys: Vec<StickyKey> = (0..128u32)
        .map(|i| StickyKey {
            public_model: "model".to_string(),
            prefix: format!("sid:session-{i}"),
        })
        .collect();
    for key in &keys {
        table.bind(key.clone(), "channel-a");
    }
    let now = table.now_ms();
    (table, keys, now)
}

/// Deterministic sample values built with `f64::from` (no lossy casts): a repeating spread.
fn sample_values(count: u32) -> Vec<f64> {
    (0..count)
        .map(|i| f64::from(i % 500) * 2.0 + f64::from(i % 7))
        .collect()
}

/// A warmed digest holding a few thousand samples so `quantile` scans a realistic centroid set.
fn filled_digest() -> Tdigest {
    let mut digest = Tdigest::new(TDIGEST_DELTA);
    for value in sample_values(10_000) {
        digest.add(value);
    }
    digest
}

/// Fill two completed windows on a fixed clock so `prev` is non-empty and queries return `Some`.
fn latency_table_harness() -> Arc<LatencyDigestTable> {
    let clock = Arc::new(AtomicU64::new(FIXED_NOW_MS));
    let table = LatencyDigestTable::with_clock(
        {
            let clock = Arc::clone(&clock);
            move || clock.load(Ordering::Relaxed)
        },
        LATENCY_WINDOW_MS,
    );
    for k in 0..100u32 {
        table.report("channel-a", 20 + (k % 50));
    }
    clock.store(FIXED_NOW_MS + LATENCY_WINDOW_MS + 1, Ordering::Relaxed);
    for k in 0..100u32 {
        table.report("channel-a", 15 + (k % 40));
    }
    table
}

/// A fixed-clock token budget with a large per-minute cap so `admit_window` keeps taking the
/// account branch instead of the cheap uncapped/reject branches.
fn token_budget_harness() -> Arc<TokenBudgetState> {
    let state = TokenBudgetState::with_clock(|| FIXED_NOW_MS);
    state.register_channel("channel-a", TOKEN_CAP);
    state
}

fn state_ops_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("state_ops");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(2));
    group.warm_up_time(Duration::from_secs(1));

    group.bench_function("sliding_window_admits_weighted", |b| {
        let (window, spec) = rate_limit_harness();
        b.iter(|| {
            let admitted = window.admits_weighted(
                black_box("channel-a"),
                black_box(Some(&spec)),
                black_box(1),
                black_box(FIXED_NOW_MS),
            );
            black_box(admitted)
        });
    });

    group.bench_function("sticky_lookup", |b| {
        let (table, keys, now) = sticky_harness();
        b.iter(|| black_box(table.lookup(black_box(&keys[0]), black_box(now))));
    });

    group.bench_function("tdigest_add", |b| {
        let values = sample_values(256);
        let mut digest = Tdigest::new(TDIGEST_DELTA);
        for value in &values {
            digest.add(*value);
        }
        let mut i = 0usize;
        b.iter(|| {
            let value = values[i % values.len()];
            i = i.wrapping_add(1);
            digest.add(black_box(value));
            black_box(digest.count())
        });
    });

    group.bench_function("tdigest_quantile", |b| {
        let digest = filled_digest();
        b.iter(|| black_box(digest.quantile(black_box(0.99))));
    });

    group.bench_function("latency_digest_quantiles_with_weight", |b| {
        let table = latency_table_harness();
        b.iter(|| black_box(table.quantiles_with_weight(black_box("channel-a"))));
    });

    group.bench_function("token_budget_admit_window", |b| {
        let state = token_budget_harness();
        b.iter(|| black_box(state.admit_window(black_box("channel-a"), black_box(16))));
    });

    group.finish();
}

criterion_group!(bench, state_ops_bench);
criterion_main!(bench);
