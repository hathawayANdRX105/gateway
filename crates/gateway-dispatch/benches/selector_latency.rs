//! P2C latency-factor benchmark (design §3.4): EWMA baseline vs mounted t-digest, running
//! the full `Selector::pick` chain over the same 100-candidate pool and reporting p99.
//!
//! Acceptance threshold: select acquires one lock per candidate to collect p50/p95/weight
//! (O(log δ)), and p99 over the 100-candidate pool must stay below 100µs.
//! Not in CI (CI only runs `just test-fast`) — run it locally once and report the measured
//! p99 numbers for the record; warn-level manual review.
//!
//! Both configurations share the same 100-candidate pool (equal weights, full health,
//! non-empty concurrency slots):
//! - `ewma_baseline`: no digest mounted; `latency_factor` takes the EWMA scalar branch
//!   (status quo).
//! - `digest_mounted`: mounts `LatencyDigestTable` (both windows pre-filled so quantiles hit).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use app_contract::records::{RouteUnitRecord, SyncMeta};
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use gateway_dispatch::STATUS_ENABLED;
use gateway_dispatch::health::{AttemptReport, HealthTable, MemoryHealthTable};
use gateway_dispatch::latency_digest::LatencyDigestTable;
use gateway_dispatch::selector::{DEFAULT_EXPLORE_RATE, P2CSelector, Selector};
use gateway_gate::concurrency::ConcurrencyState;
use rand::SeedableRng;

const N: u32 = 100;
/// Rotation window (ms), aligned with the `LatencyDigestTable::new` default window.
const WINDOW_MS: u64 = 30_000;

fn unit_i(i: u32) -> RouteUnitRecord {
    let key = format!("u{i}");
    RouteUnitRecord {
        meta: SyncMeta {
            key: key.clone(),
            schema_version: 1,
            logical_version: 1,
            origin: "bench".to_string(),
            updated_at: chrono::Utc::now(),
        },
        group: "g".to_string(),
        public_model: "m".to_string(),
        channel_key: key,
        key_index: 0,
        upstream_model: "m".to_string(),
        weight: 10,
        status: STATUS_ENABLED,
    }
}

struct Harness {
    units: Vec<RouteUnitRecord>,
    health: Arc<MemoryHealthTable>,
    sel_ewma: P2CSelector,
    sel_digest: P2CSelector,
}

fn build_harness() -> Harness {
    let units: Vec<RouteUnitRecord> = (0..N).map(unit_i).collect();

    // Health table: record 10 successes with varying latency per channel, filling the EWMA
    // baseline and the warm gate.
    let health = Arc::new(MemoryHealthTable::new());
    for u in &units {
        for k in 0..10u32 {
            health.record(
                &u.meta.key,
                AttemptReport::new(Ok(200), Some(20 + k * 5), None),
            );
        }
    }

    // Concurrency slots non-empty: register a concurrency limit per channel so
    // available_permits returns Some.
    let concurrency = Arc::new(ConcurrencyState::default());
    for u in &units {
        concurrency.register_channel(&u.channel_key, 8);
    }

    // digest: pre-fill two windows so quantiles hit (prev absorbs the window-1 samples;
    // queries read prev).
    let now = Arc::new(AtomicU64::new(0));
    let digest = LatencyDigestTable::with_clock(
        {
            let c = Arc::clone(&now);
            move || c.load(Ordering::Relaxed)
        },
        WINDOW_MS,
    );
    // Window 1 (cur accumulates 50 samples).
    now.store(0, Ordering::Relaxed);
    for u in &units {
        for k in 0..50u32 {
            digest.report(&u.meta.key, 20 + k % 30);
        }
    }
    // Window 2 (advance the clock to trigger rotation: prev absorbs window 1, cur receives
    // the window-2 samples).
    now.store(WINDOW_MS + 1, Ordering::Relaxed);
    for u in &units {
        for k in 0..50u32 {
            digest.report(&u.meta.key, 15 + k % 40);
        }
    }

    let sel_ewma = P2CSelector::new(Arc::clone(&concurrency), 2, DEFAULT_EXPLORE_RATE);
    let sel_digest = P2CSelector::new(concurrency, 2, DEFAULT_EXPLORE_RATE)
        .with_latency_digest(Arc::clone(&digest));

    Harness {
        units,
        health,
        sel_ewma,
        sel_digest,
    }
}

/// Manual p99 measurement (µs): after warm-up, run `pick` `iters` times and take the 99th
/// percentile of the elapsed time.
fn measure_p99_us(sel: &P2CSelector, h: &Harness, seed: u64) -> f64 {
    let refs: Vec<&RouteUnitRecord> = h.units.iter().collect();
    let iters = 20_000usize;
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    // Warm-up: amortize first allocations, branch prediction, and cold lock startup.
    for _ in 0..200 {
        let _ = sel.pick(&refs, &*h.health, &[], None, 0, &mut rng);
    }
    let mut samples: Vec<f64> = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t0 = Instant::now();
        let picked = sel.pick(&refs, &*h.health, &[], None, 0, &mut rng);
        samples.push(t0.elapsed().as_secs_f64() * 1e6);
        black_box(picked);
    }
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    samples[iters * 99 / 100]
}

fn selector_latency_bench(c: &mut Criterion) {
    let h = build_harness();
    let refs: Vec<&RouteUnitRecord> = h.units.iter().collect();

    // p99 threshold measurement (< 100µs, warn-level manual review, not in CI).
    let p99_ewma = measure_p99_us(&h.sel_ewma, &h, 11);
    let p99_digest = measure_p99_us(&h.sel_digest, &h, 12);
    println!(
        "p99 ewma_baseline = {p99_ewma:8.3} µs  (threshold < 100µs {})",
        if p99_ewma < 100.0 { "PASS" } else { "FAIL" }
    );
    println!(
        "p99 digest_mounted = {p99_digest:8.3} µs  (threshold < 100µs {})",
        if p99_digest < 100.0 { "PASS" } else { "FAIL" }
    );

    let mut group = c.benchmark_group("selector_latency");
    group.sample_size(100);
    group.measurement_time(Duration::from_secs(2));
    group.warm_up_time(Duration::from_secs(1));

    group.bench_function("pick_ewma_baseline", |b| {
        let mut rng = rand::rngs::StdRng::seed_from_u64(11);
        b.iter(|| {
            let picked = h.sel_ewma.pick(&refs, &*h.health, &[], None, 0, &mut rng);
            black_box(picked)
        });
    });

    group.bench_function("pick_digest_mounted", |b| {
        let mut rng = rand::rngs::StdRng::seed_from_u64(12);
        b.iter(|| {
            let picked = h.sel_digest.pick(&refs, &*h.health, &[], None, 0, &mut rng);
            black_box(picked)
        });
    });
    group.finish();
}

criterion_group!(bench, selector_latency_bench);
criterion_main!(bench);
