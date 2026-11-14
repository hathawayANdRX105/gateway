//! Rotation behavior tests for the windowed t-digest (`with_clock` fake clock, deterministic
//! reproduction).
//!
//! Pinned behavior of the windowed t-digest:
//! - Queries read only the `prev` window; `cur` samples from the current round stay out of
//!   queries;
//! - On rotation (`now - last_rotate > window`) `prev = merge(prev, cur)` unconditionally;
//! - Empty `prev` in the first window → `quantiles = None`; unreported unit `weight = 0.0`;
//! - `MemoryHealthTable::record` writes twice: with a digest attached, the scalar EWMA and
//!   the digest side coexist.

use gateway_dispatch::health::{AttemptReport, HealthSetting, HealthTable, MemoryHealthTable};
use gateway_dispatch::latency_digest::LatencyDigestTable;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Rotation window (ms), aligned with the default window of `LatencyDigestTable::new`.
const WINDOW_MS: u64 = 30_000;

fn clock_digest(now: &Arc<AtomicU64>) -> Arc<LatencyDigestTable> {
    let c = Arc::clone(now);
    LatencyDigestTable::with_clock(move || c.load(Ordering::Relaxed), WINDOW_MS)
}

/// In the first window `prev` has not rotated in yet → `quantiles = None`; but `weight`
/// (the `cur` side) already sees samples.
#[test]
fn quantiles_empty_in_first_window() {
    let now = Arc::new(AtomicU64::new(1_000_000));
    let d = clock_digest(&now);
    for v in [10u32, 20, 30] {
        d.report("u", v);
    }
    assert_eq!(
        d.quantiles("u"),
        None,
        "prev is empty before first-window rotation"
    );
    assert_eq!(
        d.weight("u"),
        3.0,
        "cur has accumulated 3 samples, cold-start gate visible"
    );
}

/// After rotation `quantiles` reads only `prev` (`cur` samples from this round stay out).
#[test]
fn quantiles_from_prev_window_after_rotation() {
    let now = Arc::new(AtomicU64::new(0));
    let d = clock_digest(&now);
    for _ in 0..10 {
        d.report("u", 100);
    }
    assert_eq!(d.quantiles("u"), None, "first window still has no prev");

    now.store(WINDOW_MS + 1, Ordering::Relaxed);
    d.report("u", 1); // rotation: prev = 10×100; the 1ms enters the new cur, prev untouched

    let (p50, p95) = d.quantiles("u").unwrap();
    assert_eq!(p50, 100.0, "prev is 10×100, p50=100");
    assert_eq!(p95, 100.0);
    // The 1ms in cur is not reflected in prev yet (queries read only the prev window).
    assert_eq!(d.quantiles("u").unwrap().0, 100.0);
}

/// A new spike enters `cur` this round and only reaches `prev` on the next rotation
/// (windowed behavior of the tail-latency signal).
#[test]
fn spike_in_cur_this_round_visible_next_round() {
    let now = Arc::new(AtomicU64::new(0));
    let d = clock_digest(&now);
    for _ in 0..10 {
        d.report("u", 10); // window 1: low values
    }
    now.store(WINDOW_MS + 1, Ordering::Relaxed);
    d.report("u", 10); // rotation: prev = window 1; cur resets
    d.report("u", 5_000); // spike enters cur (this round, not prev yet)

    assert_eq!(
        d.quantiles("u").unwrap().1,
        10.0,
        "spike not yet in prev this round, p95 unchanged"
    );

    now.store(2 * WINDOW_MS + 2, Ordering::Relaxed);
    d.report("u", 10); // rotation: prev = merge(prev, cur) → the spike enters prev
    let (_, p95) = d.quantiles("u").unwrap();
    assert!(
        p95 > 10.0,
        "spike enters prev next round, p95 should rise significantly: {p95}"
    );
}

/// Unreported unit: `quantiles = None`, `weight = 0.0`.
#[test]
fn unreported_unit_quantiles_none_weight_zero() {
    let now = Arc::new(AtomicU64::new(0));
    let d = clock_digest(&now);
    assert_eq!(d.quantiles("ghost"), None);
    assert_eq!(d.weight("ghost"), 0.0);
}

/// With a digest attached, `record(Some(latency))` writes twice: `weight + 1` on the digest
/// side and the scalar EWMA (rollback path) updated in step.
#[test]
fn record_writes_twice_with_digest() {
    let now = Arc::new(AtomicU64::new(1_000_000));
    let digest = clock_digest(&now);
    let table = MemoryHealthTable::with_config_and_clock(HealthSetting::default(), move || {
        now.load(Ordering::Relaxed)
    })
    .with_digest(digest.clone());

    table.record("u", AttemptReport::new(Ok(200), Some(123), None));

    assert_eq!(
        digest.weight("u"),
        1.0,
        "digest side should record 1 sample after record"
    );
    assert_eq!(
        table.get("u").latency_ewma_ms,
        123.0,
        "scalar EWMA (rollback path) updated in step; coexistence, not replaced by digest"
    );
}

/// A default (`digest = None`) `MemoryHealthTable`: `record` only takes the EWMA path, with
/// no digest side effects — byte-for-byte identical to the status quo (rollback path).
#[test]
fn no_digest_behavior_matches_status_quo() {
    let now = Arc::new(AtomicU64::new(1_000_000));
    let table = MemoryHealthTable::with_config_and_clock(HealthSetting::default(), move || {
        now.load(Ordering::Relaxed)
    });

    table.record("u", AttemptReport::new(Ok(200), Some(123), None));
    assert_eq!(table.get("u").latency_ewma_ms, 123.0);
    // No digest attached: there is no separate queryable table — the semantics follow from
    // "the digest field defaults to None and record's `if let (Some, Some)` short-circuits";
    // this assertion guards that the EWMA path is not bypassed.
    assert!(table.get("u").request_count >= 1);
}

/// An old-window spike is diluted away by low samples on the next rotation: `p95` drops
/// markedly. The spike window's `p95` is dominated by the high-value region; a following
/// window of many spread-out low values (`10..=29`) then dilutes the high samples below a 5%
/// share, and the next rotation brings `p95` back down to the low band. The spread-out low
/// values let compression produce several low centroids, making the spike a genuine tail
/// outlier that dilution can wash out.
#[test]
fn old_window_spike_washed_out_by_next_rotation() {
    let now = Arc::new(AtomicU64::new(0));
    let d = clock_digest(&now);
    // `i` is a loop counter below 2_000 in this test, so `u32::try_from` never
    // fails here; the cast only converts the loop counter to u32.
    let low = |i: usize| 10 + (u32::try_from(i).expect("i < 2_000") % 20); // 10..=29

    // window 1: half spread-out low values + half spikes (5000ms) → the spike region
    // dominates p95.
    for i in 0..25 {
        d.report("u", low(i));
    }
    for _ in 0..25 {
        d.report("u", 5_000);
    }
    now.store(WINDOW_MS + 1, Ordering::Relaxed);
    d.report("u", 10); // rotation 1: prev = window 1
    let p95_spike = d.quantiles("u").unwrap().1;
    assert!(
        p95_spike > 4_000.0,
        "spike window p95 dominated by high values: {p95_spike}"
    );

    // window 2: 2000 spread-out low values dilute the spike (high-value share drops below
    // 5%).
    for i in 0..2_000 {
        d.report("u", low(i));
    }
    now.store(2 * WINDOW_MS + 2, Ordering::Relaxed);
    d.report("u", 10); // rotation 2: prev = merge(prev, cur)
    let p95_washed = d.quantiles("u").unwrap().1;
    assert!(
        p95_washed < 100.0,
        "p95 falls back to low band after dilution: {p95_spike} -> {p95_washed}"
    );
    assert!(
        p95_washed < p95_spike,
        "p95 must fall back after rotation dilution"
    );
}

/// Old-vs-new interface parity: `quantiles_with_weight` must match `quantiles` + `weight`
/// taken separately, value for value — the hot path may only change how it takes the lock,
/// never the semantics (including empty prev → None on both sides).
#[test]
fn merged_and_split_interfaces_match_item_by_item() {
    let now = Arc::new(AtomicU64::new(0));
    let d = clock_digest(&now);

    // Unreported unit: both sides None, weight 0.
    assert_eq!(
        d.quantiles_with_weight("ghost"),
        None,
        "unreported → None, consistent with quantiles"
    );
    assert_eq!(d.quantiles("ghost"), None);
    assert_eq!(d.weight("ghost"), 0.0);

    // First window, no rotation yet: prev empty → both sides None, weight (cur side)
    // visible.
    for v in [10u32, 20, 30] {
        d.report("u", v);
    }
    assert_eq!(d.quantiles("u"), None);
    assert_eq!(
        d.quantiles_with_weight("u"),
        None,
        "prev empty → None, same as quantiles"
    );
    assert_eq!(d.weight("u"), 3.0);

    // After rotation: the three values fetched under one lock equal the separate getters
    // item by item.
    now.store(WINDOW_MS + 1, Ordering::Relaxed);
    d.report("u", 100); // rotation: prev = 3 low values; 100 enters the new cur
    let (q, merged, w) = (
        d.quantiles("u"),
        d.quantiles_with_weight("u"),
        d.weight("u"),
    );
    let (Some((p50, p95)), Some((m50, m95, mw))) = (q, merged) else {
        panic!("both sides should be Some after rotation: q={q:?} merged={merged:?}");
    };
    assert_eq!(m50, p50, "p50 matches exactly");
    assert_eq!(m95, p95, "p95 matches exactly");
    assert_eq!(mw, w, "weight matches separate weight() exactly");

    // Report without rotation (goes only into cur): quantiles unchanged, weight +1 on both
    // sides.
    d.report("u", 5_000);
    let (q, merged, w) = (
        d.quantiles("u"),
        d.quantiles_with_weight("u"),
        d.weight("u"),
    );
    assert_eq!((q, w), (Some((p50, p95)), 5.0));
    assert_eq!(
        merged,
        Some((p50, p95, 5.0)),
        "no rotation: quantiles unchanged, weight grows on both sides"
    );
}
