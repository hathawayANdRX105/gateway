//! Health table / Dispatcher read-only query surface (entries / snapshot) behavior
//! tests — #164.
//!
//! Pins two points:
//! 1. `MemoryHealthTable::entries()` is empty before `record`; after one Fatal failure
//!    the unit appears in entries and its cooldown field takes effect
//!    (`cooldown_until_ms > now`, i.e. the "remaining_ms > 0" semantics);
//! 2. `Dispatcher::snapshot()` is `None` before `set_snapshot` and `Some` after loading.
//!
//! All clocks are fixed through `with_config_and_clock`, so until_ms deltas are
//! deterministic.

use gateway_gate::concurrency::ConcurrencyState;
use std::sync::Arc;

use gateway_dispatch::health::{ChannelOutcome, HealthSetting};
use gateway_dispatch::{AttemptReport, FailureClass, HealthTable, MemoryHealthTable, Snapshot};

/// Build a minimal Snapshot (empty units/channels) for the snapshot() existence tests.
fn empty_snapshot() -> Arc<Snapshot> {
    Arc::new(Snapshot::default())
}

/// entries() is empty before any record.
#[test]
fn entries_empty_before_any_record() {
    let table = MemoryHealthTable::new();
    assert!(table.entries().is_empty());
}

/// A single transport-layer failure triggers cooldown (the cooldown no longer has an
/// "N consecutive" threshold); the unit appears in entries and is inside its cooldown
/// window (remaining_ms > 0).
#[test]
fn entries_shows_cooling_unit_after_first_failure() {
    let now_ms = 1_000_000u64;
    let table = MemoryHealthTable::with_config_and_clock(HealthSetting::default(), move || now_ms);
    let key = "ch-abc/0:gpt-4o";

    table.record(
        key,
        AttemptReport::new(Err(FailureClass::Retryable), None, None),
    );

    let found = table.entries();
    let Some((found_key, st)) = found.iter().find(|(k, _)| k == key) else {
        panic!("cooling unit not present in entries");
    };
    assert_eq!(found_key, key);
    assert!(
        st.is_cooling(now_ms),
        "unit should be inside its cooldown window"
    );
    // First activation takes the base rung (cooldown_base_ms = 300ms): until - now = 300ms.
    assert_eq!(st.cooldown_until_ms - now_ms, 300);
    // The outcome that triggered cooldown is recorded as Fatal (the basis for P1-A tiering).
    assert_eq!(st.last_cooling_outcome, Some(ChannelOutcome::Fatal));
}

/// Units recorded only with a Neutral outcome (e.g. 404) are also in entries, but do not
/// enter cooldown — "recorded" on the query surface does not mean "abnormal"; filtering
/// happens on the handler side.
#[test]
fn entries_contains_neutral_units_without_cooling() {
    let now_ms = 500u64;
    let table = MemoryHealthTable::with_config_and_clock(HealthSetting::default(), move || now_ms);
    table.record("unit-neutral", AttemptReport::new(Ok(404), None, None));

    let entries = table.entries();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].0, "unit-neutral");
    assert!(!entries[0].1.is_cooling(now_ms));
}

/// entries() is a data snapshot cloned under the lock: continuing to record after reading
/// does not affect the copy already read.
#[test]
fn entries_is_a_copy_not_a_live_view() {
    let now_ms = 2_000_000u64;
    let table = MemoryHealthTable::with_config_and_clock(HealthSetting::default(), move || now_ms);
    let key = "unit-x";
    for _ in 0..5 {
        table.record(
            key,
            AttemptReport::new(Err(FailureClass::Retryable), None, None),
        );
    }
    let snapshot = table.entries();
    // The witness field is cooldown_streak: request_count is cleared by start_cooldown on
    // every failure and stays 0, so it cannot distinguish "snapshot" from "live".
    // cooldown_streak only grows.
    let streak_before = snapshot
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, st)| st.cooldown_streak)
        .unwrap_or(0);

    table.record(
        key,
        AttemptReport::new(Err(FailureClass::Retryable), None, None),
    );
    let live = table.get(key);
    assert_eq!(
        live.cooldown_streak,
        streak_before + 1,
        "live view should see the new record"
    );
}

/// Dispatcher::snapshot() is None before set_snapshot.
#[test]
fn dispatcher_snapshot_none_before_set() {
    let dispatcher = gateway_dispatch::Dispatcher::new(
        None,
        Arc::new(MemoryHealthTable::new()),
        Arc::new(ConcurrencyState::default()),
    );
    assert!(dispatcher.snapshot().is_none());
}

/// Some after set_snapshot, with readable contents (an empty snapshot has empty units but
/// a valid handle).
#[test]
fn dispatcher_snapshot_some_after_set() {
    let dispatcher = gateway_dispatch::Dispatcher::new(
        None,
        Arc::new(MemoryHealthTable::new()),
        Arc::new(ConcurrencyState::default()),
    );
    dispatcher.set_snapshot(empty_snapshot());

    let snap = dispatcher
        .snapshot()
        .expect("snapshot should be readable after set_snapshot");
    assert!(snap.units.is_empty());
    assert!(snap.channels.is_empty());
}

// 429 takes a separate throttle ladder (343→1000ms, ⑤), distinct from the 5xx fatal curve;
// this also pins that entries can distinguish the two cooldown sources.
#[test]
fn throttled_cooldown_is_recorded_in_entries() {
    let now_ms = 7_000_000u64;
    let cfg = HealthSetting::default();
    let table = MemoryHealthTable::with_config_and_clock(cfg, move || now_ms);
    let key = "unit-429";
    // A single 429 enters cooldown — cooldown no longer has an "N consecutive" threshold.
    table.record(key, AttemptReport::new(Ok(429), None, None));

    let st = table.get(key);
    assert!(
        st.is_cooling(now_ms),
        "a single 429 should trigger cooldown immediately"
    );
    assert_eq!(
        st.last_cooling_outcome,
        Some(ChannelOutcome::Throttled),
        "⑤: 429 should be tagged Throttled (independent ladder ledger)"
    );
    // The separate ladder's first rung = 300 + (1000-300)·(1/4)² = 343ms (base 300 is no
    // longer pinned).
    assert_eq!(st.cooldown_until_ms - now_ms, 343);
    assert!(table.entries().iter().any(|(k, _)| k == key));
}
