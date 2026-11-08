//! Idle annealing after a cooldown expires: cooldown_streak decays linearly in base windows
//! (cooldown_base_ms), and an active success report resets the annealing origin. Everything goes
//! through the public MemoryHealthTable API + an injected clock.

use gateway_dispatch::health::{
    AttemptReport, FailureClass, HealthSetting, HealthTable, MemoryHealthTable,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Default config: cooldown_base_ms = 300 → annealing window 300ms.
const T0: u64 = 1_000_000;
const WINDOW_MS: u64 = 300;

fn clock_table(now: &Arc<AtomicU64>) -> MemoryHealthTable {
    let clock = {
        let now = now.clone();
        move || now.load(Ordering::Relaxed)
    };
    MemoryHealthTable::with_config_and_clock(HealthSetting::default(), clock)
}

/// Accumulate two cooldown rounds (cooldown_streak 1 → 2) and return the second round's expiry
/// instant + 1.
/// After the caller advances the clock to that instant and triggers lazy settlement, the
/// annealing anchor is in place.
fn wind_to_second_cooldown(now: &Arc<AtomicU64>, table: &MemoryHealthTable, key: &str) -> u64 {
    // Cooldown has no threshold: one transport-layer failure activates a round immediately,
    // streak 0 → 1, landing on the base rung of 300ms.
    table.record(
        key,
        AttemptReport::new(Err(FailureClass::Retryable), None, None),
    );
    let expiry1 = table.get(key).cooldown_until_ms;
    now.store(expiry1 + 1, Ordering::Relaxed);
    // Failing once more after expiry → streak=2, and the duration climbs to the second rung of
    // the power law, 353ms.
    // The annealing window keeps reusing base (cooldown_base_ms).
    table.record(
        key,
        AttemptReport::new(Err(FailureClass::Retryable), None, None),
    );
    table.get(key).cooldown_until_ms + 1
}

#[test]
fn idle_after_cooldown_anneals_streak_to_zero() {
    let now = Arc::new(AtomicU64::new(T0));
    let table = clock_table(&now);
    let key = "ch-a/0:gpt-4o";

    let anchor = wind_to_second_cooldown(&now, &table, key);

    // Cooldown expiry: is_selectable settles lazily → finish_cooldown sets the annealing anchor
    // and the streak is preserved.
    now.store(anchor, Ordering::Relaxed);
    assert!(table.is_selectable(key, anchor));
    assert_eq!(
        table.get(key).cooldown_streak,
        2,
        "streak retained before annealing"
    );
    assert_eq!(
        table.get(key).anneal_since_ms,
        anchor,
        "anchor = cooldown end time",
    );

    // Half a window: no decay (only full windows count).
    let half = anchor + WINDOW_MS / 2;
    now.store(half, Ordering::Relaxed);
    assert!(table.is_selectable(key, half));
    assert_eq!(
        table.get(key).cooldown_streak,
        2,
        "no decay before a full window"
    );

    // Advance to 2 full windows: 2 → 0.
    let full = anchor + 2 * WINDOW_MS;
    now.store(full, Ordering::Relaxed);
    assert!(table.is_selectable(key, full));
    assert_eq!(
        table.get(key).cooldown_streak,
        0,
        "streak should decay to 0 after advancing 2 base windows",
    );
}

#[test]
fn success_resets_anneal_anchor() {
    let now = Arc::new(AtomicU64::new(T0));
    let table = clock_table(&now);
    let key = "ch-a/0:gpt-4o";

    let anchor = wind_to_second_cooldown(&now, &table, key);
    now.store(anchor, Ordering::Relaxed);
    assert!(table.is_selectable(key, anchor));

    // After advancing one window, one success report: the Success branch drops the streak 2 → 1
    // and clears the anchor.
    now.store(anchor + WINDOW_MS, Ordering::Relaxed);
    table.record(key, AttemptReport::new(Ok(200), None, None));
    let st = table.get(key);
    assert_eq!(st.cooldown_streak, 1, "success decrements cooldown streak");
    assert_eq!(st.anneal_since_ms, 0, "active event resets annealing start");

    // With the anchor at 0, annealing sleeps: advancing 2 more windows leaves the streak at 1
    // (active traffic keeps decrementing it itself; annealing waits for the next cooldown end to
    // set a new anchor).
    let later = anchor + 3 * WINDOW_MS;
    now.store(later, Ordering::Relaxed);
    assert!(table.is_selectable(key, later));
    assert_eq!(table.get(key).cooldown_streak, 1);
}
