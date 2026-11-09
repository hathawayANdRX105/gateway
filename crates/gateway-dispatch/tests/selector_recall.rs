//! Exhausted-pool emergency recall: when every enabled, non-excluded channel is
//! cooling down, pick recalls the channel with the shortest remaining cooldown
//! instead of letting the caller report an unavailable error.

use gateway_core::records::{RouteUnitRecord, SyncMeta};
use gateway_dispatch::STATUS_ENABLED;
use gateway_dispatch::health::{AttemptReport, HealthSetting, HealthTable, MemoryHealthTable};
use gateway_dispatch::selector::{DEFAULT_EXPLORE_RATE, P2CSelector, Selector};
use gateway_gate::concurrency::ConcurrencyState;
use rand::SeedableRng;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

const T0: u64 = 1_000_000;

fn clock_table(now: &Arc<AtomicU64>) -> MemoryHealthTable {
    let clock = {
        let now = now.clone();
        move || now.load(Ordering::Relaxed)
    };
    MemoryHealthTable::with_config_and_clock(HealthSetting::default(), clock)
}

fn unit(key: &str, weight: u32, status: u8) -> RouteUnitRecord {
    RouteUnitRecord {
        meta: SyncMeta {
            key: key.to_string(),
            schema_version: 1,
            logical_version: 1,
            origin: "test".to_string(),
            updated_at: chrono::Utc::now(),
        },
        group: "default".to_string(),
        public_model: "gpt-4o".to_string(),
        channel_key: key.to_string(),
        key_index: 0,
        upstream_model: "gpt-4o".to_string(),
        weight,
        status,
    }
}

fn pick<'a>(
    units: &[&'a RouteUnitRecord],
    health: &dyn HealthTable,
    exclude: &[String],
    sticky_channel_key: Option<&str>,
    now_ms: u64,
) -> Option<&'a RouteUnitRecord> {
    let mut rng = rand::rngs::StdRng::seed_from_u64(42);
    let selector = P2CSelector::new(
        Arc::new(ConcurrencyState::default()),
        2,
        DEFAULT_EXPLORE_RATE,
    );
    selector.pick(units, health, exclude, sticky_channel_key, now_ms, &mut rng)
}

/// Both channels are cooling: a has ~0.3s left, b ~1.1s → recall a, and a becomes
/// selectable again.
#[test]
fn exhausted_pool_recalls_shortest_cooldown() {
    let now = Arc::new(AtomicU64::new(T0));
    let table = clock_table(&now);

    // Cooldown has no threshold; each failure climbs one rung. Power-law ladder
    // (n/N)^2:
    //   rung 3 = 300 + 7700·(3/12)² = 781ms
    //   rung 5 = 300 + 7700·(5/12)² = 1636ms
    // a accrues 4 failures → rung 3; b accrues 6 → rung 5. Both deadlines are at T0+…
    for _ in 0..4 {
        table.record("a", AttemptReport::new(Ok(500), None, None));
    }
    for _ in 0..6 {
        table.record("b", AttemptReport::new(Ok(500), None, None));
    }

    // Read at a moment when both are still cooling: a has 781-500=281ms left; b has
    // 1636-500=1136ms left.
    let now_ms = T0 + 500;
    now.store(now_ms, Ordering::Relaxed);
    assert_eq!(table.cooling_remaining_ms("a", now_ms), Some(281));
    let b_left = table
        .cooling_remaining_ms("b", now_ms)
        .expect("b should be cooling");
    assert_eq!(b_left, 1_136, "b remaining cooldown should be about 1.14s");
    assert!(
        b_left > 281,
        "b's remaining cooldown must exceed a's, otherwise recall direction is reversed"
    );

    let units = [
        unit("a", 100, STATUS_ENABLED),
        unit("b", 100, STATUS_ENABLED),
    ];
    let refs: Vec<&RouteUnitRecord> = units.iter().collect();
    let picked = pick(&refs, &table, &[], None, now_ms)
        .expect("exhausted pool should recall the shortest-cooldown channel");
    assert_eq!(
        picked.meta.key, "a",
        "should recall a, the channel with the shortest remaining cooldown"
    );

    // Recall = cooldown ends now (ramp armed), but the streak is kept.
    assert!(
        table.is_selectable("a", now_ms),
        "after recall, a should be selectable"
    );
    assert!(
        !table.is_selectable("b", now_ms),
        "b is still cooling, not recalled"
    );
    let st = table.get("a");
    assert_eq!(
        st.cooldown_until_ms, 0,
        "recall clears the cooldown deadline"
    );
    assert_eq!(
        st.cooldown_streak, 4,
        "recall is not forgiveness: cooldown streak retained"
    );
    assert!(st.ramp_pending, "recall should arm the slow-start ramp");
}

/// Every enabled channel is in the same request's exclude set → no candidates → None.
#[test]
fn all_excluded_pool_returns_none() {
    let now = Arc::new(AtomicU64::new(T0));
    let table = clock_table(&now);
    for key in ["a", "b"] {
        for _ in 0..5 {
            table.record(key, AttemptReport::new(Ok(429), None, None));
        }
    }

    let units = [
        unit("a", 100, STATUS_ENABLED),
        unit("b", 100, STATUS_ENABLED),
    ];
    let refs: Vec<&RouteUnitRecord> = units.iter().collect();
    let exclude = ["a".to_string(), "b".to_string()];
    assert!(
        pick(&refs, &table, &exclude, None, T0).is_none(),
        "all enabled channels in exclude set -> return None",
    );
}

/// Disabled channels take no part in recall: the pool holds only cooling disabled
/// channels → None.
#[test]
fn disabled_units_are_not_recalled() {
    let now = Arc::new(AtomicU64::new(T0));
    let table = clock_table(&now);
    for _ in 0..5 {
        table.record("a", AttemptReport::new(Ok(429), None, None));
    }

    let units = [unit("a", 100, 0)];
    let refs: Vec<&RouteUnitRecord> = units.iter().collect();
    assert!(
        pick(&refs, &table, &[], None, T0).is_none(),
        "disabled channels must not be recalled"
    );
    assert!(
        table.cooling_remaining_ms("a", T0).is_some(),
        "recall did not happen: a is still cooling"
    );
}
