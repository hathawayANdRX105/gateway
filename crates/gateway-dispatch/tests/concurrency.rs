//! Concurrency behavior — Mutex-protected shared state neither panics nor exceeds its
//! limits under multiple threads.
//!
//! Covers: `SlidingWindow` concurrent accounting (the admitted count must be ≤ the limit —
//! Mutex atomicity), `MemoryHealthTable` concurrent record/get (no panic, readable state,
//! eventually consistent).

use gateway_dispatch::health::{AttemptReport, FailureClass, HealthTable, MemoryHealthTable};
use gateway_dispatch::ratelimit::{RateLimitSpec, SlidingWindow};
use gateway_gate::concurrency::ConcurrencyState;
use std::collections::HashMap;
use std::sync::Arc;

#[test]
fn sliding_window_concurrent_admits_never_exceeds_limit() {
    // 8 threads × 200 requests hammering the same key concurrently (limit 100/1s,
    // fixed now → same one-second bucket): with the Mutex serializing them, the
    // admitted count must be exactly ≤ 100 — an accounting leak or a double admit
    // shows up immediately under multiple threads.
    let rl = Arc::new(SlidingWindow::new());
    let spec = RateLimitSpec {
        requests: 100,
        window_ms: 1_000,
    };
    let now = 1_000_000u64; // fixed clock, everything lands in the same one-second bucket

    let mut handles = Vec::new();
    for _ in 0..8 {
        let rl = Arc::clone(&rl);
        handles.push(std::thread::spawn(move || {
            let mut allowed = 0;
            for _ in 0..200 {
                if rl.admits("shared", Some(&spec), now) {
                    allowed += 1;
                }
            }
            allowed
        }));
    }

    let total: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
    assert!(
        total <= 100,
        "Concurrent accounting exceeds limit: {total} admitted (limit 100)"
    );
    // With a single fixed bucket the theory says exactly 100; the conservative ≤ 100
    // assertion guards against implementation-detail changes
    assert_eq!(
        total, 100,
        "Same-second bucket within limit: after concurrency exactly 100 should be admitted"
    );
}

#[test]
fn health_table_concurrent_record_get_no_panic() {
    // 8 threads × 1000 mixed records each (success/failure interleaved, 16 shared keys):
    // verifies no panic under the Mutex (poisoning is absorbed by unwrap_or_else → state
    // stays consistent), and that the failure-streak statistics accumulate correctly
    // across threads.
    let table = Arc::new(MemoryHealthTable::new());

    let mut handles = Vec::new();
    for _ in 0..8 {
        let table = Arc::clone(&table);
        handles.push(std::thread::spawn(move || {
            for i in 0..1000 {
                let key = format!("k{}", i % 16);
                let outcome = if i % 3 == 0 {
                    Err(FailureClass::Retryable)
                } else {
                    Ok(200)
                };
                table.record(&key, AttemptReport::new(outcome, None, None));
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    // No panic is the main assertion; additionally: the state is readable and has
    // observations
    let st = table.get("k0");
    assert!(
        st.request_count > 0,
        "After concurrent record there should be an observation count"
    );
    assert!(
        st.ewma_score > 0.0 && st.ewma_score <= 1.0,
        "Health score should be in (0,1]",
    );
    // Under concurrent interleaving, failures for the same key may arrive consecutively
    // (across threads), so whether cooldown trips is scheduling-dependent — not asserted
    // here; the core concurrency-safety assertions are: no panic + readable state +
    // bounded health score. Breaker behavior is covered by the single-threaded test
    // (fatal_streak_trips...).
}

#[test]
fn health_table_concurrent_streak_trips_breaker() {
    // Multiple threads fail the same key repeatedly: any single failure starts a cooldown
    // (there is no N-consecutive threshold), so 30 failures arriving across threads must
    // leave the key cooling — and cooldown_streak must accumulate correctly across
    // threads under the lock.
    let table = Arc::new(MemoryHealthTable::new());

    let mut handles = Vec::new();
    for _ in 0..3 {
        let table = Arc::clone(&table);
        handles.push(std::thread::spawn(move || {
            for _ in 0..10 {
                table.record(
                    "hot",
                    AttemptReport::new(Err(FailureClass::Retryable), None, None),
                );
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    let st = table.get("hot");
    assert!(
        st.cooldown_until_ms > 0,
        "30 concurrent failures should trip breaker (streak accumulates across threads)",
    );
    // 30 concurrent failures trip cooldown on the first one; cooldown resets
    // request_count=0, so assert the cooldown and streak result
    assert!(
        st.cooldown_streak > 0,
        "30 concurrent failures should trigger cooldown (streak accumulates)",
    );
}

// ---------- Lock-free path concurrent reads: Dispatcher select and health reads ----------

#[test]
fn dispatcher_concurrent_select_returns_valid_candidate() {
    use gateway_core::records::{ChannelKey, ChannelRecord, RouteUnitRecord, SyncMeta};
    use gateway_dispatch::Dispatch;

    fn unit(key: &str) -> RouteUnitRecord {
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
            weight: 10,
            status: 1,
        }
    }

    let units: Vec<RouteUnitRecord> = (0..8).map(|i| unit(&format!("ch{i}"))).collect();
    let channels: HashMap<String, ChannelRecord> = units
        .iter()
        .map(|u| {
            (
                u.meta.key.clone(),
                ChannelRecord {
                    meta: u.meta.clone(),
                    name: u.meta.key.clone(),
                    provider_type: "openai".to_string(),
                    base_url: "https://u".to_string(),
                    keys: vec![ChannelKey {
                        index: 0,
                        secret: "s".to_string(),
                        rpm_limit: 0,
                    }],
                    max_concurrency: 10,
                    status: 1,
                    groups: vec!["g".to_string()],
                    settings: serde_json::Value::Null,
                },
            )
        })
        .collect();

    let dispatcher = Arc::new(gateway_dispatch::Dispatcher::new(
        Some(Arc::new(gateway_dispatch::Snapshot { units, channels })),
        Arc::new(MemoryHealthTable::new()),
        Arc::new(ConcurrencyState::default()),
    ));

    // 8 threads selecting concurrently: every call must successfully return a
    // forwardable candidate (snapshot is read-only + health table lock-protected, so no
    // panic and no empty-handed return)
    let mut handles = Vec::new();
    for _ in 0..8 {
        let d = Arc::clone(&dispatcher);
        handles.push(std::thread::spawn(move || {
            for _ in 0..200 {
                let c = d.select("g", "m", &[], None).unwrap();
                assert_ne!(c.secret, "");
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
}
