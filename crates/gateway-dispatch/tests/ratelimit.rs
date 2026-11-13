//! Sliding-window rate-limit parsing and gating — parse boundaries / window sliding / per-key
//! isolation / Dispatcher integration.

use gateway_core::records::{
    ChannelKey, ChannelRecord, ChannelSettings, RouteUnitRecord, SyncMeta,
};
use gateway_dispatch::Dispatch;
use gateway_dispatch::health::MemoryHealthTable;
use gateway_dispatch::ratelimit::{RateLimitSpec, SlidingWindow};
use gateway_gate::concurrency::ConcurrencyState;
use std::collections::HashMap;
use std::sync::Arc;

// ---------- Parse boundaries ----------

#[test]
fn parse_rejects_malformed_and_overflow() {
    // Malformed input must yield None, never admit (missing config = unrestricted, wrong config =
    // reject)
    for bad in [
        "",       // empty
        "100",    // missing /
        "100/",   // missing unit
        "/50s",   // missing request count
        "100/50", // missing unit character
        "100/0s", // zero multiplier
        "0/50s",  // zero requests
        "100/5x", // unknown unit
        " 100/s", // whitespace
        "100/s ", // trailing whitespace
    ] {
        assert_eq!(RateLimitSpec::parse(bad), None, "should reject: {bad:?}");
    }
    // u64 overflow: parse fails → None (checked_mul semantics)
    assert_eq!(RateLimitSpec::parse("18446744073709551616/1s"), None);
    assert_eq!(RateLimitSpec::parse("100/18446744073709551616s"), None);
}

#[test]
fn stale_buckets_pruned_after_two_hours() {
    // Buckets older than 2h must be pruned by retain — otherwise memory for long-tail keys grows
    // without bound. Verify pruning by inspecting bucket_count, not just behavior (sliding-window
    // counting ignores out-of-window buckets anyway, so behavior cannot observe memory
    // reclamation).
    let now = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let clock = {
        let now = now.clone();
        move || now.load(std::sync::atomic::Ordering::Relaxed)
    };
    let rl = SlidingWindow::with_clock(clock);
    let spec = RateLimitSpec {
        requests: 1000,
        window_ms: 60_000,
    };

    rl.admits("k", Some(&spec), 0);
    assert_eq!(rl.bucket_count("k"), 1);

    // Advance 2h+1s: the same admits call prunes old buckets before counting
    now.store(2 * 3_600_000 + 1_000, std::sync::atomic::Ordering::Relaxed);
    assert!(rl.admits(
        "k",
        Some(&spec),
        now.load(std::sync::atomic::Ordering::Relaxed)
    ));
    assert_eq!(
        rl.bucket_count("k"),
        1,
        "old buckets from 2h ago should be cleaned, only current-second bucket remains"
    );
}

// ---------- Parse boundaries ----------

#[test]
fn parse_basic_units() {
    assert_eq!(
        RateLimitSpec::parse("100/s"),
        Some(RateLimitSpec {
            requests: 100,
            window_ms: 1_000
        })
    );
    assert_eq!(
        RateLimitSpec::parse("200/5m"),
        Some(RateLimitSpec {
            requests: 200,
            window_ms: 300_000
        })
    );
    assert_eq!(
        RateLimitSpec::parse("10000/d"),
        Some(RateLimitSpec {
            requests: 10_000,
            window_ms: 86_400_000
        })
    );
    assert_eq!(
        RateLimitSpec::parse("50/h"),
        Some(RateLimitSpec {
            requests: 50,
            window_ms: 3_600_000
        })
    );
}

#[test]
fn parse_invalid_returns_none() {
    // empty
    assert_eq!(RateLimitSpec::parse(""), None);
    // missing slash
    assert_eq!(RateLimitSpec::parse("100s"), None);
    // extra trailing characters
    assert_eq!(RateLimitSpec::parse("100/sx"), None);
    // invalid unit
    assert_eq!(RateLimitSpec::parse("100/w"), None);
    // invalid characters
    assert_eq!(RateLimitSpec::parse("abc/s"), None);
    // requests == 0
    assert_eq!(RateLimitSpec::parse("0/s"), None);
    // multiplier == 0 (an empty multiplier means 1; 0 is not allowed)
    assert_eq!(RateLimitSpec::parse("100/0s"), None);
    // unit only
    assert_eq!(RateLimitSpec::parse("/s"), None);
}

// ---------- Window sliding ----------

#[test]
fn admits_respects_window_slide() {
    let rl = SlidingWindow::new();
    let spec = RateLimitSpec {
        requests: 60,
        window_ms: 60_000,
    }; // 60 req / 60s

    // 60 requests in the same second → all admitted (a single second-bucket does not split)
    let t0 = 1_000_000u64;
    for i in 0..60 {
        assert!(
            rl.admits("k", Some(&spec), t0 + i),
            "request {} should be allowed",
            i + 1
        );
    }
    // The 61st request in the same second → that second's bucket already holds 60 → rejected
    assert!(!rl.admits("k", Some(&spec), t0 + 60));

    // Advance past the window: t = t0 + 61s → all earlier buckets fall outside [now-60s, now] →
    // recovery
    let t1 = t0 + 61_000;
    assert!(rl.admits("k", Some(&spec), t1));
}

#[test]
fn admits_none_spec_always_allows() {
    let rl = SlidingWindow::new();
    for _ in 0..10_000 {
        assert!(rl.admits("k", None, 0));
    }
}

// ---------- Per-key isolation ----------

#[test]
fn keys_are_isolated() {
    let rl = SlidingWindow::new();
    let spec = RateLimitSpec {
        requests: 5,
        window_ms: 60_000,
    };

    // k1 consumes all 5
    for _ in 0..5 {
        assert!(rl.admits("k1", Some(&spec), 1_000));
    }
    assert!(!rl.admits("k1", Some(&spec), 1_000));

    // k2 is completely unaffected
    for _ in 0..5 {
        assert!(rl.admits("k2", Some(&spec), 1_000));
    }
    assert!(!rl.admits("k2", Some(&spec), 1_000));
}

// ---------- Dispatcher integration ----------

fn unit(key: &str, status: u8) -> RouteUnitRecord {
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
        status,
    }
}

fn channel(key: &str) -> ChannelRecord {
    ChannelRecord {
        meta: SyncMeta {
            key: key.to_string(),
            schema_version: 1,
            logical_version: 1,
            origin: "test".to_string(),
            updated_at: chrono::Utc::now(),
        },
        name: key.to_string(),
        provider_type: "openai".to_string(),
        base_url: "https://upstream.example".to_string(),
        keys: vec![ChannelKey {
            index: 0,
            secret: "sk-secret".to_string(),
            rpm_limit: 0,
        }],
        max_concurrency: 10,
        status: 1,
        groups: vec!["default".to_string()],
        settings: ChannelSettings::default(),
    }
}

fn build_snapshot(
    units: Vec<RouteUnitRecord>,
    channels: Vec<&str>,
) -> Arc<gateway_dispatch::Snapshot> {
    let mut ch_map = HashMap::new();
    for k in channels {
        ch_map.insert(k.to_string(), channel(k));
    }
    Arc::new(gateway_dispatch::Snapshot {
        units,
        channels: ch_map,
    })
}

#[test]
fn dispatcher_without_limits_keeps_legacy_behavior() {
    // Consistent with the existing dispatcher_select_resolves_full_candidate — behavior is
    // unchanged when limits are not configured
    let units = vec![unit("ch1", 1)];
    let snap = build_snapshot(units, vec!["ch1"]);
    let dispatcher = gateway_dispatch::Dispatcher::new(
        Some(snap),
        Arc::new(MemoryHealthTable::new()),
        Arc::new(ConcurrencyState::default()),
    );
    let c = dispatcher.select("g", "m", &[], None, None).unwrap();
    assert_eq!(c.secret, "sk-secret");
}

#[test]
fn dispatcher_rate_limited_when_all_candidates_throttled() {
    // Both units are limited to 1 req / 60s; consume each of the two keys once,
    // then all are over the limit → RateLimited (429 distinguishes it from 503 NoCandidate).
    let units = vec![unit("ch1", 1), unit("ch2", 1)];
    let snap = build_snapshot(units.clone(), vec!["ch1", "ch2"]);

    let rl = Arc::new(SlidingWindow::new());
    let mut limits = HashMap::new();
    limits.insert(
        "ch1".to_string(),
        RateLimitSpec {
            requests: 1,
            window_ms: 60_000,
        },
    );
    limits.insert(
        "ch2".to_string(),
        RateLimitSpec {
            requests: 1,
            window_ms: 60_000,
        },
    );

    let dispatcher = gateway_dispatch::Dispatcher::with_limits(
        Some(snap),
        Arc::new(MemoryHealthTable::new()),
        Arc::new(ConcurrencyState::default()),
        Arc::new(limits),
        rl,
    );

    // Consume both keys: each select round consumes exactly the key it picks (a throttled pick is
    // swapped out)
    let _ = dispatcher.select("g", "m", &[], None, None).unwrap();
    let _ = dispatcher.select("g", "m", &[], None, None).unwrap();

    // Third call — both keys are over the limit → RateLimited
    let err = dispatcher.select("g", "m", &[], None, None).unwrap_err();
    match err {
        gateway_dispatch::DispatchError::RateLimited { group, model } => {
            assert_eq!(group, "g");
            assert_eq!(model, "m");
        }
        other => panic!("expected RateLimited, got {other:?}"),
    }
}

#[test]
fn dispatcher_only_consumes_picked_unit_quota() {
    // Regression: accounting means +1 only for the selected channel (wildtoken runProxyAttempts),
    // never consuming quota of unselected candidates. Both units are limited to 1/60s — after the
    // first pick, the second must switch to the other unconsumed key instead of returning
    // RateLimited.
    let units = vec![unit("ch1", 1), unit("ch2", 1)];
    let snap = build_snapshot(units.clone(), vec!["ch1", "ch2"]);

    let rl = Arc::new(SlidingWindow::new());
    let mut limits = HashMap::new();
    limits.insert(
        "ch1".to_string(),
        RateLimitSpec {
            requests: 1,
            window_ms: 60_000,
        },
    );
    limits.insert(
        "ch2".to_string(),
        RateLimitSpec {
            requests: 1,
            window_ms: 60_000,
        },
    );

    let dispatcher = gateway_dispatch::Dispatcher::with_limits(
        Some(snap),
        Arc::new(MemoryHealthTable::new()),
        Arc::new(ConcurrencyState::default()),
        Arc::new(limits),
        rl,
    );

    // Both the first and second calls must succeed — the second switches candidates instead of
    // retrying the same key
    let _ = dispatcher.select("g", "m", &[], None, None).unwrap();
    let _ = dispatcher.select("g", "m", &[], None, None).unwrap();
}

#[test]
fn dispatcher_rate_limited_with_per_unit_limits() {
    // Unit a is limited to 1 req / 60s, unit b is unrestricted. The first call selects a (RL
    // allows); on the second request a is throttled but b can still pass, so b is returned instead
    // of RateLimited.
    let units = vec![unit("a", 1), unit("b", 1)];
    let snap = build_snapshot(units.clone(), vec!["a", "b"]);

    let rl = Arc::new(SlidingWindow::new());
    let mut limits = HashMap::new();
    limits.insert(
        "a".to_string(),
        RateLimitSpec {
            requests: 1,
            window_ms: 60_000,
        },
    );

    let dispatcher = gateway_dispatch::Dispatcher::with_limits(
        Some(snap),
        Arc::new(MemoryHealthTable::new()),
        Arc::new(ConcurrencyState::default()),
        Arc::new(limits),
        rl,
    );

    // First call: either one may be selected — a or b is fine
    let _ = dispatcher.select("g", "m", &[], None, None).unwrap();
    // Second call: a is over the limit but b can still pass → it must succeed
    let _ = dispatcher.select("g", "m", &[], None, None).unwrap();
}

#[test]
fn dispatcher_no_candidate_when_snapshot_empty_unchanged() {
    // The original candidate set is empty with no fallback → keep ModelUnknown (genuinely no
    // model), not replaced by RateLimited
    let snap = build_snapshot(vec![], vec![]);
    let mut limits = HashMap::new();
    limits.insert(
        "any".to_string(),
        RateLimitSpec {
            requests: 1,
            window_ms: 60_000,
        },
    );
    let dispatcher = gateway_dispatch::Dispatcher::with_limits(
        Some(snap),
        Arc::new(MemoryHealthTable::new()),
        Arc::new(ConcurrencyState::default()),
        Arc::new(limits),
        Arc::new(SlidingWindow::new()),
    );
    assert!(matches!(
        dispatcher.select("g", "m", &[], None, None),
        Err(gateway_dispatch::DispatchError::ModelUnknown { .. })
    ));
}

// ---------- Weighted accounting / k/M magnitude suffixes ----------

#[test]
fn weighted_cumulative_sum_rejects_later_arrivals() {
    // Limit 100, 30 recorded per request → 30/60/90 are admitted in turn; the 4th
    // (90+30=120>100) is rejected on the strict side. If weighted accounting degraded to "+1 per
    // request" (treating weight as 1), four 30s would accumulate to just 4 < 100 and all would
    // pass → red.
    let rl = SlidingWindow::new();
    let spec = RateLimitSpec {
        requests: 100,
        window_ms: 60_000,
    };
    let now = 1_000_000u64;
    for i in 0..3 {
        assert!(
            rl.admits_weighted("k", Some(&spec), 30, now),
            "weighted request {} should be allowed",
            i + 1
        );
    }
    assert!(
        !rl.admits_weighted("k", Some(&spec), 30, now),
        "accumulated 90 + 30 = 120 > 100, the 4th should be rejected"
    );
}

#[test]
fn weight_exceeding_limit_is_strictly_rejected() {
    // A single weight 150 > limit 100: with an empty window current+weight>limit, so the strict
    // side rejects without accounting. If the check degraded to "reject only when current >= limit"
    // (omitting this request's weight), the empty window would admit it → red.
    let rl = SlidingWindow::new();
    let spec = RateLimitSpec {
        requests: 100,
        window_ms: 60_000,
    };
    let now = 1_000_000u64;
    assert!(
        !rl.admits_weighted("k", Some(&spec), 150, now),
        "weight 150 > limit 100 must be strictly rejected"
    );
    // A rejected request is not accounted: a following request exactly at the limit (weight=100)
    // still passes.
    assert!(rl.admits_weighted("k", Some(&spec), 100, now));
    // Now 100 is accumulated; one more weight=1 (100+1>100) is rejected on the strict side.
    assert!(!rl.admits_weighted("k", Some(&spec), 1, now));
}

#[test]
fn weight_zero_is_free_and_does_not_account() {
    // weight==0 ⇒ a cost-free request: admitted with no accounting at all (not even a
    // second-bucket). If 0 were accounted, buckets filled with 0 would wrongly reject, or
    // bucket_count would not be 0 → red.
    let rl = SlidingWindow::new();
    let spec = RateLimitSpec {
        requests: 2,
        window_ms: 60_000,
    };
    for _ in 0..1_000 {
        assert!(rl.admits_weighted("k", Some(&spec), 0, 1_000_000));
    }
    assert_eq!(
        rl.bucket_count("k"),
        0,
        "weight=0 must not create a per-second bucket"
    );
}

#[test]
fn parse_k_m_suffix_units() {
    // Magnitude suffixes k=1e3, M=1e6: 200k/1m = 200_000 tokens / 60s.
    assert_eq!(
        RateLimitSpec::parse("200k/1m"),
        Some(RateLimitSpec {
            requests: 200_000,
            window_ms: 60_000
        })
    );
    assert_eq!(
        RateLimitSpec::parse("1M/5m"),
        Some(RateLimitSpec {
            requests: 1_000_000,
            window_ms: 300_000
        })
    );
    // The old suffix-less syntax still works (regression): suffix support must not break `100/1m`.
    assert_eq!(
        RateLimitSpec::parse("100/1m"),
        Some(RateLimitSpec {
            requests: 100,
            window_ms: 60_000
        })
    );
}

#[test]
fn parse_zero_magnitude_suffix_is_none() {
    // 0k / 0M reduce to 0 → requests==0 → None (same as 0/s). A missed case would treat a
    // "0 limit" as unrestricted → red.
    assert_eq!(RateLimitSpec::parse("0k/1m"), None);
    assert_eq!(RateLimitSpec::parse("0M/1s"), None);
}

#[test]
fn admits_equivalent_to_admits_weighted_one() {
    // admits is a thin delegation to admits_weighted(weight=1); both must decide identically for
    // the same sequence. If admits did not delegate (still hard-coded on the old counting path),
    // the weighted and request-count paths would diverge → red.
    let a = SlidingWindow::new();
    let b = SlidingWindow::new();
    let spec = RateLimitSpec {
        requests: 4,
        window_ms: 60_000,
    };
    let now = 2_000_000u64;
    for i in 0..6u64 {
        assert_eq!(
            a.admits("k", Some(&spec), now + i),
            b.admits_weighted("k", Some(&spec), 1, now + i),
            "admits and admits_weighted(1) at {i} must agree",
        );
    }
}
