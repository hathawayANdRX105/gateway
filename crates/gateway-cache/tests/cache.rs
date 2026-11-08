//! `gateway-cache` behavior tests: bounded capacity (LRU), TTL expiry, idle
//! reclamation, weighted byte bounds, key isolation, and the response cache's
//! store/lookup + oversized/byte-budget guards. Pure in-memory, real clock with
//! generous margins; CI runs them directly.

use std::time::Duration;

use bytes::Bytes;
use gateway_cache::{
    CacheScope, CachedResponse, CachedUsage, ResponseCache, TtlCache, response_key,
};

fn entry(status: u16, body: &[u8], channel: &str) -> CachedResponse {
    CachedResponse {
        status,
        body: Bytes::copy_from_slice(body),
        usage: CachedUsage {
            prompt_tokens: 10,
            completion_tokens: 20,
            cached_tokens: 0,
            ..Default::default()
        },
        channel_key: channel.to_string(),
        unit_key: format!("unit-{channel}"),
        public_model: "gpt-4o".to_string(),
        upstream_model: "gpt-4o-2024".to_string(),
        group: "default".to_string(),
    }
}

fn scope(user: &str, group: &str) -> CacheScope {
    CacheScope {
        user_key: user.to_string(),
        group: group.to_string(),
    }
}

// ---------- TtlCache: capacity (LRU) ----------

/// Capacity is a hard bound: the (capacity+1)th insert evicts the least
/// recently used entry. Bug it catches: an unbounded map behind a "cache" name
/// (the original sticky table / client caches grew without limit).
#[test]
fn capacity_evicts_least_recently_used_entry() {
    let cache: TtlCache<String, u32> = TtlCache::bounded(3);
    cache.insert("a".into(), 1);
    cache.insert("b".into(), 2);
    cache.insert("c".into(), 3);
    assert_eq!(cache.len(), 3);

    cache.insert("d".into(), 4);
    assert_eq!(cache.len(), 3, "capacity must hold after evict + insert");
    assert_eq!(cache.get(&"d".into()), Some(4), "the fresh key is admitted");
    assert_eq!(
        cache.get(&"a".into()),
        None,
        "the first-inserted, never-read entry is the LRU victim"
    );
    assert_eq!(cache.get(&"b".into()), Some(2));
    assert_eq!(cache.get(&"c".into()), Some(3));
}

/// After an overflow insert and the maintenance pass, the fresh key is admitted
/// and the bound holds even with interleaved reads. Bug it catches: a store
/// that admits everything (bound not enforced) or starves fresh keys when the
/// table is hot. The victim *identity* under interleaved reads is the store's
/// policy detail (moka applies queued reads on maintenance): not pinned here —
/// `capacity_evicts_least_recently_used_entry` pins the no-reads case, and the
/// TTI test pins the read-refreshes-idle-clock contract.
#[test]
fn fresh_key_admitted_under_interleaved_reads() {
    let cache: TtlCache<String, u32> = TtlCache::bounded(3);
    cache.insert("a".into(), 1);
    cache.insert("b".into(), 2);
    let _ = cache.get(&"a".into());
    cache.insert("c".into(), 3);
    cache.insert("d".into(), 4);
    assert_eq!(cache.len(), 3, "bound holds after maintenance");
    assert_eq!(cache.get(&"d".into()), Some(4), "the fresh key is admitted");
}

// ---------- TtlCache: TTL ----------

/// Expired entries miss on read even while still in the map. Bug it catches:
/// treating the store-level TTL as "always live until evicted" — a stale
/// sticky binding or client config would be served.
#[test]
fn ttl_expired_entry_misses_on_read() {
    let cache: TtlCache<String, u32> = TtlCache::bounded_with_ttl(10, Duration::from_millis(50));
    cache.insert("k".into(), 1);
    assert_eq!(cache.get(&"k".into()), Some(1), "live within TTL");

    std::thread::sleep(Duration::from_millis(120));
    assert_eq!(cache.get(&"k".into()), None, "expired entries must miss");
}

/// Re-insert slides the TTL (measured from the last write). Bug it catches:
/// keeping the original expiry on overwrite — a long session would go stale
/// halfway through.
#[test]
fn reinsert_slides_ttl() {
    let cache: TtlCache<String, u32> = TtlCache::bounded_with_ttl(10, Duration::from_millis(100));
    cache.insert("k".into(), 1);
    std::thread::sleep(Duration::from_millis(60));
    cache.insert("k".into(), 2);
    std::thread::sleep(Duration::from_millis(60));
    assert_eq!(
        cache.get(&"k".into()),
        Some(2),
        "120ms after the first insert but only 60ms after the second: sliding TTL holds"
    );
}

// ---------- TtlCache: weighted size ----------

/// The weigher bounds total weight, not entry count: the second 600-byte value
/// exceeds a 1000-byte budget and evicts the first. Bug it catches: bounding
/// the response cache by entry count — one 1 MB answer among thousands of
/// small ones would blow the memory bound. (Eviction is lazy: `len()` forces
/// the maintenance pass that applies it.)
#[test]
fn weighted_bound_evicts_by_total_weight() {
    let cache: TtlCache<String, Vec<u8>> =
        TtlCache::bounded_weighted_with_ttl(1000, Duration::from_secs(60), |_, v: &Vec<u8>| {
            u32::try_from(v.len()).unwrap_or(u32::MAX)
        });
    cache.insert("big1".into(), vec![0u8; 600]);
    cache.insert("big2".into(), vec![0u8; 600]);
    let _ = cache.len(); // force maintenance: apply the pending eviction
    assert_eq!(
        cache.get(&"big1".into()),
        None,
        "first insert is the LRU victim once the total weight exceeds the bound"
    );
    assert_eq!(cache.get(&"big2".into()), Some(vec![0u8; 600]));
    assert!(
        cache.len() <= 1,
        "weight bound respected, got {}",
        cache.len()
    );
}

// ---------- TtlCache: idle (time-to-idle) ----------

/// Idle reclamation drops entries unused past the idle window; a read refreshes
/// the clock so an actively used entry never expires. Bug it catches: the old
/// client caches held every stale (node, fingerprint) config forever — idle is
/// what makes the bound reclaim rather than just cap.
#[test]
fn idle_reclaims_unused_entries_and_reads_refresh() {
    let cache: TtlCache<String, u32> = TtlCache::bounded_with_idle(10, Duration::from_millis(100));
    cache.insert("k".into(), 1);
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(
        cache.get(&"k".into()),
        Some(1),
        "read refreshes the idle clock"
    );
    std::thread::sleep(Duration::from_millis(60));
    assert_eq!(
        cache.get(&"k".into()),
        Some(1),
        "60ms after the refresh, still inside the 100ms idle window"
    );
    std::thread::sleep(Duration::from_millis(150));
    assert_eq!(cache.get(&"k".into()), None, "unused past the idle window");
}

// ---------- TtlCache: stats + invalidate ----------

/// Hit/miss counters and explicit invalidation. Bug it catches: silent counter
/// drift (hits recorded on misses or vice versa) — the counters are the only
/// observability into whether a cache earns its memory.
#[test]
fn stats_and_invalidate() {
    let cache: TtlCache<String, u32> = TtlCache::bounded(10);
    let _ = cache.get(&"absent".into());
    cache.insert("k".into(), 1);
    let _ = cache.get(&"k".into());
    let _ = cache.get(&"k".into());
    let stats = cache.stats();
    assert_eq!(stats.hits, 2);
    assert_eq!(stats.misses, 1);

    cache.invalidate(&"k".into());
    assert_eq!(cache.get(&"k".into()), None);
    assert!(cache.is_empty());
    assert_eq!(cache.stats().misses, 2);
}

// ---------- response_key: isolation ----------

/// Same inputs → same key (the write side and the read side must derive the
/// identical key or the cache never hits). Bug it catches: any non-determinism
/// (random salt, map iteration order, unstable serialization) in the key.
#[test]
fn key_is_stable_for_identical_requests() {
    let body = b"{\"model\":\"gpt-4o\",\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}";
    let k1 = response_key(&scope("u-1", "default"), "gpt-4o", "openai", body);
    let k2 = response_key(&scope("u-1", "default"), "gpt-4o", "openai", body);
    assert_eq!(k1, k2, "identical requests share one entry");
}

/// The scope (user + group) isolates entries: a cached answer is never replayed
/// across users, groups, or model price classes. Bug it catches: a user-scoped
/// leak (user A reading user B's answer) — the worst possible cache bug on a
/// billing plane.
#[test]
fn key_changes_with_scope_model_protocol_and_body() {
    let body_a = b"{\"prompt\":\"a\"}";
    let base = response_key(&scope("u-1", "default"), "gpt-4o", "openai", body_a);

    assert_ne!(
        response_key(&scope("u-2", "default"), "gpt-4o", "openai", body_a),
        base,
        "another user never shares an entry"
    );
    assert_ne!(
        response_key(&scope("u-1", "premium"), "gpt-4o", "openai", body_a),
        base,
        "another group (price multiplier / allowlist) never shares"
    );
    assert_ne!(
        response_key(&scope("u-1", "default"), "gpt-4o-mini", "openai", body_a),
        base,
        "another model never shares"
    );
    assert_ne!(
        response_key(&scope("u-1", "default"), "gpt-4o", "anthropic", body_a),
        base,
        "another inbound protocol (different body shape) never shares"
    );
    assert_ne!(
        response_key(
            &scope("u-1", "default"),
            "gpt-4o",
            "openai",
            b"{\"prompt\":\"b\"}"
        ),
        base,
        "a different body is a different request"
    );
}

/// Length-delimited component boundaries: `("ab", "c")` and `("a", "bc")` style
/// concatenation must not collide. Bug it catches: joining components with a
/// plain concat — a client-controlled model name could bridge into the body
/// boundary and replay another request's answer.
#[test]
fn key_component_boundaries_do_not_concatenate() {
    // ("user" = "u-1", model = "23gpt-4o") vs ("user" = "u-123", model = "gpt-4o"):
    // identical under naive concatenation of user_key + model.
    let k1 = response_key(&scope("u-1", "default"), "23gpt-4o", "openai", b"body");
    let k2 = response_key(&scope("u-123", "default"), "gpt-4o", "openai", b"body");
    assert_ne!(k1, k2, "length prefixes keep component boundaries intact");
}

// ---------- ResponseCache: store / lookup / guards ----------

/// Store → lookup roundtrip replays the same body and attribution. Bug it
/// catches: losing any replay field (status / body / counts / channel) would
/// make the replay bill or behave differently from the original call.
#[test]
fn store_then_lookup_roundtrips_entry() {
    let cache = ResponseCache::with_limits(Duration::from_secs(60), 64 * 1024 * 1024, 1024 * 1024);
    let key = response_key(&scope("u-1", "default"), "gpt-4o", "openai", b"req");
    cache.store(key.clone(), entry(200, b"{\"answer\":true}", "ch-a"));

    let hit = cache.lookup(&key).expect("stored entry must hit");
    assert_eq!(hit.status, 200);
    assert_eq!(hit.body.as_ref(), b"{\"answer\":true}");
    assert_eq!(hit.usage.prompt_tokens, 10);
    assert_eq!(hit.usage.completion_tokens, 20);
    assert_eq!(hit.channel_key, "ch-a");
    assert_eq!(hit.unit_key, "unit-ch-a");
    assert_eq!(hit.public_model, "gpt-4o");
    assert_eq!(hit.upstream_model, "gpt-4o-2024");
    assert_eq!(hit.group, "default");
}

/// Entries above the per-entry limit are refused at store time. Bug it
/// catches: one huge answer dominating the whole byte budget for one slot.
#[test]
fn oversized_body_is_refused() {
    let cache = ResponseCache::with_limits(Duration::from_secs(60), 64 * 1024 * 1024, 100);
    let key = response_key(&scope("u-1", "default"), "gpt-4o", "openai", b"req");
    cache.store(key.clone(), entry(200, &[0u8; 101], "ch-a"));
    assert!(
        cache.lookup(&key).is_none(),
        "oversized bodies are never stored"
    );
}

/// The total byte budget bounds stored bodies: the second 1500-byte answer
/// exceeds the 2000-byte budget and evicts the first (LRU). Bug it catches:
/// the memory bound not being enforced at all. (`len()` forces the maintenance
/// pass that applies the pending eviction.)
#[test]
fn total_byte_budget_evicts_oldest_entry() {
    let cache = ResponseCache::with_limits(Duration::from_secs(60), 2000, 10_000);
    let k1 = response_key(&scope("u-1", "default"), "gpt-4o", "openai", b"req-1");
    let k2 = response_key(&scope("u-1", "default"), "gpt-4o", "openai", b"req-2");
    cache.store(k1.clone(), entry(200, &[0u8; 1500], "ch-a"));
    cache.store(k2.clone(), entry(200, &[0u8; 1500], "ch-b"));
    assert_eq!(cache.len(), 1, "byte budget holds after maintenance");

    assert!(
        cache.lookup(&k1).is_none(),
        "first answer is the LRU victim"
    );
    assert!(
        cache.lookup(&k2).is_some(),
        "second answer is admitted within budget"
    );
}

/// Expired entries miss on lookup. Bug it catches: replaying an answer past
/// its TTL — the TTL is the freshness contract, not an eviction hint.
#[test]
fn expired_entry_misses_on_lookup() {
    let cache =
        ResponseCache::with_limits(Duration::from_millis(50), 64 * 1024 * 1024, 1024 * 1024);
    let key = response_key(&scope("u-1", "default"), "gpt-4o", "openai", b"req");
    cache.store(key.clone(), entry(200, b"{}", "ch-a"));
    assert!(cache.lookup(&key).is_some());

    std::thread::sleep(Duration::from_millis(120));
    assert!(
        cache.lookup(&key).is_none(),
        "expired answers must not replay"
    );
}

/// Hit/miss counters surface the cache's value. Bug it catches: counter drift
/// making a broken cache look healthy (or a healthy one look dead).
#[test]
fn response_cache_counts_hits_and_misses() {
    let cache = ResponseCache::with_limits(Duration::from_secs(60), 64 * 1024 * 1024, 1024 * 1024);
    let key = response_key(&scope("u-1", "default"), "gpt-4o", "openai", b"req");
    let _ = cache.lookup(&key);
    cache.store(key.clone(), entry(200, b"{}", "ch-a"));
    let _ = cache.lookup(&key);

    let stats = cache.stats();
    assert_eq!(stats.hits, 1);
    assert_eq!(stats.misses, 1);
}
