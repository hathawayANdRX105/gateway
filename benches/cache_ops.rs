//! Criterion micro-benchmarks for `gateway-cache` hot paths.
//!
//! `response_key` runs once per cacheable request, `TtlCache::get` / `TtlCache::insert`
//! are the shared bounded-store primitives every gateway cache sits on, and
//! `ResponseCache::store` / `ResponseCache::lookup` are the data-plane entry points
//! for replaying an upstream answer. Their per-call cost directly bounds replay
//! throughput, so each is measured against prepared data rather than cold structures.
//! The lookup is split into miss / hit, and the full hit path (`response_key` +
//! lookup) is measured on its own: a hit pays the complete key derivation, which
//! is the cost any cheaper key pre-screen has to be weighed against.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use criterion::{BatchSize, Criterion, black_box, criterion_group, criterion_main};
use gateway_cache::{
    CacheKey, CacheScope, CachedResponse, CachedUsage, ResponseCache, TtlCache, response_key,
};

const N: usize = 256;
const TTL_CAPACITY: u64 = 4096;
// Body sizes shared with hash_candidates/prefilter_probe, so key-derivation costs
// are comparable across benches. Padding is applied to reach each target size.
const BODY_SIZES: [(&str, usize); 4] = [
    ("512b", 512),
    ("4kb", 4 * 1024),
    ("16kb", 16 * 1024),
    ("32kb", 32 * 1024),
];

struct Harness {
    scope: CacheScope,
    model: String,
    protocol: String,
    bodies: Vec<(&'static str, Vec<u8>)>,
    ttl: TtlCache<String, u32>,
    ttl_keys: Vec<String>,
    insert_keys: Vec<String>,
    cache: Arc<ResponseCache>,
    keys: Vec<CacheKey>,
    responses: Vec<CachedResponse>,
    /// Keys derived from bodies that were never stored, so every lookup on them
    /// is a guaranteed miss (the bench measures the miss path, never a
    /// population side effect).
    miss_keys: Vec<CacheKey>,
}

fn bench_scope() -> CacheScope {
    CacheScope {
        user_key: "u-1".to_string(),
        group: "default".to_string(),
    }
}

/// Build a JSON request body of exactly `total_len` bytes.
fn request_body(total_len: usize) -> Vec<u8> {
    let mut body = br#"{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}]}"#.to_vec();
    body.resize(total_len, b' ');
    body
}

fn cached_response(body: &[u8], index: usize) -> CachedResponse {
    CachedResponse {
        status: 200,
        body: Bytes::copy_from_slice(body),
        usage: CachedUsage::default(),
        channel_key: format!("ch-{index}"),
        unit_key: format!("unit-{index}"),
        public_model: "gpt-4o".to_string(),
        upstream_model: "gpt-4o-2024".to_string(),
        group: "default".to_string(),
    }
}

fn build_harness() -> Harness {
    let scope = bench_scope();
    let model = "gpt-4o".to_string();
    let protocol = "openai".to_string();
    let bodies: Vec<(&'static str, Vec<u8>)> = BODY_SIZES
        .iter()
        .map(|(name, size)| (*name, request_body(*size)))
        .collect();
    let body = bodies
        .last()
        .expect("body sizes are non-empty")
        .1
        .as_slice();

    let ttl: TtlCache<String, u32> = TtlCache::bounded(TTL_CAPACITY);
    let ttl_keys: Vec<String> = (0..N).map(|i| format!("key-{i}")).collect();
    let insert_keys: Vec<String> = (0..N).map(|i| format!("new-{i}")).collect();
    for key in &ttl_keys {
        ttl.insert(key.clone(), 7);
    }

    let keys: Vec<CacheKey> = (0..N)
        .map(|i| response_key(&scope, &model, &protocol, format!("req-{i}").as_bytes()))
        .collect();
    let responses: Vec<CachedResponse> = (0..N).map(|i| cached_response(body, i)).collect();

    let cache = ResponseCache::with_limits(Duration::from_secs(300), 64 * 1024 * 1024, 1024 * 1024);
    for (key, response) in keys.iter().zip(&responses) {
        cache.store(key.clone(), response.clone());
    }
    // One realistic payload resident so the hit-path bench derives a key that is
    // actually cached: a hit re-derives the key from the request bytes on every
    // call, so the benched body must be one that was stored.
    cache.store(
        response_key(&scope, &model, &protocol, body),
        cached_response(body, N),
    );
    // Never-stored bodies: a lookup on these keys always misses, so the miss
    // bench measures the miss path with no population side effect.
    let miss_keys: Vec<CacheKey> = (0..N)
        .map(|i| response_key(&scope, &model, &protocol, format!("absent-{i}").as_bytes()))
        .collect();

    Harness {
        scope,
        model,
        protocol,
        bodies,
        ttl,
        ttl_keys,
        insert_keys,
        cache,
        keys,
        responses,
        miss_keys,
    }
}

fn cache_ops_bench(c: &mut Criterion) {
    let h = build_harness();
    let mut group = c.benchmark_group("cache_ops");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(2));
    group.warm_up_time(Duration::from_secs(1));

    for (name, body) in &h.bodies {
        group.bench_function(format!("response_key_{name}"), |b| {
            b.iter(|| {
                let key = response_key(
                    &h.scope,
                    h.model.as_str(),
                    h.protocol.as_str(),
                    body.as_slice(),
                );
                black_box(key)
            });
        });
    }

    group.bench_function("ttl_get_hit", |b| {
        let mut i = 0usize;
        b.iter(|| {
            let key = &h.ttl_keys[i];
            i = i.wrapping_add(1) % h.ttl_keys.len();
            let value = h.ttl.get(key);
            black_box(value)
        });
    });

    group.bench_function("ttl_insert", |b| {
        let mut i = 0usize;
        b.iter_batched(
            || {
                let key = &h.insert_keys[i];
                i = i.wrapping_add(1) % h.insert_keys.len();
                (black_box(key.clone()), 7u32)
            },
            |(key, value)| h.ttl.insert(key, value),
            BatchSize::SmallInput,
        );
    });

    group.bench_function("response_cache_store", |b| {
        let mut i = 0usize;
        b.iter_batched(
            || {
                let key = &h.keys[i];
                let response = &h.responses[i];
                i = i.wrapping_add(1) % h.keys.len();
                (black_box(key.clone()), black_box(response.clone()))
            },
            |(key, response)| h.cache.store(key, response),
            BatchSize::SmallInput,
        );
    });

    group.bench_function("response_cache_lookup", |b| {
        let mut i = 0usize;
        b.iter(|| {
            let key = &h.keys[i];
            i = i.wrapping_add(1) % h.keys.len();
            let hit = h.cache.lookup(key);
            black_box(hit)
        });
    });

    // Miss path in isolation: keys that were never stored, so the lookup
    // measures the empty-result walk (hash + probe + counter) and nothing else.
    group.bench_function("cache_lookup_miss", |b| {
        let mut i = 0usize;
        b.iter(|| {
            let key = &h.miss_keys[i];
            i = i.wrapping_add(1) % h.miss_keys.len();
            let miss = h.cache.lookup(key);
            black_box(miss)
        });
    });

    // Hit lookup in isolation, separated from key derivation: the other half of
    // the cost a cache lookup pays, and the baseline the hit-path bench adds
    // `response_key` on top of.
    group.bench_function("cache_lookup_hit", |b| {
        let mut i = 0usize;
        b.iter(|| {
            let key = &h.keys[i];
            i = i.wrapping_add(1) % h.keys.len();
            let hit = h.cache.lookup(key);
            black_box(hit)
        });
    });

    // End-to-end hit path: derive the key from the request bytes, then find it.
    // This is the full cost a cache hit still pays — the only scenario where a
    // pre-screen cannot skip the key derivation, so it is the upper bound any
    // cheaper key pre-screen must stay under. The largest body is the
    // conservative case: key derivation hashes the raw bytes, so the hit path
    // grows with payload size (small payloads are covered by the
    // `response_key_<size>` benches plus the two lookup benches above).
    group.bench_function("cache_hit_path", |b| {
        let body = h
            .bodies
            .last()
            .expect("body sizes are non-empty")
            .1
            .as_slice();
        b.iter(|| {
            let key = response_key(&h.scope, h.model.as_str(), h.protocol.as_str(), body);
            let hit = h.cache.lookup(&key);
            black_box(hit)
        });
    });

    group.finish();
}

criterion_group!(bench, cache_ops_bench);
criterion_main!(bench);
