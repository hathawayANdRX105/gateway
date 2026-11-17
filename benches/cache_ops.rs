//! Criterion micro-benchmarks for `gateway-cache` hot paths.
//!
//! `response_key` runs once per cacheable request, `TtlCache::get` / `TtlCache::insert`
//! are the shared bounded-store primitives every gateway cache sits on, and
//! `ResponseCache::store` / `ResponseCache::lookup` are the data-plane entry points
//! for replaying an upstream answer. Their per-call cost directly bounds replay
//! throughput, so each is measured against prepared data rather than cold structures.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use gateway_cache::{
    CacheKey, CacheScope, CachedResponse, CachedUsage, ResponseCache, TtlCache, response_key,
};

const N: usize = 256;
const TTL_CAPACITY: u64 = 4096;
const BODY_PADDING: usize = 4096;

struct Harness {
    scope: CacheScope,
    model: String,
    protocol: String,
    body: Vec<u8>,
    ttl: TtlCache<String, u32>,
    ttl_keys: Vec<String>,
    insert_keys: Vec<String>,
    cache: Arc<ResponseCache>,
    keys: Vec<CacheKey>,
    responses: Vec<CachedResponse>,
}

fn bench_scope() -> CacheScope {
    CacheScope {
        user_key: "u-1".to_string(),
        group: "default".to_string(),
    }
}

fn request_body() -> Vec<u8> {
    let mut body = br#"{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}]}"#.to_vec();
    body.resize(body.len() + BODY_PADDING, b' ');
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
    let body = request_body();

    let ttl: TtlCache<String, u32> = TtlCache::bounded(TTL_CAPACITY);
    let ttl_keys: Vec<String> = (0..N).map(|i| format!("key-{i}")).collect();
    let insert_keys: Vec<String> = (0..N).map(|i| format!("new-{i}")).collect();
    for key in &ttl_keys {
        ttl.insert(key.clone(), 7);
    }

    let keys: Vec<CacheKey> = (0..N)
        .map(|i| response_key(&scope, &model, &protocol, format!("req-{i}").as_bytes()))
        .collect();
    let responses: Vec<CachedResponse> = (0..N).map(|i| cached_response(&body, i)).collect();

    let cache = ResponseCache::with_limits(Duration::from_secs(300), 64 * 1024 * 1024, 1024 * 1024);
    for (key, response) in keys.iter().zip(&responses) {
        cache.store(key.clone(), response.clone());
    }

    Harness {
        scope,
        model,
        protocol,
        body,
        ttl,
        ttl_keys,
        insert_keys,
        cache,
        keys,
        responses,
    }
}

fn cache_ops_bench(c: &mut Criterion) {
    let h = build_harness();
    let mut group = c.benchmark_group("cache_ops");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(2));
    group.warm_up_time(Duration::from_secs(1));

    group.bench_function("response_key", |b| {
        b.iter(|| {
            let key = response_key(
                &h.scope,
                h.model.as_str(),
                h.protocol.as_str(),
                h.body.as_slice(),
            );
            black_box(key)
        });
    });

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
        b.iter(|| {
            let key = &h.insert_keys[i];
            i = i.wrapping_add(1) % h.insert_keys.len();
            h.ttl.insert(black_box(key.clone()), black_box(7u32));
        });
    });

    group.bench_function("response_cache_store", |b| {
        let mut i = 0usize;
        b.iter(|| {
            let key = &h.keys[i];
            let response = &h.responses[i];
            i = i.wrapping_add(1) % h.keys.len();
            h.cache
                .store(black_box(key.clone()), black_box(response.clone()));
        });
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

    group.finish();
}

criterion_group!(bench, cache_ops_bench);
criterion_main!(bench);
