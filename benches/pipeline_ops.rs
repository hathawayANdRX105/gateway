//! Forward hot-path microbenchmarks: the pure CPU helpers that run on every forwarding
//! attempt, isolated from IO so their own cost stays visible.
//!
//! - `capture_prompt_body` — replayable body clone (one refcount bump per attempt);
//! - `resolve_upstream_model` / `rewrite_upstream_model` — model extraction / rewrite JSON,
//!   both on the model-in-path and alias-rewrite paths;
//! - `sanitize_client_headers` — hop-by-hop / credential header filtering;
//! - `classify_status` — upstream status to normalized error on the failure path.
//!
//! No sockets are involved: every measured helper is pure and deterministic. Inputs and
//! results are black-boxed so the compiler cannot elide the measured work. Local only,
//! not run in CI.

use std::time::Duration;

use bytes::Bytes;
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use gateway_forward::adapter::sanitize_client_headers;
use gateway_forward::pipeline::{
    capture_prompt_body, resolve_upstream_model, rewrite_upstream_model,
};
use gateway_forward::status::classify_status;

/// A representative chat-completions request body (alias model name, one user message).
const CHAT_BODY: &[u8] = br#"{"model":"gpt-4","messages":[{"role":"user","content":"hello"}]}"#;

fn pipeline_ops_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline_ops");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(2));
    group.warm_up_time(Duration::from_secs(1));

    let body = Bytes::from_static(CHAT_BODY);
    group.bench_function("capture_prompt_body", |b| {
        b.iter(|| {
            black_box(capture_prompt_body(black_box(&body)));
        });
    });

    group.bench_function("resolve_upstream_model_from_body", |b| {
        b.iter(|| {
            black_box(resolve_upstream_model(black_box(&body), black_box("")));
        });
    });
    group.bench_function("resolve_upstream_model_override", |b| {
        b.iter(|| {
            black_box(resolve_upstream_model(
                black_box(&body),
                black_box("gpt-4-0613"),
            ));
        });
    });

    group.bench_function("rewrite_upstream_model_passthrough", |b| {
        b.iter(|| {
            black_box(rewrite_upstream_model(black_box(&body), black_box("")));
        });
    });
    group.bench_function("rewrite_upstream_model_json", |b| {
        b.iter(|| {
            black_box(rewrite_upstream_model(
                black_box(&body),
                black_box("gpt-4-0613"),
            ));
        });
    });

    let headers: Vec<(String, String)> = vec![
        ("accept".into(), "application/json".into()),
        ("authorization".into(), "Bearer test-token".into()),
        ("connection".into(), "keep-alive".into()),
        ("proxy-authorization".into(), "Basic dGVzdA==".into()),
        ("user-agent".into(), "bench/1.0".into()),
        ("x-custom".into(), "keep-me".into()),
    ];
    group.bench_function("sanitize_client_headers", |b| {
        b.iter(|| {
            black_box(sanitize_client_headers(black_box(&headers)));
        });
    });

    let cases: [(u16, &str, Option<u64>); 3] = [
        (401, "invalid api key", None),
        (429, "rate limited", Some(1_000)),
        (502, "bad gateway", None),
    ];
    for (status, preview, retry_after) in cases {
        group.bench_function(format!("classify_status_{status}"), |b| {
            b.iter(|| {
                black_box(classify_status(
                    black_box(status),
                    black_box(preview),
                    black_box(retry_after),
                ));
            });
        });
    }

    group.finish();
}

criterion_group!(bench, pipeline_ops_bench);
criterion_main!(bench);
