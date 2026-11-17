//! Micro-benchmarks for the hot codec and error-mapping paths in `gateway-protocol-bridge`.
//!
//! Each proxied request resolves a format codec, translates its request and response bodies
//! through the two-hop registry, feeds the response stream through the SSE scanner, and maps any
//! stage failure into a protocol error body. These operations are on the forwarding path and are
//! allocation-sensitive, so this file measures their per-call cost. The registry, JSON bodies,
//! SSE chunk stream and error value are all built once outside the measured closure.

use std::time::Duration;

use bytes::Bytes;
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use gateway_pipeline::{ProtocolKind, StageError};
use gateway_protocol_bridge::adaptor::Protocol;
use gateway_protocol_bridge::error_mapping::map_error;
use gateway_protocol_bridge::format_codec::FormatRegistry;
use gateway_protocol_bridge::sse::SseScanner;

/// Minimal `OpenAI` Chat request body (the source format used by the request translation bench).
fn request_body() -> Bytes {
    Bytes::from_static(
        br#"{"model":"gpt-x","messages":[{"role":"user","content":"hello, please summarize 42 items today"}]}"#,
    )
}

/// Minimal Claude response body (the source format used by the response translation bench).
fn response_body() -> Bytes {
    Bytes::from_static(
        br#"{"id":"msg_1","type":"message","role":"assistant","model":"claude-x","content":[{"type":"text","text":"pong"}],"stop_reason":"end_turn","usage":{"input_tokens":12,"output_tokens":7}}"#,
    )
}

/// `OpenAI`-style SSE data frames, including a keepalive, a usage frame and the terminal frame,
/// so the scanner's first-token / ping / usage / done detection all run each iteration.
fn sse_chunks() -> Vec<Bytes> {
    vec![
        Bytes::from_static(
            b"data: {\"id\":\"1\",\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\n",
        ),
        Bytes::from_static(
            b"data: {\"id\":\"2\",\"choices\":[{\"delta\":{\"content\":\"hello \"}}]}\n\n",
        ),
        Bytes::from_static(
            b"data: {\"id\":\"3\",\"choices\":[{\"delta\":{\"content\":\"world 42\"}}]}\n\n",
        ),
        Bytes::from_static(b": keepalive\n\n"),
        Bytes::from_static(
            b"data: {\"id\":\"4\",\"usage\":{\"prompt_tokens\":8,\"completion_tokens\":3}}\n\n",
        ),
        Bytes::from_static(b"data: [DONE]\n\n"),
    ]
}

fn codec_ops(c: &mut Criterion) {
    let registry = FormatRegistry::with_defaults();
    let request = request_body();
    let response = response_body();
    let chunks = sse_chunks();
    // map_error takes the error by reference, so one prepared value can be reused every iteration.
    let error = StageError::Unauthenticated("bench token".to_string());

    let mut group = c.benchmark_group("codec_ops");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(2));
    group.warm_up_time(Duration::from_secs(1));

    group.bench_function("resolve", |b| {
        b.iter(|| black_box(registry.resolve(black_box(Protocol::OpenAi))));
    });

    group.bench_function("translate_request", |b| {
        b.iter(|| {
            black_box(registry.translate_request(
                black_box(Protocol::OpenAi),
                black_box(Protocol::Claude),
                black_box(request.clone()),
            ))
        });
    });

    group.bench_function("translate_response", |b| {
        b.iter(|| {
            black_box(registry.translate_response(
                black_box(Protocol::Claude),
                black_box(Protocol::OpenAi),
                black_box(response.clone()),
            ))
        });
    });

    group.bench_function("sse_push", |b| {
        b.iter(|| {
            let mut scanner = SseScanner::default();
            for chunk in &chunks {
                black_box(scanner.push(black_box(chunk)));
            }
            black_box(scanner.take_data_frames())
        });
    });

    group.bench_function("map_error", |b| {
        b.iter(|| {
            black_box(map_error(
                black_box(&error),
                black_box(ProtocolKind::OpenAI),
            ))
        });
    });

    group.finish();
}

criterion_group!(bench, codec_ops);
criterion_main!(bench);
