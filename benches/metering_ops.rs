//! Micro-benchmarks for the hot pure functions in `gateway-metering`.
//!
//! Every metered request runs character-class token estimation (over raw text and over the
//! adapted prompt body), incremental stream scanning, price arithmetic, and non-streaming
//! usage extraction. These functions sit on the request path and are allocation-sensitive, so
//! their per-call cost is what matters here. All inputs — text, JSON bodies, price rows and the
//! chunk stream — are built once outside the measured closure; only the function under test is
//! timed.

use std::time::Duration;

use bytes::Bytes;
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use gateway_metering::estimate::{estimate_prompt_tokens, estimate_tokens};
use gateway_metering::pricing::{ModelPrice, price_of};
use gateway_metering::scanner::{StreamScanner, TokenCounts};
use gateway_metering::usage::extract_usage;

/// Mixed Latin / CJK / digit text: the three weight classes the estimator distinguishes,
/// long enough that per-character branch cost dominates call overhead.
fn sample_text() -> String {
    const UNIT: &str = "The quick brown fox 123 jumps; 你好世界 via 42 paths. ";
    UNIT.repeat(40)
}

/// An adapted prompt body in the shape the estimator recognizes (a `messages` array), so the
/// measured path parses JSON and walks billable text rather than taking the raw-length fallback.
fn prompt_body() -> Bytes {
    Bytes::from_static(
        br#"{"model":"gpt-x","messages":[{"role":"system","content":"You are a concise assistant. Answer in at most three sentences."},{"role":"user","content":"Summarize the 2024 release notes and list the top 5 changes with numbers 1 through 5."}]}"#,
    )
}

/// Small non-streaming response body with `OpenAI`-shaped usage and a cached-token detail.
const USAGE_BODY: &[u8] =
    br#"{"usage":{"prompt_tokens":128,"completion_tokens":64,"prompt_tokens_details":{"cached_tokens":32}}}"#;

fn sample_counts() -> TokenCounts {
    TokenCounts {
        prompt: 1_024,
        completion: 256,
        cached: 64,
        cache_creation: 0,
        prompt_includes_cache: true,
        reasoning: 0,
    }
}

fn sample_price() -> ModelPrice {
    ModelPrice {
        input: 2.5,
        output: 10.0,
        cache: 0.5,
        group_multiplier: 1.0,
    }
}

/// `OpenAI`-style SSE chunks, one split across a chunk boundary so the scanner exercises its
/// carried-line buffer as well as the no-partial-line fast path.
fn stream_chunks() -> Vec<Bytes> {
    vec![
        Bytes::from_static(b"data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\n"),
        Bytes::from_static(b"data: {\"choices\":[{\"delta\":{\"content\":\"Hel"),
        Bytes::from_static(b"lo, this is a streamed reply with 42 tokens!\"}}]}\n\n"),
        Bytes::from_static(
            b"data: {\"choices\":[{\"delta\":{\"content\":\" and more text here\"}}]}\n\n",
        ),
        Bytes::from_static(b"data: {\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5}}\n\n"),
        Bytes::from_static(b"data: [DONE]\n\n"),
    ]
}

fn metering_ops(c: &mut Criterion) {
    let text = sample_text();
    let prompt = prompt_body();
    let counts = sample_counts();
    let price = sample_price();
    let chunks = stream_chunks();

    let mut group = c.benchmark_group("metering_ops");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(2));
    group.warm_up_time(Duration::from_secs(1));

    group.bench_function("estimate_tokens", |b| {
        b.iter(|| black_box(estimate_tokens(black_box(text.as_str()))));
    });

    group.bench_function("estimate_prompt_tokens", |b| {
        b.iter(|| black_box(estimate_prompt_tokens(black_box(&prompt))));
    });

    group.bench_function("stream_scan", |b| {
        b.iter(|| {
            let mut scanner = StreamScanner::new();
            for chunk in &chunks {
                scanner.push(black_box(chunk));
            }
            black_box(scanner.finish(black_box(1_024u64)))
        });
    });

    group.bench_function("price_of", |b| {
        b.iter(|| {
            black_box(price_of(
                black_box(counts),
                black_box(&price),
                black_box(1.0),
            ))
        });
    });

    group.bench_function("extract_usage", |b| {
        b.iter(|| black_box(extract_usage(black_box(USAGE_BODY))));
    });

    group.finish();
}

criterion_group!(bench, metering_ops);
criterion_main!(bench);
