//! Criterion micro-benchmarks for `gateway-security` filter-word scanning.
//!
//! `WordFilter::filter` and `WordFilter::has_match` run on every request and response
//! body: the first measures the `aho-corasick` scan plus the redaction allocation on a
//! matching body, the second the allocation-free yes/no fast path. `StreamFilter::push`
//! is the streaming counterpart on the response path; it runs once per network chunk,
//! so it is measured with small chunks cut from the same sample text.

use std::sync::Arc;
use std::time::Duration;

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use gateway_security::{FilterConfig, StreamFilter, WordFilter};

const CHUNK_LEN: usize = 48;
const REPEAT: usize = 64;
const SENSITIVE_WORDS: [&str; 6] = [
    "confidential",
    "password",
    "secret",
    "token",
    "internal",
    "apikey",
];
const SAMPLE_LINE: &str =
    "The confidential report lists a password, a secret token and an internal apikey.\n";

struct Harness {
    filter: Arc<WordFilter>,
    text: String,
    chunks: Vec<String>,
}

fn filter_config() -> FilterConfig {
    FilterConfig {
        words: SENSITIVE_WORDS
            .iter()
            .copied()
            .map(str::to_string)
            .collect(),
        replacement: "[REDACTED]".to_string(),
        filter_request: true,
        filter_response: true,
    }
}

fn sample_text() -> String {
    let mut text = String::with_capacity(SAMPLE_LINE.len() * REPEAT);
    for _ in 0..REPEAT {
        text.push_str(SAMPLE_LINE);
    }
    text
}

fn sample_chunks(text: &str) -> Vec<String> {
    text.as_bytes()
        .chunks(CHUNK_LEN)
        .map(|bytes| {
            std::str::from_utf8(bytes)
                .expect("sample text is ASCII, so every byte offset is a UTF-8 boundary")
                .to_string()
        })
        .collect()
}

fn build_harness() -> Harness {
    let config = filter_config();
    let filter = Arc::new(WordFilter::new(&config));
    let text = sample_text();
    let chunks = sample_chunks(&text);
    Harness {
        filter,
        text,
        chunks,
    }
}

fn word_filter_bench(c: &mut Criterion) {
    let h = build_harness();
    let mut group = c.benchmark_group("word_filter");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(2));
    group.warm_up_time(Duration::from_secs(1));

    group.bench_function("filter", |b| {
        b.iter(|| black_box(h.filter.filter(black_box(h.text.as_str()))));
    });

    group.bench_function("has_match", |b| {
        b.iter(|| black_box(h.filter.has_match(black_box(h.text.as_str()))));
    });

    group.bench_function("stream_push_small_chunks", |b| {
        b.iter(|| {
            let mut stream = StreamFilter::new(Arc::clone(&h.filter));
            for chunk in &h.chunks {
                black_box(stream.push(black_box(chunk.as_str())));
            }
            black_box(stream.flush());
        });
    });

    group.finish();
}

criterion_group!(bench, word_filter_bench);
criterion_main!(bench);
