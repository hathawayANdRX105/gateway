//! Security wiring tests — mounting the word filter on the request side (dormant by default).
//!
//! Verifies the mount semantics of `ForwardStage::handle`:
//! 1. dormant by default (no word list mounted) ⇒ the request body passes through byte for byte;
//! 2. word list mounted + request-side switch ⇒ the body egress receives has words replaced
//!    (ASCII case-insensitive); `filter_request=false` ⇒ pass through even with a list mounted;
//! 3. non-UTF-8 request body (binary) ⇒ pass through even with a word list mounted (never
//!    rewrite invalid UTF-8).
//!
//! The mock request builders (`candidate` / `ctx_with_body`) are extracted in `tests/common`.
//! The mount point sits after body materialization and before `ForwardTask` construction, so
//! these assertions cover the body that egress actually receives (OpenAI→OpenAI protocol
//! conversion is a zero-conversion passthrough; `model` already equals the real name, so apart
//! from filtering the bytes match).

mod common;

use bytes::Bytes;
use common::*;
use gateway_core::error::NormalizedError;
use gateway_forward::ForwardStage;
use gateway_forward::egress::{Egress, ForwardedResponse, Timeouts};
use gateway_pipeline::{Stage, StageOutcome};
use gateway_protocol_bridge::format_codec::FormatRegistry;
use gateway_security::{FilterConfig, WordFilter};
use parking_lot::Mutex;
use std::pin::Pin;
use std::sync::Arc;

/// Mock upstream response body: 200 + OpenAI-style usage (readable by non-streaming
/// `commit_forwarded`; not settled when `price_table` is missing).
const RESPONSE_BODY: &[u8] = b"{\"choices\":[{\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1}}";

/// Captures the request body that egress actually receives (one entry per execute, for
/// mount-point assertions).
struct CaptureEgress {
    requests: Mutex<Vec<Bytes>>,
}

impl CaptureEgress {
    fn new() -> Self {
        Self {
            requests: Mutex::new(Vec::new()),
        }
    }

    /// The single upstream request body in single-shot mode.
    fn last_request(&self) -> Bytes {
        self.requests
            .lock()
            .last()
            .cloned()
            .expect("egress must receive at least one request")
    }
}

impl Egress for CaptureEgress {
    fn execute<'a>(
        &'a self,
        _url: &'a str,
        _headers: &'a [(String, String)],
        body: Bytes,
        _timeouts: &'a Timeouts,
    ) -> Pin<Box<dyn Future<Output = Result<ForwardedResponse, NormalizedError>> + Send + 'a>> {
        self.requests.lock().push(body);
        let stream = futures_util::stream::iter(vec![Ok::<Bytes, std::io::Error>(
            Bytes::from_static(RESPONSE_BODY),
        )]);
        Box::pin(async move {
            Ok(ForwardedResponse::from_stream(
                200,
                "application/json",
                None,
                stream,
            ))
        })
    }
}

/// No word list (dormant by default): the body that egress receives is byte-identical to the
/// request body.
///
/// Which bug makes this red: filtering on an unconditional path, an empty word list still
/// changing bytes, or re-encoding JSON first (breaking fidelity) — any dormant-state change.
#[tokio::test]
async fn dormant_by_default() {
    let egress = Arc::new(CaptureEgress::new());
    let adaptors = Arc::new(FormatRegistry::new());
    // No with_word_filter call ⇒ dormant by default.
    let stage = ForwardStage::new(egress.clone(), adaptors);

    let body: &'static [u8] = br#"{"model":"m","content":"hello world"}"#;
    let mut ctx = ctx_with_body(body, candidate("c1"));
    let outcome = stage
        .handle(&mut ctx)
        .await
        .expect("single-shot forward must succeed");
    assert!(
        matches!(outcome, StageOutcome::Continue),
        "non-streaming success must be Continue, got {outcome:?}"
    );

    assert_eq!(
        egress.last_request().as_ref(),
        body,
        "dormant state (no word list mounted) must pass through request body byte for byte, not a single byte may change"
    );
}

/// With a word list mounted: in single-shot mode the body that egress receives has words
/// replaced (ASCII case-insensitive; the list has case variants and a multi-word term);
/// `filter_request=false` skips filtering.
///
/// Which bug makes this red: filtering mounted in the wrong place (after build_task / cost
/// estimation — split accounting), forgetting the `filter_request` switch (filtering while
/// off), case sensitivity (variants not replaced), or the hit path not rebuilding Bytes
/// (estimation/cache still sees the original body).
#[tokio::test]
async fn request_body_filtered_when_enabled() {
    // Word list: a short word + one multi-character long word (on the request side a word
    // crossing a chunk boundary equals "a multi-word term anywhere in the body" — the body is
    // materialized as one segment and aho-corasick scans linearly at any position; real
    // cross-chunk capture is the response-side StreamFilter's job and out of scope here).
    // Words are lowercased in the list while the body uses case variants, verifying ASCII
    // case-insensitivity.
    let cfg = FilterConfig {
        words: vec!["secretword".to_string(), "classifieddata".to_string()],
        replacement: "***".to_string(),
        filter_request: true,
        filter_response: true,
    };
    let filter = Arc::new(WordFilter::new(&cfg));

    let body: &'static [u8] =
        br#"{"model":"m","content":"the secretword and CLASSIFIEDData are here"}"#;
    let expected: &'static [u8] = br#"{"model":"m","content":"the *** and *** are here"}"#;

    // Request side enabled (default): egress receives the replaced body.
    let egress = Arc::new(CaptureEgress::new());
    let adaptors = Arc::new(FormatRegistry::new());
    let stage = ForwardStage::new(egress.clone(), adaptors).with_word_filter(filter.clone());
    let mut ctx = ctx_with_body(body, candidate("c1"));
    let outcome = stage
        .handle(&mut ctx)
        .await
        .expect("single-shot forward must succeed");
    assert!(
        matches!(outcome, StageOutcome::Continue),
        "non-streaming success must be Continue, got {outcome:?}"
    );
    assert_eq!(
        egress.last_request().as_ref(),
        expected,
        "with request-side filtering enabled, egress-received body must already be word-replaced (ASCII case-insensitive)"
    );

    // filter_request=false: the same word list is mounted, but the whole path is skipped and
    // egress gets the original body.
    let egress_off = Arc::new(CaptureEgress::new());
    let adaptors_off = Arc::new(FormatRegistry::new());
    let stage_off = ForwardStage::new(egress_off.clone(), adaptors_off)
        .with_word_filter(filter)
        .with_word_filter_request(false);
    let mut ctx_off = ctx_with_body(body, candidate("c1"));
    let outcome_off = stage_off
        .handle(&mut ctx_off)
        .await
        .expect("single-shot forward must succeed");
    assert!(
        matches!(outcome_off, StageOutcome::Continue),
        "non-streaming success must be Continue, got {outcome_off:?}"
    );
    assert_eq!(
        egress_off.last_request().as_ref(),
        body,
        "when filter_request=false, must pass through as-is, no replacements"
    );
}

/// Non-UTF-8 request body (0xFF / 0xFE are not valid UTF-8 leading bytes): pass through
/// unchanged even with a word list mounted.
///
/// Which bug makes this red: `String::from_utf8_lossy` or similar silently rewriting invalid
/// bytes to U+FFFD before filtering, or a forced UTF-8 decode panicking — any regression
/// that rewrites or crashes on an invalid UTF-8 body.
#[tokio::test]
async fn non_utf8_body_passthrough() {
    let cfg = FilterConfig {
        words: vec!["secretword".to_string()],
        replacement: "***".to_string(),
        filter_request: true,
        filter_response: true,
    };
    let filter = Arc::new(WordFilter::new(&cfg));

    let egress = Arc::new(CaptureEgress::new());
    let adaptors = Arc::new(FormatRegistry::new());
    let stage = ForwardStage::new(egress.clone(), adaptors).with_word_filter(filter);

    // Leading 0xFF/0xFE invalid + legal ASCII (secretword) in the middle + trailing 0x00: if
    // filtering mistakenly goes through lossy decoding, the middle secretword is replaced and
    // 0xFF/0xFE are altered.
    let body: &'static [u8] = b"\xff\xfe\x10\x20secretword\x00";
    let mut ctx = ctx_with_body(body, candidate("c1"));
    let outcome = stage
        .handle(&mut ctx)
        .await
        .expect("non-UTF-8 body must be forwarded as usual, not panic");
    assert!(
        matches!(outcome, StageOutcome::Continue),
        "non-streaming success must be Continue, got {outcome:?}"
    );
    assert_eq!(
        egress.last_request().as_ref(),
        body,
        "non-UTF-8 body must not be filtered / rewritten, whole thing passed through as-is"
    );
}
