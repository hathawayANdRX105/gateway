//! forward ↔ protocol-bridge wiring tests: verify that requests and responses go
//! through format conversion.
//!
//! The old version verified wiring with a SpyCodec built on a directed
//! `(source, target)` `Codec`, and even kept a dedicated regression — "using the
//! request-direction codec to convert a response → 502 Unsupported". Under the new
//! abstraction the direction is carried by the method name (`decode_request` /
//! `encode_request` vs `decode_response` / `encode_response`), and requests and
//! responses share the same single-format codec, so that defect is structurally
//! impossible: as long as a codec is registered, both directions exist. The cases in
//! this file therefore verify "both hops really happen and the product is in the
//! inbound format".

use bytes::Bytes;
use gateway_core::error::NormalizedError;
use gateway_core::records::RouteUnitRecord;
use gateway_forward::ForwardTask;
use gateway_forward::egress::{Egress, ForwardedResponse, Timeouts};
use gateway_forward::pipeline::forward_once;
use gateway_pipeline::ProtocolKind;
use gateway_protocol_bridge::format_codec::FormatRegistry;
use parking_lot::Mutex;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

// ---------- mock Egress ----------

/// Fake upstream: returns a fixed body and records the request body it received.
struct MockEgress {
    captured_body: Arc<Mutex<Option<Bytes>>>,
    /// Final URL sent upstream (the assertion surface for `base_url` `/v1`
    /// tolerance).
    captured_url: Arc<Mutex<Option<String>>>,
    response: Bytes,
    content_type: &'static str,
}

impl Egress for MockEgress {
    fn execute<'a>(
        &'a self,
        url: &'a str,
        _headers: &'a [(String, String)],
        body: Bytes,
        _timeouts: &'a Timeouts,
    ) -> Pin<Box<dyn Future<Output = Result<ForwardedResponse, NormalizedError>> + Send + 'a>> {
        *self.captured_body.lock() = Some(body.clone());
        *self.captured_url.lock() = Some(url.to_string());
        let payload = self.response.clone();
        let ct = self.content_type;
        let stream = futures_util::stream::iter(vec![Ok::<Bytes, std::io::Error>(payload)]);
        Box::pin(async move { Ok(ForwardedResponse::from_stream(200, ct, None, stream)) })
    }
}

// ---------- helpers ----------

fn mk_task(stream: bool, inbound: ProtocolKind, provider_type: &str) -> ForwardTask {
    ForwardTask {
        candidate: gateway_dispatch::candidate::Candidate {
            unit: RouteUnitRecord {
                meta: gateway_core::records::SyncMeta {
                    key: "u1".into(),
                    schema_version: 1,
                    logical_version: 1,
                    origin: "test".into(),
                    updated_at: chrono::Utc::now(),
                },
                group: "default".into(),
                public_model: "m".into(),
                channel_key: "ch1".into(),
                key_index: 0,
                upstream_model: "m".into(),
                weight: 10,
                status: 1,
            },
            secret: "s".into(),
            base_url: "https://upstream.example".into(),
            upstream_model: "m".into(),
            provider_type: provider_type.into(),
            settings: serde_json::Value::Null,
        },
        path: "/v1/chat/completions".to_string(),
        headers: vec![],
        // Public name equals the upstream real name: alias rewriting is a no-op;
        // this file only tests format conversion.
        body: Bytes::from_static(
            b"{\"model\":\"m\",\"messages\":[{\"role\":\"user\",\"content\":\"ping\"}]}",
        ),
        stream,
        provider_type: provider_type.into(),
        extra_headers: vec![],
        inbound_format: inbound,
    }
}

/// Drains a response stream.
async fn drain(forwarded: gateway_forward::Forwarded) -> Bytes {
    use futures_util::StreamExt;
    let mut body = forwarded.body;
    let mut out = Vec::new();
    while let Some(chunk) = body.next().await {
        out.extend_from_slice(&chunk.expect("chunk"));
    }
    Bytes::from(out)
}

// ---------- Request direction ----------

// OpenAI client → Claude channel: the request body must be converted to Claude
// shape (top-level system, required max_tokens).
// The old implementation only worked under the assumption "inbound is always
// OpenAI"; this explicitly verifies that the inbound format really drives the
// conversion.
#[tokio::test]
async fn openai_client_request_is_translated_to_claude_upstream() {
    let captured = Arc::new(Mutex::new(None));
    let egress = MockEgress {
        captured_body: captured.clone(),
        captured_url: Arc::new(Mutex::new(None)),
        response: Bytes::from_static(br#"{"id":"msg_1","type":"message","role":"assistant","model":"m","content":[{"type":"text","text":"pong"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}"#),
        content_type: "application/json",
    };

    let task = mk_task(false, ProtocolKind::OpenAI, "claude");
    let _ = forward_once(
        &task,
        &egress,
        &FormatRegistry::with_defaults(),
        &Timeouts::default(),
    )
    .await
    .expect("forward must succeed");

    let sent = captured.lock().clone().unwrap();
    let v: serde_json::Value =
        serde_json::from_slice(&sent).expect("what is sent upstream must be JSON");

    assert_eq!(
        v["model"], "m",
        "alias rewrite must happen before conversion (otherwise model has already been moved into the vendor field)"
    );
    assert!(
        v["max_tokens"].as_u64().is_some_and(|n| n > 0),
        "Claude requires max_tokens to be filled in, actual: {v}"
    );
    assert!(
        v.get("messages").is_some(),
        "Claude body must have messages, actual: {v}"
    );
}

// Claude client → Claude channel: same format should pass through with zero
// conversion (it must not be parsed and rewritten).
#[tokio::test]
async fn same_format_request_passes_through_verbatim() {
    let captured = Arc::new(Mutex::new(None));
    let egress = MockEgress {
        captured_body: captured.clone(),
        captured_url: Arc::new(Mutex::new(None)),
        response: Bytes::from_static(b"{}"),
        content_type: "application/json",
    };

    let task = mk_task(false, ProtocolKind::Anthropic, "claude");
    let _ = forward_once(
        &task,
        &egress,
        &FormatRegistry::with_defaults(),
        &Timeouts::default(),
    )
    .await
    .expect("forward must succeed");

    let sent = captured.lock().clone().unwrap();
    // Upstream real-name rewriting still happens (the model field), but otherwise
    // the bytes must stay as-is: the original body has only model+messages, and a
    // conversion would produce Claude-specific fields.
    let v: serde_json::Value = serde_json::from_slice(&sent).unwrap();
    assert!(
        v.get("max_tokens").is_none(),
        "same-format must not inject vendor-mandatory fields, actual: {v}"
    );
}

// ---------- Response direction ----------

// Claude channel response → OpenAI client: must be converted to chat.completion
// shape.
// This is the positive version of that 502 regression from the old implementation —
// now it necessarily holds once a codec is registered.
#[tokio::test]
async fn claude_upstream_response_is_translated_to_openai_shape() {
    let egress = MockEgress {
        captured_body: Arc::new(Mutex::new(None)),
        captured_url: Arc::new(Mutex::new(None)),
        response: Bytes::from_static(br#"{"id":"msg_1","type":"message","role":"assistant","model":"claude-sonnet","content":[{"type":"text","text":"pong"}],"stop_reason":"end_turn","usage":{"input_tokens":7,"output_tokens":3}}"#),
        content_type: "application/json",
    };

    let task = mk_task(false, ProtocolKind::OpenAI, "claude");
    let forwarded = forward_once(
        &task,
        &egress,
        &FormatRegistry::with_defaults(),
        &Timeouts::default(),
    )
    .await
    .expect("claude response must be convertible back to OpenAI shape");

    let body = drain(forwarded).await;
    let v: serde_json::Value = serde_json::from_slice(&body).expect("the product must be JSON");

    assert_eq!(
        v["object"], "chat.completion",
        "response must be converted to OpenAI shape, actual: {v}"
    );
    assert_eq!(v["choices"][0]["message"]["content"], "pong");
    assert_eq!(
        v["choices"][0]["finish_reason"], "stop",
        "Claude's end_turn must be mapped to stop"
    );
}

// OpenAI client → OpenAI channel: same-format response passes through with zero
// conversion.
#[tokio::test]
async fn same_format_response_passes_through_verbatim() {
    let payload = Bytes::from_static(
        br#"{"id":"chatcmpl-1","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}]}"#,
    );
    let egress = MockEgress {
        captured_body: Arc::new(Mutex::new(None)),
        captured_url: Arc::new(Mutex::new(None)),
        response: payload.clone(),
        content_type: "application/json",
    };

    let task = mk_task(false, ProtocolKind::OpenAI, "openai");
    let forwarded = forward_once(
        &task,
        &egress,
        &FormatRegistry::with_defaults(),
        &Timeouts::default(),
    )
    .await
    .expect("forward must succeed");

    let body = drain(forwarded).await;
    assert_eq!(
        body, payload,
        "same-format response must pass through byte-for-byte (must not be parsed and re-serialized)"
    );
}

// ---------- Streaming: the inbound format decides what the client receives ----------

// **G2 core regression**: a Claude client must receive streaming responses in the
// Claude event shape.
// The old implementation did not convert the streaming path at all (`ctx.upstream`
// was written only on the non-streaming branch), so the client only ever got OpenAI
// chunks; this uses the real registry to verify outbound frames carry Claude's
// `event:` lines.
#[tokio::test]
async fn claude_client_streaming_receives_claude_event_shape() {
    // The upstream is an OpenAI channel and returns Chat chunks.
    let upstream_sse = Bytes::from_static(
        b"data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"po\"},\"finish_reason\":null}]}\n\n\
data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ng\"},\"finish_reason\":\"stop\"}]}\n\n\
data: [DONE]\n\n",
    );
    let egress = MockEgress {
        captured_body: Arc::new(Mutex::new(None)),
        captured_url: Arc::new(Mutex::new(None)),
        response: upstream_sse,
        content_type: "text/event-stream",
    };

    // Client says Claude (path /v1/messages determines Anthropic); channel is
    // openai.
    let mut task = mk_task(true, ProtocolKind::Anthropic, "openai");
    task.path = "/v1/messages".to_string();

    let forwarded = forward_once(
        &task,
        &egress,
        &FormatRegistry::with_defaults(),
        &Timeouts::default(),
    )
    .await
    .expect("streaming forward must succeed");

    let body = drain(forwarded).await;
    let text = String::from_utf8_lossy(&body);

    assert!(
        text.contains("event: message_start"),
        "Claude client must receive message_start, actual: {text}"
    );
    assert!(
        text.contains("\"type\":\"content_block_delta\""),
        "Claude client must receive content_block_delta, actual: {text}"
    );
    assert!(
        text.contains("event: message_stop"),
        "Claude client must receive message_stop, actual: {text}"
    );
    assert!(
        !text.contains("chat.completion.chunk"),
        "OpenAI chunk must not be leaked verbatim to Claude client, actual: {text}"
    );

    // Text is split across frames per Claude's delta semantics: concatenate the
    // `text_delta` text values and the result must be the complete original text.
    // Grepping "pong" directly would misjudge — the two deltas (po / ng) are
    // separated by frame boundaries and are not adjacent in the byte stream.
    let deltas: String = text
        .lines()
        .filter_map(|l| l.strip_prefix("data: "))
        .filter_map(|d| serde_json::from_str::<serde_json::Value>(d).ok())
        .filter(|v| v["type"] == "content_block_delta")
        .filter_map(|v| v["delta"]["text"].as_str().map(str::to_string))
        .collect();
    assert_eq!(
        deltas, "pong",
        "deltas concatenated must be the complete text, actual: {deltas:?}"
    );
}

// OpenAI client streaming against a Claude channel: must receive Chat chunks (not
// Claude events).
#[tokio::test]
async fn openai_client_streaming_receives_chat_chunk_shape() {
    let upstream_sse = Bytes::from_static(
        b"event: message_start\n\
data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"m\",\"content\":[],\"usage\":{\"input_tokens\":3,\"output_tokens\":0}}}\n\n\
event: content_block_start\n\
data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n\
event: content_block_delta\n\
data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"pong\"}}\n\n\
event: content_block_stop\n\
data: {\"type\":\"content_block_stop\",\"index\":0}\n\n\
event: message_delta\n\
data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":2}}\n\n\
event: message_stop\n\
data: {\"type\":\"message_stop\"}\n\n",
    );
    let egress = MockEgress {
        captured_body: Arc::new(Mutex::new(None)),
        captured_url: Arc::new(Mutex::new(None)),
        response: upstream_sse,
        content_type: "text/event-stream",
    };

    // Client says OpenAI, channel is claude, path uses Claude style (the server SSE
    // shape is decided by the upstream).
    let task = mk_task(true, ProtocolKind::OpenAI, "claude");
    let forwarded = forward_once(
        &task,
        &egress,
        &FormatRegistry::with_defaults(),
        &Timeouts::default(),
    )
    .await
    .expect("streaming forward must succeed");

    let body = drain(forwarded).await;
    let text = String::from_utf8_lossy(&body);

    assert!(
        text.contains("chat.completion.chunk"),
        "OpenAI client must receive chat chunk, actual: {text}"
    );
    assert!(
        text.contains("\"content\":\"pong\""),
        "text delta must be carried over, actual: {text}"
    );
    assert!(
        !text.contains("event: message_start"),
        "Claude events must not be leaked verbatim to OpenAI client, actual: {text}"
    );
}

// The SSE spec does not require a stream to end with a blank line: when the
// upstream disconnects without a trailing blank line after the last frame, that
// frame must not be dropped. The dropped one is usually the closing frame, and the
// client waits forever or judges the stream abnormal.
#[tokio::test]
async fn unterminated_trailing_frame_is_not_dropped() {
    // Note the ending: the last frame's data line has **no** blank-line separator
    // after it.
    let upstream_sse = Bytes::from_static(
        b"data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"hi\"},\"finish_reason\":null}]}\n\n\
data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}",
    );
    let egress = MockEgress {
        captured_body: Arc::new(Mutex::new(None)),
        captured_url: Arc::new(Mutex::new(None)),
        response: upstream_sse,
        content_type: "text/event-stream",
    };

    let mut task = mk_task(true, ProtocolKind::Anthropic, "openai");
    task.path = "/v1/messages".to_string();

    let forwarded = forward_once(
        &task,
        &egress,
        &FormatRegistry::with_defaults(),
        &Timeouts::default(),
    )
    .await
    .expect("streaming forward must succeed");

    let body = drain(forwarded).await;
    let text = String::from_utf8_lossy(&body);

    // The trailing frame's finish_reason must show up in the outbound event (on the
    // Claude side, message_delta's stop_reason).
    assert!(
        text.contains("\"stop_reason\":\"end_turn\""),
        "unterminated trailing frame must not be dropped, actual: {text}"
    );
}

// The upstream sends data frames again after [DONE] (the SSE spec does not allow
// it, but real upstreams do): these frames must not be encoded and sent to the
// client — receiving content after the terminal frame is a protocol violation and
// also breaks the "stream already ended" determination.
#[tokio::test]
async fn frames_after_done_are_not_forwarded_to_client() {
    let upstream_sse = Bytes::from_static(
        b"data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"},\"finish_reason\":null}]}\n\n\
data: [DONE]\n\n\
data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"stray\"},\"finish_reason\":null}]}\n\n",
    );
    let egress = MockEgress {
        captured_body: Arc::new(Mutex::new(None)),
        captured_url: Arc::new(Mutex::new(None)),
        response: upstream_sse,
        content_type: "text/event-stream",
    };

    let mut task = mk_task(true, ProtocolKind::Anthropic, "openai");
    task.path = "/v1/messages".to_string();

    let forwarded = forward_once(
        &task,
        &egress,
        &FormatRegistry::with_defaults(),
        &Timeouts::default(),
    )
    .await
    .expect("streaming forward must succeed");

    let body = drain(forwarded).await;
    let text = String::from_utf8_lossy(&body);

    // Normal content must arrive (a text_delta on the Claude side).
    assert!(
        text.contains("hi"),
        "content before terminal frame must be delivered, actual: {text}"
    );
    // The "stray" from the frame after the terminal frame must not appear in the
    // outbound stream.
    assert!(
        !text.contains("stray"),
        "[DONE] data frames after it must not be forwarded to client, actual: {text}"
    );
    // And the client must receive exactly one terminal frame (the event line and
    // the data line count as the same one).
    assert_eq!(
        text.matches(r#""type":"message_stop""#).count(),
        1,
        "there must be exactly one terminal frame, actual: {text}"
    );
}

// ---------- Streaming non-2xx guard: fault injection ----------

/// Fault-injection outlet: knowingly violates the contract by returning
/// `Ok(non-2xx)`.
///
/// Both production Egress implementations classify non-2xx as Err inside `execute`,
/// so the pipeline guard is never reached in normal operation; what is injected here
/// is exactly the "Ok(non-2xx) pass-through" case the guard is meant to catch,
/// verifying defense in depth — this is not reproducing a production defect.
struct LeakyEgress {
    status: u16,
    body: Bytes,
}

impl Egress for LeakyEgress {
    fn execute<'a>(
        &'a self,
        _url: &'a str,
        _headers: &'a [(String, String)],
        _body: Bytes,
        _timeouts: &'a Timeouts,
    ) -> Pin<Box<dyn Future<Output = Result<ForwardedResponse, NormalizedError>> + Send + 'a>> {
        let status = self.status;
        let payload = self.body.clone();
        let stream = futures_util::stream::iter(vec![Ok::<Bytes, std::io::Error>(payload)]);
        Box::pin(async move {
            Ok(ForwardedResponse::from_stream(
                status,
                "text/event-stream",
                None,
                stream,
            ))
        })
    }
}

/// The guard sits before passthrough: regardless of whether inbound and upstream
/// share a format, the streaming branch must turn `Ok(non-2xx)` into Err and
/// classify it by status code (413 fatal, no retry / 429 rate-limited, retryable).
#[tokio::test]
async fn stream_guard_rejects_leaked_non_2xx_before_passthrough() {
    // (inbound format, channel, upstream status code, retryable)
    // Same format takes the zero-conversion passthrough shortcut, cross-format takes
    // the SSE conversion path — both entries must be blocked before the body is
    // consumed.
    let cases: [(ProtocolKind, &str, u16, bool); 2] = [
        (ProtocolKind::OpenAI, "openai", 413, false),
        (ProtocolKind::Anthropic, "openai", 429, true),
    ];
    for (inbound, provider, status, retryable) in cases {
        let egress = LeakyEgress {
            status,
            body: Bytes::from(format!(
                "{{\"error\":{{\"message\":\"upstream {status}\",\"type\":\"upstream_error\"}}}}"
            )),
        };
        let mut task = mk_task(true, inbound, provider);
        if inbound == ProtocolKind::Anthropic {
            task.path = "/v1/messages".to_string();
        }

        let err = forward_once(
            &task,
            &egress,
            &Arc::new(FormatRegistry::with_defaults()),
            &Timeouts::default(),
        )
        .await
        .expect_err("Ok(non-2xx) must be blocked into Err by the stream guard");

        assert_eq!(
            err.status, status,
            "upstream status code must be propagated as-is, not degraded to 502"
        );
        assert_eq!(
            err.retryable, retryable,
            "413 means the request itself is bad, not retryable; 429 is rate-limited, retryable"
        );
    }
}

/// When the first chunk already exceeds 2048 bytes, the guard must stop
/// immediately — the preview is bounded and the whole error stream must not be read
/// into memory. The second poll panics outright: the test goes red if the guard
/// keeps reading.
struct OversizedErrorStream {
    polls: Arc<std::sync::atomic::AtomicUsize>,
}

impl futures_util::Stream for OversizedErrorStream {
    type Item = Result<Bytes, std::io::Error>;
    fn poll_next(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::sync::atomic::Ordering;
        let n = self.polls.fetch_add(1, Ordering::SeqCst);
        match n {
            0 => std::task::Poll::Ready(Some(Ok(Bytes::from(vec![b'e'; 8192])))),
            _ => panic!(
                "preview bounded: must stop after 2048 bytes, must not keep polling the error stream"
            ),
        }
    }
}

struct OversizedErrorEgress {
    status: u16,
    polls: Arc<std::sync::atomic::AtomicUsize>,
}

impl Egress for OversizedErrorEgress {
    fn execute<'a>(
        &'a self,
        _url: &'a str,
        _headers: &'a [(String, String)],
        _body: Bytes,
        _timeouts: &'a Timeouts,
    ) -> Pin<Box<dyn Future<Output = Result<ForwardedResponse, NormalizedError>> + Send + 'a>> {
        let status = self.status;
        let stream = OversizedErrorStream {
            polls: self.polls.clone(),
        };
        Box::pin(async move {
            Ok(ForwardedResponse::from_stream(
                status,
                "text/event-stream",
                None,
                stream,
            ))
        })
    }
}

#[tokio::test]
async fn stream_guard_preview_is_bounded_and_stops_reading() {
    let polls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let egress = OversizedErrorEgress {
        status: 413,
        polls: polls.clone(),
    };
    let task = mk_task(true, ProtocolKind::OpenAI, "openai");

    let err = forward_once(
        &task,
        &egress,
        &Arc::new(FormatRegistry::with_defaults()),
        &Timeouts::default(),
    )
    .await
    .expect_err("Ok(non-2xx) must be blocked into Err by the stream guard");

    assert_eq!(err.status, 413);
    assert_eq!(
        polls.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "first chunk already at 2048 limit, guard must stop polling error stream"
    );
}

/// When the error stream hangs and returns nothing, the guard must be bounded by
/// `total_ms` and must not wait on the stream forever; a timeout only degrades the
/// preview, and the status classification is unchanged. A pending stream has no
/// further bytes, so only the timeout can settle it — that is the actual assertion
/// of this contract (reusing tokio's already enabled time feature, no new
/// dependency; total_ms is 1ms rather than a real long timeout, so the assertion
/// result is independent of when it fires).
struct HangingErrorEgress {
    status: u16,
}

impl Egress for HangingErrorEgress {
    fn execute<'a>(
        &'a self,
        _url: &'a str,
        _headers: &'a [(String, String)],
        _body: Bytes,
        _timeouts: &'a Timeouts,
    ) -> Pin<Box<dyn Future<Output = Result<ForwardedResponse, NormalizedError>> + Send + 'a>> {
        let status = self.status;
        Box::pin(async move {
            Ok(ForwardedResponse::from_stream(
                status,
                "text/event-stream",
                None,
                futures_util::stream::pending::<Result<Bytes, std::io::Error>>(),
            ))
        })
    }
}

#[tokio::test]
async fn stream_guard_preview_timeouts_instead_of_hanging() {
    let egress = HangingErrorEgress { status: 413 };
    let task = mk_task(true, ProtocolKind::OpenAI, "openai");
    let timeouts = Timeouts {
        total_ms: 1,
        ..Timeouts::default()
    };

    let err = forward_once(
        &task,
        &egress,
        &Arc::new(FormatRegistry::with_defaults()),
        &timeouts,
    )
    .await
    .expect_err("hanging error stream must be timed out into Err");

    assert_eq!(
        err.status, 413,
        "timeout only degrades the preview, status code classification unchanged"
    );
}

// ---------- base_url `/v1` tolerance (2026-09-29) ----------

/// Which bug would turn it red: the openai/claude/gemini branches appending their
/// own version prefix when the base already carries `/v1` → `/v1/v1/...` 404 (the
/// wt-* seed channels in the library have this shape); or overcorrecting and
/// stripping the passthrough base too (its `{base}{path}` is decided by the client
/// path); or stripping gemini's authoritative `/v1beta` as if it were a version
/// prefix.
///
/// Only the URL is asserted: `execute` records it on entry, so there is no need to
/// wait for response decoding to succeed.
#[tokio::test]
async fn upstream_url_tolerates_base_with_v1_suffix() {
    for (provider, want) in [
        ("openai", "https://upstream.example/v1/chat/completions"),
        ("claude", "https://upstream.example/v1/messages"),
        (
            "gemini",
            "https://upstream.example/v1beta/models/m:generateContent",
        ),
    ] {
        let captured_url = Arc::new(Mutex::new(None));
        let egress = MockEgress {
            captured_body: Arc::new(Mutex::new(None)),
            captured_url: captured_url.clone(),
            response: Bytes::from_static(b"{}"),
            content_type: "application/json",
        };
        let mut task = mk_task(false, ProtocolKind::OpenAI, provider);
        task.candidate.base_url = "https://upstream.example/v1".into();
        let _ = forward_once(
            &task,
            &egress,
            &FormatRegistry::with_defaults(),
            &Timeouts::default(),
        )
        .await;
        assert_eq!(
            captured_url.lock().clone().as_deref(),
            Some(want),
            "provider {provider}: with /v1 in base must build {want}"
        );
    }

    // passthrough: base as-is (the client path decides), and it must not be
    // stripped.
    let captured_url = Arc::new(Mutex::new(None));
    let egress = MockEgress {
        captured_body: Arc::new(Mutex::new(None)),
        captured_url: captured_url.clone(),
        response: Bytes::from_static(b"{}"),
        content_type: "application/json",
    };
    let mut task = mk_task(false, ProtocolKind::OpenAI, "passthrough");
    task.candidate.base_url = "https://upstream.example/v1".into();
    let _ = forward_once(
        &task,
        &egress,
        &FormatRegistry::with_defaults(),
        &Timeouts::default(),
    )
    .await;
    assert_eq!(
        captured_url.lock().clone().as_deref(),
        Some("https://upstream.example/v1/v1/chat/completions"),
        "passthrough base must not be stripped ({{base}}{{client path}} is decided by client path)"
    );
}
