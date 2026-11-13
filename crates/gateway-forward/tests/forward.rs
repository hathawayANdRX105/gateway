//! Behavioral tests for the forward pipeline — adapter URL/headers / sanitize /
//! egress / stream.

use bytes::Bytes;
use gateway_forward::ForwardTask;
use gateway_forward::adapter::{prepare, sanitize_client_headers};
use gateway_forward::egress::{Egress, ReqwestEgress, Timeouts};
use gateway_forward::stream::{AbortGuard, SseContext, finish, pipe_chunk};
use gateway_protocol_bridge::FormatRegistry;
use std::sync::Arc;
use std::time::Duration;

use gateway_core::records::{ChannelSettings, RouteUnitRecord, SyncMeta};
use gateway_dispatch::candidate::Candidate;
use gateway_pipeline::ProtocolKind;

// ---------- Test helpers ----------

fn candidate(provider: &str) -> Candidate {
    Candidate {
        unit: RouteUnitRecord {
            meta: SyncMeta {
                key: "u1".to_string(),
                schema_version: 1,
                logical_version: 1,
                origin: "test".to_string(),
                updated_at: chrono::Utc::now(),
            },
            group: "g".to_string(),
            public_model: "m".to_string(),
            channel_key: "ch1".to_string(),
            key_index: 0,
            upstream_model: "m".to_string(),
            weight: 10,
            status: 1,
        },
        secret: format!("sk-{provider}-secret"),
        base_url: "https://upstream.example".to_string(),
        upstream_model: "m".to_string(),
        provider_type: provider.to_string(),
        settings: ChannelSettings::default(),
    }
}

// ---------- adapter ----------

/// The client path must not determine the upstream endpoint — the URL must match
/// the format of the converted body. This is the other half of making cross-format
/// conversion work end to end (the body is converted in protocol-bridge, the path
/// in adapter).
#[test]
fn adapter_url_follows_upstream_protocol_not_client_path() {
    // Claude client (hitting /v1/messages) against an OpenAI channel → must be
    // /v1/chat/completions, because the body has been encoded as Chat; using the
    // client's /v1/messages would 404.
    let c = candidate("openai");
    let p = prepare(
        &c,
        "/v1/messages",
        "openai",
        ProtocolKind::Anthropic,
        false,
        "m",
        vec![],
    );
    assert_eq!(p.url, "https://upstream.example/v1/chat/completions");

    // Gemini client against a Claude channel → /v1/messages; the client's
    // :generateContent is void.
    let c = candidate("claude");
    let p = prepare(
        &c,
        "/v1beta/models/gemini-pro:generateContent",
        "claude",
        ProtocolKind::Gemini,
        false,
        "m",
        vec![],
    );
    assert_eq!(p.url, "https://upstream.example/v1/messages");

    // OpenAI channel + Responses client → the Responses endpoint (the body is in
    // Responses format).
    let c = candidate("openai");
    let p = prepare(
        &c,
        "/v1/chat/completions",
        "openai",
        ProtocolKind::OpenAIResp,
        false,
        "m",
        vec![],
    );
    assert_eq!(p.url, "https://upstream.example/v1/responses");

    // Gemini channel: model goes into the path, verb follows stream; model is the
    // upstream real name, not the client alias.
    let c = candidate("gemini");
    let p = prepare(
        &c,
        "/v1/messages",
        "gemini",
        ProtocolKind::OpenAI,
        true,
        "gpt-4-0613",
        vec![],
    );
    assert_eq!(
        p.url,
        "https://upstream.example/v1beta/models/gpt-4-0613:streamGenerateContent"
    );
    let p = prepare(
        &c,
        "/v1/messages",
        "gemini",
        ProtocolKind::OpenAI,
        false,
        "gpt-4-0613",
        vec![],
    );
    assert_eq!(
        p.url,
        "https://upstream.example/v1beta/models/gpt-4-0613:generateContent"
    );

    // passthrough: the client path is kept as-is and not rewritten.
    let c = candidate("passthrough");
    let p = prepare(
        &c,
        "/custom/endpoint",
        "passthrough",
        ProtocolKind::OpenAI,
        false,
        "m",
        vec![],
    );
    assert_eq!(p.url, "https://upstream.example/custom/endpoint");
}

#[test]
fn adapter_builds_url_per_provider() {
    // openai → base + /v1 + path
    let c = candidate("openai");
    let p = prepare(
        &c,
        "/chat/completions",
        "openai",
        ProtocolKind::OpenAI,
        false,
        "m",
        vec![],
    );
    assert_eq!(p.url, "https://upstream.example/v1/chat/completions");
    assert_eq!(p.auth_header.0, "Authorization");
    assert_eq!(p.auth_header.1, "Bearer sk-openai-secret");

    // claude → base + /v1/messages
    let c = candidate("claude");
    let p = prepare(
        &c,
        "/messages",
        "claude",
        ProtocolKind::Anthropic,
        false,
        "m",
        vec![],
    );
    assert!(p.url.ends_with("/v1/messages"));
    assert_eq!(p.auth_header.0, "x-api-key");
    assert_eq!(p.auth_header.1, "sk-claude-secret");

    // gemini → base + /v1beta/...
    let c = candidate("gemini");
    let p = prepare(
        &c,
        "/v1/messages",
        "gemini",
        ProtocolKind::Anthropic,
        false,
        "gemini-pro",
        vec![],
    );
    assert_eq!(
        p.url,
        "https://upstream.example/v1beta/models/gemini-pro:generateContent"
    );
    assert_eq!(p.auth_header.0, "x-goog-api-key");
}

#[test]
fn adapter_extra_headers_carried_into_merge() {
    // prepare only stores extra_headers in the output; overriding happens in
    // pipeline::merge_headers (later headers override earlier ones — channel
    // settings headers beat auth headers).
    let c = candidate("openai");
    let p = prepare(
        &c,
        "/chat/completions",
        "openai",
        ProtocolKind::OpenAI,
        false,
        "m",
        vec![("authorization".to_string(), "Bearer override".to_string())],
    );
    assert_eq!(p.auth_header.0, "Authorization");
    assert_eq!(p.auth_header.1, "Bearer sk-openai-secret");
    assert_eq!(p.extra_headers.len(), 1);
    // merge_headers: channel settings headers beat the adapter auth header, and
    // same-name headers are deduplicated (case-insensitive) — Authorization is not
    // sent twice.
    let merged = gateway_forward::pipeline::merge_headers(&p, &[]);
    assert_eq!(merged.len(), 1);
    assert_eq!(
        merged
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("authorization"))
            .unwrap()
            .1,
        "Bearer override",
        "extra_headers must override auth_header at merge and not be duplicated"
    );
}

#[test]
fn merge_headers_channel_settings_win_over_client_headers() {
    // When a channel settings header and a client header that survived filtering
    // have the same name (different case), the channel header wins and is not
    // duplicated — this is the last segment of merge_headers' three-segment chain
    // (extra > auth > client); the first two are covered by
    // adapter_extra_headers_carried_into_merge.
    let c = candidate("openai");
    let p = prepare(
        &c,
        "/chat/completions",
        "openai",
        ProtocolKind::OpenAI,
        false,
        "m",
        vec![("x-custom".to_string(), "from-channel".to_string())],
    );
    let client = vec![("X-Custom".to_string(), "from-client".to_string())];
    let merged = gateway_forward::pipeline::merge_headers(&p, &client);
    assert_eq!(
        merged
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("x-custom"))
            .unwrap()
            .1,
        "from-channel",
        "channel settings headers must take precedence over client headers"
    );
    assert_eq!(
        merged
            .iter()
            .filter(|(k, _)| k.eq_ignore_ascii_case("x-custom"))
            .count(),
        1,
        "same-name headers must not be duplicated"
    );
}

#[test]
fn sanitize_strips_hop_by_hop_and_credentials() {
    let headers = vec![
        ("authorization".to_string(), "Bearer sk-xxx".to_string()),
        ("cookie".to_string(), "session=1".to_string()),
        ("host".to_string(), "client.example".to_string()),
        ("content-length".to_string(), "100".to_string()),
        ("connection".to_string(), "keep-alive".to_string()),
        ("transfer-encoding".to_string(), "chunked".to_string()),
        ("proxy-authorization".to_string(), "Basic abc".to_string()),
        ("x-api-key".to_string(), "sk-xxx".to_string()),
        ("accept".to_string(), "application/json".to_string()),
        ("user-agent".to_string(), "curl/8".to_string()),
        ("x-custom".to_string(), "keep-me".to_string()),
    ];
    let kept = sanitize_client_headers(&headers);
    let names: Vec<&str> = kept.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(names, vec!["accept", "user-agent", "x-custom"]);
}

// ---------- egress ----------

/// Starts a minimal TCP HTTP service that responds with `status` and `body`.
async fn serve_once(
    status_line: &'static str,
    body: &'static str,
) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 4096];
        let _ = sock.read(&mut buf).await; // read the request headers
        let resp = format!(
            "HTTP/1.1 {status_line}\r\ncontent-type: text/plain\r\ncontent-length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        let _ = sock.write_all(resp.as_bytes()).await;
        let _ = sock.flush().await;
    });
    (format!("http://{addr}"), handle)
}

#[tokio::test]
async fn egress_returns_body_for_2xx() {
    use futures_util::StreamExt;
    let (url, server) = serve_once("200 OK", "hello world").await;
    let egress = ReqwestEgress::new();
    let resp = egress
        .execute(&url, &[], Bytes::new(), &Timeouts::default())
        .await
        .expect("2xx should succeed");
    assert_eq!(resp.status(), 200);

    let mut stream = resp.into_body_stream();
    let mut all = Vec::new();
    while let Some(chunk) = stream.next().await {
        all.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(String::from_utf8_lossy(&all), "hello world");
    server.await.unwrap();
}

#[tokio::test]
async fn egress_classifies_429_as_retryable() {
    let (url, server) = serve_once("429 Too Many Requests", "rate limited").await;
    let egress = ReqwestEgress::new();
    let err = egress
        .execute(&url, &[], Bytes::new(), &Timeouts::default())
        .await
        .expect_err("429 should be an error");
    assert!(err.retryable, "429 should be marked retryable");
    assert_eq!(err.status, 429);
    server.await.unwrap();
}

// ---------- stream ----------

#[test]
fn pipe_chunk_passthrough_preserves_bytes() {
    let mut ctx = SseContext::new();
    let chunk = Bytes::from_static(b"data: {\"hello\":1}\n\n");
    let out = pipe_chunk(&mut ctx, &chunk);
    assert_eq!(
        out.passthrough, chunk,
        "passthrough must preserve bytes verbatim"
    );
}

#[test]
fn pipe_chunk_passthrough_and_scanner_contract() {
    // protocol::SseScanner is currently a stub (the TODO#502 line state
    // machine is not implemented): push always returns no events and finish always
    // returns Truncated. This verifies that forward's call contract is correct:
    // 1) passthrough is byte-for-byte faithful (forward's hard guarantee); 2)
    // events is a Vec and is consumable; 3) finish returns SseEnd (consumed by the
    // caller once the scanner completes; it must not be dropped).
    let mut ctx = SseContext::new();
    let frame = Bytes::from_static(b"data: {\"role\":\"assistant\"}\n\n");
    let out = pipe_chunk(&mut ctx, &frame);
    assert_eq!(
        out.passthrough, frame,
        "passthrough must preserve bytes verbatim"
    );
    assert_eq!(
        out.events.len(),
        1,
        "first data line should trigger FirstToken"
    );
    assert_eq!(
        out.events[0],
        gateway_protocol_bridge::sse::SseEvent::FirstToken
    );
    let (_end, _counts, _event) = finish(ctx, 200, None); // consume the scanner completion signal
}

#[test]
fn abort_guard_cancel_signals_watcher() {
    let (guard, _stop_rx) = AbortGuard::new();
    guard.cancel();
    // The cancellation signal travels via CancellationToken (cancelled() future);
    // oneshot is the stop channel
    let res = tokio::runtime::Runtime::new().unwrap().block_on(async {
        tokio::time::timeout(Duration::from_millis(200), guard.cancelled()).await
    });
    assert!(res.is_ok(), "after cancel(), cancelled() should be woken");
}

#[test]
fn forward_task_is_cloneable() {
    let task = ForwardTask {
        candidate: candidate("openai"),
        path: "/v1/chat/completions".to_string(),
        headers: vec![],
        body: Bytes::from_static(b"{}"),
        stream: false,
        provider_type: "openai".to_string(),
        extra_headers: vec![],
        inbound_format: gateway_pipeline::ProtocolKind::OpenAI,
    };
    let clone = task.clone();
    assert_eq!(clone.path, task.path);
    assert_eq!(clone.body, task.body);
    assert_eq!(clone.candidate.secret, task.candidate.secret);
}

// ---------- Public alias → upstream real-name rewriting ----------

/// Upstream recognizes only the real name: when the route unit maps `gpt-4` to
/// `gpt-4-0613`, the `model` in the outgoing body must be the real name, otherwise
/// the upstream reports model not found.
/// Regression: smoke found the alias mapping was not in effect, and the upstream
/// still received the public alias.
/// `resolve_upstream_model` decides the model in the Gemini path: channel mapping
/// first, falling back to the client by default.
/// Real HTTP loopback: when a Claude client (hitting /v1/messages) hits an `OpenAI`
/// channel, the upstream must receive /v1/chat/completions — an observable
/// reproduction of the bug fixed in this PR.
///
/// Full `forward_once` chain (prepare builds the real URL → reqwest really sends →
/// the response is converted back to the inbound format); the mock upstream returns
/// only a minimal Chat completion body; a conversion failure turns the test red
/// directly.
#[tokio::test]
async fn forward_once_routes_claude_client_to_openai_endpoint() {
    use std::io::Read;
    use std::net::TcpListener;

    // One-shot mock upstream: capture the request line and return a minimal
    // non-streaming Chat response.
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let (req_path_tx, req_path_rx) = std::sync::mpsc::channel::<String>();
    std::thread::spawn(move || {
        use std::io::Write;
        let (mut stream, _) = listener.accept().expect("accept");
        let mut buf = [0u8; 4096];
        let n = stream.read(&mut buf).expect("read");
        let req = String::from_utf8_lossy(&buf[..n]);
        // First segment of the request line: "POST /v1/chat/completions HTTP/1.1"
        let path = req
            .lines()
            .next()
            .and_then(|l| l.split_whitespace().nth(1))
            .unwrap_or_default()
            .to_string();
        let _ = req_path_tx.send(path);
        let body = serde_json::json!({
            "id":"chatcmpl-1","object":"chat.completion","created":0,
            "model":"m","choices":[{"index":0,"message":{
                "role":"assistant","content":"hi"},"finish_reason":"stop"}],
            "usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}
        });
        let bytes = body.to_string();
        let resp = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
             content-length: {}\r\nconnection: close\r\n\r\n{}",
            bytes.len(),
            bytes
        );
        let _ = stream.write_all(resp.as_bytes());
        let _ = stream.flush();
    });

    // Request body in Claude client shape + client path /v1/messages.
    let client_body = Bytes::from(
        serde_json::json!({
            "model":"m","max_tokens":16,
            "messages":[{"role":"user","content":"hi"}]
        })
        .to_string(),
    );
    let task = ForwardTask {
        candidate: candidate("openai"),
        path: "/v1/messages".to_string(),
        headers: vec![("accept".to_string(), "application/json".to_string())],
        body: client_body,
        stream: false,
        provider_type: "openai".to_string(),
        extra_headers: vec![],
        inbound_format: ProtocolKind::Anthropic,
    };
    // candidate()'s base_url points at example; switch it to the local mock
    // upstream.
    let mut task = task;
    task.candidate.base_url = format!("http://127.0.0.1:{port}");

    let egress = ReqwestEgress::new();
    let formats = Arc::new(FormatRegistry::with_defaults());
    let timeouts = Timeouts::default();

    let forwarded = gateway_forward::pipeline::forward_once(&task, &egress, &formats, &timeouts)
        .await
        .expect("forward must succeed");

    assert_eq!(forwarded.status, 200);
    // Core assertion: the path the upstream received is the OpenAI endpoint, not
    // the client's /v1/messages.
    assert_eq!(
        req_path_rx.recv().expect("upstream saw a request"),
        "/v1/chat/completions",
        "Claude client calling the OpenAI channel must land on chat/completions endpoint"
    );
}

/// Real contract for cross-format + upstream 4xx: `forward_once` must propagate the
/// upstream's 400 verbatim as Err(NormalizedError{status:400, retryable:false}),
/// not turn it into a retryable 502.
///
/// There is an easy-to-trip point on this path: both production Egress
/// implementations (`ReqwestEgress` / `AdapterEgress`) classify non-2xx as Err inside
/// execute and let `?` propagate it, so `translate_response` only ever sees 2xx. This
/// test locks that contract — if an Egress ever changes to passing non-2xx through
/// as Ok(ForwardedResponse), the error body would reach `translate_response` and fail
/// to parse as the success shape decoder, degrading the upstream's 400 into
/// ferrite's retryable 502 (inducing retries of an already rejected request).
#[tokio::test]
async fn cross_format_upstream_400_propagates_as_400_not_502() {
    use std::io::Read;
    use std::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().expect("addr").port();
    std::thread::spawn(move || {
        use std::io::Write;
        let (mut stream, _) = listener.accept().expect("accept");
        let mut buf = [0u8; 4096];
        let _ = stream.read(&mut buf).expect("read");
        let err = r#"{"error":{"message":"invalid_api_key","type":"invalid_request_error","code":"invalid_api_key"}}"#;
        let resp = format!(
            "HTTP/1.1 400 Bad Request\r\ncontent-type: application/json\r\n\
             content-length: {}\r\nconnection: close\r\n\r\n{}",
            err.len(),
            err
        );
        let _ = stream.write_all(resp.as_bytes());
        let _ = stream.flush();
    });

    let mut task = ForwardTask {
        candidate: candidate("openai"),
        path: "/v1/messages".to_string(),
        headers: vec![("accept".to_string(), "application/json".to_string())],
        body: Bytes::from(
            serde_json::json!({"model":"m","max_tokens":16,"messages":[{"role":"user","content":"hi"}]})
                .to_string(),
        ),
        stream: false,
        provider_type: "openai".to_string(),
        extra_headers: vec![],
        inbound_format: ProtocolKind::Anthropic,
    };
    task.candidate.base_url = format!("http://127.0.0.1:{port}");

    // Client says Anthropic, channel is openai: cross-format. Real reqwest path.
    let result = gateway_forward::pipeline::forward_once(
        &task,
        &ReqwestEgress::new(),
        &Arc::new(FormatRegistry::with_defaults()),
        &Timeouts::default(),
    )
    .await;

    let err = match result {
        Ok(forwarded) => panic!(
            "upstream 4xx must propagate as Err, not Ok(Forwarded): status={}",
            forwarded.status
        ),
        Err(e) => e,
    };

    assert_eq!(
        err.status, 400,
        "upstream 400 must stay 400, not become 502"
    );
    assert!(
        !err.retryable,
        "4xx means the request itself is bad, should not be retryable"
    );
    assert!(
        err.message.contains("invalid_api_key"),
        "upstream error body should go into message for error_mapping to convert to client format, actual: {}",
        err.message
    );
}

/// Real loopback contract for a non-2xx streaming upstream: when the upstream
/// replies 413 with an SSE content-type, the client must get 413 (not a retryable
/// 502), and the error body is converted to the inbound format shape by
/// `error_mapping`.
///
/// Note: this loopback **does not verify the pipeline guard itself** —
/// `ReqwestEgress` already classifies non-2xx as Err inside execute (see the note on
/// the non-streaming test above), so the request never reaches the guard; the
/// guard's fault injection is covered in `adaptor_wiring.rs`. This test locks the
/// client-facing contract that "413 does not degrade into 502 and the error shape
/// is returned in the inbound format".
#[tokio::test]
async fn stream_upstream_413_propagates_as_413_not_sse() {
    use std::io::Read;
    use std::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().expect("addr").port();
    std::thread::spawn(move || {
        use std::io::Write;
        let (mut stream, _) = listener.accept().expect("accept");
        let mut buf = [0u8; 4096];
        let _ = stream.read(&mut buf).expect("read");
        // Key point: the content-type is text/event-stream while the status code
        // is 413 — exactly the scenario that fools the streaming branch and lets
        // error bytes into the SSE scanner.
        let err = r#"{"error":{"message":"context_length_exceeded","type":"invalid_request_error","code":"context_length_exceeded"}}"#;
        let resp = format!(
            "HTTP/1.1 413 Payload Too Large\r\ncontent-type: text/event-stream\r\n\
             content-length: {}\r\nconnection: close\r\n\r\n{}",
            err.len(),
            err
        );
        let _ = stream.write_all(resp.as_bytes());
        let _ = stream.flush();
    });

    // Client says Anthropic (SSE), channel is openai: cross-format streaming, must
    // go through the inbound != upstream_format SSE conversion branch, not the
    // same-format passthrough shortcut.
    let mut task = ForwardTask {
        candidate: candidate("openai"),
        path: "/v1/messages".to_string(),
        headers: vec![("accept".to_string(), "text/event-stream".to_string())],
        body: Bytes::from(
            serde_json::json!({"model":"m","max_tokens":16,"stream":true,
                "messages":[{"role":"user","content":"hi"}]})
            .to_string(),
        ),
        stream: true,
        provider_type: "openai".to_string(),
        extra_headers: vec![],
        inbound_format: ProtocolKind::Anthropic,
    };
    task.candidate.base_url = format!("http://127.0.0.1:{port}");

    let result = gateway_forward::pipeline::forward_once(
        &task,
        &ReqwestEgress::new(),
        &Arc::new(FormatRegistry::with_defaults()),
        &Timeouts::default(),
    )
    .await;

    let err = match result {
        Ok(forwarded) => panic!(
            "streaming upstream 4xx must propagate as Err, not pass the error body through as SSE: status={} content_type={}",
            forwarded.status, forwarded.content_type
        ),
        Err(e) => e,
    };

    assert_eq!(
        err.status, 413,
        "upstream 413 must stay 413, not become 502"
    );
    assert!(
        !err.retryable,
        "4xx means the request itself is bad, should not be retryable (induces retries of an already rejected request)"
    );
    assert!(
        err.message.contains("context_length_exceeded"),
        "upstream error body should go into message for error_mapping to convert to client format, actual: {}",
        err.message
    );

    // Client side: the inbound format is Anthropic, so the error body must be in
    // Anthropic shape (not the upstream's OpenAI shape) — this chain is settled by
    // error_mapping::map_error.
    let shape = gateway_protocol_bridge::error_mapping::to_anthropic_shape(&err);
    assert_eq!(
        shape["type"], "error",
        "Anthropic error body top-level type must be error"
    );
    // Do not pin the whole message: classify_status truncates the preview to at
    // most 200 characters, and the exact client-side error text is up to the
    // upstream, so only the observable shape is locked.
}

/// model goes directly into a URL path segment, and the client may send any
/// string — invalid values must be rejected, not rewrite the upstream path.
#[test]
fn resolve_upstream_model_rejects_unsafe_for_url() {
    use gateway_forward::pipeline::resolve_upstream_model;
    // Invalid values from the channel mapping are rejected the same way (bad
    // config must not produce a malformed URL).
    assert_eq!(
        resolve_upstream_model(&Bytes::from(b"{}".as_ref()), "a/b"),
        ""
    );
    assert_eq!(
        resolve_upstream_model(&Bytes::from(b"{}".as_ref()), "../x"),
        ""
    );
    // A lone ".." is not traversal: in a URL it is one "..:verb" segment, so it is
    // allowed.
    assert_eq!(
        resolve_upstream_model(&Bytes::from(b"{}".as_ref()), ".."),
        ".."
    );
    // Invalid client model → no addressing.
    let bad = Bytes::from(r#"{"model":"x/../../etc"}"#);
    assert_eq!(resolve_upstream_model(&bad, ""), "");
    // Valid values pass as usual: model names containing . - _ are normal.
    assert_eq!(
        resolve_upstream_model(&Bytes::from(r#"{"model":"gemini-1.5.pro"}"#), ""),
        "gemini-1.5.pro"
    );
}

#[test]
fn resolve_upstream_model_prefers_channel_mapping() {
    use gateway_forward::pipeline::resolve_upstream_model;
    let body = Bytes::from(r#"{"model":"gpt-4","messages":[]}"#);
    // Channel has a real-name mapping → use the real name; the client alias is
    // void.
    assert_eq!(resolve_upstream_model(&body, "gpt-4-0613"), "gpt-4-0613");
    // No mapping (empty string) → fall back to the model the client sent.
    assert_eq!(resolve_upstream_model(&body, ""), "gpt-4");
    // No mapping and a non-JSON body → empty, and build_url falls back to the
    // client path.
    assert_eq!(
        resolve_upstream_model(&Bytes::from(b"not json".as_ref()), ""),
        ""
    );
    // No model field → empty.
    assert_eq!(resolve_upstream_model(&Bytes::from(r#"{"foo":1}"#), ""), "");
}

#[test]
fn rewrite_upstream_model_replaces_alias() {
    let body =
        Bytes::from_static(br#"{"model":"gpt-4","messages":[{"role":"user","content":"x"}]}"#);
    let out = gateway_forward::pipeline::rewrite_upstream_model(&body, "gpt-4-0613");
    let v: serde_json::Value = serde_json::from_slice(&out).expect("still valid JSON");
    assert_eq!(v["model"], "gpt-4-0613");
    // Other fields must not be damaged by the rewrite.
    assert_eq!(v["messages"][0]["content"], "x");
}

/// Pass through when there is no reliable rewrite position, without guessing or
/// erroring: empty real name, non-JSON, no model field.
#[test]
fn rewrite_upstream_model_passes_through_when_not_applicable() {
    let json = Bytes::from_static(br#"{"model":"gpt-4"}"#);
    assert_eq!(
        gateway_forward::pipeline::rewrite_upstream_model(&json, ""),
        json,
        "passthrough as-is when upstream real name is empty"
    );

    let not_json = Bytes::from_static(b"not json at all");
    assert_eq!(
        gateway_forward::pipeline::rewrite_upstream_model(&not_json, "m"),
        not_json,
        "passthrough as-is for non-JSON body"
    );

    // No model field = a non-chat request (e.g. /v1/models); model must not be
    // inserted out of nowhere.
    let no_model = Bytes::from_static(br#"{"input":"hi"}"#);
    assert_eq!(
        gateway_forward::pipeline::rewrite_upstream_model(&no_model, "m"),
        no_model,
        "do not insert when model field is missing"
    );
}
