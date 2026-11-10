//! Behaviour tests for the pipeline orchestration core: `StageOutcome` three-state flow,
//! error short-circuiting, router integration.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use gateway_pipeline::{
    PipeStream, Pipeline, RequestCtx, RequestMeta, Stage, StageError, StageOutcome,
};
use std::net::IpAddr;
use std::sync::Arc;
use uuid::Uuid;

// ---------- Test helpers ----------

/// Build a minimal `RequestMeta` (empty body)
fn meta(path: &str) -> RequestMeta {
    RequestMeta {
        method: "POST".to_string(),
        path: path.to_string(),
        headers: axum::http::HeaderMap::new(),
        body: gateway_pipeline::BodySource::InMemory(bytes::Bytes::new()),
        client_ip: IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        request_id: Uuid::now_v7(),
        inbound_protocol: gateway_pipeline::ProtocolKind::OpenAI,
    }
}

/// Stub stage that records call order — the behaviour is chosen by `Behavior`, avoiding a
/// stored `StageOutcome` (which holds a non-Send axum Body) that would break Stage:
/// Send+Sync.
enum Behavior {
    Continue,
    ShortCircuit(u16),
    Stream,
}

struct SpyStage {
    name: &'static str,
    behavior: Behavior,
    calls: Arc<std::sync::atomic::AtomicU32>,
}

#[async_trait::async_trait]
impl Stage for SpyStage {
    fn name(&self) -> &'static str {
        self.name
    }
    async fn handle(&self, _ctx: &mut RequestCtx) -> Result<StageOutcome, StageError> {
        self.calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(match self.behavior {
            Behavior::Continue => StageOutcome::Continue,
            Behavior::ShortCircuit(status) => StageOutcome::ShortCircuit(
                axum::http::Response::builder()
                    .status(status)
                    .body(Body::empty())
                    .unwrap(),
            ),
            Behavior::Stream => StageOutcome::Stream(PipeStream::new(Body::empty())),
        })
    }
}

// ---------- StageOutcome flow ----------

/// Covers the all-`Continue` tail path: a chain that never produces a response must
/// fail with `StageError::Internal`, while still invoking every registered stage.
#[test]
fn pipeline_continue_runs_all_stages() {
    let calls = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let pipe = Pipeline::new()
        .push(SpyStage {
            name: "a",
            behavior: Behavior::Continue,
            calls: calls.clone(),
        })
        .push(SpyStage {
            name: "b",
            behavior: Behavior::Continue,
            calls: calls.clone(),
        });

    let ctx = RequestCtx::new(meta("/v1/chat/completions"));
    let result = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(pipe.run(ctx));
    // Every stage returned Continue with no output → internal error (the pipeline cannot
    // conjure a response)
    assert!(matches!(result, Err(StageError::Internal(_))));
    assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 2);
}

/// Covers `ShortCircuit`: the chain stops at the stage returning `ShortCircuit`, its
/// response (status preserved) is returned, and all later stages are skipped.
#[test]
fn pipeline_short_circuit_stops_chain() {
    let calls = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let pipe = Pipeline::new()
        .push(SpyStage {
            name: "first",
            behavior: Behavior::Continue,
            calls: calls.clone(),
        })
        .push(SpyStage {
            name: "stop",
            behavior: Behavior::ShortCircuit(401),
            calls: calls.clone(),
        })
        .push(SpyStage {
            name: "never",
            behavior: Behavior::Continue,
            calls: calls.clone(),
        });

    let ctx = RequestCtx::new(meta("/v1/chat/completions"));
    let result = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(pipe.run(ctx));
    let resp = result.expect("ShortCircuit should return Ok");
    assert_eq!(resp.status(), 401);
    assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 2);
}

/// Covers `Stream`: the chain terminates on the first `Stream` outcome, returns `Ok`,
/// and does not invoke any subsequent stage.
#[test]
fn pipeline_stream_terminates_immediately() {
    let calls = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let pipe = Pipeline::new()
        .push(SpyStage {
            name: "first",
            behavior: Behavior::Stream,
            calls: calls.clone(),
        })
        .push(SpyStage {
            name: "never",
            behavior: Behavior::Continue,
            calls: calls.clone(),
        });

    let ctx = RequestCtx::new(meta("/v1/chat/completions"));
    let result = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(pipe.run(ctx));
    assert!(result.is_ok(), "Stream should return the response directly");
    assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 1);
}

// ---------- Channel attribution packaging ----------

/// Mimics `DispatchStage`'s write contract: set route + `selected_channel`_*, then return
/// Stream.
struct AttributionStreamStage;

#[async_trait::async_trait]
impl Stage for AttributionStreamStage {
    fn name(&self) -> &'static str {
        "attribution-stream"
    }
    async fn handle(&self, ctx: &mut RequestCtx) -> Result<StageOutcome, StageError> {
        ctx.route = Some(gateway_pipeline::SelectedRoute {
            unit: gateway_core::records::RouteUnitRecord {
                meta: gateway_core::records::SyncMeta {
                    key: "u1".into(),
                    schema_version: 1,
                    logical_version: 1,
                    origin: "test".into(),
                    updated_at: chrono::Utc::now(),
                },
                group: "g".into(),
                public_model: "m".into(),
                channel_key: "ch-uuid".into(),
                key_index: 0,
                upstream_model: "m".into(),
                weight: 10,
                status: 1,
            },
            secret: "s".into(),
            base_url: "https://u".into(),
            upstream_model: "m".into(),
            provider_type: "openai".into(),
            settings: serde_json::Value::Null,
        });
        ctx.selected_channel_key = Some("ch-uuid".into());
        ctx.selected_channel_name = Some("Channel A".into());
        ctx.requested_model = Some("m".into());
        Ok(StageOutcome::Stream(PipeStream::new(Body::empty())))
    }
}

/// The streaming exit must carry channel attribution back too: ctx is consumed inside run
/// (once the body is handed to `SsePipe` the handler cannot get ctx back), so packaging can
/// only happen before run returns. The usage middleware reads the persisted fields from
/// extensions accordingly — losing attribution on streaming requests would drop half of the
/// per-channel usage statistics.
#[test]
fn run_attaches_attribution_to_stream_response() {
    let pipe = Pipeline::new().push(AttributionStreamStage);
    let ctx = RequestCtx::new(meta("/v1/chat/completions"));
    let resp = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(pipe.run(ctx))
        .expect("Stream should return Ok");
    let attr = resp
        .extensions()
        .get::<gateway_pipeline::RouteAttribution>()
        .expect("Streaming response should carry RouteAttribution");
    assert_eq!(attr.channel_key, "ch-uuid");
    assert_eq!(attr.channel_name, "Channel A");
    assert_eq!(attr.model, "m");
}

/// No attribution is inserted when no route was selected (a short-circuit before dispatch,
/// e.g. auth rejection): there is no "actually hit channel", so the usage side treats it as
/// missing rather than writing a ledger entry with an empty channel.
#[test]
fn short_circuit_without_route_has_no_attribution() {
    let calls = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let pipe = Pipeline::new().push(SpyStage {
        name: "reject",
        behavior: Behavior::ShortCircuit(401),
        calls,
    });
    let ctx = RequestCtx::new(meta("/v1/chat/completions"));
    let resp = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(pipe.run(ctx))
        .expect("ShortCircuit should return Ok");
    assert!(
        resp.extensions()
            .get::<gateway_pipeline::RouteAttribution>()
            .is_none(),
        "An unselected route must not insert attribution"
    );
}

/// A streaming response must carry `content-type`.
///
/// SSE clients (`OpenAI` / Anthropic SDK) use `text/event-stream` to decide to read the
/// response as an event stream; without it, clients take the body in one go and streaming
/// degrades into blocking waits. Regression: `PipeStream::into_response` once wrapped the
/// body with `Response::new` directly and dropped the header.
#[test]
fn pipe_stream_response_carries_content_type() {
    let sse = PipeStream::new(axum::body::Body::from("data: {}\n\n"));
    let resp = sse.into_response();
    assert_eq!(
        resp.headers().get(http::header::CONTENT_TYPE).unwrap(),
        "text/event-stream",
        "Default construction should set SSE type"
    );

    // The upstream type is passed through verbatim (non-SSE streaming responses must stay
    // faithful too).
    let json = PipeStream::with_content_type(
        axum::body::Body::from("{}"),
        "application/json; charset=utf-8",
    );
    assert_eq!(
        json.into_response()
            .headers()
            .get(http::header::CONTENT_TYPE)
            .unwrap(),
        "application/json; charset=utf-8"
    );
}

// ---------- Error short-circuit ----------

struct ErrStage;

#[async_trait::async_trait]
impl Stage for ErrStage {
    fn name(&self) -> &'static str {
        "err"
    }
    async fn handle(&self, _ctx: &mut RequestCtx) -> Result<StageOutcome, StageError> {
        Err(StageError::Unauthenticated("bad key".into()))
    }
}

/// Covers an `Err` from `stage.handle`: propagation stops the chain immediately and
/// surfaces the original `StageError` unchanged.
#[test]
fn pipeline_error_stops_chain() {
    let pipe = Pipeline::new().push(ErrStage);
    let ctx = RequestCtx::new(meta("/v1/chat/completions"));
    let result = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(pipe.run(ctx));
    assert!(matches!(result, Err(StageError::Unauthenticated(_))));
}

// ---------- RequestCtx::from_axum ----------

/// Covers `RequestCtx::from_axum`: it extracts method, path, client IP (via
/// `x-forwarded-for`) and infers the inbound protocol, reading the body into `InMemory`.
#[test]
fn from_axum_extracts_meta_and_protocol() {
    let req = Request::builder()
        .method("POST")
        .uri("/v1/messages")
        .header("content-type", "application/json")
        .header("x-forwarded-for", "203.0.113.9")
        .body(Body::from(r#"{"model":"claude-3-5"}"#))
        .unwrap();
    let ctx = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(RequestCtx::from_axum(req))
        .unwrap();
    assert_eq!(ctx.request.method, "POST");
    assert_eq!(ctx.request.path, "/v1/messages");
    assert_eq!(
        ctx.request.client_ip,
        "203.0.113.9".parse::<IpAddr>().unwrap()
    );
    assert_eq!(
        ctx.request.inbound_protocol,
        gateway_pipeline::ProtocolKind::Anthropic
    );
    // The body has been read into InMemory
    assert!(matches!(
        ctx.request.body,
        gateway_pipeline::BodySource::InMemory(_)
    ));
}

/// Covers inbound protocol classification through `from_axum`: the four dialect
/// families, the `OpenAI` fallback, and the tightened segment rule — a path merely
/// containing "gemini" outside the Gemini dialect is no longer misclassified.
#[test]
fn from_axum_classifies_inbound_protocol() {
    let cases = [
        (
            "/v1/chat/completions",
            gateway_pipeline::ProtocolKind::OpenAI,
        ),
        ("/v1/messages", gateway_pipeline::ProtocolKind::Anthropic),
        ("/v1/responses", gateway_pipeline::ProtocolKind::OpenAIResp),
        (
            "/v1beta/models/gemini-2.5-pro",
            gateway_pipeline::ProtocolKind::Gemini,
        ),
        (
            "/v1/chat/completions?model=gpt-4o",
            gateway_pipeline::ProtocolKind::OpenAI,
        ),
        (
            "/v1/embeddings-gemini",
            gateway_pipeline::ProtocolKind::OpenAI,
        ),
    ];
    for (path, expected) in cases {
        let req = Request::builder()
            .method("POST")
            .uri(path)
            .body(Body::empty())
            .unwrap();
        let ctx = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(RequestCtx::from_axum(req))
            .unwrap();
        assert_eq!(ctx.request.inbound_protocol, expected, "path={path}");
    }
}

// ---------- router integration ----------

/// Covers status/body mapping for representative error variants — quota exhausted,
/// no-route (incl. 404 body context) and unauthenticated — through `error_to_response`.
#[test]
fn error_to_response_maps_status() {
    let resp = gateway_pipeline::error_to_response(StageError::QuotaExhausted {
        remaining: 0,
        required: 10,
    });
    assert_eq!(resp.status(), StatusCode::PAYMENT_REQUIRED);

    let resp = gateway_pipeline::error_to_response(StageError::NoRoute {
        group: "g".into(),
        model: "m".into(),
    });
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    // The 404 error body must carry the no-route group/model context (the NoRoute arm of
    // router::error_to_response: format!("model not configured for {group}/{model}")).
    let body = tokio::runtime::Runtime::new().unwrap().block_on(async {
        axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap()
    });
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["error"]["code"], "no_route");
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("for g/m"),
        "404 message must include group/model: {}",
        json["error"]["message"]
    );

    let resp = gateway_pipeline::error_to_response(StageError::Unauthenticated("no key".into()));
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

// ---------- gate: Unavailable promises Retry-After only while a channel is cooling ----------

/// Covers the cooling case (`retry_after_ms > 0`): maps to 504 with `Retry-After`
/// set to the ceiling of ms/1000.
#[test]
fn unavailable_cooling_emits_retry_after_header() {
    let resp = gateway_pipeline::error_to_response(StageError::Unavailable {
        group: "g".into(),
        model: "m".into(),
        retry_after_ms: 8_000,
    });
    assert_eq!(
        resp.status(),
        StatusCode::GATEWAY_TIMEOUT,
        "Unavailable while cooling should be 504"
    );
    assert_eq!(
        resp.headers()
            .get(axum::http::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok()),
        Some("8"),
        "8000ms cooling → Retry-After: 8 (ceil)"
    );
}

/// Covers the no-cooling case (`retry_after_ms == 0`): all-disabled / rate-limited /
/// genuinely-no-model unavailable maps to 503 and promises no retry time, because an
/// immediate resend is not actionable.
#[test]
fn unavailable_not_cooling_maps_to_503() {
    let resp = gateway_pipeline::error_to_response(StageError::Unavailable {
        group: "g".into(),
        model: "m".into(),
        retry_after_ms: 0,
    });
    assert_eq!(
        resp.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "No cooling (ra=0) should be 503 unavailable, not 504"
    );
    assert!(
        resp.headers()
            .get(axum::http::header::RETRY_AFTER)
            .is_none(),
        "No cooling (ra=0) must not carry Retry-After header"
    );
}

/// Covers the `Retry-After` conversion boundary: seconds must be the ceiling of
/// `retry_after_ms / 1000`, including partial-second cases such as `1_999 -> 2`.
#[test]
fn unavailable_retry_after_is_ceiling_of_cooling_ms() {
    for (ms, expected) in [(1_000u64, "1"), (1_999, "2"), (8_000, "8"), (8_500, "9")] {
        let resp = gateway_pipeline::error_to_response(StageError::Unavailable {
            group: "g".into(),
            model: "m".into(),
            retry_after_ms: ms,
        });
        assert_eq!(
            resp.headers()
                .get(axum::http::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok()),
            Some(expected),
            "Retry-After = ceil({ms}ms / 1000)"
        );
    }
}
