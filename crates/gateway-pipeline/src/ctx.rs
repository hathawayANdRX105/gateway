//! `ctx` — request context shared across stages
//!
//! Design notes:
//! - `RequestCtx` exists only for the current request and **must not be shared across requests**.
//! - State shared across requests (token table / route index) lives in `ArcSwap`; each stage
//!   explicitly `.load()`s it.
//! - `StageOutcome` is a three-state enum: Continue / `ShortCircuit` / Stream (defined in `stage`).

use std::net::IpAddr;

use gateway_core::metering::Hold;
use gateway_core::protocol::{ProtocolKind, RequestPath};

use crate::stage::StageError;
use bytes::Bytes;
use http::{Extensions, HeaderMap};
use uuid::Uuid;

/// Request body source (one read per attempt: in-memory buffer or on-disk file)
///
/// Each upstream attempt gets a fresh `Reader`, so it never reads at EOF. In memory the reader
/// borrows a `Bytes`; on disk it carries the file path and length.
#[derive(Debug, Clone)]
pub enum BodySource {
    InMemory(Bytes),
    OnDisk { path: std::path::PathBuf, len: u64 },
}

impl BodySource {
    /// Get a fresh read cursor (for retries / multiple attempts)
    pub fn reader(&self) -> BodyReader<'_> {
        match self {
            BodySource::InMemory(b) => BodyReader::Memory(b),
            BodySource::OnDisk { path, len } => BodyReader::Disk { path, len: *len },
        }
    }
}

pub enum BodyReader<'a> {
    Memory(&'a Bytes),
    Disk {
        path: &'a std::path::PathBuf,
        len: u64,
    },
}

/// Immutable request input
#[derive(Debug, Clone)]
pub struct RequestMeta {
    pub method: String,
    pub path: String,
    pub headers: HeaderMap,
    pub body: BodySource,
    pub client_ip: IpAddr,
    pub request_id: Uuid,
    pub inbound_protocol: ProtocolKind,
}

/// Route selection output (written by Dispatch) — a complete target that can be forwarded
/// directly.
///
/// Resolution completeness is the core invariant of this type: once forward receives it, it
/// **must not consult any snapshot again**. `provider_type` and `settings` come from
/// `ChannelRecord` and are resolved together during selection by dispatch's `resolve_candidate`,
/// so forward does not reverse-depend on the catalog snapshot.
///
/// Why this lives in `pipeline` rather than `dispatch`: `pipeline` is the common foundation of
/// all stage crates and does not reverse-depend on any concrete stage (see the crate docs). The
/// routing product must cross stages via `RequestCtx`, so it can only live in the foundation;
/// `gateway_dispatch::Candidate` is an alias of this type.
///
/// `channel_key` (`unit.channel_key`) = `ChannelRecord.meta.key`, the lease key for the channel's
/// egress proxy. `ProxyManager::acquire(channel_key)` selects a node with it.
#[derive(Debug, Clone)]
pub struct SelectedRoute {
    /// Matched route unit (raw record, referenced by metering / logging).
    pub unit: gateway_core::records::RouteUnitRecord,
    /// Resolved upstream credential (`channel.keys[key_index].secret`).
    pub secret: String,
    /// Upstream `base_url` (path joining rules in `gateway_forward::adapter::build_url`).
    pub base_url: String,
    /// Upstream real model name (`unit.upstream_model`); used for the model field sent upstream.
    pub upstream_model: String,
    /// Channel protocol family (`ChannelRecord.provider_type`): "openai" / "claude" /
    /// "gemini" / "passthrough"; determines auth headers and the path template.
    pub provider_type: String,
    /// Channel-level overrides (`ChannelRecord.settings`); the `headers` subset is consumed by
    /// forward, the rest is left for metering and auditing.
    pub settings: serde_json::Value,
}

/// Channel attribution for this request (used when writing usage).
///
/// `Pipeline::run` packages this from `RequestCtx` before returning the response and writes it
/// to `response.extensions()`: ctx is consumed inside run (especially on the streaming branch —
/// once the body is taken over by `SsePipe` the caller cannot get ctx back), so response extensions
/// are the only channel middleware can read.
///
/// `channel_key` = `ChannelRecord.meta.key` (UUID string, → `usage_logs.channel_key`);
/// `channel_name` is a denormalized display name, an empty string when the snapshot has no match;
/// `model` is the public alias (`requested_model`), matching `usage_logs.model_name`.
#[derive(Debug, Clone)]
pub struct RouteAttribution {
    pub channel_key: String,
    pub channel_name: String,
    pub model: String,
}

/// Upstream response (non-streaming, written by Forward)
#[derive(Debug)]
pub struct UpstreamResponse {
    pub status: u16,
    pub body: Bytes,
}

/// Streaming accumulation (written by `StreamingIntercept`)
#[derive(Debug, Default, Clone)]
pub struct StreamedAccum {
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub first_token_at: Option<std::time::Instant>,
}

/// Streaming response (Forward → client handover)
///
/// Wraps the axum Body and the upstream `content-type`; `gateway-forward`'s `SsePipe` constructs it
/// via [`PipeStream::with_content_type`] / [`PipeStream::new`].
#[derive(Debug)]
pub struct PipeStream {
    body: axum::body::Body,
    content_type: String,
}

impl PipeStream {
    /// Build from an upstream stream, explicitly supplying the `content-type` returned to the
    /// client.
    ///
    /// It must be included: SSE clients (`OpenAI` / Anthropic SDKs) use `text/event-stream` to
    /// decide to read the response as an event stream; without it they collect the whole response
    /// as an ordinary body at once.
    pub fn with_content_type(body: axum::body::Body, content_type: impl Into<String>) -> Self {
        Self {
            body,
            content_type: content_type.into(),
        }
    }

    /// Build from an upstream SSE stream (called by forward); content-type is `text/event-stream`.
    #[must_use]
    pub fn new(body: axum::body::Body) -> Self {
        Self::with_content_type(body, "text/event-stream")
    }

    /// Convert into an axum Response carrying the upstream `content-type`.
    pub fn into_response(self) -> http::Response<axum::body::Body> {
        http::Response::builder()
            .header(http::header::CONTENT_TYPE, &self.content_type)
            .body(self.body)
            // The header value comes from the upstream content-type string; an invalid value
            // falls back to a response without the header instead of panicking.
            .unwrap_or_else(|_| http::Response::new(axum::body::Body::empty()))
    }
}

/// RAII guard collection that stages place in the request context.
///
/// Stages push temporary resources that must live for the request lifetime (such as the
/// `OwnedSemaphorePermit` issued by `gateway-gate`'s `ConcurrencyGate`) into
/// [`RequestCtx::drop_guards`] as `Box<dyn Send + Sync>`; when the request ends and `RequestCtx`
/// is dropped, the guards drop with it and the underlying resources return automatically — no
/// explicit release call, and no slot leak from a lost handle.
///
/// **Known limitation**: for streaming responses the handler returns `StageOutcome::Stream` and
/// ctx is dropped immediately, so guards are released during **stream setup**, not at the end of
/// the whole stream. The field currently accepts that semantics (the concurrency slot covers the
/// window in which "the request is admitted and forwarding is established"); binding guards to
/// the full stream lifetime is follow-up work.
pub type DropGuards = Vec<Box<dyn Send + Sync>>;

/// Mutable context shared across stages
///
/// Each field is filled only after a specific stage; unfilled fields are None.
pub struct RequestCtx {
    /// Immutable input
    pub request: RequestMeta,

    /// Written by Admission: authenticated token metadata
    pub token: Option<super::TokenInfo>,

    /// Written by Dispatch: selected route
    pub route: Option<SelectedRoute>,

    /// Written by Dispatch: attribution key of the selected channel (= `route.unit.channel_key`,
    /// duplicated so readers can avoid destructuring route). `Pipeline::run` uses it to package
    /// [`RouteAttribution`].
    pub selected_channel_key: Option<String>,

    /// Written by Dispatch: display name of the selected channel (looked up from the Dispatcher
    /// snapshot; None if not found).
    pub selected_channel_name: Option<String>,

    /// Explicit session id (`x-session-id` header, or extracted by the assembly layer from
    /// another source). The **strong** key source for prefix-cache stickiness:
    /// `gateway_dispatch::StickyKey` prefers it (`"sid:" + value`); the same session hits the same
    /// key no matter how the request body grows, and it is consistent across client protocols.
    /// `None` = no session header, so stickiness falls back to a raw body-prefix fingerprint.
    /// Prefilled during bin/api assembly, or read directly from `request.headers` by `DispatchStage`.
    pub session_hint: Option<String>,

    /// Written by `Gate::model`: model name parsed from the request body (dispatch uses it for
    /// lookup).
    pub requested_model: Option<String>,

    /// Written by `Gate::model` (promoted through the admission chain with the model name):
    /// `max_tokens` / `max_completion_tokens` parsed from the request body. Dispatch uses it for
    /// the context-fit estimate (`prompt + max_tokens` must fit the selected channel's window);
    /// `None` = the request carried no explicit budget.
    pub requested_max_tokens: Option<u32>,

    /// Written by Forward: non-streaming response body
    pub upstream: Option<UpstreamResponse>,

    /// Written by `StreamingIntercept`: streaming accumulation
    pub streamed: StreamedAccum,

    /// Any stage may write: cross-stage error (do not return directly; handle via `StageOutcome`)
    pub error: Option<StageError>,
    /// Written by the gate chain: the prehold taken by `QuotaGate` at admission.
    /// Settled by the forward commit point (actual cost) or released when a later gate
    /// rejects or the response is dropped; `None` until the gate chain finishes.
    pub hold: Option<Hold>,

    /// Any stage may write: RAII guards dropped with ctx at the end of this request (semantics
    /// and known limitations in [`DropGuards`]).
    pub drop_guards: DropGuards,
}

impl RequestCtx {
    pub fn new(request: RequestMeta) -> Self {
        Self {
            request,
            token: None,
            route: None,
            selected_channel_key: None,
            selected_channel_name: None,
            requested_model: None,
            requested_max_tokens: None,
            session_hint: None,
            upstream: None,
            streamed: StreamedAccum::default(),
            error: None,
            hold: None,
            drop_guards: Vec::new(),
        }
    }

    /// Build from an axum Request (bin/api entry point)
    ///
    /// Extracts method / path / headers / body / `client_ip` / `request_id` and infers the protocol
    /// type from the path.
    ///
    /// # Errors
    ///
    /// Returns the body-collection error when the request body cannot be read into
    /// memory (the axum limit is `usize::MAX`, so this is a transport failure).
    pub async fn from_axum(req: http::Request<axum::body::Body>) -> anyhow::Result<Self> {
        let (parts, body) = req.into_parts();
        let body_bytes = axum::body::to_bytes(body, usize::MAX).await?;

        let client_ip = client_ip(&parts.headers, &parts.extensions);

        Ok(Self::from_parts(
            &parts.method,
            parts.uri.path().to_string(),
            parts.headers,
            body_bytes,
            client_ip,
        ))
    }

    /// Build from transport-neutral HTTP parts.
    ///
    /// Any HTTP server adapter can produce these values: method, path, headers, the
    /// fully-read body and the peer address. Kept separate from [`Self::from_axum`] so a
    /// non-axum transport (the standalone actix-web process) assembles the same context
    /// instead of re-implementing protocol inference.
    pub fn from_parts(
        method: &http::Method,
        path: String,
        headers: HeaderMap,
        body: Bytes,
        client_ip: IpAddr,
    ) -> Self {
        let inbound_protocol = RequestPath::parse(&path).protocol();

        Self::new(RequestMeta {
            method: method.to_string(),
            path,
            headers,
            body: BodySource::InMemory(body),
            client_ip,
            request_id: Uuid::now_v7(),
            inbound_protocol,
        })
    }
}

/// Client IP: prefer the first `x-forwarded-for` segment, then the connection-layer `SocketAddr`;
/// fall back to the unspecified address when neither exists.
fn client_ip(headers: &HeaderMap, extensions: &Extensions) -> IpAddr {
    headers
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(',').next())
        .and_then(|s| s.trim().parse().ok())
        .or_else(|| {
            extensions
                .get::<std::net::SocketAddr>()
                .map(std::net::SocketAddr::ip)
        })
        .unwrap_or(IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED))
}
