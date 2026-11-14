//! # forward — upstream forwarding and protocol adaptation (hot path step 3)
//!
//! Responsibility: send admitted, routed requests upstream and stream the response back
//! unchanged. This crate does **IO orchestration** only; format conversion belongs to the
//! protocol crate.
//!
//! ## Module map
//!
//! | Module | Responsibility |
//! |------|------|
//! | [`pipeline`] | full forwarding pipeline for one attempt (build request → send → stream back) |
//! | [`stage`]    | `ForwardStage`: single/retry orchestration (hooks in stage_* modules) |
//! | [`adapter`]  | upstream request prep: URL/headers/auth (2 of new-api adaptor's five methods) |
//! | [`adapter_egress`] | protocol egress: `StreamDialer` + `AdapterDialer` (meow) + TLS/proxy dial |
//! | [`egress`]   | direct egress client pool: timeouts/HTTP2 shards/alias shards (proxy dial is [`adapter_egress`]) |
//! | [`stream`]   | SSE pipe: frames → `SseScanner` → `StreamScanner` → client; `body_wants_stream` |
//! | [`stream_resilience`] | P2 resilience: buffer window/heartbeat/disconnect classification |
//!
//! ## Design points (investigation conclusions)
//!
//! 1. **Passthrough first**: same protocol = zero-conversion byte passthrough; only
//!    cross-protocol goes through `gateway_protocol_bridge::FormatRegistry`;
//! 2. **Streaming first**: Bytes chunk pipeline, never buffer the whole response;
//! 3. **Retry boundary**: this layer only reports `FailureClass` to `gateway_dispatch::retry`
//!    and never swaps candidates itself;
//! 4. **Replayable**: request body cached (Bytes pool), so retries avoid re-reading the
//!    original body.

pub mod adapter;
pub mod adapter_egress;
pub mod egress;
pub mod pipeline;
pub mod stage;
mod stage_cache;
mod stage_commit;
mod stage_concurrency;
mod stage_retry;
mod stage_sticky;
mod stage_telemetry;
pub mod stream;
pub mod stream_resilience;

pub use stage::ForwardStage;
pub use stream_resilience::{
    BufferedStream, ResilientChunk, StreamPhase, StreamResilience, WindowDrain,
    drain_stream_window, has_completion_evidence, pipe_resilient,
};

use bytes::Bytes;

/// Unified constructor for admission-side 429 rejections (concurrency slots full /
/// token budget window full).
///
/// Concurrency/budget rejections are not upstream errors: they are `retryable` and
/// channel-independent, the caller does **not** report `submit_failed`, and the health
/// side treats them as Throttled.
pub(crate) fn rate_limited(message: impl Into<String>) -> gateway_core::error::NormalizedError {
    gateway_core::error::NormalizedError {
        code: gateway_core::error::code::RATE_LIMITED,
        status: 429,
        retryable: true,
        channel_scoped: false,
        message: message.into(),
        retry_after_ms: None,
    }
}

/// All inputs of one forwarding attempt (assembled per attempt by [`ForwardStage`]).
#[derive(Debug, Clone)]
pub struct ForwardTask {
    /// Selected candidate (`gateway_dispatch::Candidate`).
    pub candidate: gateway_dispatch::Candidate,
    /// Original client request path (e.g. /v1/chat/completions).
    pub path: String,
    /// Client headers to pass through (hop-by-hop/auth headers already filtered).
    pub headers: Vec<(String, String)>,
    /// Request body (replayable).
    pub body: Bytes,
    /// Whether this is SSE (decided by the client's Accept).
    pub stream: bool,
    /// Channel protocol family ("openai" / "claude" / "gemini" / "passthrough") — the
    /// adapter picks the path template and auth header by it. The candidate carries the
    /// channel's `provider_type`, resolved once at routing time.
    /// ponytail: `provider_type` travels with the candidate, so forward never re-queries the
    /// catalog snapshot.
    pub provider_type: String,
    /// Channel `settings.headers` override headers (the parsed `Vec` form of
    /// `ChannelRecord.settings.headers`, already lower-cased; parsed once when the
    /// `ForwardTask` is assembled).
    /// The full settings JSON is consumed by metering/audit; forward only cares about the
    /// headers subset.
    pub extra_headers: Vec<(String, String)>,
    /// Protocol the client speaks (decided by the request path) — the request direction
    /// decodes by it and the response direction encodes by it, so Claude/Gemini/Responses
    /// clients get responses in their own format.
    ///
    /// Older versions lacked this field: the request direction was hard-coded `OpenAI`, the
    /// response always converted to `OpenAI`, and the streaming path did no conversion at all
    /// (G2). The host injects it from `RequestCtx.request.inbound_protocol`.
    pub inbound_format: gateway_pipeline::ProtocolKind,
}

/// Forwarding result: upstream status code + response stream.
pub struct Forwarded {
    pub status: u16,
    /// Response byte stream (SSE or a plain body, same shape).
    pub body:
        std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<Bytes, std::io::Error>> + Send>>,
    /// Response content type (passed through to the client unchanged).
    pub content_type: String,
    /// Per-channel concurrency permit (retry mode attaches it after the winning attempt
    /// acquires a channel slot).
    ///
    /// Lifetime follows the response: for streaming, the unfold stream state in
    /// `commit_forwarded` releases it via RAII when the stream ends (fully read / error /
    /// dropped after a client disconnect); for non-streaming, it is released when the
    /// enclosing scope ends after `commit_forwarded` collects the whole body. `None` = no
    /// per-channel gate attached, or the channel has no registered concurrency limit
    /// (unlimited). Keeping it on `Forwarded` is what lets P2C's `available_permits` reflect
    /// real concurrent responses rather than only established connections.
    pub channel_permit: Option<tokio::sync::OwnedSemaphorePermit>,
    /// Channel token budget pre-claim handle (attempt-level soft budget, mirroring the
    /// `channel_permit` RAII shape): after the winning attempt passes `TokenBudgetState`
    /// window admission, `preclaim` reserves the estimated in-flight tokens and the handle
    /// rides with this struct for the lifetime of the response — streaming releases it via
    /// the `commit_forwarded` unfold stream state's RAII on stream end/drop, non-streaming
    /// with the commit scope; the settle commit point (metering settlement) calls
    /// `settle_actual` first to refund the difference (idempotent, independent of Drop
    /// order), and Drop returns whatever is still held — no path leaks. `None` = no token
    /// budget attached (behavior = status quo).
    pub token_handle: Option<gateway_dispatch::token_budget::TokenHandle>,
}

impl std::fmt::Debug for Forwarded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // body is an opaque byte stream; Debug reports only the status code, content type,
        // whether a channel slot is held, and whether a token budget handle is held —
        // test failure messages need "why Ok rather than Err", not the whole stream.
        f.debug_struct("Forwarded")
            .field("status", &self.status)
            .field("content_type", &self.content_type)
            .field("channel_permit", &self.channel_permit.is_some())
            .field("token_handle", &self.token_handle.is_some())
            .finish_non_exhaustive()
    }
}
