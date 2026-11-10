//! `stage` — the Stage trait + outcome / error definitions
//!
//! Every concrete stage (`AdmissionStage` / `DispatchStage` / `ForwardStage` / `ProtocolBridgeStage`)
//! implements this trait.

use crate::ctx::{PipeStream, RequestCtx};
use async_trait::async_trait;
use thiserror::Error;

/// One processing node in the chain
#[async_trait]
pub trait Stage: Send + Sync {
    /// Stage name, used for tracing spans / metric labels
    fn name(&self) -> &'static str;

    /// Processing logic. Returning `Err` short-circuits.
    async fn handle(&self, ctx: &mut RequestCtx) -> Result<StageOutcome, StageError>;
}

/// Three-state stage result
#[derive(Debug)]
pub enum StageOutcome {
    /// Continue to the next stage
    Continue,
    /// Return a response directly (auth rejection / quota exhaustion / static error)
    ShortCircuit(http::Response<axum::body::Body>),
    /// Streaming response, terminating the chain
    Stream(PipeStream),
}

/// Upstream call error (hyper / parsing / business failure)
#[derive(Debug, Error)]
pub enum UpstreamError {
    #[error("connect timeout")]
    Timeout,
    #[error("connect failed: {0}")]
    Connect(#[from] std::io::Error),
    #[error("upstream status {code}: {body_preview}")]
    Status { code: u16, body_preview: String },
    #[error("ssrf detected")]
    SSRF,
    #[error("body read error: {0}")]
    Body(#[from] bytes::TryGetError),
}

/// Unified error across stages
#[derive(Debug, Error)]
pub enum StageError {
    #[error("unauthenticated: {0}")]
    Unauthenticated(String),

    #[error("quota exhausted: remaining={remaining} required={required}")]
    QuotaExhausted { remaining: i64, required: i64 },

    #[error("forbidden: {0}")]
    Forbidden(String),

    /// This station has no route unit for the model at all (and no fallback) — missing
    /// configuration, terminal 404; the client should not retry (the model really is not
    /// configured).
    #[error("model not configured for {group}/{model}")]
    NoRoute { group: String, model: String },

    /// The model has route units, but they are all cooling / disabled right now, or the
    /// 45s decision window is exhausted with no successful candidate — transient.
    /// Retryable 504 when `retry_after_ms > 0`, fed to the `Retry-After` header; 503 when
    /// it is 0 (no cooldown to wait for, so no retry time is promised).
    #[error("all candidates unavailable for {group}/{model}, retry after {retry_after_ms}ms")]
    Unavailable {
        group: String,
        model: String,
        retry_after_ms: u64,
    },

    /// All candidates were refused by the dispatch-side rate limiter
    /// (`gateway_dispatch::DispatchError::RateLimited`). Distinct from `NoRoute`: the client
    /// may succeed if it resends later, mapped to 429.
    #[error("rate limited")]
    RateLimited,

    /// Catalog snapshot not ready / service temporarily unavailable (fail-closed 503).
    #[error("service not ready")]
    NotReady,

    #[error("payload too large")]
    PayloadTooLarge,

    #[error("upstream error: {0}")]
    Upstream(#[from] UpstreamError),

    #[error("internal error: {0}")]
    Internal(#[from] anyhow::Error),
}
