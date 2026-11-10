//! `stage` — `ProtocolBridgeStage` implementation
//!
//! Pipeline Stage 4: **packages** the upstream response forward has already prepared into
//! an HTTP response, or returns an error body shaped like the inbound protocol on the
//! error path.
//!
//! ## Division of labor (WP6 final)
//!
//! Protocol conversion (request-direction encode + response-direction decode) **closes the
//! loop entirely inside forward** — the request and response directions must cooperate
//! through the same codec instance, and the request direction happens inside forward
//! (only there are the request body and the upstream format visible together). Putting
//! response conversion in the same place keeps the direction information inherently
//! consistent, with no need to carry "which encoder was used this time" across stages.
//!
//! The old implementation did non-streaming conversion in this stage and **hardcoded the
//! response source protocol to `OpenAI`**: the streaming path did not go through this stage
//! at all (`ctx.upstream` was only written in the non-streaming branch), so streaming
//! Claude/Gemini clients always received `OpenAI` event shapes. That is G2.
//!
//! So this stage now does only two things:
//!
//! 1. **Error mapping**: when `ctx.error` has been written by an earlier stage, return an
//!    error body shaped like the inbound protocol;
//! 2. **Packaging**: turn `ctx.upstream` (written by forward; the body is already in
//!    inbound format) into an HTTP response.

use crate::format_codec::FormatRegistry;
use async_trait::async_trait;
use gateway_pipeline::{RequestCtx, Stage, StageError, StageOutcome};
use std::sync::Arc;

/// Data-plane protocol egress stage.
pub struct ProtocolBridgeStage {
    formats: Arc<FormatRegistry>,
}

impl ProtocolBridgeStage {
    /// Construct from one format-registry instance (production assembly uses
    /// [`FormatRegistry::with_defaults`]).
    #[must_use]
    pub fn new(formats: Arc<FormatRegistry>) -> Self {
        Self { formats }
    }

    /// Registry reference (forward-side conversion shares the same codec instance).
    #[must_use]
    pub fn formats(&self) -> &Arc<FormatRegistry> {
        &self.formats
    }
}

#[async_trait]
impl Stage for ProtocolBridgeStage {
    fn name(&self) -> &'static str {
        "protocol-bridge"
    }

    async fn handle(&self, ctx: &mut RequestCtx) -> Result<StageOutcome, StageError> {
        let inbound = ctx.request.inbound_protocol;

        // Error first: if an earlier stage has written ctx.error, convert it to the
        // inbound protocol shape.
        if let Some(e) = ctx.error.take() {
            let resp = crate::error_mapping::map_error(&e, inbound);
            return Ok(StageOutcome::ShortCircuit(resp));
        }

        // Normal path: forward has already converted the upstream response to inbound
        // format and written it to ctx.upstream; here we only package it.
        let upstream = ctx.upstream.take().ok_or_else(|| {
            StageError::Internal(anyhow::anyhow!(
                "protocol-bridge: no upstream response and no error to map"
            ))
        })?;

        let resp = http::Response::builder()
            .status(upstream.status)
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(axum::body::Body::from(upstream.body))
            .map_err(|e| StageError::Internal(anyhow::anyhow!("build response: {e}")))?;

        Ok(StageOutcome::ShortCircuit(resp))
    }
}
