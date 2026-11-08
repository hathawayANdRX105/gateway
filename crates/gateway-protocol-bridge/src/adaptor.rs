//! `adaptor` — shared vocabulary for protocol conversion: format identifiers and error types.
//!
//! The conversion logic itself lives outside this module: per-format bidirectional codecs sit
//! under the `FormatCodec` trait in [`crate::format_codec`] (four implementations: `openai` /
//! `claude` / `gemini` / `responses`), with registration and two-hop dispatch in
//! `FormatRegistry`.
//!
//! Only the two things every codec must reference remain here — [`Protocol`] and
//! [`AdaptorError`] — keeping them decoupled from any concrete codec.

use thiserror::Error;

/// Protocol family — the value domain corresponds to contract `ChannelRecord.provider_type`.
///
/// It serves both as the "inbound format" (the protocol the client speaks, determined by the
/// request path) and the "upstream format" (decided by the channel's `provider_type`); both
/// directions share this enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Protocol {
    /// OpenAI Chat Completions (`/v1/chat/completions`).
    OpenAi,
    /// OpenAI Responses API (`/v1/responses`) — same name as Chat Completions but a different
    /// shape; mirrors `gateway_pipeline::ProtocolKind::OpenAIResp`.
    OpenAIResp,
    /// Anthropic Messages (`/v1/messages`).
    Claude,
    /// Gemini `generateContent` (`/v1beta/...:generateContent`).
    Gemini,
    /// Other vendors: no conversion, bytes pass through unchanged.
    Passthrough,
}

/// Protocol conversion error.
#[derive(Debug, Error)]
pub enum AdaptorError {
    #[error("no codec registered for {from:?} -> {to:?}")]
    NotRegistered { from: Protocol, to: Protocol },
    #[error("encode failed: {0}")]
    EncodeFailed(String),
    #[error("decode failed: {0}")]
    DecodeFailed(String),
    #[error("unsupported conversion: {from:?} -> {to:?}")]
    Unsupported { from: Protocol, to: Protocol },
}

impl Protocol {
    /// Inbound protocol decision (pipeline side) → this crate's format identifier.
    ///
    /// The two value domains correspond one-to-one but belong to different crates:
    /// `ProtocolKind` is the shared-surface inbound contract (lives in `gateway-contract`,
    /// re-exported by the pipeline facade, passed between stages), while `Protocol`
    /// only serves protocol conversion. The conversion is centralized here so every call site
    /// does not write its own match.
    pub fn from_kind(kind: gateway_pipeline::ProtocolKind) -> Self {
        match kind {
            gateway_pipeline::ProtocolKind::OpenAI => Protocol::OpenAi,
            gateway_pipeline::ProtocolKind::OpenAIResp => Protocol::OpenAIResp,
            gateway_pipeline::ProtocolKind::Anthropic => Protocol::Claude,
            gateway_pipeline::ProtocolKind::Gemini => Protocol::Gemini,
        }
    }

    /// Channel `provider_type` → upstream format. Unknown values are treated as passthrough
    /// (zero conversion is the safest).
    pub fn from_provider_type(provider_type: &str) -> Self {
        match provider_type {
            "claude" | "anthropic" => Protocol::Claude,
            "gemini" | "google" => Protocol::Gemini,
            "openai-resp" | "openai-responses" | "responses" => Protocol::OpenAIResp,
            "openai" | "" => Protocol::OpenAi,
            _ => Protocol::Passthrough,
        }
    }
}
