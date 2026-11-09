//! `error_mapping` — NormalizedError → per-protocol error shapes
//!
//! The single internal error representation (`gateway_core::error::NormalizedError`); this
//! module maps it into the error response body each of OpenAI Chat / OpenAI Responses /
//! Anthropic / Gemini specifies.
//!
//! ## Classification is an enum, never a string probe
//!
//! Every client-visible `type` renders from [`ErrorClass`], decided by **exact** code lookups
//! over the two live code vocabularies: the bridge-local codes rendered by [`StageFailure`]
//! and the `gateway_core::error::code` contract constants. The old
//! `code.contains("auth")`-style probes misfired in both directions: a contract code without
//! the probe substring (an upstream 401 carrying `invalid_api_key`) fell through to
//! `api_error`, while an unrelated code merely *containing* a probe substring was
//! misclassified. The dispatch is exact-match table-driven, so both are impossible.
//!
//! Likewise, the wire code / HTTP status / Retry-After triple for a `StageError` is decided
//! once by [`StageFailure`] — the old shape kept three parallel `match`es that could drift
//! when a variant was added.

use axum::body::Body;
use bytes::Bytes;
use gateway_core::error::{ErrorCode, NormalizedError, code as contract_code};
use gateway_pipeline::{ProtocolKind, StageError};
use http::Response;

/// Client-visible error classification — the only distinctions the vendor error `type`
/// fields actually draw; everything the per-protocol classifications do not name collapses
/// to [`ErrorClass::Other`] (each protocol's generic API error).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ErrorClass {
    /// Authentication failed or the key is invalid/expired (bridge-local
    /// `unauthenticated`; contract `invalid_api_key` / `token_expired`).
    Auth,
    /// Quota or balance exhausted (bridge-local `quota_exhausted`; contract
    /// `insufficient_quota`).
    Quota,
    /// Rate limited (`rate_limited` — the one string shared by both vocabularies).
    RateLimited,
    /// Anything a per-protocol classification does not name.
    Other,
}

impl ErrorClass {
    /// Exact-match lookup over both live vocabularies (bridge-local + contract constants);
    /// unknown codes collapse to `Other` instead of being substring-probed.
    fn from_code(code: &str) -> Self {
        match code {
            "unauthenticated" | contract_code::INVALID_API_KEY | contract_code::TOKEN_EXPIRED => {
                Self::Auth
            }
            "quota_exhausted" | contract_code::INSUFFICIENT_QUOTA => Self::Quota,
            // `rate_limited` is the same string in both vocabularies; listing the contract
            // constant next to it would be an unreachable pattern.
            "rate_limited" => Self::RateLimited,
            _ => Self::Other,
        }
    }

    /// OpenAI Chat / Responses `error.type`.
    fn openai_type(self) -> &'static str {
        match self {
            Self::Auth => "invalid_api_key",
            Self::Quota => "insufficient_quota",
            Self::RateLimited => "rate_limit_error",
            Self::Other => "api_error",
        }
    }

    /// Anthropic `error.type` — a different naming scheme for the same classes.
    fn anthropic_type(self) -> &'static str {
        match self {
            Self::Auth => "authentication_error",
            Self::Quota => "billing_error",
            Self::RateLimited => "rate_limit_error",
            Self::Other => "api_error",
        }
    }
}

/// `StageError` semantics — the single exhaustive dispatch deciding the wire code, HTTP
/// status, and Retry-After hint together.
#[derive(Debug, Clone, Copy)]
enum StageFailure {
    Auth,
    Quota,
    Forbidden,
    NoRoute,
    /// A cooling channel exists (`retry_after_secs == Some(..)`): 504 waitable +
    /// Retry-After. No channel to wait for (`None`; all disabled / rate limited / no
    /// such model): 503 with no retry time promised.
    Unavailable {
        retry_after_secs: Option<u64>,
    },
    RateLimited,
    NotReady,
    PayloadTooLarge,
    Upstream,
    Internal,
}

impl StageFailure {
    fn from_stage_error(e: &StageError) -> Self {
        use gateway_pipeline::StageError::*;
        match e {
            Unauthenticated(_) => Self::Auth,
            QuotaExhausted { .. } => Self::Quota,
            Forbidden(_) => Self::Forbidden,
            NoRoute { .. } => Self::NoRoute,
            Unavailable { retry_after_ms, .. } => Self::Unavailable {
                retry_after_secs: cooldown_secs(*retry_after_ms),
            },
            RateLimited => Self::RateLimited,
            NotReady => Self::NotReady,
            PayloadTooLarge => Self::PayloadTooLarge,
            Upstream(_) => Self::Upstream,
            Internal(_) => Self::Internal,
        }
    }

    /// The internal code carried into the client-visible error body.
    fn code(self) -> ErrorCode {
        match self {
            Self::Auth => "unauthenticated",
            Self::Quota => "quota_exhausted",
            Self::Forbidden => "forbidden",
            Self::NoRoute => "no_route",
            Self::Unavailable { .. } => "all_candidates_unavailable",
            Self::RateLimited => "rate_limited",
            Self::NotReady => "service_not_ready",
            Self::PayloadTooLarge => "payload_too_large",
            Self::Upstream => "upstream_error",
            Self::Internal => "internal",
        }
    }

    fn status(self) -> http::StatusCode {
        match self {
            Self::Auth => http::StatusCode::UNAUTHORIZED,
            Self::Quota => http::StatusCode::PAYMENT_REQUIRED,
            Self::Forbidden => http::StatusCode::FORBIDDEN,
            Self::NoRoute => http::StatusCode::NOT_FOUND,
            // b+c split: with cooling (retry_after_secs is Some) = 504, waitable +
            // Retry-After; without cooling (all disabled / rate limited / no such model)
            // = 503 unavailable, with no retry time promised.
            Self::Unavailable {
                retry_after_secs: Some(_),
            } => http::StatusCode::GATEWAY_TIMEOUT,
            Self::Unavailable {
                retry_after_secs: None,
            } => http::StatusCode::SERVICE_UNAVAILABLE,
            // Matches the RateLimited mapping in gateway_pipeline::error_to_response.
            Self::RateLimited => http::StatusCode::TOO_MANY_REQUESTS,
            Self::NotReady => http::StatusCode::SERVICE_UNAVAILABLE,
            Self::PayloadTooLarge => http::StatusCode::PAYLOAD_TOO_LARGE,
            Self::Upstream => http::StatusCode::BAD_GATEWAY,
            Self::Internal => http::StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// The Retry-After hint in seconds: promised only while a channel is cooling.
    fn retry_after_secs(self) -> Option<u64> {
        match self {
            Self::Unavailable { retry_after_secs } => retry_after_secs,
            _ => None,
        }
    }
}

/// OpenAI Chat Completions-compatible error shape (for SDK parsing).
pub fn to_openai_shape(err: &NormalizedError) -> serde_json::Value {
    serde_json::json!({
        "error": {
            "code": err.code,
            "message": err.message,
            "type": ErrorClass::from_code(err.code).openai_type(),
        }
    })
}

/// OpenAI Responses error shape.
///
/// Responses and Chat are both OpenAI, but their error bodies differ: client SDKs read
/// `error.{code,message,type}`, where **`code` is a nullable string** (not the internal
/// error code) and `type` carries the classification. The old implementation folded
/// `OpenAIResp` and `OpenAI` into one branch in [`map_error`], so Responses clients got
/// the Chat shape — they are split here.
pub fn to_responses_shape(err: &NormalizedError) -> serde_json::Value {
    serde_json::json!({
        "error": {
            "code": serde_json::Value::Null,
            "message": err.message,
            "type": ErrorClass::from_code(err.code).openai_type(),
        }
    })
}

/// Anthropic error shape.
pub fn to_anthropic_shape(err: &NormalizedError) -> serde_json::Value {
    serde_json::json!({
        "type": "error",
        "error": {
            "type": ErrorClass::from_code(err.code).anthropic_type(),
            "message": err.message,
        }
    })
}

/// Gemini error shape.
pub fn to_gemini_shape(err: &NormalizedError) -> serde_json::Value {
    serde_json::json!({
        "error": {
            "code": err.code,
            "message": err.message,
            "status": gemini_status_name(err.status),
        }
    })
}

fn gemini_status_name(status: u16) -> &'static str {
    match status {
        400 => "INVALID_ARGUMENT",
        401 => "UNAUTHENTICATED",
        403 => "PERMISSION_DENIED",
        404 => "NOT_FOUND",
        429 => "RESOURCE_EXHAUSTED",
        500..=599 => "INTERNAL",
        _ => "UNKNOWN",
    }
}

/// `StageError` → HTTP Response in each protocol's error shape
pub fn map_error(e: StageError, target: ProtocolKind) -> Response<Body> {
    let failure = StageFailure::from_stage_error(&e);
    // message comes from StageError's Display, so `model not configured for
    // {group}/{model}` is carried into the error body as-is.
    let normalized = NormalizedError {
        code: failure.code(),
        message: e.to_string(),
        status: failure.status().as_u16(),
        retryable: false,
        channel_scoped: false,
        retry_after_ms: None,
    };

    let body = match target {
        ProtocolKind::OpenAI => to_openai_shape(&normalized),
        // Responses and Chat are two response shapes, so their error bodies are split
        // too — the old implementation merged them and Responses clients got the Chat
        // shape.
        ProtocolKind::OpenAIResp => to_responses_shape(&normalized),
        ProtocolKind::Anthropic => to_anthropic_shape(&normalized),
        ProtocolKind::Gemini => to_gemini_shape(&normalized),
    };

    let body_bytes = Bytes::from(serde_json::to_vec(&body).unwrap_or_default());
    let mut builder = Response::builder()
        .status(failure.status())
        .header("content-type", "application/json");
    // Unavailable (504) carries Retry-After so the client can wait out the shortest
    // cooldown recovery before resending.
    if let Some(secs) = failure.retry_after_secs()
        && let Ok(hv) = secs.to_string().parse::<http::HeaderValue>()
    {
        builder = builder.header(http::header::RETRY_AFTER, hv);
    }
    builder
        .body(Body::from(body_bytes))
        .unwrap_or_else(|_| Response::new(Body::empty()))
}

/// Retry-After (seconds) from the shortest remaining cooldown ms:
/// `ceil(retry_after_ms / 1000)`. Promised only when "a channel is cooling"
/// (retry_after_ms > 0); 0 ms (no cooling channel, e.g. all disabled / rate limited /
/// no such model) returns None and promises no retry time — resending immediately is
/// pointless.
fn cooldown_secs(retry_after_ms: u64) -> Option<u64> {
    let secs = retry_after_ms.div_ceil(1000);
    // gate: secs == 0 if and only if retry_after_ms == 0 (no cooling channel).
    (secs > 0).then_some(secs)
}
