//! # error — shared error-code table across surfaces
//!
//! A single error-code vocabulary shared by three consumers: gateway error bodies, console
//! envelope messages, and web frontend i18n. The categorization follows new-api relaykit
//! `types/channel_error.go`.
//!
//! ## Error code → HTTP status → client behavior
//!
//! | code                    | HTTP | Client behavior |
//! |-------------------------|------|-----------------|
//! | INVALID_API_KEY         | 401  | Stop retrying; re-authenticate |
//! | TOKEN_EXPIRED           | 401  | Refresh, then retry once |
//! | INSUFFICIENT_QUOTA      | 402  | Prompt to top up |
//! | MODEL_FORBIDDEN         | 403  | Stop retrying |
//! | RATE_LIMITED            | 429  | Retry with exponential backoff |
//! | NO_CANDIDATE            | 404  | No available route for the model/group |
//! | UPSTREAM_ERROR          | 502  | Gateway retried but upstream still failed |
//! | CATALOG_NOT_READY       | 503  | Retry later (during startup) |
//! | INTERNAL                | 500  | Contact the administrator |

/// Error code constants — serialized as strings by serde.
pub mod code {
    pub const INVALID_API_KEY: &str = "invalid_api_key";
    pub const TOKEN_EXPIRED: &str = "token_expired";
    pub const INSUFFICIENT_QUOTA: &str = "insufficient_quota";
    pub const MODEL_FORBIDDEN: &str = "model_forbidden";
    pub const RATE_LIMITED: &str = "rate_limited";
    pub const NO_CANDIDATE: &str = "no_candidate";
    pub const UPSTREAM_ERROR: &str = "upstream_error";
    pub const CATALOG_NOT_READY: &str = "catalog_not_ready";
    pub const INTERNAL: &str = "internal";
}

/// gateway /v1 error body (`OpenAI` style, compatible with mainstream SDK parsers).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GatewayErrorBody {
    pub error: GatewayErrorDetail,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GatewayErrorDetail {
    /// One of the [`crate::error::code`] constants.
    pub code: String,
    /// Masked human-readable message (must not contain upstream keys/internal addresses).
    pub message: String,
    /// OpenAI-compatible field (read by SDKs).
    pub r#type: String,
}

impl GatewayErrorBody {
    /// Builds an OpenAI-style error body from `NormalizedError` (SDK-compatible shape, to be
    /// settled by TODO(ferrite-#214, re-anchored: #17)).
    #[must_use]
    pub fn from_normalized(err: &NormalizedError) -> Self {
        Self {
            error: GatewayErrorDetail {
                code: err.code.to_string(),
                message: err.message.clone(),
                r#type: match err.status {
                    401 => "authentication_error".into(),
                    402 => "insufficient_quota".into(),
                    403 => "permission_error".into(),
                    404 => "not_found".into(),
                    429 => "rate_limit_error".into(),
                    500..=599 => "api_error".into(),
                    _ => "invalid_request_error".into(),
                },
            },
        }
    }
}

/// Machine-readable error code (domain = the constants in [`code`]).
pub type ErrorCode = &'static str;

/// Normalized error — the single error protocol for internal flow and external exposure.
///
/// Produced by the upstream classification in forward; the dispatch state machine uses it
/// to decide retries/candidate switching, and protocol-bridge maps it into each protocol's
/// error shape. Shared across crates, so it lives in contract.
#[derive(Debug, Clone, PartialEq)]
pub struct NormalizedError {
    pub code: ErrorCode,
    /// HTTP status exposed to the client.
    pub status: u16,
    /// The dispatch state machine uses this to decide whether to retry another candidate.
    pub retryable: bool,
    /// Whether the failure is "channel-scoped" (P1-B downgrade): 401/403/404 may succeed on
    /// another channel → true; request-scoped 4xx (400/413/422) fail on any channel → false.
    /// Candidate switching is decided only by `retryable`; this field merely lets a
    /// non-retryable 4xx downgrade to the next candidate, with health recorded as Neutral
    /// (`gateway_dispatch::failure_scope` is the classification authority).
    pub channel_scoped: bool,
    /// Human-readable message (masked; must not contain upstream keys/internal addresses).
    pub message: String,
    /// `Retry-After` hint carried by an upstream 429 (milliseconds) — Some when the header
    /// is present and parses, overriding the dispatch 429 cooldown ladder
    /// (clamped to `cooldown_max_ms`). Always None for non-429 or a missing/unparseable
    /// header.
    pub retry_after_ms: Option<u64>,
}

impl NormalizedError {
    /// Classifies by upstream status code — TODO(ferrite-#501, re-anchored: #16) rule-table prototype:
    /// 401/403 → auth (no retry), 429 → rate limited (retryable), 408 → upstream gateway
    /// timeout (retryable, transient), 5xx → upstream (retryable), other 4xx → request
    /// problem (no retry), anything else → 502 fallback.
    pub fn from_status(status: u16, raw_message: impl Into<String>) -> Self {
        let raw = raw_message.into();
        let (code, http_status, retryable, message) = match status {
            // 408 falls inside the 400..=499 range and must precede the fallback arm to match.
            // 408 and 429 both fall inside 400..=499 and must precede the
            // catch-all 4xx arm to match.
            408 | 500..=599 => (code::UPSTREAM_ERROR, status, true, raw),
            429 => (code::RATE_LIMITED, status, true, raw),
            400..=499 => (code::INVALID_API_KEY, status, false, raw),
            _ => (code::UPSTREAM_ERROR, 502, false, raw),
        };
        Self {
            code,
            status: http_status,
            retryable,
            message,
            // Same table as gateway_dispatch::failure_scope::classify_channel_scope
            // (contract is the lowest layer and cannot depend upward, so the matches! lives
            // here).
            channel_scoped: !retryable && matches!(status, 401 | 403 | 404),
            retry_after_ms: None,
        }
    }
}
