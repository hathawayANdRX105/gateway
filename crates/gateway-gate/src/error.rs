//! `error` — rejection reasons + OpenAI-style 4xx response bodies

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;
use thiserror::Error;

#[derive(Debug, Error, Clone)]
pub enum Rejection {
    /// 401 — key not found
    #[error("invalid api key")]
    InvalidApiKey,

    /// 401 — token expired
    #[error("token expired")]
    TokenExpired,

    /// 401 — user not found
    #[error("user not found")]
    UserNotFound,

    /// 403 — user is disabled
    #[error("user disabled")]
    UserDisabled,

    /// 401 — auth_version mismatch (old tokens are invalidated immediately)
    #[error("auth version mismatch")]
    TokenAuthVersionMismatch,

    /// 403 — IP not in the allowlist
    #[error("ip not allowed")]
    IpNotAllowed,

    /// 500 — internal error: gate ordering mistake
    #[error("auth skipped: previous gate did not fill token")]
    AuthSkipped,

    /// 402 — insufficient balance
    #[error("insufficient quota: remaining={remaining} cost={cost}")]
    InsufficientQuota { remaining: i64, cost: i64 },

    /// 429 — rate limit exceeded
    #[error("rate limited")]
    RateLimited,

    /// 400 — request body is missing model
    #[error("model not specified")]
    ModelNotSpecified,

    /// 403 — model outside the allowlist
    #[error("model forbidden: {model}")]
    ModelForbidden { model: String },

    /// 403 — model outside the group-level allowlist
    #[error("model not allowed for group {group}: {model}")]
    ModelNotAllowedForGroup { model: String, group: String },

    /// 403 — group disabled in the admin console (`api_groups.status ≠ 1`); every
    /// request in that group is rejected
    #[error("group disabled: {group}")]
    GroupDisabled { group: String },

    /// 429 — graylist ban in effect
    #[error("graylisted")]
    Graylisted,

    /// 429 — concurrency slots exhausted
    #[error("concurrency exhausted")]
    ConcurrencyExhausted,

    /// 413 — request body too large
    #[error("payload too large")]
    PayloadTooLarge,
}

/// Rejection → HTTP response
pub fn rejection_to_response(rej: Rejection) -> Response {
    use Rejection::*;
    // Status code and error code are a fixed mapping; the message always comes from
    // `Rejection`'s Display instead of repeating `rej.to_string()` in every arm.
    let (status, code) = match &rej {
        InvalidApiKey => (StatusCode::UNAUTHORIZED, "invalid_api_key"),
        TokenExpired => (StatusCode::UNAUTHORIZED, "token_expired"),
        UserNotFound => (StatusCode::UNAUTHORIZED, "user_not_found"),
        UserDisabled => (StatusCode::FORBIDDEN, "user_disabled"),
        TokenAuthVersionMismatch => (StatusCode::UNAUTHORIZED, "auth_version_mismatch"),
        IpNotAllowed => (StatusCode::FORBIDDEN, "ip_not_allowed"),
        AuthSkipped => (StatusCode::INTERNAL_SERVER_ERROR, "internal"),
        InsufficientQuota { .. } => (StatusCode::PAYMENT_REQUIRED, "insufficient_quota"),
        RateLimited => (StatusCode::TOO_MANY_REQUESTS, "rate_limited"),
        ModelNotSpecified => (StatusCode::BAD_REQUEST, "model_not_specified"),
        ModelForbidden { .. } => (StatusCode::FORBIDDEN, "model_forbidden"),
        ModelNotAllowedForGroup { .. } => (StatusCode::FORBIDDEN, "model_not_allowed_for_group"),
        GroupDisabled { .. } => (StatusCode::FORBIDDEN, "group_disabled"),
        Graylisted => (StatusCode::TOO_MANY_REQUESTS, "graylisted"),
        ConcurrencyExhausted => (StatusCode::TOO_MANY_REQUESTS, "concurrency_exhausted"),
        PayloadTooLarge => (StatusCode::PAYLOAD_TOO_LARGE, "payload_too_large"),
    };

    let body = json!({
        "error": {
            "code": code,
            "message": rej.to_string(),
        }
    });
    (status, axum::Json(body)).into_response()
}
