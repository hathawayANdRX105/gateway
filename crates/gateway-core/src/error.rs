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
    /// Builds the gateway `/v1` error body from a [`NormalizedError`].
    ///
    /// Settled shape (issue #17): `{"error": {"code", "message", "type"}}` — OpenAI-compatible,
    /// so mainstream SDKs parse failures with their stock error handlers. `code` is one of the
    /// [`crate::error::code`] constants, `message` is the masked human-readable message, and
    /// `type` is the OpenAI-compatible error category derived from the HTTP status
    /// (`authentication_error` / `insufficient_quota` / `permission_error` / `not_found` /
    /// `rate_limit_error` / `api_error` / `invalid_request_error`).
    ///
    /// Rationale: the three consumers of this contract (gateway error bodies, console envelopes,
    /// frontend i18n) all read `code`/`message`; `type` exists only for SDK wire compatibility
    /// and carries no dispatch semantics — the dispatch state machine classifies via
    /// [`NormalizedError::retryable`] and [`NormalizedError::channel_scoped`], never via `type`.
    ///
    /// Boundary: this settles the body shape, not an external multi-SDK matrix. Verifying each
    /// vendor SDK against this shape is out of scope for #17 (tracked under ferrite-#214); if a
    /// vendor SDK needs different field names, add a protocol-specific shape in
    /// gateway-protocol-bridge instead of reshaping this contract.
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
    /// non-retryable 4xx downgrade to the next candidate, with health recorded as Neutral.
    /// The classification table (`STATUS_RULES` / [`classify_status`] in this module) is the
    /// single source of truth for `channel_scoped` across the workspace:
    /// `gateway_dispatch::failure_scope::classify_channel_scope` and forward's egress both
    /// delegate to it, so those consumers cannot drift. `code`/`retryable` are additionally
    /// mapped by `gateway_forward::egress::classify_status` for the client-facing error
    /// body — that mapping deliberately reports `UPSTREAM_ERROR` for generic 4xx (fatal
    /// request errors, see its doc comment); the parity test in forward's
    /// `classify_status.rs` pins where the two tables must agree.
    pub channel_scoped: bool,
    /// Human-readable message (masked; must not contain upstream keys/internal addresses).
    pub message: String,
    /// `Retry-After` hint carried by an upstream 429 (milliseconds) — Some when the header
    /// is present and parses, overriding the dispatch 429 cooldown ladder
    /// (clamped to `cooldown_max_ms`). Always None for non-429 or a missing/unparseable
    /// header.
    pub retry_after_ms: Option<u64>,
}

/// One row of the status classification table, checked in order; the first match wins.
///
/// Specific codes (408, 429, 401/403/404) precede the ranges that contain them
/// (400..=499 / 500..=599) so they keep their own row instead of being swallowed
/// by the catch-all range.
#[derive(Clone, Copy)]
struct StatusRule {
    lo: u16, // inclusive low bound; lo == hi means an exact status match
    hi: u16, // inclusive high bound
    /// One of the [`crate::error::code`] constants.
    code: ErrorCode,
    /// Whether the dispatch retry loop switches candidates for this status.
    retryable: bool,
    /// Whether another channel may succeed (401/403/404) vs a request fault (400/413/422).
    channel_scoped: bool,
}

/// Single source of truth for `status -> (code, retryable, channel_scoped)` (issue #16).
///
/// Lives in `gateway-core` because `gateway-dispatch` depends on `core`, never the reverse;
/// `gateway_dispatch::failure_scope::classify_channel_scope` delegates to [`classify_status`],
/// so there is one table and no duplicated inline classification between the two crates.
const STATUS_RULES: &[StatusRule] = &[
    // exact matches first — they fall inside the ranges below
    StatusRule {
        lo: 401,
        hi: 401,
        code: code::INVALID_API_KEY,
        retryable: false,
        channel_scoped: true,
    },
    StatusRule {
        lo: 403,
        hi: 403,
        code: code::INVALID_API_KEY,
        retryable: false,
        channel_scoped: true,
    },
    StatusRule {
        lo: 404,
        hi: 404,
        code: code::INVALID_API_KEY,
        retryable: false,
        channel_scoped: true,
    },
    StatusRule {
        lo: 408,
        hi: 408,
        code: code::UPSTREAM_ERROR,
        retryable: true,
        channel_scoped: false,
    },
    StatusRule {
        lo: 429,
        hi: 429,
        code: code::RATE_LIMITED,
        retryable: true,
        channel_scoped: false,
    },
    // ranges last
    StatusRule {
        lo: 400,
        hi: 499,
        code: code::INVALID_API_KEY,
        retryable: false,
        channel_scoped: false,
    },
    StatusRule {
        lo: 500,
        hi: 599,
        code: code::UPSTREAM_ERROR,
        retryable: true,
        channel_scoped: false,
    },
];

/// Classifies `status` into `(code, retryable, channel_scoped)` per [`STATUS_RULES`].
///
/// Returns `None` when no row matches (e.g. a non-HTTP status); the caller owns the
/// 502 fallback ([`NormalizedError::from_status`] maps those to status 502).
#[must_use]
pub fn classify_status(status: u16) -> Option<(ErrorCode, bool, bool)> {
    for rule in STATUS_RULES {
        if (rule.lo..=rule.hi).contains(&status) {
            return Some((rule.code, rule.retryable, rule.channel_scoped));
        }
    }
    None
}

impl NormalizedError {
    /// Classifies by upstream status code (issue #16): [`STATUS_RULES`] is the single source
    /// of truth for `(code, retryable, channel_scoped)` in core and dispatch:
    /// 401/403/404 → `INVALID_API_KEY`, channel-scoped (another channel may succeed);
    /// 429 → `RATE_LIMITED`, retryable; 408 → `UPSTREAM_ERROR`, retryable and transient;
    /// 5xx → `UPSTREAM_ERROR`, retryable; other 4xx → `INVALID_API_KEY`, not retryable;
    /// anything else → `UPSTREAM_ERROR` with status 502, not retryable.
    ///
    /// Forward's `egress::classify_status` (the client-facing production classifier)
    /// deliberately keeps `UPSTREAM_ERROR` for generic 4xx — see its doc comment and the
    /// parity test in forward's `classify_status.rs`.
    ///
    /// Zero behavior change vs the inline match it replaces: 408 must stay ahead of the
    /// 400..=499 range, and 401/403/404 stay channel-scoped while the rest of 4xx is not.
    pub fn from_status(status: u16, raw_message: impl Into<String>) -> Self {
        let raw = raw_message.into();
        let http_status = if matches!(status, 400..=599) {
            status
        } else {
            502
        };
        let (code, retryable, channel_scoped) =
            classify_status(status).unwrap_or((code::UPSTREAM_ERROR, false, false));
        Self {
            code,
            status: http_status,
            retryable,
            message: raw,
            channel_scoped,
            retry_after_ms: None,
        }
    }
}
