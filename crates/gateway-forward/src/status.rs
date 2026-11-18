//! Upstream status semantics — error-domain helpers kept apart from the client pool.
//!
//! `classify_status` maps an upstream status code to the client-facing
//! `NormalizedError`; `parse_retry_after_ms` parses the `Retry-After` header.
//!
//! These live with the egress error domain rather than in `ReqwestEgress` (the
//! connection pool / sharding layer) because status→error classification is the
//! error boundary: it decides which upstream failures are retryable and whether a
//! channel is channel-scoped. The classification table itself stays single-sourced
//! in `gateway_core` (`STATUS_RULES`); this module only applies forward's deliberate
//! divergence (generic 4xx → `UPSTREAM_ERROR`) via `failure_scope`. The parity test
//! in `tests/classify_status.rs` pins that contract — every status 100..=600
//! classified by both tables must agree on `retryable`/`channel_scoped`, and `code`
//! agrees everywhere except the intentional 4xx divergence.

/// Non-2xx → `NormalizedError`.
///
/// Classified by status code (aligned with `gateway_dispatch::health::FailureClass)`:
/// - 401/403 → `invalid_api_key`, not retryable, but channel-scoped → demote and
///   switch candidates (bad credentials ≠ a bad provider)
/// - 404 → `upstream_error`, not retryable, channel-scoped (this channel lacks the
///   model; another channel may work)
/// - 429 → `rate_limited`, retryable
/// - 408 → `upstream_error`, retryable (an upstream gateway timeout is transient;
///   switching channels or retrying may both work)
/// - 5xx → `upstream_error`, retryable
/// - other 4xx → `upstream_error` (fatal), neither retried nor demoted (the request
///   itself is bad; switching channels cannot save it)
///
/// Deliberate divergence from core's `STATUS_RULES`: generic 4xx (including 404)
/// reports `UPSTREAM_ERROR` rather than `INVALID_API_KEY`, because these are fatal
/// request errors, not credential problems. `retryable` and `channel_scoped`
/// delegate to the core table (via `failure_scope`), so the semantic flags cannot
/// drift — forward's `classify_status.rs` parity test pins this contract.
#[must_use]
pub fn classify_status(
    status: u16,
    body_preview: &str,
    retry_after_ms: Option<u64>,
) -> gateway_core::error::NormalizedError {
    use gateway_core::error::code;
    let (code, http_status, retryable) = match status {
        401 | 403 => (code::INVALID_API_KEY, status, false),
        429 => (code::RATE_LIMITED, status, true),
        // 408 sits inside 400..=499, so it must precede the catch-all arm to match.
        500..=599 | 408 => (code::UPSTREAM_ERROR, status, true),
        400..=499 => (code::UPSTREAM_ERROR, status, false),
        _ => (code::UPSTREAM_ERROR, 502, false),
    };
    // Channel-scoped 4xx reuses gateway_core::failure_scope (P1-B demotion)
    // instead of duplicating a table here; retryable failures (429/5xx) already switch
    // candidates, so this flag only matters on the non-retry branch.
    let channel_scoped = !retryable
        && matches!(status, 400..=499)
        && gateway_core::failure_scope::classify_channel_scope(status)
            == gateway_core::failure_scope::FailureScope::Channel;
    gateway_core::error::NormalizedError {
        code,
        status: http_status,
        retryable,
        channel_scoped,
        message: body_preview.chars().take(200).collect(),
        // Retry-After only matters on 429 (the override hint for dispatch's separate
        // 429 ladder); other status codes are treated as having no hint even if the
        // header is present.
        retry_after_ms: if status == 429 { retry_after_ms } else { None },
    }
}

/// Parse the upstream `Retry-After` response header into a wait duration (ms) — a
/// pure function called by the egress side when a 429 response arrives.
///
/// Supports both legal forms from RFC 9110 §12.5.3:
/// - **delta-seconds** (`"120"`): non-negative integer seconds × 1000;
/// - **HTTP-date** (`"Sun, 06 Nov 1994 08:49:37 GMT"`): parsed as an absolute
///   instant, then subtracted from `now_ms`; a past instant / unparsable value →
///   `None`.
///
/// Parameters:
/// - `raw`: the raw header value (`None` = header absent);
/// - `now_ms`: the parsing instant (unix ms, needed only by the HTTP-date branch;
///   passing 0 does not affect the delta-seconds branch).
///
/// `Some(ms)` is the waitable duration; `None` means absent / negative /
/// unparsable — callers treat it as "no hint" (the local rung of the separate 429
/// ladder).
#[must_use]
pub fn parse_retry_after_ms(raw: Option<&str>, now_ms: i64) -> Option<u64> {
    let raw = raw?.trim();
    if raw.is_empty() {
        return None;
    }
    // delta-seconds: non-negative integer (a minus sign fails to parse → falls into
    // the date branch, fails there too → None).
    if let Ok(secs) = raw.parse::<u64>() {
        return Some(secs.saturating_mul(1_000));
    }
    // HTTP-date (IMF-fixdate): its shape matches the RFC 2822 weekday form, and
    // `parse_from_rfc2822` accepts GMT/UT→+0000, but its named-timezone table does
    // **not contain "UTC"** (unknown three-letter zone names are rejected), so a
    // trailing UTC is rewritten to the equivalent GMT before parsing.
    let date_str = if raw.len() >= 3 && raw.as_bytes()[raw.len() - 3..].eq_ignore_ascii_case(b"UTC")
    {
        // The suffix is ASCII ⇒ len-3 always lands on a character boundary.
        format!("{}GMT", &raw[..raw.len() - 3])
    } else {
        raw.to_string()
    };
    let Ok(dt) = chrono::DateTime::parse_from_rfc2822(&date_str) else {
        return None;
    };
    // Negative value (an instant in the past) → None.
    u64::try_from(dt.timestamp_millis() - now_ms).ok()
}
