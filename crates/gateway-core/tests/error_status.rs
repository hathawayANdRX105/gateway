//! `NormalizedError::from_status` status-code table behavior.
//!
//! **Which bug turns this red**: `400..=499` is the catch-all arm and takes
//! first-match. If the dedicated `408` arm is dropped, reordered after the
//! catch-all, or labeled non-retryable, then `408` (transient upstream
//! gateway timeout) gets classified as a non-retryable request fault → the
//! dispatch retry loop stops switching candidates for that failure.
//! The 400/404 controls pin the flip side: the catch-all must stay
//! non-retryable, and 408 must not make all of 4xx retryable.

use gateway_core::error::{NormalizedError, code};

#[test]
fn upstream_408_is_a_retryable_upstream_error() {
    let err = NormalizedError::from_status(408, "gateway timeout");
    assert_eq!(
        err.code,
        code::UPSTREAM_ERROR,
        "408 is an upstream fault, not a request fault"
    );
    assert_eq!(err.status, 408, "status is passed through unchanged");
    assert!(
        err.retryable,
        "408 is transient: the retry loop must switch candidates"
    );
    assert!(
        !err.channel_scoped,
        "408 is not channel-scoped (already retryable), it must not additionally downgrade layers"
    );
    assert_eq!(err.message, "gateway timeout");
}

#[test]
fn other_4xx_stay_non_retryable() {
    for status in [400u16, 422] {
        let err = NormalizedError::from_status(status, "nope");
        assert_eq!(
            err.code,
            code::INVALID_API_KEY,
            "catch-all arm reads other 4xx as INVALID_API_KEY"
        );
        assert_eq!(err.status, status);
        assert!(
            !err.retryable,
            "{status} is a request-side fault, switching channels cannot save it"
        );
        assert!(
            !err.channel_scoped,
            "{status} is request-scoped, not channel-scoped"
        );
    }
    // 404 is the one channel-scoped 4xx: same INVALID_API_KEY read, but the
    // flag tells the dispatch loop "this channel lacks the model, try another".
    let not_found = NormalizedError::from_status(404, "nope");
    assert_eq!(not_found.code, code::INVALID_API_KEY);
    assert!(!not_found.retryable);
    assert!(
        not_found.channel_scoped,
        "404 → channel lacks model, layer downgrade should switch candidates"
    );
}

#[test]
fn neighbouring_retryable_codes_are_unaffected() {
    let throttled = NormalizedError::from_status(429, "slow down");
    assert_eq!(throttled.code, code::RATE_LIMITED);
    assert_eq!(throttled.status, 429);
    assert!(throttled.retryable);

    let server_error = NormalizedError::from_status(503, "unavailable");
    assert_eq!(server_error.code, code::UPSTREAM_ERROR);
    assert_eq!(server_error.status, 503);
    assert!(server_error.retryable);
}
#[test]
fn boundary_statuses_classify_per_table() {
    // 408 sits inside 400..=499 but is not the catch-all: retryable upstream error,
    // not channel-scoped; neighbouring 409 is a plain non-retryable 4xx.
    let timeout = NormalizedError::from_status(408, "gateway timeout");
    assert_eq!(timeout.code, code::UPSTREAM_ERROR);
    assert_eq!(timeout.status, 408);
    assert!(timeout.retryable);
    assert!(!timeout.channel_scoped);

    let conflict = NormalizedError::from_status(409, "conflict");
    assert_eq!(conflict.code, code::INVALID_API_KEY);
    assert_eq!(conflict.status, 409);
    assert!(!conflict.retryable);
    assert!(!conflict.channel_scoped);

    // 401/403 share the INVALID_API_KEY read with other 4xx but are channel-scoped:
    // another channel may succeed.
    for status in [401u16, 403] {
        let scoped = NormalizedError::from_status(status, "no auth");
        assert_eq!(scoped.code, code::INVALID_API_KEY);
        assert!(!scoped.retryable);
        assert!(
            scoped.channel_scoped,
            "{status} must be channel-scoped: another channel may succeed"
        );
    }

    // 5xx is retryable upstream; anything outside the table falls back to 502.
    let server = NormalizedError::from_status(500, "internal");
    assert_eq!(server.code, code::UPSTREAM_ERROR);
    assert_eq!(server.status, 500);
    assert!(server.retryable);
    assert!(!server.channel_scoped);

    // Range edges: 599 is the top of the retryable 5xx range; 600 and 399 are outside
    // every specific rule and must hit the 502 fallback; 499 is the top of the 4xx
    // range (non-retryable, not channel-scoped). These are exactly the edges where a
    // first-match/reordering bug in STATUS_RULES would hide.
    let top_5xx = NormalizedError::from_status(599, "gateway gone");
    assert_eq!(top_5xx.code, code::UPSTREAM_ERROR);
    assert_eq!(top_5xx.status, 599);
    assert!(top_5xx.retryable);

    let above_5xx = NormalizedError::from_status(600, "weird");
    assert_eq!(above_5xx.code, code::UPSTREAM_ERROR);
    assert_eq!(above_5xx.status, 502);
    assert!(!above_5xx.retryable);

    let below_4xx = NormalizedError::from_status(399, "early");
    assert_eq!(below_4xx.code, code::UPSTREAM_ERROR);
    assert_eq!(below_4xx.status, 502);
    assert!(!below_4xx.retryable);

    let top_4xx = NormalizedError::from_status(499, "client fat finger");
    assert_eq!(top_4xx.code, code::INVALID_API_KEY);
    assert_eq!(top_4xx.status, 499);
    assert!(!top_4xx.retryable);
    assert!(!top_4xx.channel_scoped);

    let unknown = NormalizedError::from_status(700, "nope");
    assert_eq!(unknown.code, code::UPSTREAM_ERROR);
    assert_eq!(unknown.status, 502);
    assert!(!unknown.retryable);
    assert!(!unknown.channel_scoped);
}
