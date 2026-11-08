//! `NormalizedError::from_status` status-code table behavior.
//!
//! **Which bug turns this red**: `400..=499` is the catch-all arm and takes
//! first-match. If the dedicated `408` arm is dropped, reordered after the
//! catch-all, or labeled non-retryable, then `408` (transient upstream
//! gateway timeout) gets classified as a non-retryable request fault → the
//! dispatch retry loop stops switching candidates for that failure.
//! The 400/404 controls pin the flip side: the catch-all must stay
//! non-retryable, and 408 must not make all of 4xx retryable.

use app_contract::error::{NormalizedError, code};

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
