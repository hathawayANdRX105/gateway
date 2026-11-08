//! P1-B channel relevance classification scenario registry (#158 skeleton; the
//! assertions were filled in once P1-B landed).
//!
//! Expectation: 401/403 (credentials), 404 (the channel lacks this model) →
//! [`FailureScope::Channel`] — another channel may succeed; 400/413/422 (the request
//! itself is bad) → [`FailureScope::Request`] — any channel fails. 5xx/429 are not
//! handled by `classify_channel_scope`: retryable classification is already decided by
//! `gateway_forward::egress::classify_status`, and this function only consumes 4xx.
//!
//! Demotion behavior inside the retry loop (switch / pass through only on the last
//! candidate / Neutral report) is covered by `channel_downgrade.rs`; the end-to-end
//! dual-channel demotion is covered by `bin/api/tests/route_resolution.rs` P0-2.

use gateway_dispatch::{FailureScope, classify_channel_scope};

#[test]
fn channel_scoped_4xx_maps_to_channel() {
    for status in [401u16, 403, 404] {
        assert_eq!(
            classify_channel_scope(status),
            FailureScope::Channel,
            "{status} switching channels may be valid, should classify as Channel"
        );
    }
}

#[test]
fn request_scoped_4xx_maps_to_request() {
    for status in [400u16, 413, 422] {
        assert_eq!(
            classify_channel_scope(status),
            FailureScope::Request,
            "{status} the request itself is bad, switching channels is invalid, should classify as Request"
        );
    }
}

#[test]
#[ignore = "TODO(#158): skeleton not implemented"]
fn policy_for_hits_per_model_then_default() {
    todo!(
        "TODO(#158): ModelRetryPolicies::policy_for exactly matches per_model, otherwise falls back to default"
    )
}
