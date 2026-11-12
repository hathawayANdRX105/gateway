//! Behavior tests for each protocol's error shape.
//!
//! Core assertion: **the four inbound protocols must produce four different error bodies**.
//! The old implementation merged `ProtocolKind::OpenAIResp` and `OpenAI` into one branch, so
//! Responses clients received a Chat-shaped error — `responses_error_shape_differs_from_chat`
//! in this file watches exactly that.
//!
//! All cases are synchronous: the shape functions (`to_*_shape`) are pure and return JSON
//! directly; `map_error` assertions cover only the status code and content type, not the body
//! (reading the body needs an async runtime, and this crate has no tokio dependency).

use gateway_core::error::{NormalizedError, code};
use gateway_pipeline::{ProtocolKind, StageError};
use gateway_protocol_bridge::error_mapping::{
    map_error, to_anthropic_shape, to_gemini_shape, to_openai_shape, to_responses_shape,
};

fn sample() -> NormalizedError {
    NormalizedError {
        code: "unauthenticated",
        message: "bad token".into(),
        status: 401,
        retryable: false,
        channel_scoped: false,
        retry_after_ms: None,
    }
}

// Per-protocol error body shapes: each vendor SDK reads different field names, and mixing
// them up makes clients parse an error as success (or crash during parsing).
#[test]
fn each_protocol_has_its_own_error_shape() {
    // Chat: error.{code,message,type}.
    let chat = to_openai_shape(&sample());
    assert_eq!(chat["error"]["message"], "bad token");
    assert_eq!(
        chat["error"]["code"], "unauthenticated",
        "Chat's code is the internal error code, actual: {chat}"
    );
    assert!(
        chat["error"]["type"]
            .as_str()
            .is_some_and(|t| !t.is_empty()),
        "Chat error must have a type classification, actual: {chat}"
    );
    // Settled shape (issue #17): `error.{code, message, type}` — exactly these three keys,
    // OpenAI-compatible; no extra fields, no renamed fields.
    let chat_error = chat["error"].as_object().expect("chat error body is a JSON object");
    assert_eq!(
        chat_error.len(),
        3,
        "settled shape has exactly three fields, actual: {chat}"
    );
    assert!(chat_error.contains_key("code"), "settled shape keeps error.code, actual: {chat}");
    assert!(
        chat_error.contains_key("message"),
        "settled shape keeps error.message, actual: {chat}"
    );
    assert!(chat_error.contains_key("type"), "settled shape keeps error.type, actual: {chat}");

    // Responses: also error.{code,message,type}, but code is absent on the SDK side (null).
    let resp = to_responses_shape(&sample());
    assert_eq!(resp["error"]["message"], "bad token");
    assert_eq!(
        resp["error"]["code"],
        serde_json::Value::Null,
        "Responses code should be null (SDK-side default), actual: {resp}"
    );
    // Settled shape (issue #17): `error.{code, message, type}` — exactly these three keys
    // (code is intentionally null on the Responses SDK side).
    let resp_error = resp["error"].as_object().expect("responses error body is a JSON object");
    assert_eq!(
        resp_error.len(),
        3,
        "settled shape has exactly three fields, actual: {resp}"
    );
    assert!(resp_error.contains_key("code"), "settled shape keeps error.code, actual: {resp}");
    assert!(
        resp_error.contains_key("message"),
        "settled shape keeps error.message, actual: {resp}"
    );
    assert!(resp_error.contains_key("type"), "settled shape keeps error.type, actual: {resp}");

    // Anthropic: top-level type=error; the inner object has only type+message (no code).
    let anth = to_anthropic_shape(&sample());
    assert_eq!(anth["type"], "error");
    assert_eq!(anth["error"]["message"], "bad token");
    assert_eq!(
        anth["error"]["type"], "authentication_error",
        "Anthropic's classification name differs from OpenAI's, actual: {anth}"
    );
    assert!(
        anth["error"].get("code").is_none(),
        "Anthropic error body should not have a code field, actual: {anth}"
    );

    // Gemini: error.{code,message,status}, where status is Google's enum name.
    let gem = to_gemini_shape(&sample());
    assert_eq!(gem["error"]["status"], "UNAUTHENTICATED");
    assert_eq!(gem["error"]["message"], "bad token");
}

// **WP7 core regression**: the Responses error body must not be identical to Chat's.
// The old implementation shared one branch for
// `ProtocolKind::OpenAI | ProtocolKind::OpenAIResp`, making the two byte-identical — the
// categorization the Responses SDK received was wrong.
#[test]
fn responses_error_shape_differs_from_chat() {
    let chat = to_openai_shape(&sample());
    let resp = to_responses_shape(&sample());

    assert_ne!(
        chat, resp,
        "Responses error body must differ from Chat (old implementation merged the two), both equal: {chat}"
    );
}

// Error class comes from the exact code vocabulary (both the bridge-local codes and the
// contract constants), never a substring probe.
#[test]
fn error_type_classification_follows_code() {
    let mk = |code: &'static str| NormalizedError {
        code,
        message: "m".into(),
        status: 400,
        retryable: false,
        channel_scoped: false,
        retry_after_ms: None,
    };

    assert_eq!(
        to_openai_shape(&mk("rate_limited"))["error"]["type"],
        "rate_limit_error"
    );
    assert_eq!(
        to_openai_shape(&mk("quota_exhausted"))["error"]["type"],
        "insufficient_quota"
    );
    assert_eq!(
        to_openai_shape(&mk("unauthenticated"))["error"]["type"],
        "invalid_api_key"
    );
    assert_eq!(
        to_openai_shape(&mk("something_else"))["error"]["type"],
        "api_error"
    );
    // Settled OpenAI shape (issue #17): error carries exactly {code, message, type}.
    let rate_body = to_openai_shape(&mk("rate_limited"));
    let rate_error = rate_body["error"]
        .as_object()
        .expect("error body is a JSON object");
    assert_eq!(rate_error.len(), 3);
    assert!(rate_error.contains_key("code"));
    assert!(rate_error.contains_key("message"));
    assert!(rate_error.contains_key("type"));

    // Anthropic uses a different naming scheme.
    assert_eq!(
        to_anthropic_shape(&mk("rate_limited"))["error"]["type"],
        "rate_limit_error"
    );
    assert_eq!(
        to_anthropic_shape(&mk("quota_exhausted"))["error"]["type"],
        "billing_error"
    );
}

// **Typed-classification fix**: contract-vocabulary codes must classify even though they do
// not contain the old probe substrings ("auth"/"quota"/"rate"). Scenario: the upstream
// classifier (gateway_core NormalizedError::from_status) labels an upstream 401 with
// INVALID_API_KEY; the old substring probe missed it and rendered `api_error`. Acceptance:
// exact-match classification renders every auth/quota code as its class, in both OpenAI and
// Anthropic naming.
#[test]
fn contract_codes_classify_without_substring_probes() {
    let mk = |code: &'static str| NormalizedError {
        code,
        message: "m".into(),
        status: 401,
        retryable: false,
        channel_scoped: false,
        retry_after_ms: None,
    };

    assert_eq!(
        to_openai_shape(&mk(code::INVALID_API_KEY))["error"]["type"],
        "invalid_api_key",
        "upstream-side auth failure (contract code) must not collapse to api_error"
    );
    assert_eq!(
        to_openai_shape(&mk(code::TOKEN_EXPIRED))["error"]["type"],
        "invalid_api_key"
    );
    assert_eq!(
        to_openai_shape(&mk(code::INSUFFICIENT_QUOTA))["error"]["type"],
        "insufficient_quota"
    );
    assert_eq!(
        to_anthropic_shape(&mk(code::INVALID_API_KEY))["error"]["type"],
        "authentication_error"
    );
}

// **Typed-classification fix (other direction)**: an arbitrary code that merely *contains* a
// probe substring must not be classified. Scenario: an unmodeled code like a vendor passthrough
// string; the old `code.contains(..)` probes mapped it into a bucket it does not belong to.
// Acceptance: only the exact vocabulary classifies; everything else renders `api_error`.
#[test]
fn substring_lookalikes_do_not_classify() {
    let mk = |code: &'static str| NormalizedError {
        code,
        message: "m".into(),
        status: 400,
        retryable: false,
        channel_scoped: false,
        retry_after_ms: None,
    };

    // Old probes that would have fired: contains("auth"), contains("rate"), contains("quota").
    for look_alike in [
        "authorized_personnel",
        "moderator_status",
        "author_quota_note",
    ] {
        assert_eq!(
            to_openai_shape(&mk(look_alike))["error"]["type"],
            "api_error",
            "`{look_alike}` is not an error code, it must fall back"
        );
    }
}

// Status-code mapping: identical across all four protocols (different shapes, same source
// codes).
#[test]
fn status_codes_are_shared_across_protocols() {
    let cases: [(StageError, u16); 5] = [
        (StageError::Unauthenticated("x".into()), 401),
        (StageError::Forbidden("x".into()), 403),
        (
            StageError::NoRoute {
                group: "g".into(),
                model: "m".into(),
            },
            404,
        ),
        (StageError::RateLimited, 429),
        (
            StageError::Unavailable {
                group: "g".into(),
                model: "m".into(),
                retry_after_ms: 8_000,
            },
            504,
        ),
    ];

    for (err, expected) in cases {
        for target in [
            ProtocolKind::OpenAI,
            ProtocolKind::OpenAIResp,
            ProtocolKind::Anthropic,
            ProtocolKind::Gemini,
        ] {
            // StageError is not Clone: rebuild an equivalent error per case.
            let fresh = match &err {
                StageError::Unauthenticated(m) => StageError::Unauthenticated(m.clone()),
                StageError::Forbidden(m) => StageError::Forbidden(m.clone()),
                StageError::NoRoute { group, model } => StageError::NoRoute {
                    group: group.clone(),
                    model: model.clone(),
                },
                StageError::RateLimited => StageError::RateLimited,
                StageError::Unavailable {
                    group,
                    model,
                    retry_after_ms,
                } => StageError::Unavailable {
                    group: group.clone(),
                    model: model.clone(),
                    retry_after_ms: *retry_after_ms,
                },
                other => panic!("test case only covers the five categories above, actual: {other}"),
            };
            let resp = map_error(&fresh, target);
            assert_eq!(
                resp.status().as_u16(),
                expected,
                "{target:?} status code must match error semantics"
            );
        }
    }
    // Unavailable(504) must carry a Retry-After header (ceil(8000/1000)=8s).
    let resp = map_error(
        &StageError::Unavailable {
            group: "g".into(),
            model: "m".into(),
            retry_after_ms: 8_000,
        },
        ProtocolKind::OpenAI,
    );
    assert_eq!(
        resp.headers()
            .get(http::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok()),
        Some("8"),
        "Unavailable should carry Retry-After: 8s"
    );
}

// Gemini status-name table: Google's SDK classifies by these enum names.
#[test]
fn gemini_status_names_follow_http_status() {
    let mk = |status: u16| NormalizedError {
        code: "c",
        message: "m".into(),
        status,
        retryable: false,
        channel_scoped: false,
        retry_after_ms: None,
    };

    assert_eq!(
        to_gemini_shape(&mk(400))["error"]["status"],
        "INVALID_ARGUMENT"
    );
    assert_eq!(
        to_gemini_shape(&mk(403))["error"]["status"],
        "PERMISSION_DENIED"
    );
    assert_eq!(to_gemini_shape(&mk(404))["error"]["status"], "NOT_FOUND");
    assert_eq!(
        to_gemini_shape(&mk(429))["error"]["status"],
        "RESOURCE_EXHAUSTED"
    );
    assert_eq!(to_gemini_shape(&mk(503))["error"]["status"], "INTERNAL");
}

// Error responses must carry the application/json content type: client SDKs use it to decide
// whether to parse the body.
#[test]
fn error_response_is_json_content_type() {
    let resp = map_error(&StageError::RateLimited, ProtocolKind::OpenAI);
    assert_eq!(
        resp.headers()
            .get(http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()),
        Some("application/json")
    );
}

// b+c: no cooling channel (retry_after_ms=0) → map_error's 503 does not promise Retry-After.
#[test]
fn unavailable_without_cooling_maps_to_503() {
    let resp = map_error(
        &StageError::Unavailable {
            group: "g".into(),
            model: "m".into(),
            retry_after_ms: 0,
        },
        ProtocolKind::OpenAI,
    );
    assert_eq!(
        resp.status().as_u16(),
        503,
        "No cooling (ra=0) should be 503 unavailable, not 504"
    );
    assert!(
        resp.headers().get(http::header::RETRY_AFTER).is_none(),
        "No cooling (ra=0) must not carry Retry-After header"
    );
}
