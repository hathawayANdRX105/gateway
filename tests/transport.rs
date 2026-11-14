//! Transport and process-config behavior.
//!
//! Purpose: the standalone process decides the client address, guards the served path
//! scope, bounds the request body and drops the upstream's framing headers before the
//! response leaves the process. A mistake in any of these reaches every request, so each
//! is asserted directly rather than through compilation alone.
//!
//! Scenarios: config defaults + required catalog, right-to-left `x-forwarded-for`
//! selection, path scope, hop-by-hop stripping, the body ceiling (exact limit / over
//! limit / stream failure), and the router's 413 / 404 / probe responses.
//!
//! Acceptance: every value survives the path unchanged, untrusted peers cannot forge
//! their address, oversized bodies are rejected before the pipeline, and probe endpoints
//! answer without a token.

use std::net::IpAddr;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{HeaderMap, HeaderName, HeaderValue, Request, StatusCode};
use tower::ServiceExt;

use gateway_daemon::catalog::CatalogCounts;
use gateway_daemon::config::DaemonConfig;
use gateway_daemon::server::{
    AppState, BodyError, client_addr, forwarded_for, is_data_plane_path, is_hop_by_hop, read_body,
    router, strip_hop_by_hop,
};
use gateway_facade::Pipeline;
use gateway_facade::calibration::CalibrationLog;

fn ip(text: &str) -> IpAddr {
    text.parse().expect("test address must parse")
}

fn forwarded(headers: &mut HeaderMap, value: &'static str) {
    headers.append(
        HeaderName::from_static("x-forwarded-for"),
        HeaderValue::from_static(value),
    );
}

#[test]
fn client_address_uses_rightmost_untrusted_hop() {
    let mut headers = HeaderMap::new();
    forwarded(&mut headers, "9.9.9.9, 1.2.3.4, 10.0.0.7");
    let trusted = [ip("10.0.0.7")];

    // The trusted proxy appended its own address; the client-supplied leftmost entry
    // cannot win, and the first untrusted hop from the right is the real client.
    assert_eq!(
        client_addr(Some(ip("10.0.0.7")), &headers, &trusted),
        ip("1.2.3.4")
    );
}

#[test]
fn untrusted_peer_ignores_forwarded_header() {
    let mut headers = HeaderMap::new();
    forwarded(&mut headers, "1.2.3.4");

    assert_eq!(
        client_addr(Some(ip("203.0.113.9")), &headers, &[]),
        ip("203.0.113.9")
    );
    assert_eq!(
        forwarded_for(&headers, &[]),
        Some(ip("1.2.3.4")),
        "the pure helper still parses the header; the trust decision is in client_addr"
    );
}

#[test]
fn missing_peer_falls_back_to_unspecified() {
    let headers = HeaderMap::new();
    assert_eq!(
        client_addr(None, &headers, &[]),
        ip("0.0.0.0"),
        "without a socket peer the address must not be invented"
    );
}

#[test]
fn path_scope_matches_the_library_guard() {
    for served in ["/v1", "/v1/chat/completions", "/v1beta/models/gpt-4o"] {
        assert!(is_data_plane_path(served), "{served} must be served");
    }
    for rejected in ["/healthz", "/readyz", "/v2/chat", "/v1x", "/"] {
        assert!(
            !is_data_plane_path(rejected),
            "{rejected} must not reach the pipeline"
        );
    }
}

#[test]
fn hop_by_hop_headers_are_stripped() {
    for name in ["content-length", "transfer-encoding", "connection"] {
        assert!(is_hop_by_hop(name), "{name} must be dropped");
    }
    for name in ["content-type", "cache-control", "x-request-id"] {
        assert!(!is_hop_by_hop(name), "{name} must be forwarded");
    }
    // Header names are case-insensitive: a producer writing the framing header in
    // mixed case must be dropped just the same.
    assert!(
        is_hop_by_hop("Content-Length") && is_hop_by_hop("Transfer-Encoding"),
        "mixed-case framing headers must be dropped"
    );
}

#[test]
fn response_strip_keeps_status_and_drops_framing_headers() {
    let mut response = http::Response::new(axum::body::Body::from("hi"));
    *response.status_mut() = StatusCode::IM_A_TEAPOT;
    response.headers_mut().insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain"),
    );
    response
        .headers_mut()
        .insert(http::header::CONTENT_LENGTH, HeaderValue::from_static("2"));
    response.headers_mut().insert(
        http::header::CONNECTION,
        HeaderValue::from_static("keep-alive"),
    );

    let stripped = strip_hop_by_hop(response);
    assert_eq!(stripped.status(), StatusCode::IM_A_TEAPOT);
    assert!(
        stripped.headers().get("content-type").is_some(),
        "an end-to-end header must survive"
    );
    assert!(
        stripped.headers().get("content-length").is_none(),
        "the transport frames the body itself; the upstream length would be wrong"
    );
    assert!(stripped.headers().get("connection").is_none());
}

#[tokio::test]
async fn body_reader_bounds_at_the_exact_limit() {
    // Scenario: a body exactly at the ceiling, one byte over it, and a body stream that
    // fails mid-read.
    // Acceptance: the exact limit passes; one byte over is TooLarge; a stream error is
    // Payload (never silently truncated).
    let exact = read_body(Body::from(vec![b'a'; 8]), 8).await;
    assert_eq!(exact.expect("limit is inclusive").len(), 8);

    assert_eq!(
        read_body(Body::from(vec![b'a'; 9]), 8).await,
        Err(BodyError::TooLarge)
    );

    let failing = Body::from_stream(futures_util::stream::once(async {
        Err::<bytes::Bytes, std::io::Error>(std::io::Error::other("boom"))
    }));
    assert_eq!(read_body(failing, 8).await, Err(BodyError::Payload));
}

fn test_state(max_request_bytes: usize, billing_enabled: bool) -> Arc<AppState> {
    Arc::new(AppState::new(
        Arc::new(Pipeline::new()),
        max_request_bytes,
        Vec::new(),
        Arc::new(CalibrationLog::new()),
        CatalogCounts::default(),
        billing_enabled,
    ))
}

async fn get(state: Arc<AppState>, path: &str) -> (StatusCode, serde_json::Value) {
    let request = Request::builder()
        .uri(path)
        .body(Body::empty())
        .expect("request builds");
    let response = router(state)
        .oneshot(request)
        .await
        .expect("router answers");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body collects");
    let json = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, json)
}

#[tokio::test]
async fn probes_answer_without_a_token() {
    let (health_status, health) = get(test_state(1024, false), "/healthz").await;
    assert_eq!(health_status, StatusCode::OK);
    assert_eq!(health["status"], "ok");

    let (ready_status, ready) = get(test_state(1024, false), "/readyz").await;
    assert_eq!(ready_status, StatusCode::OK);
    assert_eq!(ready["status"], "ready");
}

#[tokio::test]
async fn readyz_reports_billing_enabled() {
    // Scenario: the deployment mounted a settlement sink (billing_enabled = true).
    // Acceptance: /readyz always returns "billing": "enabled" in the parsed body.
    let (status, json) = get(test_state(1024, true), "/readyz").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["billing"], "enabled");
}

#[tokio::test]
async fn readyz_reports_billing_disabled() {
    // Scenario: the deployment mounted no settlement sink (billing_enabled = false).
    // Acceptance: /readyz always returns "billing": "disabled" in the parsed body.
    let (status, json) = get(test_state(1024, false), "/readyz").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["billing"], "disabled");
}

#[tokio::test]
async fn unknown_path_is_404_before_the_pipeline() {
    let (status, _) = get(test_state(1024, false), "/nope").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn oversized_body_is_rejected_before_the_pipeline() {
    let request = Request::builder()
        .method(http::Method::POST)
        .uri("/v1/chat/completions")
        .body(Body::from(vec![b'a'; 64]))
        .expect("request builds");
    let response = router(test_state(8, false))
        .oneshot(request)
        .await
        .expect("router answers");
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body collects");
    let json: serde_json::Value = serde_json::from_slice(&bytes).expect("413 carries JSON");
    assert_eq!(json["error"]["type"], "invalid_request_error");
}

#[test]
fn config_defaults_fill_optional_fields_and_require_catalog() {
    let dir = std::env::temp_dir().join(format!("gatewayd-config-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    let full = dir.join("full.toml");
    std::fs::write(&full, "catalog = \"catalog.toml\"\n").expect("write config");

    let config = DaemonConfig::load(&full).expect("minimal config must load");
    assert_eq!(config.listen, "127.0.0.1:8080");
    assert_eq!(config.max_request_bytes, 32 * 1024 * 1024);
    assert_eq!(config.shutdown_timeout_secs, 30);
    assert_eq!(
        config.catalog_path(&full),
        dir.join("catalog.toml"),
        "a relative catalog resolves against the config file"
    );

    let missing = dir.join("missing.toml");
    std::fs::write(&missing, "listen = \"127.0.0.1:9000\"\n").expect("write config");
    assert!(
        DaemonConfig::load(&missing).is_err(),
        "a config without a catalog must fail at load"
    );

    std::fs::remove_dir_all(&dir).ok();
}
