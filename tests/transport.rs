//! Transport and process-config behavior.
//!
//! Purpose: the standalone process converts between actix's `http` 0.2 and the pipeline's
//! `http` 1, decides the client address, guards the served path scope and bounds the
//! request body. A mistake in any of these reaches every request, so each is asserted
//! directly rather than through compilation alone.
//!
//! Scenarios: config defaults + required catalog, header conversion, right-to-left
//! `x-forwarded-for` selection, path scope, hop-by-hop stripping and status mapping, and
//! the handler's 413 / 404 / probe responses.
//!
//! Acceptance: every conversion preserves the intended value, untrusted peers cannot
//! forge their address, oversized bodies are rejected before the pipeline, and probe
//! endpoints answer without a token.

use std::net::IpAddr;
use std::sync::Arc;

use actix_web::http::header::{HeaderName, HeaderValue};
use actix_web::{App, web};
use gateway_daemon::catalog::CatalogCounts;
use gateway_daemon::config::DaemonConfig;
use gateway_daemon::server::{
    AppState, client_addr, convert_headers, dispatch, forwarded_for, healthz, is_data_plane_path,
    is_hop_by_hop, readyz, to_actix_response,
};
use gateway_facade::Pipeline;

use gateway_metering::calibration::CalibrationLog;
fn ip(text: &str) -> IpAddr {
    text.parse().expect("test address must parse")
}

fn forwarded(headers: &mut actix_web::http::header::HeaderMap, value: &'static str) {
    headers.append(
        HeaderName::from_static("x-forwarded-for"),
        HeaderValue::from_static(value),
    );
}

#[test]
fn client_address_uses_rightmost_untrusted_hop() {
    let mut headers = actix_web::http::header::HeaderMap::new();
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
    let mut headers = actix_web::http::header::HeaderMap::new();
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
    let headers = actix_web::http::header::HeaderMap::new();
    assert_eq!(
        client_addr(None, &headers, &[]),
        ip("0.0.0.0"),
        "without a socket peer the address must not be invented"
    );
}

#[test]
fn header_conversion_preserves_names_and_duplicates() {
    let mut source = actix_web::http::header::HeaderMap::new();
    source.append(
        HeaderName::from_static("content-type"),
        HeaderValue::from_static("application/json"),
    );
    source.append(
        HeaderName::from_static("x-request-id"),
        HeaderValue::from_static("abc"),
    );
    source.append(
        HeaderName::from_static("x-request-id"),
        HeaderValue::from_static("def"),
    );

    let converted = convert_headers(&source);
    assert_eq!(
        converted.get("content-type").and_then(|v| v.to_str().ok()),
        Some("application/json")
    );
    assert_eq!(converted.get_all("x-request-id").iter().count(), 2);
}

#[test]
fn path_scope_matches_the_axum_guard() {
    for served in ["/v1", "/v1/chat/completions", "/v1beta/models/gpt-4o"] {
        assert!(is_data_plane_path(served), "{served} must be served");
    }
    for rejected in ["/healthz", "/v2/chat", "/v1x", "/"] {
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
fn response_conversion_maps_status_and_drops_framing_headers() {
    let mut response = http::Response::new(axum::body::Body::from("hi"));
    *response.status_mut() = http::StatusCode::IM_A_TEAPOT;
    response.headers_mut().insert(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("text/plain"),
    );
    response.headers_mut().insert(
        http::header::CONTENT_LENGTH,
        http::HeaderValue::from_static("2"),
    );

    let converted = to_actix_response(response);
    assert_eq!(converted.status().as_u16(), 418);
    assert!(converted.headers().get("content-type").is_some());
    assert!(
        converted.headers().get("content-length").is_none(),
        "actix frames the body itself; the upstream length would be wrong"
    );
}

fn test_state(max_request_bytes: usize) -> web::Data<AppState> {
    web::Data::new(AppState::new(
        Arc::new(Pipeline::new()),
        max_request_bytes,
        Vec::new(),
        Arc::new(CalibrationLog::new()),
        CatalogCounts::default(),
    ))
}

#[actix_web::test]
async fn probes_answer_without_a_token() {
    let app = actix_web::test::init_service(
        App::new()
            .app_data(test_state(1024))
            .route("/healthz", web::get().to(healthz))
            .route("/readyz", web::get().to(readyz))
            .default_service(web::to(dispatch)),
    )
    .await;

    let health = actix_web::test::call_service(
        &app,
        actix_web::test::TestRequest::get()
            .uri("/healthz")
            .to_request(),
    )
    .await;
    assert_eq!(health.status().as_u16(), 200);

    let ready = actix_web::test::call_service(
        &app,
        actix_web::test::TestRequest::get()
            .uri("/readyz")
            .to_request(),
    )
    .await;
    assert_eq!(ready.status().as_u16(), 200);
    let body = actix_web::test::read_body(ready).await;
    let json: serde_json::Value = serde_json::from_slice(&body).expect("readyz must return JSON");
    assert_eq!(json["status"], "ready");
}

#[actix_web::test]
async fn unknown_path_is_404_before_the_pipeline() {
    let app = actix_web::test::init_service(
        App::new()
            .app_data(test_state(1024))
            .default_service(web::to(dispatch)),
    )
    .await;

    let response = actix_web::test::call_service(
        &app,
        actix_web::test::TestRequest::get()
            .uri("/nope")
            .to_request(),
    )
    .await;
    assert_eq!(response.status().as_u16(), 404);
}

#[actix_web::test]
async fn oversized_body_is_rejected_before_the_pipeline() {
    let app = actix_web::test::init_service(
        App::new()
            .app_data(test_state(8))
            .default_service(web::to(dispatch)),
    )
    .await;

    let request = actix_web::test::TestRequest::post()
        .uri("/v1/chat/completions")
        .set_payload(vec![b'a'; 64])
        .to_request();
    let response = actix_web::test::call_service(&app, request).await;
    assert_eq!(response.status().as_u16(), 413);
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
