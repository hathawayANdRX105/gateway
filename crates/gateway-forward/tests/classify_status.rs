//! Upstream status code classification behavior — driven through the real
//! `ReqwestEgress` + a local mock upstream (same `spawn_mock` shape as
//! `proxy_degraded.rs`), not by directly calling the private `classify_status`.
//!
//! **Which bug turns this red**: Rust `match` takes the first matching arm, and
//! `400..=499` is a catch-all. If the dedicated `408` arm gets deleted, reordered
//! after the catch-all, or mislabeled `retryable: false`, then `408` falls through
//! to `400..=499` → `retryable = false` → the retry chain breaks on transient
//! "upstream gateway timeout" failures (no switching to another candidate).
//! The 400 control case pins the flip side of the same table: the catch-all arm
//! must stay non-retryable, and 408 must not turn all of 4xx retryable.

use bytes::Bytes;
use gateway_forward::egress::parse_retry_after_ms;
use gateway_forward::egress::{Egress, ReqwestEgress, Timeouts};

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::time::Duration;

const TIMEOUTS: Timeouts = Timeouts {
    connect_ms: 2_000,
    first_byte_ms: 2_000,
    total_ms: 5_000,
    stream_idle_ms: 300_000,
    stream_max_ms: 1_800_000,
};

/// Minimal HTTP/1.1 mock: every request gets a fixed `status` + JSON body,
/// `connection: close`. First drains the headers + the body declared by
/// content-length, then responds (closing early would trigger RST and degrade a
/// 408 semantic response into a 502 transport error).
/// Returns the port.
fn spawn_mock(status: u16, body: &'static [u8]) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    let port = listener.local_addr().expect("local_addr").port();
    std::thread::spawn(move || {
        let head = format!(
            "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            body.len()
        );
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut content_length = 0usize;
            loop {
                let mut line = String::new();
                if reader.read_line(&mut line).unwrap_or(0) == 0 {
                    break;
                }
                let t = line.trim().to_ascii_lowercase();
                if t.is_empty() {
                    break;
                }
                if let Some(v) = t.strip_prefix("content-length:") {
                    content_length = v.trim().parse().unwrap_or(0);
                }
            }
            if content_length > 0 {
                let mut discard = vec![0u8; content_length];
                let _ = reader.read_exact(&mut discard);
            }
            let _ = stream.write_all(head.as_bytes());
            let _ = stream.write_all(body);
            let _ = stream.flush();
            let _ = stream.shutdown(std::net::Shutdown::Write);
        }
    });
    port
}

/// Real send → classification, returns the `NormalizedError` the egress layer
/// hands back to the retry loop.
async fn classify(status: u16, body: &'static [u8]) -> gateway_core::error::NormalizedError {
    let port = spawn_mock(status, body);
    let egress = ReqwestEgress::with_client(reqwest::Client::new(), Duration::from_secs(2));
    let url = format!("http://127.0.0.1:{port}/v1/chat/completions");
    let headers = [("content-type".to_string(), "application/json".to_string())];
    let request_body = Bytes::from_static(b"{\"model\":\"m\"}");
    egress
        .execute(&url, &headers, request_body, &TIMEOUTS)
        .await
        .err()
        .unwrap_or_else(|| panic!("{status} is non-2xx, it must be classified as an error"))
}

#[tokio::test]
async fn upstream_408_is_a_retryable_upstream_error() {
    // 408 Request Timeout = upstream gateway timeout, a transient condition:
    // another channel may well succeed, so it must be retryable and must not
    // be treated as a request-side fault.
    let err = classify(408, br#"{"error":{"message":"upstream timed out"}}"#).await;
    assert_eq!(err.code, gateway_core::error::code::UPSTREAM_ERROR);
    assert_eq!(err.status, 408, "status is passed through unchanged");
    assert!(
        err.retryable,
        "408 must be retryable, otherwise the retry chain breaks on transient upstream timeouts"
    );
    assert!(
        !err.channel_scoped,
        "408 is not channel-scoped (already retryable), it should not additionally downgrade layers"
    );
    assert_eq!(
        err.message, "{\"error\":{\"message\":\"upstream timed out\"}}",
        "the body preview is passed through to the error message"
    );
}

#[tokio::test]
async fn other_4xx_stay_non_retryable() {
    // Control case: the `400..=499` catch-all still reads as a bad request (non-retryable).
    // If adding the 408 arm made the catch-all retryable, this turns red.
    let err = classify(400, br#"{"error":{"message":"bad request"}}"#).await;
    assert_eq!(err.code, gateway_core::error::code::UPSTREAM_ERROR);
    assert_eq!(err.status, 400);
    assert!(
        !err.retryable,
        "a bad request is a request-side fault, switching channels cannot save it"
    );
    assert!(!err.channel_scoped);
}

#[tokio::test]
async fn neighbouring_retryable_codes_are_unaffected() {
    // 429 / 5xx already shared the retryable semantics; pinning them prevents
    // the 408 arm from shadowing them.
    let throttled = classify(429, br#"{"error":{"message":"slow down"}}"#).await;
    assert_eq!(throttled.code, gateway_core::error::code::RATE_LIMITED);
    assert_eq!(throttled.status, 429);
    assert!(throttled.retryable);

    let server_error = classify(503, br#"{"error":{"message":"unavailable"}}"#).await;
    assert_eq!(server_error.code, gateway_core::error::code::UPSTREAM_ERROR);
    assert_eq!(server_error.status, 503);
    assert!(server_error.retryable);
}

// ---------- Retry-After header parsing (RFC 9110 §10.2.1) ----------

const NOW_MS: i64 = 1_700_000_000_000; // 2023-11-14 22:13:20 UTC

#[test]
fn retry_after_delta_seconds_to_ms() {
    assert_eq!(parse_retry_after_ms(Some("0"), NOW_MS), Some(0));
    assert_eq!(parse_retry_after_ms(Some("5"), NOW_MS), Some(5_000));
    assert_eq!(
        parse_retry_after_ms(Some("86400"), NOW_MS),
        Some(86_400_000),
        "large value saturating_mul does not overflow"
    );
    // Leading/trailing whitespace is tolerated (the header value is trimmed).
    assert_eq!(parse_retry_after_ms(Some("  30 "), NOW_MS), Some(30_000));
}

#[test]
fn retry_after_http_date_future_is_delta() {
    // NOW_MS = 2023-11-14 22:13:20 UTC; an IMF-fixdate 60s after it.
    // The weekday name must agree with the date (2023-11-14 is a Tuesday): the RFC 2822
    // parser validates weekday consistency and rejects a mismatched string instead of
    // ignoring it.
    let future = "Tue, 14 Nov 2023 22:14:20 GMT";
    assert_eq!(parse_retry_after_ms(Some(future), NOW_MS), Some(60_000));
    // The UTC variant (non-GMT suffix) parses as well.
    let future_utc = "Tue, 14 Nov 2023 22:14:20 UTC";
    assert_eq!(parse_retry_after_ms(Some(future_utc), NOW_MS), Some(60_000));
}

#[test]
fn retry_after_http_date_past_is_none() {
    // A past instant → negative delta → None (a negative cooldown must never be armed).
    let past = "Tue, 14 Nov 2023 22:12:20 GMT"; // NOW_MS - 60s
    assert_eq!(parse_retry_after_ms(Some(past), NOW_MS), None);
}

#[test]
fn retry_after_negative_or_garbage_is_none() {
    // Negative delta-seconds: `-3` fails u64 parsing → falls into the date branch → also
    // fails → None.
    assert_eq!(parse_retry_after_ms(Some("-3"), NOW_MS), None);
    // Non-numeric / empty / unknown layout.
    assert_eq!(parse_retry_after_ms(Some("soon"), NOW_MS), None);
    assert_eq!(parse_retry_after_ms(Some(""), NOW_MS), None);
    assert_eq!(parse_retry_after_ms(Some("   "), NOW_MS), None);
    assert_eq!(
        parse_retry_after_ms(Some("15 Nov 2023 10:00:00"), NOW_MS),
        None
    );
    assert_eq!(parse_retry_after_ms(None, NOW_MS), None);
}

/// Purpose: pin the parity contract between forward's client-facing
/// `egress::classify_status` and core's `STATUS_RULES` table.
/// Scenario: every status 100..=600 classified by both tables.
/// Accepts: `retryable` and `channel_scoped` agree everywhere (the flags that drive
/// retry/demotion semantics); `code` agrees everywhere EXCEPT the deliberately
/// divergent generic-4xx set (400..=499 minus 401/403/408/429, where egress reports
/// `UPSTREAM_ERROR` for fatal request errors while core's table says
/// `INVALID_API_KEY`) — that divergence is documented on both sides, and this test
/// fails if either table drifts beyond it.
#[test]
fn egress_and_core_tables_agree_on_flags_and_documented_code_divergence() {
    use gateway_core::error::code;

    for status in 100u16..=600 {
        let egress = gateway_forward::egress::classify_status(status, "preview", None);
        let core = gateway_core::error::NormalizedError::from_status(status, "preview");

        assert_eq!(
            egress.retryable, core.retryable,
            "retryable must not drift between egress and core at {status}"
        );
        assert_eq!(
            egress.channel_scoped, core.channel_scoped,
            "channel_scoped must not drift between egress and core at {status}"
        );

        let in_divergent_set =
            (400..=499).contains(&status) && !matches!(status, 401 | 403 | 408 | 429);
        if in_divergent_set {
            assert_eq!(
                egress.code,
                code::UPSTREAM_ERROR,
                "egress generic 4xx must stay UPSTREAM_ERROR (fatal request error) at {status}"
            );
            assert_eq!(
                core.code, code::INVALID_API_KEY,
                "core generic 4xx must stay INVALID_API_KEY at {status}"
            );
        } else {
            assert_eq!(
                egress.code, core.code,
                "code must not drift between egress and core at {status}"
            );
        }
    }
}
