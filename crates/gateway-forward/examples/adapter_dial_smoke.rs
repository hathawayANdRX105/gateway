//! Smoke test: meow adapter → adapter_egress bridge → real upstream.
//!
//! Verifies the full promise of PR #84: the stream produced by
//! `ProxyAdapter::dial_tcp` can carry a real HTTPS request through
//! `AdapterEgress` (dial + rustls TLS + hyper).
//!
//! Run: `cargo run --example adapter_dial_smoke -p forward`
//! Prerequisite: a local sing-box mixed inbound on `127.0.0.1:7890` (or change the
//! port below).

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use gateway_forward::adapter_egress::{AdapterDialer, AdapterEgress};
use gateway_forward::egress::{Egress, Timeouts};
use gateway_proxy::adapter::adapter_for;
use gateway_proxy::node::{BasicAuth, ProxyNode, ProxyScheme};

#[tokio::main]
async fn main() {
    // Isolate the system proxy: ensure traffic really goes through adapter dialing
    // rather than an environment-variable fallback.
    for var in [
        "ALL_PROXY",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "all_proxy",
        "http_proxy",
        "https_proxy",
    ] {
        // SAFETY: main clears the proxy variables before spawning any task or
        // reading any env; runtime worker threads already exist but are idle, so
        // no concurrent reader races this remove_var.
        unsafe { std::env::remove_var(var) };
    }

    let node = ProxyNode {
        id: 1,
        scheme: ProxyScheme::Socks5,
        host: "127.0.0.1".into(),
        port: 7890,
        auth: Some(BasicAuth {
            user: String::new(),
            pass: String::new(),
        }),
        channel_keys: vec!["smoke".into()],
        opts: None,
        priority: 10,
    };
    let Some(adapter) = adapter_for(&node) else {
        panic!("socks5 node must map to an adapter");
    };
    println!(
        "adapter: type={:?} addr={}",
        adapter.adapter_type(),
        adapter.addr()
    );

    let egress = AdapterEgress::new(
        Arc::new(AdapterDialer::new(adapter)),
        Duration::from_secs(10),
    );

    println!("=== 1) HTTPS via adapter dial + rustls ===");
    let resp = egress
        .execute(
            "https://httpbin.org/post",
            &[("accept".into(), "application/json".into())],
            Bytes::new(),
            &Timeouts::default(),
        )
        .await
        .expect("HTTPS over adapter dial must work");
    let (status, content_type) = (resp.status(), resp.content_type().to_string());
    println!("status={status} content_type={content_type}");
    let mut stream = resp.into_body_stream();
    let mut body = Vec::new();
    while let Some(chunk) = futures_util::StreamExt::next(&mut stream).await {
        body.extend_from_slice(&chunk.expect("body chunk"));
    }
    let text = String::from_utf8_lossy(&body);
    println!("body: {}", text.trim());
    assert!(
        text.contains("origin"),
        "httpbin.org/post must echo the origin IP: {text}"
    );
    assert_eq!(status, 200, "httpbin.org must return 200");

    println!("\n=== SMOKE PASSED: adapter dial → TLS → HTTP fully usable end to end ===");
}
