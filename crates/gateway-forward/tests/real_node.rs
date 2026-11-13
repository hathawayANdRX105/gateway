//! Opt-in verification against a real proxy node — a real process (sing-box / mihomo / any
//! SOCKS5 service) takes the place of the mock server and runs the same chain:
//! `adapter_for` → a real meow handshake → the `AdapterEgress` bridge → HTTP.
//!
//! # How to run
//!
//! ```sh
//! # local sing-box mixed inbound listens on 7890
//! FERRITE_PROXY_SOCKS5=127.0.0.1:7890 cargo test -p gateway-forward --test real_node
//! ```
//!
//! When the environment variable is absent the test **passes immediately and prints the skip
//! reason**: CI has no proxy process and must not turn red because of it.
//! The upstream target is a local HTTP server started inside the test, so even a real dial
//! **never leaves the public network** — what is verified is "the real proxy process's protocol
//! handshake + our bridge", not external connectivity.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;

use gateway_forward::adapter_egress::{AdapterDialer, AdapterEgress};
use gateway_forward::egress::{Egress, Timeouts};
use gateway_proxy::{ProxyNode, ProxyScheme, adapter_for};

/// A real node may sit on the public network, so the timeouts are generous; a hang fails within
/// 20s instead of waiting for the 300s default.
const TIMEOUTS: Timeouts = Timeouts {
    connect_ms: 10_000,
    first_byte_ms: 20_000,
    total_ms: 20_000,
    stream_idle_ms: 20_000,
    stream_max_ms: 20_000,
};

/// Minimal HTTP/1.1 server: replies 200 + `real-ok` once the headers are read, and counts hits.
fn spawn_local_http() -> (SocketAddr, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    let addr = listener.local_addr().expect("local_addr");
    let hits = Arc::new(AtomicUsize::new(0));
    let hits2 = Arc::clone(&hits);
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            hits2.fetch_add(1, Ordering::Relaxed);
            std::thread::spawn(move || {
                let mut got = Vec::new();
                let mut buf = [0u8; 4096];
                loop {
                    match stream.read(&mut buf) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            got.extend_from_slice(&buf[..n]);
                            if got.windows(4).any(|w| w == b"\r\n\r\n") {
                                break;
                            }
                        }
                    }
                }
                let body = b"real-ok";
                let _ = stream.write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    )
                    .as_bytes(),
                );
                let _ = stream.write_all(body);
            });
        }
    });
    (addr, hits)
}

/// Run the full egress chain over a real SOCKS5 node (sing-box mixed inbound, etc.).
///
/// Asserts the local HTTP server is hit — proving the bytes really traveled through the external
/// proxy process and back to us, rather than the bridge secretly connecting directly.
#[tokio::test]
async fn egress_over_real_socks5_node() {
    let Ok(proxy) = std::env::var("FERRITE_PROXY_SOCKS5") else {
        eprintln!(
            "skip: set FERRITE_PROXY_SOCKS5=<host:port> (e.g. local sing-box 127.0.0.1:7890) to dial for real"
        );
        return;
    };
    // Set but malformed: emit an actionable error rather than an obscure parse panic.
    let proxy: SocketAddr = match proxy.parse() {
        Ok(a) => a,
        Err(e) => panic!(
            "FERRITE_PROXY_SOCKS5 invalid format (expected host:port, e.g. 127.0.0.1:7890): {e}"
        ),
    };

    let (http_addr, hits) = spawn_local_http();
    let node = ProxyNode {
        id: 1,
        scheme: ProxyScheme::Socks5,
        host: proxy.ip().to_string(),
        port: proxy.port(),
        auth: None,
        opts: None,
        channel_keys: vec!["openai".to_string()],
        priority: 0,
    };
    let adapter = adapter_for(&node).expect("socks5 node must map to an adapter");
    let egress = AdapterEgress::new(
        Arc::new(AdapterDialer::new(adapter)),
        Duration::from_secs(10),
    );

    let resp = egress
        .execute(
            "probe",
            &format!("http://{http_addr}/real"),
            &[],
            Bytes::new(),
            &TIMEOUTS,
        )
        .await
        .expect("HTTP egress through a real SOCKS5 node must succeed");
    assert_eq!(resp.status(), 200);
    assert!(
        hits.load(Ordering::Relaxed) >= 1,
        "request must actually traverse the external proxy process"
    );
}
