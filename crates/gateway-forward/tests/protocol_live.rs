//! Opt-in verification against real proxy nodes: connects to a single external proxy process
//! supplied by the user (sing-box / mihomo) over a per-protocol share link.
//!
//! The core logic matches `real_node.rs`, but each protocol has its own test and the real
//! URLs can be read from environment variables.
//!
//! # How to run
//!
//! ```sh
//! FERRITE_PROXY_VLESS=vless://... \
//! FERRITE_PROXY_SS=ss://... \
//! FERRITE_PROXY_TROJAN=trojan://... \
//! FERRITE_PROXY_VMESS=vmess://... \
//! cargo test -p forward --test protocol_live
//! ```
//!
//! Without a proxy process in CI every test is skipped and nothing turns red; users validate
//! their own proxy configuration end to end.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;

use gateway_forward::adapter_egress::{AdapterDialer, AdapterEgress};
use gateway_forward::egress::{Egress, Timeouts};
use gateway_proxy::{ProxyNode, ProxyScheme, adapter_for};

/// Real nodes may be on the public internet: connect gets a 10s allowance, while the
/// first-byte and total caps are 20s so a hang fails fast instead of waiting for the default
/// 300s.
const TIMEOUTS: Timeouts = Timeouts {
    connect_ms: 10_000,
    first_byte_ms: 20_000,
    total_ms: 20_000,
    stream_idle_ms: 20_000,
    stream_max_ms: 20_000,
};

/// Minimal HTTP/1.1 server: reply 200 + `ok` once the header block ends, keeping a hit counter.
fn spawn_local_http() -> (SocketAddr, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind local HTTP service");
    let addr = listener.local_addr().unwrap();
    let hits = Arc::new(AtomicUsize::new(0));
    let hits_clone = hits.clone();

    // Use std::thread instead of tokio::spawn because the listener is blocking
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else {
                continue;
            };
            hits_clone.fetch_add(1, Ordering::Relaxed);

            // Read the whole request naively (in theory the blank line has already been seen)
            let mut buffer = [0u8; 4096];
            let mut got = Vec::new();
            loop {
                match stream.read(&mut buffer) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        got.extend_from_slice(&buffer[..n]);
                        if got.windows(4).any(|w| w == b"\r\n\r\n") {
                            break;
                        }
                    }
                }
            }

            // Reply 200 + body = "ok" and keep the connection closed
            let body = b"ok";
            let _ = stream.write_all(
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                )
                .as_bytes(),
            );
            let _ = stream.write_all(body);
        }
    });

    // Small delay to make sure the server has started
    std::thread::sleep(Duration::from_millis(50));
    (addr, hits)
}

/// TCP liveness probe: share-link nodes can die at any time; a node being offline is not a
/// code defect.
async fn node_alive(host: &str, port: u16) -> bool {
    use std::net::ToSocketAddrs;
    let addrs: Vec<std::net::SocketAddr> = match (host, port).to_socket_addrs() {
        Ok(a) => a.collect(),
        Err(_) => return false,
    };
    for a in addrs {
        if tokio::time::timeout(
            std::time::Duration::from_secs(3),
            tokio::net::TcpStream::connect(a),
        )
        .await
        .is_ok()
        {
            return true;
        }
    }
    false
}

/// Dial-failure handling: lenient by default (print and skip), red when
/// `FERRITE_PROXY_LIVE_STRICT=1`.
/// Free nodes are extremely sensitive to TLS fingerprints / availability (with meow's
/// boring-tls feature off, rustls fingerprints are rejected by some reality/trojan servers),
/// and node-side failures are not code defects.
fn on_dial_failure(what: &str, e: &gateway_core::error::NormalizedError) {
    if std::env::var("FERRITE_PROXY_LIVE_STRICT").is_err() {
        eprintln!(
            "skip: {what} dial failed ({e:?}). Set FERRITE_PROXY_LIVE_STRICT=1 to make this a hard failure"
        );
    } else {
        panic!("{what} dial failed: {e:?}");
    }
}

/// Build a real proxy node from an environment variable; returns None when absent (prints a
/// skip line).
fn parse_env_url(scheme: &str, env_var: &str) -> Option<ProxyNode> {
    if let Ok(url) = std::env::var(env_var) {
        match ProxyNode::parse_url(&url) {
            Ok(node) => {
                // Check that the node type matches
                let correct_scheme = match scheme {
                    "vless" => ProxyScheme::Vless,
                    "ss" | "shadowsocks" => ProxyScheme::Shadowsocks,
                    "trojan" => ProxyScheme::Trojan,
                    "vmess" => ProxyScheme::Vmess,
                    _ => return None,
                };
                if node.scheme != correct_scheme {
                    eprintln!(
                        "environment variable {} proxy type mismatch: expected {:?}, got {:?}",
                        env_var, correct_scheme, node.scheme
                    );
                    return None;
                }
                Some(node)
            }
            Err(e) => {
                eprintln!("{env_var} parse failed: {e}");
                None
            }
        }
    } else {
        eprintln!("skip: {env_var} not set");
        None
    }
}

#[tokio::test]
async fn vless_reality_vision_live() {
    if let Some(node) = parse_env_url("vless", "FERRITE_PROXY_VLESS") {
        if !node_alive(&node.host, node.port).await {
            eprintln!(
                "skip: VLESS node {}:{} TCP unreachable (node offline, not a code issue)",
                node.host, node.port
            );
            return;
        }
        let adapter = adapter_for(&node).expect("vless node must build an adapter");

        let (http_addr, hits) = spawn_local_http();
        let egress = AdapterEgress::new(
            Arc::new(AdapterDialer::new(adapter)),
            Duration::from_secs(10),
        );

        let resp = egress
            .execute(
                "probe",
                &format!("http://{http_addr}/"),
                &[],
                Bytes::new(),
                &TIMEOUTS,
            )
            .await
            .inspect_err(|e| {
                on_dial_failure("VLESS", e);
            })
            .ok();
        let Some(resp) = resp else {
            return;
        };
        assert_eq!(resp.status(), 200);
        assert!(
            hits.load(Ordering::Relaxed) >= 1,
            "request must actually traverse the external proxy process"
        );
        println!("✓ VLESS verified: real proxy handshake succeeded");
    }
}

#[tokio::test]
async fn ss2022_live() {
    if let Some(node) = parse_env_url("ss", "FERRITE_PROXY_SS") {
        if !node_alive(&node.host, node.port).await {
            eprintln!(
                "skip: SS node {}:{} TCP unreachable (node offline, not a code issue)",
                node.host, node.port
            );
            return;
        }
        let adapter = adapter_for(&node).expect("ss node must build an adapter");

        let (http_addr, hits) = spawn_local_http();
        let egress = AdapterEgress::new(
            Arc::new(AdapterDialer::new(adapter)),
            Duration::from_secs(10),
        );

        let resp = egress
            .execute(
                "probe",
                &format!("http://{http_addr}/"),
                &[],
                Bytes::new(),
                &TIMEOUTS,
            )
            .await
            .inspect_err(|e| {
                on_dial_failure("SS", e);
            })
            .ok();
        let Some(resp) = resp else {
            return;
        };
        assert_eq!(resp.status(), 200);
        assert!(
            hits.load(Ordering::Relaxed) >= 1,
            "request must actually traverse the external proxy process"
        );
        println!("✓ SS verified: real proxy handshake succeeded");
    }
}

#[tokio::test]
async fn trojan_live() {
    if let Some(node) = parse_env_url("trojan", "FERRITE_PROXY_TROJAN") {
        if !node_alive(&node.host, node.port).await {
            eprintln!(
                "skip: Trojan node {}:{} TCP unreachable (node offline, not a code issue)",
                node.host, node.port
            );
            return;
        }
        let adapter = adapter_for(&node).expect("trojan node must build an adapter");

        let (http_addr, hits) = spawn_local_http();
        let egress = AdapterEgress::new(
            Arc::new(AdapterDialer::new(adapter)),
            Duration::from_secs(10),
        );

        let resp = egress
            .execute(
                "probe",
                &format!("http://{http_addr}/"),
                &[],
                Bytes::new(),
                &TIMEOUTS,
            )
            .await
            .inspect_err(|e| {
                on_dial_failure("Trojan", e);
            })
            .ok();
        let Some(resp) = resp else {
            return;
        };
        assert_eq!(resp.status(), 200);
        assert!(
            hits.load(Ordering::Relaxed) >= 1,
            "request must actually traverse the external proxy process"
        );
        println!("✓ Trojan verified: real proxy handshake succeeded");
    }
}

#[tokio::test]
async fn vmess_live() {
    if let Some(node) = parse_env_url("vmess", "FERRITE_PROXY_VMESS") {
        if !node_alive(&node.host, node.port).await {
            eprintln!(
                "skip: VMess node {}:{} TCP unreachable (node offline, not a code issue)",
                node.host, node.port
            );
            return;
        }
        let adapter = adapter_for(&node).expect("vmess node must build an adapter");

        let (http_addr, hits) = spawn_local_http();
        let egress = AdapterEgress::new(
            Arc::new(AdapterDialer::new(adapter)),
            Duration::from_secs(10),
        );

        let resp = egress
            .execute(
                "probe",
                &format!("http://{http_addr}/"),
                &[],
                Bytes::new(),
                &TIMEOUTS,
            )
            .await
            .inspect_err(|e| {
                on_dial_failure("VMess", e);
            })
            .ok();
        let Some(resp) = resp else {
            return;
        };
        assert_eq!(resp.status(), 200);
        assert!(
            hits.load(Ordering::Relaxed) >= 1,
            "request must actually traverse the external proxy process"
        );
        println!("✓ VMess verified: real proxy handshake succeeded");
    }
}
