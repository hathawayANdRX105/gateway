//! `adapter_egress` bridge test — a local HTTP server exercises the full
//! "dialer → HTTP egress" chain.
//!
//! No real network or proxy node is needed: the `StreamDialer` implementation connects
//! directly to a local echo server with `TcpStream::connect`, and `AdapterEgress` sends
//! real HTTP/1.1 requests over it. Once this chain holds, swapping the dialer for meow's
//! `ProxyAdapter::dial_tcp` is the production path.
//!
//! The TLS path (HTTPS) needs a real certificate or a self-signed CA and is not covered
//! by local unit tests — the examples smoke test verifies it against a real upstream.

use std::io::{Read, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;

use futures_util::StreamExt;
use gateway_forward::adapter_egress::{AdapterEgress, DialedStream, StreamDialer};
use gateway_forward::egress::Egress;
use gateway_forward::egress::Timeouts;

/// Minimal HTTP/1.1 server on a synchronous thread: replies 200 as soon as the request
/// headers end. Returns the listen address and a hit counter (so tests can assert "the
/// request really went through the dialer").
fn spawn_local_http() -> (std::net::SocketAddr, Arc<AtomicUsize>) {
    use std::net::TcpListener;
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    let addr = listener.local_addr().expect("local_addr");
    let hits = Arc::new(AtomicUsize::new(0));
    let hits2 = Arc::clone(&hits);
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            hits2.fetch_add(1, Ordering::Relaxed);
            std::thread::spawn(move || {
                let mut buf = [0u8; 4096];
                let mut got = Vec::new();
                // Read until \r\n\r\n (end of request headers); loopback is fast
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
                let body = b"local-ok";
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = stream.write_all(resp.as_bytes());
                let _ = stream.write_all(body);
            });
        }
    });
    (addr, hits)
}

/// Dialer that connects to the local server directly — a stand-in: in production meow's
/// `ProxyAdapter` plays this role.
#[derive(Debug)]
struct LocalDialer(std::net::SocketAddr);

#[async_trait::async_trait]
impl StreamDialer for LocalDialer {
    async fn dial(&self, host: &str, port: u16) -> std::io::Result<Box<dyn DialedStream>> {
        // Ignore the caller's host/port: the test always connects to the local server.
        // This also lets the test assert "the URI host is unrelated to the dial target" —
        // the bridge does no DNS; target resolution is entirely up to the dialer (the proxy
        // protocol server in production).
        let _ = (host, port);
        let conn = tokio::net::TcpStream::connect(self.0).await?;
        Ok(Box::new(conn))
    }
}

/// Dialer that always fails — simulates an unreachable proxy node.
#[derive(Debug)]
struct RefusingDialer;

#[async_trait::async_trait]
impl StreamDialer for RefusingDialer {
    async fn dial(&self, _host: &str, _port: u16) -> std::io::Result<Box<dyn DialedStream>> {
        Err(std::io::Error::other("dial refused (simulated)"))
    }
}

/// Deliberately slow dialer — verifies the connect timeout takes effect.
#[derive(Debug)]
struct SlowDialer(Duration);

#[async_trait::async_trait]
impl StreamDialer for SlowDialer {
    async fn dial(&self, _host: &str, _port: u16) -> std::io::Result<Box<dyn DialedStream>> {
        tokio::time::sleep(self.0).await;
        Err(std::io::Error::other("slow dial should have timed out"))
    }
}

fn headers_for(body_len: usize) -> Vec<(String, String)> {
    vec![
        ("content-type".into(), "application/json".into()),
        ("content-length".into(), body_len.to_string()),
    ]
}

/// Normal path: dialer connects to the local server → Egress sends real HTTP → 200 +
/// correct body, and request headers really are carried (the server echoes no header count,
/// so assert on the hit counter instead).
#[tokio::test]
async fn egress_sends_http_over_dialer() {
    let (addr, hits) = spawn_local_http();
    let egress = AdapterEgress::new(Arc::new(LocalDialer(addr)), Duration::from_secs(5));

    let body = Bytes::from_static(br#"{"model":"probe"}"#);
    let resp = egress
        .execute(
            &format!("http://{addr}/v1/chat/completions"),
            &headers_for(body.len()),
            body,
            &Timeouts::default(),
        )
        .await
        .expect("egress over local dialer should succeed");

    assert_eq!(resp.status(), 200);
    assert_eq!(resp.content_type(), "text/plain");
    let mut stream = resp.into_body_stream();
    let mut collected = Vec::new();
    while let Some(chunk) = stream.next().await {
        collected.extend_from_slice(&chunk.expect("body chunk"));
    }
    assert_eq!(collected, b"local-ok");
    assert_eq!(
        hits.load(Ordering::Relaxed),
        1,
        "the request must really have gone through the dialer"
    );
}

/// dial failure → 502 with retryable=true (aligned with the transport-error mapping in
/// `ReqwestEgress`). The retry layer swaps candidate nodes based on this semantic; a wrong
/// semantic would keep a bad node out of cooldown.
#[tokio::test]
async fn dial_failure_is_retryable_502() {
    let egress = AdapterEgress::new(Arc::new(RefusingDialer), Duration::from_secs(5));
    let err = egress
        .execute(
            "http://upstream.invalid/v1/chat/completions",
            &headers_for(2),
            Bytes::from_static(b"{}"),
            &Timeouts::default(),
        )
        .await
        .expect_err("dial failure must error");
    assert_eq!(err.status, 502, "got: {err:?}");
    assert!(
        err.retryable,
        "transport failure must be retryable: {err:?}"
    );
}

/// dial slower than `connect_timeout` → timeout, 504, retryable.
#[tokio::test]
async fn slow_dial_hits_connect_timeout() {
    let egress = AdapterEgress::new(
        Arc::new(SlowDialer(Duration::from_secs(30))),
        Duration::from_millis(200), // connect_timeout covers dial
    );
    let err = egress
        .execute(
            "http://upstream.invalid/v1/chat/completions",
            &headers_for(2),
            Bytes::from_static(b"{}"),
            &Timeouts::default(),
        )
        .await
        .expect_err("slow dial must time out");
    assert_eq!(err.status, 504, "timeout maps to 504: {err:?}");
    assert!(err.retryable);
}

/// Streaming: the server response has more data after the first chunk → `into_body_stream`
/// gets all of it. The first-byte pre-read + prepend logic must not drop bytes (key
/// semantics of `ReqwestEgress`).
#[tokio::test]
async fn body_stream_not_truncated() {
    // Reuse spawn_local_http: the fixed body "local-ok" (8 bytes) is enough to verify
    let (addr, _hits) = spawn_local_http();
    let egress = AdapterEgress::new(Arc::new(LocalDialer(addr)), Duration::from_secs(5));
    let resp = egress
        .execute(
            &format!("http://{addr}/"),
            &headers_for(0),
            Bytes::new(),
            &Timeouts::default(),
        )
        .await
        .expect("ok");
    let mut stream = resp.into_body_stream();
    let mut all = Vec::new();
    while let Some(chunk) = stream.next().await {
        all.extend_from_slice(&chunk.expect("chunk"));
    }
    assert_eq!(all, b"local-ok", "body must not drop first or last byte");
}
