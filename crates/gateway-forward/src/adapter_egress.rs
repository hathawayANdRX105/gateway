//! `adapter_egress` — bridges an arbitrary "dial function" into an HTTP egress.
//!
//! [`StreamDialer`] abstracts "given a target host/port, return a bidirectional stream
//! with the protocol handshake already complete"; [`AdapterEgress`] layers TLS for HTTPS
//! on top and sends requests with hyper, implementing [`crate::egress::Egress`]. On the
//! `gateway-proxy` side, meow's `ProxyAdapter` implements the dialer; tests use a local
//! TCP server — both implementations share the same bridging code.
//!
//! ## Why not reqwest
//! reqwest 0.12's custom connector is `pub(crate)` (`connect.rs:70`
//! `ConnectorBuilder`), so it cannot be injected from outside. hyper-util's legacy client
//! accepts any `tower::Service<Uri>` connector, so this module uses hyper directly with
//! `crate::egress::Egress` (`ForwardedResponse::from_stream` is a public constructor).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use futures_util::StreamExt;
use gateway_core::error::NormalizedError;
use http::Uri;
use http_body_util::{BodyExt, Full};
use hyper::Request;
use hyper::body::Incoming;
use hyper_util::client::legacy::Client;
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::time::timeout;
use tower::Service;

use crate::egress::{
    ByteStream, ForwardedResponse, Timeouts, build_header_map, finish_forwarded_response,
    parse_retry_after_ms, upstream_error,
};

/// Egress dialer: given a target host/port, return a bidirectional stream with the
/// protocol handshake complete.
///
/// The protocol handshake (SOCKS5 / VLESS / SS / Trojan…) is the implementation's job;
/// this module only layers TLS on the stream and carries HTTP over it.
#[async_trait::async_trait]
pub trait StreamDialer: Send + Sync + std::fmt::Debug {
    /// Dial `host:port`. Failures return `io::Error` (mapped to a retryable 502).
    async fn dial(&self, host: &str, port: u16) -> std::io::Result<Box<dyn DialedStream>>;
}

/// A dialed stream. `Unpin + Send + Sync` let `TokioIo` wrap it for hyper.
pub trait DialedStream: AsyncRead + AsyncWrite + Unpin + Send + Sync {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send + Sync> DialedStream for T {}

/// Adaptation from meow `ProxyAdapter` to [`StreamDialer`].
///
/// The `dial(host, port)` semantics line up: meow's `Metadata.host/dst_port` are the
/// dial target, the protocol handshake happens inside `dial_tcp`, and the returned
/// stream carries the upper-layer traffic directly.
pub struct AdapterDialer(Arc<dyn gateway_proxy::ProxyAdapter>);

impl AdapterDialer {
    /// Build a dialer from a meow `ProxyAdapter`.
    pub fn new(adapter: Arc<dyn gateway_proxy::ProxyAdapter>) -> Self {
        Self(adapter)
    }
}

impl std::fmt::Debug for AdapterDialer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdapterDialer").finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl StreamDialer for AdapterDialer {
    async fn dial(&self, host: &str, port: u16) -> std::io::Result<Box<dyn DialedStream>> {
        let meta = gateway_proxy::tcp_metadata(host, port);
        let conn = self
            .0
            .dial_tcp(&meta)
            .await
            .map_err(std::io::Error::other)?;
        Ok(Box::new(conn))
    }
}

/// hyper wrapper around a dialed stream: `TokioIo` provides Read/Write and the outer
/// type adds the `Connection` metadata (hyper-util's blanket `Connect` requires the
/// response type to implement it).
pub struct DialedIo(Box<dyn DialedStream>);

impl hyper::rt::Read for DialedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: hyper::rt::ReadBufCursor<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut TokioIo::new(&mut self.0)).poll_read(cx, buf)
    }
}

impl hyper::rt::Write for DialedIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut TokioIo::new(&mut self.0)).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut TokioIo::new(&mut self.0)).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut TokioIo::new(&mut self.0)).poll_shutdown(cx)
    }
}

impl hyper_util::client::legacy::connect::Connection for DialedIo {
    fn connected(&self) -> hyper_util::client::legacy::connect::Connected {
        hyper_util::client::legacy::connect::Connected::new()
    }
}

type DialFuture = Pin<Box<dyn Future<Output = std::io::Result<DialedIo>> + Send>>;

/// hyper connector: turns a [`StreamDialer`] into a `tower::Service<Uri>`.
///
/// HTTPS layers rustls over the dialed plaintext stream (standard `WebPKI` validation,
/// SNI = target host); the dial and the TLS handshake are each bounded by
/// `connect_timeout`.
#[derive(Clone)]
pub struct DialerConnector {
    dialer: Arc<dyn StreamDialer>,
    connect_timeout: Duration,
}

impl Service<Uri> for DialerConnector {
    type Response = DialedIo;
    type Error = std::io::Error;
    type Future = DialFuture;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Dialing has no shared resource to warm up, so always ready
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        let is_tls = uri.scheme_str() == Some("https");
        let host = uri.host().unwrap_or_default().to_string();
        let port = uri.port_u16().unwrap_or(if is_tls { 443 } else { 80 });
        let dialer = Arc::clone(&self.dialer);
        let connect_timeout = self.connect_timeout;

        Box::pin(async move {
            let dial_fut = dialer.dial(&host, port);
            let plain = timeout(connect_timeout, dial_fut).await.map_err(|e| {
                tracing::warn!(host = %host, port = %port, error = %e, "dial timed out");
                timeout_err(format!("dial {host}:{port} timed out"))
            })??;

            let stream: Box<dyn DialedStream> = if is_tls {
                // The TLS handshake must also finish within connect_timeout: dial only
                // covers the TCP connection, and a bad node with a hung handshake would
                // otherwise consume the whole total timeout.
                let tls = timeout(connect_timeout, dial_and_wrap_tls(plain, &host))
                    .await
                    .map_err(|e| {
                        tracing::warn!(host = %host, port = %port, error = %e, "tls handshake timed out");
                        timeout_err(format!("tls handshake to {host}:{port} timed out"))
                    })?
                    .map_err(|e| io_err(format!("tls to {host}:{port}: {e}")))?;
                Box::new(tls)
            } else {
                plain
            };
            Ok(DialedIo(stream))
        })
    }
}

fn io_err(msg: impl Into<String>) -> std::io::Error {
    std::io::Error::other(msg.into())
}

/// Timeout-shaped io error: keeps the `TimedOut` kind (egress error classification
/// checks the kind).
fn timeout_err(msg: impl Into<String>) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::TimedOut, msg.into())
}

/// Perform the TLS client handshake over a plaintext stream (standard `WebPKI` roots,
/// SNI = host).
async fn dial_and_wrap_tls(
    plain: Box<dyn DialedStream>,
    host: &str,
) -> std::io::Result<tokio_rustls::client::TlsStream<Box<dyn DialedStream>>> {
    // When both aws-lc-rs (meow / older dependencies) and ring exist in the dependency
    // tree, rustls cannot pick a provider automatically and one must be installed
    // process-wide once (idempotent: ignore the Err when already installed).
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
    let server_name = rustls::pki_types::ServerName::try_from(host.to_string())
        .map_err(|e| io_err(format!("invalid server name `{host}`: {e}")))?;
    connector.connect(server_name, plain).await
}

/// Adapter egress: a hyper client holding the dial connector; [`Egress`] semantics match
/// `ReqwestEgress` (redirects disabled, three-stage timeouts, first-byte pre-read).
pub struct AdapterEgress {
    client: Client<DialerConnector, Full<Bytes>>,
    connect_timeout: Duration,
}

impl std::fmt::Debug for AdapterEgress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdapterEgress").finish_non_exhaustive()
    }
}

impl AdapterEgress {
    /// Default connect timeout (for diagnostics, aligned with
    /// [`crate::egress::ReqwestEgress::connect_timeout`]).
    #[must_use]
    pub fn connect_timeout(&self) -> Duration {
        self.connect_timeout
    }

    /// Build an egress client from a dialer. hyper-util provides the connection pool
    /// (per-host reuse).
    pub fn new(dialer: Arc<dyn StreamDialer>, connect_timeout: Duration) -> Self {
        let connector = DialerConnector {
            dialer,
            connect_timeout,
        };
        let client = Client::builder(TokioExecutor::new()).build(connector);
        Self {
            client,
            connect_timeout,
        }
    }
}

/// hyper response body → the `ForwardedResponse` byte stream (the `io::Error` boundary
/// matches the reqwest version).
fn incoming_to_stream(body: Incoming) -> ByteStream {
    Box::pin(BodyExt::into_stream(body).map(|chunk| match chunk {
        Ok(frame) => Ok(frame.into_data().unwrap_or_default()),
        Err(e) => Err(std::io::Error::other(format!("upstream stream error: {e}"))),
    }))
}

impl crate::egress::Egress for AdapterEgress {
    fn execute<'a>(
        &'a self,
        _alias: &str,
        url: &'a str,
        headers: &'a [(String, String)],
        body: Bytes,
        timeouts: &'a Timeouts,
    ) -> Pin<Box<dyn Future<Output = Result<ForwardedResponse, NormalizedError>> + Send + 'a>> {
        Box::pin(self.execute_impl(url, headers, body, timeouts))
    }
}

impl AdapterEgress {
    /// The actual forwarding logic; `Egress::execute` only wraps it in `Box::pin`.
    async fn execute_impl(
        &self,
        url: &str,
        headers: &[(String, String)],
        body: Bytes,
        timeouts: &Timeouts,
    ) -> Result<ForwardedResponse, NormalizedError> {
        let total_timeout = Duration::from_millis(timeouts.total_ms);
        let first_byte_timeout = Duration::from_millis(timeouts.first_byte_ms);

        let req = build_request(url, headers, body)?;

        let resp = timeout(total_timeout, self.client.request(req))
            .await
            .map_err(|_| upstream_error(504, true, "upstream total timeout"))?
            .map_err(|e| {
                // Connecting/timeout detection on hyper-util legacy Error: is_connect
                // has an API; timeout lives in the io ErrorKind of the error chain (we
                // construct TimedOut while dialing)
                let is_timeout = std::error::Error::source(&e)
                    .and_then(|s| s.downcast_ref::<std::io::Error>())
                    .is_some_and(|io| io.kind() == std::io::ErrorKind::TimedOut)
                    || e.to_string().to_lowercase().contains("timed out");
                upstream_error(
                    if is_timeout { 504 } else { 502 },
                    e.is_connect() || is_timeout,
                    e.to_string(),
                )
            })?;

        let status = resp.status().as_u16();
        let retry_after_ms = parse_retry_after_ms(
            resp.headers()
                .get(hyper::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok()),
            chrono::Utc::now().timestamp_millis(),
        );
        let content_type = resp
            .headers()
            .get(hyper::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();

        // The adapter version carries the upstream Retry-After into the response as-is
        // (the reqwest version always passes None on the success branch).
        finish_forwarded_response(
            status,
            content_type,
            retry_after_ms,
            incoming_to_stream(resp.into_body()),
            first_byte_timeout,
            retry_after_ms,
        )
        .await
    }
}

/// Assemble the hyper POST request: URI parsing + header mapping + Content-Length +
/// body.
fn build_request(
    url: &str,
    headers: &[(String, String)],
    body: Bytes,
) -> Result<Request<Full<Bytes>>, NormalizedError> {
    let uri: Uri = url
        .parse()
        .map_err(|e| upstream_error(502, false, format!("invalid upstream url `{url}`: {e}")))?;
    let header_map = build_header_map(headers)?;

    let mut builder = Request::builder()
        .method(hyper::Method::POST)
        .uri(uri)
        .header(hyper::header::CONTENT_LENGTH, body.len());
    // `headers_mut` is always Some before the builder is built; handle it explicitly to
    // avoid a panic (the unreachable branch returns 502 like other construction errors).
    let Some(hm) = builder.headers_mut() else {
        return Err(upstream_error(
            502,
            false,
            "build request: headers unavailable",
        ));
    };
    header_map.iter().for_each(|(k, v)| {
        hm.append(k, v.clone());
    });
    builder
        .body(Full::new(body))
        .map_err(|e| upstream_error(502, false, format!("build request: {e}")))
}
