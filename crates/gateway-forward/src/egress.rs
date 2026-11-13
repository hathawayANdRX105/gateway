//! Egress client pool — connection reuse / proxy / timeouts.
//!
//! References: new-api internal/egress (client pool / HTTP2 sharding / proxy),
//! wildtoken (per-upstream timeouts). V1 scope: direct + HTTP(S) proxy;
//! SOCKS/sing-box/TLS camouflage deferred (account-pool scenarios).

use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_util::{Stream, StreamExt, TryStreamExt};
use gateway_core::error::NormalizedError;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::redirect::Policy as RedirectPolicy;
use tokio::time::timeout;

/// Timeout configuration (ms) — owned by the injection surface, re-exported here
/// so existing `gateway_forward::egress::Timeouts` paths keep resolving.
pub use gateway_core::egress::Timeouts;

/// Forwarded response byte-stream shape (SSE and plain bodies share it).
pub(crate) type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>>;

/// Unified `NormalizedError` constructor on the egress side: the code is always
/// `UPSTREAM_ERROR` and channel-independent.
///
/// The egress layer only produces upstream errors (URL/header construction,
/// timeouts, transport failures); the fixed fields are centralized here so each
/// error site passes only status / retryable / message.
pub(crate) fn upstream_error(
    status: u16,
    retryable: bool,
    message: impl Into<String>,
) -> NormalizedError {
    NormalizedError {
        code: gateway_core::error::code::UPSTREAM_ERROR,
        status,
        retryable,
        channel_scoped: false,
        message: message.into(),
        retry_after_ms: None,
    }
}

/// Upstream error-body preview: truncated to 2048 bytes and decoded lossily as UTF-8.
pub(crate) fn body_preview(bytes: &[u8]) -> String {
    String::from_utf8_lossy(&bytes[..bytes.len().min(2048)]).into_owned()
}

/// Egress executor — holder of a pooled reqwest Client.
///
/// V1 scope: a single direct pool, redirect following disabled (upstream 3xx is
/// treated as an error, never followed silently). SOCKS/HTTP proxying is deferred
/// to a later phase; issue #14 shards the pool by the requested model alias
/// (public name): many upstream channels serve the same public alias and requests
/// aggregate per alias, so a per-alias pool isolates a hot alias from other
/// aliases while reqwest still pools per host inside each client.
///
/// Returning `Pin<Box<dyn Future>>` instead of `impl Future` keeps the trait
/// dyn-compatible (bin/api injects it as `Arc<dyn Egress>`).
pub trait Egress: Send + Sync {
    /// Send a prepared request and return the raw response (including the body
    /// stream).
    ///
    /// `alias` is the requested model alias (public name): it selects the
    /// connection-pool shard — the same alias reuses one pooled client while
    /// different aliases get isolated pools ([`ReqwestEgress`] shards by alias;
    /// other implementations may ignore it).
    ///
    /// Three-part timeout semantics:
    /// - `connect_ms`: the reqwest Client's built-in connect timeout (TCP/TLS
    ///   handshake)
    /// - `first_byte_ms`: time allowed for the first chunk once response headers
    ///   have arrived
    /// - `total_ms`: the whole request lifecycle (including reading the body)
    ///
    /// The returned `ForwardedResponse` carries header information only; the caller
    /// takes over reading the byte stream through
    /// `ForwardedResponse::into_body_stream` (the first bytes already grabbed are
    /// prepended).
    fn execute<'a>(
        &'a self,
        alias: &str,
        url: &'a str,
        headers: &'a [(String, String)],
        body: Bytes,
        timeouts: &'a Timeouts,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<ForwardedResponse, gateway_core::error::NormalizedError>>
                + Send
                + 'a,
        >,
    >;
}

/// Forwarded response — `reqwest::Response` headers + byte stream.
///
/// The first bytes grabbed by the first-byte check are already prepended (if any),
/// so the byte order the caller sees from the body stream matches the upstream byte
/// for byte. 429 responses carry a `Retry-After` hint (parsed on the egress side)
/// for the downstream 429 cooldown ladder to consume.
pub struct ForwardedResponse {
    status: u16,
    content_type: String,
    /// `Retry-After` hint from an upstream 429 (ms); None when not a 429, header
    /// absent, or unparsable.
    retry_after_ms: Option<u64>,
    body: ByteStream,
}

impl ForwardedResponse {
    /// Test/mock constructor — assembles directly from a byte stream (bypassing
    /// reqwest). `retry_after_ms` is the optional 429 hint (mocks usually pass
    /// `None`). The production path builds this in `ReqwestEgress::execute`.
    pub fn from_stream(
        status: u16,
        content_type: impl Into<String>,
        retry_after_ms: Option<u64>,
        body: impl futures_util::Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static,
    ) -> Self {
        Self {
            status,
            content_type: content_type.into(),
            retry_after_ms,
            body: Box::pin(body),
        }
    }

    /// Upstream HTTP status code.
    #[must_use]
    pub fn status(&self) -> u16 {
        self.status
    }

    /// Upstream Content-Type (the stream module uses it to decide whether to attach
    /// an SSE scanner).
    #[must_use]
    pub fn content_type(&self) -> &str {
        &self.content_type
    }

    /// `Retry-After` hint from an upstream 429 (ms); None when not a 429, header
    /// absent, or unparsable.
    #[must_use]
    pub fn retry_after_ms(&self) -> Option<u64> {
        self.retry_after_ms
    }

    /// Consume the body stream (ownership moves; `status/content_type` are no longer
    /// accessible afterwards).
    pub fn into_body_stream(
        self,
    ) -> Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>> {
        self.body
    }
}

impl std::fmt::Debug for ForwardedResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ForwardedResponse")
            .field("status", &self.status)
            .field("content_type", &self.content_type)
            .field("retry_after_ms", &self.retry_after_ms)
            .finish_non_exhaustive()
    }
}

/// Maximum number of alias-shared clients held at once. A `Client` is dropped
/// when its alias shard is evicted (FIFO: least recently inserted alias first),
/// which closes that client's idle connections; reqwest still pools per host
/// inside each surviving client.
pub const MAX_ALIAS_SHARDS: usize = 64;

/// One shard of the egress connection pool: the aliases mapped to their clients,
/// plus insertion order for FIFO eviction.
struct AliasShards {
    clients: HashMap<String, Arc<reqwest::Client>>,
    order: VecDeque<String>,
}

/// Inner state shared by [`ReqwestEgress`] clones: one shared client when
/// sharding is off, otherwise a bounded alias-keyed map of clients.
struct EgressInner {
    /// When `true` (the default), `client_for` returns a distinct pooled client
    /// per requested model alias; when `false`, one shared client serves every
    /// alias (the old behavior).
    shard_by_alias: bool,
    /// Single pooled client used when sharding is off.
    shared: Arc<reqwest::Client>,
    /// Alias-sharded clients, used when `shard_by_alias` is on. A plain mutex is
    /// enough: the hit path is a `HashMap` lookup plus an `Arc` clone, and a
    /// client is only built on the rare miss (cold start or a new alias).
    shards: Mutex<AliasShards>,
}

/// reqwest egress — alias-sharded pooled clients.
///
/// Direct connection; no redirect following; the connection pool buckets by host
/// by default (built into reqwest). Issue #14: when sharding is on, clients are
/// keyed by the requested model alias (public name) — the same public alias is
/// served by many upstream channels and requests aggregate per alias, so a
/// per-alias pool isolates a hot alias from other aliases while reqwest still
/// pools per host inside each client. With sharding off, one globally reused
/// Client serves every alias (the old behavior).
#[derive(Clone)]
pub struct ReqwestEgress {
    inner: Arc<EgressInner>,
    /// Default connect timeout (per-request timeouts could override it — the current
    /// implementation uses this default).
    connect_timeout: Duration,
}

impl std::fmt::Debug for ReqwestEgress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReqwestEgress").finish_non_exhaustive()
    }
}

impl Default for ReqwestEgress {
    fn default() -> Self {
        Self::new(true)
    }
}

impl ReqwestEgress {
    /// Direct client — redirects disabled, built-in connection pool, no proxy.
    /// Connect timeout defaults to 5s.
    ///
    /// `shard_by_alias` selects issue #14 alias sharding: `true` (the default;
    /// the `pool_shard_by_alias` config knob) keys a distinct pooled client per
    /// requested model alias; `false` serves every alias from one shared client
    /// (the old behavior).
    ///
    /// # Panics
    ///
    /// Panics if reqwest cannot build the client (unrecoverable environment
    /// error). `new` is a constructor contract (`Self`, also used by `Default`),
    /// so panic at startup is the only fail-fast option.
    #[allow(unknown_lints)]
    #[allow(expect_in_non_result)]
    #[must_use]
    pub fn new(shard_by_alias: bool) -> Self {
        let connect_timeout = Duration::from_millis(5_000);
        Self {
            inner: Arc::new(EgressInner {
                shard_by_alias,
                shared: Arc::new(Self::build_client(connect_timeout)),
                shards: Mutex::new(AliasShards {
                    clients: HashMap::new(),
                    order: VecDeque::new(),
                }),
            }),
            connect_timeout,
        }
    }

    /// Build one production reqwest client: every shard gets identical
    /// parameters (redirects disabled, connect timeout fixed at builder time,
    /// `pool_max_idle_per_host(8)` per-host pool).
    #[allow(unknown_lints)]
    #[allow(expect_in_non_result)]
    fn build_client(connect_timeout: Duration) -> reqwest::Client {
        reqwest::Client::builder()
            .redirect(RedirectPolicy::none())
            .connect_timeout(connect_timeout)
            .pool_max_idle_per_host(8)
            .build()
            .expect("reqwest client build")
    }

    /// Custom client — for tests (injecting a mock transport) or future proxy pooling.
    /// The injected client serves every alias: sharding stays off so a mock is never
    /// bypassed by a lazily built shard.
    #[must_use]
    pub fn with_client(client: reqwest::Client, connect_timeout: Duration) -> Self {
        Self {
            inner: Arc::new(EgressInner {
                shard_by_alias: false,
                shared: Arc::new(client),
                shards: Mutex::new(AliasShards {
                    clients: HashMap::new(),
                    order: VecDeque::new(),
                }),
            }),
            connect_timeout,
        }
    }

    /// Default connect timeout (the per-call `timeouts.connect_ms` is not currently
    /// accepted by `ClientBuilder`, so it is fixed at builder time). Exposed for
    /// diagnostics.
    #[must_use]
    pub fn connect_timeout(&self) -> Duration {
        self.connect_timeout
    }

    /// Resolve the pooled client for `alias`: the same alias always returns the
    /// same `Arc` (pool reuse across the channels serving that alias); different
    /// aliases get isolated clients. With sharding disabled, every alias returns
    /// the single shared client.
    ///
    /// The map is bounded by [`MAX_ALIAS_SHARDS`]: a miss past the bound evicts
    /// the first-inserted alias (FIFO) and drops its `Client`, closing that
    /// client's idle connections.
    #[must_use]
    pub fn client_for(&self, alias: &str) -> Arc<reqwest::Client> {
        if !self.inner.shard_by_alias {
            return Arc::clone(&self.inner.shared);
        }
        let mut shards = self.inner.shards.lock();
        if let Some(client) = shards.clients.get(alias) {
            return Arc::clone(client);
        }
        let key = alias.to_string();
        let client = Arc::new(Self::build_client(self.connect_timeout));
        if shards.clients.len() >= MAX_ALIAS_SHARDS
            && let Some(oldest) = shards.order.pop_front()
        {
            shards.clients.remove(&oldest); // dropped ⇒ idle connections close
        }
        shards.clients.insert(key.clone(), Arc::clone(&client));
        shards.order.push_back(key);
        client
    }

    /// Number of alias shards currently held (bounded by [`MAX_ALIAS_SHARDS`]);
    /// `1` when sharding is off.
    #[must_use]
    pub fn shard_count(&self) -> usize {
        self.inner.shards.lock().clients.len()
    }
}

/// Non-2xx → `NormalizedError`.
///
/// Classified by status code (aligned with `gateway_dispatch::health::FailureClass)`:
/// - 401/403 → `invalid_api_key`, not retryable, but channel-scoped → demote and
///   switch candidates (bad credentials ≠ a bad provider)
/// - 404 → `upstream_error`, not retryable, channel-scoped (this channel lacks the
///   model; another channel may work)
/// - 429 → `rate_limited`, retryable
/// - 408 → `upstream_error`, retryable (an upstream gateway timeout is transient;
///   switching channels or retrying may both work)
/// - 5xx → `upstream_error`, retryable
/// - other 4xx → `upstream_error` (fatal), neither retried nor demoted (the request
///   itself is bad; switching channels cannot save it)
///
/// Deliberate divergence from core's `STATUS_RULES`: generic 4xx (including 404)
/// reports `UPSTREAM_ERROR` rather than `INVALID_API_KEY`, because these are fatal
/// request errors, not credential problems. `retryable` and `channel_scoped`
/// delegate to the core table (via `failure_scope`), so the semantic flags cannot
/// drift — forward's `classify_status.rs` parity test pins this contract.
#[must_use]
pub fn classify_status(
    status: u16,
    body_preview: &str,
    retry_after_ms: Option<u64>,
) -> gateway_core::error::NormalizedError {
    use gateway_core::error::code;
    let (code, http_status, retryable) = match status {
        401 | 403 => (code::INVALID_API_KEY, status, false),
        429 => (code::RATE_LIMITED, status, true),
        // 408 sits inside 400..=499, so it must precede the catch-all arm to match.
        500..=599 | 408 => (code::UPSTREAM_ERROR, status, true),
        400..=499 => (code::UPSTREAM_ERROR, status, false),
        _ => (code::UPSTREAM_ERROR, 502, false),
    };
    // Channel-scoped 4xx reuses gateway_dispatch::failure_scope (P1-B demotion)
    // instead of duplicating a table here; retryable failures (429/5xx) already switch
    // candidates, so this flag only matters on the non-retry branch.
    let channel_scoped = !retryable
        && matches!(status, 400..=499)
        && gateway_dispatch::failure_scope::classify_channel_scope(status)
            == gateway_dispatch::failure_scope::FailureScope::Channel;
    gateway_core::error::NormalizedError {
        code,
        status: http_status,
        retryable,
        channel_scoped,
        message: body_preview.chars().take(200).collect(),
        // Retry-After only matters on 429 (the override hint for dispatch's separate
        // 429 ladder); other status codes are treated as having no hint even if the
        // header is present.
        retry_after_ms: if status == 429 { retry_after_ms } else { None },
    }
}

/// Parse the upstream `Retry-After` response header into a wait duration (ms) — a
/// pure function called by the egress side when a 429 response arrives.
///
/// Supports both legal forms from RFC 9110 §12.5.3:
/// - **delta-seconds** (`"120"`): non-negative integer seconds × 1000;
/// - **HTTP-date** (`"Sun, 06 Nov 1994 08:49:37 GMT"`): parsed as an absolute
///   instant, then subtracted from `now_ms`; a past instant / unparsable value →
///   `None`.
///
/// Parameters:
/// - `raw`: the raw header value (`None` = header absent);
/// - `now_ms`: the parsing instant (unix ms, needed only by the HTTP-date branch;
///   passing 0 does not affect the delta-seconds branch).
///
/// `Some(ms)` is the waitable duration; `None` means absent / negative /
/// unparsable — callers treat it as "no hint" (the local rung of the separate 429
/// ladder).
#[must_use]
pub fn parse_retry_after_ms(raw: Option<&str>, now_ms: i64) -> Option<u64> {
    let raw = raw?.trim();
    if raw.is_empty() {
        return None;
    }
    // delta-seconds: non-negative integer (a minus sign fails to parse → falls into
    // the date branch, fails there too → None).
    if let Ok(secs) = raw.parse::<u64>() {
        return Some(secs.saturating_mul(1_000));
    }
    // HTTP-date (IMF-fixdate): its shape matches the RFC 2822 weekday form, and
    // `parse_from_rfc2822` accepts GMT/UT→+0000, but its named-timezone table does
    // **not contain "UTC"** (unknown three-letter zone names are rejected), so a
    // trailing UTC is rewritten to the equivalent GMT before parsing.
    let date_str = if raw.len() >= 3 && raw.as_bytes()[raw.len() - 3..].eq_ignore_ascii_case(b"UTC")
    {
        // The suffix is ASCII ⇒ len-3 always lands on a character boundary.
        format!("{}GMT", &raw[..raw.len() - 3])
    } else {
        raw.to_string()
    };
    let Ok(dt) = chrono::DateTime::parse_from_rfc2822(&date_str) else {
        return None;
    };
    // Negative value (an instant in the past) → None.
    u64::try_from(dt.timestamp_millis() - now_ms).ok()
}

pub(crate) fn build_header_map(
    headers: &[(String, String)],
) -> Result<HeaderMap, gateway_core::error::NormalizedError> {
    let mut map = HeaderMap::new();
    for (k, v) in headers {
        let name = HeaderName::try_from(k.as_str())
            .map_err(|e| upstream_error(502, false, format!("invalid header name `{k}`: {e}")))?;
        let value = HeaderValue::try_from(v.as_str())
            .map_err(|e| upstream_error(502, false, format!("invalid header value `{v}`: {e}")))?;
        map.append(name, value);
    }
    Ok(map)
}

/// Prepend the "first bytes already read" to the byte stream — keeps the first-byte
/// check from losing bytes.
fn prepend_first(first: Bytes, rest: ByteStream) -> ByteStream {
    let head = futures_util::stream::once(async move { Ok(first) });
    Box::pin(head.chain(rest))
}

/// First-byte pre-read for 2xx responses: the first chunk must arrive within
/// `first_byte_timeout`.
///
/// The grabbed first chunk is prepended back onto the response stream (no bytes
/// lost); an empty body passes an empty stream through; a read error is raised as a
/// retryable 502 (both Egress implementations share these semantics).
pub(crate) async fn first_byte_body(
    mut body: ByteStream,
    first_byte_timeout: Duration,
) -> Result<ByteStream, NormalizedError> {
    let first = timeout(first_byte_timeout, body.next())
        .await
        .map_err(|e| {
            tracing::warn!(error = %e, "upstream first-byte timeout");
            upstream_error(504, true, "upstream first-byte timeout")
        })?;
    match first {
        None => Ok(Box::pin(futures_util::stream::empty())),
        Some(Ok(first)) => Ok(prepend_first(first, body)),
        Some(Err(e)) => Err(upstream_error(
            502,
            true,
            format!("upstream stream error: {e}"),
        )),
    }
}

/// Non-2xx error body → bounded preview: read the whole stream, then truncate (a
/// read error yields an empty string).
///
/// The two Egress implementations obtain error bodies differently (reqwest's
/// `bytes()` / hyper's `collect`); both are mapped to [`ByteStream`] and normalized
/// here.
async fn read_error_body(mut body: ByteStream) -> String {
    let mut buf = Vec::new();
    while let Some(chunk) = body.next().await {
        match chunk {
            Ok(chunk) => buf.extend_from_slice(&chunk),
            // On a read error the whole preview degrades to empty (matching the
            // original all-or-nothing semantics).
            Err(_) => return String::new(),
        }
    }
    body_preview(&buf)
}

/// Response finishing: non-2xx reads the error body and classifies it; 2xx does the
/// first-byte pre-read and assembles the response.
///
/// `retry_after_ms` is used to classify non-2xx 429s; `success_retry_after_ms` is
/// carried as-is into successful responses (the reqwest version always passes
/// `None`, the adapter version passes the parsed value through — the existing
/// semantic difference stays at each call site).
pub(crate) async fn finish_forwarded_response(
    status: u16,
    content_type: String,
    retry_after_ms: Option<u64>,
    body: ByteStream,
    first_byte_timeout: Duration,
    success_retry_after_ms: Option<u64>,
) -> Result<ForwardedResponse, NormalizedError> {
    if !(200..=299).contains(&status) {
        let preview = read_error_body(body).await;
        return Err(classify_status(status, &preview, retry_after_ms));
    }
    let body = first_byte_body(body, first_byte_timeout).await?;
    Ok(ForwardedResponse::from_stream(
        status,
        content_type,
        success_retry_after_ms,
        body,
    ))
}

/// Send the POST until response headers are complete (the total timeout bounds the
/// whole send).
///
/// The connect timeout is fixed at `ClientBuilder` time in `ReqwestEgress::new`;
/// reqwest 0.12 has no per-request connect timeout, so `timeouts.connect_ms` is not
/// read here (that field is consumed by assembly and diagnostics). Transport
/// failures are classified retryable by connect/timeout/request, and timeouts map to
/// 504.
async fn send_request(
    client: Arc<reqwest::Client>,
    url: String,
    header_map: HeaderMap,
    body: Bytes,
    total_timeout: Duration,
) -> Result<reqwest::Response, NormalizedError> {
    let send = async move {
        client
            .request(reqwest::Method::POST, &url)
            .headers(header_map)
            .body(body)
            .send()
            .await
            .map_err(|e| {
                let retryable = e.is_connect() || e.is_timeout() || e.is_request();
                upstream_error(
                    if e.is_timeout() { 504 } else { 502 },
                    retryable,
                    e.to_string(),
                )
            })
    };
    timeout(total_timeout, send).await.map_err(|e| {
        tracing::warn!(error = %e, "upstream total timeout");
        upstream_error(504, true, "upstream total timeout")
    })?
}

impl Egress for ReqwestEgress {
    fn execute<'a>(
        &'a self,
        alias: &str,
        url: &'a str,
        headers: &'a [(String, String)],
        body: Bytes,
        timeouts: &'a Timeouts,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<ForwardedResponse, gateway_core::error::NormalizedError>>
                + Send
                + 'a,
        >,
    > {
        let first_byte_timeout = Duration::from_millis(timeouts.first_byte_ms);
        let total_timeout = Duration::from_millis(timeouts.total_ms);

        let header_map = match build_header_map(headers) {
            Ok(m) => m,
            Err(e) => return Box::pin(async move { Err(e) }),
        };

        let client = Arc::clone(&self.client_for(alias));
        let url = url.to_string();

        Box::pin(async move {
            let resp = send_request(client, url, header_map, body, total_timeout).await?;

            let status = resp.status().as_u16();
            let retry_after_ms = parse_retry_after_ms(
                resp.headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|v| v.to_str().ok()),
                chrono::Utc::now().timestamp_millis(),
            );
            let content_type = resp
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string();

            // reqwest::bytes_stream yields `Result<Bytes, reqwest::Error>`; it is
            // mapped to `std::io::Error` at the boundary for uniform handling
            // upstream.
            let body: ByteStream = Box::pin(
                resp.bytes_stream()
                    .map_err(|e| std::io::Error::other(format!("upstream stream error: {e}"))),
            );
            // 2xx branch: Retry-After semantics belong to 429 only, so successful
            // responses never carry the hint.
            finish_forwarded_response(
                status,
                content_type,
                retry_after_ms,
                body,
                first_byte_timeout,
                None,
            )
            .await
        })
    }
}
