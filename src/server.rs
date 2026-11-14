//! axum transport over the assembled gateway pipeline.
//!
//! One handler owns the whole request path: read the body under a hard limit, build the
//! transport-neutral [`RequestCtx`], run the pipeline, then hand the resulting response
//! back unchanged (the body streams frame by frame). The transport is the same stack the
//! library's [`gateway_facade::Gateway::router`] exit uses, so no `http`-version or body
//! conversion sits in the request path; only the process-level concerns live here
//! (body ceiling, trusted-proxy policy, `/healthz` + `/readyz`, hop-by-hop stripping,
//! listener and drain budget).

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, ensure};
use axum::body::Body;
use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, Request, StatusCode};
use axum::response::{IntoResponse, Json, Response};
use axum::routing::get;
use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;
use serde_json::{Value, json};
use tokio::sync::watch;

use gateway_facade::calibration::{self, CalibrationLog};
use gateway_facade::{
    Components, Config, Context, GatewayBuilder, Ledger, MemoryLedger, Pipeline, ProxyManager,
    QuotaGate, RequestCtx, ReqwestEgress, Settle, SettleSink, State as FacadeState, UsageEvent,
    error_to_response,
};

use crate::catalog::{Catalog, CatalogCounts};
use crate::config::DaemonConfig;

/// Shared request state. One instance per process, cloned as an `Arc` per request.
///
/// Public (with a constructor) so integration tests can drive the transport handlers
/// without booting a real gateway; the fields stay private.
pub struct AppState {
    pipeline: Arc<Pipeline>,
    max_request_bytes: usize,
    trusted_proxies: Vec<IpAddr>,
    calibration: Arc<CalibrationLog>,
    counts: CatalogCounts,
    /// Whether a settlement sink is mounted. The standalone shape always mounts
    /// the logging sink (assemble → Settle); embedded consumers derive this from
    /// their own facade Config. Drives the always-present /readyz `billing`
    /// field so an unbilled relay is visible to ops.
    billing_enabled: bool,
}

impl AppState {
    #[must_use]
    pub fn new(
        pipeline: Arc<Pipeline>,
        max_request_bytes: usize,
        trusted_proxies: Vec<IpAddr>,
        calibration: Arc<CalibrationLog>,
        counts: CatalogCounts,
        billing_enabled: bool,
    ) -> Self {
        Self {
            pipeline,
            max_request_bytes,
            trusted_proxies,
            calibration,
            counts,
            billing_enabled,
        }
    }
}

/// Read the catalog, assemble the plane, then serve until `SIGINT` / `SIGTERM`.
///
/// `check` loads the config and catalog, logs the summary and returns without binding.
///
/// # Errors
///
/// Returns catalog loading errors and listener bind errors.
pub async fn run(
    config: DaemonConfig,
    catalog_path: std::path::PathBuf,
    check: bool,
) -> anyhow::Result<()> {
    let catalog = Catalog::load(&catalog_path)?;
    let counts = catalog.counts;
    let calibration = Arc::new(CalibrationLog::new());
    calibration::install(Arc::clone(&calibration));

    tracing::info!(
        channels = counts.channels,
        route_units = counts.route_units,
        tokens = counts.tokens,
        users = counts.users,
        groups = counts.groups,
        prices = counts.prices,
        "catalog loaded"
    );
    if check {
        return Ok(());
    }

    let gateway = assemble(
        catalog,
        config.dispatch.selector,
        config.pool_shard_by_alias,
    );
    // /readyz exposes billing so ops can tell an unbilled relay from one that
    // settles usage. The standalone shape always mounts the logging settle sink
    // (assemble → Settle), so this is fixed at true here; an embedded consumer
    // derives it from its own facade Config (settle.is_some()).
    let state = Arc::new(AppState::new(
        gateway.pipeline(),
        config.max_request_bytes,
        config.trusted_proxies.clone(),
        calibration,
        counts,
        true,
    ));

    let tcp = tokio::net::TcpListener::bind(&config.listen)
        .await
        .with_context(|| format!("bind {}", config.listen))?;
    let uds = match &config.uds_path {
        Some(path) => {
            remove_stale_socket(path)?;
            Some(
                tokio::net::UnixListener::bind(path)
                    .with_context(|| format!("bind uds {}", path.display()))?,
            )
        }
        None => None,
    };

    tracing::info!(
        listen = %config.listen,
        uds = ?config.uds_path,
        workers = config.workers,
        "gatewayd serving"
    );

    let (stop_tx, stop_rx) = watch::channel(false);
    tokio::spawn(async move {
        wait_for_shutdown_signal().await;
        let _ = stop_tx.send(true);
    });

    let tcp_serve = axum::serve(
        tcp,
        router(Arc::clone(&state)).into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(until_stopped(stop_rx.clone()));
    let uds_serve = uds.map(|listener| {
        axum::serve(listener, router(state).into_make_service())
            .with_graceful_shutdown(until_stopped(stop_rx.clone()))
    });

    // Drain budget: once the stop signal fires, in-flight requests (SSE streams
    // included) get `shutdown_timeout_secs` before the process exits. Whichever
    // future ends first wins; the deadline arm only starts counting after the
    // signal, so a long-running server is never cut short.
    let drain = drain_deadline(stop_rx, Duration::from_secs(config.shutdown_timeout_secs));
    match uds_serve {
        Some(uds_serve) => {
            tokio::select! {
                result = async { tokio::try_join!(tcp_serve, uds_serve) } => {
                    result.context("gatewayd server exited with an error")?;
                }
                () = drain => tracing::warn!("drain budget exhausted; exiting with requests in flight"),
            }
        }
        None => {
            tokio::select! {
                result = tcp_serve => result.context("gatewayd server exited with an error")?,
                () = drain => tracing::warn!("drain budget exhausted; exiting with requests in flight"),
            }
        }
    }
    Ok(())
}

/// The process router: probe endpoints plus the one data-plane fallback.
pub fn router(state: Arc<AppState>) -> axum::Router {
    axum::Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .fallback(dispatch)
        .with_state(state)
}

/// Build the gateway once: one ledger seeded from the catalog, one quota gate, one
/// settlement pair. Nothing here is per-request.
fn assemble(
    catalog: Catalog,
    selector: gateway_facade::SelectorKind,
    pool_shard_by_alias: bool,
) -> gateway_facade::Gateway {
    let ledger = Arc::new(MemoryLedger::new());
    ledger.reseed(&catalog.quota_snapshot);

    let quotas = Arc::new(arc_swap::ArcSwap::from_pointee(catalog.quota_snapshot));
    let pricing = Arc::new(arc_swap::ArcSwap::from_pointee(catalog.pricing_snapshot));

    GatewayBuilder::relay(Context {
        config: Config {
            selector,
            ..Config::default()
        },
        components: Components {
            egress: Arc::new(ReqwestEgress::new(pool_shard_by_alias)),
            // The standalone shape never leases from the proxy pool: no node rows exist
            // until a control plane installs them, and the direct egress client serves
            // every channel.
            wire_proxy_pool: false,
            proxies: Arc::new(ProxyManager::new()),
            proxy_snapshot: gateway_facade::ProxySnapshot::default(),
            state: FacadeState {
                dispatch: catalog.dispatch,
                token_snapshot: Arc::new(arc_swap::ArcSwap::from_pointee(catalog.token_snapshot)),
                user_snapshot: Arc::new(arc_swap::ArcSwap::from_pointee(catalog.user_snapshot)),
                group_snapshot: Arc::new(arc_swap::ArcSwap::from_pointee(catalog.group_snapshot)),
            },
            quota_gate: Some(QuotaGate::new(quotas, pricing, ledger.clone())),
            settle: Some(Settle {
                price_table: Arc::new(catalog.price_table),
                sink: Arc::new(LogSettleSink),
            }),
            ledger: Some(ledger),
        },
    })
    .build()
}

/// Settlement delivery for the standalone shape: the event is logged, not persisted.
///
/// The library contract requires `submit` to return fast; a log line satisfies that. A
/// product that needs durable usage rows installs its own sink instead.
struct LogSettleSink;

impl SettleSink for LogSettleSink {
    fn submit(&self, event: UsageEvent) {
        tracing::info!(
            token = %event.token_key,
            model = %event.public_model,
            prompt_tokens = event.prompt_tokens,
            completion_tokens = event.completion_tokens,
            cost = event.cost,
            status = event.status_code,
            "settled"
        );
    }
}

fn remove_stale_socket(path: &std::path::Path) -> anyhow::Result<()> {
    if let Err(err) = std::fs::remove_file(path) {
        ensure!(
            err.kind() == std::io::ErrorKind::NotFound,
            "remove stale socket {}: {err}",
            path.display()
        );
    }
    Ok(())
}

pub async fn healthz() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

pub async fn readyz(State(state): State<Arc<AppState>>) -> Json<Value> {
    // Billing is always reported: an unbilled relay is invisible to ops
    // otherwise, unlike `calibration`, which only appears once samples have
    // been recorded. Additive for existing /readyz consumers.
    let billing = if state.billing_enabled {
        "enabled"
    } else {
        "disabled"
    };
    let mut body = json!({
        "status": "ready",
        "billing": billing,
        "channels": state.counts.channels,
        "routeUnits": state.counts.route_units,
        "groups": state.counts.groups,
        "users": state.counts.users,
        "tokens": state.counts.tokens,
        "prices": state.counts.prices,
    });
    // Calibration statistics from real traffic: only exposed once samples have
    // been recorded, keeping the endpoint shape backwards compatible otherwise.
    let report = state.calibration.report();
    if report.n > 0
        && let Ok(value) = serde_json::to_value(report)
    {
        body["calibration"] = value;
    }
    Json(body)
}

/// The one request entry: path guard → body read → pipeline → response.
async fn dispatch(State(state): State<Arc<AppState>>, req: Request<Body>) -> Response {
    let path = req.uri().path().to_string();
    if !is_data_plane_path(&path) {
        return StatusCode::NOT_FOUND.into_response();
    }

    // The connect-info layer only exists on the TCP listener; a Unix-socket peer has no
    // `SocketAddr`, so the extension is simply absent there.
    let peer = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ConnectInfo(addr)| addr.ip());

    let (parts, body) = req.into_parts();
    let body = match read_body(body, state.max_request_bytes).await {
        Ok(body) => body,
        Err(BodyError::TooLarge) => {
            return (
                StatusCode::PAYLOAD_TOO_LARGE,
                Json(json!({
                    "error": { "message": "request body too large", "type": "invalid_request_error" }
                })),
            )
                .into_response();
        }
        Err(BodyError::Payload) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": { "message": "request body read failed", "type": "invalid_request_error" }
                })),
            )
                .into_response();
        }
    };

    let client_ip = client_addr(peer, &parts.headers, &state.trusted_proxies);
    let ctx = RequestCtx::from_parts(&parts.method, path, parts.headers, body, client_ip);

    let response = match state.pipeline.run(ctx).await {
        Ok(response) => response,
        Err(err) => error_to_response(err),
    };
    strip_hop_by_hop(response)
}

/// Body ceiling check per chunk: the process never buffers more than the configured limit.
/// Read the request body under `limit`.
///
/// # Errors
///
/// [`BodyError::TooLarge`] when the accumulated bytes would exceed `limit`, and
/// [`BodyError::Payload`] when the transport stream fails mid-body.
pub async fn read_body(body: Body, limit: usize) -> Result<Bytes, BodyError> {
    let mut out = BytesMut::with_capacity(8 * 1024);
    let mut stream = body.into_data_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| BodyError::Payload)?;
        if out.len() + chunk.len() > limit {
            return Err(BodyError::TooLarge);
        }
        out.extend_from_slice(&chunk);
    }
    Ok(out.freeze())
}

#[derive(Debug, PartialEq, Eq)]
pub enum BodyError {
    TooLarge,
    Payload,
}

/// Data-plane path scope: `/v1`, `/v1/…`, `/v1beta…` — the same set the library's
/// `data_plane_guard` accepts minus `/healthz` (`/readyz` is a registered route and
/// never reaches this handler).
#[must_use]
pub fn is_data_plane_path(path: &str) -> bool {
    path == "/v1" || path.starts_with("/v1/") || path.starts_with("/v1beta")
}

/// Client address: the socket peer unless it is an explicitly trusted proxy, in which case
/// the forwarded chain is walked from the right and the first address that is not itself a
/// trusted proxy wins.
///
/// Right-to-left is the only safe direction: every proxy appends to the chain, so the
/// left-most entry is whatever the client sent and is trivially forgeable. The default
/// (`trusted_proxies` empty) never reads the header at all.
#[must_use]
pub fn client_addr(peer: Option<IpAddr>, headers: &HeaderMap, trusted: &[IpAddr]) -> IpAddr {
    match peer {
        Some(peer) if trusted.contains(&peer) => forwarded_for(headers, trusted).unwrap_or(peer),
        Some(peer) => peer,
        None => Ipv4Addr::UNSPECIFIED.into(),
    }
}

#[must_use]
pub fn forwarded_for(headers: &HeaderMap, trusted: &[IpAddr]) -> Option<IpAddr> {
    headers
        .get("x-forwarded-for")?
        .to_str()
        .ok()?
        .split(',')
        .rev()
        .filter_map(|part| part.trim().parse::<IpAddr>().ok())
        .find(|ip| !trusted.contains(ip))
}

/// Drop the upstream's framing headers before the response leaves the process: the
/// transport frames the body itself (`chunked`), so forwarding `content-length` /
/// `transfer-encoding` would describe bytes that no longer match.
#[must_use]
pub fn strip_hop_by_hop(mut response: Response) -> Response {
    let blocked: Vec<axum::http::HeaderName> = response
        .headers()
        .keys()
        .filter(|name| is_hop_by_hop(name.as_str()))
        .cloned()
        .collect();
    for name in blocked {
        response.headers_mut().remove(&name);
    }
    response
}

/// Headers the transport must reframe itself.
const HOP_BY_HOP: [&str; 9] = [
    "content-length",
    "transfer-encoding",
    "connection",
    "keep-alive",
    "upgrade",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
];

#[must_use]
pub fn is_hop_by_hop(name: &str) -> bool {
    // Header names are case-insensitive: a producer writing `Content-Length` must be
    // dropped just the same, and the comparison must not allocate.
    HOP_BY_HOP
        .iter()
        .any(|blocked| name.eq_ignore_ascii_case(blocked))
}

/// Resolves once the shared stop flag is set.
async fn until_stopped(mut rx: watch::Receiver<bool>) {
    let _ = rx.wait_for(|stopped| *stopped).await;
}

/// Resolves `budget` after the shared stop flag is set — the hard drain deadline.
async fn drain_deadline(mut rx: watch::Receiver<bool>, budget: Duration) {
    let _ = rx.wait_for(|stopped| *stopped).await;
    tokio::time::sleep(budget).await;
}

/// `SIGINT` / `SIGTERM`, whichever arrives first.
async fn wait_for_shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut stream) => {
                stream.recv().await;
            }
            // No SIGTERM support: park forever so `ctrl_c` decides.
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {}
        () = terminate => {}
    }
    tracing::info!("shutdown signal received; draining in-flight requests");
}
