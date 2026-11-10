//! actix-web transport over the assembled gateway pipeline.
//!
//! One handler owns the whole request path: read the body under a hard limit, build the
//! transport-neutral [`RequestCtx`], run the pipeline, then stream the resulting response
//! back chunk by chunk. The library's axum router is deliberately not used here — the
//! standalone process is the actix shape, while embedding consumers keep the axum exit.
//!
//! Two conversions are unavoidable and kept in this file: actix-web 4 speaks `http` 0.2
//! while the pipeline speaks `http` 1, and the pipeline body is an `axum::body::Body`
//! stream. Both are mechanical; no request logic lives here.

use std::io;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;

use actix_web::http::header::{HeaderName, HeaderValue};
use actix_web::{App, HttpRequest, HttpResponse, HttpServer, web};
use anyhow::Context as _;
use futures_util::StreamExt;

use gateway_facade::{
    Components, Config, Context, GatewayBuilder, Ledger, MemoryLedger, Pipeline, ProxyManager,
    QuotaGate, RequestCtx, ReqwestEgress, Settle, SettleSink, State, UsageEvent, error_to_response,
};

use crate::catalog::{Catalog, CatalogCounts};
use crate::config::DaemonConfig;

/// What a run needs: the config file and whether to stop after validation.
#[derive(Debug, Clone)]
pub struct RunArgs {
    pub config_path: std::path::PathBuf,
    /// Load the config and catalog, log the summary, exit without binding.
    pub check: bool,
}

/// Shared request state. One instance per process; actix clones the `Data` handle per
/// worker, not the state itself.
struct AppState {
    pipeline: Arc<Pipeline>,
    max_request_bytes: usize,
    trusted_proxies: Vec<IpAddr>,
    counts: CatalogCounts,
}

/// Read the config and catalog, then serve until `SIGINT` / `SIGTERM`.
///
/// # Errors
///
/// Returns config/catalog loading errors and listener bind errors.
pub async fn run(args: RunArgs) -> anyhow::Result<()> {
    let config = DaemonConfig::load(&args.config_path)?;
    let catalog = Catalog::load(&config.catalog_path(&args.config_path))?;
    let counts = catalog.counts;

    tracing::info!(
        channels = counts.channels,
        route_units = counts.route_units,
        tokens = counts.tokens,
        users = counts.users,
        groups = counts.groups,
        prices = counts.prices,
        "catalog loaded"
    );
    if args.check {
        return Ok(());
    }

    let gateway = assemble(catalog);
    let pipeline = gateway.pipeline();
    let state = web::Data::new(AppState {
        pipeline,
        max_request_bytes: config.max_request_bytes,
        trusted_proxies: config.trusted_proxies.clone(),
        counts,
    });

    let factory = {
        let state = state.clone();
        move || {
            App::new()
                .app_data(state.clone())
                .route("/healthz", web::get().to(healthz))
                .route("/readyz", web::get().to(readyz))
                .default_service(web::to(dispatch))
        }
    };

    let mut server = HttpServer::new(factory)
        .shutdown_timeout(config.shutdown_timeout_secs)
        .bind(&config.listen)
        .with_context(|| format!("bind {}", config.listen))?;
    if config.workers > 0 {
        server = server.workers(config.workers);
    }
    if let Some(path) = &config.uds_path {
        remove_stale_socket(path)?;
        server = server
            .bind_uds(path)
            .with_context(|| format!("bind uds {}", path.display()))?;
    }

    tracing::info!(
        listen = %config.listen,
        uds = ?config.uds_path,
        workers = config.workers,
        "gatewayd serving"
    );
    server.run().await?;
    Ok(())
}

/// Build the gateway once: one ledger seeded from the catalog, one quota gate, one
/// settlement pair. Nothing here is per-request.
fn assemble(catalog: Catalog) -> gateway_facade::Gateway {
    let ledger = Arc::new(MemoryLedger::new());
    ledger.reseed(&catalog.quota_snapshot);

    let quotas = Arc::new(arc_swap::ArcSwap::from_pointee(catalog.quota_snapshot));
    let pricing = Arc::new(arc_swap::ArcSwap::from_pointee(catalog.pricing_snapshot));

    GatewayBuilder::relay(Context {
        config: Config::default(),
        components: Components {
            egress: Arc::new(ReqwestEgress::new()),
            // The standalone shape never leases from the proxy pool: no node rows exist
            // until a control plane installs them, and the direct egress client serves
            // every channel.
            wire_proxy_pool: false,
            proxies: Arc::new(ProxyManager::new()),
            proxy_snapshot: gateway_facade::ProxySnapshot::default(),
            state: State {
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
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(anyhow::anyhow!(
            "remove stale socket {}: {err}",
            path.display()
        )),
    }
}

async fn healthz() -> HttpResponse {
    HttpResponse::Ok().json(serde_json::json!({ "status": "ok" }))
}

async fn readyz(state: web::Data<AppState>) -> HttpResponse {
    HttpResponse::Ok().json(serde_json::json!({
        "status": "ready",
        "channels": state.counts.channels,
        "routeUnits": state.counts.route_units,
        "groups": state.counts.groups,
        "users": state.counts.users,
        "tokens": state.counts.tokens,
        "prices": state.counts.prices,
    }))
}

/// The one request entry: path guard → body read → pipeline → actix response.
async fn dispatch(
    req: HttpRequest,
    mut payload: web::Payload,
    state: web::Data<AppState>,
) -> HttpResponse {
    if !is_data_plane_path(req.path()) {
        return HttpResponse::NotFound().finish();
    }

    let body = match read_body(&mut payload, state.max_request_bytes).await {
        Ok(body) => body,
        Err(BodyError::TooLarge) => {
            return HttpResponse::PayloadTooLarge().json(serde_json::json!({
                "error": { "message": "request body too large", "type": "invalid_request_error" }
            }));
        }
        Err(BodyError::Payload) => {
            return HttpResponse::BadRequest().json(serde_json::json!({
                "error": { "message": "request body read failed", "type": "invalid_request_error" }
            }));
        }
    };

    let Ok(method) = http::Method::from_bytes(req.method().as_str().as_bytes()) else {
        return HttpResponse::MethodNotAllowed().finish();
    };
    let ctx = RequestCtx::from_parts(
        &method,
        req.path().to_string(),
        convert_headers(req.headers()),
        body,
        client_ip(&state, &req),
    );

    let response = match state.pipeline.run(ctx).await {
        Ok(response) => response,
        Err(err) => error_to_response(err),
    };
    to_actix_response(response)
}

/// Body ceiling check per chunk: the process never buffers more than the configured limit.
async fn read_body(payload: &mut web::Payload, limit: usize) -> Result<bytes::Bytes, BodyError> {
    let mut body = web::BytesMut::with_capacity(8 * 1024);
    while let Some(chunk) = payload.next().await {
        let chunk = chunk.map_err(|_| BodyError::Payload)?;
        if body.len() + chunk.len() > limit {
            return Err(BodyError::TooLarge);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body.freeze())
}

enum BodyError {
    TooLarge,
    Payload,
}

/// Data-plane path scope, matching the axum router's guard: `/v1*` plus the two probe
/// endpoints (which are registered routes and never reach this handler).
fn is_data_plane_path(path: &str) -> bool {
    path == "/v1" || path.starts_with("/v1/") || path.starts_with("/v1beta")
}

/// Convert actix's `http` 0.2 header map to the pipeline's `http` 1 map.
///
/// Header names/values that are not valid in `http` 1 are dropped rather than failing the
/// request: a malformed optional header must not turn into a 500.
fn convert_headers(source: &actix_web::http::header::HeaderMap) -> http::HeaderMap {
    let mut headers = http::HeaderMap::with_capacity(source.len());
    for (name, value) in source {
        let Ok(name) = http::HeaderName::from_bytes(name.as_str().as_bytes()) else {
            continue;
        };
        let Ok(value) = http::HeaderValue::from_bytes(value.as_bytes()) else {
            continue;
        };
        headers.append(name, value);
    }
    headers
}

/// Client address: the socket peer unless it is an explicitly trusted proxy, in which case
/// the forwarded chain is walked from the right and the first address that is not itself a
/// trusted proxy wins.
///
/// Right-to-left is the only safe direction: every proxy appends to the chain, so the
/// left-most entry is whatever the client sent and is trivially forgeable. The default
/// (`trusted_proxies` empty) never reads the header at all.
fn client_ip(state: &AppState, req: &HttpRequest) -> IpAddr {
    match req.peer_addr().map(|addr| addr.ip()) {
        Some(peer) if state.trusted_proxies.contains(&peer) => {
            forwarded_for(req, &state.trusted_proxies).unwrap_or(peer)
        }
        Some(peer) => peer,
        None => Ipv4Addr::UNSPECIFIED.into(),
    }
}

fn forwarded_for(req: &HttpRequest, trusted: &[IpAddr]) -> Option<IpAddr> {
    req.headers()
        .get("x-forwarded-for")?
        .to_str()
        .ok()?
        .split(',')
        .rev()
        .filter_map(|part| part.trim().parse::<IpAddr>().ok())
        .find(|ip| !trusted.contains(ip))
}

/// Pipeline response → actix response, streaming the body frame by frame.
///
/// Hop-by-hop headers are dropped: actix frames the body itself (`chunked`), so forwarding
/// the upstream's `content-length` / `transfer-encoding` would describe the wrong bytes.
fn to_actix_response(response: http::Response<axum::body::Body>) -> HttpResponse {
    let (parts, body) = response.into_parts();
    let status = actix_web::http::StatusCode::from_u16(parts.status.as_u16())
        .unwrap_or(actix_web::http::StatusCode::INTERNAL_SERVER_ERROR);
    let mut builder = HttpResponse::build(status);
    for (name, value) in &parts.headers {
        if is_hop_by_hop(name.as_str()) {
            continue;
        }
        let Ok(name) = HeaderName::from_bytes(name.as_str().as_bytes()) else {
            continue;
        };
        let Ok(value) = HeaderValue::from_bytes(value.as_bytes()) else {
            continue;
        };
        builder.append_header((name, value));
    }

    let stream = body
        .into_data_stream()
        .map(|chunk| chunk.map_err(io::Error::other));
    builder.streaming(stream)
}

fn is_hop_by_hop(name: &str) -> bool {
    matches!(
        name,
        "content-length"
            | "transfer-encoding"
            | "connection"
            | "keep-alive"
            | "upgrade"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
    )
}
