//! `router` — axum integration + error response conversion
//!
//! Wires the Pipeline into the axum `fallback` and centralizes `RequestCtx` construction
//! and error conversion.

use crate::ctx::RequestCtx;
use crate::pipeline::Pipeline;
use crate::stage::StageError;
use axum::body::Body;
use axum::extract::State;
use axum::http::{Request, StatusCode};
use axum::response::{IntoResponse, Response};
use serde_json::json;
use std::convert::Infallible;
use std::sync::Arc;

/// Builds the axum Router: `/healthz` is its own route and everything else falls
/// through to the pipeline fallback.
///
/// `/healthz` must bypass the pipeline: the chain starts with the gate, so any request
/// without a valid key is rejected with 401 by `AuthGate`, health checks included. It
/// only reports that the process is alive, not upstream availability.
pub fn build_router(pipeline: Arc<Pipeline>) -> axum::Router {
    async fn dispatch(
        State(pipeline): State<Arc<Pipeline>>,
        req: Request<Body>,
    ) -> Result<Response, Infallible> {
        let ctx = match RequestCtx::from_axum(req).await {
            Ok(c) => c,
            Err(e) => return Ok(error_to_response(StageError::Internal(e))),
        };
        match pipeline.run(ctx).await {
            Ok(resp) => Ok(resp),
            Err(e) => Ok(error_to_response(e)),
        }
    }

    async fn healthz() -> Response {
        axum::Json(json!({ "status": "ok" })).into_response()
    }

    axum::Router::new()
        .route("/healthz", axum::routing::get(healthz))
        .fallback(dispatch)
        .with_state(pipeline)
}

/// Data-plane path guard: the pipeline only serves data-plane paths, and every
/// other unmatched path gets a plain 404 instead of falling into the pipeline
/// and being judged 401 by the `AuthGate` (which would misreport "path does not
/// exist" as "unauthenticated").
///
/// It lives here, next to the router integration, because the served path set is
/// a transport concern; the facade only stacks the layer.
pub async fn data_plane_guard(req: Request<Body>, next: axum::middleware::Next) -> Response {
    let path = req.uri().path();
    let scoped = path == "/healthz"
        || path == "/v1"
        || path.starts_with("/v1/")
        || path.starts_with("/v1beta");
    if !scoped {
        return (StatusCode::NOT_FOUND, "not found").into_response();
    }
    next.run(req).await
}

/// The 504/503 split for `Unavailable`: cooling (`retry_after_ms > 0`) = can wait →
/// 504 + Retry-After (the client waits out the shortest recovery and resends); no
/// cooling (== 0, e.g. all disabled / rate limited / genuinely no model) = unavailable
/// → 503, with no retry time promised (an immediate resend is pointless; leave backoff
/// to the client).
fn unavailable_parts(
    group: &str,
    model: &str,
    retry_after_ms: u64,
) -> (StatusCode, &'static str, String, Option<u64>) {
    if retry_after_ms > 0 {
        (
            StatusCode::GATEWAY_TIMEOUT,
            "all_candidates_unavailable",
            format!("all candidates cooling for {group}/{model}; retry after {retry_after_ms}ms"),
            Some(retry_after_ms.div_ceil(1000)),
        )
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "all_candidates_unavailable",
            format!("no available candidate for {group}/{model}; not cooling"),
            None,
        )
    }
}

/// Error → HTTP response (`OpenAI` error shape)
/// Classify a stage error into the response parts (status, code, message,
/// retry-after seconds), logging which branch produced it. Rendering happens in
/// [`error_to_response`].
fn error_parts(e: StageError) -> (StatusCode, &'static str, String, Option<u64>) {
    use StageError::{
        Forbidden, Internal, NoRoute, NotReady, PayloadTooLarge, QuotaExhausted, RateLimited,
        Unauthenticated, Unavailable, Upstream,
    };
    // 4-tuple: (status, code, message, retry_after_secs). retry_after is carried only by
    // Unavailable (504 + Retry-After header); the remaining variants use None.
    match e {
        Unauthenticated(msg) => {
            tracing::warn!(reason = %msg, "stage error -> response: unauthenticated");
            (StatusCode::UNAUTHORIZED, "unauthenticated", msg, None)
        }
        QuotaExhausted {
            remaining,
            required,
        } => {
            tracing::warn!(remaining = %remaining, required = %required, "stage error -> response: quota exhausted");
            (
                StatusCode::PAYMENT_REQUIRED,
                "quota_exhausted",
                format!("remaining={remaining} required={required}"),
                None,
            )
        }
        Forbidden(msg) => {
            tracing::warn!(reason = %msg, "stage error -> response: forbidden");
            (StatusCode::FORBIDDEN, "forbidden", msg, None)
        }
        NoRoute { group, model } => {
            tracing::warn!(group = %group, model = %model, "stage error -> response: no route");
            (
                StatusCode::NOT_FOUND,
                "no_route",
                format!("model not configured for {group}/{model}"),
                None,
            )
        }
        Unavailable {
            group,
            model,
            retry_after_ms,
        } => {
            tracing::warn!(group = %group, model = %model, retry_after_ms = %retry_after_ms, "stage error -> response: unavailable");
            unavailable_parts(&group, &model, retry_after_ms)
        }
        RateLimited => {
            tracing::warn!("stage error -> response: rate limited");
            (
                StatusCode::TOO_MANY_REQUESTS,
                "rate_limited",
                "all candidates rate limited".to_string(),
                None,
            )
        }
        NotReady => {
            tracing::warn!("stage error -> response: not ready");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "service_not_ready",
                "gateway not ready".to_string(),
                None,
            )
        }
        PayloadTooLarge => {
            tracing::warn!("stage error -> response: payload too large");
            (
                StatusCode::PAYLOAD_TOO_LARGE,
                "payload_too_large",
                "request body too large".to_string(),
                None,
            )
        }
        Upstream(ue) => {
            tracing::warn!(error = %ue, "stage error -> response: upstream");
            (
                StatusCode::BAD_GATEWAY,
                "upstream_error",
                ue.to_string(),
                None,
            )
        }
        Internal(err) => {
            tracing::error!(error = %err, "internal stage error");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                "internal error".to_string(),
                None,
            )
        }
    }
}

/// Error → HTTP response (`OpenAI` error shape); classification lives in
/// [`error_parts`].
pub fn error_to_response(e: StageError) -> Response {
    let (status, code, message, retry_after_secs) = error_parts(e);

    let body = json!({
        "error": {
            "code": code,
            "message": message,
        }
    });
    let mut resp = (status, axum::Json(body)).into_response();
    if let Some(secs) = retry_after_secs
        && let Ok(hv) = secs.to_string().parse()
    {
        resp.headers_mut()
            .insert(axum::http::header::RETRY_AFTER, hv);
    }
    resp
}
