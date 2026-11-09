//! `gateway` — the assembled data plane handed back to the consumer.

use std::sync::Arc;

use arc_swap::ArcSwap;

use axum::Router;
use axum::response::IntoResponse;
use gateway_core::dispatch::{DispatcherHandle, RetryPolicy};
use gateway_core::health::HealthHandle;
use gateway_dispatch::Dispatcher;
use gateway_dispatch::MemoryHealthTable;
use gateway_pipeline::Pipeline;

use crate::config::RuntimeKnobs;

/// Assembled data plane: shared handles reusable by the consumer + the pipeline
/// Router.
///
/// Four exits, one rule: **the consumer never reaches past this object into an
/// engine**. Handles stay for the pieces whose admin port has not been folded
/// into the knobs / admin surface yet.
pub struct Gateway {
    pub(crate) dispatcher: Arc<Dispatcher>,
    pub(crate) health: Arc<MemoryHealthTable>,
    pub(crate) retry_handle: Arc<ArcSwap<RetryPolicy>>,
    pub(crate) knobs: Arc<RuntimeKnobs>,
    pub(crate) pipeline: Arc<Pipeline>,
    pub(crate) router: Router,
}

impl Gateway {
    /// Dispatcher port (the same instance is shared by the ForwardStage retry
    /// loop). Handed out as the injection-surface trait so the admin plane never
    /// names the engine type.
    pub fn dispatcher(&self) -> Arc<dyn DispatcherHandle> {
        self.dispatcher.clone()
    }

    /// Channel health port (per-unit readout + live config replacement).
    pub fn health(&self) -> Arc<dyn HealthHandle> {
        self.health.clone()
    }

    /// Retry budget hot handle (live reload polling and the ForwardStage share
    /// the same `ArcSwap`); also one of the runtime knobs.
    pub fn retry_handle(&self) -> Arc<ArcSwap<RetryPolicy>> {
        self.retry_handle.clone()
    }

    /// Runtime knobs: the single surface for every runtime-mutable value.
    pub fn knobs(&self) -> Arc<RuntimeKnobs> {
        self.knobs.clone()
    }

    /// Assembled pipeline (gates → consumer stages → forwarding stage →
    /// protocol bridge → consumer stages), exposed for tests and inspection.
    pub fn pipeline(&self) -> Arc<Pipeline> {
        self.pipeline.clone()
    }

    /// Data-plane router: pipeline as fallback + path guard (only /healthz and
    /// /v1* are served, see [`gateway_path_guard`]).
    pub fn router(&self) -> Router {
        self.router.clone()
    }
}

/// Path guard: the pipeline provides the fallback in the merged Router; without
/// the guard any unmatched path (e.g. a mistyped /api/*) would fall into the
/// pipeline and be judged 401 by the AuthGate, misreporting "path does not
/// exist" as "unauthenticated". The guard returns a plain-text 404 (matching the
/// e2e contract `not found`).
pub async fn gateway_path_guard(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let path = req.uri().path();
    let scoped = path == "/healthz"
        || path == "/v1"
        || path.starts_with("/v1/")
        || path.starts_with("/v1beta");
    if scoped {
        next.run(req).await
    } else {
        (axum::http::StatusCode::NOT_FOUND, "not found").into_response()
    }
}
