//! `gateway` — the assembled data plane handed back to the consumer.

use std::sync::Arc;

use arc_swap::ArcSwap;

use axum::Router;
use gateway_core::dispatch::{DispatcherHandle, RetryPolicy};
use gateway_core::health::HealthHandle;
use gateway_dispatch::Dispatcher;
use gateway_dispatch::MemoryHealthTable;
use gateway_gate::graylist::GrayListState;
use gateway_gate::snapshot::IpPolicy;
use gateway_pipeline::Pipeline;

use crate::config::RuntimeKnobs;

/// Assembled data plane: shared handles reusable by the consumer + the pipeline
/// Router.
///
/// Exits, and nothing else — the consumer never reaches past this object into
/// an engine:
/// - [`Self::router`] / [`Self::pipeline`] — serve or drive it;
/// - [`Self::knobs`] — the runtime-mutable config surface;
/// - [`Self::graylist`] / [`Self::ip_policy`] — the two consumer-tunable state
///   slots the knobs do not cover (the admin side owns their content);
/// - [`Self::dispatcher`] / [`Self::health`] / [`Self::retry_handle`] — the
///   handles a reload / observability path needs until the admin surface folds
///   them in.
pub struct Gateway {
    pub(crate) dispatcher: Arc<Dispatcher>,
    pub(crate) health: Arc<MemoryHealthTable>,
    pub(crate) retry_handle: Arc<ArcSwap<RetryPolicy>>,
    pub(crate) knobs: Arc<RuntimeKnobs>,
    pub(crate) pipeline: Arc<Pipeline>,
    pub(crate) router: Router,
    pub(crate) graylist: Arc<ArcSwap<GrayListState>>,
    pub(crate) ip_policy: Arc<ArcSwap<IpPolicy>>,
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

    /// Runtime knobs: the single surface for every runtime-mutable config value.
    pub fn knobs(&self) -> Arc<RuntimeKnobs> {
        self.knobs.clone()
    }

    /// Gray-list state slot: the consumer records successes / failures and reads
    /// the current bans. Exposed so the running state is never hidden (and never
    /// tuned through the config knobs).
    pub fn graylist(&self) -> Arc<ArcSwap<GrayListState>> {
        self.graylist.clone()
    }

    /// State-gate IP policy slot: the consumer stores the effective allow/deny
    /// list; the gate reads it per request.
    pub fn ip_policy(&self) -> Arc<ArcSwap<IpPolicy>> {
        self.ip_policy.clone()
    }

    /// Assembled pipeline (gates → consumer stages → forwarding stage →
    /// protocol bridge → consumer stages), exposed for tests and inspection.
    pub fn pipeline(&self) -> Arc<Pipeline> {
        self.pipeline.clone()
    }

    /// Data-plane router: pipeline as fallback + path guard (only /healthz and
    /// /v1* are served, see [`gateway_pipeline::data_plane_guard`]).
    pub fn router(&self) -> Router {
        self.router.clone()
    }
}
