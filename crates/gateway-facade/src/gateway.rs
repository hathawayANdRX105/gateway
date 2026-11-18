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

/// Narrow observability port over the response cache's hit/miss counters.
///
/// Implemented for the engine's [`gateway_cache::ResponseCache`], so the
/// consumer reads the cache stats through the facade vocabulary instead of
/// naming the cache engine type. Cumulative counters only; the cache itself
/// stays private to the forward stage.
pub trait ResponseCacheStats: Send + Sync {
    /// Lookups that found a live entry.
    fn hits(&self) -> u64;

    /// Lookups that found nothing or only an expired entry.
    fn misses(&self) -> u64;
}

impl ResponseCacheStats for gateway_cache::ResponseCache {
    fn hits(&self) -> u64 {
        gateway_cache::ResponseCache::stats(self).hits
    }

    fn misses(&self) -> u64 {
        gateway_cache::ResponseCache::stats(self).misses
    }
}

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
///   them in;
/// - [`Self::response_cache`] — the hit/miss readout of the mounted response
///   cache (`None` when the cache is off).
pub struct Gateway {
    pub(crate) dispatcher: Arc<Dispatcher>,
    pub(crate) health: Arc<MemoryHealthTable>,
    pub(crate) retry_handle: Arc<ArcSwap<RetryPolicy>>,
    pub(crate) knobs: Arc<RuntimeKnobs>,
    pub(crate) pipeline: Arc<Pipeline>,
    pub(crate) router: Router,
    pub(crate) graylist: Arc<ArcSwap<GrayListState>>,
    pub(crate) ip_policy: Arc<ArcSwap<IpPolicy>>,
    pub(crate) response_cache: Option<Arc<gateway_cache::ResponseCache>>,
}

impl Gateway {
    /// Dispatcher port (the same instance is shared by the `ForwardStage` retry
    /// loop). Handed out as the injection-surface trait so the admin plane never
    /// names the engine type.
    #[must_use]
    pub fn dispatcher(&self) -> Arc<dyn DispatcherHandle> {
        self.dispatcher.clone()
    }

    /// Channel health port (per-unit readout + live config replacement).
    #[must_use]
    pub fn health(&self) -> Arc<dyn HealthHandle> {
        self.health.clone()
    }

    /// Retry budget hot handle (live reload polling and the `ForwardStage` share
    /// the same `ArcSwap`); also one of the runtime knobs.
    #[must_use]
    pub fn retry_handle(&self) -> Arc<ArcSwap<RetryPolicy>> {
        self.retry_handle.clone()
    }

    /// Runtime knobs: the single surface for every runtime-mutable config value.
    #[must_use]
    pub fn knobs(&self) -> Arc<RuntimeKnobs> {
        self.knobs.clone()
    }

    /// Gray-list state slot: the consumer records successes / failures and reads
    /// the current bans. Exposed so the running state is never hidden (and never
    /// tuned through the config knobs).
    #[must_use]
    pub fn graylist(&self) -> Arc<ArcSwap<GrayListState>> {
        self.graylist.clone()
    }

    /// State-gate IP policy slot: the consumer stores the effective allow/deny
    /// list; the gate reads it per request.
    #[must_use]
    pub fn ip_policy(&self) -> Arc<ArcSwap<IpPolicy>> {
        self.ip_policy.clone()
    }

    /// Assembled pipeline (gates → consumer stages → forwarding stage →
    /// protocol bridge → consumer stages), exposed for tests and inspection.
    #[must_use]
    pub fn pipeline(&self) -> Arc<Pipeline> {
        self.pipeline.clone()
    }

    /// Data-plane router: pipeline as fallback + path guard (`/healthz`, `/v1`, `/v1/…`
    /// and `/v1beta…` are served, everything else 404s before the pipeline, see
    /// [`gateway_pipeline::data_plane_guard`]).
    pub fn router(&self) -> Router {
        self.router.clone()
    }

    /// Response-cache hit/miss readout: the counters of the cache mounted on the
    /// forwarding stage, or `None` when the response cache is off. The cache
    /// instance is shared with the forward stage, so the counters accumulate as
    /// the plane serves; the handle is handed out as the [`ResponseCacheStats`]
    /// port so the consumer names no cache engine type.
    #[must_use]
    pub fn response_cache(&self) -> Option<Arc<dyn ResponseCacheStats>> {
        self.response_cache
            .clone()
            .map(|cache| cache as Arc<dyn ResponseCacheStats>)
    }
}
