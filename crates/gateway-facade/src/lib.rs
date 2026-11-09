//! # gateway — public assembly facade for the data plane
//!
//! This crate is the single entry a consumer sees when it wants to run a gateway
//! data plane. [`GatewayBuilder`] assembles the default relay chain (gate chain →
//! forwarding stage → protocol bridge) from already-loaded consumer values, and
//! exposes the named [`Stage`] insertion points so a consumer can add stages of
//! its own without naming the engine crates behind them.
//!
//! # Declaration vs assembly
//!
//! The cut runs where it always has: the consumer loads the values (snapshots,
//! options, egress, proxy rows, ledger) and hands plain values and shared
//! handles to [`GatewayBuilder`]; this crate wires them. Nothing here reads a
//! database, opens a config or builds a service.
//!
//! # Default chain
//!
//! ```text
//! ┌ custom stages ──┐   ┌ consumer custom stages ──┐
//! │ (with_stage_    │   │ (with_stage_after_chain) │
//! │  before_forward)│   │                          │
//! └────────┬────────┘   └────────────┬─────────────┘
//!          │                         │
//! AuthGate → … → gate chain → ForwardStage → ProtocolBridgeStage → (post)
//! ```
//!
//! Route selection is **inside** the forwarding stage: after `with_retry` the
//! ForwardStage drives selection itself through the shared Dispatcher, so no
//! separate DispatchStage sits in the default chain (a second one would double
//! both selection and rate-limit counting).
//!
//! # Insertion points
//!
//! | Method | Position | Typical use |
//! |---|---|---|
//! | [`GatewayBuilder::with_stage_before_forward`] | after the gates, before forwarding | auth, logging, metrics |
//! | [`GatewayBuilder::with_stage_after_chain`] | after the whole default chain | telemetry, audit |
//! | [`GatewayBuilder::with_stage`] | appended to the end (alias of after-chain) | same as above |
//!
//! A stage is any `impl Stage + 'static`; the builder boxes it into the chain.

use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use axum::Router;
use axum::middleware::from_fn;
use axum::response::IntoResponse;

use gateway_core::dispatch::{DispatcherHandle, RetryPolicy, Snapshot};
use gateway_core::egress::Timeouts;
use gateway_core::gate::{SharedGroupSnapshot, SharedTokenSnapshot, SharedUserSnapshot};
use gateway_core::health::{HealthHandle, HealthSetting};
use gateway_core::metering::{Ledger, PriceTable, SettleSink};
use gateway_dispatch::{Dispatcher, LatencyDigestTable, MemoryHealthTable, StickyTable};
use gateway_forward::StreamResilience;
use gateway_forward::egress::Egress;
use gateway_forward::stage::ForwardStage;
use gateway_gate::auth::AuthGate;
use gateway_gate::chain::GateChain;
use gateway_gate::concurrency::ConcurrencyState;
use gateway_gate::graylist::{GrayListGate, GrayListState};
use gateway_gate::model::{GroupModelGate, ModelGate};
use gateway_gate::quota::QuotaGate;
use gateway_gate::ratelimit::{RateLimitGate, RateLimiter};
use gateway_gate::snapshot::IpPolicy;
use gateway_gate::state::StateGate;
use gateway_pipeline::{Pipeline, Stage, build_router};
use gateway_protocol_bridge::format_codec::FormatRegistry;
use gateway_protocol_bridge::stage::ProtocolBridgeStage;
use gateway_proxy::{ProxyManager, ProxySnapshot};
use gateway_security::{FilterConfig, WordFilter};

/// Input to data-plane assembly: plain values and shared handles loaded by the
/// consumer, plus the two billing injection points.
pub struct Context {
    /// Upstream egress. Production = `ReqwestEgress`; tests inject a mock (the
    /// contract is "no real upstream requests", see the `wire_proxy_pool` note).
    pub egress: Arc<dyn Egress>,
    /// Whether model requests borrow an egress Client (direct / proxied) from
    /// [`ProxyManager`]. Tests pass false — no leased Client, the injected
    /// egress is called.
    pub wire_proxy_pool: bool,
    /// Egress proxy pool manager: admin-plane CRUD and data-plane leases must
    /// share the same instance.
    pub proxies: Arc<ProxyManager>,
    /// Boot snapshot of the proxy_nodes table (obtained by the consumer through
    /// control-plane loading functions).
    pub proxy_snapshot: ProxySnapshot,
    /// Word filter configuration (`config.api.security.words`; an empty word list
    /// means dormant by default, no word filter is installed).
    pub security: FilterConfig,
    /// Shared handles of the directory snapshots: they must be the same source as
    /// the consumer reload route / live reload bus, so that after storing a new
    /// value both sides see the same data.
    pub state: State,
    /// Knobs fixed by a single boot read (changing a value requires a restart).
    pub tuning: GatewayConfig,
    /// Quota gate injection point. `Some` = installed into the gate chain (after
    /// the group gate, before rate limiting); the consumer only builds it in
    /// shapes that have a price table and a settlement chain.
    pub quota_gate: Option<QuotaGate>,
    /// Settlement injection point. `Some` = the ForwardStage settlement commit
    /// point emits settlement events (price table + sink installed as a pair);
    /// `None` = no settlement, no charge, no usage_logs writes, the data plane
    /// keeps forwarding as usual.
    pub settle: Option<Settle>,
    /// Prehold ledger from the admission track; shared with QuotaGate (admission),
    /// GateChain (release on later-gate rejection) and ForwardStage (settle /
    /// release at the settlement commit points). `None` in the coding face.
    pub ledger: Option<Arc<dyn Ledger>>,
}

/// Shared handle set of the directory snapshots (loaded by the consumer at boot).
pub struct State {
    /// Channel / route unit dispatch snapshot (loaded by the Dispatcher at boot;
    /// reload swaps a new one in place through `set_snapshot`).
    pub dispatch: Snapshot,
    /// Token snapshot (the AuthGate allow-list decision).
    pub token_snapshot: SharedTokenSnapshot,
    /// User snapshot (the StateGate ban / version decision).
    pub user_snapshot: SharedUserSnapshot,
    /// Group snapshot (the GroupModelGate group allow-list intersection).
    pub group_snapshot: SharedGroupSnapshot,
}

/// Knobs fixed by a single boot read (options table → values read by the
/// consumer; changing a value requires a restart, a different rule from the hot
/// reload items covered by the reload bus).
pub struct GatewayConfig {
    /// Channel health configuration (cooldown / slow start).
    pub health: HealthSetting,
    /// Boot value of the dispatch retry budget (installed into the hot handle,
    /// shared with the reload polling).
    pub retry_policy: RetryPolicy,
    /// Egress timeouts (first_byte_ms decides how long a stalled upstream stays
    /// on the wire before failover).
    pub timeouts: Timeouts,
    /// Windowed t-digest latency signal gray switch (design §3.3).
    pub digest_enabled: bool,
    /// Prefix-cache sticky routing switch (design prompt-cache-sticky-routing).
    pub sticky_enabled: bool,
    /// Sticky routing boost (selector injection at construction time; changing the
    /// value requires a restart).
    pub sticky_boost: f64,
    /// Global default of the per-channel token budget (TPM; capacity is fixed at
    /// registration, changing the value requires a restart).
    pub token_tpm_default: u32,
    /// Per-token rate limit: maximum requests per window.
    pub ratelimit_max_requests: u32,
    /// Per-token rate limit: window length in seconds.
    pub ratelimit_window_seconds: u32,
    /// Global forwarding concurrency gate capacity.
    pub global_max_concurrency: u32,
    /// Response-cache configuration (`None` = not mounted; read once at boot,
    /// changing the value requires a restart, same policy as the sticky switch).
    pub response_cache: Option<ResponseCacheTuning>,
}

/// Response-cache limits, all fixed at construction.
pub struct ResponseCacheTuning {
    /// Entry TTL in seconds.
    pub ttl_secs: u32,
    /// Total body-byte budget.
    pub max_total_bytes: u32,
    /// Per-entry refusal limit in bytes.
    pub max_entry_bytes: u32,
}

/// Billing settlement injection point: price table + settlement sink installed as
/// a pair at the ForwardStage settlement commit point.
///
/// The fields are trait objects — the implementations belong to the consumer
/// (e.g. the billing domain's DbPriceTable / DbSettleSink); this crate does not
/// know billing domain types.
pub struct Settle {
    /// Price table (resolves the unit price per model).
    pub price_table: Arc<dyn PriceTable>,
    /// Settlement sink (settlement event / charging write point).
    pub sink: Arc<dyn SettleSink>,
}

/// Chain builder for the gateway data plane.
///
/// [`GatewayBuilder::relay`] assembles the default relay chain; the
/// `with_stage_*` methods add consumer stages at named points. Every
/// configuration change is expressed as a value in [`GatewayConfig`] — there is
/// no options table read here, the consumer owns its configuration.
pub struct GatewayBuilder {
    ctx: Context,
    pre_forward: Vec<Arc<dyn Stage>>,
    post: Vec<Arc<dyn Stage>>,
}

impl GatewayBuilder {
    /// Start the default relay chain from consumer-loaded values.
    pub fn relay(ctx: Context) -> Self {
        Self {
            ctx,
            pre_forward: vec![],
            post: vec![],
        }
    }

    /// Replace the knobs (defaults to the values already carried by
    /// [`Context::tuning`]).
    pub fn with_config(mut self, config: GatewayConfig) -> Self {
        self.ctx.tuning = config;
        self
    }

    /// Add a custom stage after the gate chain, before the forwarding stage.
    pub fn with_stage_before_forward(mut self, stage: impl Stage + 'static) -> Self {
        self.pre_forward.push(Arc::new(stage));
        self
    }

    /// Add a custom stage after the whole default chain.
    pub fn with_stage_after_chain(mut self, stage: impl Stage + 'static) -> Self {
        self.post.push(Arc::new(stage));
        self
    }

    /// Alias of [`Self::with_stage_after_chain`]: append at the end.
    pub fn with_stage(self, stage: impl Stage + 'static) -> Self {
        self.with_stage_after_chain(stage)
    }

    /// Wire the deterministic default chain. Assembly is pure wiring with no
    /// failure point: everything that can fail (migrations, snapshot / knob /
    /// proxy row loading) has already been done on the consumer side.
    pub fn build(self) -> Gateway {
        let Context {
            egress,
            wire_proxy_pool,
            proxies,
            proxy_snapshot,
            security,
            state,
            tuning,
            quota_gate,
            settle,
            ledger,
        } = self.ctx;
        let GatewayConfig {
            health: health_setting,
            retry_policy,
            timeouts,
            digest_enabled,
            sticky_enabled,
            sticky_boost,
            token_tpm_default,
            ratelimit_max_requests,
            ratelimit_window_seconds,
            global_max_concurrency,
            response_cache,
        } = tuning;

        // Egress proxy pool: the consumer has already loaded the enabled DB
        // proxy_nodes rows into a snapshot and they are installed into the manager
        // here; the in-place reload for admin console CRUD is wired by the consumer.
        proxies.install(proxy_snapshot);

        // Windowed t-digest latency signal gray switch: when true the
        // LatencyDigestTable is installed and health records are dual-written to
        // the window digest; false installs nothing.
        let digest_table = if digest_enabled {
            Some(LatencyDigestTable::new())
        } else {
            None
        };
        // Prefix-cache sticky routing: one StickyTable shared by the write side
        // (ForwardStage settlement commit point) and the read side (route
        // selection). false = no table, degrading byte for byte to pure P2C.
        let sticky_table = if sticky_enabled {
            Some(StickyTable::new())
        } else {
            None
        };
        let mut health = MemoryHealthTable::with_config(health_setting);
        if let Some(digest) = &digest_table {
            health = health.with_digest(digest.clone());
        }
        let health = Arc::new(health);
        // Per-channel concurrency slots: the Dispatcher P2C likelihood and the
        // actual slot occupancy of the ForwardStage must share the same state,
        // otherwise the available slot count read by the selector drifts from
        // real occupancy.
        let concurrency = Arc::new(ConcurrencyState::default());
        // Per-channel token budget (token-side signal source of the P2C load
        // factor): selection and forwarding share the same instance; the global
        // default is fixed at registration, per-channel overrides come from the
        // channel settings on snapshot load.
        let token_budget = gateway_dispatch::token_budget::TokenBudgetState::new();
        token_budget.set_global_default(token_tpm_default as u64);
        let mut dispatcher = Dispatcher::new(
            Some(Arc::new(state.dispatch)),
            health.clone(),
            concurrency.clone(),
        )
        .with_token_budget(token_budget.clone());
        if let Some(digest) = &digest_table {
            dispatcher = dispatcher.with_latency_digest(digest.clone());
        }
        if sticky_enabled {
            dispatcher = dispatcher.with_sticky_boost(sticky_boost);
        }
        let dispatcher = Arc::new(dispatcher);

        // Dispatch knob hot handle: the boot value is placed into an ArcSwap, and
        // the ForwardStage and the consumer-side live reload polling share the
        // same handle — changing max_attempts in the admin console does not
        // require rebuilding the stage.
        let retry_handle = Arc::new(ArcSwap::from_pointee(retry_policy));

        // The snapshots are already Shared* (Arc<ArcSwap<T>>) and are fed to the
        // gates directly.
        let gates = GateChain::new()
            .push(AuthGate::new(state.token_snapshot.clone()))
            .push(StateGate::new(
                state.user_snapshot.clone(),
                Arc::new(ArcSwap::from_pointee(IpPolicy::default())),
            ))
            .push(ModelGate)
            .push(GroupModelGate::new(state.group_snapshot.clone()));

        // The quota gate is inserted after the group gate and before rate limiting.
        // It is only injected in shapes that have a price table and a settlement
        // chain: the price snapshot must be the same source as billing.
        let mut gates = match quota_gate {
            Some(quota) => gates.push(quota),
            None => gates,
        };
        // The prehold ledger is shared by the quota gate (admission), the chain's
        // release-on-later-rejection path, and ForwardStage (settle / release at
        // the settlement commit points).
        if let Some(l) = &ledger {
            gates = gates.with_ledger(Arc::clone(l));
        }

        // Per-token rate limiting and the global concurrency gate: semaphore
        // capacity and rate limit parameters are fixed at construction time with
        // no hot handle — adjusting them means a restart. Per-channel concurrency
        // capacity differs: it comes from the snapshot load and is hot-updated on
        // every reload round.
        let gates = gates
            .push(RateLimitGate::new(Arc::new(RateLimiter::new(
                ratelimit_max_requests,
                ratelimit_window_seconds,
            ))))
            .push(GrayListGate::new(Arc::new(ArcSwap::from_pointee(
                GrayListState::default(),
            ))));

        let adaptors = Arc::new(FormatRegistry::with_defaults());

        // Egress wiring: production attaches the proxy pool (leased Client first),
        // tests keep the mock egress passthrough.
        let forward_stage = ForwardStage::new(egress, adaptors.clone());
        let forward_stage = forward_stage.with_ledger(ledger.clone());
        let forward_stage = if wire_proxy_pool {
            forward_stage.with_proxies(proxies.clone())
        } else {
            forward_stage
        };

        // Billing wiring: the pipeline settlement point is the only charging write
        // point; the price table + settlement sink are built by the consumer and
        // injected here as a pair. `None` = no settlement, no charge, no
        // usage_logs writes (the data plane keeps forwarding as usual).
        let forward_stage = match settle {
            Some(settle) => forward_stage.with_price_table(settle.price_table, settle.sink),
            None => forward_stage,
        };

        // Sticky binding table installation (write side): shares the same Arc
        // with the read-side route selection. Not installed when sticky is off.
        let forward_stage = if let Some(table) = &sticky_table {
            forward_stage.with_sticky_table(table.clone())
        } else {
            forward_stage
        };

        // Response-cache mount (read once at boot): identical non-streaming
        // requests replay the previous upstream answer at the top of the forward
        // stage, settled exactly like the original call. `None` = not mounted.
        let forward_stage = if let Some(cfg) = &response_cache {
            tracing::info!(
                ttl_secs = cfg.ttl_secs,
                max_total_bytes = cfg.max_total_bytes,
                max_entry_bytes = cfg.max_entry_bytes,
                "response cache mounted"
            );
            forward_stage.with_response_cache(gateway_cache::ResponseCache::with_limits(
                Duration::from_secs(u64::from(cfg.ttl_secs)),
                u64::from(cfg.max_total_bytes),
                usize::try_from(cfg.max_entry_bytes).unwrap_or(usize::MAX),
            ))
        } else {
            forward_stage
        };

        // Word filter (dormant by default): only a non-empty word list builds the
        // WordFilter and attaches the request-side switch; an empty list = not
        // attached, forward skips the whole thing. The info! log records only the
        // word count, never the words themselves.
        let forward_stage = if security.words.is_empty() {
            forward_stage
        } else {
            tracing::info!(
                words = security.words.len(),
                filter_request = security.filter_request,
                "word-filter enabled on the request path"
            );
            forward_stage
                .with_word_filter(Arc::new(WordFilter::new(&security)))
                .with_word_filter_request(security.filter_request)
        };

        // Default chain: gates → (consumer stages) → forwarding stage → protocol
        // bridge → (consumer stages). Route selection lives inside the forwarding
        // stage (see the module docs), so no DispatchStage sits in the chain.
        let mut pipeline = Pipeline::new().push(gates);
        for stage in self.pre_forward {
            pipeline = pipeline.push_shared(stage);
        }
        let mut pipeline = pipeline
            .push(
                forward_stage
                    .with_timeouts(timeouts)
                    .with_retry_handle(dispatcher.clone(), retry_handle.clone())
                    // Global concurrency gate: the overall cap, from the options
                    // key gateway.dispatch.global_max_concurrency (default 64).
                    .with_concurrency(global_max_concurrency as usize)
                    // Per-channel token budget: shares the same TokenBudgetState
                    // with the Dispatcher.
                    .with_token_budget(token_budget)
                    // Per-channel concurrency gate: shares the state with the
                    // Dispatcher so P2C can read the real available slot count.
                    // Streaming resilience Window phase (buffer before the first
                    // byte + switch to a candidate retry on stream break).
                    .with_channel_concurrency(concurrency.clone())
                    .with_stream_resilience(Some(StreamResilience::default())),
            )
            .push(ProtocolBridgeStage::new(adaptors));
        for stage in self.post {
            pipeline = pipeline.push_shared(stage);
        }
        // The prehold ledger is shared with the admission track (QuotaGate) and
        // the gate chain (release on later-gate rejection): the pipeline releases
        // a still-outstanding hold exactly once on any terminal outcome where no
        // commit point settled it.
        let pipeline = Arc::new(pipeline.with_ledger(ledger));

        // The path guard wraps the pipeline router: the pipeline only serves
        // data-plane paths (/v1* and /healthz), and every other unmatched path
        // gets a 404 instead of falling into the pipeline and being judged 401 by
        // the AuthGate.
        let router = build_router(pipeline.clone()).layer(from_fn(gateway_path_guard));

        Gateway {
            dispatcher,
            health,
            retry_handle,
            pipeline,
            router,
        }
    }
}

/// Assembled data plane: shared handles reusable by the consumer + the pipeline Router.
pub struct Gateway {
    dispatcher: Arc<Dispatcher>,
    health: Arc<MemoryHealthTable>,
    retry_handle: Arc<ArcSwap<RetryPolicy>>,
    pipeline: Arc<Pipeline>,
    router: Router,
}

impl Gateway {
    /// Dispatcher port (the same instance is shared by the ForwardStage retry
    /// loop, the admin-plane health endpoint and the reload route). Handed out as
    /// the injection-surface trait so the admin plane never names the engine type.
    pub fn dispatcher(&self) -> Arc<dyn DispatcherHandle> {
        self.dispatcher.clone()
    }

    /// Channel health port (the admin plane / api/gateway/health read the same
    /// instance).
    pub fn health(&self) -> Arc<dyn HealthHandle> {
        self.health.clone()
    }

    /// Retry budget hot handle (live reload polling and the ForwardStage share
    /// the same `ArcSwap`).
    pub fn retry_handle(&self) -> Arc<ArcSwap<RetryPolicy>> {
        self.retry_handle.clone()
    }

    /// Assembled pipeline (gate chain → consumer stages → forwarding stage →
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
async fn gateway_path_guard(
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
