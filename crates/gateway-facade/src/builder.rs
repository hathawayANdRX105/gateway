//! `builder` — chain assembly: pick a preset, then compose.

use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;

use gateway_core::dispatch::{RetryPolicy, SelectorKind, Snapshot};
use gateway_core::egress::Timeouts;
use gateway_core::gate::{SharedGroupSnapshot, SharedTokenSnapshot, SharedUserSnapshot};
use gateway_core::health::HealthSetting;
use gateway_core::metering::Ledger;
use gateway_dispatch::token_budget::TokenBudgetState;
use gateway_dispatch::{
    Dispatcher, LatencyDigestTable, MemoryHealthTable, StickyTable, WeightedRandom,
};
use gateway_forward::StreamResilience;
use gateway_forward::egress::Egress;
use gateway_forward::stage::ForwardStage;
use gateway_gate::chain::GateChain;
use gateway_gate::concurrency::ConcurrencyState;
use gateway_gate::graylist::GrayListState;
use gateway_gate::quota::QuotaGate;
use gateway_gate::ratelimit::RateLimiter;
use gateway_gate::snapshot::IpPolicy;
use gateway_pipeline::{Pipeline, Stage, build_router};
use gateway_protocol_bridge::format_codec::FormatRegistry;
use gateway_protocol_bridge::stage::ProtocolBridgeStage;
use gateway_proxy::ProxyManager;
use gateway_security::{FilterConfig, WordFilter};

use crate::config::{
    BootConfig, Components, Config, Context, ResponseCacheTuning, RuntimeKnobs, Settle,
};
use crate::gateway::Gateway;
use crate::preset::{GateParts, Shape};

/// Chain builder for the gateway data plane.
///
/// ```ignore
/// // preset + composition, one call each:
/// let gateway = GatewayBuilder::relay(ctx)          // Shape::Relay chain
///     .with_gate(MyGate::new())                     // append at the chain tail
///     .with_stage_before_forward(MyStage::new())    // stage slot
///     .build();                                     // pure wiring, no failure point
/// ```
///
/// Composition order: the admission chain comes from the preset (or your own
/// [`GateChain`]), then `with_gate` appends to its tail, then the stage slots,
/// then build. Everything the running plane can change afterwards lives in
/// [`RuntimeKnobs`] on the returned [`Gateway`].
pub struct GatewayBuilder {
    ctx: Context,
    shape: Shape,
    chain: Option<GateChain>,
    extra_gates: Vec<Arc<dyn gateway_gate::Gate>>,
    drop_gates: Vec<&'static str>,
    pre_forward: Vec<Arc<dyn Stage>>,
    post: Vec<Arc<dyn Stage>>,
}

impl GatewayBuilder {
    /// Full relay shape (auth → state → model → group → quota? → rate → gray).
    #[must_use]
    pub fn relay(ctx: Context) -> Self {
        Self::with_shape(ctx, Shape::Relay)
    }

    /// No-billing shape: the relay chain **without** the quota gate. A quota gate
    /// still sitting in `Components` is dropped, so the shape is a decision and
    /// not a hope that the consumer remembered to leave it out.
    #[must_use]
    pub fn no_billing(ctx: Context) -> Self {
        Self::with_shape(ctx, Shape::NoBilling)
    }

    /// Trusted shape: model-parse gate only — the caller fronts authentication
    /// and the allowlist itself.
    #[must_use]
    pub fn open(ctx: Context) -> Self {
        Self::with_shape(ctx, Shape::Open)
    }

    /// Start from an explicit shape.
    #[must_use]
    pub fn with_shape(ctx: Context, shape: Shape) -> Self {
        Self {
            ctx,
            shape,
            chain: None,
            extra_gates: vec![],
            drop_gates: vec![],
            pre_forward: vec![],
            post: vec![],
        }
    }

    /// Replace the knob set (defaults to the values carried by
    /// [`Context::config`]).
    #[must_use]
    pub fn with_config(mut self, config: Config) -> Self {
        self.ctx.config = config;
        self
    }

    /// Append one gate at the tail of the admission chain (after the preset's
    /// own gates).
    #[must_use]
    pub fn with_gate<G: gateway_gate::Gate + 'static>(mut self, gate: G) -> Self {
        self.extra_gates.push(Arc::new(gate));
        self
    }

    /// Hand the builder a fully composed gate chain, replacing the preset's.
    /// The builder still attaches the prehold ledger, so a custom chain keeps
    /// the hold lifecycle intact.
    #[must_use]
    pub fn with_gates(mut self, chain: GateChain) -> Self {
        self.chain = Some(chain);
        self
    }

    /// Drop a gate by name (see [`crate::preset::GATE_NAMES`]) from the chain
    /// that is currently assembled. No-op when the name is absent from the
    /// assembled chain; a name outside the known list trips a debug assertion,
    /// because a typo would otherwise look like a successful removal.
    #[must_use]
    pub fn without_gate(mut self, name: &'static str) -> Self {
        debug_assert!(
            crate::preset::GATE_NAMES.contains(&name),
            "without_gate: unknown gate name {name:?}; known names: {:?}",
            crate::preset::GATE_NAMES
        );
        self.drop_gates.push(name);
        self
    }

    /// Add a custom stage after the gate chain, before the forwarding stage.
    #[must_use]
    pub fn with_stage_before_forward(mut self, stage: impl Stage + 'static) -> Self {
        self.pre_forward.push(Arc::new(stage));
        self
    }

    /// Add a custom stage after the whole default chain.
    #[must_use]
    pub fn with_stage_after_chain(mut self, stage: impl Stage + 'static) -> Self {
        self.post.push(Arc::new(stage));
        self
    }

    /// Alias of [`Self::with_stage_after_chain`]: append at the end.
    #[must_use]
    pub fn with_stage(self, stage: impl Stage + 'static) -> Self {
        self.with_stage_after_chain(stage)
    }

    /// Assemble the admission chain: the consumer's own composition when given,
    /// otherwise the preset's, plus appended gates and drops. The prehold ledger
    /// is mounted last regardless of who built the chain, so a custom chain
    /// keeps the hold lifecycle intact.
    fn admission_chain(inputs: ChainInputs<'_>) -> GateChain {
        let ChainInputs {
            shape,
            chain,
            extra_gates,
            drop_gates,
            tokens,
            users,
            groups,
            quota_gate,
            ledger,
            parts,
        } = inputs;
        // A no-billing shape drops an injected quota gate rather than silently
        // installing preholds it was never asked for.
        let quota_gate = match shape {
            Shape::Relay => quota_gate,
            Shape::NoBilling | Shape::Open => {
                if quota_gate.is_some() {
                    tracing::warn!(
                        shape = ?shape,
                        "{shape:?} shape drops an injected quota gate; preholds are not installed"
                    );
                }
                None
            }
        };
        let mut gates = match chain {
            Some(chain) => chain.clone(),
            None => shape.gate_chain(tokens, users, groups, quota_gate, parts),
        };
        for name in drop_gates {
            gates.remove(name);
        }
        for gate in extra_gates {
            gates.push_gate_shared(Arc::clone(gate));
        }
        if let Some(ledger) = ledger {
            gates = gates.with_ledger(Arc::clone(ledger));
        }
        gates
    }

    /// Wire the deterministic chain. Pure wiring with no failure point: everything
    /// that can fail (migrations, snapshot / knob / proxy row loading) has already
    /// been done on the consumer side.
    ///
    /// The work splits in three: [`Self::wire_signals`] builds the dispatch-side
    /// state whose instances must be *shared* to stay coherent,
    /// [`Self::admission_chain`] assembles the gates, and
    /// [`Self::wire_forward_stage`] mounts the forwarding stage. This function
    /// only sequences them and hands the handles out.
    // Sequencing only: every subsystem assembles in its own helper
    // (`wire_signals` / `admission_chain` / `wire_forward_stage`); the residual
    // length is the flat destructure of builder inputs, so a further split would
    // just relocate that destructure into a helper that reads worse.
    #[allow(clippy::too_many_lines)]
    pub fn build(self) -> Gateway {
        let Self {
            ctx,
            shape,
            chain,
            extra_gates,
            drop_gates,
            pre_forward,
            post,
        } = self;
        let Context {
            config,
            components:
                Components {
                    egress,
                    wire_proxy_pool,
                    proxies,
                    proxy_snapshot,
                    state,
                    quota_gate,
                    settle,
                    ledger,
                },
        } = ctx;
        let Config {
            timeouts,
            retry_policy,
            health: health_setting,
            selector: selector_kind,
            digest_enabled,
            sticky_enabled,
            sticky_boost,
            token_tpm_default,
            ratelimit_max_requests,
            ratelimit_window_seconds,
            global_max_concurrency,
            response_cache,
            security,
        } = config;

        // Egress proxy pool: the consumer has already loaded the enabled DB
        // proxy_nodes rows into a snapshot; they are installed here, and the
        // in-place reload for admin console CRUD is wired by the consumer.
        proxies.install(proxy_snapshot);

        // Move the catalog snapshot out of `State` (a deep copy otherwise); the
        // admission chain keeps the three directory snapshots it needs.
        let crate::config::State {
            dispatch,
            token_snapshot,
            user_snapshot,
            group_snapshot,
        } = state;
        let signals = Self::wire_signals(
            dispatch,
            &Signals {
                health_setting,
                retry_policy,
                selector_kind,
                digest_enabled,
                sticky_enabled,
                sticky_boost,
                token_tpm_default,
            },
        );
        let SignalHub {
            health,
            concurrency,
            token_budget,
            sticky_table,
            dispatcher,
            retry_handle,
        } = signals;

        // Admission chain: preset, or the consumer's own composition.
        let rate_limiter = Arc::new(RateLimiter::new(
            ratelimit_max_requests,
            ratelimit_window_seconds,
        ));
        let graylist = Arc::new(ArcSwap::from_pointee(GrayListState::default()));
        let ip_policy = Arc::new(ArcSwap::from_pointee(IpPolicy::default()));
        let gates = Self::admission_chain(ChainInputs {
            shape,
            chain: chain.as_ref(),
            extra_gates: &extra_gates,
            drop_gates: &drop_gates,
            tokens: &token_snapshot,
            users: &user_snapshot,
            groups: &group_snapshot,
            quota_gate,
            ledger: ledger.as_ref(),
            parts: &GateParts {
                rate_limiter: rate_limiter.clone(),
                graylist: graylist.clone(),
            },
        });
        let adaptors = Arc::new(FormatRegistry::with_defaults());
        let forward_stage = Self::wire_forward_stage(
            &Mounts {
                egress,
                adaptors: adaptors.clone(),
                wire_proxy_pool,
                proxies,
                ledger,
                settle,
                sticky: sticky_table,
                response_cache,
                // Clone at assembly only: `BootConfig` keeps the original for the
                // read-only view.
                security: security.clone(),
            },
            &dispatcher,
            retry_handle.clone(),
            timeouts,
            global_max_concurrency,
            token_budget.clone(),
            concurrency.clone(),
        );

        // Default chain: gates → (consumer stages) → forwarding stage → protocol
        // bridge → (consumer stages). Route selection lives inside the forwarding
        // stage (see the module docs), so no DispatchStage sits in the chain.
        let mut pipeline = Pipeline::new().push(gates);
        pipeline = pre_forward
            .into_iter()
            .fold(pipeline, Pipeline::push_shared);
        pipeline = pipeline
            .push(forward_stage)
            .push(ProtocolBridgeStage::new(adaptors));
        pipeline = post.into_iter().fold(pipeline, Pipeline::push_shared);
        let pipeline = Arc::new(pipeline);

        // Runtime knobs: the one surface for everything the running plane can
        // change. Boot-fixed values travel as a read-only view.
        let knobs = Arc::new(RuntimeKnobs::new(
            retry_handle.clone(),
            health.clone(),
            rate_limiter,
            token_budget,
            BootConfig {
                timeouts,
                digest_enabled,
                sticky_enabled,
                sticky_boost,
                global_max_concurrency,
                response_cache,
                security,
            },
        ));

        // The path guard wraps the pipeline router: the pipeline only serves
        // data-plane paths (/v1* and /healthz), and every other unmatched path
        // gets a 404 instead of falling into the pipeline and being judged 401 by
        // the AuthGate. The guard itself lives with the router integration in
        // `gateway-pipeline`, not here.
        let router = build_router(pipeline.clone()).layer(axum::middleware::from_fn(
            gateway_pipeline::data_plane_guard,
        ));

        Gateway {
            dispatcher,
            health,
            retry_handle,
            knobs,
            pipeline,
            router,
            graylist,
            ip_policy,
        }
    }

    /// Build the dispatch-side shared state. Everything returned here must be
    /// **the same instance** for the selector and the forward stage: P2C reads
    /// health, concurrency slots, and token-budget occupancy that the forward
    /// path actually holds.
    fn wire_signals(dispatch: Snapshot, signals: &Signals) -> SignalHub {
        let Signals {
            health_setting,
            retry_policy,
            selector_kind,
            digest_enabled,
            sticky_enabled,
            sticky_boost,
            token_tpm_default,
        } = *signals;

        // Windowed t-digest latency signal gray switch: when true the
        // LatencyDigestTable is installed and health records are dual-written to
        // the window digest; false installs nothing.
        let digest_table = digest_enabled.then(LatencyDigestTable::new);
        // Prefix-cache sticky routing: one StickyTable shared by the write side
        // (ForwardStage settlement commit point) and the read side (route
        // selection). false = no table, degrading byte for byte to pure P2C.
        let sticky_table = sticky_enabled.then(StickyTable::new);
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
        let token_budget = TokenBudgetState::new();
        token_budget.set_global_default(token_tpm_default);
        // Selection strategy: P2C (default) receives the P2C-specific knobs through
        // the historical dispatcher builders; weighted random (new-api same-tier
        // semantics) consumes only the static weight + sticky boost and is installed
        // via `with_selector`. The token budget registration and the shared
        // concurrency/health state are strategy-independent and stay on the
        // dispatcher either way.
        let mut dispatcher = Dispatcher::new(
            Some(Arc::new(dispatch)),
            health.clone(),
            concurrency.clone(),
        )
        .with_token_budget(token_budget.clone());
        dispatcher = match selector_kind {
            SelectorKind::P2c => {
                if let Some(digest) = &digest_table {
                    dispatcher = dispatcher.with_latency_digest(digest.clone());
                }
                if sticky_enabled {
                    dispatcher = dispatcher.with_sticky_boost(sticky_boost);
                }
                dispatcher
            }
            SelectorKind::WeightedRandom => {
                dispatcher.with_selector(Box::new(WeightedRandom::new(if sticky_enabled {
                    sticky_boost
                } else {
                    1.0
                })))
            }
        };
        let dispatcher = Arc::new(dispatcher);

        // Dispatch knob hot handle: the boot value is placed into an ArcSwap, and
        // the ForwardStage and the consumer-side live reload polling share the
        // same handle — changing max_attempts in the admin console does not
        // require rebuilding the stage.
        let retry_handle = Arc::new(ArcSwap::from_pointee(retry_policy));

        SignalHub {
            health,
            concurrency,
            token_budget,
            sticky_table,
            dispatcher,
            retry_handle,
        }
    }

    /// Mount the optional pieces on the forwarding stage, then the always-on
    /// ones. Every optional mount is a bare conditional: absent knob/port → the
    /// stage stays untouched, so there is no else branch that exists only to
    /// echo the value back.
    fn wire_forward_stage(
        mounts: &Mounts,
        dispatcher: &Arc<Dispatcher>,
        retry_handle: Arc<ArcSwap<RetryPolicy>>,
        timeouts: Timeouts,
        global_max_concurrency: u32,
        token_budget: Arc<TokenBudgetState>,
        concurrency: Arc<ConcurrencyState>,
    ) -> ForwardStage {
        let Mounts {
            egress,
            adaptors,
            wire_proxy_pool,
            proxies,
            ledger,
            settle,
            sticky,
            response_cache,
            security,
        } = mounts;
        let mut forward_stage =
            ForwardStage::new(Arc::clone(egress), Arc::clone(adaptors)).with_ledger(ledger.clone());
        // Egress wiring: production attaches the proxy pool (leased Client first),
        // tests keep the mock egress passthrough.
        if *wire_proxy_pool {
            forward_stage = forward_stage.with_proxies(Arc::clone(proxies));
        }
        // Billing wiring: the pipeline settlement point is the only charging write
        // point; the price table + settlement sink are built by the consumer and
        // injected here as a pair. `None` = no settlement, no charge, no
        // usage_logs writes (the data plane keeps forwarding as usual).
        if let Some(Settle { price_table, sink }) = settle {
            forward_stage =
                forward_stage.with_price_table(Arc::clone(price_table), Arc::clone(sink));
        }
        // Sticky binding table installation (write side): shares the same Arc
        // with the read-side route selection. Not installed when sticky is off.
        if let Some(table) = sticky {
            forward_stage = forward_stage.with_sticky_table(Arc::clone(table));
        }
        // Response-cache mount (read once at boot): identical non-streaming
        // requests replay the previous upstream answer at the top of the forward
        // stage, settled exactly like the original call. `None` = not mounted.
        if let Some(cfg) = response_cache {
            tracing::info!(
                ttl_secs = cfg.ttl_secs,
                max_total_bytes = cfg.max_total_bytes,
                max_entry_bytes = cfg.max_entry_bytes,
                "response cache mounted"
            );
            forward_stage =
                forward_stage.with_response_cache(gateway_cache::ResponseCache::with_limits(
                    Duration::from_secs(u64::from(cfg.ttl_secs)),
                    u64::from(cfg.max_total_bytes),
                    usize::try_from(cfg.max_entry_bytes).unwrap_or(usize::MAX),
                ));
        }
        // Word filter (dormant by default): only a non-empty word list builds the
        // WordFilter and attaches the request-side switch; an empty list = not
        // attached, forward skips the whole thing. The info! log records only the
        // word count, never the words themselves.
        if !security.words.is_empty() {
            tracing::info!(
                words = security.words.len(),
                filter_request = security.filter_request,
                "word-filter enabled on the request path"
            );
            forward_stage = forward_stage
                .with_word_filter(Arc::new(WordFilter::new(security)))
                .with_word_filter_request(security.filter_request);
        }

        forward_stage
            .with_timeouts(timeouts)
            .with_retry_handle(dispatcher.clone(), retry_handle)
            // Global concurrency gate: the overall cap.
            .with_concurrency(global_max_concurrency as usize)
            // Per-channel token budget: shares the same TokenBudgetState
            // with the Dispatcher.
            .with_token_budget(token_budget)
            // Per-channel concurrency gate: shares the state with the
            // Dispatcher so P2C can read the real available slot count.
            // Streaming resilience Window phase (buffer before the first
            // byte + switch to a candidate retry on stream break).
            .with_channel_concurrency(concurrency)
            .with_stream_resilience(Some(StreamResilience::default()))
    }
}

/// Dispatch-side shared state handed from [`GatewayBuilder::wire_signals`] to
/// the rest of the wiring.
struct SignalHub {
    health: Arc<MemoryHealthTable>,
    concurrency: Arc<ConcurrencyState>,
    token_budget: Arc<TokenBudgetState>,
    sticky_table: Option<Arc<StickyTable>>,
    dispatcher: Arc<Dispatcher>,
    retry_handle: Arc<ArcSwap<RetryPolicy>>,
}

/// The config slice `wire_signals` consumes.
#[derive(Debug, Clone)]
struct Signals {
    health_setting: HealthSetting,
    retry_policy: RetryPolicy,
    selector_kind: SelectorKind,
    digest_enabled: bool,
    sticky_enabled: bool,
    sticky_boost: f64,
    token_tpm_default: u64,
}

/// The runtime objects `wire_forward_stage` consumes.
struct Mounts {
    egress: Arc<dyn Egress>,
    adaptors: Arc<FormatRegistry>,
    wire_proxy_pool: bool,
    proxies: Arc<ProxyManager>,
    ledger: Option<Arc<dyn Ledger>>,
    settle: Option<Settle>,
    sticky: Option<Arc<StickyTable>>,
    response_cache: Option<ResponseCacheTuning>,
    security: FilterConfig,
}

/// Everything the admission chain needs beyond the builder fields: the
/// directory snapshots the stock gates read, the quota gate and ledger the
/// billing hooks inject, and the runtime `GateParts` the builder created.
struct ChainInputs<'a> {
    shape: Shape,
    chain: Option<&'a GateChain>,
    extra_gates: &'a [Arc<dyn gateway_gate::Gate>],
    drop_gates: &'a [&'static str],
    tokens: &'a SharedTokenSnapshot,
    users: &'a SharedUserSnapshot,
    groups: &'a SharedGroupSnapshot,
    quota_gate: Option<QuotaGate>,
    ledger: Option<&'a Arc<dyn Ledger>>,
    parts: &'a GateParts,
}
