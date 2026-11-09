//! `builder` — chain assembly: pick a preset, then compose.

use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;

use gateway_dispatch::token_budget::TokenBudgetState;
use gateway_dispatch::{Dispatcher, LatencyDigestTable, MemoryHealthTable, StickyTable};
use gateway_forward::StreamResilience;
use gateway_forward::stage::ForwardStage;
use gateway_gate::chain::GateChain;
use gateway_gate::concurrency::ConcurrencyState;
use gateway_gate::graylist::GrayListState;
use gateway_gate::ratelimit::RateLimiter;
use gateway_pipeline::{Pipeline, Stage, build_router};
use gateway_protocol_bridge::format_codec::FormatRegistry;
use gateway_protocol_bridge::stage::ProtocolBridgeStage;
use gateway_security::WordFilter;

use crate::config::{BootConfig, Components, Config, Context, RuntimeKnobs, Settle};
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
    pub fn relay(ctx: Context) -> Self {
        Self::with_shape(ctx, Shape::Relay)
    }

    /// No-billing shape: the relay chain without the quota gate.
    pub fn no_billing(ctx: Context) -> Self {
        Self::with_shape(ctx, Shape::NoBilling)
    }

    /// Trusted shape: model gate only — the caller fronts authentication itself.
    pub fn open(ctx: Context) -> Self {
        Self::with_shape(ctx, Shape::Open)
    }

    /// Start from an explicit shape.
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
    pub fn with_config(mut self, config: Config) -> Self {
        self.ctx.config = config;
        self
    }

    /// Append one gate at the tail of the admission chain (after the preset's
    /// own gates).
    pub fn with_gate<G: gateway_gate::Gate + 'static>(mut self, gate: G) -> Self {
        self.extra_gates.push(Arc::new(gate));
        self
    }

    /// Hand the builder a fully composed gate chain, replacing the preset's.
    /// The builder still attaches the prehold ledger and the runtime knobs, so
    /// a custom chain keeps the hold lifecycle intact.
    pub fn with_gates(mut self, chain: GateChain) -> Self {
        self.chain = Some(chain);
        self
    }

    /// Drop a gate by name (see [`crate::preset::GATE_NAMES`]) from the chain
    /// that is currently assembled. No-op when the name is absent.
    pub fn without_gate(mut self, name: &'static str) -> Self {
        self.drop_gates.push(name);
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

    /// Wire the deterministic chain. Pure wiring with no failure point: everything
    /// that can fail (migrations, snapshot / knob / proxy row loading) has already
    /// been done on the consumer side.
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

        let Some(egress) = egress else {
            // A data plane without an egress cannot forward; fail loudly at assembly
            // rather than panicking on the first request.
            panic!("facade assembly requires Components::egress");
        };

        // Egress proxy pool: the consumer has already loaded the enabled DB
        // proxy_nodes rows into a snapshot; they are installed here, and the
        // in-place reload for admin console CRUD is wired by the consumer.
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
        let token_budget = TokenBudgetState::new();
        token_budget.set_global_default(u64::from(token_tpm_default));
        let mut dispatcher = Dispatcher::new(
            Some(Arc::new(state.dispatch.clone())),
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

        // Admission chain: preset, or the consumer's own composition.
        let rate_limiter = Arc::new(RateLimiter::new(
            ratelimit_max_requests,
            ratelimit_window_seconds,
        ));
        let graylist = Arc::new(ArcSwap::from_pointee(GrayListState::default()));
        let parts = GateParts {
            rate_limiter: rate_limiter.clone(),
            graylist: graylist.clone(),
        };
        let mut gates = match chain {
            Some(chain) => chain,
            None => shape.gate_chain(&state, quota_gate, &parts),
        };
        for name in &drop_gates {
            gates.remove(name);
        }
        for gate in &extra_gates {
            gates.push_gate_shared(Arc::clone(gate));
        }
        // The prehold ledger is shared by the quota gate (admission), the chain's
        // release-on-later-rejection path, and ForwardStage (settle / release at
        // the settlement commit points) — mounted here regardless of who built
        // the chain.
        if let Some(l) = &ledger {
            gates = gates.with_ledger(Arc::clone(l));
        }

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
            Some(Settle { price_table, sink }) => forward_stage.with_price_table(price_table, sink),
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
        for stage in pre_forward {
            pipeline = pipeline.push_shared(stage);
        }
        let mut pipeline = pipeline
            .push(
                forward_stage
                    .with_timeouts(timeouts)
                    .with_retry_handle(dispatcher.clone(), retry_handle.clone())
                    // Global concurrency gate: the overall cap.
                    .with_concurrency(global_max_concurrency as usize)
                    // Per-channel token budget: shares the same TokenBudgetState
                    // with the Dispatcher.
                    .with_token_budget(token_budget.clone())
                    // Per-channel concurrency gate: shares the state with the
                    // Dispatcher so P2C can read the real available slot count.
                    // Streaming resilience Window phase (buffer before the first
                    // byte + switch to a candidate retry on stream break).
                    .with_channel_concurrency(concurrency.clone())
                    .with_stream_resilience(Some(StreamResilience::default())),
            )
            .push(ProtocolBridgeStage::new(adaptors));
        for stage in post {
            pipeline = pipeline.push_shared(stage);
        }
        // The prehold ledger is shared with the admission track (QuotaGate) and
        // the gate chain (release on later-gate rejection): the pipeline releases
        // a still-outstanding hold exactly once on any terminal outcome where no
        // commit point settled it.
        let pipeline = Arc::new(pipeline.with_ledger(ledger));

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
        // the AuthGate.
        let router = build_router(pipeline.clone()).layer(axum::middleware::from_fn(
            crate::gateway::gateway_path_guard,
        ));

        Gateway {
            dispatcher,
            health,
            retry_handle,
            knobs,
            pipeline,
            router,
        }
    }
}
