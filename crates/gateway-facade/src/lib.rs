//! # gateway-facade — the one crate a consumer depends on
//!
//! Assembly surface for the gateway data plane, in three responsibilities plus a
//! prelude-style re-export of everything a consumer needs to name:
//!
//! | Responsibility | Module | What it owns |
//! |---|---|---|
//! | **config** | [`config`] | [`Config`] (boot values), [`Components`] (injected runtime), [`RuntimeKnobs`] (runtime-mutable subset + `epoch`) |
//! | **builder** | [`builder`] | [`GatewayBuilder`]: preset choice, gate composition, stage insertion points, `build()` wiring |
//! | **preset** | [`preset`] | [`Shape`]: named admission chains (`Relay` / `NoBilling` / `Open`) |
//! | **gateway** | [`gateway`] | [`Gateway`]: the assembled plane and its four exits |
//!
//! ## Ownership boundary (the answer to "what belongs here")
//!
//! - **owns**: assembly *policy* — chain order, which instances must be shared,
//!   the input/output contracts, the insertion points, the knob surface.
//! - **does not own**: any request-path logic (that is the engines), any data
//!   loading (that is the consumer), any process shape (this crate does not know
//!   whether it is embedded in a server or a daemon).
//! - **does not leak**: consumers name nothing but `gateway_facade::…`; the
//!   whole vocabulary is re-exported here (see below), and the engine crates
//!   are implementation details.
//!
//! ## The chain
//!
//! ```text
//! ┌ consumer gates ──┐   ┌ consumer stages ──┐   ┌ consumer stages ──┐
//! │ (preset chain +  │   │ (with_stage_      │   │ (with_stage_after │
//! │  with_gate)      │   │  before_forward)  │   │  _chain)          │
//! └────────┬─────────┘   └─────────┬──────────┘   └─────────┬─────────┘
//!          │                       │                        │
//!     GateChain ────────────► ForwardStage ────────► ProtocolBridgeStage ──► …
//! ```
//!
//! Route selection is **inside** the forwarding stage: with the retry handle
//! wired the ForwardStage drives selection itself through the shared
//! Dispatcher, so no separate `DispatchStage` sits in the default chain.
//!
//! ## One entry, no side doors
//!
//! ```ignore
//! use gateway_facade::*;   // vocabulary + engines + assembly, nothing else
//! ```

pub mod builder;
pub mod config;
pub mod gateway;
pub mod preset;

// ── prelude: the whole vocabulary, re-exported ──────────────────────────────
pub use gateway_core::egress::Timeouts;
pub use gateway_core::error::{GatewayErrorBody, GatewayErrorDetail, code as error_code};
pub use gateway_core::gate::{
    GroupEntry, GroupSnapshot, PriceRow, PricingSnapshot, QuotaSnapshot, SharedGroupSnapshot,
    SharedPricing, SharedQuota, SharedTokenSnapshot, SharedUserSnapshot, TokenEntry, TokenSnapshot,
    UserSnapshot, estimate_cost,
};
pub use gateway_core::health::{
    ChannelOutcome, DEFAULT_SCORE, HealthSetting, HealthState, TPS_ALPHA_ESTIMATED,
    TPS_ALPHA_MEASURED, TpsSample,
};
pub use gateway_core::*;

// ── assembly surface ────────────────────────────────────────────────────────
pub use builder::GatewayBuilder;
pub use config::{
    BootConfig, Components, Config, ConfigDraft, ConfigError, Context, EffectiveConfig,
    ResponseCacheTuning, RuntimeKnobs, Settle, State,
};
pub use gateway::Gateway;
pub use preset::{GATE_NAMES, GateParts, Shape};

// ── engine surface a consumer may reference without naming an engine crate ──
pub use gateway_cache::{
    CacheKey, CacheStats, CachedResponse, CachedUsage, ResponseCache, TtlCache,
};
pub use gateway_dispatch::{
    COMPLETION_EMA_INIT, DEFAULT_EXPLORE_RATE, DEFAULT_STICKY_BOOST, Dispatch, DispatchError,
    DispatchStage, Dispatcher, FailureScope, LatencyDigestTable, MemoryHealthTable, P2CSelector,
    RateLimitSpec, Selector, StickyEntry, StickyKey, StickyTable, TokenBudgetState, TokenHandle,
    classify_channel_scope, resolve_candidate,
};
pub use gateway_forward::egress::{Egress, ForwardedResponse, ReqwestEgress};
pub use gateway_forward::{ForwardStage, ForwardTask, Forwarded, StreamResilience};
pub use gateway_gate::{
    AuthGate, ConcurrencyGate, ConcurrencyState, Gate, GateChain, GateCtx, Gated, GrayListGate,
    GrayListState, GroupModelGate, InjectSpec, IpPolicy, LimitScope, ModelGate, QuotaGate,
    RateLimitGate, RateLimiter, Rejection, StateGate, TokenInfo, UserInfo, rejection_to_response,
    rewrite_body, sha256,
};
pub use gateway_pipeline::{
    BodyReader, BodySource, PipeStream, Pipeline, RequestCtx, RequestMeta, RouteAttribution,
    SelectedRoute, Stage, StageError, StageOutcome, TokenInfo as PipelineTokenInfo, UpstreamError,
    UpstreamResponse, build_router, error_to_response,
};
pub use gateway_protocol_bridge::{
    AdaptorError, FormatCodec, FormatRegistry, Protocol, ProtocolBridgeStage, StreamEncoder,
    error_mapping::map_error, ir as protocol_ir,
};
pub use gateway_proxy::{
    BasicAuth, Lease, NodeOpts, ParseError, ProbeResult, ProxyAdapter, ProxyManager, ProxyNode,
    ProxyPool, ProxyScheme, adapter_for, probe_node, tcp_metadata, validate_url,
};
pub use gateway_security::{FilterConfig, StreamFilter, WordFilter};
