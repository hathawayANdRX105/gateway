//! # gateway-facade — the one crate a consumer depends on
//!
//! Assembly surface for the gateway data plane, in four modules plus a
//! prelude-style re-export of everything a consumer needs to name:
//!
//! | Responsibility | Module | What it owns |
//! |---|---|---|
//! | **config** | [`config`] | [`Config`] (boot values), [`Components`] (injected runtime), [`RuntimeKnobs`] (runtime-mutable subset + `epoch`) |
//! | **builder** | [`builder`] | [`GatewayBuilder`]: preset choice, gate composition, stage insertion points, `build()` wiring |
//! | **preset** | [`preset`] | [`Shape`]: named admission chains (`Relay` / `NoBilling` / `Open`) |
//! | **gateway** | [`gateway`] | [`Gateway`]: the assembled plane and its exits (router / pipeline / knobs / graylist / ip policy / handles) |
//!
//! ## Ownership boundary (the answer to "what belongs here")
//!
//! - **owns**: assembly *policy* — chain order, which instances must be shared,
//!   the input/output contracts, the insertion points, the knob surface.
//! - **does not own**: request-path logic (that is the engines — the data-plane
//!   path guard lives with the router integration in `gateway-pipeline`), any
//!   data loading (that is the consumer), any process shape (this crate does not
//!   know whether it is embedded in a server or a daemon).
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
//! wired the `ForwardStage` drives selection itself through the shared
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
pub use preset::{GATE_NAMES, Shape};

// ── engine surface a consumer may reference without naming an engine crate ──
//
// The rule for this list: a name is here when assembling or administering a data
// plane genuinely requires naming it. Engine *internals* (forward task shapes,
// cache tables, selector/health/sticky tables, proxy leases, stage constructors
// the facade owns) are deliberately absent — naming them would let a consumer
// bypass the facade instead of using it.
//
// Vocabulary: the gate snapshot / health / metering / proxy / protocol / error
// groups come from `gateway-core` above.
pub use gateway_forward::egress::{Egress, ForwardedResponse, ReqwestEgress};
// Gate composition material: the trait, the chain, the rejection vocabulary, and
// the stock gates a `with_gates` composition can reuse.
pub use gateway_gate::{
    AuthGate, BLOCK_DURATION, FAIL_STREAK_THRESHOLD, FailEntry, Gate, GateChain, GateCtx, Gated,
    GrayListGate, GrayListState, GroupModelGate, InjectSpec, IpPolicy, LimitScope, ModelGate,
    ModelParseGate, QuotaGate, RateLimitGate, RateLimiter, Rejection, SharedIpPolicy, StateGate,
    TokenInfo as GateTokenInfo, UserInfo, rejection_to_response, rewrite_body, sha256,
};
// Stage material: the trait a custom stage implements, the error vocabulary a
// stage speaks, and the context / attribution types it reads and writes.
pub use gateway_pipeline::{
    BodyReader, BodySource, PipeStream, Pipeline, RequestCtx, RequestMeta, RouteAttribution,
    SelectedRoute, Stage, StageError, StageOutcome, UpstreamError, UpstreamResponse,
    error_to_response,
};
// Egress pool: the manager (consumer builds / reloads it), the URL validator
// (SSRF policy is the consumer's too), and the probe surface the control plane
// uses for node health.
pub use gateway_proxy::{
    BasicAuth, NodeOpts, ParseError, ProbeResult, ProxyManager, ProxyNode, ProxyScheme,
    ShareLinkBatch, VmessShareLink, mask_link, parse_share_link, parse_share_links, probe_node,
    validate_url,
};
// Security configuration (the word list and its switches); the filter instance
// itself is built by the facade from this config.
pub use gateway_security::FilterConfig;
// Model matching used by catalog-facing surfaces the consumer serves.
pub use gateway_gate::model::match_model;
// Price arithmetic lives with the metering engine; the *table* is a port, the
// *arithmetic* the billing domain needs when testing its own wiring.
pub use gateway_metering::pricing::price_of;
// In-memory ledger: the standalone process has no database, so its admission ledger is the
// engine's memory implementation seeded from the catalog's quota snapshot.
pub use gateway_metering::ledger::MemoryLedger;
// Calibration ring: process-level telemetry, not assembly policy. The standalone process
// installs it and serves the report on /readyz, and it is re-exported here so that process
// names no engine crate (see the daemon dependency rule in the root README).
pub use gateway_metering::calibration;
