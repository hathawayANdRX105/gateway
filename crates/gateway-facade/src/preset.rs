//! `preset` — named chain shapes a consumer can start from.
//!
//! A preset is a *decision*, not a code path: it fixes which gates compose the
//! admission chain and in what order. Everything else (timeouts, retry budget,
//! mounts) stays in [`crate::config::Config`], and a consumer that wants a
//! different chain either composes it with the builder's `with_gate` family or
//! hands a fully built [`GateChain`] to the builder.
//!
//! | Preset | Gate chain | For |
//! |---|---|---|
//! | [`Shape::Relay`] | auth → state → model → group → quota? → rate limit → gray list | the public relay shape |
//! | [`Shape::NoBilling`] | auth → state → model → group → rate limit → gray list (**quota gate dropped even if injected**) | product faces without a billing chain |
//! | [`Shape::Open`] | model-parse | trusted deployments whose front door does its own auth and allowlist |
//!
//! `Relay` is byte-identical to the historical default chain.

use std::sync::Arc;

use arc_swap::ArcSwap;

use gateway_core::gate::{SharedGroupSnapshot, SharedTokenSnapshot, SharedUserSnapshot};
use gateway_gate::auth::AuthGate;
use gateway_gate::chain::GateChain;
use gateway_gate::graylist::{GrayListGate, GrayListState};
use gateway_gate::model::{GroupModelGate, ModelGate, ModelParseGate};
use gateway_gate::quota::QuotaGate;
use gateway_gate::ratelimit::{RateLimitGate, RateLimiter};
use gateway_gate::snapshot::IpPolicy;
use gateway_gate::state::StateGate;

/// Gate names a consumer may `without_gate` by, kept in sync with the `name()`
/// of each gate the presets install. The builder's removal takes the string,
/// not the type, so no engine type needs naming here.
pub const GATE_NAMES: &[&str] = &[
    "auth",
    "state",
    "model",
    "model-parse",
    "group_model",
    "quota",
    "ratelimit",
    "graylist",
];

/// Runtime state the preset-built gates need beyond the directory snapshots.
///
/// Built by the builder and shared with the runtime knobs, so a retune made
/// through the knobs is visible to the same gate instances the chain runs.
/// Not part of the public surface: a consumer building its own `GateParts`
/// would silently detach the chain from the knob handles.
pub(crate) struct GateParts {
    /// Per-token rate limiter (hot limits via the runtime knobs).
    pub(crate) rate_limiter: Arc<RateLimiter>,
    /// Gray-list state (consumer-owned `ArcSwap`).
    pub(crate) graylist: Arc<ArcSwap<GrayListState>>,
}

/// Admission-chain shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shape {
    /// Full relay chain with billing hooks when the consumer provides them.
    Relay,
    /// Full chain minus the quota gate: an injected quota gate is dropped.
    NoBilling,
    /// Model-parse gate only — the caller fronts authentication itself.
    Open,
}

impl Shape {
    /// Build the admission chain for this shape. The quota gate is inserted
    /// after the group gate and before rate limiting, exactly where the
    /// historical chain had it; `None` = not installed.
    pub(crate) fn gate_chain(
        self,
        tokens: &SharedTokenSnapshot,
        users: &SharedUserSnapshot,
        groups: &SharedGroupSnapshot,
        quota_gate: Option<QuotaGate>,
        parts: &GateParts,
    ) -> GateChain {
        // Open: parse the model and nothing else. Every other stock gate reads a
        // token (allowlist / rate scope / gray-list key), so a token-less chain
        // cannot contain them; the caller's front door owns that policy.
        if self == Shape::Open {
            return GateChain::new().push(ModelParseGate);
        }
        let chain = GateChain::new()
            .push(AuthGate::new(Arc::clone(tokens)))
            .push(StateGate::new(
                Arc::clone(users),
                Arc::new(ArcSwap::from_pointee(IpPolicy::default())),
            ))
            .push(ModelGate)
            .push(GroupModelGate::new(Arc::clone(groups)));
        // Quota sits after the group gate and before rate limiting; only shapes
        // that carry a price table and a settlement chain get one.
        let mut chain = match quota_gate {
            Some(quota) => chain.push(quota),
            None => chain,
        };
        chain = chain
            .push(RateLimitGate::new(Arc::clone(&parts.rate_limiter)))
            .push(GrayListGate::new(Arc::clone(&parts.graylist)));
        chain
    }
}
