//! Candidate artifact — the complete executable target of a routing decision.

use gateway_core::records::{ChannelRecord, RouteUnitRecord};

/// Route-unit enable status — defined on the injection surface (both planes test it).
pub use gateway_core::dispatch::STATUS_ENABLED;

/// The final output of dispatch: one concrete target that can be forwarded.
///
/// The authoritative definition lives in `gateway_pipeline::SelectedRoute` — the routing
/// product travels across stages through `RequestCtx`, and pipeline is the shared base of every
/// stage crate, so the dependency cannot point back to dispatch.
/// This alias keeps the semantic name `gateway_dispatch::Candidate`; both are the same type.
pub type Candidate = gateway_pipeline::SelectedRoute;

/// Route unit + channel snapshot → a complete candidate.
///
/// Credentials come from the key in `channel.keys` whose `index == unit.key_index`
/// (new-api `GetNextEnabledKeyForIndex` semantics: the key is resolved by index, never
/// rotated).
/// A missing or out-of-range key → None, which the upper layer treats as NoCandidate.
///
/// `provider_type` and `settings` are copied verbatim from `channel`: resolved once during
/// routing, so forward does not have to go back to the catalog snapshot for the protocol family
/// and channel override headers.
pub fn resolve_candidate(unit: &RouteUnitRecord, channel: &ChannelRecord) -> Option<Candidate> {
    let key = channel.keys.iter().find(|k| k.index == unit.key_index)?;
    Some(Candidate {
        unit: unit.clone(),
        secret: key.secret.clone(),
        base_url: channel.base_url.clone(),
        upstream_model: unit.upstream_model.clone(),
        provider_type: channel.provider_type.clone(),
        settings: channel.settings.clone(),
    })
}
