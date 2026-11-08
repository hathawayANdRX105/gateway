//! P4 channel-level body rewriting — inject fills defaults / drop removes fields (skeleton,
//! PR #158).
//!
//! Mirrors the semantics of api-hub `_inject_body` (:257-287): **fields the client sent
//! explicitly take priority; injection only fills defaults**; `drop_fields` lists fields the
//! channel does not support, removed before sending (executed last).
//!
//! Shape difference from api-hub: in the relay, "channel A needs the thinking parameter
//! injected" is a **channel attribute** (it follows
//! [`gateway_pipeline::SelectedRoute`] to the forward mount point
//! `stage.rs::build_task`), not api-hub's global `[inject]` default — the latter is
//! personal-script semantics, see the "not doing" list in todo/gateway-resilience.md. This
//! module provides only the pure rewriting step; the config surface (`inject` /
//! `drop_fields` channel settings) belongs to the contract/apps side and will land
//! separately.

use std::collections::HashMap;

use bytes::Bytes;

/// Channel-level body-rewrite spec (plain data, passed through with the candidate).
#[derive(Debug, Clone, Default)]
pub struct InjectSpec {
    /// Injection fields: fill only keys the client did not send explicitly (client values
    /// always win).
    pub fields: HashMap<String, serde_json::Value>,
    /// Field names the channel does not support; removed before sending, after injection.
    pub drop_fields: Vec<String>,
}

/// Apply [`InjectSpec`] to the upstream request body.
///
/// The intended semantics: a body that is not a JSON object (empty body / malformed JSON /
/// array) is returned unchanged — rewriting is best-effort channel adaptation and must not
/// turn an unparseable request body into a 500. The skeleton currently returns the body
/// unchanged for every input.
pub fn rewrite_body(body: &Bytes, _spec: &InjectSpec) -> Bytes {
    // TODO(#158): inject keys that were not sent explicitly + remove drop_fields; return
    // non-JSON bodies unchanged (main P4 implementation).
    // Skeleton semantics: return the body unchanged, no rewriting (not wired up yet; removes
    // the runtime panic trap of `todo!`).
    body.clone()
}
