//! Channel records — upstream provider access points.
//!
//! References: new-api `internal/catalog/store_channel.go` (multi-key/settings),
//! sub2api ent/schema/account.go (dispatch metadata), domain one of 07-database-schema.md.

use super::SyncMeta;
use serde::{Deserialize, Serialize};

/// Upstream channel: an external provider access point that dispatch can select.
///
/// Deliberate difference: multiple keys are split into `keys: Vec<ChannelKey>`, and
/// dispatch granularity is the key rather than the channel (new-api's big channel table
/// keeps the keys in one column; here they are explicit).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelRecord {
    pub meta: SyncMeta,
    pub name: String,
    /// Provider protocol family: "openai" | "claude" | "gemini" | "passthrough"
    /// (lowercase). Corresponds to the `gateway_protocol_bridge::Protocol` whitelist.
    pub provider_type: String,
    pub base_url: String,
    /// One set of upstream credentials usable by this channel (`key_index` is the index
    /// into this Vec; dispatch selects by index).
    pub keys: Vec<ChannelKey>,
    /// Channel-wide maximum concurrency (capacity source for the edge-local Semaphore).
    pub max_concurrency: u32,
    /// 1 enabled / 2 manually disabled / 3 auto circuit-broken (driven by health
    /// observations and decided by the center's aggregation)
    pub status: u8,
    /// Groups under which this channel is visible/usable; corresponds to `GroupRecord::id`.
    pub groups: Vec<String>,
    /// Channel-level overrides: timeout/header overrides/parameter overrides/system prompt.
    /// The structure is deferred (TODO(ferrite-#206, re-anchored: #15)); for now this keeps JSONB semantics.
    pub settings: serde_json::Value,
}

/// One upstream credential. Encryption is the store layer's responsibility; the contract
/// only cares about the logical structure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelKey {
    /// Key index within the channel (stable; deleting a middle key must not renumber,
    /// otherwise route snapshots go stale).
    pub index: u32,
    /// Ciphertext or plaintext, decided by the store layer; the contract layer does not
    /// parse it.
    pub secret: String,
    /// Per-key rate limit (RPM), 0 = unlimited. TODO(ferrite-#211, re-anchored: #13): confirm whether per-key RPM
    /// is needed.
    pub rpm_limit: u32,
}
