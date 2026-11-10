//! Routing records — groups and dispatch units.
//!
//! `RouteUnitRecord` is an in-memory dispatch unit (never persisted): the snapshot loader in
//! `crates/api-gateway-state` expands `api_channels.models` JSONB across each channel's groups
//! (`channel_groups`) and keys. It keeps one-api abilities' "indexes are rebuilt when a channel
//! changes" semantics, but rebuilding happens in memory rather than in a materialized table (the
//! `route_units` table was dropped in migration 0006).

use super::SyncMeta;
use serde::{Deserialize, Serialize};

/// User group: carries the rate multiplier and the visible models.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupRecord {
    pub meta: SyncMeta,
    pub id: String,
    pub display_name: String,
    /// Global multiplier applied when billing users in this group (matches new-api's group ratio).
    pub rate_multiplier: f64,
    /// Whitelist of public model aliases this group may access; empty = all visible.
    pub allowed_models: Vec<String>,
}

/// Routing dispatch unit — the smallest selection object in the dispatch state machine.
///
/// Decouples new-api's "one large channel table mixes multiple models and keys" problem:
/// the (channel, key, upstream model) triple is explicit, and the public-alias → upstream-name
/// mapping lives here.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RouteUnitRecord {
    pub meta: SyncMeta,
    pub group: String,
    /// Model name requested by the client (public alias), e.g. "gpt-4o".
    pub public_model: String,
    pub channel_key: String, // points to ChannelRecord::meta.key
    /// Index into `ChannelRecord::keys`.
    pub key_index: u32,
    /// Model name actually sent upstream, e.g. "gpt-4o-2024-08-06".
    pub upstream_model: String,
    /// Sampling prior = this field / the sum of candidate weights for the same model; 0 = zero
    /// baseline share, leaving only explore observation traffic.
    pub weight: u32,
    pub status: u8,
}
