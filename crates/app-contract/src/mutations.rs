//! # mutations — center ↔ edge incremental sync contract
//!
//! Synchronization mechanism replacing Redis; data flow:
//!
//! ```text
//! edge pull:  POST /internal/sync/delta { my_summary } → DeltaResponse
//! edge push:  POST /internal/sync/push  { mutations }  → AckResponse
//! ```
//!
//! Correctness rules (from the 02-b roadmap, D3/D4):
//! 1. The edge reports a version summary → the center returns only the missing ranges; a
//!    gap/incompatibility → full-snapshot fallback;
//! 2. Duplicate delivery of a MutationId is idempotent (the center deduplicates by id);
//! 3. The edge advances its local cursor only after receiving an Ack;
//! 4. Notifications (LISTEN/NOTIFY) are only an accelerator; polling is the correctness path.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Globally unique mutation id, UUIDv7 (time-ordered → naturally scannable by time range over
/// the WAL).
pub type MutationId = Uuid;

/// Sync domain partition: each domain has its own version number and does not block the others.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SyncDomain {
    /// Configuration: channel / route-unit / group (center-authoritative, edge read-only).
    Catalog,
    /// Identity: user / token (center-authoritative, edge holds a read-only snapshot).
    Identity,
    /// Usage: usage-event / health (authoritatively produced at the edge, aggregated at the
    /// center).
    Usage,
}

/// Change payload: upsert one record, or delete one key.
/// The record type matches the domain — Catalog only ever carries Channel/Group/RouteUnit, and
/// so on.
/// TODO(#230): the center should reject and alert on payloads that do not match the domain
/// (validation belongs in the sync layer).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum MutationPayload {
    UpsertChannel {
        record: crate::records::ChannelRecord,
    },
    UpsertGroup {
        record: crate::records::GroupRecord,
    },
    UpsertRouteUnit {
        record: crate::records::RouteUnitRecord,
    },
    UpsertUser {
        record: crate::records::UserRecord,
    },
    UpsertToken {
        record: crate::records::TokenRecord,
    },
    /// Usage/health do not use upsert semantics — they are append-only events that carry the
    /// record directly.
    UsageEvent {
        record: crate::records::UsageEventRecord,
    },
    HealthObservation {
        record: crate::records::HealthObservationRecord,
    },
    /// Soft delete: carries only the key; the sync layer is responsible for the tombstone
    /// encoding of each store.
    Delete {
        key: String,
    },
}

/// One change = the smallest unit of idempotent delivery.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Mutation {
    pub id: MutationId,
    pub domain: SyncDomain,
    /// Originating node identifier ("center" / edge node id).
    pub origin: String,
    /// Creation time (the v7 UUID already carries a timestamp, but a separate field makes log
    /// searches easier).
    pub created_at: i64,
    pub payload: MutationPayload,
}

/// Version summary: carried by the edge on every handshake, answering "which version have I
/// applied".
/// The key is the domain name; the value is the last revision applied in that domain.
pub type VersionSummary = BTreeMap<SyncDomain, u64>;

/// The center's reply to a pull: the incremental range for each domain.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeltaRange {
    /// Latest revision currently held by the center for the domain.
    pub latest: u64,
    /// Changes the edge should apply (empty = already up to date).
    /// None = gap/too far behind; fall back to a snapshot (see below).
    pub mutations: Option<Vec<Mutation>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeltaResponse {
    pub catalog: DeltaRange,
    pub identity: DeltaRange,
    /// The Usage domain flows edge→center, so in a pull reply it is always latest=0.
    pub usage: DeltaRange,
    /// Non-empty when any domain has a gap: a full compacted snapshot of that domain
    /// (serde_json bytes).
    /// TODO(#231): snapshot payload encoding — plain `Vec<Record>` JSON or zstd? JSON for now;
    /// compression can be added later.
    pub snapshot: Option<SnapshotEnvelope>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotEnvelope {
    pub domain: SyncDomain,
    pub revision: u64,
    /// JSON-serialized record array; after decoding, the store layer atomically replaces the
    /// local snapshot.
    pub records: Vec<serde_json::Value>,
}

/// Reply to an edge push batch: the center ACKs one by one.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AckResponse {
    /// MutationIds successfully persisted; after receiving them the edge advances its cursor and
    /// prunes the WAL.
    pub acked: Vec<MutationId>,
    /// Rejected entries and the reason (schema incompatibility, etc.); the edge moves them to a
    /// dead-letter queue instead of retrying forever.
    /// TODO(#232): dead-letter strategy — drop after N retries? Emit an alert? Undecided.
    pub rejected: Vec<RejectedMutation>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RejectedMutation {
    pub id: MutationId,
    pub reason: String,
}
