//! # records — domain entities (logical schema)
//!
//! These are the **storage-agnostic** record definitions shared by center/edge/web.
//! Physical encoding (PG rows, Fjall key/value) is always done by the store crate's codec.
//!
//! ## Submodule map
//!
//! | Module | Contents |
//! |------|------|
//! | [`channel`]  | Upstream channels + credentials |
//! | [`routing`]  | Groups + in-memory dispatch units (never persisted) |
//! | [`identity`] | Users + API tokens |
//! | [`usage`]    | Usage events + health observations |
//! | [`billing`]  | Subscription/order/redemption (v2 domain; fields match 07-database-schema.md) |
//!
//! ## Homogeneous record envelope
//!
//! Every record that needs syncing is wrapped in a [`SyncMeta`]: key / schema version /
//! logical version / origin node / time. This is the minimum metadata for center-edge
//! incremental sync (02-b roadmap D0).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub mod billing;
pub mod channel;
pub mod identity;
pub mod routing;
pub mod usage;

pub use billing::{
    CurrencyDefRecord, PaymentOrderRecord, RedeemCodeRecord, SubscriptionPlanRecord,
    UserBalanceRecord, UserSubscriptionRecord,
};
pub use channel::{ChannelKey, ChannelRecord};
pub use identity::{TokenRecord, UserRecord};
pub use routing::{GroupRecord, RouteUnitRecord};
pub use usage::{HealthObservationRecord, UsageEventRecord};

/// Metadata envelope shared by all syncable records.
///
/// - `key`: logical primary key unique within the node (string form, so Fjall and PG
///   encode it uniformly);
/// - `logical_version`: the record's own monotonic version, incremented on every change;
/// - `origin`: the node that first created the record ("center" when the center pushes
///   configuration).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncMeta {
    pub key: String,
    pub schema_version: u32,
    pub logical_version: u64,
    pub origin: String,
    pub updated_at: DateTime<Utc>,
}
