//! # records — the data-plane domain vocabulary
//!
//! Storage-agnostic records the engines pass around: channel catalog entries, routing
//! units, identity and the usage event. The consumer's persistence layer maps these to
//! its own tables; nothing here knows a database.
//!
//! ## Submodule map
//!
//! | Submodule | Contents |
//! |---|---|
//! | [`channel`]  | Upstream channels + credentials |
//! | [`routing`]  | Groups + in-memory dispatch units (never persisted) |
//! | [`identity`] | Users + API tokens |
//! | [`usage`]    | Metering event handed to the consumer's settle sink |

pub mod channel;
pub mod identity;
pub mod routing;
pub mod usage;

pub use channel::{ChannelKey, ChannelRecord, ChannelSettings, MAX_CONTEXT_WINDOW};
pub use identity::{TokenRecord, UserRecord};
pub use routing::{GroupRecord, RouteUnitRecord};
pub use usage::UsageEvent;

/// Metadata envelope of every catalog record.
///
/// - `key`: logical primary key (string form, so PG and any edge store encode it uniformly);
/// - `logical_version`: the record's own monotonic version, incremented on every change;
/// - `origin`: the node that first created the record ("center" when the center pushes
///   configuration).
///
/// Record *identity the consumer assigns on persistence* (sync key / schema version /
/// origin / timestamp) belongs to the consumer; this envelope is what the engines read
/// and match on (chiefly `key`).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncMeta {
    pub key: String,
    pub schema_version: u32,
    pub logical_version: u64,
    pub origin: String,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}
