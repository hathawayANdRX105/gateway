//! # gateway-core — vocabulary and ports of the gateway data plane
//!
//! Single source of truth shared by every consumer of the data plane: the engines pass
//! these types around, the admin plane names them without depending on an engine, and
//! `gateway-facade` re-exports the whole surface so a consumer depends on one crate.
//!
//! Two kinds of things live here and nothing else:
//!
//! - **Vocabulary**: plain data both sides pass around — catalog records, snapshots,
//!   retry policy, timeouts, health and metering vocabulary, proxy nodes, protocol
//!   dialects, usage events, the error-code table.
//! - **Ports**: the few operations the admin plane performs on the running data plane
//!   (ledger, settle sink, price table, dispatch/health handles). Engines keep their
//!   own richer traits; only what a consumer genuinely calls lives here.
//!
//! ## Dependency discipline (hard rule)
//!
//! Only pure data and pure serialization dependencies (serde / `serde_json` / chrono /
//! thiserror / base64 / url / percent-encoding, plus the in-memory shared-state
//! primitives arc-swap / dashmap the snapshot vocabulary is built from). Forbidden:
//! tokio / axum / reqwest / any engine crate — that is what keeps this crate wasm-safe
//! and stops the surface from growing back into implementations.
//!
//! ## Module map
//!
//! | Module | Contents |
//! |---|---|
//! | [`error`] | Cross-surface error-code table + gateway error body + `NormalizedError` |
//! | [`records`] | Catalog / routing / identity / usage records |
//! | [`dispatch`] | [`dispatch::Snapshot`], [`dispatch::RetryPolicy`], [`dispatch::STATUS_ENABLED`], [`dispatch::DispatcherHandle`] |
//! | [`health`] | Health vocabulary: state, settings, outcomes, tps samples, [`health::HealthHandle`] |
//! | [`gate`] | token / user / quota / group / pricing snapshots, [`gate::PriceRow`], [`gate::estimate_cost`] |
//! | [`metering`] | [`metering::ModelPrice`], [`metering::PriceTable`], [`metering::SettleSink`], [`metering::Hold`], [`metering::Ledger`] |
//! | [`egress`] | [`egress::Timeouts`] — connect / first-byte / total budgets |
//! | [`protocol`] | [`protocol::ProtocolKind`], [`protocol::RequestPath`] — inbound dialect and URL shape |
//! | [`proxy`] | [`proxy::node`] (URL/share-link parsers), [`proxy::pool`] (snapshots, node stats, [`proxy::pool::ProxyPoolHandle`]) |

pub mod dispatch;
pub mod egress;
pub mod error;
pub mod gate;
pub mod health;
pub mod metering;
pub mod protocol;
pub mod proxy;
pub mod records;

pub use dispatch::{DispatcherHandle, RetryPolicy, STATUS_ENABLED, Snapshot};
pub use error::{ErrorCode, NormalizedError};
pub use health::HealthHandle;
pub use metering::{Hold, Insufficient, Ledger, ModelPrice, PriceTable, SettleSink};
pub use protocol::{ProtocolKind, RequestPath};
pub use proxy::pool::NodeStats;
pub use proxy::sharelink::{
    ShareLinkBatch, VmessShareLink, mask_link, parse_share_link, parse_share_links,
};
pub use proxy::{ProxyNode, ProxyPoolHandle, ProxyScheme, ProxySnapshot};
pub use records::{
    ChannelKey, ChannelRecord, GroupRecord, RouteUnitRecord, SyncMeta, TokenRecord, UsageEvent,
    UserRecord,
};
