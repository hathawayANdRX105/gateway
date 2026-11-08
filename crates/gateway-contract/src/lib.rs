//! # gateway-contract — injection surface between the admin plane and the data plane
//!
//! The admin plane (`crates/api-*`) and the data plane (`crates/gateway-*`) share
//! two kinds of things and nothing else:
//!
//! - **Vocabulary**: plain data both sides pass around — egress timeouts, the
//!   channel/dispatch snapshot, health records and settings, gate snapshot types.
//!   These types used to live in the implementation crates, which forced every
//!   admin crate that merely *named* one of them to depend on a whole engine.
//! - **Ports**: the few operations the admin plane performs on the running data
//!   plane (read the current catalog snapshot, replace it on reload, read/replace
//!   the health table, read its entries). Each port carries only the methods the
//!   admin side actually calls; the engines keep their own richer traits.
//!
//! Direction is strictly one-way: implementation crates depend on this crate and
//! re-export the types they used to own, so existing `gateway_*::X` paths keep
//! working while the admin plane only ever names `gateway_contract::X`. The reverse
//! direction (`gateway-contract -> gateway-*`) is a forbidden edge in
//! `scripts/layering-policy.toml`: the surface must not grow back into the
//! implementations it hides.
//!
//! Internal dependency discipline: `app-contract` only. Anything else here would
//! drag a domain into both planes' closures.
//!
//! ## Module map
//!
//! | Module | Vocabulary / port | Owner engine |
//! |---|---|---|
//! | [`egress`] | [`egress::Timeouts`] — connect / first-byte / total budgets | `gateway-forward` |
//! | [`dispatch`] | [`dispatch::Snapshot`], [`dispatch::RetryPolicy`], [`dispatch::STATUS_ENABLED`], [`dispatch::DispatcherHandle`] | `gateway-dispatch` |
//! | [`health`] | [`health::HealthState`], [`health::HealthSetting`], [`health::ChannelOutcome`], [`health::TpsSample`], [`health::HealthHandle`] | `gateway-dispatch` |
//! | [`gate`] | token / user / quota / group / pricing snapshots, [`gate::PriceRow`], [`gate::estimate_cost`] | `gateway-gate` |
//! | [`proxy`] | [`proxy::node`] (`ProxyNode` + URL parser), [`proxy::sharelink`], [`proxy::pool`] (`ProxySnapshot`, `NodeStats`, [`proxy::pool::ProxyPoolHandle`]) | `gateway-proxy` |
//! | [`metering`] | [`metering::ModelPrice`], [`metering::PriceTable`], [`metering::SettleSink`] | `gateway-metering` |
//! | [`protocol`] | [`protocol::ProtocolKind`], [`protocol::RequestPath`] — inbound dialect and URL shape | `gateway-pipeline` |

pub mod dispatch;
pub mod egress;
pub mod gate;
pub mod health;
pub mod metering;
pub mod protocol;
pub mod proxy;

pub use dispatch::{DispatcherHandle, RetryPolicy, STATUS_ENABLED, Snapshot};
pub use health::HealthHandle;
pub use metering::{Hold, Insufficient, Ledger, ModelPrice, PriceTable, SettleSink};
pub use protocol::{ProtocolKind, RequestPath};
pub use proxy::{ProxyNode, ProxyPoolHandle, ProxyScheme, ProxySnapshot};
