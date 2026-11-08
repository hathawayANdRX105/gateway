//! Dispatch vocabulary shared by the admin plane and the dispatch engine.
//!
//! The engine (`gateway-dispatch`) owns candidate selection and the live table.
//! What lives here is the data the admin plane reads or feeds:
//!
//! - [`Snapshot`] — the channel / route-unit catalog the engine selects from, built
//!   by `api-gateway-state`, read by the `/api/gateway/health` panel and the
//!   `/v1/models` endpoint, replaced on every reload;
//! - [`RetryPolicy`] — the retry budget read from the options table and hot-swapped
//!   into the forwarding loop;
//! - [`STATUS_ENABLED`] — the route-unit enable bit both sides test;
//! - [`DispatcherHandle`] — the port the admin plane uses to read and replace the
//!   running snapshot without naming the engine type.

use std::collections::HashMap;
use std::sync::Arc;

use app_contract::records::{ChannelRecord, RouteUnitRecord};

/// Route-unit enable status (aligned with ChannelRecord::status: 1 enabled / 2 manually
/// disabled / 3 auto circuit-broken).
/// dispatch selects only units with status == 1; automatic circuit breaking is decided locally
/// by the in-memory health table and does not interfere with the center's aggregated
/// circuit-breaker (status=3).
pub const STATUS_ENABLED: u8 = 1;

/// Channel / route-unit catalog snapshot the dispatcher selects from.
///
/// `units` is the flattened route-unit list (one entry per channel key); `channels`
/// is keyed by `ChannelRecord::meta.key` and carries the forwarding details
/// (base URL, keys, protocol family, settings).
#[derive(Debug, Clone, Default)]
pub struct Snapshot {
    pub units: Vec<RouteUnitRecord>,
    pub channels: HashMap<String, ChannelRecord>,
}

/// Retry policy parameters.
///
/// Controls only the attempt budget; "which status codes are retryable" is decided
/// by the forwarding layer (`NormalizedError.retryable`), so it is not duplicated
/// here. Per-attempt timeout semantics are covered by the egress
/// [`Timeouts`](crate::egress::Timeouts) (options key `gateway.timeout.*`), hence the
/// policy keeps no per-attempt timeout field.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RetryPolicy {
    /// Maximum number of attempts including the first (new-api retry-count semantics;
    /// wildtoken defaults to 1).
    pub max_attempts: u32,
    /// Request-level failover time budget (ms) — S2: accumulates **failed-attempt time** plus
    /// **cooldown-recovery waits** only; successful connection time does **not** count (and
    /// does not clamp egress timeouts; P1: success spends no budget). Not wall clock: gaps a
    /// busy server leaves in scheduling the request do not count (fairness — healthy requests
    /// are not misjudged as 503). `0` = off (legacy behavior). Exhausted without a success →
    /// budget-exhausted error (upper layer: 503 without cooldown / 504 + Retry-After with
    /// cooldown).
    pub total_budget_ms: u64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            total_budget_ms: 45_000,
        }
    }
}

/// Admin-plane port over the running dispatcher.
///
/// Only the operations the admin plane performs: read the current catalog snapshot
/// (health panel join, `/v1/models` route listing) and install a freshly loaded one
/// on reload. Selection itself stays behind the engine's own `Dispatch` trait.
pub trait DispatcherHandle: Send + Sync {
    /// Read-only port to the current snapshot — returns the shared handle (zero-copy,
    /// not a data clone); `None` = catalog not loaded yet.
    fn snapshot(&self) -> Option<Arc<Snapshot>>;

    /// Install a freshly loaded snapshot. The engine re-registers everything derived
    /// from it (per-channel concurrency caps, token budgets) before publishing, so a
    /// reload sees the same registrations a boot would.
    fn set_snapshot(&self, snapshot: Arc<Snapshot>);
}
