//! `metering` — settlement vocabulary shared by the admin plane and the metering engine.
//!
//! The engine (`gateway-metering`) owns preholds, stream scanning, and settlement
//! computation. What lives here is what the two sides agree on:
//!
//! - Pricing vocabulary — [`ModelPrice`] and the [`PriceTable`] port the billing
//!   domain implements over its `model_prices` table. The price *arithmetic*
//!   (`gateway_metering::pricing::price_of`) stays with the engine: it is settlement
//!   behavior, not vocabulary.
//! - [`SettleSink`] — the delivery channel for settlement events. The library layer
//!   produces `UsageEvent`s and knows no storage; the apps side implements this
//!   trait to persist them.

use crate::records::UsageEvent;

/// Price of a single model.
///
/// Deserializable: the engine's config-backed table is built from deserialized values of
/// this type, so callers do not need to redefine a struct carrying the same fields.
#[derive(Debug, Clone, Copy, serde::Deserialize)]
pub struct ModelPrice {
    /// $/M input tokens.
    pub input: f64,
    /// $/M output tokens.
    pub output: f64,
    /// $/M cached-read tokens.
    pub cache: f64,
    /// Group multiplier (GroupRecord.rate_multiplier).
    #[serde(default = "default_group_multiplier")]
    pub group_multiplier: f64,
}

fn default_group_multiplier() -> f64 {
    1.0
}

/// Price-table trait — a projection of the catalog snapshot.
pub trait PriceTable: Send + Sync {
    /// Look up the price for `(model, group)`.
    ///
    /// `group` is the request group name (e.g. "default" / "vip"); implementations may
    /// return a different price per group. [`ModelPrice::group_multiplier`] in the returned
    /// value participates in the settlement arithmetic; the **authoritative request-side
    /// source** of the group multiplier (GroupRecord.rate_multiplier) is passed by the
    /// caller as `group_ratio`, not through this table.
    fn lookup(&self, model: &str, group: &str) -> Option<ModelPrice>;
}

/// Prehold credential — the `hold_id` is issued by the metering engine on admission
/// and travels with the response until it is settled or released.
#[derive(Debug, Clone)]
pub struct Hold {
    pub id: u64,
    /// Preheld amount (internal billing units).
    pub amount: i64,
    pub user_key: String,
    pub token_key: String,
}

/// Insufficient balance for a prehold.
#[derive(Debug, thiserror::Error)]
#[error("insufficient balance: need {need}, have {have}")]
pub struct Insufficient {
    pub need: i64,
    pub have: i64,
}

/// Admission ledger port: prehold / settle / release must be atomic (the in-memory
/// implementation guards the balance map with one mutex).
///
/// - `prehold` reserves the estimated cost at admission; insufficient balance →
///   [`Insufficient`].
/// - `settle` reconciles the reservation with the metered actual cost: refunds the
///   difference (positive delta) or charges the extra (negative delta); returns the
///   net difference (negative = the user was charged extra).
/// - `release` refunds the full preheld amount when a request fails after admission
///   and before settlement; a hold is settled or released exactly once.
/// - `reseed` overwrites balances from a fresh quota snapshot on reload while
///   keeping in-flight holds, so reservations taken before the reload are not lost.
pub trait Ledger: Send + Sync {
    /// Prehold: reserve `estimated` for `(user_key, token_key)`; insufficient →
    /// [`Insufficient`].
    fn prehold(
        &self,
        user_key: &str,
        token_key: &str,
        estimated: i64,
    ) -> Result<Hold, Insufficient>;
    /// Settle: refund the difference (estimated - actual); charge extra when
    /// actual > estimated. Returns the net difference.
    fn settle(&self, hold: &Hold, actual: i64) -> i64;
    /// Release the full preheld amount.
    fn release(&self, hold: &Hold);
    /// Overwrite balances from a fresh quota snapshot, keeping in-flight holds.
    /// Used by the snapshot reload path so in-flight reservations are preserved.
    fn reseed(&self, quotas: &crate::gate::QuotaSnapshot);
}

/// Settle-artifact delivery channel: implemented on the apps side (write usage_logs / debit
/// the in-memory quota).
///
/// The library layer only produces events and knows no storage. Implementations must be
/// **non-blocking or fast-returning**: `submit` is called synchronously on the hot path
/// (stream end / response commit point), and a slow implementation would stall the forward
/// pipeline — spawn heavy work (persisting, pushing) to the background yourself.
pub trait SettleSink: Send + Sync {
    /// Submit one settle event. Record identity (sync key / version / origin / time)
    /// is assigned by the implementation when it wraps and persists the event; idempotency
    /// belongs to that consumer (dedupe on the assigned key, never re-book).
    fn submit(&self, event: UsageEvent);
}
