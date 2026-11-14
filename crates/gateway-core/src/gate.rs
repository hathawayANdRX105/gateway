//! Gate snapshot vocabulary shared by the admin plane and the admission gates.
//!
//! The gates (`gateway-gate`) own admission decisions; the snapshots they read are
//! built by the admin plane's catalog loader (`api-gateway-state`) and handed over
//! as shared handles. Keeping the snapshot types here is what lets the loader
//! construct token / user / quota / group / pricing snapshots without depending on
//! `gateway-gate`.
//!
//! Stored as `Arc<ArcSwap<T>>` (the `Shared*` aliases): the reload path stores a new
//! value into the **same** instance, and every holder (gates, settlement sinks,
//! reload route) sees the new data without rebuilding a component.

use std::collections::HashMap;
use std::sync::Arc;

use crate::records::{TokenRecord, UserRecord};
use arc_swap::ArcSwap;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};

// ============================================================================
// TokenEntry — gate view of a token record (sha256 index)
// ============================================================================

/// Token view used by the gate: contract record + private `allowed_models`.
#[derive(Debug, Clone)]
pub struct TokenEntry {
    pub record: TokenRecord,
    /// Allowed model names; None = all allowed.
    /// ponytail: this is gate-local state and does not pollute the contract.
    pub allowed_models: Option<Vec<String>>,
}

impl TokenEntry {
    #[must_use]
    pub fn new(record: TokenRecord, allowed_models: Option<Vec<String>>) -> Self {
        Self {
            record,
            allowed_models,
        }
    }
}

// ============================================================================
// TokenSnapshot — sha256(raw_key) → TokenEntry
// ============================================================================

/// Token snapshot (from the snapshot reload).
#[derive(Debug, Clone, Default)]
pub struct TokenSnapshot {
    by_hash: DashMap<[u8; 32], TokenEntry>,
}

impl TokenSnapshot {
    pub fn upsert(&self, hash: [u8; 32], entry: TokenEntry) {
        self.by_hash.insert(hash, entry);
    }

    #[must_use]
    pub fn remove(&self, hash: &[u8; 32]) -> Option<TokenEntry> {
        self.by_hash.remove(hash).map(|(_, v)| v)
    }

    #[must_use]
    pub fn lookup(&self, hash: &[u8; 32]) -> Option<TokenEntry> {
        self.by_hash.get(hash).map(|r| r.clone())
    }
}

// ============================================================================
// UserSnapshot — UserRecord indexed by user_key string
// ============================================================================

/// User snapshot (key = contract `UserRecord.meta.key`, string form).
#[derive(Debug, Clone, Default)]
pub struct UserSnapshot {
    by_key: DashMap<String, UserRecord>,
}

impl UserSnapshot {
    pub fn upsert(&self, rec: UserRecord) {
        let key = rec.meta.key.clone();
        self.by_key.insert(key, rec);
    }

    #[must_use]
    pub fn remove(&self, user_key: &str) -> Option<UserRecord> {
        self.by_key.remove(user_key).map(|(_, v)| v)
    }

    #[must_use]
    pub fn lookup(&self, user_key: &str) -> Option<UserRecord> {
        self.by_key.get(user_key).map(|r| r.clone())
    }
}

// ============================================================================
// PricingSnapshot — (model, group) → PriceRow, with model-only fallback
// ============================================================================

/// Pricing snapshot. Lookup: (model, group) → (model, "default") → None.
///
/// Keyed as model → group → row instead of a `(String, String)` tuple key: a tuple
/// key cannot be borrowed, so every pricing lookup (once per billed request) had to
/// allocate two `String`s just to probe the map. The nested map probes by `&str`.
///
/// All costs are hash-map expectations; `n` = number of price rows.
#[derive(Debug, Clone, Default)]
pub struct PricingSnapshot {
    rows: DashMap<String, DashMap<String, PriceRow>>,
}

impl PricingSnapshot {
    /// Insert or replace one row. Cost: O(1).
    pub fn upsert(&self, model: String, group: String, price: PriceRow) {
        self.rows.entry(model).or_default().insert(group, price);
    }

    /// Remove one row, returning it. Cost: O(1). An emptied model bucket is left
    /// in place (bounded by the number of models, pruned by the next snapshot
    /// rebuild) rather than paying a second lock round per removal.
    #[must_use]
    pub fn remove(&self, model: &str, group: &str) -> Option<PriceRow> {
        self.rows
            .get(model)
            .and_then(|groups| groups.remove(group))
            .map(|(_, row)| row)
    }

    /// Exact `(model, group)` row, falling back to the model's `"default"` bucket.
    /// Cost: O(1), zero allocation (the lookup borrows both keys).
    #[must_use]
    pub fn lookup(&self, model: &str, group: &str) -> Option<PriceRow> {
        self.rows.get(model).and_then(|groups| {
            groups
                .get(group)
                .or_else(|| groups.get("default"))
                .map(|r| r.clone())
        })
    }
}

/// One billing row. Unit: "internal units" per 1M tokens (new-api `500_000` = $1).
///
/// Production fills this in `api_gateway_state::snapshot::build_pricing_snapshot` from
/// the `model_prices` table's raw **$/M** values ×`500_000`, **never put dollar values
/// here directly** — once this conversion was missed, the quota gate's cost
/// estimation returned 0, leaving "balance < estimate → 402" spinning in production.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceRow {
    pub input_per_m: f64,
    pub output_per_m: f64,
    /// cached/read prompt discount (prefix-cache-hit reads).
    pub cache_per_m: f64,
}

/// Estimate cost from the output token limit. Input and cache-hit tokens are excluded
/// for now to avoid double counting.
///
/// ponytail: only `output_per_m × max_tokens` is estimated; the real input/cache
/// consumption is written back to the balance by the metering layer once it settles.
///
/// Unit convention: `output_per_m` is internal units per million (`$1/500_000 =
/// 500_000` internal units/M), `max_tokens` is a token count, and dividing by 1e6
/// yields internal units. `ceil` rounds up to whole units (a zero `output_per_m` or a
/// zero `max_tokens` still yields `0`), matching the metering layer's round-up.
#[must_use]
#[allow(clippy::cast_possible_truncation)]
pub fn estimate_cost(price: &PriceRow, max_tokens: u32) -> i64 {
    // `ceil` keeps the value non-negative and bounded by the price row, so the
    // f64 -> i64 cast cannot saturate for any realistic price/token pair.
    (price.output_per_m * f64::from(max_tokens) / 1_000_000.0).ceil() as i64
}

// ============================================================================
// QuotaSnapshot — token_key → remaining quota (i64, internal units)
// ============================================================================

/// Quota snapshot (key = `TokenRecord.meta.key`, string).
///
/// ponytail: a single balance table keyed by token; split by user dimension when needed.
#[derive(Debug, Clone, Default)]
pub struct QuotaSnapshot {
    by_token: DashMap<String, i64>,
}

impl QuotaSnapshot {
    pub fn upsert(&self, token_key: String, remaining: i64) {
        self.by_token.insert(token_key, remaining);
    }

    pub fn add(&self, token_key: &str, delta: i64) {
        let mut e = self.by_token.entry(token_key.to_string()).or_insert(0);
        *e = e.saturating_add(delta);
    }

    #[must_use]
    pub fn remove(&self, token_key: &str) -> Option<i64> {
        self.by_token.remove(token_key).map(|(_, v)| v)
    }

    /// Token remaining quota; missing → 0 (conservative rejection = treated as insufficient).
    #[must_use]
    pub fn remaining(&self, token_key: &str) -> i64 {
        self.by_token.get(token_key).map_or(0, |r| *r.value())
    }
    /// Iterate `(token_key, remaining)` pairs: used by `MemoryLedger::reseed` to
    /// overwrite balances from a fresh snapshot while preserving in-flight holds.
    pub fn iter(&self) -> impl Iterator<Item = (String, i64)> + '_ {
        self.by_token
            .iter()
            .map(|entry| (entry.key().clone(), *entry.value()))
    }
}

// ============================================================================
// GroupSnapshot — group name → GroupEntry (group-level model allowlist + multiplier)
// ============================================================================

/// Configuration of one group: group-level model allowlist and group multiplier (the
/// group semantics of `api_groups`).
#[derive(Debug, Clone)]
pub struct GroupEntry {
    /// Model names allowed for this group, supporting the same trailing-`*` wildcard
    /// as the gate's model matcher; empty = no model restriction for this group.
    pub allowed_models: Vec<String>,
    /// Group multiplier, default 1.0 (neutral value, no billing amplification).
    pub multiplier: f64,
    /// Admin-console enable bit (corresponds to `api_groups.status = 1`);
    /// false = disabled in the admin console, and the gate rejects every request of this
    /// group (see [`GroupSnapshot::is_disabled`]).
    pub enabled: bool,
}

impl Default for GroupEntry {
    fn default() -> Self {
        Self {
            allowed_models: vec![],
            multiplier: 1.0,
            // Enabled by default: the default group seed and every construction path that
            // does not explicitly disable use enabled semantics.
            enabled: true,
        }
    }
}

/// Group snapshot (group name → [`GroupEntry`]).
///
/// Unlike the other snapshots (`DashMap` interior mutability, incremental `&self` upsert),
/// groups are low-frequency config pushed wholesale by sync, so this uses a `HashMap` with
/// `&mut self` upsert: the reload path builds a fresh snapshot and stores it wholesale,
/// which lets `allowed_models` return `&[String]` without copying.
#[derive(Debug, Clone, Default)]
pub struct GroupSnapshot {
    by_name: HashMap<String, GroupEntry>,
}

impl GroupSnapshot {
    /// Insert or overwrite one group's config.
    ///
    /// The multiplier is a financial multiplier; illegal values (≤0 / NaN / inf) are
    /// validated on the control-plane write side, and this is the second guard so dirty
    /// data cannot reach the snapshot through sync and pollute billing — an illegal value
    /// falls back to the neutral 1.0.
    pub fn upsert(&mut self, name: String, mut entry: GroupEntry) {
        if !entry.multiplier.is_finite() || entry.multiplier <= 0.0 {
            tracing::warn!(
                group = %name,
                multiplier = entry.multiplier,
                "GroupEntry.multiplier invalid, falling back to 1.0"
            );
            entry.multiplier = 1.0;
        }
        self.by_name.insert(name, entry);
    }

    /// Model allowlist for a group; group missing (unconfigured) → None.
    #[must_use]
    pub fn allowed_models(&self, group: &str) -> Option<&[String]> {
        self.by_name.get(group).map(|e| e.allowed_models.as_slice())
    }

    /// Group multiplier; group missing → 1.0 (neutral fallback).
    #[must_use]
    pub fn multiplier(&self, group: &str) -> f64 {
        self.by_name.get(group).map_or(1.0, |e| e.multiplier)
    }

    /// Whether the group is explicitly disabled by the admin console (present in the
    /// snapshot and `enabled = false`).
    /// Unknown groups return false — "unconfigured" is not blocked, fail-open semantics
    /// are unchanged.
    #[must_use]
    pub fn is_disabled(&self, group: &str) -> bool {
        self.by_name.get(group).is_some_and(|e| !e.enabled)
    }
}

// ============================================================================
// ArcSwap handle aliases
// ============================================================================

pub type SharedTokenSnapshot = Arc<ArcSwap<TokenSnapshot>>;
pub type SharedUserSnapshot = Arc<ArcSwap<UserSnapshot>>;
pub type SharedPricing = Arc<ArcSwap<PricingSnapshot>>;
pub type SharedQuota = Arc<ArcSwap<QuotaSnapshot>>;
pub type SharedGroupSnapshot = Arc<ArcSwap<GroupSnapshot>>;
