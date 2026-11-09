//! Pricing — TokenCounts → cost (internal units).
//!
//! V1: static multiplier table (contract catalog snapshot); V2: the new-api
//! `price_expression` engine (versioned compile / AST / frozen snapshot / request rules).
//!
//! Price source: model prices from the catalog snapshot (input/output/cache columns,
//! in $/M tokens).
//!
//! The price **vocabulary** — [`ModelPrice`] and the [`PriceTable`] port — lives in
//! `gateway-contract::metering` and is re-exported here, because the billing crate
//! implements that port over its own table and must not link this engine to do so. The
//! arithmetic below ([`price_of`], which needs [`crate::scanner::TokenCounts`]) stays with
//! the engine: it is settlement behavior, not a value the two sides exchange.

use std::collections::HashMap;

pub use gateway_core::metering::{ModelPrice, PriceTable};

/// Settlement price computation.
///
/// Internal unit conversion: 500_000 units = $1 (new-api semantics), i.e.
/// cost = (in+out+cache dollars) × price.group_multiplier × group_ratio × 500_000,
/// rounded up (`.ceil()`: non-zero usage costs at least 1 unit, zero usage stays 0).
///
/// `group_ratio` is the request group multiplier (GroupRecord.rate_multiplier), default
/// 1.0; it multiplies the table's `group_multiplier`, the two multiplier levels keeping
/// independent semantics.
///
/// Cache accounting follows the vendor's shape ([`TokenCounts::prompt_includes_cache`]):
/// OpenAI/Gemini report cached tokens **inside** `prompt`, so the cached portion is
/// removed from the input charge and priced once at `price.cache`; Claude reports them
/// outside `prompt`, so `prompt` is already the uncached input. Without this the OpenAI
/// and Gemini cache hits were charged twice (input price + cache price).
/// Cache *writes* stay inside the input charge: the price table has no write column.
pub fn price_of(counts: crate::scanner::TokenCounts, price: &ModelPrice, group_ratio: f64) -> i64 {
    let billable_input = if counts.prompt_includes_cache {
        counts.prompt.saturating_sub(counts.cached)
    } else {
        counts.prompt
    };
    let input_cost = billable_input as f64 * price.input / 1e6;
    let output_cost = counts.completion as f64 * price.output / 1e6;
    let cache_cost = counts.cached as f64 * price.cache / 1e6;
    let total_dollars =
        (input_cost + output_cost + cache_cost) * price.group_multiplier * group_ratio;
    let raw = total_dollars * 500_000.0;
    // Billing rounds up, but floating-point noise can push a sum that should be an integer
    // (e.g. 2250.0) to 2250.0000000002, and a plain ceil would overcharge 1 unit per
    // event. Snap to the nearest integer within a tiny tolerance first, then ceil the real
    // fractional part.
    let snapped = if (raw - raw.round()).abs() < 1e-6 {
        raw.round()
    } else {
        raw.ceil()
    };
    snapped as i64
}
/// Price-table implementation backed by configuration.
#[derive(Debug, Clone)]
pub struct ConfigPriceTable {
    prices: HashMap<String, ModelPrice>,
}

impl ConfigPriceTable {
    pub fn new(prices: HashMap<String, ModelPrice>) -> Self {
        Self { prices }
    }
}

impl PriceTable for ConfigPriceTable {
    /// The config table matches on model only; the authoritative group multiplier
    /// (`group_ratio`) is supplied by the caller at settle time, so this implementation
    /// ignores the `group` argument.
    fn lookup(&self, model: &str, _group: &str) -> Option<ModelPrice> {
        self.prices.get(model).copied()
    }
}
