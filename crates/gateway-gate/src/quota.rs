//! `quota` — gate 3: account balance vs. estimated cost (pre-consume semantics)

use std::sync::Arc;

use arc_swap::ArcSwap;
use async_trait::async_trait;

use super::chain::{Gate, GateCtx};
use super::error::Rejection;
use super::snapshot::{PricingSnapshot, QuotaSnapshot};
use gateway_metering::ledger::Ledger;

/// Cost estimation from the output token limit — defined on the injection surface next
/// to [`gateway_core::gate::PriceRow`], re-exported so
/// `gateway_gate::quota::estimate_cost` keeps resolving.
pub use gateway_core::gate::estimate_cost;

pub struct QuotaGate {
    quotas: Arc<ArcSwap<QuotaSnapshot>>,
    pricing: Arc<ArcSwap<PricingSnapshot>>,
    /// Admission ledger: preholds the estimated cost at admission so concurrent requests
    /// cannot all observe the same `remaining` and overdraft the balance.
    ledger: Arc<dyn Ledger>,
}
impl QuotaGate {
    pub fn new(
        quotas: Arc<ArcSwap<QuotaSnapshot>>,
        pricing: Arc<ArcSwap<PricingSnapshot>>,
        ledger: Arc<dyn Ledger>,
    ) -> Self {
        Self {
            quotas,
            pricing,
            ledger,
        }
    }
}

#[async_trait]
impl Gate for QuotaGate {
    fn name(&self) -> &'static str {
        "quota"
    }

    async fn check(&self, ctx: &mut GateCtx) -> Result<(), Rejection> {
        // 0. Parse the body for model + max_tokens when not parsed yet, then read token
        //    and model
        ctx.ensure_model_parsed()?;
        let (model, token) = ctx.model_and_token()?;

        // 1. Look up the balance by token identity key; the key is the TokenInfo.id ==
        //    meta.key UUID string
        let remaining = self.quotas.load().remaining(&token.id);
        if remaining <= 0 {
            return Err(Rejection::InsufficientQuota { remaining, cost: 0 });
        }

        // 2. Estimate the cost
        // Note: "default" here is the **pricing fallback bucket name**, not the "default
        // group": `PricingSnapshot::lookup` falls back to `(model, "default")` when the
        // `(model, group)` row is absent, and production currently inserts every row
        // under that bucket (see `api_gateway_state::snapshot::build_pricing_snapshot`),
        // so renaming the default group does not affect this lookup.
        let max_tokens = ctx.requested_max_tokens.unwrap_or(4096);
        let group = ctx.group.as_deref().unwrap_or("default");
        let cost = self
            .pricing
            .load()
            .lookup(model, group)
            .map(|p| estimate_cost(&p, max_tokens))
            .unwrap_or(0);

        // Balance < cost → reject (fast filter; the prehold below is authoritative).
        if remaining < cost {
            return Err(Rejection::InsufficientQuota { remaining, cost });
        }

        // Atomically reserve the estimated cost: the snapshot read above is only a fast
        // filter — concurrent requests can all observe the same `remaining`. prehold
        // deducts under the ledger lock, so the first request to prehold wins and the
        // rest observe the drained balance (this is what closes the concurrent-overdraft
        // hole).
        let hold = self
            .ledger
            .prehold(&token.user_key, &token.id, cost)
            .map_err(|err| Rejection::InsufficientQuota {
                remaining: err.have,
                cost: err.need,
            })?;

        ctx.estimated_cost = Some(cost);
        ctx.hold = Some(hold);
        Ok(())
    }
}
