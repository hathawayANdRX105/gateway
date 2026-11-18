//! Settlement delivery for the standalone shape: the event is logged, not persisted.
//!
//! A product that needs durable usage rows installs its own sink instead.

use gateway_facade::{SettleSink, UsageEvent};

pub(crate) struct LogSettleSink;

impl SettleSink for LogSettleSink {
    fn submit(&self, event: UsageEvent) {
        tracing::info!(
            token = %event.token_key,
            model = %event.public_model,
            prompt_tokens = event.prompt_tokens,
            completion_tokens = event.completion_tokens,
            cost = event.cost,
            status = event.status_code,
            "settled"
        );
    }
}
