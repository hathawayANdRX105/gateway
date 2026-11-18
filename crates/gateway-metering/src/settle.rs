//! Settlement — scan result + pricing → `UsageEvent` (handed to the consumer's settle sink).
//!
//! The `finish_reason` / `usage` extraction for non-streaming bodies now lives in
//! [`crate::usage`] (`extract_finish_reason` / `extract_usage`); this file keeps only
//! `settle_event`.
//!
//! Callers: the settlement points of `gateway_forward` — `stream::finish` on the streaming
//! path and the stage telemetry hooks on the non-streaming success / failure exits. The
//! sink persists the event (and assigns record identity: sync key / version / origin /
//! time); observe aggregates it straight from the storage table, without blocking
//! forwarding.

use gateway_core::records::UsageEvent;

use crate::ledger::Hold;
use crate::pricing::{PriceTable, price_of};
use crate::scanner::TokenCounts;

/// Build one `UsageEvent` that the caller persists through the settle sink.
///
/// `is_stream` is the request's streaming intent (the streaming scan chain passes `true`;
/// the non-streaming commit point and failure-observation events pass the request body's
/// `stream` field); it lands verbatim in [`UsageEvent::is_stream`].
/// `finish_reason` is the upstream stop reason (the non-streaming commit point extracts it
/// from the response body through [`crate::usage::extract_finish_reason`], the streaming scan chain passes
/// the value captured from the last frame, and the failure-observation path passes `None`
/// because it is structurally absent); it lands verbatim in
/// [`UsageEvent::finish_reason`].
/// `group` goes into [`PriceTable::lookup`] together with model (implementations may price
/// per group); `group_ratio` is the request group multiplier (`GroupRecord.rate_multiplier`)
/// multiplied into [`price_of`]; library-level callers that cannot obtain the real group
/// multiplier pass 1.0.
/// Record identity (dedupe key / schema version / origin / timestamp) is NOT assigned
/// here: the sink implementation assigns it when it persists, so uniqueness and
/// de-duplication belong to the event consumer.
// ponytail: the many parameters are independent fields of one usage event; wrapping them
// in a struct would only move the parameters elsewhere.
#[allow(clippy::too_many_arguments)]
pub fn settle_event(
    counts: TokenCounts,
    is_stream: bool,
    group: &str,
    group_ratio: f64,
    hold: &Hold,
    price_table: &dyn PriceTable,
    channel_key: &str,
    route_unit_key: &str,
    public_model: &str,
    upstream_model: &str,
    first_token_ms: u32,
    duration_ms: u32,
    status_code: u16,
    error: Option<&str>,
    finish_reason: Option<&str>,
) -> UsageEvent {
    // The group multiplier comes entirely from the caller-supplied group_ratio.
    let price = price_table
        .lookup(public_model, group)
        .unwrap_or(crate::pricing::ModelPrice {
            input: 0.0,
            output: 0.0,
            cache: 0.0,
            group_multiplier: 1.0,
        });

    let cost = price_of(counts, &price, group_ratio);

    UsageEvent {
        token_key: hold.token_key.clone(),
        user_key: hold.user_key.clone(),
        channel_key: channel_key.to_string(),
        route_unit_key: route_unit_key.to_string(),
        // The effective group is back-filled by the caller (the forward settle point) from
        // the winning candidate unit; the metering library layer does not know route units,
        // so it stays empty and the write side fills it in.
        group_id: String::new(),
        public_model: public_model.to_string(),
        upstream_model: upstream_model.to_string(),
        prompt_tokens: counts.prompt,
        completion_tokens: counts.completion,
        cached_tokens: counts.cached,
        cache_creation_tokens: counts.cache_creation,
        reasoning_tokens: counts.reasoning,
        is_stream,
        first_token_ms,
        duration_ms,
        cost,
        status_code,
        error: error.map(std::string::ToString::to_string),
        finish_reason: finish_reason.map(std::string::ToString::to_string),
    }
}
