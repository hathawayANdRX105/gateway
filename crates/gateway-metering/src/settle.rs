//! Settlement — scan result + pricing → UsageEvent (written to the flat usage_logs table).
//!
//! Callers: the settlement points of gateway_forward — `stream::finish` on the streaming
//! path and the stage telemetry hooks on the non-streaming success / failure exits. Settled
//! events land in usage_logs and observe aggregates them straight from the table, without
//! blocking forwarding.

use app_contract::SCHEMA_VERSION;
use app_contract::records::{SyncMeta, UsageEventRecord};
use serde_json::Value as JsonValue;

use crate::ledger::Hold;
use crate::pricing::{PriceTable, price_of};
use crate::scanner::{
    TokenCounts, choice_finish_reason, counts_from_usage, gemini_finish_reason, top_stop_reason,
};

/// Build one UsageEvent that the caller persists through the settle sink.
///
/// `is_stream` is the request's streaming intent (the streaming scan chain passes `true`;
/// the non-streaming commit point and failure-observation events pass the request body's
/// `stream` field); it lands verbatim in [`UsageEventRecord::is_stream`].
/// `finish_reason` is the upstream stop reason (the non-streaming commit point extracts it
/// from the response body through [`extract_finish_reason`], the streaming scan chain passes
/// the value captured from the last frame, and the failure-observation path passes `None`
/// because it is structurally absent); it lands verbatim in
/// [`UsageEventRecord::finish_reason`].
/// `group` goes into [`PriceTable::lookup`] together with model (implementations may price
/// per group); `group_ratio` is the request group multiplier (GroupRecord.rate_multiplier)
/// multiplied into [`price_of`]; library-level callers that cannot obtain the real group
/// multiplier pass 1.0.
/// Uniqueness: meta.key = a random UUIDv4 generated at the edge; de-duplication is left to the
/// event consumer.
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
) -> UsageEventRecord {
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

    UsageEventRecord {
        meta: SyncMeta {
            key: uuid::Uuid::new_v4().to_string(),
            schema_version: SCHEMA_VERSION,
            logical_version: 1,
            origin: "edge".into(),
            updated_at: chrono::Utc::now(),
        },
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
        error: error.map(|s| s.to_string()),
        finish_reason: finish_reason.map(|s| s.to_string()),
    }
}

/// Extract the upstream stop reason from a **non-streaming** response body (complete JSON).
///
/// Two shapes are supported: OpenAI `choices[0].finish_reason` (stop/length/
/// content_filter/tool_calls) and Claude's top-level `stop_reason` (end_turn/
/// max_tokens/stop_sequence). Values pass through verbatim without cross-provider
/// normalization (offline fitting buckets by value itself). Leniency matches
/// [`extract_usage`]: a body that is not valid JSON, a missing field, a non-string value, or
/// an empty string → `None`, leaving the caller to store NULL instead of a fabricated value.
pub fn extract_finish_reason(body: &[u8]) -> Option<String> {
    let v: JsonValue = serde_json::from_slice(body).ok()?;
    choice_finish_reason(&v)
        .or_else(|| gemini_finish_reason(&v))
        .or_else(|| top_stop_reason(&v))
        .map(ToString::to_string)
}

/// Parse usage counts from a **non-streaming** response body (complete JSON).
///
/// Two shapes are supported: OpenAI (`prompt_tokens`/`completion_tokens`, with cache in
/// `prompt_tokens_details.cached_tokens` or top-level `cached_tokens`) and Claude
/// (`input_tokens`/`output_tokens`, with cache in `cache_read_input_tokens`). A body that is
/// not valid JSON, lacks the `usage` field, or reports all zeros → `None`, leaving the caller
/// to fall back to estimation.
pub fn extract_usage(body: &[u8]) -> Option<TokenCounts> {
    let v: JsonValue = serde_json::from_slice(body).ok()?;
    // OpenAI/Claude nest usage under `usage`; Gemini uses `usageMetadata`.
    let usage = v.get("usage").or_else(|| v.get("usageMetadata"))?;
    let counts = counts_from_usage(usage);
    // No primary counter = the upstream gave no real usage (all-zero or detail-only);
    // treat it as absent so the caller's estimation fallback kicks in. Same rule as the
    // streaming path's `finish()`.
    counts.has_primary_usage().then_some(counts)
}
