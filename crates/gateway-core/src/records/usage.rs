//! Usage records — the metering event the data plane hands to the consumer.
//!
//! Split boundary: this crate owns the **event payload**; record identity (sync key,
//! schema version, logical version, origin, timestamp) belongs to the consumer's
//! persistence machinery and is assigned by the `SettleSink` implementation, not here.

use serde::{Deserialize, Serialize};

/// Metering event for one request (produced on the forward path, persisted by the consumer).
///
/// Wire-compatible field-for-field with the persisted usage-log record: the consumer's
/// `SettleSink` wraps it in its own identity envelope (`key`, `schema_version`,
/// `logical_version`, `origin`, `updated_at`) before writing.
///
/// New fields must carry a compatible default: a consumer deserializing an event produced
/// by an older data plane must not fail on the missing field.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageEvent {
    pub token_key: String,
    pub user_key: String,
    pub channel_key: String,
    pub route_unit_key: String,
    /// Effective group (authoritative for routing: `RouteUnitRecord::group` of the winning
    /// candidate; filled in at the settlement point).
    #[serde(default)]
    pub group_id: String,
    pub public_model: String,
    pub upstream_model: String,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub cached_tokens: u64,
    /// Prompt tokens written to the cache (Claude `cache_creation_input_tokens`,
    /// `OpenAI` `prompt_tokens_details.cache_write_tokens`). Quantity only; the
    /// pricing layer does not consume it.
    #[serde(default)]
    pub cache_creation_tokens: u64,
    /// Reasoning/thinking tokens already included in `completion_tokens` (`OpenAI`
    /// `completion_tokens_details.reasoning_tokens`, Gemini `thoughtsTokenCount`).
    /// Quantity only; the pricing layer does not consume it.
    #[serde(default)]
    pub reasoning_tokens: u64,
    /// Whether the request intended to stream (SSE). Persisted rows use it to distinguish
    /// streaming from non-streaming sources; failure-observation rows record the streaming
    /// intent at request time.
    pub is_stream: bool,
    /// First-token latency (streaming).
    pub first_token_ms: u32,
    pub duration_ms: u32,
    /// Billing amount for this call (internal units, same dimension as the quota balance).
    pub cost: i64,
    /// Final upstream status code; 0 = connection failure.
    pub status_code: u16,
    /// Failure summary (truncated upstream error message), None on success.
    pub error: Option<String>,
    /// Upstream finish reason (the raw `choices[0].finish_reason` / `stop_reason` value,
    /// not normalized across providers). None = the response lacks the field / parsing
    /// failed / this path never received a successful upstream response.
    #[serde(default)]
    pub finish_reason: Option<String>,
}
