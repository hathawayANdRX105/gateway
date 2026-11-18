//! Upstream usage / finish-reason wire parsing — the single home for both the streaming
//! (`scanner`) and non-streaming (`settle`) paths. Keeping one reader per vendor key set
//! prevents a new dialect from being taught to one path while the other silently keeps
//! reporting no usage.
//!
//! `protocol-bridge` owns dialect conversion (IR ↔ vendor payloads); metering only reads
//! usage keys from upstream replies and never rewrites them.
//!
//! The name overlap with `gateway_protocol_bridge::gemini::gemini_finish_reason` is
//! deliberate: the bridge function maps a raw reason to `StopReason`, while the one here
//! extracts the raw string from the wire. Do not merge the two by accident.

use serde_json::Value as JsonValue;

use crate::scanner::TokenCounts;

/// Frame-level `finish_reason` extraction (lenient style, as in [`try_extract_usage`]):
/// `OpenAI` streaming's last frame `choices[0].finish_reason` (null in intermediate frames
/// is filtered out naturally by `as_str()`), Claude streaming's `message_delta` frame
/// `delta.stop_reason`; a top-level `stop_reason` covers the non-streaming Claude shape.
/// Non-JSON / missing field / non-string / empty string → None.
pub(crate) fn frame_finish_reason(data: &[u8]) -> Option<String> {
    let s = std::str::from_utf8(data).ok()?;
    let v: JsonValue = serde_json::from_str(s).ok()?;
    choice_finish_reason(&v)
        .or_else(|| delta_stop_reason(&v))
        .or_else(|| gemini_finish_reason(&v))
        .or_else(|| top_stop_reason(&v))
        .map(ToString::to_string)
}

/// Gemini `candidates[0].finishReason` (e.g. STOP / `MAX_TOKENS`); non-string / empty
/// string → None.
pub(crate) fn gemini_finish_reason(v: &JsonValue) -> Option<&str> {
    non_empty_str(
        v.get("candidates")
            .and_then(|c| c.get(0))
            .and_then(|c| c.get("finishReason")),
    )
}

/// Frame-level usage extraction (lenient style, as in [`frame_finish_reason`]):
/// - the usage object is at the top level (`OpenAI` / `DeepSeek` last frame, Claude
///   `message_delta`) or nested under `message.usage` (Claude `message_start`);
/// - non-JSON / no usage object / all zero → None (all zero = the frame gives no real
///   usage, so no `{0,0,0}` record is produced).
pub(crate) fn try_extract_usage(data: &[u8]) -> Option<TokenCounts> {
    let s = std::str::from_utf8(data).ok()?;
    let v: JsonValue = serde_json::from_str(s).ok()?;
    let usage = v
        .get("usage")
        .or_else(|| v.get("message").and_then(|m| m.get("usage")))
        // Gemini puts the usage object at `usageMetadata`.
        .or_else(|| v.get("usageMetadata"))?;
    let counts = counts_from_usage(usage);
    counts.has_primary_usage().then_some(counts)
}

/// Parse a `usage` object into counts: both `OpenAI` and Claude token key sets coexist and
/// the first present value wins; all cache locations are recognized
/// (`prompt_tokens_details.cached_tokens` / top-level `cached_tokens` /
/// `cache_read_input_tokens` / `cachedContentTokenCount`).
///
/// Completion convention: the vendor's primary counter, **with Gemini thinking folded
/// in**. Gemini reports `thoughtsTokenCount` outside `candidatesTokenCount`, and thinking
/// is billed at the output price, so `completion = candidates + thoughts` (same rule as
/// new-api and sub2api). `OpenAI`'s `reasoning_tokens` is already inside
/// `completion_tokens`, so it is only surfaced as a detail and never added again.
/// Cache-write is collected as a detail (`cache_write_tokens` /
/// `cache_creation_input_tokens`); `price_of` does not consume it.
/// Shared by streaming and non-streaming extraction.
pub(crate) fn counts_from_usage(usage: &JsonValue) -> TokenCounts {
    let get = |keys: &[&str]| {
        keys.iter()
            .find_map(|k| usage.get(*k).and_then(JsonValue::as_u64))
            .unwrap_or(0)
    };
    let detail = |group: &str, key: &str| {
        usage
            .get(group)
            .and_then(|d| d.get(key))
            .and_then(JsonValue::as_u64)
            .unwrap_or(0)
    };
    let candidates = usage
        .get("candidatesTokenCount")
        .and_then(JsonValue::as_u64);
    let thoughts = usage.get("thoughtsTokenCount").and_then(JsonValue::as_u64);
    // Gemini shape only: thinking sits outside candidatesTokenCount but is billed as
    // output. Guarded on `candidatesTokenCount` so an OpenAI-shaped usage that also
    // carries a thoughts field cannot be double-counted.
    let completion = match (candidates, thoughts) {
        (Some(c), Some(t)) => c.saturating_add(t),
        _ => get(&["completion_tokens", "output_tokens", "candidatesTokenCount"]),
    };
    // Which primary key supplied `prompt` decides whether cached tokens are a
    // subset or additive. `prompt_tokens` (OpenAI Chat) and `promptTokenCount`
    // (Gemini) are totals. `input_tokens` is shared by Claude (excludes cache) and
    // OpenAI Responses (includes cache), so Claude's cache keys disambiguate.
    let claude_cache_keys = usage.get("cache_read_input_tokens").is_some()
        || usage.get("cache_creation_input_tokens").is_some();
    let prompt_includes_cache = usage.get("prompt_tokens").is_some()
        || usage.get("promptTokenCount").is_some()
        || (usage.get("input_tokens").is_some() && !claude_cache_keys);
    TokenCounts {
        prompt: get(&["prompt_tokens", "input_tokens", "promptTokenCount"]),
        completion,
        cached: usage
            .get("prompt_tokens_details")
            .and_then(|d| d.get("cached_tokens"))
            // OpenAI Responses nests the cache detail under input_tokens_details.
            .or_else(|| {
                usage
                    .get("input_tokens_details")
                    .and_then(|d| d.get("cached_tokens"))
            })
            .or_else(|| usage.get("cached_tokens"))
            .or_else(|| usage.get("cache_read_input_tokens"))
            .or_else(|| usage.get("cachedContentTokenCount"))
            .and_then(JsonValue::as_u64)
            .unwrap_or(0),
        cache_creation: usage
            .get("cache_creation_input_tokens")
            .or_else(|| {
                usage
                    .get("prompt_tokens_details")
                    .and_then(|d| d.get("cache_write_tokens"))
            })
            .and_then(JsonValue::as_u64)
            .unwrap_or(0),
        reasoning: detail("completion_tokens_details", "reasoning_tokens")
            .max(detail("output_tokens_details", "reasoning_tokens"))
            .max(
                usage
                    .get("thoughtsTokenCount")
                    .and_then(JsonValue::as_u64)
                    .unwrap_or(0),
            )
            .max(
                usage
                    .get("reasoning_tokens")
                    .and_then(JsonValue::as_u64)
                    .unwrap_or(0),
            ),
        prompt_includes_cache,
    }
}

/// `choices[0].finish_reason` (`OpenAI` shape); non-string / empty string → None.
pub(crate) fn choice_finish_reason(v: &JsonValue) -> Option<&str> {
    non_empty_str(
        v.get("choices")
            .and_then(|choices| choices.get(0))
            .and_then(|choice| choice.get("finish_reason")),
    )
}

/// `delta.stop_reason` (Claude streaming `message_delta` shape); non-string / empty
/// string → None.
pub(crate) fn delta_stop_reason(v: &JsonValue) -> Option<&str> {
    non_empty_str(v.get("delta").and_then(|delta| delta.get("stop_reason")))
}

/// Top-level `stop_reason` (Claude non-streaming shape); non-string / empty string →
/// None.
pub(crate) fn top_stop_reason(v: &JsonValue) -> Option<&str> {
    non_empty_str(v.get("stop_reason"))
}

/// Extract the upstream stop reason from a **non-streaming** response body (complete JSON).
///
/// Two shapes are supported: `OpenAI` `choices[0].finish_reason` (stop/length/
/// `content_filter/tool_calls`) and Claude's top-level `stop_reason` (`end_turn`/
/// `max_tokens/stop_sequence`). Values pass through verbatim without cross-provider
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
/// Two shapes are supported: `OpenAI` (`prompt_tokens`/`completion_tokens`, with cache in
/// `prompt_tokens_details.cached_tokens` or top-level `cached_tokens`) and Claude
/// (`input_tokens`/`output_tokens`, with cache in `cache_read_input_tokens`). A body that is
/// not valid JSON, lacks the `usage` field, or reports all zeros → `None`, leaving the caller
/// to fall back to estimation.
#[must_use]
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

/// JSON string lookup: non-string or empty string is always treated as missing.
fn non_empty_str(value: Option<&JsonValue>) -> Option<&str> {
    value.and_then(JsonValue::as_str).filter(|s| !s.is_empty())
}
