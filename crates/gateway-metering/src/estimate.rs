//! Token estimation — the fallback when the upstream returns no usage.
//!
//! Reference: new-api `usage/estimate_tokens.go` (character-class weights: Latin/CJK/digits/
//! emoji/math/URL). This implementation folds emoji/math/URL into a single `other` class.
//! Accuracy target: ±10% (the same order as wildtoken), for fallback rather than authoritative
//! metering.

use serde_json::{Map, Value};

/// Estimate a token count by character-class weighting.
///
/// Weight table (TODO(#332) calibration): latin≈0.25 tok/char, cjk≈0.6, digit≈0.3...
///
/// Cost: O(n) in characters, single pass, no allocation.
pub fn estimate_tokens(text: &str) -> u64 {
    let mut cjk = 0u64;
    let mut latin = 0u64;
    let mut digit = 0u64;
    let mut other = 0u64;

    for ch in text.chars() {
        match ch {
            '\u{4e00}'..='\u{9fff}' | '\u{3400}'..='\u{4dbf}' | '\u{20000}'..='\u{2a6df}' => {
                cjk += 1;
            }
            c if c.is_ascii_alphabetic() => latin += 1,
            c if c.is_ascii_digit() => digit += 1,
            c if !c.is_ascii_whitespace() => other += 1,
            _ => {}
        }
    }

    // CJK ≈ 0.6 tok/char, Latin ≈ 0.25 tok/char, digit ≈ 0.3 tok/char, other ≈ 1.0
    let tokens =
        (cjk as f64 * 0.6) + (latin as f64 * 0.25) + (digit as f64 * 0.3) + (other as f64 * 1.0);
    tokens.ceil() as u64
}

/// Billable prompt tokens extracted from an already-parsed request body.
///
/// Covers the content-bearing shapes a translated body can take (`translate_request`
/// passes bodies through untouched when src == dst, so the body is typically the client's
/// own JSON):
/// - OpenAI chat / Claude: `messages[].content` (plain string, or a parts array — text
///   blocks under `text`, `tool_result` blocks under `content`) plus Claude's top-level
///   `system`;
/// - Gemini: `contents[].parts[].text` and `systemInstruction.parts[].text`;
/// - OpenAI Responses: `input` (a string, or items carrying `content`).
///
/// Other billable carriers (Responses `instructions`, `function_call.arguments`,
/// `function_call_output.output`) were never counted by the pre-rewrite scanner either;
/// wiring them in is TODO(#334). Non-text parts (image_url, base64 data URLs) carry
/// neither field and never count.
///
/// The dialect extractors are summed, not dispatched: real bodies carry exactly one
/// dialect's keys, and a hand-crafted body that mixes keys is then billed conservatively
/// high — the safe direction for both the soft budget pre-claim and settlement.
///
/// Returns `None` when no billable text was found (unknown shape, or only empty strings);
/// the caller then applies its own raw-text fallback.
///
/// Cost: O(n) in body bytes — one tree walk, no allocation.
pub fn estimate_prompt_from_value(body: &Value) -> Option<u64> {
    let total =
        messages_tokens(body) + system_tokens(body) + gemini_tokens(body) + responses_tokens(body);
    (total > 0).then_some(total)
}

/// Prompt-side pre-scan of the request body, used by the forward stages: the token-budget
/// estimate before sending (`stage.rs`) and the usage fallback at settlement
/// (`stage_telemetry.rs`).
///
/// Cost: one UTF-8 check + one JSON parse + one tree walk = O(n) in body bytes, with a
/// single tree allocation. Non-UTF-8 or invalid JSON ⇒ 0. Extracted text is priced with
/// `estimate_tokens`' character-class weights; a body with no recognized billable field
/// falls back to its raw character count divided by 4 (mixed text averages about 4 chars
/// per token).
pub fn estimate_prompt_tokens(adapted_body: &bytes::Bytes) -> u64 {
    let Ok(s) = std::str::from_utf8(adapted_body) else {
        return 0;
    };
    let Ok(body) = serde_json::from_str::<Value>(s) else {
        return 0;
    };
    estimate_prompt_from_value(&body).unwrap_or_else(|| raw_length_tokens(s))
}

/// Fallback estimate for a body whose shape carries no billable text: raw character count
/// divided by 4. Counts chars, never bytes, so CJK-heavy bodies are not inflated 3×.
///
/// Cost: O(n) in characters.
pub fn raw_length_tokens(raw: &str) -> u64 {
    raw.chars().count() as u64 / 4
}

/// Sum over `messages[].content` (OpenAI chat / Claude shape; Responses items go through
/// `responses_tokens` because their text lives under `input`).
fn messages_tokens(body: &Value) -> u64 {
    body.get("messages")
        .and_then(Value::as_array)
        .map(|msgs| {
            msgs.iter()
                .filter_map(|m| m.get("content"))
                .map(content_tokens)
                .sum()
        })
        .unwrap_or(0)
}

/// One `content`-shaped value: a plain string, or a parts/blocks array whose text sits in
/// per-item `text` fields.
fn content_tokens(content: &Value) -> u64 {
    content
        .as_str()
        .map(estimate_tokens)
        .unwrap_or_else(|| parts_tokens(content))
}

/// Parts/blocks array: estimates one value per object item (see [`part_tokens`]).
fn parts_tokens(parts: &Value) -> u64 {
    parts
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_object)
                .map(part_tokens)
                .sum()
        })
        .unwrap_or(0)
}

/// One parts item: the `text` field for text blocks; otherwise the nested `content`
/// value — Claude `tool_result` blocks carry billable tool output under `content` (a
/// string or a nested block array, see `gateway-protocol-bridge` claude.rs), which the
/// pre-rewrite literal scanner billed by matching that key anywhere; dropping it here
/// would silently make tool output free. Items with neither field (`image_url`,
/// documents) contribute nothing — the base64 skip rule, enforced by structure.
fn part_tokens(part: &Map<String, Value>) -> u64 {
    part.get("text").and_then(Value::as_str).map_or_else(
        || part.get("content").map_or(0, content_tokens),
        estimate_tokens,
    )
}

/// Claude's top-level `system`: plain string or text-block array.
fn system_tokens(body: &Value) -> u64 {
    body.get("system").map(content_tokens).unwrap_or(0)
}

/// Gemini dialect: `contents[].parts[].text` plus `systemInstruction.parts[].text`.
fn gemini_tokens(body: &Value) -> u64 {
    let contents = body
        .get("contents")
        .and_then(Value::as_array)
        .map(|cs| {
            cs.iter()
                .filter_map(|c| c.get("parts"))
                .map(parts_tokens)
                .sum()
        })
        .unwrap_or(0);
    let instruction = body
        .get("systemInstruction")
        .and_then(|s| s.get("parts"))
        .map(parts_tokens)
        .unwrap_or(0);
    contents + instruction
}

/// OpenAI Responses dialect: `input` is a plain string or an array of items, each a bare
/// string or an object carrying a `content` field.
fn responses_tokens(body: &Value) -> u64 {
    body.get("input").map_or(0, |input| {
        input.as_array().map_or_else(
            || content_tokens(input),
            |items| items.iter().map(input_item_tokens).sum(),
        )
    })
}

/// One Responses `input[]` item: bare string, or an object with a content field.
fn input_item_tokens(item: &Value) -> u64 {
    item.as_str()
        .map(estimate_tokens)
        .or_else(|| item.get("content").map(content_tokens))
        .unwrap_or(0)
}
