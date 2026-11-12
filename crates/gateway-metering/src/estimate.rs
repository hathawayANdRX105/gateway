//! Token estimation — the fallback when the upstream returns no usage.
//!
//! Reference: new-api `usage/estimate_tokens.go` (character-class weights: Latin/CJK/digits/
//! emoji/math/URL). This implementation folds emoji/math/URL into a single `other` class.
//! Accuracy target: ±10% (the same order as wildtoken), for fallback rather than authoritative
//! metering.

use serde_json::{Map, Value, to_string};

/// Estimate a token count by character-class weighting.
///
/// Weight table (TODO(ferrite-#332, re-anchored: #20) calibration): latin≈0.25 tok/char, cjk≈0.6, digit≈0.3...
///
/// Cost: O(n) in characters, single pass, no allocation.
#[must_use]
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
    //
    // The u64 -> f64 casts are exact for any realistic input: a request body cannot
    // carry more than 2^52 characters, so the mantissa is never exhausted. The
    // f64 -> u64 cast is safe because every term is non-negative and the sum is
    // finite, so `ceil` yields a non-negative value.
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    let tokens =
        ((cjk as f64 * 0.6) + (latin as f64 * 0.25) + (digit as f64 * 0.3) + (other as f64 * 1.0))
            .ceil() as u64;
    tokens
}

/// Billable prompt tokens extracted from an already-parsed request body.
///
/// Covers the content-bearing shapes a translated body can take (`translate_request`
/// passes bodies through untouched when src == dst, so the body is typically the client's
/// own JSON):
/// - `OpenAI` chat / Claude: `messages[].content` (plain string, or a parts array — text
///   blocks under `text`, `tool_result` blocks under `content`, `image_url` / Claude
///   `image` blocks, `tool_use` blocks, `function_call_output` blocks) plus Claude's
///   top-level `system`;
/// - Gemini: `contents[].parts[].text` and `systemInstruction.parts[].text`, plus
///   `inline_data` image parts (258 tokens each);
/// - `OpenAI` Responses: `input` (a string, or items carrying `content`) and `instructions`.
///
/// Tool carriers are counted as text: `function_call.arguments`, `function_call_output.output`,
/// Claude `tool_use` input (serialized to compact JSON), and Responses `instructions`.
///
/// Image parts are billed by their decoded pixel header (PNG IHDR / JPEG SOF), never by
/// the full base64 payload: `OpenAI` `image_url` detail `low` = 85 flat, `high`/absent =
/// 85 + 170 * tiles after fitting to 2048x2048; Claude `image` = (w*h)/750 capped at 1568;
/// undecodable headers bill 85 (`OpenAI`) or 0 (`Claude`).
///
/// The dialect extractors are summed, not dispatched: real bodies carry exactly one
/// dialect's keys, and a hand-crafted body that mixes keys is then billed conservatively
/// high — the safe direction for both the soft budget pre-claim and settlement.
///
/// Returns `None` when no billable text was found (unknown shape, or only empty strings);
/// the caller then applies its own raw-text fallback.
///
/// Cost: O(n) in body bytes — one tree walk, no allocation.
#[must_use]
pub fn estimate_prompt_from_value(body: &Value) -> Option<u64> {
    let total = messages_tokens(body)
        + system_tokens(body)
        + gemini_tokens(body)
        + responses_tokens(body)
        + instructions_tokens(body);
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
#[must_use]
pub fn raw_length_tokens(raw: &str) -> u64 {
    raw.chars().count() as u64 / 4
}

/// Sum over `messages[].content` (`OpenAI` chat / Claude shape; Responses items go through
/// `responses_tokens` because their text lives under `input`).
fn messages_tokens(body: &Value) -> u64 {
    body.get("messages")
        .and_then(Value::as_array)
        .map_or(0, |msgs| {
            msgs.iter()
                .filter_map(Value::as_object)
                .map(message_tokens)
                .sum()
        })
}

/// One chat/Claude message: billable content plus any `tool_calls` function arguments.
fn message_tokens(msg: &Map<String, Value>) -> u64 {
    content_tokens(msg.get("content").unwrap_or(&Value::Null))
        + openai_tool_call_arguments(msg)
        + legacy_function_call_arguments(msg)
}

/// `OpenAI` legacy `message.function_call.arguments` — the deprecated single-call shape
/// (superseded by `tool_calls[]`); its arguments string is billable text.
fn legacy_function_call_arguments(msg: &Map<String, Value>) -> u64 {
    msg.get("function_call")
        .and_then(|fc| fc.get("arguments"))
        .and_then(Value::as_str)
        .map_or(0, estimate_tokens)
}

/// `OpenAI` `message.tool_calls[].function.arguments` — each arguments string is billable text.
fn openai_tool_call_arguments(msg: &Map<String, Value>) -> u64 {
    msg.get("tool_calls")
        .and_then(Value::as_array)
        .map_or(0, |calls| {
            calls
                .iter()
                .filter_map(Value::as_object)
                .map(|call| {
                    call.get("function")
                        .and_then(Value::as_object)
                        .and_then(|f| f.get("arguments").and_then(Value::as_str))
                        .map_or(0, estimate_tokens)
                })
                .sum()
        })
}

/// One `content`-shaped value: a plain string, or a parts/blocks array whose text sits in
/// per-item `text` fields.
fn content_tokens(content: &Value) -> u64 {
    content
        .as_str()
        .map_or_else(|| parts_tokens(content), estimate_tokens)
}

/// Parts/blocks array: estimates one value per object item (see [`part_tokens`]).
fn parts_tokens(parts: &Value) -> u64 {
    parts.as_array().map_or(0, |items| {
        items
            .iter()
            .filter_map(Value::as_object)
            .map(part_tokens)
            .sum()
    })
}

/// One parts item: text blocks under `text`, tool output under `content` (Claude
/// `tool_result`), image parts (`OpenAI` `image_url`, Claude `image`, Gemini `inline_data`),
/// Claude `tool_use` input, and `OpenAI` `function_call_output` output. Items with no
/// billable carrier contribute nothing.
fn part_tokens(part: &Map<String, Value>) -> u64 {
    let text_or_content = part.get("text").and_then(Value::as_str).map_or_else(
        || part.get("content").map_or(0, content_tokens),
        estimate_tokens,
    );
    text_or_content + image_part_tokens(part) + tool_part_tokens(part)
}

/// Claude's top-level `system`: plain string or text-block array.
fn system_tokens(body: &Value) -> u64 {
    body.get("system").map_or(0, content_tokens)
}

/// Gemini dialect: `contents[].parts[].text` plus `systemInstruction.parts[].text`.
fn gemini_tokens(body: &Value) -> u64 {
    let contents = body
        .get("contents")
        .and_then(Value::as_array)
        .map_or(0, |cs| {
            cs.iter()
                .filter_map(|c| c.get("parts"))
                .map(parts_tokens)
                .sum()
        });
    let instruction = body
        .get("systemInstruction")
        .and_then(|s| s.get("parts"))
        .map_or(0, parts_tokens);
    contents + instruction
}

/// `OpenAI` Responses dialect: `input` is a plain string or an array of items, each a bare
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

/// Responses `instructions` field — counted like Claude's top-level `system`.
fn instructions_tokens(body: &Value) -> u64 {
    body.get("instructions").map_or(0, content_tokens)
}

/// Image parts billed from a decoded pixel header, never the full base64 payload.
fn image_part_tokens(part: &Map<String, Value>) -> u64 {
    // OpenAI `image_url` part: `detail` lives on the part or inside `image_url`.
    if part.get("image_url").is_some() {
        return openai_image_tokens(part);
    }
    // Claude `image` block: base64 payload under `source.data`.
    if part.get("type").and_then(Value::as_str) == Some("image") {
        return claude_image_tokens(part);
    }
    // Gemini `inline_data` image: fixed 258 tokens per image.
    if let Some(mime) = part
        .get("inline_data")
        .and_then(|d| d.get("mime_type").and_then(Value::as_str))
        && mime.starts_with("image/")
    {
        return 258;
    }
    0
}

/// `OpenAI` `image_url` image tokens: 85 flat for `detail: "low"`; otherwise
/// 85 + 170 * tiles after fitting the decoded dimensions into 2048x2048.
/// Undecodable header falls back to the `low` flat 85.
fn openai_image_tokens(part: &Map<String, Value>) -> u64 {
    let detail = part
        .get("detail")
        .and_then(Value::as_str)
        .or_else(|| {
            part.get("image_url")
                .and_then(|u| u.get("detail").and_then(Value::as_str))
        })
        .unwrap_or("high");
    if detail == "low" {
        return 85;
    }
    let url = part
        .get("image_url")
        .and_then(|u| u.get("url").and_then(Value::as_str));
    let Some(url) = url else {
        return 85;
    };
    let b64 = data_url_base64(url);
    let Some(b64) = b64 else {
        return 85;
    };
    let dims = decode_image_header_dimensions(b64);
    let Some((w, h)) = dims else {
        return 85;
    };
    let (w, h) = fit_to_2048(w, h);
    let tiles: u64 = u64::from(w.div_ceil(512)) * u64::from(h.div_ceil(512));
    85 + 170 * tiles
}

/// Claude `image` block tokens: (w*h)/750 with each side capped at 1568.
/// Undecodable header bills 0.
fn claude_image_tokens(part: &Map<String, Value>) -> u64 {
    let b64 = part
        .get("source")
        .and_then(|s| s.get("data").and_then(Value::as_str));
    let Some(b64) = b64 else {
        return 0;
    };
    let dims = decode_image_header_dimensions(b64);
    let Some((w, h)) = dims else {
        return 0;
    };
    let w = w.min(1568);
    let h = h.min(1568);
    (u64::from(w) * u64::from(h)) / 750
}

/// Tool carriers in a parts block: Claude `tool_use` input and `OpenAI`
/// `function_call_output` output, each counted as text.
fn tool_part_tokens(part: &Map<String, Value>) -> u64 {
    if part.get("type").and_then(Value::as_str) == Some("tool_use")
        && let Some(input) = part.get("input")
        && let Ok(serialized) = to_string(input)
    {
        return estimate_tokens(&serialized);
    }
    if part.get("type").and_then(Value::as_str) == Some("function_call_output") {
        return part
            .get("output")
            .and_then(Value::as_str)
            .map_or(0, estimate_tokens);
    }
    0
}

/// Extract the base64 payload from a `data:` URL.
///
/// Real `OpenAI` `image_url` values are `data:` URLs like
/// `data:image/png;base64,iVBORw0KGgo...` (payload after the first `,`). Some
/// clients write `data:image/png;base64:iVBORw0KGgo...` (payload after `:`);
/// accept either. Only treat it as base64 when `base64` is declared in the
/// media-type section.
fn data_url_base64(url: &str) -> Option<&str> {
    let rest = url.strip_prefix("data:")?;
    let (media, payload) = rest.split_once(',').or_else(|| rest.split_once(':'))?;
    if media.contains("base64") {
        Some(payload)
    } else {
        None
    }
}

/// Decode just enough of a base64 string to read an image header (capped at `limit`
/// bytes — the whole payload is never decoded).
fn decode_base64_prefix(s: &str, limit: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(limit.min(s.len() * 3 / 4));
    let mut buf = [0u8; 4];
    let mut n = 0;
    for c in s.bytes() {
        let Some(v) = b64_val(c) else {
            if c == b'=' || c.is_ascii_whitespace() {
                continue;
            }
            break;
        };
        buf[n] = v;
        n += 1;
        if n == 4 {
            if out.len() < limit {
                out.push((buf[0] << 2) | (buf[1] >> 4));
            }
            if out.len() < limit {
                out.push(((buf[1] & 0x0f) << 4) | (buf[2] >> 2));
            }
            if out.len() < limit {
                out.push(((buf[2] & 0x03) << 6) | buf[3]);
            }
            n = 0;
            if out.len() >= limit {
                break;
            }
        }
    }
    out
}

/// Read PNG width/height from the IHDR chunk (bytes 16-23) of a decoded header.
fn png_dimensions(data: &[u8]) -> Option<(u32, u32)> {
    if data.len() < 24 || &data[0..8] != b"\x89PNG\r\n\x1a\n" {
        return None;
    }
    // A PNG signature alone cannot fabricate dimensions: the first chunk after the
    // signature must be the IHDR chunk (its 4-byte type at offset 12..16).
    if data.len() < 24 || &data[12..16] != b"IHDR" {
        return None;
    }
    let w = u32::from_be_bytes([data[16], data[17], data[18], data[19]]);
    let h = u32::from_be_bytes([data[20], data[21], data[22], data[23]]);
    Some((w, h))
}

/// Read width/height from the first JPEG SOF marker (SOF0/SOF2/…) in a decoded header.
fn jpeg_dimensions(data: &[u8]) -> Option<(u32, u32)> {
    if data.len() < 4 || &data[0..2] != b"\xff\xd8" {
        return None;
    }
    let mut i = 2usize;
    while i + 3 <= data.len() {
        if data[i] != 0xff {
            return None;
        }
        let marker = data[i + 1];
        i += 2;
        while i < data.len() && data[i] == 0xff {
            i += 1;
        }
        match marker {
            0xd9 | 0xda => return None, // EOI / SOS -> entropy data, no more SOF
            _ => {}
        }
        if i + 2 > data.len() {
            return None;
        }
        let seg_len = u16::from_be_bytes([data[i], data[i + 1]]) as usize;
        if matches!(
            marker,
            0xc0 | 0xc1
                | 0xc2
                | 0xc3
                | 0xc5
                | 0xc6
                | 0xc7
                | 0xc9
                | 0xca
                | 0xcb
                | 0xcd
                | 0xce
                | 0xcf
        ) && i + 7 <= data.len()
            && seg_len >= 8
        {
            let h = u32::from(u16::from_be_bytes([data[i + 3], data[i + 4]]));
            let w = u32::from(u16::from_be_bytes([data[i + 5], data[i + 6]]));
            return Some((w, h));
        }
        i = i.saturating_add(seg_len);
    }
    None
}

/// Decode image dimensions from a base64 payload: PNG IHDR first, then JPEG SOF.
fn decode_image_header_dimensions(b64: &str) -> Option<(u32, u32)> {
    const HEADER_LIMIT: usize = 65536;
    let data = decode_base64_prefix(b64, HEADER_LIMIT);
    png_dimensions(&data).or_else(|| jpeg_dimensions(&data))
}

/// Scale an image so its longest side fits 2048, then round each side down to a
/// multiple of 32 (`OpenAI` tiling grid).
fn fit_to_2048(w: u32, h: u32) -> (u32, u32) {
    // Intermediate math in u64: a crafted PNG header can declare dimensions up to
    // u32::MAX, and `w * 2048` in u32 would wrap (debug panic / release under-billing).
    let max = u64::from(w.max(h));
    if max <= 2048 {
        return (w, h);
    }
    #[allow(clippy::cast_possible_truncation)]
    let scale = |side: u32| -> u32 {
        let scaled = (u64::from(side) * 2048 / max) as u32;
        scaled - scaled % 32
    };
    (scale(w), scale(h))
}

/// One base64 character -> its 6-bit value.
fn b64_val(c: u8) -> Option<u8> {
    match c {
        b'A'..=b'Z' => Some(c - b'A'),
        b'a'..=b'z' => Some(c - b'a' + 26),
        b'0'..=b'9' => Some(c - b'0' + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    }
}
