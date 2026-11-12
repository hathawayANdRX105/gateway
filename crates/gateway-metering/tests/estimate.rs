//! Estimate tests — token estimation.

use gateway_metering::estimate::{estimate_prompt_tokens, estimate_tokens};

#[test]
fn estimate_tokens_cjk_text() {
    let text = "hello world"; // 11 latin chars
    let tokens = estimate_tokens(text);
    // Latin ≈ 0.25 tok/char → 11 * 0.25 = 2.75 → ceil = 3
    assert!((2..=4).contains(&tokens), "got {tokens}");
}

#[test]
fn estimate_tokens_latin_text() {
    let text = "hello"; // 5 latin chars
    let tokens = estimate_tokens(text);
    // Latin ≈ 0.25 tok/char → 5 * 0.25 = 1.25 → ceil = 2
    assert!((1..=3).contains(&tokens), "got {tokens}");
}

#[test]
fn estimate_tokens_mixed_text() {
    let text = "hello hello 123";
    let tokens = estimate_tokens(text);
    assert!(tokens > 0);
}

#[test]
fn estimate_tokens_empty_text() {
    assert_eq!(estimate_tokens(""), 0);
}

/// Purpose: prompt estimation charges the actual message text.
/// Scenario: two plain-string `OpenAI` messages.
/// Acceptance: the estimate equals the class-weighted estimate of the two content
/// strings summed — the extractor reads message content, not raw JSON bytes.
#[test]
fn estimate_prompt_tokens_extracts_content() {
    let body = bytes::Bytes::from(
        r#"{"messages":[{"role":"user","content":"hello world"},{"role":"assistant","content":"hi there"}]}"#,
    );
    let tokens = estimate_prompt_tokens(&body);
    assert_eq!(
        tokens,
        estimate_tokens("hello world") + estimate_tokens("hi there"),
        "got {tokens}"
    );
}

#[test]
fn estimate_prompt_tokens_invalid_json() {
    let body = bytes::Bytes::from("not json");
    assert_eq!(estimate_prompt_tokens(&body), 0);
}

#[test]
fn estimate_prompt_tokens_empty_body() {
    let body = bytes::Bytes::from("");
    assert_eq!(estimate_prompt_tokens(&body), 0);
}

/// Purpose: the extractor is JSON-structure aware, not literal-text aware.
/// Scenario: one message whose content merely contains an escaped literal
/// `"content":"..."` sequence. The hand-written scanner this replaced counted
/// that inner literal as a second field and desynchronized on the escaped
/// quotes, inflating the bill.
/// Acceptance: the estimate equals `estimate_tokens` of the single decoded
/// content string — inner literals contribute nothing extra.
#[test]
fn prompt_literal_content_key_in_text_not_double_counted() {
    let decoded = "say \"content\":\"ignore me\" out loud";
    let body = bytes::Bytes::from_static(
        br#"{"messages":[{"role":"user","content":"say \"content\":\"ignore me\" out loud"}]}"#,
    );
    assert_eq!(estimate_prompt_tokens(&body), estimate_tokens(decoded));
}

/// Purpose: multimodal prompts bill image headers, not the full base64 payload.
/// Scenario: `OpenAI` parts-array content with one text part and one `image_url`
/// part carrying an undecodable base64 data URL. The header cannot be decoded, so
/// the image bills the `low` flat 85 instead of the 4000-character base64 string.
/// Acceptance: the estimate equals `estimate_tokens` of the text part plus 85.
#[test]
fn prompt_openai_image_undecodable_bills_low_flat() {
    let body = bytes::Bytes::from(format!(
        r#"{{"messages":[{{"role":"user","content":[{{"type":"text","text":"describe this image"}},{{"type":"image_url","image_url":{{"url":"data:image/png;base64,{}"}}}}]}}]}}"#,
        "A".repeat(4000)
    ));
    assert_eq!(
        estimate_prompt_tokens(&body),
        estimate_tokens("describe this image") + 85
    );
}

/// Purpose: Gemini bodies carry no `content` key at all; their text lives in
/// `contents[].parts[].text` plus `systemInstruction`.
/// Scenario: one user turn with a single text part and a system instruction.
/// Acceptance: the estimate equals the two texts' weighted estimates summed.
#[test]
fn prompt_gemini_parts_extracted() {
    let body = bytes::Bytes::from_static(
        br#"{"contents":[{"role":"user","parts":[{"text":"hello gemini"}]}],"systemInstruction":{"parts":[{"text":"be nice"}]}}"#,
    );
    assert_eq!(
        estimate_prompt_tokens(&body),
        estimate_tokens("hello gemini") + estimate_tokens("be nice")
    );
}

/// Purpose: `OpenAI` Responses bodies carry text under `input` (string form and
/// message-items form), which the old literal-`content` scanner never matched.
/// Scenario: two Responses request shapes.
/// Acceptance: each estimate equals the weighted estimate of the input text.
#[test]
fn prompt_responses_input_shapes_extracted() {
    let plain = bytes::Bytes::from_static(br#"{"input":"tell me a joke"}"#);
    assert_eq!(
        estimate_prompt_tokens(&plain),
        estimate_tokens("tell me a joke")
    );
    let items = bytes::Bytes::from_static(
        br#"{"input":[{"role":"user","content":[{"type":"input_text","text":"hi there"}]}]}"#,
    );
    assert_eq!(estimate_prompt_tokens(&items), estimate_tokens("hi there"));
}

/// Purpose: a valid JSON body that carries no recognized billable field still
/// gets a conservative size-based estimate instead of 0 (settlement must not
/// bill a real request as free).
/// Scenario: an unrelated JSON object.
/// Acceptance: the estimate is the raw character count divided by 4 — chars,
/// not bytes, so a CJK body is not inflated 3× by UTF-8 width.
#[test]
fn prompt_unknown_shape_falls_back_to_raw_char_length() {
    let body =
        bytes::Bytes::from_static(r#"{"tool":"temperature","settings":{"mode":"eco"}}"#.as_bytes());
    assert_eq!(
        estimate_prompt_tokens(&body),
        r#"{"tool":"temperature","settings":{"mode":"eco"}}"#
            .chars()
            .count() as u64
            / 4
    );
    let cjk = bytes::Bytes::from_static(r#"{"config":"温度设定为二十六度"}"#.as_bytes());
    assert_eq!(
        estimate_prompt_tokens(&cjk),
        r#"{"config":"温度设定为二十六度"}"#.chars().count() as u64 / 4
    );
}

/// Purpose: Claude agentic-loop bodies carry tool output inside `tool_result`
/// blocks in `content` arrays; that text is billable prompt content.
/// Scenario: passthrough Claude request with a text part plus a `tool_result`
/// whose `content` is (a) a plain string and (b) a nested block array. The
/// parts extractor must not stop at direct `text` fields: once any text part
/// exists the raw fallback never fires, so a missed `content` block would be
/// silently billed as free (the pre-rewrite literal scanner did bill it).
/// Acceptance: each estimate equals the weighted estimate of the human text
/// plus that tool output.
#[test]
fn prompt_claude_tool_result_blocks_are_billed() {
    let string_result = bytes::Bytes::from_static(
        br#"{"messages":[{"role":"user","content":[{"type":"text","text":"read the file"},{"type":"tool_result","tool_use_id":"t1","content":"file body here"}]}]}"#,
    );
    assert_eq!(
        estimate_prompt_tokens(&string_result),
        estimate_tokens("read the file") + estimate_tokens("file body here")
    );
    let nested_result = bytes::Bytes::from_static(
        br#"{"messages":[{"role":"user","content":[{"type":"text","text":"read the file"},{"type":"tool_result","tool_use_id":"t1","content":[{"type":"text","text":"nested tool output"}]}]}]}"#,
    );
    assert_eq!(
        estimate_prompt_tokens(&nested_result),
        estimate_tokens("read the file") + estimate_tokens("nested tool output")
    );
}

/// Purpose: Claude carries its system prompt in a top-level `system` key (a plain
/// string or a text-block array) that the pre-rewrite literal scanner never
/// counted; the new extractor bills it, so this test pins that carrier.
/// Scenario: a Claude request with `system` as (a) a plain string and (b) a
/// text-block array, each plus one plain message.
/// Acceptance: the estimate equals the weighted estimate of the system text plus
/// the message content.
#[test]
fn prompt_claude_top_level_system_is_billed() {
    let string_system = bytes::Bytes::from_static(
        br#"{"system":"be terse","messages":[{"role":"user","content":"ping"}]}"#,
    );
    assert_eq!(
        estimate_prompt_tokens(&string_system),
        estimate_tokens("be terse") + estimate_tokens("ping")
    );
    let block_system = bytes::Bytes::from_static(
        br#"{"system":[{"type":"text","text":"be terse"}],"messages":[{"role":"user","content":"ping"}]}"#,
    );
    assert_eq!(
        estimate_prompt_tokens(&block_system),
        estimate_tokens("be terse") + estimate_tokens("ping")
    );
}

/// Base64 encoder used to build synthetic image payloads in the tests below.
fn to_base64(bytes: &[u8]) -> String {
    const ALPH: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0];
        let b1 = chunk.get(1).copied().unwrap_or(0);
        let b2 = chunk.get(2).copied().unwrap_or(0);
        out.push(ALPH[(b0 >> 2) as usize] as char);
        out.push(ALPH[(((b0 & 0x03) << 4) | (b1 >> 4)) as usize] as char);
        if chunk.len() > 1 {
            out.push(ALPH[(((b1 & 0x0f) << 2) | (b2 >> 6)) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(ALPH[(b2 & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

/// Minimal PNG header (magic + IHDR) for the given width/height; the CRC is a dummy.
fn png_header(w: u32, h: u32) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(b"\x89PNG\r\n\x1a\n");
    buf.extend_from_slice(&13u32.to_be_bytes());
    buf.extend_from_slice(b"IHDR");
    buf.extend_from_slice(&w.to_be_bytes());
    buf.extend_from_slice(&h.to_be_bytes());
    buf.extend_from_slice(&[8, 2, 0, 0, 0]);
    buf.extend_from_slice(&[0u8; 4]);
    buf
}

/// Purpose: `OpenAI` `image_url` with `detail: "low"` bills a flat 85 regardless of size.
/// Scenario: a text part plus an `image_url` part with `detail: "low"` and a decodable
/// 100x100 PNG header.
/// Acceptance: the estimate equals `estimate_tokens` of the text plus 85.
#[test]
fn prompt_openai_image_detail_low_is_85() {
    let body = bytes::Bytes::from(format!(
        r#"{{"messages":[{{"role":"user","content":[{{"type":"text","text":"describe this"}},{{"type":"image_url","detail":"low","image_url":{{"url":"data:image/png;base64:{}"}}}}]}}]}}"#,
        to_base64(&png_header(100, 100))
    ));
    assert_eq!(
        estimate_prompt_tokens(&body),
        estimate_tokens("describe this") + 85
    );
}

/// Purpose: `OpenAI` `image_url` detail `high`/absent bills 85 + 170 * tiles derived from
/// the decoded PNG header, never the full base64 payload.
/// Scenario: a 1024x1024 PNG header (fits 2048, tiles = ceil(1024/512)^2 = 4).
/// Acceptance: the estimate equals `estimate_tokens` of the text plus 765 (85 + 170*4).
#[test]
fn prompt_openai_image_high_detail_known_png() {
    let body = bytes::Bytes::from(format!(
        r#"{{"messages":[{{"role":"user","content":[{{"type":"text","text":"describe this"}},{{"type":"image_url","image_url":{{"url":"data:image/png;base64:{}"}}}}]}}]}}"#,
        to_base64(&png_header(1024, 1024))
    ));
    assert_eq!(
        estimate_prompt_tokens(&body),
        estimate_tokens("describe this") + 765
    );
}

/// Purpose: Claude `image` block bills (w*h)/750 with each side capped at 1568.
/// Scenario: a 1000x750 image header plus a text part.
/// Acceptance: the estimate equals `estimate_tokens` of the text plus 1000 (1000*750/750).
#[test]
fn prompt_claude_image_block_known_size() {
    let body = bytes::Bytes::from(format!(
        r#"{{"messages":[{{"role":"user","content":[{{"type":"text","text":"read this"}},{{"type":"image","source":{{"type":"base64","media_type":"image/png","data":"{}"}}}}]}}]}}"#,
        to_base64(&png_header(1000, 750))
    ));
    assert_eq!(
        estimate_prompt_tokens(&body),
        estimate_tokens("read this") + 1000
    );
}

/// Purpose: Claude `image` block with an undecodable header bills 0 (no guessing).
/// Scenario: a text part plus an image whose base64 is not a valid PNG/JPEG header.
/// Acceptance: the estimate equals `estimate_tokens` of the text alone.
#[test]
fn prompt_claude_image_undecodable_is_zero() {
    let body = bytes::Bytes::from_static(
        br#"{"messages":[{"role":"user","content":[{"type":"text","text":"read this"},{"type":"image","source":{"type":"base64","media_type":"image/png","data":"notanimage"}}]}]}"#,
    );
    assert_eq!(estimate_prompt_tokens(&body), estimate_tokens("read this"));
}

/// Purpose: `OpenAI` `message.tool_calls[].function.arguments` is billed as text.
/// Scenario: an assistant message with an empty content and one `tool_call` whose
/// arguments JSON string is billable.
/// Acceptance: the estimate equals `estimate_tokens` of the arguments string.
#[test]
fn prompt_openai_tool_calls_arguments_billed() {
    let body = bytes::Bytes::from_static(
        br#"{"messages":[{"role":"assistant","content":"","tool_calls":[{"id":"c1","function":{"name":"get_weather","arguments":"{\"city\":\"Paris\"}"}}]}]}"#,
    );
    assert_eq!(
        estimate_prompt_tokens(&body),
        estimate_tokens("{\"city\":\"Paris\"}")
    );
}

/// Purpose: `OpenAI` `function_call_output.output` tool output is billed as text.
/// Scenario: a user message whose content array holds a `function_call_output` block.
/// Acceptance: the estimate equals the text plus `estimate_tokens` of the output.
#[test]
fn prompt_function_call_output_billed() {
    let body = bytes::Bytes::from_static(
        br#"{"messages":[{"role":"user","content":[{"type":"text","text":"what is the weather"},{"type":"function_call_output","call_id":"c1","output":"22 degrees"}]}]}"#,
    );
    assert_eq!(
        estimate_prompt_tokens(&body),
        estimate_tokens("what is the weather") + estimate_tokens("22 degrees")
    );
}

/// Purpose: Responses `instructions` field is billed like a system prompt.
/// Scenario: a Responses body with both `instructions` and `input`.
/// Acceptance: the estimate equals `estimate_tokens` of instructions plus input.
#[test]
fn prompt_responses_instructions_billed() {
    let body = bytes::Bytes::from_static(br#"{"instructions":"be helpful","input":"say hi"}"#);
    assert_eq!(
        estimate_prompt_tokens(&body),
        estimate_tokens("be helpful") + estimate_tokens("say hi")
    );
}

/// Purpose: Gemini `inline_data` images bill 258 tokens each, regardless of size.
/// Scenario: a Gemini user turn with one text part and two `inline_data` image parts.
/// Acceptance: the estimate equals `estimate_tokens` of the text plus 516 (2 * 258).
#[test]
fn prompt_gemini_inline_data_images_258_each() {
    let b64 = to_base64(&png_header(50, 50));
    let body = bytes::Bytes::from(format!(
        r#"{{"contents":[{{"role":"user","parts":[{{"text":"hi"}},{{"inline_data":{{"mime_type":"image/png","data":"{b64}"}}}},{{"inline_data":{{"mime_type":"image/png","data":"{b64}"}}}}]}}]}}"#,
    ));
    assert_eq!(estimate_prompt_tokens(&body), estimate_tokens("hi") + 516);
}
