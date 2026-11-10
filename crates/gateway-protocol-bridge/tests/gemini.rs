//! Behaviour tests for the Gemini codec.
//!
//! Besides the two-way round-trip, these tests pin three concrete defects of the old
//! implementation (`adaptor::convert_gemini_event_to_openai`): `finish_reason` passed through
//! verbatim, a freshly generated uuid per chunk id, and `choices` being an empty array in
//! the usage frame. All three cases are written so that the old behaviour necessarily fails.

use bytes::Bytes;
use gateway_protocol_bridge::adaptor::Protocol;
use gateway_protocol_bridge::format_codec::FormatCodec;
use gateway_protocol_bridge::gemini::GeminiCodec;
use gateway_protocol_bridge::ir::{ContentBlock, Role, StopReason, StreamEvent};
use serde_json::{Value, json};

/// Split encoder output into a sequence of JSON values.
fn frames_to_values(frames: &[Bytes]) -> Vec<Value> {
    frames
        .iter()
        .filter_map(|f| {
            let text = String::from_utf8_lossy(f);
            let line = text.lines().find(|l| l.starts_with("data: "))?;
            serde_json::from_str(line.strip_prefix("data: ")?).ok()
        })
        .collect()
}

// ---------- Request direction ----------

// Two-way request round-trip: contents / systemInstruction / generationConfig / tools
// survive.
#[test]
fn gemini_request_roundtrip_preserves_structure() {
    let codec = GeminiCodec::new();
    let body = serde_json::to_vec(&json!({
        "systemInstruction": {"parts": [{"text": "be brief"}]},
        "contents": [
            {"role": "user", "parts": [{"text": "hi"}]},
            {"role": "model", "parts": [{"text": "hello"}]}
        ],
        "generationConfig": {
            "temperature": 0.4,
            "topP": 0.9,
            "maxOutputTokens": 128,
            "stopSequences": ["###"]
        },
        "tools": [{"functionDeclarations": [{
            "name": "search",
            "description": "find things",
            "parameters": {"type": "object", "properties": {"q": {"type": "string"}}}
        }]}]
    }))
    .unwrap();

    let ir = codec.decode_request(Bytes::from(body)).expect("decode");
    assert_eq!(ir.system.len(), 1);
    assert_eq!(ir.system[0].text, "be brief");
    assert_eq!(ir.messages.len(), 2);
    assert_eq!(ir.messages[0].role, Role::User);
    assert_eq!(
        ir.messages[1].role,
        Role::Assistant,
        "model should map to assistant"
    );

    assert_eq!(
        ir.tools.len(),
        1,
        "functionDeclarations must be flattened into tools"
    );
    assert_eq!(ir.tools[0].name, "search");
    assert_eq!(
        ir.tools[0].input_schema["properties"]["q"]["type"], "string",
        "Gemini's parameters must go into input_schema"
    );

    let s = ir
        .sampling
        .as_ref()
        .expect("generationConfig should produce sampling parameters");
    assert_eq!(s.temperature, Some(0.4));
    assert_eq!(s.top_p, Some(0.9));
    assert_eq!(s.max_tokens, Some(128));
    assert_eq!(s.extra.get("stop"), Some(&json!(["###"])));

    // Encode back: field names must return to Gemini's naming.
    let out: Value = serde_json::from_slice(&codec.encode_request(&ir).unwrap()).unwrap();
    assert_eq!(out["systemInstruction"]["parts"][0]["text"], "be brief");
    assert_eq!(out["contents"][0]["role"], "user");
    assert_eq!(out["contents"][1]["role"], "model");
    assert_eq!(out["generationConfig"]["maxOutputTokens"], 128);
    assert_eq!(out["generationConfig"]["topP"], 0.9);
    assert_eq!(out["generationConfig"]["stopSequences"][0], "###");
    assert_eq!(
        out["tools"][0]["functionDeclarations"][0]["parameters"]["type"], "object",
        "round-trip must use Gemini's parameters field names"
    );
}

// Both functionCall and inline_data must decode into structured blocks, never plain text.
#[test]
fn gemini_request_decodes_function_call_and_image_parts() {
    let codec = GeminiCodec::new();
    let body = serde_json::to_vec(&json!({
        "contents": [{"role": "model", "parts": [
            {"functionCall": {"name": "search", "args": {"q": "x"}}},
            {"inline_data": {"mime_type": "image/png", "data": "AAAB"}}
        ]}]
    }))
    .unwrap();

    let ir = codec.decode_request(Bytes::from(body)).expect("decode");
    let content = &ir.messages[0].content;

    assert!(
        content
            .iter()
            .any(|b| matches!(b, ContentBlock::ToolUse { name, input, .. }
                if name == "search" && input["q"] == "x")),
        "functionCall should become ToolUse, actual: {content:?}"
    );
    assert!(
        content
            .iter()
            .any(|b| matches!(b, ContentBlock::Image { media_type, data }
                if media_type == "image/png" && data == "AAAB")),
        "inline_data should become Image, actual: {content:?}"
    );
}

// ---------- Defect 1: finish_reason mapping ----------

// **Hard counterexample**: Gemini's finishReason is an upper-case enum and must be mapped
// to the IR's semantic values, not passed through verbatim (the old implementation assigned
// it directly, so the client got "STOP" — a value that does not exist in the OpenAI shape).
#[test]
fn gemini_finish_reason_is_mapped_not_passed_through() {
    let codec = GeminiCodec::new();

    let cases = [
        ("STOP", StopReason::Stop),
        ("MAX_TOKENS", StopReason::Length),
        ("SAFETY", StopReason::ContentFilter),
        ("RECITATION", StopReason::ContentFilter),
        ("MALFORMED_FUNCTION_CALL", StopReason::ToolUse),
    ];

    for (raw, expected) in cases {
        let body = serde_json::to_vec(&json!({
            "candidates": [{
                "content": {"role": "model", "parts": [{"text": "x"}]},
                "finishReason": raw
            }],
            "usageMetadata": {"promptTokenCount": 1, "candidatesTokenCount": 1}
        }))
        .unwrap();

        let resp = codec.decode_response(Bytes::from(body)).expect("decode");
        assert_eq!(
            resp.stop_reason, expected,
            "{raw} must be mapped to {expected:?}, not passed through verbatim"
        );
        assert_ne!(
            resp.stop_reason,
            StopReason::Other(raw.to_string()),
            "{raw} must not fall into the Other escape hatch (indicates the mapping table misses this item)"
        );
    }
}

// The streaming path must map it too: the old streaming branch also emitted
// finish_reason verbatim.
#[test]
fn gemini_stream_finish_reason_is_mapped() {
    let codec = GeminiCodec::new();
    let frame = json!({
        "candidates": [{
            "content": {"role": "model", "parts": [{"text": "done"}]},
            "finishReason": "MAX_TOKENS"
        }]
    })
    .to_string();

    let events = codec.decode_event(&frame).expect("decode");
    let stop = events
        .iter()
        .find_map(|e| match e {
            StreamEvent::MessageStop { reason } => Some(reason.clone()),
            _ => None,
        })
        .expect("should produce MessageStop");

    assert_eq!(
        stop,
        StopReason::Length,
        "streaming finishReason must also be mapped"
    );
}

// ---------- Defect 2: stable id ----------

// **Hard counterexample**: the chunk id is reused across the whole stream. The old
// implementation called `Uuid::new_v4()` per chunk, so one completion looked like several
// responses to the client and to streaming-billing aggregation. This asserts that every
// frame carries the same id.
#[test]
fn gemini_encoder_keeps_id_stable_across_stream() {
    let codec = GeminiCodec::new();
    let mut enc = codec.stream_encoder();

    let mut frames = Vec::new();
    frames.extend(
        enc.encode_event(&StreamEvent::MessageStart {
            id: "chatcmpl-fixed".into(),
            model: "gemini-3".into(),
        })
        .unwrap(),
    );
    for text in ["a", "b", "c", "d"] {
        frames.extend(
            enc.encode_event(&StreamEvent::TextDelta {
                index: 0,
                text: text.into(),
            })
            .unwrap(),
        );
    }
    frames.extend(
        enc.encode_event(&StreamEvent::MessageStop {
            reason: StopReason::Stop,
        })
        .unwrap(),
    );

    let ids: Vec<String> = frames_to_values(&frames)
        .iter()
        .filter_map(|v| v.get("id").and_then(Value::as_str).map(str::to_string))
        .collect();

    assert!(
        ids.len() >= 4,
        "there should be multiple frames with id, actual: {ids:?}"
    );
    assert!(
        ids.iter().all(|id| id == "chatcmpl-fixed"),
        "stream id must be consistent (old implementation generated uuid per chunk, must fail), actual: {ids:?}"
    );
}

// ---------- Defect 3: usage frame shape ----------

// **Hard counterexample**: the usage frame must not be `choices: []` (the old
// behaviour, which most clients reject).
#[test]
fn gemini_usage_frame_carries_usage_metadata() {
    let codec = GeminiCodec::new();
    let mut enc = codec.stream_encoder();

    let frames = enc
        .encode_event(&StreamEvent::MessageDelta {
            usage: Some(gateway_protocol_bridge::ir::Usage {
                prompt_tokens: 8,
                completion_tokens: 5,
                cached_tokens: None,
                ..Default::default()
            }),
        })
        .unwrap();

    let values = frames_to_values(&frames);
    let usage_frame = values
        .iter()
        .find(|v| v.get("usageMetadata").is_some())
        .expect("there should be a frame with usageMetadata");

    assert_eq!(usage_frame["usageMetadata"]["promptTokenCount"], 8);
    assert_eq!(usage_frame["usageMetadata"]["candidatesTokenCount"], 5);
    assert_eq!(usage_frame["usageMetadata"]["totalTokenCount"], 13);

    // The frame must carry a candidates array (structurally complete), not empty choices.
    assert!(
        usage_frame["candidates"].is_array(),
        "usage frame must carry candidates array, actual: {usage_frame}"
    );
}

// ---------- Non-streaming response ----------

#[test]
fn gemini_decode_response_maps_parts_and_usage() {
    let codec = GeminiCodec::new();
    let body = serde_json::to_vec(&json!({
        "candidates": [{
            "content": {"role": "model", "parts": [
                {"text": "calling a tool"},
                {"functionCall": {"name": "search", "args": {"q": "z"}}}
            ]},
            "finishReason": "STOP"
        }],
        "usageMetadata": {
            "promptTokenCount": 6,
            "candidatesTokenCount": 3,
            "cachedContentTokenCount": 2
        }
    }))
    .unwrap();

    let resp = codec.decode_response(Bytes::from(body)).expect("decode");
    assert_eq!(resp.usage.prompt_tokens, 6);
    assert_eq!(resp.usage.completion_tokens, 3);
    assert_eq!(resp.usage.cached_tokens, Some(2));
    assert!(
        resp.outputs
            .iter()
            .any(|b| matches!(b, ContentBlock::ToolUse { name, .. } if name == "search")),
        "functionCall in non-streaming response must not be lost, actual: {:?}",
        resp.outputs
    );
}

#[test]
fn gemini_encode_response_uses_gemini_field_names() {
    let codec = GeminiCodec::new();
    let resp = gateway_protocol_bridge::ir::LlmResponse {
        id: "r1".into(),
        model: "gemini-3".into(),
        outputs: vec![ContentBlock::Text { text: "hi".into() }],
        usage: gateway_protocol_bridge::ir::Usage {
            prompt_tokens: 2,
            completion_tokens: 3,
            cached_tokens: None,
            ..Default::default()
        },
        stop_reason: StopReason::Stop,
    };

    let out: Value = serde_json::from_slice(&codec.encode_response(&resp).unwrap()).unwrap();
    assert_eq!(out["candidates"][0]["content"]["role"], "model");
    assert_eq!(out["candidates"][0]["finishReason"], "STOP");
    assert_eq!(out["usageMetadata"]["promptTokenCount"], 2);
    assert_eq!(out["usageMetadata"]["candidatesTokenCount"], 3);
}

// ---------- Streaming decode ----------

// Each part must get its own index: putting them all at 0 makes downstream formats
// that attribute by index overwrite each other.
#[test]
fn gemini_multiple_parts_get_distinct_indices() {
    let codec = GeminiCodec::new();
    let frame = json!({
        "candidates": [{
            "content": {"role": "model", "parts": [
                {"text": "first"},
                {"text": "second"}
            ]}
        }]
    })
    .to_string();

    let events = codec.decode_event(&frame).expect("decode");
    let indices: Vec<u32> = events
        .iter()
        .filter_map(|e| match e {
            StreamEvent::TextDelta { index, .. } => Some(*index),
            _ => None,
        })
        .collect();

    assert_eq!(indices.len(), 2);
    assert_ne!(
        indices[0], indices[1],
        "the two parts' indices must not be the same"
    );
}

// In streaming a functionCall arrives as a complete object, so it should produce a start
// (with name) plus one complete args delta.
#[test]
fn gemini_decode_event_emits_function_call_pair() {
    let codec = GeminiCodec::new();
    let frame = json!({
        "candidates": [{
            "content": {"role": "model", "parts": [
                {"functionCall": {"name": "search", "args": {"q": "y"}}}
            ]}
        }]
    })
    .to_string();

    let events = codec.decode_event(&frame).expect("decode");

    let start = events
        .iter()
        .find_map(|e| match e {
            StreamEvent::ToolCallStart { id, name, .. } => Some((id.clone(), name.clone())),
            _ => None,
        })
        .expect("should produce ToolCallStart");
    assert_eq!(start.1, "search");
    assert!(
        start.0.starts_with("gemini_call_"),
        "id should be a deterministic synthesized value (not uuid), actual: {}",
        start.0
    );

    let args = events
        .iter()
        .find_map(|e| match e {
            StreamEvent::ToolCallDelta {
                arguments_delta, ..
            } => Some(arguments_delta.clone()),
            _ => None,
        })
        .expect("should produce ToolCallDelta");
    let parsed: Value = serde_json::from_str(&args).expect("args should be JSON");
    assert_eq!(parsed["q"], "y");
}

// Non-JSON / control frames must not produce an error.
#[test]
fn gemini_decode_event_ignores_noise() {
    let codec = GeminiCodec::new();
    for frame in ["", "[DONE]", "not json", "{\"no_candidates\":1}"] {
        let events = codec.decode_event(frame).expect("must not error");
        assert!(
            events.is_empty(),
            "{frame:?} should produce empty events, actual: {events:?}"
        );
    }
}

// ---------- Streaming encode ----------

// Tool arguments accumulate until the end, when the whole functionCall is emitted
// (Gemini's args are a whole object, not a delta).
#[test]
fn gemini_encoder_accumulates_tool_args_until_stop() {
    let codec = GeminiCodec::new();
    let mut enc = codec.stream_encoder();

    let mut out = Vec::new();
    out.extend(
        enc.encode_event(&StreamEvent::ToolCallStart {
            index: 0,
            id: "call_1".into(),
            name: "search".into(),
        })
        .unwrap(),
    );
    out.extend(
        enc.encode_event(&StreamEvent::ToolCallDelta {
            index: 0,
            arguments_delta: "{\"q\":".into(),
        })
        .unwrap(),
    );
    out.extend(
        enc.encode_event(&StreamEvent::ToolCallDelta {
            index: 0,
            arguments_delta: "\"w\"}".into(),
        })
        .unwrap(),
    );

    // No functionCall before the arguments are complete (half a JSON document makes
    // Gemini clients fail to parse).
    assert!(
        !frames_to_values(&out).iter().any(|v| v["candidates"][0]
            .get("content")
            .and_then(|c| c.get("parts"))
            .and_then(Value::as_array)
            .is_some_and(|p| p.iter().any(|x| x.get("functionCall").is_some()))),
        "no functionCall may be emitted while arguments accumulate"
    );

    let finish = enc
        .encode_event(&StreamEvent::MessageStop {
            reason: StopReason::ToolUse,
        })
        .unwrap();

    let fc = frames_to_values(&finish)
        .into_iter()
        .find_map(|v| {
            v["candidates"][0]["content"]["parts"]
                .as_array()
                .and_then(|p| p.first().cloned())
                .filter(|p| p.get("functionCall").is_some())
        })
        .expect("finish must emit a complete functionCall");

    assert_eq!(fc["functionCall"]["name"], "search");
    assert_eq!(
        fc["functionCall"]["args"]["q"], "w",
        "arguments must be joined into a complete object"
    );
}

// On an interrupted stream, finish must backfill a closing frame, and repeated calls are
// idempotent.
#[test]
fn gemini_encoder_finish_backfills_and_is_idempotent() {
    let codec = GeminiCodec::new();
    let mut enc = codec.stream_encoder();

    enc.encode_event(&StreamEvent::TextDelta {
        index: 0,
        text: "x".into(),
    })
    .unwrap();

    let out = enc.finish().expect("finish");
    let values = frames_to_values(&out);
    assert!(
        values
            .iter()
            .any(|v| v["candidates"][0]["finishReason"] == "STOP"),
        "finish must emit a closing frame, actual: {values:?}"
    );

    let again = enc.finish().expect("finish again");
    assert!(
        again.is_empty(),
        "repeated finish must not emit frames again, actual: {again:?}"
    );
}

#[test]
fn gemini_codec_reports_gemini_format() {
    assert_eq!(GeminiCodec::new().format(), Protocol::Gemini);
}

#[test]
fn usage_thoughts_tokens_are_preserved() {
    let codec = GeminiCodec;
    let body = serde_json::to_vec(&json!({
        "candidates": [{
            "content": {"role": "model", "parts": [{"text": "hi"}]},
            "finishReason": "STOP",
            "index": 0
        }],
        "usageMetadata": {
            "promptTokenCount": 10,
            "candidatesTokenCount": 20,
            "thoughtsTokenCount": 7
        }
    }))
    .unwrap();

    let resp = codec.decode_response(Bytes::from(body)).expect("decode");
    // IR completion is the total output: 20 candidates + 7 thoughts.
    assert_eq!(resp.usage.completion_tokens, 27);
    assert_eq!(resp.usage.reasoning_tokens, Some(7));

    // Gemini round-trip: candidatesTokenCount excludes thinking again.
    let out = codec.encode_response(&resp).expect("encode");
    let v: Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(v["usageMetadata"]["candidatesTokenCount"], 20);
    assert_eq!(v["usageMetadata"]["thoughtsTokenCount"], 7);
    // Total = prompt + completion = prompt + candidates + thoughts.
    assert_eq!(v["usageMetadata"]["totalTokenCount"], 37);
}
