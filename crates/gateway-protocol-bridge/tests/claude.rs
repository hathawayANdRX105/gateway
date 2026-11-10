//! Behavior tests for the Claude codec: request/response round-trips in both directions,
//! the three concrete G1 defects in streaming events, and block-boundary completion in the
//! stream encoder.
//!
//! Focus on `decode_event` and `stream_encoder`: the old implementation
//! (`adaptor::convert_claude_event_to_openai`) had three real defects here, and the cases
//! in this file target them specifically — if someone restores the old behavior, these
//! cases must fail.

use bytes::Bytes;
use gateway_protocol_bridge::adaptor::Protocol;
use gateway_protocol_bridge::claude::ClaudeCodec;
use gateway_protocol_bridge::format_codec::FormatCodec;
use gateway_protocol_bridge::ir::{
    ContentBlock, LlmResponse, Role, StopReason, StreamEvent, Usage,
};
use serde_json::{Value, json};

/// Split `data:` frame payloads into JSON values for assertions; `event:` lines are ignored.
fn frames_to_values(frames: &[Bytes]) -> Vec<Value> {
    let mut out = Vec::new();
    for frame in frames {
        let text = String::from_utf8_lossy(frame);
        for line in text.lines() {
            if let Some(data) = line.strip_prefix("data: ") {
                out.push(serde_json::from_str(data).expect("frame data must be valid JSON"));
            }
        }
    }
    out
}

/// Collect the frames whose `type` equals the given value.
fn frames_of_type(frames: &[Bytes], kind: &str) -> Vec<Value> {
    frames_to_values(frames)
        .into_iter()
        .filter(|v| v.get("type").and_then(Value::as_str) == Some(kind))
        .collect()
}

// ---------- Request direction ----------

// Bidirectional request round-trip: tool_use / tool_result / thinking blocks keep their
// content across the IR round-trip; guards against "text-only codec drops structured blocks".
#[test]
fn claude_request_roundtrip_preserves_tool_and_thinking_blocks() {
    let codec = ClaudeCodec::new();
    let body = serde_json::to_vec(&json!({
        "model": "claude-sonnet",
        "max_tokens": 512,
        "temperature": 0.3,
        "system": "be brief",
        "messages": [
            {"role": "user", "content": "hi"},
            {"role": "assistant", "content": [
                {"type": "text", "text": "calling"},
                {"type": "tool_use", "id": "tu_1", "name": "search", "input": {"q": "ferrite"}},
                {"type": "thinking", "thinking": "reasoning here"}
            ]},
            {"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "tu_1", "content": "found 3"}
            ]}
        ]
    }))
    .unwrap();

    let ir = codec.decode_request(Bytes::from(body)).expect("decode");
    assert_eq!(ir.model, "claude-sonnet");
    assert_eq!(ir.system.len(), 1);
    assert_eq!(ir.system[0].text, "be brief");
    assert_eq!(
        ir.sampling.as_ref().and_then(|s| s.max_tokens),
        Some(512),
        "max_tokens must go into sampling"
    );

    let assistant = &ir.messages[1];
    assert_eq!(assistant.role, Role::Assistant);
    assert!(
        assistant
            .content
            .iter()
            .any(|b| matches!(b, ContentBlock::ToolUse { name, .. } if name == "search")),
        "tool_use must become a ToolUse block, actual: {:?}",
        assistant.content
    );
    assert!(
        assistant
            .content
            .iter()
            .any(|b| matches!(b, ContentBlock::Thinking { text } if text == "reasoning here")),
        "thinking must become a Thinking block, actual: {:?}",
        assistant.content
    );

    let user = &ir.messages[2];
    assert!(
        user.content.iter().any(
            |b| matches!(b, ContentBlock::ToolResult { tool_use_id, content }
                if tool_use_id == "tu_1" && content == "found 3")
        ),
        "tool_result must become a ToolResult block, actual: {:?}",
        user.content
    );

    // Back to the Claude shape: the key structures must be present.
    let back = codec.encode_request(&ir).expect("encode");
    let out: Value = serde_json::from_slice(&back).unwrap();
    assert_eq!(out["model"], "claude-sonnet");
    assert_eq!(out["max_tokens"], 512);
    assert_eq!(out["system"][0]["text"], "be brief");
    assert_eq!(out["messages"][1]["content"][1]["type"], "tool_use");
    assert_eq!(out["messages"][2]["content"][0]["tool_use_id"], "tu_1");
}

// A missing max_tokens must get a default: Claude requires max_tokens, and omission
// yields 400. Guards against "send the IR as-is when it has no max_tokens".
#[test]
fn claude_encode_request_fills_required_max_tokens() {
    let codec = ClaudeCodec::new();
    let body = serde_json::to_vec(&json!({
        "model": "claude-sonnet",
        "messages": [{"role": "user", "content": "hi"}]
    }))
    .unwrap();

    let ir = codec.decode_request(Bytes::from(body)).expect("decode");
    assert!(ir.sampling.is_none(), "None when no sampling parameters");

    let out: Value = serde_json::from_slice(&codec.encode_request(&ir).unwrap()).unwrap();
    assert!(
        out["max_tokens"].as_u64().is_some_and(|v| v > 0),
        "Claude's required max_tokens must be filled with a default, actual: {out}"
    );
}

// ---------- G1 core regressions ----------

/// Build a complete Claude SSE sequence (per-frame data payloads) containing a tool call.
fn claude_tool_stream_frames() -> Vec<String> {
    vec![
        json!({
            "type": "message_start",
            "message": {
                "id": "msg_1", "type": "message", "role": "assistant", "model": "claude-sonnet",
                "content": [], "usage": {"input_tokens": 11, "output_tokens": 0}
            }
        })
        .to_string(),
        json!({
            "type": "content_block_start",
            "index": 1,
            "content_block": {"type": "tool_use", "id": "tu_1", "name": "search", "input": {}}
        })
        .to_string(),
        json!({
            "type": "content_block_delta",
            "index": 1,
            "delta": {"type": "input_json_delta", "partial_json": "{\"q\":"}
        })
        .to_string(),
        json!({
            "type": "content_block_delta",
            "index": 1,
            "delta": {"type": "input_json_delta", "partial_json": "\"ferrite\"}"}
        })
        .to_string(),
        json!({"type": "content_block_stop", "index": 1}).to_string(),
        json!({
            "type": "message_delta",
            "delta": {"stop_reason": "tool_use"},
            "usage": {"output_tokens": 7}
        })
        .to_string(),
        json!({"type": "message_stop"}).to_string(),
    ]
}

// G1-a: `content_block_start`(tool_use) must produce ToolCallStart carrying id and name.
// The old implementation had no such match arm; the event was silently dropped and
// clients never learned the tool identity.
#[test]
fn claude_decode_event_emits_tool_call_start_from_content_block_start() {
    let codec = ClaudeCodec::new();
    let events = codec
        .decode_event(&claude_tool_stream_frames()[1])
        .expect("decode content_block_start");

    let start = events
        .iter()
        .find(|e| matches!(e, StreamEvent::ToolCallStart { .. }))
        .unwrap_or_else(|| {
            panic!("content_block_start(tool_use) must produce ToolCallStart, actual: {events:?}")
        });

    match start {
        StreamEvent::ToolCallStart { index, id, name } => {
            assert_eq!(*index, 1, "index must take the event's own value");
            assert_eq!(id, "tu_1", "tool id must be included");
            assert_eq!(name, "search", "tool name must be included");
        }
        other => panic!("expected ToolCallStart, actual: {other:?}"),
    }
}

// G1-b: tool fragment indices must come from the event, not a hard-coded 0.
// The old implementation pinned index 0, so parallel tool calls all collapsed onto the
// same index and overwrote each other.
#[test]
fn claude_tool_fragments_keep_their_own_index() {
    let codec = ClaudeCodec::new();
    let frames = claude_tool_stream_frames();

    let mut indices = Vec::new();
    let mut args = String::new();
    for frame in &frames {
        for ev in codec.decode_event(frame).expect("decode") {
            if let StreamEvent::ToolCallDelta {
                index,
                arguments_delta,
            } = ev
            {
                indices.push(index);
                args.push_str(&arguments_delta);
            }
        }
    }

    assert_eq!(
        indices,
        vec![1, 1],
        "Both fragment indices must be 1 (old implementation hard-coded 0)"
    );
    let parsed: Value =
        serde_json::from_str(&args).expect("Reassembled arguments must be valid JSON");
    assert_eq!(
        parsed["q"], "ferrite",
        "Fragments must be reassembled into complete JSON"
    );
}

// G1-b strengthened: fragments of two parallel tool calls must not cross-contaminate.
// This is the fatal case for a hard-coded index 0 — both calls merge into one argument set.
#[test]
fn claude_parallel_tool_calls_do_not_cross_contaminate() {
    let codec = ClaudeCodec::new();
    let frames = [
        json!({"type": "content_block_start", "index": 1,
               "content_block": {"type": "tool_use", "id": "a", "name": "first"}}),
        json!({"type": "content_block_start", "index": 2,
               "content_block": {"type": "tool_use", "id": "b", "name": "second"}}),
        json!({"type": "content_block_delta", "index": 1,
               "delta": {"type": "input_json_delta", "partial_json": "{\"k\":1}"}}),
        json!({"type": "content_block_delta", "index": 2,
               "delta": {"type": "input_json_delta", "partial_json": "{\"k\":2}"}}),
    ];

    let mut by_index: std::collections::BTreeMap<u32, String> = std::collections::BTreeMap::new();
    for frame in &frames {
        for ev in codec.decode_event(&frame.to_string()).expect("decode") {
            if let StreamEvent::ToolCallDelta {
                index,
                arguments_delta,
            } = ev
            {
                by_index
                    .entry(index)
                    .or_default()
                    .push_str(&arguments_delta);
            }
        }
    }

    assert_eq!(by_index.get(&1).map(String::as_str), Some("{\"k\":1}"));
    assert_eq!(by_index.get(&2).map(String::as_str), Some("{\"k\":2}"));
}

// G1-c: input usage from message_start must not be lost.
// The old implementation discarded the whole block, so metering missed prompt-side tokens.
#[test]
fn claude_message_start_usage_is_preserved() {
    let codec = ClaudeCodec::new();
    let events = codec
        .decode_event(&claude_tool_stream_frames()[0])
        .expect("decode message_start");

    assert!(
        matches!(events.first(), Some(StreamEvent::MessageStart { id, .. }) if id == "msg_1"),
        "The first event should be MessageStart, actual: {events:?}"
    );

    let usage = events
        .iter()
        .find_map(|e| match e {
            StreamEvent::MessageDelta { usage: Some(u) } => Some(u.clone()),
            _ => None,
        })
        .unwrap_or_else(|| {
            panic!("message_start input usage must be preserved, actual: {events:?}")
        });

    assert_eq!(
        usage.prompt_tokens, 11,
        "input_tokens must map to prompt_tokens"
    );
}

// Unknown/control frames must not fail the whole stream: SSE streams carry non-content
// frames such as ping. Guards against "one unparseable frame returns Err and kills the stream".
#[test]
fn claude_non_content_frames_are_ignored() {
    let codec = ClaudeCodec::new();
    for frame in [
        "",
        "not json at all",
        "{\"type\":\"ping\"}",
        "{\"no_type\":1}",
    ] {
        let events = codec.decode_event(frame).expect("Must not error");
        assert!(
            events.is_empty(),
            "Non-content frames should produce empty events, actual: {events:?}"
        );
    }
}

// ---------- Stream encoder ----------

// The encoder must fill in Claude's block boundaries: the IR stream has only deltas with
// an index, while Claude requires start/delta/stop as a set; a missing start breaks parsers.
#[test]
fn claude_encoder_wraps_tool_deltas_with_block_boundaries() {
    let codec = ClaudeCodec::new();
    let mut enc = codec.stream_encoder();

    let mut frames = Vec::new();
    frames.extend(
        enc.encode_event(&StreamEvent::MessageStart {
            id: "msg_1".into(),
            model: "claude-sonnet".into(),
        })
        .unwrap(),
    );
    frames.extend(
        enc.encode_event(&StreamEvent::ToolCallStart {
            index: 1,
            id: "tu_1".into(),
            name: "search".into(),
        })
        .unwrap(),
    );
    frames.extend(
        enc.encode_event(&StreamEvent::ToolCallDelta {
            index: 1,
            arguments_delta: "{\"q\":".into(),
        })
        .unwrap(),
    );
    frames.extend(
        enc.encode_event(&StreamEvent::ToolCallDelta {
            index: 1,
            arguments_delta: "\"x\"}".into(),
        })
        .unwrap(),
    );
    frames.extend(
        enc.encode_event(&StreamEvent::MessageStop {
            reason: StopReason::ToolUse,
        })
        .unwrap(),
    );

    let starts = frames_of_type(&frames, "content_block_start");
    assert_eq!(
        starts.len(),
        1,
        "Tool block should only be opened once, actual: {starts:?}"
    );
    assert_eq!(starts[0]["index"], 1);
    assert_eq!(starts[0]["content_block"]["type"], "tool_use");
    assert_eq!(starts[0]["content_block"]["id"], "tu_1");
    assert_eq!(starts[0]["content_block"]["name"], "search");

    let deltas = frames_of_type(&frames, "content_block_delta");
    assert_eq!(
        deltas.len(),
        2,
        "The two argument fragments should each be a separate frame"
    );
    let recombined: String = deltas
        .iter()
        .map(|d| d["delta"]["partial_json"].as_str().unwrap())
        .collect();
    assert_eq!(recombined, "{\"q\":\"x\"}");
    for d in &deltas {
        assert_eq!(d["index"], 1, "Delta's index must match the block");
        assert_eq!(d["delta"]["type"], "input_json_delta");
    }

    let stops = frames_of_type(&frames, "content_block_stop");
    assert_eq!(stops.len(), 1, "Block must be closed, actual: {stops:?}");
    assert_eq!(
        frames_of_type(&frames, "message_delta")[0]["delta"]["stop_reason"],
        "tool_use",
        "StopReason::ToolUse must map back to Claude's tool_use"
    );
    assert_eq!(frames_of_type(&frames, "message_stop").len(), 1);
}

// With only tool fragments and no ToolCallStart, the encoder must still open the block.
// Guards against "upstream gave no id/name, so the whole tool call is silently swallowed".
#[test]
fn claude_encoder_opens_block_even_without_tool_start() {
    let codec = ClaudeCodec::new();
    let mut enc = codec.stream_encoder();

    let frames = enc
        .encode_event(&StreamEvent::ToolCallDelta {
            index: 1,
            arguments_delta: "{}".into(),
        })
        .expect("encode");

    let starts = frames_of_type(&frames, "content_block_start");
    assert_eq!(
        starts.len(),
        1,
        "A tool_use block must be opened even without id/name, actual: {starts:?}"
    );
    assert_eq!(starts[0]["content_block"]["type"], "tool_use");
    assert_eq!(
        frames_of_type(&frames, "content_block_delta").len(),
        1,
        "The fragment itself must also be emitted"
    );
}

// finish() must close out: fill in start if needed, close blocks, send message_stop; repeated
// calls must not emit twice. Guards against "stream ends early with no message_stop".
#[test]
fn claude_encoder_finish_closes_open_stream_idempotently() {
    let codec = ClaudeCodec::new();
    let mut enc = codec.stream_encoder();

    // Content events fill in message_start (Claude requires a start before content frames).
    let head = enc
        .encode_event(&StreamEvent::TextDelta {
            index: 0,
            text: "hello".into(),
        })
        .unwrap();
    assert_eq!(
        frames_of_type(&head, "message_start").len(),
        1,
        "The first content event must prepend message_start"
    );

    // finish only closes out: close open blocks and end the message; no message_start resend.
    let out = enc.finish().expect("finish");
    assert_eq!(
        frames_of_type(&out, "message_start").len(),
        0,
        "message_start has already been sent; finish must not resend"
    );
    assert_eq!(
        frames_of_type(&out, "content_block_stop").len(),
        1,
        "Opened text block must be closed"
    );
    assert_eq!(frames_of_type(&out, "message_stop").len(), 1);

    let again = enc.finish().expect("finish idempotent");
    assert!(
        again.is_empty(),
        "Repeated finish must not emit frames twice, actual: {again:?}"
    );
}

// ---------- Response direction ----------

// Non-streaming response: tool_use must not be dropped (the old code extracted text only).
#[test]
fn claude_decode_response_keeps_tool_use_blocks() {
    let codec = ClaudeCodec::new();
    let body = serde_json::to_vec(&json!({
        "id": "msg_9",
        "type": "message",
        "role": "assistant",
        "model": "claude-sonnet",
        "content": [
            {"type": "text", "text": "using a tool"},
            {"type": "tool_use", "id": "tu_9", "name": "search", "input": {"q": "x"}}
        ],
        "stop_reason": "tool_use",
        "usage": {"input_tokens": 5, "output_tokens": 9}
    }))
    .unwrap();

    let resp = codec.decode_response(Bytes::from(body)).expect("decode");
    assert_eq!(resp.id, "msg_9");
    assert_eq!(resp.stop_reason, StopReason::ToolUse);
    assert_eq!(resp.usage.prompt_tokens, 5);
    assert_eq!(resp.usage.completion_tokens, 9);
    assert!(
        resp.outputs
            .iter()
            .any(|b| matches!(b, ContentBlock::ToolUse { id, .. } if id == "tu_9")),
        "tool_use in a non-streaming response must not be lost, actual: {:?}",
        resp.outputs
    );
}

// Response-direction round-trip: IR → Claude body uses Claude's names for usage fields.
#[test]
fn claude_encode_response_uses_claude_field_names() {
    let codec = ClaudeCodec::new();
    let resp = LlmResponse {
        id: "msg_1".into(),
        model: "claude-sonnet".into(),
        outputs: vec![ContentBlock::Text { text: "hi".into() }],
        usage: Usage {
            prompt_tokens: 3,
            completion_tokens: 4,
            cached_tokens: None,
            ..Default::default()
        },
        stop_reason: StopReason::Stop,
    };

    let out: Value = serde_json::from_slice(&codec.encode_response(&resp).unwrap()).unwrap();
    assert_eq!(out["type"], "message");
    assert_eq!(out["role"], "assistant");
    assert_eq!(out["usage"]["input_tokens"], 3);
    assert_eq!(out["usage"]["output_tokens"], 4);
    assert_eq!(out["stop_reason"], "end_turn");
}

// ---------- Format identity ----------

#[test]
fn claude_codec_reports_claude_format() {
    assert_eq!(ClaudeCodec::new().format(), Protocol::Claude);
}

#[test]
fn usage_cache_creation_is_preserved() {
    let codec = ClaudeCodec;
    let body = serde_json::to_vec(&json!({
        "id": "msg_u",
        "type": "message",
        "role": "assistant",
        "model": "claude-x",
        "content": [{"type": "text", "text": "hi"}],
        "stop_reason": "end_turn",
        "usage": {
            "input_tokens": 10,
            "output_tokens": 20,
            "cache_read_input_tokens": 4,
            "cache_creation_input_tokens": 6
        }
    }))
    .unwrap();

    let resp = codec.decode_response(Bytes::from(body)).expect("decode");
    // IR prompt is the total input: 10 + cache_read 4 + cache_creation 6.
    assert_eq!(resp.usage.prompt_tokens, 20);
    assert_eq!(resp.usage.completion_tokens, 20);
    assert_eq!(resp.usage.cached_tokens, Some(4));
    assert_eq!(resp.usage.cache_creation_tokens, Some(6));
    assert_eq!(resp.usage.reasoning_tokens, None);

    // Claude round-trip: input_tokens is the total minus the cache subsets.
    let out = codec.encode_response(&resp).expect("encode");
    let v: Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(v["usage"]["input_tokens"], 10);
    assert_eq!(v["usage"]["cache_read_input_tokens"], 4);
    assert_eq!(v["usage"]["cache_creation_input_tokens"], 6);
}

/// The Claude stream encoder used to emit only `output_tokens` in `message_delta`,
/// dropping the input-side usage the decoder had just merged from `message_start`.
/// The IR carries one merged Usage, so the delta mirrors the input side when known.
#[test]
fn claude_stream_message_delta_carries_full_usage() {
    let codec = ClaudeCodec;
    let mut enc = codec.stream_encoder();
    let ev = StreamEvent::MessageDelta {
        usage: Some(Usage {
            prompt_tokens: 20,
            completion_tokens: 30,
            cached_tokens: Some(4),
            cache_creation_tokens: Some(6),
            reasoning_tokens: None,
        }),
    };

    let frames = enc.encode_event(&ev).expect("encode");
    let text = frames
        .iter()
        .map(|f| String::from_utf8_lossy(f).into_owned())
        .collect::<String>();
    assert!(text.contains("\"input_tokens\":10"), "actual: {text}");
    assert!(text.contains("\"output_tokens\":30"), "actual: {text}");
    assert!(
        text.contains("\"cache_read_input_tokens\":4"),
        "actual: {text}"
    );
    assert!(
        text.contains("\"cache_creation_input_tokens\":6"),
        "actual: {text}"
    );
}

/// An output-only delta (prompt 0) must not claim `input_tokens: 0`.
#[test]
fn claude_stream_message_delta_output_only_omits_input() {
    let codec = ClaudeCodec;
    let mut enc = codec.stream_encoder();
    let ev = StreamEvent::MessageDelta {
        usage: Some(Usage {
            prompt_tokens: 0,
            completion_tokens: 9,
            ..Default::default()
        }),
    };

    let frames = enc.encode_event(&ev).expect("encode");
    let delta = frames
        .iter()
        .map(|f| String::from_utf8_lossy(f).into_owned())
        .find(|f| f.contains("message_delta"))
        .expect("message_delta frame");
    // The message_start prelude always carries input_tokens:0; only the delta matters.
    assert!(delta.contains("\"output_tokens\":9"), "actual: {delta}");
    assert!(!delta.contains("input_tokens"), "actual: {delta}");
}

/// Malformed upstream (cache subset larger than the total) must clamp at zero instead
/// of wrapping to a huge input count.
#[test]
fn claude_encode_usage_saturates_when_cache_exceeds_prompt() {
    let codec = ClaudeCodec;
    let resp = LlmResponse {
        id: "msg_c".into(),
        model: "claude-x".into(),
        outputs: vec![ContentBlock::Text { text: "hi".into() }],
        usage: Usage {
            prompt_tokens: 10,
            completion_tokens: 5,
            cached_tokens: Some(100),
            cache_creation_tokens: None,
            reasoning_tokens: None,
        },
        stop_reason: StopReason::Stop,
    };

    let out: Value = serde_json::from_slice(&codec.encode_response(&resp).unwrap()).unwrap();
    assert_eq!(out["usage"]["input_tokens"], 0);
}

#[test]
fn claude_citations_delta_survives_as_raw() {
    let codec = ClaudeCodec;
    let event = json!({
        "type": "content_block_delta",
        "index": 0,
        "delta": {
            "type": "citations_delta",
            "citation": {
                "type": "char_location",
                "cited_text": "hello",
                "document_index": 0,
                "start_char_index": 0,
                "end_char_index": 5
            }
        }
    });

    let events = codec.decode_event(&event.to_string()).expect("decode");
    assert_eq!(events.len(), 1, "citations_delta must not be dropped");
    let StreamEvent::Raw { data } = &events[0] else {
        panic!("expected Raw, got {:?}", events[0]);
    };
    assert_eq!(data["delta"]["type"], "citations_delta");

    let mut enc = codec.stream_encoder();
    // Prepend the message_start frame to verify the raw frame is re-emitted verbatim
    // after the required stream prelude.
    let frames = enc.encode_event(&events[0]).expect("encode");
    let text = frames
        .iter()
        .map(|f| String::from_utf8_lossy(f).into_owned())
        .collect::<String>();
    assert!(
        text.contains("event: content_block_delta"),
        "actual: {text}"
    );
    assert!(text.contains("citations_delta"), "actual: {text}");
}

#[test]
fn claude_unknown_event_survives_as_raw() {
    let codec = ClaudeCodec;
    let event = json!({"type": "future_event", "payload": {"x": 1}});

    let events = codec.decode_event(&event.to_string()).expect("decode");
    assert_eq!(events.len(), 1, "unknown events must not be dropped");
    let StreamEvent::Raw { data } = &events[0] else {
        panic!("expected Raw, got {:?}", events[0]);
    };
    assert_eq!(data["type"], "future_event");
}
