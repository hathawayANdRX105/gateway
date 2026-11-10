//! Behavior tests for the `OpenAI` Chat Completions codec.
//!
//! Two maps of behavior: lossless bidirectional round-trip (the hard contract of IR as the hub),
//! shape constraints of streaming output — id stable across the stream, non-empty choices on
//! usage frames, tool fragments keeping their own index. The last three correspond to
//! counterexamples already recorded in the repo (the old Gemini side generating a fresh uuid
//! per chunk, usage frames with `choices: []`, tool index hard-coded to 0).

use bytes::Bytes;
use gateway_protocol_bridge::adaptor::Protocol;
use gateway_protocol_bridge::format_codec::FormatCodec;
use gateway_protocol_bridge::ir::{
    ContentBlock, LlmRequest, LlmResponse, Role, SamplingParams, StopReason, StreamEvent, Usage,
};
use gateway_protocol_bridge::openai::OpenAiCodec;
use serde_json::{Map, Value, json};

/// Split encoder output frames into JSON (skipping `[DONE]`).
fn frames_to_values(frames: &[Bytes]) -> Vec<Value> {
    frames
        .iter()
        .filter_map(|f| {
            let text = String::from_utf8_lossy(f);
            let line = text.lines().find(|l| l.starts_with("data: "))?;
            let data = line.strip_prefix("data: ")?;
            if data == "[DONE]" {
                return None;
            }
            serde_json::from_str(data).ok()
        })
        .collect()
}

fn has_done(frames: &[Bytes]) -> bool {
    frames
        .iter()
        .any(|f| String::from_utf8_lossy(f).contains("[DONE]"))
}

fn mk_request(model: &str) -> LlmRequest {
    LlmRequest {
        model: model.into(),
        messages: vec![],
        system: vec![],
        tools: vec![],
        tool_choice: None,
        sampling: None,
        stream: false,
        extra: Map::new(),
    }
}

// ---------- request direction ----------

// Request round-trip: messages / tools / tool_choice / sampling params survive the IR round
// trip. Guards against "a new codec that only moves model+messages, eating tools and session
// params".
#[test]
fn openai_request_roundtrip_preserves_tools_and_sampling() {
    let codec = OpenAiCodec::new();
    let body = serde_json::to_vec(&json!({
        "model": "gpt-x",
        "temperature": 0.5,
        "top_p": 0.9,
        "max_tokens": 256,
        "messages": [
            {"role": "system", "content": "be terse"},
            {"role": "user", "content": "hi"}
        ],
        "tools": [{
            "type": "function",
            "function": {
                "name": "search",
                "description": "find things",
                "parameters": {"type": "object", "properties": {"q": {"type": "string"}}}
            }
        }],
        "tool_choice": "auto"
    }))
    .unwrap();

    let ir = codec.decode_request(Bytes::from(body)).expect("decode");
    assert_eq!(ir.model, "gpt-x");
    assert_eq!(
        ir.system.len(),
        1,
        "system message must be lifted into IR's system"
    );
    assert_eq!(ir.system[0].text, "be terse");
    assert_eq!(ir.messages.len(), 1, "system must not remain in messages");
    assert_eq!(ir.tools.len(), 1);
    assert_eq!(ir.tools[0].name, "search");
    assert_eq!(ir.tools[0].input_schema["type"], "object");
    let s = ir.sampling.as_ref().expect("sampling parameters");
    assert_eq!(s.temperature, Some(0.5));
    assert_eq!(s.top_p, Some(0.9));
    assert_eq!(s.max_tokens, Some(256));

    // Back to Chat shape: system is re-inserted as the first message, tools return into the
    // function wrapper.
    let out: Value = serde_json::from_slice(&codec.encode_request(&ir).unwrap()).unwrap();
    assert_eq!(out["model"], "gpt-x");
    assert_eq!(out["messages"][0]["role"], "system");
    assert_eq!(out["messages"][0]["content"], "be terse");
    assert_eq!(out["tools"][0]["function"]["name"], "search");
    assert_eq!(out["tool_choice"], "auto");
    assert_eq!(out["temperature"], 0.5);
}

// Unknown top-level fields must not be dropped: vendor extension parameters have to reach
// the upstream as-is.
#[test]
fn openai_request_keeps_unknown_fields() {
    let codec = OpenAiCodec::new();
    let body = serde_json::to_vec(&json!({
        "model": "gpt-x",
        "messages": [{"role": "user", "content": "hi"}],
        "vendor_specific_flag": {"nested": true}
    }))
    .unwrap();

    let ir = codec.decode_request(Bytes::from(body)).expect("decode");
    let out: Value = serde_json::from_slice(&codec.encode_request(&ir).unwrap()).unwrap();
    assert_eq!(
        out["vendor_specific_flag"]["nested"], true,
        "unknown fields must round-trip, actual: {out}"
    );
}

// Streaming requests must inject stream_options.include_usage (otherwise the upstream returns
// no usage and billing lacks data); an explicit client-provided stream_options must not be
// overwritten.
#[test]
fn openai_stream_request_injects_include_usage_without_clobbering() {
    let codec = OpenAiCodec::new();

    let ir = mk_request("gpt-x");
    let mut ir = LlmRequest { stream: true, ..ir };
    let out: Value = serde_json::from_slice(&codec.encode_request(&ir).unwrap()).unwrap();
    assert_eq!(
        out["stream_options"]["include_usage"], true,
        "streaming request should inject include_usage"
    );

    // Client-provided stream_options wins.
    ir.extra
        .insert("stream_options".into(), json!({"include_usage": false}));
    let out: Value = serde_json::from_slice(&codec.encode_request(&ir).unwrap()).unwrap();
    assert_eq!(
        out["stream_options"]["include_usage"], false,
        "client's explicit setting must not be overwritten"
    );
}

// A tool message's tool_call_id must become a ToolResult association, not plain text that
// loses the link.
#[test]
fn openai_tool_message_maps_to_tool_result_block() {
    let codec = OpenAiCodec::new();
    let body = serde_json::to_vec(&json!({
        "model": "gpt-x",
        "messages": [
            {"role": "assistant", "content": null, "tool_calls": [
                {"id": "call_1", "type": "function",
                 "function": {"name": "search", "arguments": "{\"q\":\"ferrite\"}"}}
            ]},
            {"role": "tool", "tool_call_id": "call_1", "content": "3 results"}
        ]
    }))
    .unwrap();

    let ir = codec.decode_request(Bytes::from(body)).expect("decode");

    let assistant = &ir.messages[0];
    assert_eq!(assistant.role, Role::Assistant);
    let tool_use = assistant
        .content
        .iter()
        .find_map(|b| match b {
            ContentBlock::ToolUse { id, name, input } => {
                Some((id.clone(), name.clone(), input.clone()))
            }
            _ => None,
        })
        .expect("assistant's tool_calls must become ToolUse");
    assert_eq!(tool_use.0, "call_1");
    assert_eq!(tool_use.1, "search");
    assert_eq!(
        tool_use.2["q"], "ferrite",
        "arguments string must be parsed into JSON"
    );

    let tool_msg = &ir.messages[1];
    assert_eq!(tool_msg.role, Role::Tool);
    assert!(
        tool_msg.content.iter().any(
            |b| matches!(b, ContentBlock::ToolResult { tool_use_id, content }
                if tool_use_id == "call_1" && content == "3 results")
        ),
        "tool message must become ToolResult associated with call_1, actual: {:?}",
        tool_msg.content
    );
}

// A data URL image must become an Image block (not Unknown) for cross-format image
// conversion to work.
#[test]
fn openai_data_url_image_becomes_image_block() {
    let codec = OpenAiCodec::new();
    let body = serde_json::to_vec(&json!({
        "model": "gpt-x",
        "messages": [{"role": "user", "content": [
            {"type": "text", "text": "look"},
            {"type": "image_url", "image_url": {"url": "data:image/png;base64,AAAB"}}
        ]}]
    }))
    .unwrap();

    let ir = codec.decode_request(Bytes::from(body)).expect("decode");
    let image = ir.messages[0]
        .content
        .iter()
        .find_map(|b| match b {
            ContentBlock::Image { media_type, data } => Some((media_type.clone(), data.clone())),
            _ => None,
        })
        .expect("data URL image must become an Image block");

    assert_eq!(image.0, "image/png");
    assert_eq!(
        image.1, "AAAB",
        "base64 payload must strip the data URL prefix"
    );
}

// A remote URL image cannot be expressed in base64 form; keep it as-is rather than dropping
// it.
#[test]
fn openai_remote_url_image_survives_as_unknown() {
    let codec = OpenAiCodec::new();
    let body = serde_json::to_vec(&json!({
        "model": "gpt-x",
        "messages": [{"role": "user", "content": [
            {"type": "image_url", "image_url": {"url": "https://example.com/a.png"}}
        ]}]
    }))
    .unwrap();

    let ir = codec.decode_request(Bytes::from(body)).expect("decode");
    assert!(
        ir.messages[0]
            .content
            .iter()
            .any(|b| matches!(b, ContentBlock::Unknown { raw }
                if raw["image_url"]["url"] == "https://example.com/a.png")),
        "remote image URL must be retained via escape hatch, actual: {:?}",
        ir.messages[0].content
    );
}

// ---------- response direction ----------

#[test]
fn openai_decode_response_maps_text_tools_and_usage() {
    let codec = OpenAiCodec::new();
    let body = serde_json::to_vec(&json!({
        "id": "chatcmpl-1",
        "model": "gpt-x",
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": "using a tool",
                "tool_calls": [{"id": "call_9", "type": "function",
                    "function": {"name": "search", "arguments": "{\"q\":\"z\"}"}}]
            },
            "finish_reason": "tool_calls"
        }],
        "usage": {"prompt_tokens": 12, "completion_tokens": 4,
                  "prompt_tokens_details": {"cached_tokens": 5}}
    }))
    .unwrap();

    let resp = codec.decode_response(Bytes::from(body)).expect("decode");
    assert_eq!(resp.id, "chatcmpl-1");
    assert_eq!(resp.stop_reason, StopReason::ToolUse);
    assert_eq!(resp.usage.prompt_tokens, 12);
    assert_eq!(resp.usage.completion_tokens, 4);
    assert_eq!(resp.usage.cached_tokens, Some(5));
    assert!(
        resp.outputs
            .iter()
            .any(|b| matches!(b, ContentBlock::ToolUse { id, .. } if id == "call_9")),
        "tool_calls in response must become ToolUse, actual: {:?}",
        resp.outputs
    );
}

#[test]
fn openai_encode_response_uses_chat_shape() {
    let codec = OpenAiCodec::new();
    let resp = LlmResponse {
        id: "chatcmpl-2".into(),
        model: "gpt-x".into(),
        outputs: vec![
            ContentBlock::Text { text: "hi".into() },
            ContentBlock::ToolUse {
                id: "call_9".into(),
                name: "search".into(),
                input: json!({"q": "z"}),
            },
        ],
        usage: Usage {
            prompt_tokens: 1,
            completion_tokens: 2,
            cached_tokens: None,
            ..Default::default()
        },
        stop_reason: StopReason::ToolUse,
    };

    let out: Value = serde_json::from_slice(&codec.encode_response(&resp).unwrap()).unwrap();
    assert_eq!(out["object"], "chat.completion");
    assert_eq!(
        out["choices"][0]["message"]["tool_calls"][0]["id"],
        "call_9"
    );
    assert_eq!(out["choices"][0]["finish_reason"], "tool_calls");
    assert_eq!(out["usage"]["total_tokens"], 3);
}

// ---------- streaming decode ----------

// When one frame carries both choices and usage (the final-frame shape of include_usage),
// both kinds of event must be produced. Guards against "taking only choices and dropping the
// streaming usage entirely" — billing would lose a whole stream's data.
#[test]
fn openai_decode_event_emits_usage_alongside_choices() {
    let codec = OpenAiCodec::new();
    let frame = json!({
        "id": "chatcmpl-1",
        "choices": [{"index": 0, "delta": {"content": "hi"}, "finish_reason": "stop"}],
        "usage": {"prompt_tokens": 7, "completion_tokens": 2}
    })
    .to_string();

    let events = codec.decode_event(&frame).expect("decode");
    assert!(
        events
            .iter()
            .any(|e| matches!(e, StreamEvent::TextDelta { text, .. } if text == "hi")),
        "text deltas must be produced, actual: {events:?}"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, StreamEvent::MessageStop { .. })),
        "finish_reason must produce MessageStop, actual: {events:?}"
    );
    assert!(
        events.iter().any(
            |e| matches!(e, StreamEvent::MessageDelta { usage: Some(u) } if u.prompt_tokens == 7)
        ),
        "usage in the same frame must also be produced, actual: {events:?}"
    );
}

// `[DONE]` is a control frame: not content, and not an error.
#[test]
fn openai_decode_event_ignores_done_and_garbage() {
    let codec = OpenAiCodec::new();
    for frame in ["[DONE]", "", "   ", "not json"] {
        let events = codec.decode_event(frame).expect("must not error");
        assert!(
            events.is_empty(),
            "{frame:?} should produce empty events, actual: {events:?}"
        );
    }
}

// A tool fragment's index must come from the chunk itself. Hard-coding it would tangle
// parallel calls together.
#[test]
fn openai_decode_event_keeps_tool_call_indices() {
    let codec = OpenAiCodec::new();
    let frames = [
        json!({"choices": [{"index": 0, "delta": {"tool_calls": [
            {"index": 0, "id": "c0", "type": "function", "function": {"name": "a", "arguments": ""}}
        ]}, "finish_reason": null}]}),
        json!({"choices": [{"index": 0, "delta": {"tool_calls": [
            {"index": 1, "id": "c1", "type": "function", "function": {"name": "b", "arguments": ""}}
        ]}, "finish_reason": null}]}),
        json!({"choices": [{"index": 0, "delta": {"tool_calls": [
            {"index": 1, "function": {"arguments": "{\"k\":"}}
        ]}, "finish_reason": null}]}),
        json!({"choices": [{"index": 0, "delta": {"tool_calls": [
            {"index": 1, "function": {"arguments": "1}"}}
        ]}, "finish_reason": null}]}),
    ];

    let mut starts = Vec::new();
    let mut args_by_index: std::collections::BTreeMap<u32, String> =
        std::collections::BTreeMap::new();
    for f in &frames {
        for ev in codec.decode_event(&f.to_string()).expect("decode") {
            match ev {
                StreamEvent::ToolCallStart { index, name, .. } => starts.push((index, name)),
                StreamEvent::ToolCallDelta {
                    index,
                    arguments_delta,
                } => args_by_index
                    .entry(index)
                    .or_default()
                    .push_str(&arguments_delta),
                _ => {}
            }
        }
    }

    assert_eq!(starts, vec![(0, "a".to_string()), (1, "b".to_string())]);
    assert_eq!(args_by_index.get(&1).map(String::as_str), Some("{\"k\":1}"));
    assert!(
        !args_by_index.contains_key(&0),
        "index 0 fragment should not appear (it has no delta)"
    );
}

// ---------- streaming encode ----------

// **Hard counterexample**: chunk id is stable across the whole stream. The old Gemini side
// generated a fresh uuid per chunk, so clients and stream aggregation treated one completion
// as several. Here every frame's id is collected and asserted equal.
#[test]
fn openai_encoder_keeps_chunk_id_stable_across_stream() {
    let codec = OpenAiCodec::new();
    let mut enc = codec.stream_encoder();

    let mut frames = Vec::new();
    frames.extend(
        enc.encode_event(&StreamEvent::MessageStart {
            id: "chatcmpl-fixed".into(),
            model: "gpt-x".into(),
        })
        .unwrap(),
    );
    for text in ["a", "b", "c"] {
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
        "stream id must be consistent, actual: {ids:?}"
    );
}

// A usage frame's choices must be non-empty: `choices: []` is rejected by most clients.
#[test]
fn openai_encoder_usage_chunk_has_non_empty_choices() {
    let codec = OpenAiCodec::new();
    let mut enc = codec.stream_encoder();

    let frames = enc
        .encode_event(&StreamEvent::MessageDelta {
            usage: Some(Usage {
                prompt_tokens: 5,
                completion_tokens: 6,
                cached_tokens: None,
                ..Default::default()
            }),
        })
        .unwrap();

    let values = frames_to_values(&frames);
    let usage_frame = values
        .iter()
        .find(|v| v.get("usage").is_some())
        .expect("there should be a frame with usage");

    let choices = usage_frame["choices"]
        .as_array()
        .expect("choices must be an array");
    assert!(
        !choices.is_empty(),
        "usage frame's choices must not be an empty array, actual: {usage_frame}"
    );
    assert_eq!(usage_frame["usage"]["total_tokens"], 11);
}

// The stream must end with `data: [DONE]`; a repeated finish must not emit it again.
#[test]
fn openai_encoder_terminates_with_done_once() {
    let codec = OpenAiCodec::new();
    let mut enc = codec.stream_encoder();

    let frames = enc
        .encode_event(&StreamEvent::MessageStop {
            reason: StopReason::Stop,
        })
        .unwrap();
    assert!(has_done(&frames), "[DONE] should follow MessageStop");

    let again = enc.finish().unwrap();
    assert!(
        !has_done(&again),
        "already sent [DONE], must not send again, actual: {:?}",
        String::from_utf8_lossy(&again.concat())
    );
}

// If the stream breaks without a MessageStop, finish must backfill [DONE], otherwise the
// client hangs forever.
#[test]
fn openai_encoder_finish_backfills_done() {
    let codec = OpenAiCodec::new();
    let mut enc = codec.stream_encoder();

    enc.encode_event(&StreamEvent::TextDelta {
        index: 0,
        text: "partial".into(),
    })
    .unwrap();

    let out = enc.finish().unwrap();
    assert!(has_done(&out), "finish must backfill [DONE]");
}

// Tool fragments must be assembled into tool_calls fragments on the output side, keeping the
// index.
#[test]
fn openai_encoder_emits_tool_call_fragments_with_index() {
    let codec = OpenAiCodec::new();
    let mut enc = codec.stream_encoder();

    let mut frames = Vec::new();
    frames.extend(
        enc.encode_event(&StreamEvent::ToolCallStart {
            index: 2,
            id: "call_2".into(),
            name: "search".into(),
        })
        .unwrap(),
    );
    frames.extend(
        enc.encode_event(&StreamEvent::ToolCallDelta {
            index: 2,
            arguments_delta: "{\"q\":".into(),
        })
        .unwrap(),
    );
    frames.extend(
        enc.encode_event(&StreamEvent::ToolCallDelta {
            index: 2,
            arguments_delta: "\"y\"}".into(),
        })
        .unwrap(),
    );

    let values = frames_to_values(&frames);
    let call_frames: Vec<&Value> = values
        .iter()
        .filter(|v| v["choices"][0]["delta"].get("tool_calls").is_some())
        .collect();

    assert_eq!(call_frames.len(), 3, "start + two argument segments");
    assert_eq!(
        call_frames[0]["choices"][0]["delta"]["tool_calls"][0]["id"],
        "call_2"
    );
    assert_eq!(
        call_frames[0]["choices"][0]["delta"]["tool_calls"][0]["function"]["name"],
        "search"
    );

    let args: String = call_frames[1..]
        .iter()
        .map(|v| {
            v["choices"][0]["delta"]["tool_calls"][0]["function"]["arguments"]
                .as_str()
                .unwrap_or_default()
                .to_string()
        })
        .collect();
    assert_eq!(args, "{\"q\":\"y\"}");

    for v in &call_frames {
        assert_eq!(
            v["choices"][0]["delta"]["tool_calls"][0]["index"], 2,
            "tool index must stay 2, actual: {v}"
        );
    }
}

// Fragments without a start must still be emitted (backfilling an id-less head frame)
// instead of dropping the whole call.
#[test]
fn openai_encoder_emits_tool_delta_without_start() {
    let codec = OpenAiCodec::new();
    let mut enc = codec.stream_encoder();

    let frames = enc
        .encode_event(&StreamEvent::ToolCallDelta {
            index: 0,
            arguments_delta: "{}".into(),
        })
        .unwrap();

    let values = frames_to_values(&frames);
    assert!(
        values
            .iter()
            .any(|v| v["choices"][0]["delta"].get("tool_calls").is_some()),
        "tool fragments must still be output without ToolCallStart, actual: {values:?}"
    );
}

#[test]
fn openai_codec_reports_openai_format() {
    assert_eq!(OpenAiCodec::new().format(), Protocol::OpenAi);
}

// Two-hop invariant: Chat body → IR → Chat body keeps the core fields (regression for the
// old hub path).
#[test]
fn openai_two_hop_roundtrip_is_stable() {
    let codec = OpenAiCodec::new();
    let body = serde_json::to_vec(&json!({
        "model": "gpt-x",
        "messages": [{"role": "user", "content": "ping"}],
        "temperature": 0.2
    }))
    .unwrap();

    let ir = codec.decode_request(Bytes::from(body)).expect("decode");
    let out: Value = serde_json::from_slice(&codec.encode_request(&ir).unwrap()).unwrap();

    assert_eq!(out["model"], "gpt-x");
    assert_eq!(out["messages"][0]["content"], "ping");
    assert_eq!(out["temperature"], 0.2);
    // Without stream set, stream_options must not be injected out of nowhere (it would make
    // the upstream treat a non-streaming request as streaming).
    assert!(
        out.get("stream_options").is_none(),
        "non-streaming request must not inject stream_options, actual: {out}"
    );
}

// No system in the IR means no empty system message may be produced (it would pollute the
// downstream schema).
#[test]
fn openai_encode_request_omits_absent_system() {
    let codec = OpenAiCodec::new();
    let mut ir = mk_request("gpt-x");
    ir.messages = vec![gateway_protocol_bridge::ir::Message {
        role: Role::User,
        content: vec![ContentBlock::Text { text: "hi".into() }],
    }];
    ir.sampling = Some(SamplingParams {
        temperature: None,
        top_p: None,
        max_tokens: None,
        extra: Map::new(),
    });

    let out: Value = serde_json::from_slice(&codec.encode_request(&ir).unwrap()).unwrap();
    let messages = out["messages"].as_array().unwrap();
    assert_eq!(
        messages.len(),
        1,
        "no extra system message should appear out of thin air, actual: {messages:?}"
    );
    assert_eq!(messages[0]["role"], "user");
}

#[test]
fn usage_details_preserve_cache_write_and_reasoning() {
    let codec = OpenAiCodec;
    let body = serde_json::to_vec(&json!({
        "id": "chatcmpl-u",
        "model": "gpt-x",
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": "hi"},
            "finish_reason": "stop"
        }],
        "usage": {
            "prompt_tokens": 100,
            "completion_tokens": 40,
            "prompt_tokens_details": {"cached_tokens": 30, "cache_write_tokens": 20},
            "completion_tokens_details": {"reasoning_tokens": 12}
        }
    }))
    .unwrap();

    let resp = codec.decode_response(Bytes::from(body)).expect("decode");
    // OpenAI `prompt_tokens` is already the total; it must not be reduced by the subset.
    assert_eq!(resp.usage.prompt_tokens, 100);
    assert_eq!(resp.usage.cached_tokens, Some(30));
    assert_eq!(resp.usage.cache_creation_tokens, Some(20));
    assert_eq!(resp.usage.reasoning_tokens, Some(12));

    // Round-trip: the detail quantities must survive IR → OpenAI encoding.
    let out = codec.encode_response(&resp).expect("encode");
    let v: Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(
        v["usage"]["prompt_tokens_details"]["cache_write_tokens"],
        20
    );
    assert_eq!(
        v["usage"]["completion_tokens_details"]["reasoning_tokens"],
        12
    );
}
