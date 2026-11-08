//! Behavior tests for the OpenAI Responses codec.
//!
//! Three things matter here: parsing the three input shapes (Responses' `input` can be a
//! string, a message array, or an items array), the flat tools shape (unlike Chat's
//! `function` sub-object), and the **lifecycle order** of streaming events — Responses
//! client parsers depend strictly on
//! `response.created` → `output_item.added` → delta → `response.completed`; a wrong order
//! or a missing boundary frame fails immediately.

use bytes::Bytes;
use gateway_protocol_bridge::adaptor::Protocol;
use gateway_protocol_bridge::format_codec::FormatCodec;
use gateway_protocol_bridge::ir::{ContentBlock, Role, StopReason, StreamEvent};
use gateway_protocol_bridge::responses::ResponsesCodec;
use serde_json::{Value, json};

/// Split encoder output into a (event type, payload) sequence so tests can assert on
/// order.
fn frames(frames: &[Bytes]) -> Vec<(String, Value)> {
    let mut out = Vec::new();
    for frame in frames {
        let text = String::from_utf8_lossy(frame);
        let mut kind = None;
        let mut payload = None;
        for line in text.lines() {
            if let Some(e) = line.strip_prefix("event: ") {
                kind = Some(e.to_string());
            }
            if let Some(d) = line.strip_prefix("data: ") {
                payload = serde_json::from_str(d).ok();
            }
        }
        if let (Some(k), Some(p)) = (kind, payload) {
            out.push((k, p));
        }
    }
    out
}

fn event_kinds(raw: &[Bytes]) -> Vec<String> {
    frames(raw).into_iter().map(|(k, _)| k).collect()
}

/// Index of the first occurrence of an event type.
fn first_index(kinds: &[String], kind: &str) -> Option<usize> {
    kinds.iter().position(|k| k == kind)
}

// ---------- The three input shapes ----------

// Shape one: a bare string input decodes to a single user message.
#[test]
fn responses_string_input_becomes_single_user_message() {
    let codec = ResponsesCodec::new();
    let body = serde_json::to_vec(&json!({
        "model": "gpt-5",
        "input": "hello there"
    }))
    .unwrap();

    let ir = codec.decode_request(Bytes::from(body)).expect("decode");
    assert_eq!(ir.messages.len(), 1);
    assert_eq!(ir.messages[0].role, Role::User);
    assert!(
        matches!(&ir.messages[0].content[0], ContentBlock::Text { text } if text == "hello there"),
        "string input should become a text block, actual: {:?}",
        ir.messages[0].content
    );
}

// Shape two: a message array (role + content as an array of input_text parts).
#[test]
fn responses_message_array_input_parses_parts() {
    let codec = ResponsesCodec::new();
    let body = serde_json::to_vec(&json!({
        "model": "gpt-5",
        "instructions": "be terse",
        "input": [
            {"role": "user", "content": [{"type": "input_text", "text": "ping"}]},
            {"role": "assistant", "content": [{"type": "output_text", "text": "pong"}]}
        ]
    }))
    .unwrap();

    let ir = codec.decode_request(Bytes::from(body)).expect("decode");
    assert_eq!(ir.system.len(), 1, "instructions should become system");
    assert_eq!(ir.system[0].text, "be terse");
    assert_eq!(ir.messages.len(), 2);
    assert_eq!(ir.messages[0].role, Role::User);
    assert_eq!(ir.messages[1].role, Role::Assistant);
    assert!(
        matches!(&ir.messages[1].content[0], ContentBlock::Text { text } if text == "pong"),
        "output_text part must also be parsed, actual: {:?}",
        ir.messages[1].content
    );
}

// Shape three: an items array (function_call / function_call_output).
// Both must map to ToolUse / ToolResult and preserve the call_id association — without it,
// multi-turn tool calls cannot match a result back to the call that produced it.
#[test]
fn responses_item_array_maps_function_call_and_output() {
    let codec = ResponsesCodec::new();
    let body = serde_json::to_vec(&json!({
        "model": "gpt-5",
        "input": [
            {"role": "user", "content": "search for ferrite"},
            {"type": "function_call", "call_id": "call_1", "name": "search",
             "arguments": "{\"q\":\"ferrite\"}"},
            {"type": "function_call_output", "call_id": "call_1", "output": "3 hits"}
        ]
    }))
    .unwrap();

    let ir = codec.decode_request(Bytes::from(body)).expect("decode");
    assert_eq!(ir.messages.len(), 3);

    let tool_use = ir.messages[1]
        .content
        .iter()
        .find_map(|b| match b {
            ContentBlock::ToolUse { id, name, input } => {
                Some((id.clone(), name.clone(), input.clone()))
            }
            _ => None,
        })
        .expect("function_call should become ToolUse");
    assert_eq!(tool_use.0, "call_1");
    assert_eq!(tool_use.1, "search");
    assert_eq!(
        tool_use.2["q"], "ferrite",
        "arguments string must be parsed into JSON"
    );

    assert_eq!(ir.messages[2].role, Role::Tool);
    assert!(
        ir.messages[2].content.iter().any(
            |b| matches!(b, ContentBlock::ToolResult { tool_use_id, content }
                if tool_use_id == "call_1" && content == "3 hits")
        ),
        "function_call_output should become ToolResult associated with call_1, actual: {:?}",
        ir.messages[2].content
    );
}

// Responses tools are flat (name/parameters at the top level), unlike Chat where they are
// wrapped in function. Parsing with the Chat shape would silently yield zero tools.
#[test]
fn responses_flat_tool_shape_is_parsed() {
    let codec = ResponsesCodec::new();
    let body = serde_json::to_vec(&json!({
        "model": "gpt-5",
        "input": "hi",
        "tools": [{
            "type": "function",
            "name": "search",
            "description": "find things",
            "parameters": {"type": "object", "properties": {"q": {"type": "string"}}}
        }]
    }))
    .unwrap();

    let ir = codec.decode_request(Bytes::from(body)).expect("decode");
    assert_eq!(ir.tools.len(), 1, "flat tools must parse into tools");
    assert_eq!(ir.tools[0].name, "search");
    assert_eq!(ir.tools[0].description.as_deref(), Some("find things"));
    assert_eq!(
        ir.tools[0].input_schema["properties"]["q"]["type"], "string",
        "parameters must go into input_schema"
    );

    // The encoded output must use the flat shape too.
    let out: Value = serde_json::from_slice(&codec.encode_request(&ir).unwrap()).unwrap();
    assert_eq!(out["tools"][0]["name"], "search");
    assert!(
        out["tools"][0].get("function").is_none(),
        "Responses's tools must not contain a function sub-object, actual: {}",
        out["tools"][0]
    );
}

// instructions is written back as instructions, not reinserted as a system message.
#[test]
fn responses_encode_request_uses_instructions_and_input() {
    let codec = ResponsesCodec::new();
    let body = serde_json::to_vec(&json!({
        "model": "gpt-5",
        "instructions": "be brief",
        "input": "hello",
        "max_output_tokens": 128
    }))
    .unwrap();

    let ir = codec.decode_request(Bytes::from(body)).expect("decode");
    let out: Value = serde_json::from_slice(&codec.encode_request(&ir).unwrap()).unwrap();

    assert_eq!(out["instructions"], "be brief");
    assert!(
        out.get("messages").is_none(),
        "must not be rewritten into Chat's messages field, actual: {out}"
    );
    assert_eq!(out["input"][0]["role"], "user");
    assert_eq!(
        out["max_output_tokens"], 128,
        "max_output_tokens must be preserved"
    );
}

// ---------- Non-streaming responses ----------

// The function_call arguments inside output are a JSON string and must be parsed into an
// object. Without parsing, downstream codecs receive a blob of string and converting to
// another format would produce escaped text.
#[test]
fn responses_decode_response_parses_function_call_arguments() {
    let codec = ResponsesCodec::new();
    let body = serde_json::to_vec(&json!({
        "id": "resp_1",
        "object": "response",
        "status": "completed",
        "model": "gpt-5",
        "output": [
            {"type": "message", "content": [{"type": "output_text", "text": "calling"}]},
            {"type": "function_call", "call_id": "call_7", "name": "search",
             "arguments": "{\"q\":\"x\"}"}
        ],
        "usage": {"input_tokens": 10, "output_tokens": 3,
                  "input_tokens_details": {"cached_tokens": 2}}
    }))
    .unwrap();

    let resp = codec.decode_response(Bytes::from(body)).expect("decode");
    assert_eq!(resp.id, "resp_1");
    assert_eq!(resp.stop_reason, StopReason::ToolUse);
    assert_eq!(resp.usage.prompt_tokens, 10);
    assert_eq!(resp.usage.completion_tokens, 3);
    assert_eq!(resp.usage.cached_tokens, Some(2));

    assert!(
        resp.outputs
            .iter()
            .any(|b| matches!(b, ContentBlock::Text { text } if text == "calling")),
        "message item's output_text must become Text, actual: {:?}",
        resp.outputs
    );

    let use_block = resp
        .outputs
        .iter()
        .find_map(|b| match b {
            ContentBlock::ToolUse { id, name, input } => {
                Some((id.clone(), name.clone(), input.clone()))
            }
            _ => None,
        })
        .expect("function_call should become ToolUse");
    assert_eq!(use_block.0, "call_7");
    assert_eq!(use_block.1, "search");
    assert_eq!(
        use_block.2["q"], "x",
        "arguments must be a parsed object, actual: {:?}",
        use_block.2
    );
}

// status=incomplete expresses truncation and must map to Length, not Stop.
#[test]
fn responses_incomplete_status_maps_to_length() {
    let codec = ResponsesCodec::new();
    let body = serde_json::to_vec(&json!({
        "id": "resp_2",
        "status": "incomplete",
        "incomplete_details": {"reason": "max_output_tokens"},
        "output": [{"type": "message", "content": [{"type": "output_text", "text": "partial"}]}],
        "usage": {"input_tokens": 1, "output_tokens": 2}
    }))
    .unwrap();

    let resp = codec.decode_response(Bytes::from(body)).expect("decode");
    assert_eq!(
        resp.stop_reason,
        StopReason::Length,
        "incomplete must be Length, otherwise upstream will treat it as normal completion"
    );
}

// ---------- Streaming decode ----------

// output_item.added(function_call) carries call_id/name — they appear only here.
#[test]
fn responses_decode_event_reads_function_call_start() {
    let codec = ResponsesCodec::new();
    let frame = json!({
        "type": "response.output_item.added",
        "output_index": 1,
        "item": {"type": "function_call", "id": "fc_1", "call_id": "call_1",
                 "name": "search", "arguments": "", "status": "in_progress"}
    })
    .to_string();

    let events = codec.decode_event(&frame).expect("decode");
    let start = events
        .iter()
        .find_map(|e| match e {
            StreamEvent::ToolCallStart { index, id, name } => {
                Some((*index, id.clone(), name.clone()))
            }
            _ => None,
        })
        .unwrap_or_else(|| panic!("should produce ToolCallStart, actual: {events:?}"));

    assert_eq!(start.0, 1, "index should take output_index");
    assert_eq!(start.1, "call_1");
    assert_eq!(start.2, "search");
}

// Lifecycle control frames produce no IR event but must not error either.
#[test]
fn responses_decode_event_ignores_lifecycle_noise() {
    let codec = ResponsesCodec::new();
    for frame in [
        "{\"type\":\"response.content_part.added\"}",
        "{\"type\":\"response.output_item.done\"}",
        "{\"type\":\"response.output_text.done\"}",
        "not json",
        "",
    ] {
        let events = codec.decode_event(frame).expect("must not error");
        assert!(
            events.is_empty(),
            "{frame} should emit empty events, actual: {events:?}"
        );
    }
}

// response.failed is a terminal error and must become Error rather than being silently
// ignored.
#[test]
fn responses_decode_event_reports_failure() {
    let codec = ResponsesCodec::new();
    let frame = json!({
        "type": "response.failed",
        "response": {"id": "resp_x", "status": "failed",
                     "error": {"code": "server_error", "message": "boom"}}
    })
    .to_string();

    let events = codec.decode_event(&frame).expect("decode");
    assert!(
        events
            .iter()
            .any(|e| matches!(e, StreamEvent::Error { message } if message == "boom")),
        "response.failed must become Error, actual: {events:?}"
    );
}

// ---------- Streaming encode: event lifecycle ----------

// **Hard counterexample**: `response.created` must precede the first content frame, and
// `response.output_item.added` must precede a text delta. Missing either makes a Responses
// client parser fail immediately.
#[test]
fn responses_encoder_emits_lifecycle_prelude_before_text() {
    let codec = ResponsesCodec::new();
    let mut enc = codec.stream_encoder();

    let out = enc
        .encode_event(&StreamEvent::TextDelta {
            index: 0,
            text: "hi".into(),
        })
        .expect("encode");

    let kinds = event_kinds(&out);
    let created = first_index(&kinds, "response.created");
    let item_added = first_index(&kinds, "response.output_item.added");
    let delta = first_index(&kinds, "response.output_text.delta");

    let created = created.unwrap_or_else(|| panic!("missing response.created, actual: {kinds:?}"));
    let item_added =
        item_added.unwrap_or_else(|| panic!("missing output_item.added, actual: {kinds:?}"));
    let delta = delta.unwrap_or_else(|| panic!("missing output_text.delta, actual: {kinds:?}"));

    assert!(
        created < item_added,
        "created must precede item.added, actual: {kinds:?}"
    );
    assert!(
        item_added < delta,
        "item.added must precede text delta, actual: {kinds:?}"
    );

    // created is emitted only once.
    let mut all = out;
    all.extend(
        enc.encode_event(&StreamEvent::TextDelta {
            index: 0,
            text: " again".into(),
        })
        .unwrap(),
    );
    let created_count = event_kinds(&all)
        .iter()
        .filter(|k| *k == "response.created")
        .count();
    assert_eq!(
        created_count, 1,
        "response.created should only be sent once"
    );
}

// A tool call must produce added(function_call) → arguments.delta, then completed at the
// end.
#[test]
fn responses_encoder_emits_tool_lifecycle() {
    let codec = ResponsesCodec::new();
    let mut enc = codec.stream_encoder();

    let mut out = Vec::new();
    out.extend(
        enc.encode_event(&StreamEvent::ToolCallStart {
            index: 1,
            id: "call_1".into(),
            name: "search".into(),
        })
        .unwrap(),
    );
    out.extend(
        enc.encode_event(&StreamEvent::ToolCallDelta {
            index: 1,
            arguments_delta: "{\"q\":".into(),
        })
        .unwrap(),
    );
    out.extend(
        enc.encode_event(&StreamEvent::ToolCallDelta {
            index: 1,
            arguments_delta: "\"y\"}".into(),
        })
        .unwrap(),
    );
    out.extend(
        enc.encode_event(&StreamEvent::MessageStop {
            reason: StopReason::ToolUse,
        })
        .unwrap(),
    );

    let pairs = frames(&out);
    let added = pairs
        .iter()
        .find(|(k, _)| k == "response.output_item.added")
        .expect("there should be output_item.added");
    assert_eq!(added.1["item"]["type"], "function_call");
    assert_eq!(added.1["item"]["call_id"], "call_1");
    assert_eq!(added.1["item"]["name"], "search");

    let deltas: Vec<&Value> = pairs
        .iter()
        .filter(|(k, _)| k == "response.function_call_arguments.delta")
        .map(|(_, v)| v)
        .collect();
    assert_eq!(deltas.len(), 2, "two argument fragments, one frame each");
    let recombined: String = deltas
        .iter()
        .map(|d| d["delta"].as_str().unwrap_or_default())
        .collect();
    assert_eq!(recombined, "{\"q\":\"y\"}");

    // Wrap-up: an arguments done frame plus completed, with the full arguments in
    // completed's output.
    let kinds = event_kinds(&out);
    assert!(
        kinds
            .iter()
            .any(|k| k == "response.function_call_arguments.done"),
        "tool arguments must have a done frame, actual: {kinds:?}"
    );
    let completed = pairs
        .iter()
        .find(|(k, _)| k == "response.completed")
        .expect("there should be response.completed");
    let items = completed.1["response"]["output"]
        .as_array()
        .expect("completed's output must be an array");
    assert!(
        items.iter().any(|i| i["type"] == "function_call"
            && i["arguments"] == "{\"q\":\"y\"}"
            && i["call_id"] == "call_1"),
        "completed snapshot's function_call must carry complete arguments, actual: {items:?}"
    );
}

// Truncation (Length) must end with response.incomplete instead of claiming completed.
// Otherwise the client believes it received a complete answer.
#[test]
fn responses_encoder_truncation_ends_incomplete() {
    let codec = ResponsesCodec::new();
    let mut enc = codec.stream_encoder();

    enc.encode_event(&StreamEvent::TextDelta {
        index: 0,
        text: "partial".into(),
    })
    .unwrap();
    let out = enc
        .encode_event(&StreamEvent::MessageStop {
            reason: StopReason::Length,
        })
        .unwrap();

    let kinds = event_kinds(&out);
    assert!(
        kinds.iter().any(|k| k == "response.incomplete"),
        "truncation must be incomplete, actual: {kinds:?}"
    );
    assert!(
        !kinds.iter().any(|k| k == "response.completed"),
        "truncation must not also report completed, actual: {kinds:?}"
    );
}

// On a broken stream, finish must backfill the closing events; repeated calls are
// idempotent.
#[test]
fn responses_encoder_finish_backfills_and_is_idempotent() {
    let codec = ResponsesCodec::new();
    let mut enc = codec.stream_encoder();

    enc.encode_event(&StreamEvent::TextDelta {
        index: 0,
        text: "x".into(),
    })
    .unwrap();

    let out = enc.finish().expect("finish");
    let kinds = event_kinds(&out);
    assert!(
        kinds.iter().any(|k| k == "response.completed"),
        "finish must backfill response.completed, actual: {kinds:?}"
    );

    let again = enc.finish().expect("finish again");
    assert!(
        again.is_empty(),
        "repeated finish must not emit frames again, actual: {again:?}"
    );
}

// Failure is terminal: no response.completed may be appended after response.failed.
#[test]
fn responses_encoder_error_is_terminal() {
    let codec = ResponsesCodec::new();
    let mut enc = codec.stream_encoder();

    let out = enc
        .encode_event(&StreamEvent::Error {
            message: "boom".into(),
        })
        .unwrap();
    assert!(event_kinds(&out).iter().any(|k| k == "response.failed"));

    let after = enc.finish().expect("finish");
    assert!(
        !event_kinds(&after)
            .iter()
            .any(|k| k == "response.completed"),
        "after failure must not report completed again, actual: {after:?}"
    );
}

// Text and tool items must not share an output_index (both use one incrementing sequence).
// A collision makes the client treat the tool item as part of the text item and silently
// misassemble the output.
#[test]
fn responses_encoder_text_and_tool_indices_do_not_collide() {
    let codec = ResponsesCodec::new();
    let mut enc = codec.stream_encoder();

    let mut out = Vec::new();
    out.extend(
        enc.encode_event(&StreamEvent::TextDelta {
            index: 0,
            text: "hi".into(),
        })
        .unwrap(),
    );
    out.extend(
        enc.encode_event(&StreamEvent::ToolCallStart {
            index: 1,
            id: "call_1".into(),
            name: "search".into(),
        })
        .unwrap(),
    );

    let added_indices: Vec<u64> = frames(&out)
        .iter()
        .filter(|(k, _)| k == "response.output_item.added")
        .filter_map(|(_, v)| v["output_index"].as_u64())
        .collect();

    assert_eq!(added_indices.len(), 2, "text and tool each open an item");
    assert_ne!(
        added_indices[0], added_indices[1],
        "text item and tool item's output_index must not be the same, actual: {added_indices:?}"
    );
}

#[test]
fn responses_codec_reports_responses_format() {
    assert_eq!(ResponsesCodec::new().format(), Protocol::OpenAIResp);
}

// Two hops: even when the IR has no input, the output must be a valid array (never null,
// which would make the upstream return 400).
#[test]
fn responses_encode_request_always_emits_input_array() {
    let codec = ResponsesCodec::new();
    let body = serde_json::to_vec(&json!({"model": "gpt-5", "input": "hi"})).unwrap();
    let mut ir = codec.decode_request(Bytes::from(body)).expect("decode");
    ir.messages.clear();

    let out: Value = serde_json::from_slice(&codec.encode_request(&ir).unwrap()).unwrap();
    assert_eq!(out["input"], json!([]), "input must be an array");
}

#[test]
fn usage_reasoning_tokens_are_preserved() {
    let codec = ResponsesCodec;
    let body = serde_json::to_vec(&json!({
        "id": "resp_u",
        "object": "response",
        "status": "completed",
        "model": "gpt-x",
        "output": [{
            "type": "message",
            "content": [{"type": "output_text", "text": "hi"}]
        }],
        "usage": {
            "input_tokens": 10,
            "output_tokens": 20,
            "output_tokens_details": {"reasoning_tokens": 5}
        }
    }))
    .unwrap();

    let resp = codec.decode_response(Bytes::from(body)).expect("decode");
    assert_eq!(resp.usage.reasoning_tokens, Some(5));

    let out = codec.encode_response(&resp).expect("encode");
    let v: Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(v["usage"]["output_tokens_details"]["reasoning_tokens"], 5);
}
