//! Integration tests for the four-format codecs combined through the registry.
//!
//! `tests/format_codec.rs` verifies the registry's dispatch semantics with fake codecs; this
//! file swaps in the **real codecs** assembled by `with_defaults()` and verifies that the
//! four formats genuinely convert into each other — the core promise of the two-hop
//! abstraction (N codecs covering N×(N-1) directions) and the last safety net before the WP6
//! wiring.

use bytes::Bytes;
use gateway_protocol_bridge::adaptor::Protocol;
use gateway_protocol_bridge::format_codec::FormatRegistry;
use serde_json::{Value, json};

/// Smallest request body every format can express (one user message).
fn request_for(fmt: Protocol) -> Bytes {
    let v = match fmt {
        Protocol::OpenAi => json!({
            "model": "m",
            "messages": [{"role": "user", "content": "ping"}]
        }),
        Protocol::Claude => json!({
            "model": "m",
            "max_tokens": 64,
            "messages": [{"role": "user", "content": "ping"}]
        }),
        Protocol::Gemini => json!({
            "contents": [{"role": "user", "parts": [{"text": "ping"}]}]
        }),
        Protocol::OpenAIResp => json!({"model": "m", "input": "ping"}),
        Protocol::Passthrough => json!({"whatever": true}),
    };
    Bytes::from(serde_json::to_vec(&v).unwrap())
}

/// Extract user text from a Content field. Claude/OpenAI accept both the "string" and
/// "block array" forms, so both must be recognized.
fn text_from_content(v: &Value) -> Option<String> {
    if let Some(s) = v.as_str() {
        return Some(s.to_string());
    }
    v.as_array().and_then(|a| {
        a.iter()
            .find_map(|b| b.get("text").and_then(Value::as_str))
            .map(str::to_string)
    })
}

/// Where the user text should appear in each format's request body (used to assert that the
/// content really moved).
fn user_text_of(fmt: Protocol, v: &Value) -> Option<String> {
    match fmt {
        Protocol::OpenAi | Protocol::Claude => text_from_content(&v["messages"][0]["content"]),
        Protocol::Gemini => v["contents"][0]["parts"][0]["text"]
            .as_str()
            .map(str::to_string),
        Protocol::OpenAIResp => v["input"]
            .as_array()
            .and_then(|a| a.first())
            .and_then(|i| i["content"].as_array())
            .and_then(|a| a.first())
            .and_then(|p| p["text"].as_str())
            .map(str::to_string),
        Protocol::Passthrough => None,
    }
}

// All format pairs: the 12 ordered directions (four formats, excluding self-conversion and
// Passthrough) must work, and the produced bytes must be a shape the target format can parse
// with the user text arriving verbatim.
//
// Why this assertion matters: under the old architecture every extra format required N-1
// more converters, and one missing direction meant a runtime 502; under the new architecture
// a "missing direction" is structurally impossible — as long as the codecs are all
// registered, the two hops always hold.
#[test]
fn all_real_codecs_interoperate_through_registry() {
    let reg = FormatRegistry::with_defaults();
    let formats = [
        Protocol::OpenAi,
        Protocol::Claude,
        Protocol::Gemini,
        Protocol::OpenAIResp,
    ];

    for src in formats {
        for dst in formats {
            if src == dst {
                continue;
            }
            let out = reg
                .translate_request(src, dst, request_for(src))
                .unwrap_or_else(|e| {
                    panic!("{src:?} → {dst:?} should traverse two hops, actual failure: {e}")
                });

            let v: Value = serde_json::from_slice(&out)
                .unwrap_or_else(|e| panic!("{src:?} → {dst:?} output should be JSON: {e}"));

            // The output must have the target format's shape: user text appears where that
            // format expects it.
            let text = user_text_of(dst, &v).unwrap_or_else(|| {
                panic!("{src:?} → {dst:?} output has no user text, actual: {v}")
            });
            assert_eq!(
                text, "ping",
                "{src:?} → {dst:?} user text must arrive verbatim"
            );
        }
    }
}

// Key mixed-format path: Claude client → OpenAI upstream (one of the core scenarios this PR
// fixes).
#[test]
fn claude_client_to_openai_upstream_keeps_tool_definitions() {
    let reg = FormatRegistry::with_defaults();
    let body = Bytes::from(
        serde_json::to_vec(&json!({
            "model": "claude-x",
            "max_tokens": 128,
            "messages": [{"role": "user", "content": "search something"}],
            "tools": [{
                "name": "search",
                "description": "find",
                "input_schema": {"type": "object", "properties": {"q": {"type": "string"}}}
            }]
        }))
        .unwrap(),
    );

    let out = reg
        .translate_request(Protocol::Claude, Protocol::OpenAi, body)
        .expect("Claude → OpenAI should succeed");
    let v: Value = serde_json::from_slice(&out).unwrap();

    // Chat wraps tool definitions in a `function` sub-object and names the schema field
    // `parameters`.
    assert_eq!(
        v["tools"][0]["function"]["name"], "search",
        "tool definitions must survive cross-format, actual: {v}"
    );
    assert_eq!(
        v["tools"][0]["function"]["parameters"]["properties"]["q"]["type"], "string",
        "schema must move to Chat's parameters position"
    );
    // Claude's required max_tokens must carry over to Chat's max_tokens.
    assert_eq!(v["max_tokens"], 128);
    // system placement differs: when Claude has one it becomes a Chat system message (this
    // case has none, so none may appear).
    assert_eq!(v["messages"].as_array().unwrap().len(), 1);
}

// Reverse direction: OpenAI client → Claude upstream. Claude requires max_tokens and the
// upstream returns 400 without it, so the two hops must supply a default.
#[test]
fn openai_client_to_claude_upstream_fills_required_max_tokens() {
    let reg = FormatRegistry::with_defaults();
    let body = Bytes::from(
        serde_json::to_vec(&json!({
            "model": "gpt-x",
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .unwrap(),
    );

    let out = reg
        .translate_request(Protocol::OpenAi, Protocol::Claude, body)
        .expect("OpenAI → Claude should succeed");
    let v: Value = serde_json::from_slice(&out).unwrap();

    assert!(
        v["max_tokens"].as_u64().is_some_and(|n| n > 0),
        "Claude's required max_tokens must be defaulted, actual: {v}"
    );
    assert_eq!(
        text_from_content(&v["messages"][0]["content"]).as_deref(),
        Some("hi")
    );
}

// Responses client → Claude upstream (the simplest form of composed routing).
#[test]
fn responses_client_to_claude_upstream_works() {
    let reg = FormatRegistry::with_defaults();
    let body = Bytes::from(
        serde_json::to_vec(&json!({
            "model": "gpt-5",
            "instructions": "be brief",
            "input": "hello"
        }))
        .unwrap(),
    );

    let out = reg
        .translate_request(Protocol::OpenAIResp, Protocol::Claude, body)
        .expect("Responses → Claude should succeed");
    let v: Value = serde_json::from_slice(&out).unwrap();

    // instructions must land in Claude's top-level system.
    let system_text = v["system"]
        .as_array()
        .and_then(|a| a.first())
        .and_then(|s| s["text"].as_str())
        .or_else(|| v["system"].as_str())
        .unwrap_or_default();
    assert_eq!(
        system_text, "be brief",
        "instructions must move to Claude's system"
    );

    assert_eq!(
        text_from_content(&v["messages"][0]["content"]).as_deref(),
        Some("hello")
    );
    assert!(v["max_tokens"].as_u64().is_some_and(|n| n > 0));
}

// Two hops in the response direction: Claude upstream response → OpenAI client shape.
#[test]
fn claude_response_translates_to_openai_shape() {
    let reg = FormatRegistry::with_defaults();
    let body = Bytes::from(
        serde_json::to_vec(&json!({
            "id": "msg_1",
            "type": "message",
            "role": "assistant",
            "model": "claude-x",
            "content": [{"type": "text", "text": "pong"}],
            "stop_reason": "end_turn",
            "usage": {"input_tokens": 4, "output_tokens": 2}
        }))
        .unwrap(),
    );

    let out = reg
        .translate_response(Protocol::Claude, Protocol::OpenAi, body)
        .expect("Claude response → OpenAI should succeed");
    let v: Value = serde_json::from_slice(&out).unwrap();

    assert_eq!(v["object"], "chat.completion");
    assert_eq!(v["choices"][0]["message"]["content"], "pong");
    assert_eq!(
        v["choices"][0]["finish_reason"], "stop",
        "end_turn must map to Chat's stop"
    );
    assert_eq!(
        v["usage"]["prompt_tokens"], 4,
        "usage must carry across formats"
    );
}

// Passthrough must pass through untouched with any format: even invalid JSON must not error.
#[test]
fn passthrough_bypasses_conversion_with_real_registry() {
    let reg = FormatRegistry::with_defaults();
    let garbage = Bytes::from_static(b"\x00\xff not json");

    for dst in [
        Protocol::OpenAi,
        Protocol::Claude,
        Protocol::Gemini,
        Protocol::OpenAIResp,
    ] {
        let out = reg
            .translate_request(Protocol::Passthrough, dst, garbage.clone())
            .unwrap_or_else(|e| panic!("Passthrough → {dst:?} should pass through, actual: {e}"));
        assert_eq!(out, garbage);
    }
}

// The IR normalizes usage to subset semantics (prompt/completion are totals, the
// cache/reasoning numbers are subsets). Both directions of the mismatch must hold:
// Claude's additive cache fields must become part of prompt_tokens, and OpenAI's
// already-inclusive prompt_tokens must be reduced before becoming Claude input_tokens.
#[test]
fn usage_is_normalized_across_protocols() {
    let reg = FormatRegistry::with_defaults();

    // Claude → OpenAI: cached/creation move inside prompt_tokens.
    let claude = Bytes::from(
        serde_json::to_vec(&json!({
            "id": "msg_1",
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
        .unwrap(),
    );
    let out = reg
        .translate_response(Protocol::Claude, Protocol::OpenAi, claude)
        .expect("Claude → OpenAI");
    let v: Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(
        v["usage"]["prompt_tokens"], 20,
        "total input includes cache"
    );
    assert_eq!(v["usage"]["prompt_tokens_details"]["cached_tokens"], 4);
    assert_eq!(v["usage"]["total_tokens"], 40, "20 input + 20 output");

    // OpenAI → Claude: input_tokens excludes the cached subset (no double count).
    let openai = Bytes::from(
        serde_json::to_vec(&json!({
            "id": "chatcmpl_1",
            "object": "chat.completion",
            "model": "gpt-x",
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": "hi"},
                "finish_reason": "stop"
            }],
            "usage": {
                "prompt_tokens": 100,
                "completion_tokens": 50,
                "prompt_tokens_details": {"cached_tokens": 80}
            }
        }))
        .unwrap(),
    );
    let out = reg
        .translate_response(Protocol::OpenAi, Protocol::Claude, openai)
        .expect("OpenAI → Claude");
    let v: Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(v["usage"]["input_tokens"], 20, "input excludes cached");
    assert_eq!(v["usage"]["cache_read_input_tokens"], 80);
}
