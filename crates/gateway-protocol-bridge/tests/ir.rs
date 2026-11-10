//! Behavior tests for the IR module: lossless round-trip, escape hatches,
//! `skip_serializing_if`, tagged shapes.

use gateway_protocol_bridge::ir::{
    ContentBlock, LlmRequest, Message, Role, SamplingParams, StopReason, StreamEvent, TextBlock,
    ToolChoice, ToolDef,
};
use serde_json::{Map, Value, json};

/// Build a request covering every `ContentBlock` variant as the round-trip payload.
fn full_request() -> LlmRequest {
    LlmRequest {
        model: "gpt-x".into(),
        messages: vec![Message {
            role: Role::Assistant,
            content: vec![
                ContentBlock::Text { text: "hi".into() },
                ContentBlock::ToolUse {
                    id: "tu_1".into(),
                    name: "search".into(),
                    input: json!({"q": "ferrite"}),
                },
                ContentBlock::ToolResult {
                    tool_use_id: "tu_1".into(),
                    content: "result".into(),
                },
                ContentBlock::Thinking { text: "hmm".into() },
                ContentBlock::Refusal { text: "no".into() },
                ContentBlock::Image {
                    media_type: "image/png".into(),
                    data: "AAAB".into(),
                },
                // Unknown block type: the escape hatch must swallow it and emit it unchanged
                ContentBlock::Unknown {
                    raw: json!({"type": "brand_new_block", "foo": "bar"}),
                },
            ],
        }],
        system: vec![TextBlock { text: "sys".into() }],
        tools: vec![ToolDef {
            name: "search".into(),
            description: Some("desc".into()),
            input_schema: json!({"type": "object"}),
            extra: Map::new(),
        }],
        tool_choice: Some(ToolChoice::Auto),
        sampling: Some(SamplingParams {
            temperature: Some(0.7),
            top_p: Some(0.9),
            max_tokens: Some(128),
            extra: Map::new(),
        }),
        stream: true,
        extra: Map::new(),
    }
}

// 1. Lossless round-trip: every ContentBlock variant (including the Unknown escape hatch)
//    must be semantically equivalent after serialize → deserialize (assert_eq compares
//    Values, so key order is irrelevant; Unknown key order is normalized by BTreeMap, and
//    avoiding data loss is enough). This is the hard contract of the IR as a relay hub:
//    no codec may lose information when relaying through IR.
#[test]
fn ir_roundtrip_is_lossless_for_all_content_blocks() {
    let req = full_request();
    let json_str = serde_json::to_string(&req).expect("serialize");
    let back: LlmRequest = serde_json::from_str(&json_str).expect("deserialize");
    assert_eq!(req, back);
}

// 2. Unknown top-level fields are preserved: keys not in the schema must land in extra and
//    reappear as-is on re-serialization. If the extra escape hatch failed, new vendor fields
//    would be silently dropped at the first hop and the response could not be written back
//    in its original format.
#[test]
fn ir_unknown_top_level_fields_survive_via_extra() {
    let raw = r#"{"model":"x","messages":[],"unknown_key":1}"#;
    let req: LlmRequest = serde_json::from_str(raw).expect("deserialize");
    assert_eq!(req.extra.get("unknown_key"), Some(&json!(1)));

    let again = serde_json::to_string(&req).expect("serialize");
    assert!(
        again.contains(r#""unknown_key":1"#),
        "unknown_key must round-trip, got: {again}"
    );
}

// 3. Unknown block types go into Unknown: when the parser meets a type it has no model for,
//    the whole object lands in raw and is emitted as-is on serialization (no re-tagging).
//    Still-unmodeled multimodal blocks such as audio/file pass through losslessly this way.
#[test]
fn ir_unknown_block_type_falls_into_unknown() {
    let raw = r#"{"type":"brand_new_block","foo":"bar"}"#;
    let block: ContentBlock = serde_json::from_str(raw).expect("deserialize");
    match block {
        ContentBlock::Unknown { ref raw } => {
            assert_eq!(raw, &json!({"type": "brand_new_block", "foo": "bar"}));
        }
        other => panic!("expected Unknown, got {other:?}"),
    }

    // Serialization must emit the original object in full (key order is irrelevant:
    // serde_json sorts keys via BTreeMap by default, so semantic equivalence is
    // losslessness): type and foo are both present and neither is rewritten to "unknown"
    let again: Value =
        serde_json::from_str(&serde_json::to_string(&block).expect("serialize")).expect("re-parse");
    assert_eq!(again, json!({"type": "brand_new_block", "foo": "bar"}));
}

// 3b. A modeled image block must dispatch to the Image variant, not silently fall into
//     Unknown. The bug this guards against: adding an Image variant to ContentBlock but
//     forgetting to add "image" to the deserializer's known-type dispatch list — the image
//     then still lands whole in raw, and neither the compiler nor round-trip reports it
//     (Unknown is equally lossless), but downstream codecs can no longer recognize it as an
//     image and cross-format image conversion silently degrades.
#[test]
fn ir_image_block_dispatches_to_image_variant() {
    let raw = r#"{"type":"image","media_type":"image/png","data":"AAAB"}"#;
    let block: ContentBlock = serde_json::from_str(raw).expect("deserialize");
    match block {
        ContentBlock::Image {
            ref media_type,
            ref data,
        } => {
            assert_eq!(media_type, "image/png");
            assert_eq!(data, "AAAB");
        }
        other => panic!("expected Image, got {other:?}"),
    }

    // Serialization must return the fixed tagged shape (field order is irrelevant;
    // semantic equivalence suffices)
    let again: Value =
        serde_json::from_str(&serde_json::to_string(&block).expect("serialize")).expect("re-parse");
    assert_eq!(
        again,
        json!({"type": "image", "media_type": "image/png", "data": "AAAB"})
    );
}

// 4. skip_serializing_if works: empty collections must not produce noise keys.
//    If an empty request carried "messages":[], writing back per format would insert an
//    array the schema does not have, polluting the response.
#[test]
fn ir_empty_collections_are_not_serialized() {
    let req = LlmRequest {
        model: "x".into(),
        messages: vec![],
        system: vec![],
        tools: vec![],
        tool_choice: None,
        sampling: None,
        stream: false,
        extra: Map::new(),
    };
    let j: Value = serde_json::to_value(&req).expect("serialize");
    for key in [
        "messages",
        "system",
        "tools",
        "tool_choice",
        "sampling",
        "temperature",
        "top_p",
        "max_tokens",
    ] {
        assert!(
            j.get(key).is_none(),
            "empty field {key} should be absent: {j}"
        );
    }
}

// 5. StreamEvent tagged shape: tag = "type" + snake_case, with the index field present.
//    index is the key to the G1 (tool-fragment assembly) fix: codecs attribute fragments by
//    index, and a wrong shape misaligns the whole stream.
#[test]
fn ir_stream_event_tagged_shape() {
    let delta = StreamEvent::TextDelta {
        index: 1,
        text: "hi".into(),
    };
    assert_eq!(
        serde_json::to_value(&delta).expect("serialize"),
        json!({"type":"text_delta","index":1,"text":"hi"})
    );

    let start = StreamEvent::MessageStart {
        id: "msg_1".into(),
        model: "m".into(),
    };
    assert_eq!(
        serde_json::to_value(&start).expect("serialize"),
        json!({"type":"message_start","id":"msg_1","model":"m"})
    );
}

// Also: StopReason serializes uniformly as a bare string, and unclassified reasons are
// passed through verbatim (fix: the old implementation serialized Other as {"other":"…"},
// inconsistent with the string shape of known variants; mixed into IR JSON it made
// downstream codecs emit illegal shapes).
#[test]
fn ir_stop_reason_snake_case_and_other_escape() {
    assert_eq!(
        serde_json::to_value(StopReason::ContentFilter).expect("serialize"),
        json!("content_filter")
    );
    assert_eq!(
        serde_json::to_value(StopReason::ToolUse).expect("serialize"),
        json!("tool_use")
    );
    // Unclassified reason: bare string, lossless round-trip.
    assert_eq!(
        serde_json::to_value(StopReason::Other("spam".into())).expect("serialize"),
        json!("spam")
    );
    // Deserialization fallback: unknown strings become Other, known strings become the
    // matching variant.
    assert_eq!(
        serde_json::from_value::<StopReason>(json!("spam")).expect("deserialize"),
        StopReason::Other("spam".into())
    );
    assert_eq!(
        serde_json::from_value::<StopReason>(json!("content_filter")).expect("deserialize"),
        StopReason::ContentFilter
    );
}

// Also: Role is not tagged, snake_case, avoiding hand-written string mappings in codecs.
#[test]
fn ir_role_snake_case() {
    assert_eq!(
        serde_json::to_value(Role::Assistant).expect("serialize"),
        json!("assistant")
    );
}
