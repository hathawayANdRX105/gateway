//! Behavior tests for `FormatRegistry`'s two-hop routing: directions stay separate,
//! zero-conversion passthrough, unregistered errors, per-stream encoder state isolation.
//!
//! Uses a fake codec (records the call sequence + leaves a recognizable marker in the
//! output) rather than built-ins — those land in WP2-WP5; this file tests WP1 dispatch
//! semantics and depends on no concrete format.

use bytes::Bytes;
use gateway_protocol_bridge::adaptor::{AdaptorError, Protocol};
use gateway_protocol_bridge::format_codec::{FormatCodec, FormatRegistry, StreamEncoder};
use gateway_protocol_bridge::ir::{
    ContentBlock, LlmRequest, LlmResponse, StopReason, StreamEvent, Usage,
};
use serde_json::{Map, Value, json};
use std::sync::{Arc, Mutex};

// ---------- Fake codec ----------

/// Fake codec that records its call sequence and leaves a format marker in its output.
///
/// `encode_*` output carries `"encoded_by": "<format>"`, so callers can assert "the
/// output really came from the target format's encode" — stronger than asserting
/// `is_ok()`: it checks an observable artifact, not a call trace.
struct FakeCodec {
    format: Protocol,
    calls: Arc<Mutex<Vec<String>>>,
}

impl FakeCodec {
    fn tag(&self) -> &'static str {
        match self.format {
            Protocol::OpenAi => "openai",
            Protocol::Claude => "claude",
            Protocol::Gemini => "gemini",
            Protocol::OpenAIResp => "openai_resp",
            Protocol::Passthrough => "passthrough",
        }
    }

    fn record(&self, what: &str) {
        self.calls
            .lock()
            .unwrap()
            .push(format!("{}::{what}", self.tag()));
    }
}

impl FormatCodec for FakeCodec {
    fn format(&self) -> Protocol {
        self.format
    }

    fn decode_request(&self, body: Bytes) -> Result<LlmRequest, AdaptorError> {
        self.record("decode_request");
        let v: Value =
            serde_json::from_slice(&body).map_err(|e| AdaptorError::DecodeFailed(e.to_string()))?;
        Ok(mk_request(
            v.get("model").and_then(Value::as_str).unwrap_or(""),
        ))
    }

    fn encode_request(&self, req: &LlmRequest) -> Result<Bytes, AdaptorError> {
        self.record("encode_request");
        Ok(Bytes::from(
            serde_json::to_vec(&json!({
                "encoded_by": self.tag(),
                "model": req.model,
            }))
            .map_err(|e| AdaptorError::EncodeFailed(e.to_string()))?,
        ))
    }

    fn decode_response(&self, body: Bytes) -> Result<LlmResponse, AdaptorError> {
        self.record("decode_response");
        let v: Value =
            serde_json::from_slice(&body).map_err(|e| AdaptorError::DecodeFailed(e.to_string()))?;
        Ok(LlmResponse {
            id: v
                .get("id")
                .and_then(Value::as_str)
                .unwrap_or("fallback")
                .to_string(),
            model: "m".into(),
            outputs: vec![ContentBlock::Text { text: "x".into() }],
            usage: Usage {
                prompt_tokens: 1,
                completion_tokens: 2,
                cached_tokens: None,
                ..Default::default()
            },
            stop_reason: StopReason::Stop,
        })
    }

    fn encode_response(&self, resp: &LlmResponse) -> Result<Bytes, AdaptorError> {
        self.record("encode_response");
        Ok(Bytes::from(
            serde_json::to_vec(&json!({
                "encoded_by": self.tag(),
                "id": resp.id,
            }))
            .map_err(|e| AdaptorError::EncodeFailed(e.to_string()))?,
        ))
    }

    fn decode_event(&self, data: &str) -> Result<Vec<StreamEvent>, AdaptorError> {
        self.record("decode_event");
        Ok(vec![StreamEvent::TextDelta {
            index: 0,
            text: data.to_string(),
        }])
    }

    fn stream_encoder(&self) -> Box<dyn StreamEncoder> {
        self.record("stream_encoder");
        Box::new(FakeEncoder {
            tag: self.tag(),
            // Accumulates across events: emits no bytes before finish(), proving state really
            // stays in the encoder.
            pending: Vec::new(),
        })
    }
}

/// Accumulating encoder: `encode_event` only buffers, `finish` emits everything at once.
///
/// The shape deliberately inverts "1:1 passthrough per event" — if `stream_encoder()`
/// created a fresh instance per call (no shared state, as the contract requires) and the
/// caller holds one instance, `finish` sees every event; state reuse/loss fails immediately.
struct FakeEncoder {
    tag: &'static str,
    pending: Vec<String>,
}

impl StreamEncoder for FakeEncoder {
    fn encode_event(&mut self, ev: &StreamEvent) -> Result<Vec<Bytes>, AdaptorError> {
        match ev {
            StreamEvent::TextDelta { text, .. } => self.pending.push(text.clone()),
            other => self.pending.push(format!("{other:?}")),
        }
        // Emitting no bytes while accumulating is legal (the trait docs allow zero frames).
        Ok(vec![])
    }

    fn finish(&mut self) -> Result<Vec<Bytes>, AdaptorError> {
        Ok(vec![Bytes::from(format!(
            "{}:{}",
            self.tag,
            self.pending.join("|")
        ))])
    }
}

// ---------- Fixtures ----------

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

fn mk_registry(formats: &[Protocol]) -> (FormatRegistry, Arc<Mutex<Vec<String>>>) {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let mut reg = FormatRegistry::new();
    for fmt in formats {
        reg.register(Arc::new(FakeCodec {
            format: *fmt,
            calls: calls.clone(),
        }));
    }
    (reg, calls)
}

/// Binary garbage that is not valid JSON: any codec that actually parses it will fail.
const GARBAGE: &[u8] = b"\x00\xff\x01prose not json";

// ---------- Tests ----------

// 1. Two-hop routing: src decode + dst encode; the output must come from dst's encode.
//    Guards against "registered but not two-hop" — a wrong pair-table lookup, or calling
//    one side and returning the original body.
#[test]
fn translate_request_runs_two_hops_and_returns_dst_encoding() {
    let (reg, calls) = mk_registry(&[Protocol::Claude, Protocol::Gemini]);

    let out = reg
        .translate_request(
            Protocol::Claude,
            Protocol::Gemini,
            Bytes::from_static(b"{\"model\":\"m1\"}"),
        )
        .expect("two hops should succeed");

    let v: Value = serde_json::from_slice(&out).expect("output must be JSON");
    assert_eq!(
        v["encoded_by"], "gemini",
        "output must come from the target format's encode, actual: {v}"
    );
    assert_eq!(
        v["model"], "m1",
        "IR must carry model verbatim into the second hop"
    );

    assert_eq!(
        *calls.lock().unwrap(),
        vec!["claude::decode_request", "gemini::encode_request"],
        "two hops should be exactly src.decode_request → dst.encode_request"
    );
}

// 2. Directions stay separate: request direction touches only request methods, response
//    only response methods. The old trap was "request codec translates response → 502
//    Unsupported"; the method name now carries direction, locking it against a flag.
#[test]
fn translate_keeps_request_and_response_directions_separate() {
    let (reg, calls) = mk_registry(&[Protocol::Claude, Protocol::OpenAi]);

    reg.translate_request(
        Protocol::Claude,
        Protocol::OpenAi,
        Bytes::from_static(b"{\"model\":\"m\"}"),
    )
    .expect("request direction should succeed");

    let after_req = calls.lock().unwrap().clone();
    assert!(
        !after_req.iter().any(|c| c.contains("response")),
        "translate_request must not touch any response methods, actual: {after_req:?}"
    );

    calls.lock().unwrap().clear();

    reg.translate_response(
        Protocol::OpenAi,
        Protocol::Claude,
        Bytes::from_static(b"{\"id\":\"r1\"}"),
    )
    .expect("response direction should succeed");

    let after_resp = calls.lock().unwrap().clone();
    assert_eq!(
        after_resp,
        vec!["openai::decode_response", "claude::encode_response"],
        "translate_response should take exactly the two response methods"
    );
    assert!(
        !after_resp.iter().any(|c| c.contains("request")),
        "translate_response must not touch any request methods, actual: {after_resp:?}"
    );
}

// 3. Zero-conversion passthrough: same format, or either side Passthrough, returns the
//    bytes verbatim. The payload is invalid JSON, so any real parse would fail — making
//    "verbatim" falsifiable. Guards against forcing same-format bodies through the IR.
#[test]
fn translate_passthrough_returns_bytes_verbatim() {
    // The registry is deliberately empty: passthrough depends on no codec.
    let reg = FormatRegistry::new();
    let garbage = Bytes::from_static(GARBAGE);

    for (src, dst) in [
        (Protocol::Claude, Protocol::Claude),
        (Protocol::OpenAi, Protocol::OpenAi),
        (Protocol::OpenAIResp, Protocol::OpenAIResp),
        (Protocol::Passthrough, Protocol::Claude),
        (Protocol::Gemini, Protocol::Passthrough),
    ] {
        let out = reg
            .translate_request(src, dst, garbage.clone())
            .unwrap_or_else(|e| panic!("{src:?}→{dst:?} should pass through, but failed: {e}"));
        assert_eq!(
            out, garbage,
            "{src:?}→{dst:?} request passthrough must return verbatim"
        );

        let out = reg
            .translate_response(src, dst, garbage.clone())
            .unwrap_or_else(|e| {
                panic!("{src:?}→{dst:?} response should pass through, but failed: {e}")
            });
        assert_eq!(
            out, garbage,
            "{src:?}→{dst:?} response passthrough must return verbatim"
        );
    }
}

// 4. Unregistered format: resolve returns None, translate returns NotRegistered (no panic,
//    no silent passthrough). Guards against "codec not found → passthrough": that fakes an
//    unimplemented conversion as success, surfacing as shape mismatch instead of an error.
#[test]
fn unregistered_format_reports_not_registered() {
    let (reg, calls) = mk_registry(&[Protocol::Claude]);

    assert!(
        reg.resolve(Protocol::Gemini).is_none(),
        "unregistered format resolve should be None"
    );
    assert!(
        reg.resolve(Protocol::Claude).is_some(),
        "registered format resolve should have a value"
    );

    let err = reg
        .translate_request(
            Protocol::Claude,
            Protocol::Gemini,
            Bytes::from_static(b"{\"model\":\"m\"}"),
        )
        .expect_err("unregistered target format should error");

    match err {
        AdaptorError::NotRegistered { from, to } => {
            assert_eq!(from, Protocol::Claude);
            assert_eq!(to, Protocol::Gemini);
        }
        other => panic!("expected NotRegistered, actual: {other}"),
    }

    // An unregistered source format must fail too (no pass just because dst exists).
    let err = reg
        .translate_response(
            Protocol::Gemini,
            Protocol::Claude,
            Bytes::from_static(b"{}"),
        )
        .expect_err("unregistered source format should error");
    assert!(matches!(err, AdaptorError::NotRegistered { .. }));

    // Neither failure may produce any codec call.
    assert_eq!(
        calls.lock().unwrap().len(),
        0,
        "unregistered path must not call any codec methods"
    );
}

// 5. Each stream gets a fresh encoder, states never mix: feed each encoder its own events;
//    finish sees only its own. Guards against "stream state on the codec" — cross-stream leaks.
#[test]
fn stream_encoders_do_not_share_state() {
    let (reg, _calls) = mk_registry(&[Protocol::Claude]);
    let codec = reg.resolve(Protocol::Claude).expect("registered");

    let mut a = codec.stream_encoder();
    let mut b = codec.stream_encoder();

    a.encode_event(&StreamEvent::TextDelta {
        index: 0,
        text: "alpha".into(),
    })
    .expect("a encode");
    b.encode_event(&StreamEvent::TextDelta {
        index: 0,
        text: "beta".into(),
    })
    .expect("b encode");

    let out_a = a.finish().expect("a finish");
    let out_b = b.finish().expect("b finish");

    let a_text = String::from_utf8_lossy(&out_a.concat()).to_string();
    let b_text = String::from_utf8_lossy(&out_b.concat()).to_string();

    assert_eq!(
        a_text, "claude:alpha",
        "stream a should only contain its own events"
    );
    assert_eq!(
        b_text, "claude:beta",
        "stream b should only contain its own events"
    );
}

// 6. The encoder really holds state across events: multiple events in one stream are all
//    visible at finish. Guards against "encode_event works alone" — Claude/Responses block
//    boundaries and tool fragments depend on this chain; a stateless encoder surfaces in WP3/WP5.
#[test]
fn stream_encoder_accumulates_across_events() {
    let (reg, calls) = mk_registry(&[Protocol::OpenAIResp]);
    let codec = reg.resolve(Protocol::OpenAIResp).expect("registered");

    let mut enc = codec.stream_encoder();

    // Emitting no bytes while accumulating is legal: assert zero frames to lock this semantics.
    for text in ["one", "two", "three"] {
        let frames = enc
            .encode_event(&StreamEvent::TextDelta {
                index: 0,
                text: text.into(),
            })
            .expect("encode");
        assert!(
            frames.is_empty(),
            "accumulation must not emit frames, actual: {frames:?}"
        );
    }

    let out = enc.finish().expect("finish");
    let text = String::from_utf8_lossy(&out.concat()).to_string();
    assert_eq!(
        text, "openai_resp:one|two|three",
        "finish must see all three events (proving cross-event state lives in the encoder)"
    );

    assert_eq!(
        *calls.lock().unwrap(),
        vec!["openai_resp::stream_encoder"],
        "getting the encoder should only call stream_encoder once"
    );
}
