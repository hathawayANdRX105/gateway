//! Codec for the OpenAI Responses API format — IR ↔ `/v1/responses` (WP5).
//!
//! The **first brand-new format** in this crate (the rest migrated from the old codecs),
//! used to validate the two-hop abstraction's extensibility: adding a format means
//! implementing this one trait, not pairing a converter with every existing format.
//!
//! ## Key shape differences from Chat Completions
//!
//! Both APIs are called OpenAI, but their shapes differ and mixing them fails silently:
//!
//! - Tool definitions are **flat** `{type:"function", name, description, parameters}`,
//!   not wrapped in a `function` sub-object as in Chat.
//! - The system prompt is `instructions`, not a system role inside messages.
//! - The input is `input` (a string, an array of messages, or an items array containing
//!   `function_call` / `function_call_output`), not `messages`.
//! - The output is `output[]` items (`message` / `function_call` / `reasoning`), not
//!   `choices[]`.
//! - Streaming is a set of **lifecycle events** (`response.created` →
//!   `output_item.added` → `*_delta` → `*_done` → `response.completed`), and client
//!   parsers depend strictly on the order: receiving a delta without
//!   `response.created` / `output_item.added` fails outright.
//!
//! This module's `StreamEncoder` therefore fills in those boundary events as needed,
//! which is why the encoding side must be stateful (it lives in
//! [`crate::responses_stream`], an encoder-only part of this codec).
//!
//! ## Wire models
//!
//! Every Responses field name is declared exactly once as a serde field of a DTO in
//! [`wire`] (or in [`crate::responses_items`] for the shapes request, response and
//! stream share); the codec body is decode → `wire::Request::into_ir` and IR → wire
//! struct → bytes. Fields decode leniently ([`ir::Lenient`] degrades type mismatches
//! to defaults), and the old hand-maintained key lists are exactly the declared field
//! sets — they can no longer drift. Unlike the sibling codecs this body carries NO
//! flattened `extra` map: the old decoder forwarded exactly three unmodeled keys
//! through the IR extra (an allowlist, not an exclusion list), so those three are
//! declared fields and every other unmodeled key stays dropped, key for key.
//!
//! [`ir::Lenient`]: crate::ir::Lenient

use bytes::Bytes;
use serde_json::Value;

use crate::adaptor::{AdaptorError, Protocol};
use crate::format_codec::{FormatCodec, StreamEncoder};
use crate::ir::{
    LlmRequest, LlmResponse, StreamEvent, apply_extra_fields, decode_body, encode_object,
    encode_value, struct_to_map, wire_value,
};
use crate::responses_items::InputItemOut;
use crate::responses_stream::ResponsesStreamEncoder;

/// Codec for the OpenAI Responses API format.
#[derive(Debug, Default, Clone, Copy)]
pub struct ResponsesCodec;

impl ResponsesCodec {
    /// Create a codec (stateless, shareable).
    pub const fn new() -> Self {
        Self
    }
}

impl FormatCodec for ResponsesCodec {
    fn format(&self) -> Protocol {
        Protocol::OpenAIResp
    }

    /// Responses request body → IR.
    fn decode_request(&self, body: Bytes) -> Result<LlmRequest, AdaptorError> {
        let v: Value = decode_body(&body)?;
        if !v.is_object() {
            return Err(AdaptorError::DecodeFailed(
                "responses request body is not an object".into(),
            ));
        }
        // Every wire field is lenient, so an object-shaped body never fails conversion.
        let req: wire::Request = serde_json::from_value(v).unwrap_or_default();
        Ok(req.into_ir())
    }

    /// IR → Responses request body.
    fn encode_request(&self, req: &LlmRequest) -> Result<Bytes, AdaptorError> {
        let out = wire::RequestOut {
            model: &req.model,
            // System blocks go back into the single `instructions` slot; Responses
            // has no system role inside input.
            instructions: (!req.system.is_empty()).then(|| {
                req.system
                    .iter()
                    .map(|b| b.text.as_str())
                    .collect::<Vec<_>>()
                    .join("\n")
            }),
            // Always present, even when empty: a null `input` makes upstreams 400.
            input: req.messages.iter().map(InputItemOut::from_ir).collect(),
            tools: req.tools.iter().map(wire::ToolOut::from_ir).collect(),
            temperature: req.sampling.as_ref().and_then(|s| s.temperature),
            top_p: req.sampling.as_ref().and_then(|s| s.top_p),
            max_output_tokens: req.sampling.as_ref().and_then(|s| s.max_tokens),
            stream: req.stream,
        };
        let mut out = struct_to_map(&out)?;
        apply_extra_fields(&mut out, req);
        encode_object(out)
    }

    /// Responses non-streaming response body → IR.
    fn decode_response(&self, body: Bytes) -> Result<LlmResponse, AdaptorError> {
        let v: Value = decode_body(&body)?;
        // Upstreams are hostile input, not validated senders: any missing or
        // wrong-typed field decodes to empty IR rather than failing (the
        // `.unwrap_or_default()` contract the old `.get(..).and_then(..)` chains had).
        let resp: wire::Response = serde_json::from_value(v).unwrap_or_default();
        Ok(resp.into_ir())
    }

    /// IR → Responses non-streaming response body.
    fn encode_response(&self, resp: &LlmResponse) -> Result<Bytes, AdaptorError> {
        encode_value(&wire_value(&wire::ResponseOut::from_ir(resp)))
    }

    /// One complete SSE frame's data payload → IR events (stateless, 1:1 per event).
    fn decode_event(&self, data: &str) -> Result<Vec<StreamEvent>, AdaptorError> {
        let trimmed = data.trim();
        // `[DONE]` and blank lines are SSE control frames, not content.
        if trimmed.is_empty() || trimmed == "[DONE]" {
            return Ok(Vec::new());
        }
        // Frames that are not valid JSON are control noise: skip, never fail the stream.
        let Ok(ev) = serde_json::from_str::<Value>(trimmed) else {
            return Ok(Vec::new());
        };
        Ok(wire::event_into_events(ev))
    }

    fn stream_encoder(&self) -> Box<dyn StreamEncoder> {
        Box::new(ResponsesStreamEncoder::new())
    }
}

// ---------- Wire models ----------
mod wire {
    use serde::{Deserialize, Serialize};
    use serde_json::{Map, Value, json};

    use crate::ir::{
        ContentBlock, Lenient, LlmRequest, LlmResponse, Message, Present, Role, StopReason,
        StreamEvent, ToolDef, decode_text_blocks, event_index, from_event, sampling_params,
    };
    use crate::responses_items::{
        InputItem, InputItemOut, OutputItem, UsageIn, UsageOut, resolve_call_id,
    };

    // ---- request decode ----

    /// Responses request body. One serde field per modeled wire key.
    ///
    /// This DTO deliberately has NO flattened `extra` map (unlike the Chat/Claude/
    /// Gemini request DTOs): the old decoder forwarded exactly three unmodeled keys
    /// through the IR `extra`, so those are declared below and everything else stays
    /// dropped — widening the set via a flatten would silently change cross-hop
    /// payloads.
    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    pub(super) struct Request {
        model: Lenient<String>,
        input: Lenient<Option<InputWire>>,
        /// The system-prompt slot of Responses (a string or a `{"text": ...}` parts
        /// array, decoded by the shared helper).
        instructions: Present,
        tools: Lenient<Vec<Lenient<ToolIn>>>,
        temperature: Lenient<Option<f64>>,
        top_p: Lenient<Option<f64>>,
        /// Read twice by the old code, read twice here: as the sampling ceiling
        /// (numeric) AND as an unmodeled key that round-trips through `extra` (raw).
        max_output_tokens: Present,
        stream: Lenient<bool>,
        // The only other keys the old allowlist forwarded through `extra`.
        previous_response_id: Present,
        store: Present,
    }

    /// The `input` slot: a bare string or an items array; anything else (null, an
    /// object, numbers) is no input at all — both untagged arms fail and the
    /// enclosing `Lenient` degrades it to `None`.
    #[derive(Debug, Deserialize)]
    #[serde(untagged)]
    enum InputWire {
        Text(String),
        Items(Vec<Lenient<InputItem>>),
    }

    impl Request {
        pub(super) fn into_ir(self) -> LlmRequest {
            let messages = match self.input.0 {
                // Shape one: a bare string = a single user message (even empty).
                Some(InputWire::Text(text)) => vec![Message {
                    role: Role::User,
                    content: vec![ContentBlock::Text { text }],
                }],
                // Shapes two and three: an array of messages and/or items.
                Some(InputWire::Items(items)) => items
                    .into_iter()
                    .map(|item| item.0.into_message())
                    .collect(),
                None => Vec::new(),
            };
            // The sampling side reads the key as a number; `extra` carries the raw
            // value back, so both consumers are served from one declared field.
            let max_output_tokens = self.max_output_tokens.0.as_ref().and_then(Value::as_u64);
            LlmRequest {
                model: self.model.0,
                messages,
                system: decode_text_blocks(self.instructions.0.as_ref()),
                tools: self
                    .tools
                    .0
                    .into_iter()
                    .filter_map(|t| t.0.into_def())
                    .collect(),
                // The old codec never modeled tool_choice: it is not an allowlisted
                // extra key either, so it stays None exactly as before.
                tool_choice: None,
                sampling: sampling_params(
                    self.temperature.0,
                    self.top_p.0,
                    max_output_tokens,
                    Map::new(),
                ),
                stream: self.stream.0,
                // Exactly the three allowlisted keys, present-only (old `decode_extra`).
                extra: [
                    ("previous_response_id", self.previous_response_id.0),
                    ("store", self.store.0),
                    ("max_output_tokens", self.max_output_tokens.0),
                ]
                .into_iter()
                .filter_map(|(key, value)| value.map(|value| (key.to_string(), value)))
                .collect(),
            }
        }
    }

    /// `tools[]` entry — Responses tools are FLAT (`name`/`parameters` at the top
    /// level, unlike Chat's `function` wrapper); an entry without a usable string
    /// `name` is skipped whole (the old `filter_map` contract).
    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct ToolIn {
        name: Lenient<Option<String>>,
        description: Lenient<Option<String>>,
        /// Absent means `{}`; an explicit `null` stays `null` (the old
        /// `get().cloned()` distinction) — hence `Present`, not `Lenient<Option<_>>`.
        parameters: Present,
    }

    impl ToolIn {
        fn into_def(self) -> Option<ToolDef> {
            Some(ToolDef {
                name: self.name.0?,
                description: self.description.0,
                input_schema: self.parameters.0.unwrap_or_else(|| json!({})),
                extra: Map::new(),
            })
        }
    }

    // ---- response decode ----

    /// Responses non-streaming response body.
    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    pub(super) struct Response {
        id: Lenient<String>,
        model: Lenient<String>,
        output: Lenient<Vec<Lenient<OutputItem>>>,
        status: Lenient<Option<String>>,
        usage: Lenient<UsageIn>,
    }

    impl Response {
        pub(super) fn into_ir(self) -> LlmResponse {
            let items = self.output.0;
            // Precedence of the old chain: a function_call outranks truncation,
            // truncation outranks a plain stop.
            let has_tool_call = items.iter().any(|item| item.0.is_function_call());
            let truncated = self.status.0.as_deref() == Some("incomplete");
            LlmResponse {
                id: self.id.0,
                model: self.model.0,
                outputs: items
                    .into_iter()
                    .flat_map(|item| item.0.into_blocks())
                    .collect(),
                usage: self.usage.0.into_usage(),
                stop_reason: match (has_tool_call, truncated) {
                    (true, _) => StopReason::ToolUse,
                    (false, true) => StopReason::Length,
                    (false, false) => StopReason::Stop,
                },
            }
        }
    }

    // ---- stream event decode ----

    /// Whole SSE event JSON → IR events. The `type` tag picks the DTO; frames
    /// without a string tag are control noise, and unmodeled event types stay whole
    /// as `Raw` so a same-protocol hop re-emits them verbatim.
    pub(super) fn event_into_events(ev: Value) -> Vec<StreamEvent> {
        let Some(kind) = ev.get("type").and_then(Value::as_str) else {
            return Vec::new();
        };
        match kind {
            "response.created" => from_event::<CreatedEvent>(&ev).into_events(),
            "response.output_text.delta" => from_event::<DeltaEvent>(&ev).into_text_delta(),
            // Both reasoning deltas share the same mapping.
            "response.reasoning_summary_text.delta" | "response.reasoning_text.delta" => {
                from_event::<DeltaEvent>(&ev).into_thinking_delta()
            }
            "response.output_item.added" => from_event::<ItemAddedEvent>(&ev).into_events(),
            "response.function_call_arguments.delta" => {
                from_event::<DeltaEvent>(&ev).into_arguments_delta()
            }
            "response.completed" => from_event::<CompletedEvent>(&ev).into_events(false),
            "response.incomplete" => from_event::<CompletedEvent>(&ev).into_events(true),
            "response.failed" => from_event::<FailedEvent>(&ev).into_events(),
            // The remaining lifecycle events (item.done / content_part.* / the various
            // done events) have no counterpart in IR — IR expresses boundaries through
            // the index and the StreamEvent type. Unlike Claude's decoder, an unmodeled
            // Responses event is dropped rather than preserved as `Raw` (the long-standing
            // contract of this codec; tests pin the lifecycle drops).
            _ => Vec::new(),
        }
    }

    /// `response.created`: id/model from the response snapshot.
    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct CreatedEvent {
        response: Lenient<SnapshotIds>,
    }

    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct SnapshotIds {
        id: Lenient<String>,
        model: Lenient<String>,
    }

    impl CreatedEvent {
        fn into_events(self) -> Vec<StreamEvent> {
            let resp = self.response.0;
            vec![StreamEvent::MessageStart {
                id: resp.id.0,
                model: resp.model.0,
            }]
        }
    }

    /// The delta events share this shape; the dispatch arm picks the IR event.
    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct DeltaEvent {
        output_index: Lenient<u64>,
        delta: Lenient<Option<String>>,
    }

    impl DeltaEvent {
        fn into_text_delta(self) -> Vec<StreamEvent> {
            let index = event_index(self.output_index.0);
            let text = self.delta.0.unwrap_or_default();
            vec![StreamEvent::TextDelta { index, text }]
        }

        fn into_thinking_delta(self) -> Vec<StreamEvent> {
            let index = event_index(self.output_index.0);
            let text = self.delta.0.unwrap_or_default();
            vec![StreamEvent::ThinkingDelta { index, text }]
        }

        fn into_arguments_delta(self) -> Vec<StreamEvent> {
            let index = event_index(self.output_index.0);
            let arguments_delta = self.delta.0.unwrap_or_default();
            vec![StreamEvent::ToolCallDelta {
                index,
                arguments_delta,
            }]
        }
    }

    /// `response.output_item.added`: only function_call carries the identity IR needs.
    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct ItemAddedEvent {
        output_index: Lenient<u64>,
        item: Lenient<AddedItem>,
    }

    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct AddedItem {
        #[serde(rename = "type")]
        kind: Lenient<Option<String>>,
        call_id: Present,
        id: Lenient<Option<String>>,
        name: Lenient<Option<String>>,
    }

    impl ItemAddedEvent {
        fn into_events(self) -> Vec<StreamEvent> {
            let item = self.item.0;
            if item.kind.0.as_deref() != Some("function_call") {
                return Vec::new();
            }
            // The start of a function_call item carries call_id and name — both
            // appear only here; losing them leaves the client with ownerless
            // arguments (same shape as Claude's content_block_start).
            vec![StreamEvent::ToolCallStart {
                index: event_index(self.output_index.0),
                id: resolve_call_id(item.call_id, item.id.0),
                name: item.name.0.unwrap_or_default(),
            }]
        }
    }

    /// `response.completed` / `response.incomplete`: usage + stop reason. The
    /// truncation flag comes from the event type itself, not from the snapshot.
    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct CompletedEvent {
        response: Lenient<CompletedSnapshot>,
    }

    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct CompletedSnapshot {
        usage: Lenient<UsageIn>,
        output: Lenient<Vec<Lenient<KindItem>>>,
    }

    /// Snapshot item: only the `type` tag matters for the stop-reason scan.
    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct KindItem {
        #[serde(rename = "type")]
        kind: Lenient<Option<String>>,
    }

    impl CompletedEvent {
        fn into_events(self, truncated: bool) -> Vec<StreamEvent> {
            let snapshot = self.response.0;
            let mut out = Vec::new();
            let usage = snapshot.usage.0.into_usage();
            // An all-zero usage carries no billing fact: emitting it would let a
            // downstream merge overwrite real usage with zeros (old contract).
            if usage.prompt_tokens > 0 || usage.completion_tokens > 0 {
                out.push(StreamEvent::MessageDelta { usage: Some(usage) });
            }
            let has_tool_call = snapshot
                .output
                .0
                .iter()
                .any(|item| item.0.kind.0.as_deref() == Some("function_call"));
            let reason = match (truncated, has_tool_call) {
                (true, _) => StopReason::Length,
                (false, true) => StopReason::ToolUse,
                (false, false) => StopReason::Stop,
            };
            out.push(StreamEvent::MessageStop { reason });
            out
        }
    }

    /// `response.failed` → IR `Error`.
    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct FailedEvent {
        response: Lenient<FailedBody>,
    }

    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct FailedBody {
        error: Lenient<FailedError>,
    }

    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct FailedError {
        message: Lenient<Option<String>>,
    }

    impl FailedEvent {
        fn into_events(self) -> Vec<StreamEvent> {
            vec![StreamEvent::Error {
                message: self
                    .response
                    .0
                    .error
                    .0
                    .message
                    .0
                    .unwrap_or_else(|| "responses stream failed".to_string()),
            }]
        }
    }

    // ---- encode models ----

    /// IR → Responses request body. `skip_serializing_if` reproduces the old "insert
    /// only when present" behavior; unmodeled extras merge into the emitted map
    /// afterwards and only fill missing keys, so the modeled fields always win.
    #[derive(Debug, Serialize)]
    pub(super) struct RequestOut<'a> {
        pub(super) model: &'a str,
        /// System blocks joined back into the single `instructions` slot.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(super) instructions: Option<String>,
        /// Always emitted — even empty: upstreams reject a null `input` with 400.
        pub(super) input: Vec<InputItemOut<'a>>,
        #[serde(skip_serializing_if = "Vec::is_empty")]
        pub(super) tools: Vec<ToolOut<'a>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(super) temperature: Option<f64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(super) top_p: Option<f64>,
        /// The IR `max_tokens` ceiling travels under the Responses name.
        #[serde(rename = "max_output_tokens", skip_serializing_if = "Option::is_none")]
        pub(super) max_output_tokens: Option<u64>,
        pub(super) stream: bool,
    }

    /// IR tool definition → flat Responses `tools[]` entry (never Chat's wrapper).
    #[derive(Debug, Serialize)]
    pub(super) struct ToolOut<'a> {
        #[serde(rename = "type")]
        kind: &'static str,
        name: &'a str,
        parameters: &'a Value,
        #[serde(skip_serializing_if = "Option::is_none")]
        description: Option<&'a str>,
    }

    impl<'a> ToolOut<'a> {
        pub(super) fn from_ir(t: &'a ToolDef) -> Self {
            Self {
                kind: "function",
                name: &t.name,
                parameters: &t.input_schema,
                description: t.description.as_deref(),
            }
        }
    }

    /// IR → Responses non-streaming response body.
    #[derive(Debug, Serialize)]
    pub(super) struct ResponseOut<'a> {
        id: &'a str,
        object: &'static str,
        status: &'static str,
        model: &'a str,
        output: Vec<OutputItemOut<'a>>,
        usage: UsageOut,
    }

    /// One `output[]` item on the encode side: the message item or standalone
    /// function_call items.
    #[derive(Debug, Serialize)]
    #[serde(untagged)]
    enum OutputItemOut<'a> {
        Message(MessageItemOut),
        FunctionCall(FunctionCallItemOut<'a>),
    }

    #[derive(Debug, Serialize)]
    struct MessageItemOut {
        #[serde(rename = "type")]
        kind: &'static str,
        id: String,
        role: &'static str,
        status: &'static str,
        content: Vec<OutputTextPart>,
    }

    #[derive(Debug, Serialize)]
    struct OutputTextPart {
        #[serde(rename = "type")]
        kind: &'static str,
        text: String,
        annotations: Vec<Value>,
    }

    #[derive(Debug, Serialize)]
    struct FunctionCallItemOut<'a> {
        #[serde(rename = "type")]
        kind: &'static str,
        id: String,
        call_id: &'a str,
        name: &'a str,
        arguments: String,
        status: &'static str,
    }

    impl<'a> ResponseOut<'a> {
        pub(super) fn from_ir(resp: &'a LlmResponse) -> Self {
            // Text blocks merge into one assistant message item (old shape).
            let text: String = resp
                .outputs
                .iter()
                .filter_map(|b| match b {
                    ContentBlock::Text { text } => Some(text.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("");
            let mut output = Vec::new();
            if !text.is_empty() {
                output.push(OutputItemOut::Message(MessageItemOut {
                    kind: "message",
                    id: format!("msg_{}", resp.id),
                    role: "assistant",
                    status: "completed",
                    content: vec![OutputTextPart {
                        kind: "output_text",
                        text,
                        annotations: Vec::new(),
                    }],
                }));
            }
            output.extend(resp.outputs.iter().filter_map(|b| match b {
                ContentBlock::ToolUse { id, name, input } => {
                    Some(OutputItemOut::FunctionCall(FunctionCallItemOut {
                        kind: "function_call",
                        id: format!("fc_{id}"),
                        call_id: id,
                        name,
                        // Arguments travel as a JSON string; serialization failure
                        // must not fail the whole response encode.
                        arguments: serde_json::to_string(input).unwrap_or_else(|_| "{}".into()),
                        status: "completed",
                    }))
                }
                _ => None,
            }));
            Self {
                id: &resp.id,
                object: "response",
                // Truncation is the only status the old shape distinguished.
                status: if resp.stop_reason == StopReason::Length {
                    "incomplete"
                } else {
                    "completed"
                },
                model: &resp.model,
                output,
                usage: UsageOut::from_ir(&resp.usage),
            }
        }
    }
}
