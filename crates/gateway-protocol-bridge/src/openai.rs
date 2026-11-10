//! `OpenAI` Chat Completions format codec — IR ↔ `/v1/chat/completions` (WP2).
//!
//! Single-format bidirectional codec. Chat Completions is the de-facto hub shape,
//! so this module is also the baseline the other formats are compared against:
//! field naming, usage placement, and `tool_calls` assembly all follow this file.
//!
//! ## Wire models
//!
//! Every Chat field name is declared exactly once, as a serde field of a DTO in
//! [`wire`]; the codec body is decode → [`wire::Request::into_ir`] and IR → wire
//! struct → bytes. Wire DTOs decode leniently (nested [`ir::Lenient`] fields degrade
//! type mismatches to defaults — the same contract the old `.and_then(Value::as_)
//! .unwrap_or_default()` chains had), and unmodeled top-level fields land in the
//! flattened `extra` map, so the hand-maintained "known keys" list is gone.
//!
//! ## Streaming id stability
//!
//! Every Chat Completions chunk carries an `id`, and clients (as well as the
//! billing side's streaming aggregation) treat it as the identity of one
//! completion. The old Gemini side generated a fresh uuid per chunk — the
//! counterexample recorded in G1; here [`OpenAiStreamEncoder`] remembers the first
//! id and reuses it for the whole stream.
//!
//! [`ir::Lenient`]: crate::ir

use bytes::Bytes;
use serde_json::{Value, json};
use std::collections::BTreeSet;

use crate::adaptor::{AdaptorError, Protocol};
use crate::format_codec::{FormatCodec, StreamEncoder};
use crate::ir::{
    LlmRequest, LlmResponse, StreamEvent, Usage, apply_extra_fields, apply_extra_map, decode_body,
    encode_object, encode_value, struct_to_map, wire_value,
};
use crate::sse::data_frame;

/// `OpenAI` Chat Completions format codec.
#[derive(Debug, Default, Clone, Copy)]
pub struct OpenAiCodec;

impl OpenAiCodec {
    /// Creates a codec (stateless, safe to share).
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl FormatCodec for OpenAiCodec {
    fn format(&self) -> Protocol {
        Protocol::OpenAi
    }

    /// Chat Completions request body → IR.
    fn decode_request(&self, body: Bytes) -> Result<LlmRequest, AdaptorError> {
        let v: Value = decode_body(&body)?;
        if !v.is_object() {
            return Err(AdaptorError::DecodeFailed(
                "openai request body is not an object".into(),
            ));
        }
        // Every wire field is lenient, so an object-shaped body never fails conversion.
        let req: wire::Request = serde_json::from_value(v).unwrap_or_default();
        Ok(req.into_ir())
    }

    /// IR → Chat Completions request body.
    fn encode_request(&self, req: &LlmRequest) -> Result<Bytes, AdaptorError> {
        // system goes back to the head of messages: Chat Completions carries the
        // system prompt as a role message.
        let messages: Vec<wire::MessageOut> = req
            .system
            .iter()
            .map(|b| wire::MessageOut {
                role: "system",
                content: json!(b.text),
                tool_calls: None,
                tool_call_id: None,
            })
            .chain(req.messages.iter().map(wire::MessageOut::from_ir))
            .collect();

        let out = wire::RequestOut {
            model: &req.model,
            messages,
            tools: (!req.tools.is_empty()).then(|| {
                req.tools
                    .iter()
                    .map(|t| wire_value(&wire::ToolOut::from_ir(t)))
                    .collect()
            }),
            tool_choice: req.tool_choice.as_ref().map(wire::encode_tool_choice),
            temperature: req.sampling.as_ref().and_then(|s| s.temperature),
            top_p: req.sampling.as_ref().and_then(|s| s.top_p),
            max_tokens: req.sampling.as_ref().and_then(|s| s.max_tokens),
            stream: req.stream,
            // Streaming requests inject stream_options.include_usage: the upstream
            // does not return usage by default, so the billing side would miss that
            // entire block of data (the old implementation's semantics must stay).
            stream_options: (req.stream && !req.extra.contains_key("stream_options"))
                .then(|| json!({"include_usage": true})),
        };
        let mut out = struct_to_map(&out)?;
        if let Some(s) = &req.sampling {
            apply_extra_map(&mut out, &s.extra);
        }
        apply_extra_fields(&mut out, req);
        encode_object(out)
    }

    /// Chat Completions non-streaming response body → IR.
    fn decode_response(&self, body: Bytes) -> Result<LlmResponse, AdaptorError> {
        let v: Value = decode_body(&body)?;
        // Upstreams are hostile input, not validated senders: a non-object body or
        // any missing field decodes to empty IR rather than failing (the long-standing
        // contract of the `.unwrap_or_default()` chains this replaces).
        let resp: wire::Response = serde_json::from_value(v).unwrap_or_default();
        Ok(resp.into_ir())
    }

    /// IR → Chat Completions non-streaming response body.
    fn encode_response(&self, resp: &LlmResponse) -> Result<Bytes, AdaptorError> {
        encode_value(&wire_value(&wire::ResponseOut::from_ir(resp)))
    }

    /// Data payload of one complete SSE frame → IR events (stateless, 1:1 per
    /// event).
    fn decode_event(&self, data: &str) -> Result<Vec<StreamEvent>, AdaptorError> {
        let trimmed = data.trim();
        // `[DONE]` and blank lines are SSE control frames, not content.
        if trimmed.is_empty() || trimmed == "[DONE]" {
            return Ok(Vec::new());
        }
        // Frames that are not valid JSON are control noise: skip, never fail the stream.
        let Ok(chunk) = serde_json::from_str::<wire::Chunk>(trimmed) else {
            return Ok(Vec::new());
        };
        Ok(chunk.into_events())
    }

    fn stream_encoder(&self) -> Box<dyn StreamEncoder> {
        Box::new(OpenAiStreamEncoder::new())
    }
}

// ---------- Streaming encoder ----------

/// Chat Completions stream encoder: holds a chunk id that stays stable for the
/// whole stream and assembles tool fragments into
/// `tool_calls[].function.arguments` pieces.
struct OpenAiStreamEncoder {
    id: String,
    model: String,
    created: i64,
    /// Whether the role frame has already been sent before the first outbound
    /// output.
    role_sent: bool,
    /// Tool indexes whose id/name header frame has been sent.
    tool_started: BTreeSet<u32>,
    finished: bool,
}

impl OpenAiStreamEncoder {
    fn new() -> Self {
        Self {
            id: format!("chatcmpl-{}", uuid::Uuid::new_v4().simple()),
            model: String::new(),
            created: chrono::Utc::now().timestamp(),
            role_sent: false,
            tool_started: BTreeSet::new(),
            finished: false,
        }
    }

    /// Assembles one chunk: id/object/created/model stay consistent for the whole
    /// stream.
    // json! moves `delta` and `finish_reason` into the serialized payload, so the
    // arguments are genuinely consumed — borrowing them would not compile against
    // the opaque macro.
    #[allow(clippy::needless_pass_by_value)]
    fn chunk(&self, delta: Value, finish_reason: Value) -> Value {
        json!({
            "id": self.id,
            "object": "chat.completion.chunk",
            "created": self.created,
            "model": self.model,
            "choices": [{
                "index": 0,
                "delta": delta,
                "finish_reason": finish_reason,
            }],
        })
    }

    /// Role frame: Chat clients expect the first chunk to carry `role`.
    fn role_frame(&self) -> Bytes {
        data_frame(&self.chunk(json!({"role": "assistant", "content": ""}), Value::Null))
    }

    /// First frame of a tool call (with id/name; the caller passes empty strings
    /// when they are missing).
    fn tool_start_frame(&self, index: u32, id: &str, name: &str) -> Bytes {
        data_frame(&self.chunk(
            json!({"tool_calls": [{
                "index": index,
                "id": id,
                "type": "function",
                "function": {"name": name, "arguments": ""},
            }]}),
            Value::Null,
        ))
    }

    /// Tool header frame with arguments only and no id/name (sent when the
    /// upstream gave no start).
    fn tool_orphan_frame(&self, index: u32) -> Bytes {
        data_frame(&self.chunk(
            json!({"tool_calls": [{
                "index": index,
                "type": "function",
                "function": {"arguments": ""},
            }]}),
            Value::Null,
        ))
    }

    /// Tool argument delta frame.
    fn tool_delta_frame(&self, index: u32, arguments_delta: &str) -> Bytes {
        data_frame(&self.chunk(
            json!({"tool_calls": [{
                "index": index,
                "function": {"arguments": arguments_delta},
            }]}),
            Value::Null,
        ))
    }

    /// The usage frame must carry non-empty choices: `choices: []` is rejected by
    /// most clients.
    fn usage_frame(&self, u: &Usage) -> Bytes {
        let mut payload = self.chunk(json!({}), Value::Null);
        payload["usage"] = wire::usage_value(u);
        data_frame(&payload)
    }

    fn done_frame() -> Bytes {
        Bytes::from_static(b"data: [DONE]\n\n")
    }
}

impl StreamEncoder for OpenAiStreamEncoder {
    fn encode_event(&mut self, ev: &StreamEvent) -> Result<Vec<Bytes>, AdaptorError> {
        if let StreamEvent::MessageStart { id, model } = ev {
            // Trust the id/model from upstream; keep the self-generated id when
            // they are absent.
            if !id.is_empty() {
                self.id.clone_from(id);
            }
            self.model.clone_from(model);
            self.role_sent = true;
            return Ok(vec![self.role_frame()]);
        }

        let mut out = Vec::new();
        // Prepend the role frame: Chat clients expect the first chunk to carry
        // role.
        if !self.role_sent {
            self.role_sent = true;
            out.push(self.role_frame());
        }

        match ev {
            StreamEvent::TextDelta { text, .. } => {
                out.push(data_frame(
                    &self.chunk(json!({"content": text}), Value::Null),
                ));
            }
            StreamEvent::ThinkingDelta { text, .. } => {
                out.push(data_frame(
                    &self.chunk(json!({"reasoning_content": text}), Value::Null),
                ));
            }
            StreamEvent::ToolCallStart { index, id, name } => {
                self.tool_started.insert(*index);
                out.push(self.tool_start_frame(*index, id, name));
            }
            StreamEvent::ToolCallDelta {
                index,
                arguments_delta,
            } => {
                // Assembly must work without a start: emit a header frame without
                // id/name, otherwise the client receives a stream of ownerless
                // arguments.
                if self.tool_started.insert(*index) {
                    out.push(self.tool_orphan_frame(*index));
                }
                out.push(self.tool_delta_frame(*index, arguments_delta));
            }
            StreamEvent::MessageDelta { usage: Some(u) } => {
                out.push(self.usage_frame(u));
            }
            StreamEvent::MessageDelta { usage: None }
            | StreamEvent::Raw { .. }
            | StreamEvent::MessageStart { .. } => {}
            StreamEvent::MessageStop { reason } => {
                out.push(data_frame(
                    &self.chunk(json!({}), json!(wire::stop_reason_str(reason))),
                ));
                out.push(Self::done_frame());
                self.finished = true;
            }
            // Raw vendor events have no OpenAI Chat representation; ignore explicitly
            // instead of relying on a catch-all.
            StreamEvent::Error { message } => {
                out.push(data_frame(&json!({
                    "error": {"message": message, "type": "api_error"},
                })));
            }
        }

        Ok(out)
    }

    fn finish(&mut self) -> Result<Vec<Bytes>, AdaptorError> {
        // Emit [DONE] if it has not been sent: clients use it to detect the end of
        // the stream and would hang forever without it.
        if self.finished {
            return Ok(Vec::new());
        }
        self.finished = true;
        Ok(vec![Self::done_frame()])
    }

    /// Marks that upstream already sent [DONE]: called by the caller (the
    /// pipeline-layer frame scanner) when it sees [DONE], so `finish()` does not
    /// send a second one — `OpenAI` clients treat two [DONE]s as two stream
    /// terminations.
    fn mark_done(&mut self) {
        self.finished = true;
    }
}

// ---------- Wire models ----------
// The module body is appended below in sections: request decode, response +
// stream decode, then the encode-side structs.
mod wire {
    use serde::{Deserialize, Serialize};
    use serde_json::{Map, Value, json};
    use std::borrow::Cow;

    use crate::ir::{
        ContentBlock, Lenient, LlmRequest, LlmResponse, Message as IrMessage, Present, Role,
        StopReason, StreamEvent, TextBlock, ToolChoice, ToolDef, Usage, lenient_from_value,
        sampling_params, wire_value,
    };

    // ---- shared Chat vocabulary: finish_reason and tool_choice ----

    /// Wire `finish_reason` → IR. Kept as one pair with [`stop_reason_str`]: the two
    /// directions of one table live in one place.
    pub(super) fn stop_reason(reason: &str) -> StopReason {
        match reason {
            "stop" => StopReason::Stop,
            "length" => StopReason::Length,
            "tool_calls" | "function_call" => StopReason::ToolUse,
            "content_filter" => StopReason::ContentFilter,
            other => StopReason::Other(other.to_string()),
        }
    }

    /// IR → wire `finish_reason`.
    pub(super) fn stop_reason_str(reason: &StopReason) -> Cow<'static, str> {
        match reason {
            StopReason::Stop | StopReason::Error => Cow::Borrowed("stop"),
            StopReason::Length => Cow::Borrowed("length"),
            StopReason::ToolUse => Cow::Borrowed("tool_calls"),
            StopReason::ContentFilter => Cow::Borrowed("content_filter"),
            StopReason::Other(s) => Cow::Owned(s.clone()),
        }
    }

    /// Wire `tool_choice` (any shape) → IR. Fed from the flattened `extra` map so a
    /// literal JSON `null` survives as `Raw(null)` (present-but-null is a distinct
    /// wire fact the old `.get("tool_choice")` chain preserved).
    fn decode_tool_choice(v: Value) -> ToolChoice {
        match v {
            Value::String(s) => match s.as_str() {
                "auto" => ToolChoice::Auto,
                "none" => ToolChoice::None,
                "required" => ToolChoice::Required,
                other => ToolChoice::Raw(json!(other)),
            },
            Value::Object(o) => o
                .get("function")
                .and_then(|f| f.get("name"))
                .and_then(Value::as_str)
                .map(|n| ToolChoice::Specific(n.to_string()))
                .unwrap_or(ToolChoice::Raw(Value::Object(o))),
            other => ToolChoice::Raw(other),
        }
    }

    /// IR → wire `tool_choice`.
    pub(super) fn encode_tool_choice(tc: &ToolChoice) -> Value {
        match tc {
            ToolChoice::Auto => json!("auto"),
            ToolChoice::Required => json!("required"),
            ToolChoice::None => json!("none"),
            ToolChoice::Specific(name) => json!({"type": "function", "function": {"name": name}}),
            ToolChoice::Raw(v) => v.clone(),
        }
    }

    /// `u64` wire index → IR `u32` (the old `.try_into().unwrap_or(0)` contract).
    fn frame_index(raw: u64) -> u32 {
        raw.try_into().unwrap_or(0)
    }

    // ---- request decode ----

    /// Chat Completions request body. One serde field per modeled wire key; everything
    /// unmodeled lands in the flattened `extra` and is carried back verbatim. The
    /// old hand-maintained `KNOWN` key list is exactly the set of declared fields —
    /// it can no longer drift from them.
    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    pub(super) struct Request {
        model: Lenient<String>,
        messages: Lenient<Vec<Lenient<RequestMessage>>>,
        tools: Lenient<Vec<Lenient<Tool>>>,
        temperature: Lenient<Option<f64>>,
        top_p: Lenient<Option<f64>>,
        max_tokens: Lenient<Option<u64>>,
        stream: Lenient<bool>,
        /// Chat-specific control field: consumed on purpose, never modeled into the IR
        /// and never carried across the hop (other formats have no such concept);
        /// the encoder re-injects its own default for streaming requests.
        stream_options: Option<Value>,
        #[serde(flatten)]
        extra: Map<String, Value>,
    }

    impl Request {
        pub(super) fn into_ir(self) -> LlmRequest {
            let mut messages = Vec::new();
            let mut system = Vec::new();
            for m in self.messages.0 {
                let m = m.0;
                // system/developer messages become standalone IR system blocks: vendors
                // disagree where the prompt goes, and leaving them in messages would
                // force every downstream codec to pick them out again.
                if m.role.0 == "system" || m.role.0 == "developer" {
                    system.extend(m.system_blocks());
                    continue;
                }
                messages.push(m.into_ir());
            }

            let mut extra = self.extra;
            // tool_choice and stop are modeled (IR tool_choice / sampling.extra), so
            // they leave the unmodeled `extra` here — the decode_extra exclusion.
            let tool_choice = extra.remove("tool_choice").map(decode_tool_choice);
            let stop_extra = match extra.remove("stop") {
                Some(stop) => {
                    let mut m = Map::new();
                    m.insert("stop".into(), stop);
                    m
                }
                None => Map::new(),
            };

            LlmRequest {
                model: self.model.0,
                messages,
                system,
                tools: self
                    .tools
                    .0
                    .into_iter()
                    .filter_map(|t| t.0.into_def())
                    .collect(),
                tool_choice,
                sampling: sampling_params(
                    self.temperature.0,
                    self.top_p.0,
                    self.max_tokens.0,
                    stop_extra,
                ),
                stream: self.stream.0,
                extra,
            }
        }
    }

    /// A Chat message: content (string or part array), optional `tool_calls`, and the
    /// top-level `tool_call_id` that links a `role=tool` message to its call.
    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    pub(super) struct RequestMessage {
        role: Lenient<String>,
        content: Lenient<Option<ContentWire>>,
        tool_calls: Lenient<Vec<Lenient<ToolCall>>>,
        tool_call_id: Lenient<Option<String>>,
    }

    impl RequestMessage {
        fn into_ir(self) -> IrMessage {
            let role = match self.role.0.as_str() {
                "assistant" => Role::Assistant,
                "tool" => Role::Tool,
                _ => Role::User,
            };
            let mut content = Self::wire_blocks(self.content, self.tool_calls);
            let tool_call_id = self.tool_call_id.0;
            if role == Role::Tool
                && let Some(id) = tool_call_id
            {
                let text = content
                    .iter()
                    .filter_map(|b| match b {
                        ContentBlock::Text { text } => Some(text.clone()),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                content = vec![ContentBlock::ToolResult {
                    tool_use_id: id,
                    content: text,
                }];
            }
            IrMessage { role, content }
        }

        /// Chat `content`/`tool_calls` → IR blocks.
        fn wire_blocks(
            content: Lenient<Option<ContentWire>>,
            tool_calls: Lenient<Vec<Lenient<ToolCall>>>,
        ) -> Vec<ContentBlock> {
            let mut blocks = Vec::new();
            match content.0 {
                Some(ContentWire::Text(text)) if !text.is_empty() => {
                    blocks.push(ContentBlock::Text { text });
                }
                Some(ContentWire::Parts(parts)) => {
                    blocks.extend(parts.into_iter().filter_map(Part::into_block));
                }
                // empty string / null / numbers / objects: not content the IR models
                // (old contract).
                _ => {}
            }
            blocks.extend(tool_calls.0.into_iter().map(|c| c.0.into_block()));
            blocks
        }

        /// The system-prompt slot: a non-empty string or parts carrying `text`.
        fn system_blocks(self) -> Vec<TextBlock> {
            match self.content.0 {
                Some(ContentWire::Text(text)) if !text.is_empty() => vec![TextBlock { text }],
                Some(ContentWire::Parts(parts)) => parts
                    .into_iter()
                    .filter_map(|p| p.text.map(|text| TextBlock { text }))
                    .collect(),
                Some(ContentWire::Text(_)) | None => Vec::new(),
            }
        }
    }

    /// Chat `content`: a plain string or an array of typed parts. Anything else
    /// (null, numbers, plain objects) fails both arms and degrades to `None` through
    /// the enclosing `Lenient` — the old "not content the IR models" contract.
    #[derive(Debug, Deserialize)]
    #[serde(untagged)]
    pub(super) enum ContentWire {
        Text(String),
        Parts(Vec<Part>),
    }

    /// One content part. Keeps its original JSON so the escape hatch
    /// (`ContentBlock::Unknown`) preserves vendor shapes byte-faithfully; the custom
    /// `Deserialize` never fails, so a garbage item degrades to an `Unknown` carrying
    /// the garbage rather than dropping the whole content.
    #[derive(Debug)]
    pub(super) struct Part {
        kind: Option<String>,
        text: Option<String>,
        image_url: Option<ImageUrl>,
        raw: Value,
    }

    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct PartFields {
        #[serde(rename = "type")]
        kind: Lenient<Option<String>>,
        text: Lenient<Option<String>>,
        image_url: Lenient<Option<ImageUrl>>,
    }

    impl<'de> Deserialize<'de> for Part {
        fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
            let raw = Value::deserialize(d)?;
            let fields = serde_json::from_value::<PartFields>(raw.clone()).unwrap_or_default();
            Ok(Self {
                kind: fields.kind.0,
                text: fields.text.0,
                image_url: fields.image_url.0,
                raw,
            })
        }
    }

    impl Part {
        fn into_block(self) -> Option<ContentBlock> {
            match self.kind.as_deref() {
                Some("text") => self.text.map(|text| ContentBlock::Text { text }),
                Some("image_url") => Some(self.into_image()),
                // Unknown blocks are preserved verbatim, so new shapes survive the hop.
                _ => Some(ContentBlock::Unknown { raw: self.raw }),
            }
        }

        /// `image_url` part → IR image block; remote addresses that are not data URLs
        /// cannot be expressed in the base64 shape, so they take the escape hatch.
        fn into_image(self) -> ContentBlock {
            let url = self.image_url.map(|i| i.url.0).unwrap_or_default();
            // A data URL looks like `data:image/png;base64,<payload>`.
            if let Some(rest) = url.strip_prefix("data:")
                && let Some((meta, data)) = rest.split_once(',')
            {
                let media_type = meta.split(';').next().unwrap_or("image/png").to_string();
                return ContentBlock::Image {
                    media_type,
                    data: data.to_string(),
                };
            }
            ContentBlock::Unknown { raw: self.raw }
        }
    }

    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct ImageUrl {
        url: Lenient<String>,
    }

    /// `tools[]` entry: accepts both `{type:function, function:{...}}` and the flat
    /// `{name,...}` shape.
    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct Tool {
        /// `Present` not `Option`: when the key EXISTS but is garbage/null, the old
        /// `t.get("function").unwrap_or(t)` used that garbage and dropped the entry — it
        /// never fell back to the flat shape. Only an absent key selects the flat branch.
        function: Present,
        name: Lenient<Option<String>>,
        description: Lenient<Option<String>>,
        parameters: Present,
    }

    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct Function {
        name: Lenient<Option<String>>,
        description: Lenient<Option<String>>,
        /// Absent means `{}`; an explicit `null` stays `null` (old `get().cloned()`
        /// distinction).
        parameters: Present,
    }

    impl Tool {
        /// `None` = entry without a usable `name` (the old `filter_map` skip).
        fn into_def(self) -> Option<ToolDef> {
            let (name, description, parameters) = match self.function.0 {
                Some(value) => {
                    let f: Function = lenient_from_value(value);
                    (f.name.0, f.description.0, f.parameters)
                }
                None => (self.name.0, self.description.0, self.parameters),
            };
            Some(ToolDef {
                name: name?,
                description,
                input_schema: parameters.0.unwrap_or_else(|| json!({})),
                extra: Map::new(),
            })
        }
    }

    /// A `tool_calls[]` entry in a message (request and response share the shape).
    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct ToolCall {
        id: Lenient<String>,
        function: Lenient<CallFunction>,
    }

    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct CallFunction {
        name: Lenient<String>,
        arguments: Lenient<String>,
    }

    impl ToolCall {
        fn into_block(self) -> ContentBlock {
            ContentBlock::ToolUse {
                id: self.id.0,
                name: self.function.0.name.0,
                // arguments is a JSON string; a parse failure must not fail the whole
                // request (models occasionally emit half of it).
                input: serde_json::from_str::<Value>(&self.function.0.arguments.0)
                    .unwrap_or_else(|_| json!({})),
            }
        }
    }

    // SECTION_MARKER_2

    // ---- response + stream decode ----

    /// Chat Completions non-streaming response body.
    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    pub(super) struct Response {
        id: Lenient<String>,
        model: Lenient<String>,
        choices: Lenient<Vec<Lenient<Choice>>>,
        usage: Lenient<UsageIn>,
    }

    impl Response {
        pub(super) fn into_ir(self) -> LlmResponse {
            // Only the first choice is modeled (the old `choices[0]` contract).
            let choice = self.choices.0.into_iter().next().unwrap_or_default().0;
            LlmResponse {
                id: self.id.0,
                model: self.model.0,
                outputs: RequestMessage::wire_blocks(
                    choice.message.0.content,
                    choice.message.0.tool_calls,
                ),
                usage: self.usage.0.into_usage(),
                stop_reason: choice
                    .finish_reason
                    .0
                    .map_or(StopReason::Stop, |r| stop_reason(&r)),
            }
        }
    }

    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct Choice {
        // `index` is deliberately not modeled: the codec honors the single-choice
        // (choices[0]) contract and serde ignores unmodeled keys.
        message: Lenient<RequestMessage>,
        finish_reason: Lenient<Option<String>>,
    }

    /// The Chat `usage` object. OpenAI-compatible names; the IR normalizes them.
    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct UsageIn {
        prompt_tokens: Lenient<u64>,
        completion_tokens: Lenient<u64>,
        prompt_tokens_details: Lenient<DetailsIn>,
        completion_tokens_details: Lenient<CompletionDetailsIn>,
    }

    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct DetailsIn {
        cached_tokens: Lenient<Option<u64>>,
        /// The OpenAI-compatible name for prompt tokens written to the cache
        /// (mirrors Claude's `cache_creation_input_tokens`).
        cache_write_tokens: Lenient<Option<u64>>,
    }

    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct CompletionDetailsIn {
        reasoning_tokens: Lenient<Option<u64>>,
    }

    impl UsageIn {
        fn into_usage(self) -> Usage {
            Usage {
                prompt_tokens: self.prompt_tokens.0,
                completion_tokens: self.completion_tokens.0,
                cached_tokens: self.prompt_tokens_details.0.cached_tokens.0,
                cache_creation_tokens: self.prompt_tokens_details.0.cache_write_tokens.0,
                reasoning_tokens: self.completion_tokens_details.0.reasoning_tokens.0,
            }
        }
    }

    /// One streaming chunk's payload: only the fields `decode_event` reads.
    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    pub(super) struct Chunk {
        choices: Lenient<Vec<Lenient<ChunkChoice>>>,
        /// `Present` not `Option`: a present-but-null `usage` means "no usage in this
        /// frame" (no event), while PRESENT-BUT-GARBAGE still produces a zero-usage event
        /// (the old `get("usage").filter(|u| !u.is_null())` then `decode_usage` chain).
        usage: Present,
    }

    impl Chunk {
        /// A chunk → IR events: `choices[0]` delta + finish reason, and the usage
        /// event from the same frame. Both must be emitted — taking only choices
        /// would make billing lose the entire streaming usage.
        pub(super) fn into_events(self) -> Vec<StreamEvent> {
            let mut out = self
                .choices
                .0
                .into_iter()
                .next()
                .map(|c| c.0.into_events())
                .unwrap_or_default();
            if let Some(usage) = self.usage.0.filter(|u| !u.is_null()) {
                out.push(StreamEvent::MessageDelta {
                    usage: Some(lenient_from_value::<UsageIn>(usage).into_usage()),
                });
            }
            out
        }
    }

    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct ChunkChoice {
        index: Lenient<u64>,
        delta: Lenient<Delta>,
        finish_reason: Lenient<Option<String>>,
    }

    impl ChunkChoice {
        fn into_events(self) -> Vec<StreamEvent> {
            let index = frame_index(self.index.0);
            let delta = self.delta.0;
            let content = delta.content.0;
            let reasoning_content = delta.reasoning_content.0;
            let mut out = Vec::new();
            if !content.is_empty() {
                out.push(StreamEvent::TextDelta {
                    index,
                    text: content,
                });
            }
            if !reasoning_content.is_empty() {
                out.push(StreamEvent::ThinkingDelta {
                    index,
                    text: reasoning_content,
                });
            }
            for call in delta.tool_calls.0 {
                let call = call.0;
                // The index carried by a tool_calls entry decides which tool call it
                // belongs to; hard-coding would merge parallel calls into one (the
                // counterexample recorded in G1).
                let call_index = frame_index(call.index.0);
                let id = call.id.0;
                let name = call.function.0.name.0;
                let arguments = call.function.0.arguments.0;

                // The first chunk carries id/name (OpenAI puts them only in the first
                // chunk).
                if !id.is_empty() || !name.is_empty() {
                    out.push(StreamEvent::ToolCallStart {
                        index: call_index,
                        id,
                        name,
                    });
                }
                if !arguments.is_empty() {
                    out.push(StreamEvent::ToolCallDelta {
                        index: call_index,
                        arguments_delta: arguments,
                    });
                }
            }
            if let Some(reason) = self.finish_reason.0 {
                out.push(StreamEvent::MessageStop {
                    reason: stop_reason(&reason),
                });
            }
            out
        }
    }

    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct Delta {
        content: Lenient<String>,
        reasoning_content: Lenient<String>,
        tool_calls: Lenient<Vec<Lenient<DeltaCall>>>,
    }

    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct DeltaCall {
        index: Lenient<u64>,
        id: Lenient<String>,
        function: Lenient<DeltaFunction>,
    }

    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct DeltaFunction {
        name: Lenient<String>,
        arguments: Lenient<String>,
    }

    // ---- encode models ----

    /// IR → Chat request body. `skip_serializing_if` reproduces the old "insert only
    /// when present" behavior; unmodeled fields are merged into the emitted map
    /// afterwards and only fill missing keys, so the modeled fields above always win.
    #[derive(Debug, Serialize)]
    pub(super) struct RequestOut<'a> {
        pub(super) model: &'a str,
        pub(super) messages: Vec<MessageOut>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(super) tools: Option<Vec<Value>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(super) tool_choice: Option<Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(super) temperature: Option<f64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(super) top_p: Option<f64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(super) max_tokens: Option<u64>,
        pub(super) stream: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(super) stream_options: Option<Value>,
    }

    /// IR → Chat message.
    #[derive(Debug, Serialize)]
    pub(super) struct MessageOut {
        pub(super) role: &'static str,
        pub(super) content: Value,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(super) tool_calls: Option<Vec<Value>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(super) tool_call_id: Option<String>,
    }

    impl MessageOut {
        pub(super) fn from_ir(m: &IrMessage) -> Self {
            // A tool result is a standalone role + tool_call_id in Chat, not a content
            // block.
            if let Some(ContentBlock::ToolResult {
                tool_use_id,
                content,
            }) = m
                .content
                .iter()
                .find(|b| matches!(b, ContentBlock::ToolResult { .. }))
            {
                return Self {
                    role: "tool",
                    content: json!(content),
                    tool_calls: None,
                    tool_call_id: Some(tool_use_id.clone()),
                };
            }

            let role = match m.role {
                Role::Assistant => "assistant",
                Role::Tool => "tool",
                Role::User => "user",
            };
            let (text, tool_calls) = split_outputs(&m.content);
            let mut out = Self {
                role,
                // Send a string content only when there is text; otherwise send null
                // (OpenAI's existing shape).
                content: if tool_calls.is_empty() || !text.is_empty() {
                    json!(text)
                } else {
                    Value::Null
                },
                tool_calls: (!tool_calls.is_empty()).then_some(tool_calls),
                tool_call_id: None,
            };

            // Non-text blocks such as images: convert to Chat's content-array shape.
            let images: Vec<Value> = m
                .content
                .iter()
                .filter_map(|b| match b {
                    ContentBlock::Image { media_type, data } => Some(json!({
                        "type": "image_url",
                        "image_url": {"url": format!("data:{media_type};base64,{data}")},
                    })),
                    _ => None,
                })
                .collect();
            if !images.is_empty() {
                let mut parts = vec![json!({"type": "text", "text": text})];
                parts.extend(images);
                out.content = Value::Array(parts);
            }
            out
        }
    }

    /// Split IR blocks into (text, `tool_calls` JSON).
    fn split_outputs(blocks: &[ContentBlock]) -> (String, Vec<Value>) {
        let mut text = String::new();
        let mut calls = Vec::new();
        // Two accumulators with order-sensitive updates stay a loop (the sanctioned
        // escape): one chain cannot fill both.
        for b in blocks {
            match b {
                ContentBlock::Text { text: t } => text.push_str(t),
                ContentBlock::ToolUse { id, name, input } => calls.push(json!({
                    "id": id,
                    "type": "function",
                    "function": {
                        "name": name,
                        "arguments": serde_json::to_string(input).unwrap_or_else(|_| "{}".into()),
                    },
                })),
                _ => {}
            }
        }
        (text, calls)
    }

    /// IR tool definition → Chat `tools[]` entry (always the function wrapper).
    #[derive(Debug, Serialize)]
    pub(super) struct ToolOut<'a> {
        #[serde(rename = "type")]
        kind: &'static str,
        function: FunctionOut<'a>,
    }

    #[derive(Debug, Serialize)]
    struct FunctionOut<'a> {
        name: &'a str,
        parameters: &'a Value,
        #[serde(skip_serializing_if = "Option::is_none")]
        description: Option<&'a str>,
    }

    impl<'a> ToolOut<'a> {
        pub(super) fn from_ir(t: &'a ToolDef) -> Self {
            Self {
                kind: "function",
                function: FunctionOut {
                    name: &t.name,
                    parameters: &t.input_schema,
                    description: t.description.as_deref(),
                },
            }
        }
    }

    /// IR → Chat non-streaming response body.
    #[derive(Debug, Serialize)]
    pub(super) struct ResponseOut {
        id: String,
        object: &'static str,
        created: i64,
        model: String,
        choices: Vec<ChoiceOut>,
        usage: UsageOut,
    }

    #[derive(Debug, Serialize)]
    struct ChoiceOut {
        index: u32,
        message: MessageOut,
        finish_reason: Cow<'static, str>,
    }

    impl ResponseOut {
        pub(super) fn from_ir(resp: &LlmResponse) -> Self {
            let (text, tool_calls) = split_outputs(&resp.outputs);
            // content always carries text (a string even without tool_calls); an
            // array is attached as well when tool_calls are present. The null-content
            // and image cases of `MessageOut::from_ir` are request-side only (the old
            // response shape).
            let message = MessageOut {
                role: "assistant",
                content: json!(text),
                tool_calls: (!tool_calls.is_empty()).then_some(tool_calls),
                tool_call_id: None,
            };
            Self {
                id: resp.id.clone(),
                object: "chat.completion",
                created: chrono::Utc::now().timestamp(),
                model: resp.model.clone(),
                choices: vec![ChoiceOut {
                    index: 0,
                    message,
                    finish_reason: stop_reason_str(&resp.stop_reason),
                }],
                usage: UsageOut::from_ir(&resp.usage),
            }
        }
    }

    /// IR usage → `OpenAI` Chat `usage` object.
    ///
    /// `prompt_tokens_details.cached_tokens` is always emitted (0 when the upstream
    /// reported nothing): the real `OpenAI` wire format always includes the details
    /// object, so an OpenAI-shaped client sees the same shape whether or not cache
    /// information was available. Only the additive `cache_write_tokens` /
    /// `completion_tokens_details.reasoning_tokens` are conditional.
    #[derive(Debug, Serialize)]
    struct UsageOut {
        prompt_tokens: u64,
        completion_tokens: u64,
        total_tokens: u64,
        prompt_tokens_details: DetailsOut,
        #[serde(skip_serializing_if = "Option::is_none")]
        completion_tokens_details: Option<CompletionDetailsOut>,
    }

    #[derive(Debug, Serialize)]
    struct DetailsOut {
        cached_tokens: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        cache_write_tokens: Option<u64>,
    }

    #[derive(Debug, Serialize)]
    struct CompletionDetailsOut {
        reasoning_tokens: u64,
    }

    impl UsageOut {
        fn from_ir(u: &Usage) -> Self {
            Self {
                prompt_tokens: u.prompt_tokens,
                completion_tokens: u.completion_tokens,
                total_tokens: u.prompt_tokens + u.completion_tokens,
                prompt_tokens_details: DetailsOut {
                    cached_tokens: u.cached_tokens.unwrap_or(0),
                    cache_write_tokens: u.cache_creation_tokens,
                },
                completion_tokens_details: u
                    .reasoning_tokens
                    .map(|reasoning_tokens| CompletionDetailsOut { reasoning_tokens }),
            }
        }
    }

    /// The `usage` JSON object for the streaming usage frame.
    pub(super) fn usage_value(u: &Usage) -> Value {
        wire_value(&UsageOut::from_ir(u))
    }
}
