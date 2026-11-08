//! Gemini format codec — IR ↔ `generateContent` (WP4).
//!
//! ## Shape differences from OpenAI/Claude
//!
//! - Messages are called `contents`, and roles are only `user` / `model` (no
//!   system / assistant / tool).
//! - The system prompt is standalone in `systemInstruction` and does not occupy a
//!   contents slot.
//! - Generation parameters all live in `generationConfig` (`maxOutputTokens` /
//!   `topP` / `stopSequences`).
//! - Tool declarations use `functionDeclarations`, whose schema field is called
//!   `parameters` (not Chat's `parameters` wrapped inside function, and not IR's
//!   `input_schema`).
//! - A tool call is a **whole `functionCall` object** with no id; streaming does
//!   not slice arguments the way OpenAI does, but either sends the whole object or
//!   repeats the accumulated object.
//!
//! ## Wire models
//!
//! Every Gemini field name is declared exactly once, as a serde field of a DTO in
//! [`wire`]; the codec body is decode → [`wire::Request::into_ir`] and IR → wire
//! struct → bytes. Wire DTOs decode leniently (nested [`ir::Lenient`] fields degrade
//! type mismatches to defaults — the same contract the old `.and_then(Value::as_)
//! .unwrap_or_default()` chains had), and key-presence distinctions the old code
//! made through `Value::get` ("key absent" vs "explicit null", e.g. a part's `text`
//! key) go through the shared [`ir::Present`] wrapper. Unmodeled top-level fields land
//! in the flattened `extra` map, so the hand-maintained "known keys" list is gone.
//!
//! ## Three pre-existing defects this module fixes (G1)
//!
//! The old implementation `adaptor::convert_gemini_event_to_openai` had three rough
//! spots:
//!
//! 1. **`finish_reason` was passed through verbatim** — Gemini sends uppercase
//!    enums such as `STOP` / `MAX_TOKENS` / `SAFETY`, and stuffing them directly
//!    into the OpenAI-shaped `finish_reason` leaves clients unable to recognize
//!    them. They are now mapped via [`gemini_finish_reason`].
//! 2. **Each chunk generated a fresh uuid for its id** — multiple chunks of the
//!    same completion each carried a different id, so clients and streaming
//!    billing aggregation treated them as several responses. [`GeminiStreamEncoder`]
//!    now reuses one id for the whole stream.
//! 3. **The usage block's `choices` was an empty array** — most clients reject that
//!    shape. The usage frame now carries non-empty choices.
//!
//! [`ir::Lenient`]: crate::ir

use bytes::Bytes;
use serde_json::{Value, json};
use std::collections::BTreeMap;

use crate::adaptor::{AdaptorError, Protocol};
use crate::format_codec::{FormatCodec, StreamEncoder};
use crate::ir::{
    LlmRequest, LlmResponse, StopReason, StreamEvent, Usage, apply_extra_fields, decode_body,
    encode_object, encode_value, struct_to_map, wire_value,
};
use crate::sse::data_frame;

/// Gemini format codec.
#[derive(Debug, Default, Clone, Copy)]
pub struct GeminiCodec;

impl GeminiCodec {
    /// Creates a codec (stateless, safe to share).
    pub const fn new() -> Self {
        Self
    }
}

impl FormatCodec for GeminiCodec {
    fn format(&self) -> Protocol {
        Protocol::Gemini
    }

    /// Gemini request body → IR.
    fn decode_request(&self, body: Bytes) -> Result<LlmRequest, AdaptorError> {
        let v: Value = decode_body(&body)?;
        if !v.is_object() {
            return Err(AdaptorError::DecodeFailed(
                "gemini request body is not an object".into(),
            ));
        }
        // Every wire field is lenient, so an object-shaped body never fails conversion.
        let req: wire::Request = serde_json::from_value(v).unwrap_or_default();
        Ok(req.into_ir())
    }

    /// IR → Gemini request body.
    fn encode_request(&self, req: &LlmRequest) -> Result<Bytes, AdaptorError> {
        let mut out = struct_to_map(&wire::RequestOut::from_ir(req))?;
        // Unmodeled client fields fill only missing keys. Sampling extras other than
        // `stop` have no Gemini wire slot (the old encoder dropped them too), so
        // `sampling.extra` is deliberately not replayed here — unlike OpenAI, where
        // extras map onto top-level keys.
        apply_extra_fields(&mut out, req);
        encode_object(out)
    }

    /// Gemini non-streaming response body → IR.
    fn decode_response(&self, body: Bytes) -> Result<LlmResponse, AdaptorError> {
        let v: Value = decode_body(&body)?;
        // Upstreams are hostile input, not validated senders: a non-object body or
        // any missing field decodes to empty IR rather than failing (the long-standing
        // contract of the `.unwrap_or_default()` chains this replaces).
        let resp: wire::Response = serde_json::from_value(v).unwrap_or_default();
        Ok(resp.into_ir())
    }

    /// IR → Gemini non-streaming response body.
    fn encode_response(&self, resp: &LlmResponse) -> Result<Bytes, AdaptorError> {
        encode_value(&wire_value(&wire::ResponseOut::from_ir(resp)))
    }

    /// Data payload of one complete SSE frame → IR events (stateless).
    fn decode_event(&self, data: &str) -> Result<Vec<StreamEvent>, AdaptorError> {
        let trimmed = data.trim();
        // `[DONE]` and blank frames are SSE control frames, not content.
        if trimmed.is_empty() || trimmed == "[DONE]" {
            return Ok(Vec::new());
        }
        // Frames that do not parse as a chunk object are control noise: skip, never
        // fail the stream (valid JSON of the wrong shape decodes to an empty chunk).
        let Ok(chunk) = serde_json::from_str::<wire::Chunk>(trimmed) else {
            return Ok(Vec::new());
        };
        Ok(chunk.into_events())
    }

    fn stream_encoder(&self) -> Box<dyn StreamEncoder> {
        Box::new(GeminiStreamEncoder::new())
    }
}

/// Gemini finishReason → IR StopReason.
///
/// The old implementation passed the uppercase Gemini enum through into the OpenAI
/// shape verbatim, which clients could not recognize; this table is the
/// authoritative mapping (compare the implementation in
/// `todo/ref/ferryllm/src/adapters/gemini.rs`).
fn gemini_finish_reason(reason: &str) -> StopReason {
    match reason {
        // STOP is a normal end; unlisted values fall through to Other below.
        "STOP" => StopReason::Stop,
        "MAX_TOKENS" => StopReason::Length,
        "MALFORMED_FUNCTION_CALL" => StopReason::ToolUse,
        "SAFETY" | "RECITATION" | "LANGUAGE" | "BLOCKLIST" | "PROHIBITED_CONTENT" | "SPII" => {
            StopReason::ContentFilter
        }
        other => StopReason::Other(other.to_string()),
    }
}

/// IR StopReason → Gemini finishReason (inverse of the table above).
fn gemini_finish_reason_str(reason: &StopReason) -> String {
    match reason {
        StopReason::Stop => "STOP".into(),
        StopReason::Length => "MAX_TOKENS".into(),
        StopReason::ToolUse => "MALFORMED_FUNCTION_CALL".into(),
        StopReason::ContentFilter => "SAFETY".into(),
        StopReason::Error => "OTHER".into(),
        StopReason::Other(s) => s.clone(),
    }
}

/// IR usage → Gemini `usageMetadata` object.
///
/// `candidatesTokenCount` excludes thinking (the IR completion includes it), so
/// it is the total minus `thoughtsTokenCount`; `totalTokenCount` stays
/// prompt + completion, which equals prompt + candidates + thoughts on Gemini's
/// own accounting.
fn encode_usage_metadata(u: &Usage) -> Value {
    let reasoning = u.reasoning_tokens.unwrap_or(0);
    wire_value(&wire::UsageMetadataOut {
        prompt_token_count: u.prompt_tokens,
        candidates_token_count: u.completion_tokens.saturating_sub(reasoning),
        total_token_count: u.prompt_tokens + u.completion_tokens,
        cached_content_token_count: u.cached_tokens.unwrap_or(0),
        thoughts_token_count: u.reasoning_tokens,
    })
}

// ---------- Streaming encoder ----------

/// Gemini stream encoder: reuses one chunk id for the whole stream and accumulates
/// functionCall arguments.
///
/// Gemini's `functionCall.args` is a **complete object** (not an incremental
/// fragment), so the IR `ToolCallDelta` fragments are accumulated here and a
/// complete functionCall part is emitted at the end. That accumulation state is the
/// second reason the encoding side must be stateful (the first is id reuse).
struct GeminiStreamEncoder {
    id: String,
    model: String,
    /// Tool calls accumulated per index: name and the assembled argument string.
    tools: BTreeMap<u32, (String, String)>,
    /// Whether the closing frame has already been emitted.
    finished: bool,
}

impl GeminiStreamEncoder {
    fn new() -> Self {
        Self {
            // One-time stream id: reused for the whole stream (the old
            // implementation generated a fresh uuid per chunk — the counterexample
            // recorded in G1).
            id: format!("chatcmpl-{}", uuid::Uuid::new_v4().simple()),
            model: String::new(),
            tools: BTreeMap::new(),
            finished: false,
        }
    }

    /// Shape of a Gemini streaming frame: candidates[0].content.parts.
    fn candidates_frame(&self, parts: Vec<Value>, finish_reason: Value) -> Value {
        json!({
            "id": self.id,
            "model": self.model,
            "candidates": [{
                "content": {"role": "model", "parts": parts},
                "index": 0,
                "finishReason": finish_reason,
            }],
        })
    }
}

impl StreamEncoder for GeminiStreamEncoder {
    fn encode_event(&mut self, ev: &StreamEvent) -> Result<Vec<Bytes>, AdaptorError> {
        match ev {
            StreamEvent::MessageStart { id, model } => {
                if !id.is_empty() {
                    self.id = id.clone();
                }
                self.model = model.clone();
                Ok(Vec::new())
            }
            StreamEvent::TextDelta { text, .. } => Ok(vec![data_frame(
                &self.candidates_frame(vec![json!({"text": text})], Value::Null),
            )]),
            StreamEvent::ThinkingDelta { text, .. } => {
                // Gemini's thinking content is also carried in a text part (the
                // thought flag is optional).
                Ok(vec![data_frame(&self.candidates_frame(
                    vec![json!({"text": text, "thought": true})],
                    Value::Null,
                ))])
            }
            StreamEvent::ToolCallStart { index, name, .. } => {
                self.tools
                    .entry(*index)
                    .or_insert_with(|| (name.clone(), String::new()));
                // Arguments have not arrived yet, so emit no frame (Gemini's
                // functionCall must carry complete args).
                Ok(Vec::new())
            }
            StreamEvent::ToolCallDelta {
                index,
                arguments_delta,
            } => {
                let entry = self.tools.entry(*index).or_default();
                entry.1.push_str(arguments_delta);
                Ok(Vec::new())
            }
            StreamEvent::MessageDelta { usage: Some(u) } => {
                // The usage frame's choices must not be empty (the old
                // implementation's `choices: []` is a recorded counterexample);
                // Gemini's shape uses candidates, so this adds the usage slot
                // alongside an empty candidates entry.
                let mut payload = self.candidates_frame(Vec::new(), Value::Null);
                payload["usageMetadata"] = encode_usage_metadata(u);
                Ok(vec![data_frame(&payload)])
            }
            StreamEvent::MessageDelta { usage: None } => Ok(Vec::new()),
            StreamEvent::MessageStop { reason } => {
                let mut out = Vec::new();
                // Emit the accumulated tool calls at once.
                let parts: Vec<Value> = self
                    .tools
                    .iter()
                    .map(|(_, (name, args))| {
                        let parsed: Value =
                            serde_json::from_str(args).unwrap_or_else(|_| json!({}));
                        json!({"functionCall": {"name": name, "args": parsed}})
                    })
                    .collect();
                if !parts.is_empty() {
                    out.push(data_frame(&self.candidates_frame(parts, Value::Null)));
                }
                out.push(data_frame(&self.candidates_frame(
                    Vec::new(),
                    json!(gemini_finish_reason_str(reason)),
                )));
                self.finished = true;
                Ok(out)
            }
            // Raw vendor events have no Gemini representation; ignore explicitly.
            StreamEvent::Raw { .. } => Ok(Vec::new()),
            StreamEvent::Error { message } => Ok(vec![data_frame(&json!({
                "error": {"code": 500, "message": message, "status": "INTERNAL"},
            }))]),
        }
    }

    fn finish(&mut self) -> Result<Vec<Bytes>, AdaptorError> {
        if self.finished {
            return Ok(Vec::new());
        }
        self.finished = true;
        // Stream ended without MessageStop: add a STOP closing frame, otherwise
        // the client hangs forever.
        Ok(vec![data_frame(
            &self.candidates_frame(Vec::new(), json!("STOP")),
        )])
    }
}

// ---------- Wire models ----------
// The module body is appended below in sections: shared vocabulary (presence
// wrapper), request decode, response + stream decode, then the encode-side structs.

mod wire {
    use serde::{Deserialize, Deserializer, Serialize};
    use serde_json::{Map, Value, json};

    use super::{encode_usage_metadata, gemini_finish_reason, gemini_finish_reason_str};
    use crate::ir::{
        ContentBlock, Lenient, LlmRequest, LlmResponse, Message as IrMessage, Present, Role,
        SamplingParams, StopReason, StreamEvent, TextBlock, ToolDef, Usage, lenient_from_value,
        sampling_params,
    };

    // ---- request decode ----

    /// Gemini request body. One serde field per modeled wire key; everything
    /// unmodeled lands in the flattened `extra` and is carried back verbatim. The
    /// old hand-maintained `matches!` key list is exactly this field set — it can
    /// no longer drift from it.
    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    pub(super) struct Request {
        contents: Lenient<Vec<Lenient<ContentIn>>>,
        #[serde(rename = "systemInstruction")]
        system_instruction: Lenient<SystemInstructionIn>,
        tools: Lenient<Vec<Lenient<ToolGroupIn>>>,
        #[serde(rename = "generationConfig")]
        generation_config: Lenient<GenerationConfigIn>,
        stream: Lenient<bool>,
        #[serde(flatten)]
        extra: Map<String, Value>,
    }

    impl Request {
        pub(super) fn into_ir(self) -> LlmRequest {
            LlmRequest {
                model: String::new(), // Gemini's model id lives in the URL path, not the body.
                messages: self.contents.0.into_iter().map(|c| c.0.into_ir()).collect(),
                system: self.system_instruction.0.into_blocks(),
                // Multiple `tools` groups are flattened into one IR tool list.
                tools: self
                    .tools
                    .0
                    .into_iter()
                    .flat_map(|g| g.0.into_defs())
                    .collect(),
                tool_choice: None,
                sampling: self.generation_config.0.into_sampling(),
                stream: self.stream.0,
                extra: self.extra,
            }
        }
    }

    /// One `contents[]` entry (request contents and response candidate content
    /// share the shape). The role is only `user` / `model`; anything else is user
    /// (the old `_ => User` catch-all).
    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct ContentIn {
        role: Lenient<String>,
        parts: Lenient<Vec<Part>>,
    }

    impl ContentIn {
        fn into_ir(self) -> IrMessage {
            let role = if self.role.0 == "model" {
                Role::Assistant
            } else {
                Role::User
            };
            IrMessage {
                role,
                content: self.parts.0.into_iter().map(Part::into_block).collect(),
            }
        }
    }

    /// `systemInstruction`: a standalone object with a `parts` array (a non-object
    /// or non-array means no system prompt).
    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct SystemInstructionIn {
        parts: Lenient<Vec<Lenient<SystemPartIn>>>,
    }

    impl SystemInstructionIn {
        /// Parts carrying a string `text` become system blocks; the rest are
        /// skipped, not fatal (the old `filter_map` contract).
        fn into_blocks(self) -> Vec<TextBlock> {
            self.parts
                .0
                .into_iter()
                .filter_map(|p| p.0.text.0)
                .map(|text| TextBlock { text })
                .collect()
        }
    }

    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct SystemPartIn {
        text: Lenient<Option<String>>,
    }

    /// One `tools[]` group; only the `functionDeclarations` list is modeled.
    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct ToolGroupIn {
        #[serde(rename = "functionDeclarations")]
        function_declarations: Lenient<Vec<Lenient<FunctionDeclIn>>>,
    }

    impl ToolGroupIn {
        fn into_defs(self) -> Vec<ToolDef> {
            self.function_declarations
                .0
                .into_iter()
                .filter_map(|d| {
                    let d = d.0;
                    // An entry without a usable `name` is skipped (the old `continue`).
                    let name = d.name.0?;
                    Some(ToolDef {
                        name,
                        description: d.description.0,
                        // Gemini uses `parameters`; the IR side uniformly calls it
                        // input_schema.
                        input_schema: d.parameters.0.unwrap_or_else(|| json!({})),
                        extra: Map::new(),
                    })
                })
                .collect()
        }
    }

    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct FunctionDeclIn {
        name: Lenient<Option<String>>,
        description: Lenient<Option<String>>,
        /// Absent means `{}`; an explicit `null` stays `null` (the old
        /// `get().cloned()` distinction) — hence `Present`, not `Lenient<Option<Value>>`.
        parameters: Present,
    }

    /// `generationConfig` → sampling parameters (Gemini's key names differ from IR).
    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct GenerationConfigIn {
        temperature: Lenient<Option<f64>>,
        #[serde(rename = "topP")]
        top_p: Lenient<Option<f64>>,
        #[serde(rename = "maxOutputTokens")]
        max_output_tokens: Lenient<Option<u64>>,
        /// Present (even as null) means the IR `sampling.extra` carries a `stop`
        /// key, as the old `.get("stopSequences")` chain preserved.
        #[serde(rename = "stopSequences")]
        stop_sequences: Present,
    }

    impl GenerationConfigIn {
        fn into_sampling(self) -> Option<SamplingParams> {
            let mut extra = Map::new();
            if let Some(stop) = self.stop_sequences.0 {
                extra.insert("stop".into(), stop);
            }
            sampling_params(
                self.temperature.0,
                self.top_p.0,
                self.max_output_tokens.0,
                extra,
            )
        }
    }

    // ---- parts decode ----

    /// One Gemini part (request contents, response candidate content, and stream
    /// frames all share the shape). Keeps its original JSON so the escape hatch
    /// (`ContentBlock::Unknown`) preserves vendor shapes byte-faithfully; the custom
    /// `Deserialize` never fails, so a garbage item degrades to an `Unknown`
    /// carrying the garbage rather than dropping the whole array.
    #[derive(Debug)]
    struct Part {
        text: Present,
        function_call: Present,
        inline_data: Present,
        inline_data_camel: Present,
        function_response: Present,
        raw: Value,
    }

    /// Typed view of a [`Part`] entry. Every field is hostile-input-proof
    /// (`Present`), so parsing fails only when the item is not an object at all —
    /// exactly the old `_ => Unknown` path.
    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct PartFields {
        text: Present,
        #[serde(rename = "functionCall")]
        function_call: Present,
        #[serde(rename = "inline_data")]
        inline_data: Present,
        #[serde(rename = "inlineData")]
        inline_data_camel: Present,
        #[serde(rename = "functionResponse")]
        function_response: Present,
    }

    impl<'de> Deserialize<'de> for Part {
        fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
            // Capture first, interpret second: the Unknown escape hatch and the
            // Raw re-emit must yield the ORIGINAL JSON, never a re-serialization.
            let raw = Value::deserialize(d)?;
            let fields = serde_json::from_value::<PartFields>(raw.clone()).unwrap_or_default();
            Ok(Self {
                text: fields.text,
                function_call: fields.function_call,
                inline_data: fields.inline_data,
                inline_data_camel: fields.inline_data_camel,
                function_response: fields.function_response,
                raw,
            })
        }
    }

    impl Part {
        /// Part → IR block. The branch order is the old chain: `text` (only when
        /// the value is a *string*), then `functionCall`, then inline data (either
        /// spelling), then `functionResponse`; anything else takes the escape
        /// hatch.
        fn into_block(self) -> ContentBlock {
            if let Some(text) = self.text.as_str() {
                return ContentBlock::Text {
                    text: text.to_string(),
                };
            }
            if let Some(fc) = self.function_call.0 {
                let fc: FunctionCallIn = lenient_from_value(fc);
                return ContentBlock::ToolUse {
                    // Gemini gives no id; synthesize a deterministic one.
                    id: format!("gemini_call_{}", fc.name.as_str().unwrap_or("unknown")),
                    name: fc.name.as_str().unwrap_or_default().to_string(),
                    input: fc.args.0.unwrap_or_else(|| json!({})),
                };
            }
            // `inline_data` (proto style) wins over the `inlineData` (JSON style)
            // alias when both keys are present.
            if let Some(inline) = self.inline_data.0.or(self.inline_data_camel.0) {
                let inline: InlineDataIn = lenient_from_value(inline);
                return ContentBlock::Image {
                    media_type: inline
                        .mime_type
                        .0
                        .or(inline.mime_type_camel.0)
                        .as_ref()
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                    data: inline.data.0,
                };
            }
            if let Some(fr) = self.function_response.0 {
                let fr: FunctionResponseIn = lenient_from_value(fr);
                return ContentBlock::ToolResult {
                    tool_use_id: format!("gemini_call_{}", fr.name.as_str().unwrap_or("unknown")),
                    // Any present `response` serializes whole, including an explicit
                    // null (the old `.map(Value::to_string)` contract).
                    content: fr.response.0.map(|r| r.to_string()).unwrap_or_default(),
                };
            }
            ContentBlock::Unknown { raw: self.raw }
        }

        /// One streaming `parts[]` element → IR events appended to `out`.
        ///
        /// `text` and `functionCall` are mutually exclusive (each Gemini part has
        /// only one payload): when the `text` key is present, handle it as text
        /// only (even if the value is not a string / is empty, do not fall into the
        /// tool branch).
        fn push_stream_events(self, index: u32, out: &mut Vec<StreamEvent>) {
            if self.text.0.is_some() {
                if let Some(text) = self.text.as_str()
                    && !text.is_empty()
                {
                    out.push(StreamEvent::TextDelta {
                        index,
                        text: text.to_string(),
                    });
                }
                return;
            }
            if let Some(fc) = self.function_call.0 {
                let fc: FunctionCallIn = lenient_from_value(fc);
                out.push(StreamEvent::ToolCallStart {
                    index,
                    // Gemini gives no tool call id: synthesize a deterministic one (not a
                    // uuid, otherwise replaying the same stream yields different ids).
                    id: format!("gemini_call_{index}"),
                    name: fc.name.as_str().unwrap_or_default().to_string(),
                });
                // Gemini's args is a complete object (not incremental); emit the whole
                // JSON at once.
                if let Some(args) = fc.args.0 {
                    out.push(StreamEvent::ToolCallDelta {
                        index,
                        arguments_delta: args.to_string(),
                    });
                }
            }
        }
    }

    /// The `functionCall` payload. `name` feeds both the synthesized-id fallback
    /// and the name field; `args` is kept whole (Gemini sends complete objects, not
    /// incremental fragments).
    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct FunctionCallIn {
        name: Present,
        args: Present,
    }

    /// The `functionResponse` payload.
    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct FunctionResponseIn {
        name: Present,
        response: Present,
    }

    /// The `inline_data` / `inlineData` payload. The snake_case key wins over the
    /// camelCase alias for both the payload itself and `mime_type`, reproducing the
    /// old `.get(..).or_else(get(..))` precedence.
    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct InlineDataIn {
        #[serde(rename = "mime_type")]
        mime_type: Present,
        #[serde(rename = "mimeType")]
        mime_type_camel: Present,
        data: Lenient<String>,
    }

    // ---- response + stream decode ----

    /// Gemini non-streaming response body.
    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    pub(super) struct Response {
        candidates: Lenient<Vec<Lenient<CandidateIn>>>,
        #[serde(rename = "responseId")]
        response_id: Lenient<String>,
        #[serde(rename = "modelVersion")]
        model_version: Lenient<String>,
        #[serde(rename = "usageMetadata")]
        usage_metadata: Lenient<UsageIn>,
    }

    impl Response {
        pub(super) fn into_ir(self) -> LlmResponse {
            // Only the first candidate is modeled (the old candidates[0] contract).
            let candidate = self
                .candidates
                .0
                .into_iter()
                .next()
                .map(|c| c.0)
                .unwrap_or_default();
            let outputs = candidate
                .content
                .0
                .parts
                .0
                .into_iter()
                .map(Part::into_block)
                .collect::<Vec<_>>();
            let stop_reason = candidate
                .finish_reason
                .0
                .as_deref()
                .map(gemini_finish_reason)
                // Without finishReason, finish as tool use when there are tool calls.
                .unwrap_or_else(|| {
                    if outputs
                        .iter()
                        .any(|b| matches!(b, ContentBlock::ToolUse { .. }))
                    {
                        StopReason::ToolUse
                    } else {
                        StopReason::Stop
                    }
                });
            LlmResponse {
                // `responseId` is often absent; without it there is no stable
                // identifier, so it stays empty for the upper layer to fill.
                id: self.response_id.0,
                model: self.model_version.0,
                outputs,
                stop_reason,
                usage: self.usage_metadata.0.into_usage(),
            }
        }
    }

    /// One `candidates[]` entry (response and stream share the shape). `index` and
    /// the rest are deliberately not modeled: serde ignores unmodeled keys.
    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct CandidateIn {
        content: Lenient<ContentIn>,
        #[serde(rename = "finishReason")]
        finish_reason: Lenient<Option<String>>,
    }

    /// The `usageMetadata` object (exact Gemini names; the IR normalizes them).
    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct UsageIn {
        #[serde(rename = "promptTokenCount")]
        prompt_tokens: Lenient<u64>,
        #[serde(rename = "candidatesTokenCount")]
        candidates_tokens: Lenient<u64>,
        #[serde(rename = "thoughtsTokenCount")]
        thoughts_tokens: Lenient<Option<u64>>,
        #[serde(rename = "cachedContentTokenCount")]
        cached_content_tokens: Lenient<Option<u64>>,
    }

    impl UsageIn {
        fn into_usage(self) -> Usage {
            let thoughts = self.thoughts_tokens.0;
            Usage {
                // `promptTokenCount` already includes cached content, so it is the total.
                prompt_tokens: self.prompt_tokens.0,
                // IR completion is the total output; Gemini reports thinking outside
                // `candidatesTokenCount`, so it is added back here (saturating: a
                // malformed upstream must not wrap).
                completion_tokens: self
                    .candidates_tokens
                    .0
                    .saturating_add(thoughts.unwrap_or(0)),
                cached_tokens: self.cached_content_tokens.0,
                // Gemini has no separate cache-write counter.
                cache_creation_tokens: None,
                reasoning_tokens: thoughts,
            }
        }
    }

    /// One streaming frame's payload: only the fields `decode_event` reads.
    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    pub(super) struct Chunk {
        candidates: Lenient<Vec<Lenient<CandidateIn>>>,
        #[serde(rename = "usageMetadata")]
        usage_metadata: Lenient<UsageIn>,
    }

    impl Chunk {
        /// A frame → IR events: every candidate's parts (then its finishReason),
        /// and the usage event from the same frame. Both must be emitted — taking
        /// only candidates would make billing lose the entire streaming usage.
        pub(super) fn into_events(self) -> Vec<StreamEvent> {
            let mut out = Vec::new();
            for (ci, candidate) in self.candidates.0.into_iter().enumerate() {
                let ci = ci as u32;
                for (pi, part) in candidate.0.content.0.parts.0.into_iter().enumerate() {
                    // Each part gets its own index: Gemini itself has no block
                    // index concept, but downstream consumers (Claude's
                    // content_block, Responses' output_index) attribute by
                    // index, and folding everything into 0 would make multiple
                    // parts overwrite each other.
                    let index = ci * 1000 + pi as u32;
                    part.push_stream_events(index, &mut out);
                }

                if let Some(reason) = candidate.0.finish_reason.0 {
                    out.push(StreamEvent::MessageStop {
                        reason: gemini_finish_reason(&reason),
                    });
                }
            }

            let usage = self.usage_metadata.0.into_usage();
            if usage.prompt_tokens > 0 || usage.completion_tokens > 0 {
                out.push(StreamEvent::MessageDelta { usage: Some(usage) });
            }
            out
        }
    }

    // ---- encode models ----

    /// IR → Gemini request body. `skip_serializing_if` reproduces the old "insert
    /// only when present" behavior; unmodeled fields are merged into the emitted
    /// map afterwards and only fill missing keys, so the modeled fields above
    /// always win.
    #[derive(Debug, Serialize)]
    pub(super) struct RequestOut<'a> {
        #[serde(rename = "systemInstruction", skip_serializing_if = "Option::is_none")]
        system_instruction: Option<SystemInstructionOut<'a>>,
        /// Always emitted, even when empty (the old unconditional insert).
        contents: Vec<ContentOut<'a>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        tools: Option<Vec<ToolGroupOut<'a>>>,
        #[serde(rename = "generationConfig", skip_serializing_if = "Option::is_none")]
        generation_config: Option<GenerationConfigOut>,
        stream: bool,
    }

    impl<'a> RequestOut<'a> {
        pub(super) fn from_ir(req: &'a LlmRequest) -> Self {
            Self {
                system_instruction: (!req.system.is_empty()).then(|| SystemInstructionOut {
                    parts: req
                        .system
                        .iter()
                        .map(|b| TextPartOut { text: &b.text })
                        .collect(),
                }),
                contents: req.messages.iter().map(ContentOut::from_message).collect(),
                // The encoder always emits exactly one tools group.
                tools: (!req.tools.is_empty()).then(|| {
                    vec![ToolGroupOut {
                        function_declarations: req
                            .tools
                            .iter()
                            .map(FunctionDeclOut::from_ir)
                            .collect(),
                    }]
                }),
                generation_config: req
                    .sampling
                    .as_ref()
                    .and_then(GenerationConfigOut::from_sampling),
                stream: req.stream,
            }
        }
    }

    /// `systemInstruction` slot: a standalone parts object.
    #[derive(Debug, Serialize)]
    struct SystemInstructionOut<'a> {
        parts: Vec<TextPartOut<'a>>,
    }

    /// One `tools[]` group in the encode shape.
    #[derive(Debug, Serialize)]
    struct ToolGroupOut<'a> {
        #[serde(rename = "functionDeclarations")]
        function_declarations: Vec<FunctionDeclOut<'a>>,
    }

    /// IR tool definition → Gemini `functionDeclarations[]` entry (the schema
    /// field is `parameters` on Gemini).
    #[derive(Debug, Serialize)]
    struct FunctionDeclOut<'a> {
        name: &'a str,
        parameters: &'a Value,
        #[serde(skip_serializing_if = "Option::is_none")]
        description: Option<&'a str>,
    }

    impl<'a> FunctionDeclOut<'a> {
        fn from_ir(t: &'a ToolDef) -> Self {
            Self {
                name: &t.name,
                parameters: &t.input_schema,
                description: t.description.as_deref(),
            }
        }
    }

    /// IR sampling → Gemini `generationConfig`.
    #[derive(Debug, Serialize)]
    struct GenerationConfigOut {
        #[serde(skip_serializing_if = "Option::is_none")]
        temperature: Option<f64>,
        #[serde(rename = "topP", skip_serializing_if = "Option::is_none")]
        top_p: Option<f64>,
        #[serde(rename = "maxOutputTokens", skip_serializing_if = "Option::is_none")]
        max_output_tokens: Option<u64>,
        #[serde(rename = "stopSequences", skip_serializing_if = "Option::is_none")]
        stop_sequences: Option<Value>,
    }

    impl GenerationConfigOut {
        /// Reproduces the old "insert only when non-empty": with nothing to map,
        /// `generationConfig` is omitted entirely.
        fn from_sampling(s: &SamplingParams) -> Option<Self> {
            // Gemini uses stopSequences, and it must be an array.
            let stop_sequences = s.extra.get("stop").map(|stop| match stop {
                Value::String(text) => json!([text]),
                other => other.clone(),
            });
            let cfg = Self {
                temperature: s.temperature,
                top_p: s.top_p,
                max_output_tokens: s.max_tokens,
                stop_sequences,
            };
            (!cfg.is_empty()).then_some(cfg)
        }

        fn is_empty(&self) -> bool {
            self.temperature.is_none()
                && self.top_p.is_none()
                && self.max_output_tokens.is_none()
                && self.stop_sequences.is_none()
        }
    }

    /// A Gemini `content` object (request contents entry and response candidate
    /// content share the shape): role + parts.
    #[derive(Debug, Serialize)]
    struct ContentOut<'a> {
        role: &'static str,
        parts: Vec<PartOut<'a>>,
    }

    impl<'a> ContentOut<'a> {
        /// IR message → Gemini content (roles are only user / model).
        fn from_message(m: &'a IrMessage) -> Self {
            let role = match m.role {
                Role::Assistant => "model",
                // Tool results also go on the user side in Gemini.
                Role::User | Role::Tool => "user",
            };
            Self {
                role,
                parts: m.content.iter().map(PartOut::from_ir).collect(),
            }
        }
    }

    /// One Gemini part in the encode shape. A part is a one-payload object
    /// (`{"text": …}` / `{"functionCall": …}` / …), so untagged variants whose
    /// structs each declare the single wire key reproduce the exact shapes.
    #[derive(Debug, Serialize)]
    #[serde(untagged)]
    enum PartOut<'a> {
        Text(TextPartOut<'a>),
        FunctionCall(FunctionCallPartOut<'a>),
        FunctionResponse(FunctionResponsePartOut<'a>),
        InlineData(InlineDataPartOut<'a>),
        /// The escape hatch: the preserved original, re-emitted verbatim.
        Raw(Value),
    }

    impl<'a> PartOut<'a> {
        /// IR block → Gemini part.
        fn from_ir(block: &'a ContentBlock) -> Self {
            match block {
                ContentBlock::Text { text } => PartOut::Text(TextPartOut { text }),
                ContentBlock::ToolUse { name, input, .. } => {
                    PartOut::FunctionCall(FunctionCallPartOut {
                        function_call: FunctionCallOut { name, args: input },
                    })
                }
                ContentBlock::ToolResult { content, .. } => {
                    PartOut::FunctionResponse(FunctionResponsePartOut {
                        function_response: FunctionResponseOut {
                            // Gemini echoes no id; the existing wire shape hard-codes
                            // this name (do not "fix" it — it is what downstream
                            // consumers receive today).
                            name: "unknown",
                            response: FunctionResponsePayloadOut { content },
                        },
                    })
                }
                ContentBlock::Image { media_type, data } => {
                    PartOut::InlineData(InlineDataPartOut {
                        inline_data: InlineDataOut {
                            mime_type: media_type,
                            data,
                        },
                    })
                }
                // Gemini has no thinking / refusal part type; degrade to plain text so
                // the content is not lost.
                ContentBlock::Thinking { text } | ContentBlock::Refusal { text } => {
                    PartOut::Text(TextPartOut { text })
                }
                ContentBlock::Unknown { raw } => PartOut::Raw(raw.clone()),
            }
        }
    }

    #[derive(Debug, Serialize)]
    struct TextPartOut<'a> {
        text: &'a str,
    }

    #[derive(Debug, Serialize)]
    struct FunctionCallPartOut<'a> {
        #[serde(rename = "functionCall")]
        function_call: FunctionCallOut<'a>,
    }

    #[derive(Debug, Serialize)]
    struct FunctionCallOut<'a> {
        name: &'a str,
        args: &'a Value,
    }

    #[derive(Debug, Serialize)]
    struct FunctionResponsePartOut<'a> {
        #[serde(rename = "functionResponse")]
        function_response: FunctionResponseOut<'a>,
    }

    #[derive(Debug, Serialize)]
    struct FunctionResponseOut<'a> {
        name: &'static str,
        response: FunctionResponsePayloadOut<'a>,
    }

    #[derive(Debug, Serialize)]
    struct FunctionResponsePayloadOut<'a> {
        content: &'a str,
    }

    #[derive(Debug, Serialize)]
    struct InlineDataPartOut<'a> {
        /// The encoder emits the proto-style snake_case key (the existing wire
        /// shape; the decoder accepts both spellings).
        #[serde(rename = "inline_data")]
        inline_data: InlineDataOut<'a>,
    }

    #[derive(Debug, Serialize)]
    struct InlineDataOut<'a> {
        mime_type: &'a str,
        data: &'a str,
    }

    /// IR → Gemini non-streaming response body.
    #[derive(Debug, Serialize)]
    pub(super) struct ResponseOut<'a> {
        candidates: Vec<CandidateOut<'a>>,
        #[serde(rename = "usageMetadata")]
        usage_metadata: Value,
    }

    #[derive(Debug, Serialize)]
    struct CandidateOut<'a> {
        content: ContentOut<'a>,
        #[serde(rename = "finishReason")]
        finish_reason: String,
        index: u32,
    }

    impl<'a> ResponseOut<'a> {
        pub(super) fn from_ir(resp: &'a LlmResponse) -> Self {
            Self {
                candidates: vec![CandidateOut {
                    content: ContentOut {
                        role: "model",
                        parts: resp.outputs.iter().map(PartOut::from_ir).collect(),
                    },
                    finish_reason: gemini_finish_reason_str(&resp.stop_reason),
                    index: 0,
                }],
                usage_metadata: encode_usage_metadata(&resp.usage),
            }
        }
    }

    /// The `usageMetadata` JSON object, shared by the response encoder and the
    /// stream encoder's usage frame.
    #[derive(Debug, Serialize)]
    pub(super) struct UsageMetadataOut {
        #[serde(rename = "promptTokenCount")]
        pub(super) prompt_token_count: u64,
        #[serde(rename = "candidatesTokenCount")]
        pub(super) candidates_token_count: u64,
        #[serde(rename = "totalTokenCount")]
        pub(super) total_token_count: u64,
        #[serde(rename = "cachedContentTokenCount")]
        pub(super) cached_content_token_count: u64,
        #[serde(rename = "thoughtsTokenCount", skip_serializing_if = "Option::is_none")]
        pub(super) thoughts_token_count: Option<u64>,
    }
}
