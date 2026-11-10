//! Claude (Anthropic Messages) format codec — IR ↔ `/v1/messages` (WP3).
//!
//! Single-format bidirectional codec: `decode_*` parses the Claude shape into IR,
//! `encode_*` renders IR back into the Claude shape. Adding a new format only
//! requires implementing another `FormatCodec`, not a direction-specific converter
//! for every existing format.
//!
//! ## Wire models
//!
//! Every Claude field name is declared exactly once, as a serde field of a DTO in
//! [`wire`]; the codec body is decode → [`wire::Request::into_ir`] and IR → wire
//! struct → bytes. Wire DTOs decode leniently (nested [`ir::Lenient`] fields
//! degrade type mismatches to defaults — the same contract the old
//! `.and_then(Value::as_).unwrap_or_default()` chains had), and unmodeled
//! top-level fields land in the flattened `extra` map, so the hand-maintained
//! "known keys" list is gone. Two shapes need more than `Option`: the shared
//! [`ir::Present`] keeps "key absent" apart from "explicit null" (`tool_use.input`
//! and the `thinking`/`text` fallback map those two differently), and `wire::Block`
//! captures its original JSON so the `ContentBlock::Unknown` escape hatch and the
//! `StreamEvent::Raw` re-emit stay faithful to the sender's bytes.
//!
//! [`ir::Lenient`]: crate::ir
//!
//! ## Three pre-existing defects this module fixes (G1)
//!
//! In the old implementation `adaptor::convert_claude_event_to_openai`, tool calls
//! were broken in three places:
//!
//! 1. **`content_block_start` had no match arm**, so `_ => {}` dropped the whole
//!    event. The tool's `id` / `name` are carried only in this event, so clients
//!    never received the tool identity and got only a stream of ownerless
//!    `input_json_delta` fragments. → [`ClaudeCodec::decode_event`] now emits
//!    [`StreamEvent::ToolCallStart`].
//! 2. **`input_json_delta`'s tool index was hard-coded to `0`**, so parallel tool
//!    calls all collapsed onto the same index and overwrote each other. → The
//!    event's own `index` is used now.
//! 3. **Input usage in `message_start` was dropped**. → It is now converted into
//!    [`StreamEvent::MessageDelta`].
//!
//! Only the **encoding side** needs cross-event state (it inserts the
//! `content_block_start` / `content_block_stop` boundary frames by block index);
//! [`ClaudeCodec::decode_event`] therefore remains stateless.

use bytes::Bytes;
use serde_json::{Value, json};
use std::collections::BTreeMap;

use crate::adaptor::{AdaptorError, Protocol};
use crate::format_codec::{FormatCodec, StreamEncoder};
use crate::ir::{
    LlmRequest, LlmResponse, StopReason, StreamEvent, Usage, apply_extra_fields, decode_body,
    encode_object, encode_value, struct_to_map, wire_value,
};
use crate::sse::event_frame;

/// Claude Messages format codec.
#[derive(Debug, Default, Clone, Copy)]
pub struct ClaudeCodec;

impl ClaudeCodec {
    /// Creates a codec (stateless, safe to share).
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl FormatCodec for ClaudeCodec {
    fn format(&self) -> Protocol {
        Protocol::Claude
    }

    /// Claude request body → IR.
    fn decode_request(&self, body: Bytes) -> Result<LlmRequest, AdaptorError> {
        let v: Value = decode_body(&body)?;
        if !v.is_object() {
            return Err(AdaptorError::DecodeFailed(
                "claude request body is not an object".into(),
            ));
        }
        // Every wire field is lenient, so an object-shaped body never fails conversion.
        let req: wire::Request = serde_json::from_value(v).unwrap_or_default();
        Ok(req.into_ir())
    }

    /// IR → Claude request body.
    fn encode_request(&self, req: &LlmRequest) -> Result<Bytes, AdaptorError> {
        let mut out = struct_to_map(&wire::RequestOut::from_ir(req))?;
        apply_extra_fields(&mut out, req);
        encode_object(out)
    }

    /// Claude non-streaming response body → IR.
    fn decode_response(&self, body: Bytes) -> Result<LlmResponse, AdaptorError> {
        let v: Value = decode_body(&body)?;
        // Upstreams are hostile input, not validated senders: a non-object body or
        // any missing field decodes to empty IR rather than failing (the
        // long-standing contract of the `.unwrap_or_default()` chains this replaces).
        let resp: wire::Response = serde_json::from_value(v).unwrap_or_default();
        Ok(resp.into_ir())
    }

    /// IR → Claude non-streaming response body.
    fn encode_response(&self, resp: &LlmResponse) -> Result<Bytes, AdaptorError> {
        encode_value(&wire_value(&wire::ResponseOut::from_ir(resp)))
    }

    /// Data payload of one complete SSE frame → IR events (stateless, 1:1 per
    /// event).
    fn decode_event(&self, data: &str) -> Result<Vec<StreamEvent>, AdaptorError> {
        // SSE streams contain non-JSON control frames; skip them silently
        // instead of failing the whole stream.
        let Ok(v) = serde_json::from_str::<Value>(data) else {
            return Ok(Vec::new());
        };
        Ok(wire::event_into_events(v))
    }

    fn stream_encoder(&self) -> Box<dyn StreamEncoder> {
        Box::new(ClaudeStreamEncoder::new())
    }
}

// ---------- Usage and stop-reason vocabulary ----------
// Shared by the wire models (which convert through them) and the streaming
// encoder's frame builders (which embed them), so they live at module level
// next to both users.

/// Usage object for a Claude `message_delta` frame.
///
/// Claude normally reports the input side in `message_start` and the output side in
/// `message_delta`; the IR carries one merged `Usage`, and `message_start` has already
/// been emitted by the time the delta arrives, so the input side is mirrored here when
/// it is known. An output-only delta (`prompt_tokens == 0`) keeps just `output_tokens`
/// rather than claiming zero non-cached input tokens.
fn encode_delta_usage(u: &Usage) -> Value {
    let mut out = encode_usage(u);
    if u.prompt_tokens == 0
        && let Some(obj) = out.as_object_mut()
    {
        obj.remove("input_tokens");
        obj.remove("cache_read_input_tokens");
        obj.remove("cache_creation_input_tokens");
    }
    out
}

/// IR usage → Claude `usage` object.
///
/// Converts the normalized IR (total prompt, cache as subsets) back to Claude's
/// additive shape: cache quantities are emitted only when present, and
/// `input_tokens` is the total minus them.
fn encode_usage(u: &Usage) -> Value {
    // Claude's `input_tokens` excludes cache tokens while the IR prompt is the
    // total, so the subsets are subtracted back out (saturating against
    // inconsistent upstream numbers instead of emitting a negative count).
    let mut out = json!({
        "input_tokens": u
            .prompt_tokens
            .saturating_sub(u.cached_tokens.unwrap_or(0))
            .saturating_sub(u.cache_creation_tokens.unwrap_or(0)),
        "output_tokens": u.completion_tokens,
    });
    if let Some(v) = u.cached_tokens {
        out["cache_read_input_tokens"] = json!(v);
    }
    if let Some(v) = u.cache_creation_tokens {
        out["cache_creation_input_tokens"] = json!(v);
    }
    out
}

fn claude_stop_reason(reason: &str) -> StopReason {
    match reason {
        "end_turn" | "stop_sequence" => StopReason::Stop,
        "max_tokens" => StopReason::Length,
        "tool_use" => StopReason::ToolUse,
        "refusal" => StopReason::ContentFilter,
        other => StopReason::Other(other.to_string()),
    }
}

fn claude_stop_reason_str(reason: &StopReason) -> String {
    match reason {
        StopReason::Stop | StopReason::Error => "end_turn".into(),
        StopReason::Length => "max_tokens".into(),
        StopReason::ToolUse => "tool_use".into(),
        StopReason::ContentFilter => "refusal".into(),
        StopReason::Other(s) => s.clone(),
    }
}

// ---------- Streaming encoder ----------

/// Encoding state of one opened block.
#[derive(Debug, Clone, Copy, PartialEq)]
enum BlockKind {
    Text,
    Thinking,
    Tool,
}

#[derive(Debug, Clone)]
struct BlockState {
    kind: BlockKind,
    /// Whether the tool block has already emitted `content_block_start`.
    opened: bool,
    id: String,
    name: String,
}

/// Claude stream encoder: holds block index state and inserts the boundary frames
/// required by the Claude protocol.
///
/// Claude's event model requires each block to start with `content_block_start`,
/// then several deltas, and finally `content_block_stop`; an IR event stream has
/// no such boundaries (only deltas carrying an index), so this encoder inserts
/// them on demand — which is also why "the encoding side is stateful".
struct ClaudeStreamEncoder {
    id: Option<String>,
    started: bool,
    blocks: BTreeMap<u32, BlockState>,
    stopped: bool,
}

impl ClaudeStreamEncoder {
    fn new() -> Self {
        Self {
            id: None,
            started: false,
            blocks: BTreeMap::new(),
            stopped: false,
        }
    }

    /// Emits `message_start` when necessary (an IR stream may have no explicit
    /// `MessageStart`).
    fn ensure_started(&mut self) -> Vec<Bytes> {
        if self.started {
            return Vec::new();
        }
        self.started = true;
        let id = self
            .id
            .clone()
            .unwrap_or_else(|| format!("msg_{}", uuid::Uuid::new_v4().simple()));
        self.id = Some(id.clone());
        vec![event_frame(
            "message_start",
            &json!({
                "type": "message_start",
                "message": {
                    "id": id,
                    "type": "message",
                    "role": "assistant",
                    "model": "",
                    "content": [],
                    "stop_reason": null,
                    "usage": {"input_tokens": 0, "output_tokens": 0},
                }
            }),
        )]
    }

    /// Ensures the block at an index is opened and returns the
    /// `content_block_start` frame (emitted only on the first call).
    fn ensure_block(&mut self, index: u32, kind: BlockKind, id: &str, name: &str) -> Vec<Bytes> {
        let entry = self.blocks.entry(index).or_insert_with(|| BlockState {
            kind,
            opened: false,
            id: id.to_string(),
            name: name.to_string(),
        });
        // Later events may fill in more complete information.
        if !id.is_empty() {
            entry.id = id.to_string();
        }
        if !name.is_empty() {
            entry.name = name.to_string();
        }
        if entry.opened {
            return Vec::new();
        }
        entry.opened = true;
        let content_block = match entry.kind {
            BlockKind::Text => json!({"type": "text", "text": ""}),
            BlockKind::Thinking => json!({"type": "thinking", "thinking": ""}),
            // The tool's id/name may only be complete at this point; use a
            // placeholder when id is missing so the whole call is not dropped.
            BlockKind::Tool => json!({
                "type": "tool_use",
                "id": if entry.id.is_empty() { format!("toolu_{index}") } else { entry.id.clone() },
                "name": if entry.name.is_empty() { "unknown".to_string() } else { entry.name.clone() },
                "input": {},
            }),
        };
        vec![event_frame(
            "content_block_start",
            &json!({
                "type": "content_block_start",
                "index": index,
                "content_block": content_block,
            }),
        )]
    }

    /// Incremental frame of one content block: emits `content_block_start` first
    /// when necessary.
    // `json!` moves `delta` into the serialized payload, so the argument is genuinely
    // consumed — borrowing it would not compile against the opaque macro.
    #[allow(clippy::needless_pass_by_value)]
    fn block_delta(&mut self, index: u32, kind: BlockKind, delta: Value) -> Vec<Bytes> {
        let mut out = self.ensure_block(index, kind, "", "");
        out.push(event_frame(
            "content_block_delta",
            &json!({
                "type": "content_block_delta",
                "index": index,
                "delta": delta,
            }),
        ));
        out
    }

    /// Closes every still-open block and returns the corresponding
    /// `content_block_stop` frames.
    fn close_blocks(&mut self) -> Vec<Bytes> {
        self.blocks
            .iter_mut()
            .filter(|(_, state)| state.opened)
            .map(|(index, state)| {
                state.opened = false;
                event_frame(
                    "content_block_stop",
                    &json!({"type": "content_block_stop", "index": index}),
                )
            })
            .collect()
    }

    /// `message_delta` + `message_stop` closing frames.
    fn finish_frames(&mut self, reason: &StopReason) -> Vec<Bytes> {
        if self.stopped {
            return Vec::new();
        }
        self.stopped = true;
        vec![
            event_frame(
                "message_delta",
                &json!({
                    "type": "message_delta",
                    "delta": {"stop_reason": claude_stop_reason_str(reason)},
                }),
            ),
            event_frame("message_stop", &json!({"type": "message_stop"})),
        ]
    }
}

impl StreamEncoder for ClaudeStreamEncoder {
    fn encode_event(&mut self, ev: &StreamEvent) -> Result<Vec<Bytes>, AdaptorError> {
        let mut out = match ev {
            StreamEvent::MessageStart { id, .. } => {
                if id.is_empty() {
                    self.id = None;
                } else {
                    self.id = Some(id.clone());
                }
                self.ensure_started()
            }
            StreamEvent::TextDelta { index, text } => self.block_delta(
                *index,
                BlockKind::Text,
                json!({"type": "text_delta", "text": text}),
            ),
            StreamEvent::ThinkingDelta { index, text } => self.block_delta(
                *index,
                BlockKind::Thinking,
                json!({"type": "thinking_delta", "thinking": text}),
            ),
            StreamEvent::ToolCallStart { index, id, name } => {
                self.ensure_block(*index, BlockKind::Tool, id, name)
            }
            StreamEvent::ToolCallDelta {
                index,
                arguments_delta,
            } => {
                // The upstream may send only deltas and no start (with id/name
                // missing) — the block must still be opened here, otherwise the
                // whole tool call silently disappears.
                self.block_delta(
                    *index,
                    BlockKind::Tool,
                    json!({"type": "input_json_delta", "partial_json": arguments_delta}),
                )
            }
            StreamEvent::MessageDelta { usage } => {
                let mut out = self.ensure_started();
                if let Some(u) = usage {
                    out.push(event_frame(
                        "message_delta",
                        &json!({
                            "type": "message_delta",
                            "delta": {},
                            "usage": encode_delta_usage(u),
                        }),
                    ));
                }
                out
            }
            StreamEvent::MessageStop { reason } => {
                let mut out = self.ensure_started();
                out.extend(self.close_blocks());
                out.extend(self.finish_frames(reason));
                out
            }
            StreamEvent::Raw { data } => {
                let kind = data
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or("message");
                vec![event_frame(kind, data)]
            }
            StreamEvent::Error { message } => {
                vec![event_frame(
                    "error",
                    &json!({"type": "error", "error": {"type": "api_error", "message": message}}),
                )]
            }
        };
        // Defensive: every content event should come after message_start.
        if !self.started && !out.is_empty() {
            let mut head = self.ensure_started();
            head.append(&mut out);
            out = head;
        }
        Ok(out)
    }

    fn finish(&mut self) -> Result<Vec<Bytes>, AdaptorError> {
        if self.stopped {
            return Ok(Vec::new());
        }
        let mut out = self.ensure_started();
        out.extend(self.close_blocks());
        out.extend(self.finish_frames(&StopReason::Stop));
        Ok(out)
    }
}

// ---------- Wire models ----------
mod wire {
    use serde::{Deserialize, Deserializer, Serialize};
    use serde_json::{Map, Value, json};

    use super::{claude_stop_reason, claude_stop_reason_str, encode_usage};
    use crate::ir::{
        ContentBlock, Lenient, LlmRequest, LlmResponse, Message as IrMessage, Present, Role,
        StopReason, StreamEvent, TextBlock, ToolChoice, ToolDef, Usage, event_index, from_event,
        sampling_params,
    };

    /// Claude `max_tokens` is required; this default applies when IR carries none
    /// (same as the old implementation).
    const DEFAULT_MAX_TOKENS: u64 = 4096;

    /// Wire `tool_choice` (any shape) → IR. Fed from the flattened `extra` map so
    /// a literal JSON `null` survives as `Raw(null)` — present-but-null is a
    /// distinct wire fact the old `.get("tool_choice")` chain preserved.
    fn decode_tool_choice(v: Value) -> Option<ToolChoice> {
        match v.get("type").and_then(Value::as_str) {
            Some("auto") => Some(ToolChoice::Auto),
            Some("any") => Some(ToolChoice::Required),
            Some("tool") => v
                .get("name")
                .and_then(Value::as_str)
                .map(|n| ToolChoice::Specific(n.to_string())),
            Some("none") => Some(ToolChoice::None),
            // Unknown shapes are preserved verbatim, not silently degraded to auto.
            _ => Some(ToolChoice::Raw(v)),
        }
    }

    /// IR → wire `tool_choice`.
    fn encode_tool_choice(tc: &ToolChoice) -> Option<Value> {
        match tc {
            ToolChoice::Auto => Some(json!({"type": "auto"})),
            ToolChoice::Required => Some(json!({"type": "any"})),
            ToolChoice::Specific(name) => Some(json!({"type": "tool", "name": name})),
            // None is expressed by omission (Claude's "none" is newer; omission is
            // more compatible).
            ToolChoice::None => None,
            ToolChoice::Raw(v) => Some(v.clone()),
        }
    }

    fn claude_role(role: &Role) -> &'static str {
        match role {
            Role::Assistant => "assistant",
            // Claude carries tool_result in a user message.
            Role::User | Role::Tool => "user",
        }
    }

    // ---- request decode ----

    /// Claude request body. One serde field per modeled wire key; everything
    /// unmodeled lands in the flattened `extra` and is carried back verbatim. The
    /// old hand-maintained `KNOWN` list is exactly this field set plus the
    /// `tool_choice` removal in [`Request::into_ir`] — it can no longer drift.
    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    pub(super) struct Request {
        model: Lenient<String>,
        system: Lenient<Option<SystemWire>>,
        messages: Lenient<Vec<Lenient<RequestMessage>>>,
        tools: Lenient<Vec<Lenient<Tool>>>,
        temperature: Lenient<Option<f64>>,
        top_p: Lenient<Option<f64>>,
        max_tokens: Lenient<Option<u64>>,
        stream: Lenient<bool>,
        #[serde(flatten)]
        extra: Map<String, Value>,
    }

    impl Request {
        pub(super) fn into_ir(self) -> LlmRequest {
            let system = self
                .system
                .0
                .map(SystemWire::into_blocks)
                .unwrap_or_default();
            let messages = self.messages.0.into_iter().map(|m| m.0.into_ir()).collect();
            let tools = self
                .tools
                .0
                .into_iter()
                .filter_map(|t| t.0.into_def())
                .collect();

            let mut extra = self.extra;
            // tool_choice is modeled IR-side, so it leaves the unmodeled map — but
            // through removal rather than a declared field: a declared field would
            // erase the old `.get("tool_choice")` distinction where a PRESENT-BUT-NULL
            // value decodes to `Raw(null)`.
            let tool_choice = extra.remove("tool_choice").and_then(decode_tool_choice);

            // stop_sequences / top_k are consumed into sampling.extra but
            // deliberately STAY in `extra` too: the old KNOWN list never excluded
            // them, so encode round-trips and cross-format hops must keep seeing
            // the original keys.
            let mut sampling_extra = Map::new();
            if let Some(stop) = extra.get("stop_sequences") {
                sampling_extra.insert("stop".into(), stop.clone());
            }
            if let Some(top_k) = extra.get("top_k") {
                sampling_extra.insert("top_k".into(), top_k.clone());
            }

            LlmRequest {
                model: self.model.0,
                messages,
                system,
                tools,
                tool_choice,
                sampling: sampling_params(
                    self.temperature.0,
                    self.top_p.0,
                    self.max_tokens.0,
                    sampling_extra,
                ),
                stream: self.stream.0,
                extra,
            }
        }
    }

    /// One `messages[]` entry. Claude has only the user/assistant roles;
    /// `tool_result` travels inside a user message.
    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct RequestMessage {
        role: Lenient<String>,
        content: Lenient<Option<ContentWire>>,
    }

    impl RequestMessage {
        fn into_ir(self) -> IrMessage {
            let role = if self.role.0 == "assistant" {
                Role::Assistant
            } else {
                Role::User
            };
            IrMessage {
                role,
                content: self
                    .content
                    .0
                    .map(ContentWire::into_blocks)
                    .unwrap_or_default(),
            }
        }
    }

    /// Claude `content`: a plain string or a block array. Anything else (null,
    /// numbers, plain objects) fails both arms and degrades to `None` through the
    /// enclosing `Lenient` — the old "not content the IR models" contract.
    #[derive(Debug, Deserialize)]
    #[serde(untagged)]
    enum ContentWire {
        Str(String),
        Blocks(Vec<Block>),
    }

    impl ContentWire {
        fn into_blocks(self) -> Vec<ContentBlock> {
            match self {
                // A plain string is ONE text block even when empty: Claude's
                // long-standing semantics (the OpenAI codec drops empty text).
                ContentWire::Str(text) => vec![ContentBlock::Text { text }],
                ContentWire::Blocks(blocks) => blocks.into_iter().map(Block::into_block).collect(),
            }
        }
    }

    /// Claude `system` field (string or block array). Unlike `OpenAI`'s helper
    /// (`ir::decode_text_blocks`, deliberately unused here), a Claude empty
    /// string IS a system block of its own.
    #[derive(Debug, Deserialize)]
    #[serde(untagged)]
    enum SystemWire {
        Str(String),
        Blocks(Vec<Lenient<SystemBlock>>),
    }

    impl SystemWire {
        fn into_blocks(self) -> Vec<TextBlock> {
            match self {
                SystemWire::Str(text) => vec![TextBlock { text }],
                SystemWire::Blocks(blocks) => blocks
                    .into_iter()
                    // Items without a usable `text` are skipped, not fatal (the old
                    // filter_map contract); Lenient keeps a garbage item's siblings.
                    .filter_map(|b| b.0.text.0)
                    .map(|text| TextBlock { text })
                    .collect(),
            }
        }
    }

    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct SystemBlock {
        text: Lenient<Option<String>>,
    }

    /// One Claude content block. Keeps its original JSON so the escape hatch
    /// (`ContentBlock::Unknown`) preserves vendor shapes byte-faithfully; the
    /// custom `Deserialize` never fails, so a garbage item degrades to an
    /// `Unknown` carrying the garbage rather than dropping the whole array.
    #[derive(Debug, Default)]
    struct Block {
        kind: Option<String>,
        text: Present,
        thinking: Present,
        id: Option<String>,
        name: Option<String>,
        input: Present,
        tool_use_id: Option<String>,
        content: Option<Value>,
        source: Option<Source>,
        raw: Value,
    }

    /// Typed view of a [`Block`] entry. Every field is hostile-input-proof
    /// (Lenient / Present), so parsing fails only when the item is not an object
    /// at all — exactly the old `_ => Unknown` path.
    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct BlockFields {
        #[serde(rename = "type")]
        kind: Lenient<Option<String>>,
        text: Present,
        thinking: Present,
        id: Lenient<Option<String>>,
        name: Lenient<Option<String>>,
        input: Present,
        tool_use_id: Lenient<Option<String>>,
        content: Lenient<Option<Value>>,
        source: Lenient<Option<Source>>,
    }

    impl<'de> Deserialize<'de> for Block {
        fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
            // Capture first, interpret second: the Unknown escape hatch and the
            // Raw re-emit must yield the ORIGINAL JSON, never a re-serialization.
            let raw = Value::deserialize(d)?;
            let fields = serde_json::from_value::<BlockFields>(raw.clone()).unwrap_or_default();
            Ok(Self {
                kind: fields.kind.0,
                text: fields.text,
                thinking: fields.thinking,
                id: fields.id.0,
                name: fields.name.0,
                input: fields.input,
                tool_use_id: fields.tool_use_id.0,
                content: fields.content.0,
                source: fields.source.0,
                raw,
            })
        }
    }

    impl Block {
        fn into_block(self) -> ContentBlock {
            match self.kind.as_deref() {
                Some("text") => ContentBlock::Text {
                    text: self.text.as_str().unwrap_or_default().to_string(),
                },
                Some("tool_use") => ContentBlock::ToolUse {
                    id: self.id.unwrap_or_default(),
                    name: self.name.unwrap_or_default(),
                    // Absent means `{}`; an explicit `null` stays `null` (the old
                    // `get().cloned()` distinction).
                    input: self.input.0.unwrap_or_else(|| json!({})),
                },
                Some("tool_result") => ContentBlock::ToolResult {
                    tool_use_id: self.tool_use_id.unwrap_or_default(),
                    content: extract_result_text(self.content.as_ref()),
                },
                Some("thinking") => ContentBlock::Thinking {
                    // `thinking` first, `text` as fallback — and a present-but-null
                    // `thinking` blocks the fallback: the old
                    // `.get("thinking").or_else(get("text"))` saw `Some(Value::Null)`
                    // and stopped there.
                    text: self
                        .thinking
                        .0
                        .or(self.text.0)
                        .as_ref()
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                },
                Some("image") => {
                    let source = self.source.unwrap_or_default();
                    ContentBlock::Image {
                        media_type: source.media_type.0.unwrap_or_default(),
                        data: source.data.0.unwrap_or_default(),
                    }
                }
                // Unknown types are preserved wholesale, so new shapes survive the hop.
                _ => ContentBlock::Unknown { raw: self.raw },
            }
        }

        /// `content_block_start` only carries information the IR needs for
        /// `tool_use` blocks: their id/name appear nowhere else in the stream (the
        /// G1 fix). Start frames of text / thinking blocks carry no content; all
        /// content is in the deltas.
        fn tool_start(&self, index: u32) -> Option<StreamEvent> {
            (self.kind.as_deref() == Some("tool_use")).then(|| StreamEvent::ToolCallStart {
                index,
                id: self.id.clone().unwrap_or_default(),
                name: self.name.clone().unwrap_or_default(),
            })
        }
    }

    /// The image block's `source` object. Wrong-typed members default to empty
    /// (the old per-field `unwrap_or_default`), and a garbage source degrades to
    /// an empty one rather than dropping the block.
    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct Source {
        media_type: Lenient<Option<String>>,
        data: Lenient<Option<String>>,
    }

    /// Claude's `tool_result.content` can be a string or a block array; text is
    /// extracted uniformly.
    fn extract_result_text(content: Option<&Value>) -> String {
        match content {
            Some(Value::String(s)) => s.clone(),
            Some(Value::Array(items)) => items
                .iter()
                .filter_map(|i| i.get("text").and_then(Value::as_str))
                .collect::<Vec<_>>()
                .join("\n"),
            Some(Value::Null) | None => String::new(),
            Some(other) => other.to_string(),
        }
    }

    /// `tools[]` entry: Claude's flat `{name, description?, input_schema?}` shape.
    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct Tool {
        name: Lenient<Option<String>>,
        description: Lenient<Option<String>>,
        input_schema: Present,
    }

    impl Tool {
        /// `None` = entry without a usable `name` (the old `filter_map` skip).
        fn into_def(self) -> Option<ToolDef> {
            let name = self.name.0?;
            Some(ToolDef {
                name,
                description: self.description.0,
                // Absent means `{}`; an explicit `null` stays `null` (the old
                // `get().cloned()` distinction).
                input_schema: self.input_schema.0.unwrap_or_else(|| json!({})),
                extra: Map::new(),
            })
        }
    }

    // ---- response decode ----

    /// Claude non-streaming response body. A non-object body or any wrong-typed
    /// field decodes to empty IR rather than failing (the old per-field
    /// `.unwrap_or_default()` contract, including "missing `stop_reason` means
    /// Stop").
    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    pub(super) struct Response {
        id: Lenient<String>,
        model: Lenient<String>,
        content: Lenient<Option<ContentWire>>,
        usage: Lenient<UsageIn>,
        stop_reason: Lenient<Option<String>>,
    }

    impl Response {
        pub(super) fn into_ir(self) -> LlmResponse {
            LlmResponse {
                id: self.id.0,
                model: self.model.0,
                outputs: self
                    .content
                    .0
                    .map(ContentWire::into_blocks)
                    .unwrap_or_default(),
                usage: self.usage.0.into_usage(),
                stop_reason: self
                    .stop_reason
                    .0
                    .map_or(StopReason::Stop, |reason| claude_stop_reason(&reason)),
            }
        }
    }

    /// The Claude `usage` object. Wire names stay Claude's (`input_tokens`, not
    /// `prompt_tokens`); the IR normalizes the accounting.
    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct UsageIn {
        #[serde(rename = "input_tokens")]
        input: Lenient<u64>,
        #[serde(rename = "output_tokens")]
        output: Lenient<u64>,
        #[serde(rename = "cache_read_input_tokens")]
        cache_read_input: Lenient<Option<u64>>,
        #[serde(rename = "cache_creation_input_tokens")]
        cache_creation_input: Lenient<Option<u64>>,
    }
    impl UsageIn {
        fn into_usage(self) -> Usage {
            let cache_read = self.cache_read_input.0;
            let cache_creation = self.cache_creation_input.0;
            Usage {
                // IR prompt is the **total** input; Claude reports cache tokens
                // outside `input_tokens`, so they are added back here (saturating:
                // a malformed upstream must not wrap).
                prompt_tokens: self
                    .input
                    .0
                    .saturating_add(cache_read.unwrap_or(0))
                    .saturating_add(cache_creation.unwrap_or(0)),
                completion_tokens: self.output.0,
                cached_tokens: cache_read,
                cache_creation_tokens: cache_creation,
                // Claude reports thinking tokens inside output_tokens; there is no
                // separate field to lift.
                reasoning_tokens: None,
            }
        }
    }

    // ---- stream event decode ----

    /// Whole SSE event JSON → IR events. The top-level `type` tag picks the DTO;
    /// frames without a string tag are control noise, and unknown event types are
    /// kept whole as `Raw` so a future event type survives a same-protocol hop.
    pub(super) fn event_into_events(ev: Value) -> Vec<StreamEvent> {
        // The tag is copied so the original Value stays owned for the Raw arms.
        let Some(kind) = ev.get("type").and_then(Value::as_str).map(str::to_owned) else {
            return Vec::new();
        };
        match kind.as_str() {
            "message_start" => from_event::<MessageStartEvent>(&ev).into_events(),
            "content_block_start" => from_event::<ContentBlockStartEvent>(&ev).into_events(),
            // The delta's own tag picks the arm; an unmodeled kind keeps the WHOLE
            // event as Raw (the event, not just the delta — the re-emit must be
            // verbatim).
            "content_block_delta" => from_event::<ContentBlockDeltaEvent>(&ev).into_events(ev),
            "message_delta" => from_event::<MessageDeltaEvent>(&ev).into_events(),
            "error" => from_event::<StreamErrorEvent>(&ev).into_events(),
            // Control frames carry no payload the IR models; termination is
            // expressed by SSE's [DONE] or EOF.
            "content_block_stop" | "message_stop" | "ping" => Vec::new(),
            _ => vec![StreamEvent::Raw { data: ev }],
        }
    }

    /// `message_start`: id/model + first-frame usage (the old implementation
    /// dropped the entire usage block).
    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct MessageStartEvent {
        message: Lenient<StartMessage>,
    }

    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct StartMessage {
        id: Lenient<String>,
        model: Lenient<String>,
        usage: Lenient<UsageIn>,
    }

    impl MessageStartEvent {
        fn into_events(self) -> Vec<StreamEvent> {
            let msg = self.message.0;
            let mut out = vec![StreamEvent::MessageStart {
                id: msg.id.0,
                model: msg.model.0,
            }];
            // message_start carries input usage — the old implementation discarded
            // it entirely, so metering missed the prompt side.
            let usage = msg.usage.0.into_usage();
            if usage.prompt_tokens > 0 || usage.completion_tokens > 0 {
                out.push(StreamEvent::MessageDelta { usage: Some(usage) });
            }
            out
        }
    }

    /// `content_block_start`: only `tool_use` carries information IR needs.
    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct ContentBlockStartEvent {
        index: Lenient<u64>,
        content_block: Block,
    }

    impl ContentBlockStartEvent {
        fn into_events(self) -> Vec<StreamEvent> {
            self.content_block
                .tool_start(event_index(self.index.0))
                .into_iter()
                .collect()
        }
    }

    /// `content_block_delta`: dispatch by `delta.type` to text / tool arguments /
    /// thinking.
    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct ContentBlockDeltaEvent {
        index: Lenient<u64>,
        delta: Lenient<DeltaIn>,
    }

    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct DeltaIn {
        #[serde(rename = "type")]
        kind: Lenient<Option<String>>,
        text: Lenient<Option<String>>,
        partial_json: Lenient<Option<String>>,
        thinking: Lenient<Option<String>>,
    }

    impl ContentBlockDeltaEvent {
        /// `whole` is the original event JSON, kept for the `Raw` fallback.
        fn into_events(self, whole: Value) -> Vec<StreamEvent> {
            let index = event_index(self.index.0);
            let delta = self.delta.0;
            match delta.kind.0.as_deref() {
                Some("text_delta") => vec![StreamEvent::TextDelta {
                    index,
                    text: delta.text.0.unwrap_or_default(),
                }],
                // index must come from the event itself: the old implementation hard-coded
                // 0, so parallel tool calls overwrote each other.
                Some("input_json_delta") => vec![StreamEvent::ToolCallDelta {
                    index,
                    arguments_delta: delta.partial_json.0.unwrap_or_default(),
                }],
                Some("thinking_delta") => vec![StreamEvent::ThinkingDelta {
                    index,
                    text: delta.thinking.0.unwrap_or_default(),
                }],
                // signature_delta / citations_delta and future delta kinds are not modeled
                // by the IR, but must not vanish: keep the whole event as Raw.
                _ => vec![StreamEvent::Raw { data: whole }],
            }
        }
    }

    /// `message_delta`: stop reason + completion-side usage.
    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct MessageDeltaEvent {
        delta: Lenient<StopDelta>,
        usage: Lenient<UsageIn>,
    }

    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct StopDelta {
        stop_reason: Lenient<Option<String>>,
    }

    impl MessageDeltaEvent {
        fn into_events(self) -> Vec<StreamEvent> {
            let mut out = Vec::new();
            if let Some(reason) = self.delta.0.stop_reason.0 {
                out.push(StreamEvent::MessageStop {
                    reason: claude_stop_reason(&reason),
                });
            }
            let usage = self.usage.0.into_usage();
            if usage.prompt_tokens > 0 || usage.completion_tokens > 0 {
                out.push(StreamEvent::MessageDelta { usage: Some(usage) });
            }
            out
        }
    }

    /// `error` event → IR `Error`.
    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct StreamErrorEvent {
        error: Lenient<ErrorBody>,
    }

    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct ErrorBody {
        message: Lenient<Option<String>>,
    }

    impl StreamErrorEvent {
        fn into_events(self) -> Vec<StreamEvent> {
            vec![StreamEvent::Error {
                message: self
                    .error
                    .0
                    .message
                    .0
                    .unwrap_or_else(|| "claude stream error".to_string()),
            }]
        }
    }

    // ---- encode models ----

    /// IR → Claude request body. `skip_serializing_if` reproduces the old "insert
    /// only when present" behavior; unmodeled fields are merged into the emitted
    /// map afterwards and only fill missing keys, so the modeled fields above
    /// always win.
    #[derive(Debug, Serialize)]
    pub(super) struct RequestOut<'a> {
        model: &'a str,
        #[serde(skip_serializing_if = "Vec::is_empty")]
        system: Vec<BlockOut<'a>>,
        messages: Vec<MessageOut<'a>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        tools: Option<Vec<ToolOut<'a>>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        tool_choice: Option<Value>,
        /// Claude's required `max_tokens`: defaulted when the IR carries none,
        /// otherwise the upstream answers 400 outright.
        max_tokens: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        temperature: Option<f64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        top_p: Option<f64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        stop_sequences: Option<Value>,
        stream: bool,
    }

    impl<'a> RequestOut<'a> {
        pub(super) fn from_ir(req: &'a LlmRequest) -> Self {
            let sampling = req.sampling.as_ref();
            Self {
                model: &req.model,
                system: req.system.iter().map(|b| BlockOut::text(&b.text)).collect(),
                messages: req.messages.iter().map(MessageOut::from_ir).collect(),
                tools: (!req.tools.is_empty())
                    .then(|| req.tools.iter().map(ToolOut::from_ir).collect()),
                tool_choice: req.tool_choice.as_ref().and_then(encode_tool_choice),
                max_tokens: sampling
                    .and_then(|s| s.max_tokens)
                    .unwrap_or(DEFAULT_MAX_TOKENS),
                temperature: sampling.and_then(|s| s.temperature),
                top_p: sampling.and_then(|s| s.top_p),
                // IR keeps the stop list in sampling.extra under OpenAI's name;
                // Claude calls it stop_sequences, and a lone string must be
                // wrapped in an array.
                stop_sequences: sampling
                    .and_then(|s| s.extra.get("stop"))
                    .map(|stop| match stop {
                        Value::String(s) => json!([s]),
                        other => other.clone(),
                    }),
                stream: req.stream,
            }
        }
    }

    /// IR → Claude message: role string + content block array (the array form is
    /// used even for a single text block).
    #[derive(Debug, Serialize)]
    pub(super) struct MessageOut<'a> {
        role: &'static str,
        content: Vec<BlockOut<'a>>,
    }

    impl<'a> MessageOut<'a> {
        fn from_ir(m: &'a IrMessage) -> Self {
            Self {
                role: claude_role(&m.role),
                content: m.content.iter().map(BlockOut::from_ir).collect(),
            }
        }
    }

    /// One content block in the Claude wire shape. Each arm declares its key set
    /// exactly once; `Thinking` carries its text under `thinking` (request and
    /// response sides both use that name) and `Refusal` has no Claude shape, so it
    /// degrades to a text block instead of losing the whole message.
    #[derive(Debug, Serialize)]
    #[serde(untagged)]
    pub(super) enum BlockOut<'a> {
        Text(TextOut<'a>),
        ToolUse(ToolUseOut<'a>),
        ToolResult(ToolResultOut<'a>),
        Thinking(ThinkingOut<'a>),
        Image(ImageOut<'a>),
        /// The escape hatch: the preserved original, re-emitted verbatim.
        Raw(Value),
    }

    #[derive(Debug, Serialize)]
    pub(super) struct TextOut<'a> {
        #[serde(rename = "type")]
        kind: &'static str,
        text: &'a str,
    }

    #[derive(Debug, Serialize)]
    pub(super) struct ToolUseOut<'a> {
        #[serde(rename = "type")]
        kind: &'static str,
        id: &'a str,
        name: &'a str,
        input: &'a Value,
    }

    #[derive(Debug, Serialize)]
    pub(super) struct ToolResultOut<'a> {
        #[serde(rename = "type")]
        kind: &'static str,
        tool_use_id: &'a str,
        content: &'a str,
    }

    #[derive(Debug, Serialize)]
    pub(super) struct ThinkingOut<'a> {
        #[serde(rename = "type")]
        kind: &'static str,
        thinking: &'a str,
    }

    #[derive(Debug, Serialize)]
    pub(super) struct ImageOut<'a> {
        #[serde(rename = "type")]
        kind: &'static str,
        source: ImageSourceOut<'a>,
    }

    #[derive(Debug, Serialize)]
    pub(super) struct ImageSourceOut<'a> {
        #[serde(rename = "type")]
        kind: &'static str,
        media_type: &'a str,
        data: &'a str,
    }

    impl<'a> BlockOut<'a> {
        /// Text block shape, shared by content blocks and the system slot.
        fn text(text: &'a str) -> Self {
            Self::Text(TextOut { kind: "text", text })
        }

        fn from_ir(block: &'a ContentBlock) -> Self {
            match block {
                ContentBlock::Text { text } | ContentBlock::Refusal { text } => {
                    BlockOut::text(text)
                }
                ContentBlock::ToolUse { id, name, input } => BlockOut::ToolUse(ToolUseOut {
                    kind: "tool_use",
                    id,
                    name,
                    input,
                }),
                ContentBlock::ToolResult {
                    tool_use_id,
                    content,
                } => BlockOut::ToolResult(ToolResultOut {
                    kind: "tool_result",
                    tool_use_id,
                    content,
                }),
                ContentBlock::Thinking { text } => BlockOut::Thinking(ThinkingOut {
                    kind: "thinking",
                    thinking: text,
                }),
                ContentBlock::Image { media_type, data } => BlockOut::Image(ImageOut {
                    kind: "image",
                    source: ImageSourceOut {
                        kind: "base64",
                        media_type,
                        data,
                    },
                }),
                // Claude has no refusal content block; degrade to text so the
                // whole message is not lost.
                ContentBlock::Unknown { raw } => BlockOut::Raw(raw.clone()),
            }
        }
    }

    /// IR tool definition → Claude `tools[]` entry (flat shape; `ToolDef::extra`
    /// is deliberately not emitted, matching the old encoder).
    #[derive(Debug, Serialize)]
    pub(super) struct ToolOut<'a> {
        name: &'a str,
        input_schema: &'a Value,
        #[serde(skip_serializing_if = "Option::is_none")]
        description: Option<&'a str>,
    }

    impl<'a> ToolOut<'a> {
        fn from_ir(t: &'a ToolDef) -> Self {
            Self {
                name: &t.name,
                input_schema: &t.input_schema,
                description: t.description.as_deref(),
            }
        }
    }

    /// IR → Claude non-streaming response body. The `usage` object is built by
    /// the module-level `encode_usage` — the same builder the stream encoder's
    /// `message_delta` frames use.
    #[derive(Debug, Serialize)]
    pub(super) struct ResponseOut<'a> {
        id: &'a str,
        #[serde(rename = "type")]
        kind: &'static str,
        role: &'static str,
        model: &'a str,
        content: Vec<BlockOut<'a>>,
        stop_reason: String,
        usage: Value,
    }

    impl<'a> ResponseOut<'a> {
        pub(super) fn from_ir(resp: &'a LlmResponse) -> Self {
            Self {
                id: &resp.id,
                kind: "message",
                role: "assistant",
                model: &resp.model,
                content: resp.outputs.iter().map(BlockOut::from_ir).collect(),
                stop_reason: claude_stop_reason_str(&resp.stop_reason),
                usage: encode_usage(&resp.usage),
            }
        }
    }
}
