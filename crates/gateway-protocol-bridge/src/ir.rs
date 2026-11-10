//! Provider-neutral intermediate representation (IR) — the hub of two-hop translation.
//!
//! All format codecs interconvert only with this module:
//! `decode(some format JSON) → IR → encode(another format JSON)`; N formats need N codecs
//! instead of N×M.
//!
//! ## Escape hatches (lossless round-trip)
//!
//! - Unknown block types are preserved as-is via [`ContentBlock::Unknown`] (custom serde:
//!   deserialization puts an unknown `type` wholesale into `raw`, and serialization emits `raw`
//!   as-is without re-tagging);
//! - Unknown top-level/nested fields are preserved via [`LlmRequest::extra`] /
//!   [`ToolDef::extra`] / [`SamplingParams::extra`] (`#[serde(flatten)]`).
//!
//! Images ([`ContentBlock::Image`](ContentBlock::Image)) are modeled; other multimodal blocks
//! such as audio/file are still unmodeled and always land in [`ContentBlock::Unknown`].

use bytes::Bytes;
use serde::de;
use serde::{Deserialize, Deserializer, Serialize, Serializer, ser::SerializeMap};
use serde_json::{Map, Value};

use crate::adaptor::AdaptorError;

/// Conversation role (untagged, serialized as `"user"` / `"assistant"` / `"tool"`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    User,
    Assistant,
    Tool,
}

/// System-prompt text block.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TextBlock {
    pub text: String,
}

/// Content block: a polymorphic enum distinguished by the `type` tag.
///
/// Known variants serialize to the `{"type":"text","text":"..."}` shape; unknown types are
/// preserved as-is via [`Self::Unknown`] (see the module docs).
#[derive(Debug, Clone, PartialEq)]
pub enum ContentBlock {
    Text {
        text: String,
    },
    ToolUse {
        id: String,
        name: String,
        input: Value,
    },
    ToolResult {
        tool_use_id: String,
        content: String,
    },
    Thinking {
        text: String,
    },
    Refusal {
        text: String,
    },
    /// Image content block. `data` is the **base64 payload** (without the
    /// `data:<mime>;base64,` prefix); `media_type` looks like `image/png`. Remote http(s) URLs
    /// cannot be expressed in this shape — such inputs still go through [`Self::Unknown`], and
    /// each codec decides for itself whether to degrade them.
    Image {
        media_type: String,
        data: String,
    },
    /// Escape hatch for unknown block types: `raw` keeps the whole original JSON, lossless on
    /// round-trip.
    Unknown {
        raw: Value,
    },
}

impl Serialize for ContentBlock {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            // Known variants use the tagged shape: {"type":"<snake_case>", ...}
            ContentBlock::Text { text } => {
                let mut m = serializer.serialize_map(None)?;
                m.serialize_entry("type", "text")?;
                m.serialize_entry("text", text)?;
                m.end()
            }
            ContentBlock::ToolUse { id, name, input } => {
                let mut m = serializer.serialize_map(None)?;
                m.serialize_entry("type", "tool_use")?;
                m.serialize_entry("id", id)?;
                m.serialize_entry("name", name)?;
                m.serialize_entry("input", input)?;
                m.end()
            }
            ContentBlock::ToolResult {
                tool_use_id,
                content,
            } => {
                let mut m = serializer.serialize_map(None)?;
                m.serialize_entry("type", "tool_result")?;
                m.serialize_entry("tool_use_id", tool_use_id)?;
                m.serialize_entry("content", content)?;
                m.end()
            }
            ContentBlock::Thinking { text } => {
                let mut m = serializer.serialize_map(None)?;
                m.serialize_entry("type", "thinking")?;
                m.serialize_entry("text", text)?;
                m.end()
            }
            ContentBlock::Refusal { text } => {
                let mut m = serializer.serialize_map(None)?;
                m.serialize_entry("type", "refusal")?;
                m.serialize_entry("text", text)?;
                m.end()
            }
            ContentBlock::Image { media_type, data } => {
                let mut m = serializer.serialize_map(None)?;
                m.serialize_entry("type", "image")?;
                m.serialize_entry("media_type", media_type)?;
                m.serialize_entry("data", data)?;
                m.end()
            }
            // Escape hatch: emit as-is without re-tagging — keeps unknown types lossless on
            // round-trip.
            ContentBlock::Unknown { raw } => raw.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for ContentBlock {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        /// Used only for tagged deserialization of known variants; unknown types fall back to
        /// `Unknown` outside.
        #[derive(Deserialize)]
        #[serde(tag = "type", rename_all = "snake_case")]
        enum Known {
            Text {
                text: String,
            },
            ToolUse {
                id: String,
                name: String,
                input: Value,
            },
            ToolResult {
                tool_use_id: String,
                content: String,
            },
            Thinking {
                text: String,
            },
            Refusal {
                text: String,
            },
            Image {
                media_type: String,
                data: String,
            },
        }

        impl From<Known> for ContentBlock {
            fn from(k: Known) -> Self {
                match k {
                    Known::Text { text } => ContentBlock::Text { text },
                    Known::ToolUse { id, name, input } => ContentBlock::ToolUse { id, name, input },
                    Known::ToolResult {
                        tool_use_id,
                        content,
                    } => ContentBlock::ToolResult {
                        tool_use_id,
                        content,
                    },
                    Known::Thinking { text } => ContentBlock::Thinking { text },
                    Known::Refusal { text } => ContentBlock::Refusal { text },
                    Known::Image { media_type, data } => ContentBlock::Image { media_type, data },
                }
            }
        }

        // ponytail: parse into a Value first, then dispatch — much shorter than a hand-written
        // Visitor;
        // an unknown type lands wholesale in Unknown, handed over without cloning.
        let value = Value::deserialize(deserializer)?;
        match value.get("type").and_then(Value::as_str) {
            Some("text" | "tool_use" | "tool_result" | "thinking" | "refusal" | "image") => {
                Known::deserialize(value)
                    .map(Into::into)
                    .map_err(de::Error::custom)
            }
            _ => Ok(ContentBlock::Unknown { raw: value }),
        }
    }
}

/// A single message.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Message {
    pub role: Role,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub content: Vec<ContentBlock>,
}

/// Tool definition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolDef {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub input_schema: Value,
    #[serde(default, flatten, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

/// Tool-choice strategy. `Raw` is the escape hatch for unknown shapes (e.g. vendor-specific
/// structures).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolChoice {
    Auto,
    None,
    Required,
    /// Pin a single tool.
    Specific(String),
    /// Unknown shapes are preserved as-is.
    Raw(Value),
}

/// Sampling parameters.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SamplingParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u64>,
    #[serde(default, flatten, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

/// Provider-neutral LLM request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LlmRequest {
    pub model: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub messages: Vec<Message>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub system: Vec<TextBlock>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<ToolDef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<ToolChoice>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sampling: Option<SamplingParams>,
    #[serde(default)]
    pub stream: bool,
    /// Escape hatch for unknown top-level fields; `flatten` takes and emits them verbatim.
    #[serde(default, flatten, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

/// Non-streaming response.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LlmResponse {
    pub id: String,
    pub model: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub outputs: Vec<ContentBlock>,
    pub usage: Usage,
    pub stop_reason: StopReason,
}

/// Stop reason: **all variants serialize to bare strings** (`"stop"` / `"length"` /
/// unclassified reasons verbatim).
///
/// `#[serde(tag=…)]` is deliberately not used: downstream codecs (claude/gemini/openai) consume
/// the reason as a string via `StopReason::Other(s) => s`. If `Other` serialized to
/// `{"other":"…"}`, it would inject an illegal shape into IR JSON.
#[derive(Debug, Clone, PartialEq)]
pub enum StopReason {
    Stop,
    Length,
    ContentFilter,
    ToolUse,
    Error,
    /// Unclassified vendor reason, preserved verbatim (bare string in both directions).
    Other(String),
}

impl Serialize for StopReason {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            StopReason::Stop => serializer.serialize_str("stop"),
            StopReason::Length => serializer.serialize_str("length"),
            StopReason::ContentFilter => serializer.serialize_str("content_filter"),
            StopReason::ToolUse => serializer.serialize_str("tool_use"),
            StopReason::Error => serializer.serialize_str("error"),
            StopReason::Other(s) => serializer.serialize_str(s),
        }
    }
}

impl<'de> Deserialize<'de> for StopReason {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "stop" => Ok(StopReason::Stop),
            "length" => Ok(StopReason::Length),
            "content_filter" => Ok(StopReason::ContentFilter),
            "tool_use" => Ok(StopReason::ToolUse),
            "error" => Ok(StopReason::Error),
            _ => Ok(StopReason::Other(s)),
        }
    }
}

/// Token usage — normalized to one accounting convention.
///
/// Quantities are provider-neutral counts, not prices: this struct deliberately
/// carries no cost/billing fields.
///
/// **Convention (subset semantics, matching OpenAI):**
/// - `prompt_tokens` / `completion_tokens` are **totals**;
/// - `cached_tokens`, `cache_creation_tokens` are **subsets of `prompt_tokens`**;
/// - `reasoning_tokens` is a **subset of `completion_tokens`**.
///
/// Each codec converts to and from its vendor's shape: Claude reports cache
/// tokens *outside* `input_tokens` and Gemini reports thinking *outside*
/// `candidatesTokenCount`, so both are added on decode and subtracted on encode;
/// `OpenAI` Chat / Responses already match the convention.
///
/// Without this normalization a cross-protocol hop misreported the numbers (a
/// Claude→OpenAI response showed `cached_tokens` exceeding `prompt_tokens`, and
/// an OpenAI→Claude response double-counted the cached portion).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Usage {
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    /// Cache-read tokens — a **subset of `prompt_tokens`** (Claude
    /// `cache_read_input_tokens`, `OpenAI` `prompt_tokens_details.cached_tokens`,
    /// Gemini `cachedContentTokenCount`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cached_tokens: Option<u64>,
    /// Cache-write tokens — also a **subset of `prompt_tokens`** (Claude
    /// `cache_creation_input_tokens`, `OpenAI`
    /// `prompt_tokens_details.cache_write_tokens`). Vendors bill cache writes
    /// separately from ordinary input, so losing this quantity under-reports usage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_creation_tokens: Option<u64>,
    /// Reasoning/thinking tokens already included in `completion_tokens`
    /// (`OpenAI` `completion_tokens_details.reasoning_tokens`, Gemini
    /// `thoughtsTokenCount`). Claude reports thinking inside `output_tokens`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_tokens: Option<u64>,
}

/// Streaming event (tagged: `{"type":"text_delta", ...}`).
///
/// Every content delta carries an `index` — the key to the G1 (tool-fragment assembly) fix:
/// codecs attribute fragments of the same block by index instead of guessing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamEvent {
    MessageStart {
        id: String,
        model: String,
    },
    TextDelta {
        index: u32,
        text: String,
    },
    ToolCallStart {
        index: u32,
        id: String,
        name: String,
    },
    ToolCallDelta {
        index: u32,
        arguments_delta: String,
    },
    ThinkingDelta {
        index: u32,
        text: String,
    },
    MessageDelta {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        usage: Option<Usage>,
    },
    MessageStop {
        reason: StopReason,
    },
    /// Opaque upstream event preserved verbatim: a vendor event type the IR does
    /// not model (for example Claude's `citations_delta` or a future delta kind).
    /// Encoders for the same protocol re-emit it unchanged; other protocols may
    /// ignore it rather than silently losing the whole event.
    Raw {
        data: Value,
    },
    Error {
        message: String,
    },
}

// ==== Serialization helpers: JSON boundary operations and field mapping shared by the four
// codecs ====

/// Decode a JSON body into `T` (the shared entry point for the four codecs'
/// `decode_request`/`decode_response`): parse failures always map to
/// [`AdaptorError::DecodeFailed`].
pub(crate) fn decode_body<T: serde::de::DeserializeOwned>(body: &[u8]) -> Result<T, AdaptorError> {
    serde_json::from_slice(body).map_err(|e| AdaptorError::DecodeFailed(e.to_string()))
}

/// Serialize any JSON value into bytes (the shared exit for the four codecs'
/// `encode_response`): serialization failures always map to [`AdaptorError::EncodeFailed`].
pub(crate) fn encode_value(value: &Value) -> Result<Bytes, AdaptorError> {
    serde_json::to_vec(value)
        .map(Bytes::from)
        .map_err(|e| AdaptorError::EncodeFailed(e.to_string()))
}

/// Serialize a `Map` object into JSON bytes (the shared exit for the four codecs'
/// `encode_request`).
pub(crate) fn encode_object(out: Map<String, Value>) -> Result<Bytes, AdaptorError> {
    encode_value(&Value::Object(out))
}

/// Serialize a wire struct into the object map that encode then extends with unmodeled
/// fields. A struct always serializes to an object; serialization failure maps to
/// [`AdaptorError::EncodeFailed`] like every other encode exit.
pub(crate) fn struct_to_map(
    value: &impl serde::Serialize,
) -> Result<Map<String, Value>, AdaptorError> {
    match serde_json::to_value(value) {
        Ok(Value::Object(map)) => Ok(map),
        Ok(_) => Ok(Map::new()),
        Err(e) => Err(AdaptorError::EncodeFailed(e.to_string())),
    }
}

/// Serialize a wire struct into a `Value` to embed in an enclosing body. Plain wire data
/// cannot fail serialization; the enclosing `encode_value`/`encode_object` is the real
/// error exit.
pub(crate) fn wire_value(value: &impl serde::Serialize) -> Value {
    serde_json::to_value(value).unwrap_or(Value::Null)
}

/// System-prompt slot (a string or a `{"text": ...}` part array) → IR text blocks.
///
/// An empty string produces no block (existing `OpenAI` / Responses semantics); Claude's `system`
/// allows an empty string as a block of its own, so that path does not use this helper.
pub(crate) fn decode_text_blocks(content: Option<&Value>) -> Vec<TextBlock> {
    match content {
        Some(Value::String(s)) if !s.is_empty() => vec![TextBlock { text: s.clone() }],
        Some(Value::Array(parts)) => parts
            .iter()
            .filter_map(|p| {
                p.get("text").and_then(Value::as_str).map(|t| TextBlock {
                    text: t.to_string(),
                })
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// Common sampling params + format-private extras → [`SamplingParams`]; returns `None` when all
/// are empty (IR uses `None` for "the request set no sampling params", distinct from "all
/// defaults").
pub(crate) fn sampling_params(
    temperature: Option<f64>,
    top_p: Option<f64>,
    max_tokens: Option<u64>,
    extra: Map<String, Value>,
) -> Option<SamplingParams> {
    if temperature.is_none() && top_p.is_none() && max_tokens.is_none() && extra.is_empty() {
        return None;
    }
    Some(SamplingParams {
        temperature,
        top_p,
        max_tokens,
        extra,
    })
}

/// Carry unmodeled request-side fields back verbatim: explicit client fields win and are not
/// overwritten.
pub(crate) fn apply_extra_fields(out: &mut Map<String, Value>, req: &LlmRequest) {
    apply_extra_map(out, &req.extra);
}

/// Add a set of unmodeled fields to `out`: existing keys keep their values, only missing keys are
/// added.
pub(crate) fn apply_extra_map(out: &mut Map<String, Value>, extra: &Map<String, Value>) {
    extra.iter().for_each(|(k, v)| {
        out.entry(k.clone()).or_insert_with(|| v.clone());
    });
}

/// Lenient field decoding shared by the wire DTO modules: any shape/type mismatch degrades to
/// `T::default()` instead of failing the parent structure.
///
/// This crate's decode contract has always been `unwrap_or_default` (a malformed field is
/// hostile input to shrug off, not a reason to 400 the request — models and vendors emit
/// partial garbage). A plain `#[derive(Deserialize)]` would hard-fail on the first type
/// mismatch, so wire DTOs wrap their fields in `Lenient` (or nest it, e.g.
/// `Vec<Lenient<T>>` to keep per-item leniency while never dropping or failing the array).
/// Routing the value through `serde_json::Value` costs one allocation per field — irrelevant
/// next to the body parse it rides on.
pub(crate) struct Lenient<T>(pub(crate) T);

impl<T: std::fmt::Debug> std::fmt::Debug for Lenient<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl<T: Default> Default for Lenient<T> {
    fn default() -> Self {
        Self(T::default())
    }
}

impl<'de, T> serde::Deserialize<'de> for Lenient<T>
where
    T: serde::de::DeserializeOwned + Default,
{
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = Value::deserialize(deserializer)?;
        Ok(Self(serde_json::from_value(raw).unwrap_or_default()))
    }
}

/// Leniently convert an already-parsed JSON value into a DTO: any shape/type mismatch
/// degrades to `T::default()` (the [`Lenient`] contract applied to a `Value` in hand).
pub(crate) fn lenient_from_value<T>(value: Value) -> T
where
    T: serde::de::DeserializeOwned + Default,
{
    serde_json::from_value::<Lenient<T>>(value)
        .unwrap_or_default()
        .0
}

/// One SSE event payload → lenient DTO under [`Lenient`] semantics. Never fails; the
/// caller keeps the original `Value` when it needs the `Raw` escape hatch.
pub(crate) fn from_event<T>(ev: &Value) -> T
where
    T: serde::de::DeserializeOwned + Default,
{
    lenient_from_value(ev.clone())
}

/// A `u64` wire block/output index → IR `u32`.
///
/// A missing or wrong-typed index reads 0 (the DTO field's `Lenient<u64>` default) and a
/// value past the `u32` range saturates instead of wrapping — the old
/// `try_into().unwrap_or(u32::MAX)` contract shared by the Claude and Responses decoders.
pub(crate) fn event_index(raw: u64) -> u32 {
    raw.try_into().unwrap_or(u32::MAX)
}

/// Presence wrapper: distinguishes "key absent" from "key present with an explicit JSON
/// null", which `Option<Value>` cannot express (both decode to `None`).
///
/// With `#[serde(default)]` on the holder, serde only calls this impl when the key
/// exists, so being called is the presence proof; `Value` accepts every shape including
/// null. Formats map the two cases apart — e.g. `tool_use.input` (absent → `{}`, null →
/// `null`), a present-but-null `thinking` blocking its `text` fallback, and
/// `tool_choice: null` staying `Raw(null)`.
#[derive(Debug, Default)]
pub(crate) struct Present(pub(crate) Option<Value>);

impl Present {
    /// The present value's string content — the old `.get(..).and_then(Value::as_str)`
    /// contract.
    pub(crate) fn as_str(&self) -> Option<&str> {
        self.0.as_ref().and_then(Value::as_str)
    }
}

impl<'de> serde::Deserialize<'de> for Present {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(Self(Some(Value::deserialize(deserializer)?)))
    }
}
