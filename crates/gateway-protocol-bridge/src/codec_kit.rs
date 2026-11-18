//! Shared serde toolkit every format codec compiles against.
//!
//! Lenient decoding (`Lenient`, `lenient_from_value`, `from_event`, `event_index`),
//! key-presence wrappers (`Present`), and escape-hatch serialization
//! (`decode_body`, `encode_value`, `encode_object`, `struct_to_map`, `wire_value`),
//! plus field-mapping helpers (`decode_text_blocks`, `sampling_params`,
//! `apply_extra_fields`, `apply_extra_map`).
//!
//! This is crate-internal and deliberately separate from the IR data model in
//! [`crate::ir`]: the model file stays a readable type catalogue of `Role`,
//! `ContentBlock`, `LlmRequest` and friends, while every codec (claude, gemini,
//! openai, responses) compiles against the same lenient/presence/escape-hatch
//! toolkit declared here.

use bytes::Bytes;
use serde_json::{Map, Value};

use crate::adaptor::AdaptorError;
use crate::ir::{LlmRequest, SamplingParams, TextBlock};

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
