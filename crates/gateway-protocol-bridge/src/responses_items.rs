//! `responses_items` — wire item DTOs shared by the Responses request, response
//! and stream sides (an internal part of the `responses` codec).
//!
//! One serde field per wire key (the crate's `wire` DTO pattern); hostile payloads
//! degrade through [`Lenient`](crate::ir::Lenient), and escape-hatch shapes capture
//! the ORIGINAL JSON first so a re-emit is verbatim, never a re-serialization.
//!
//! [`Lenient`]: crate::ir::Lenient

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Value, json};

use crate::ir::{ContentBlock, Lenient, Message, Present, Role, Usage};

/// `call_id` wins whenever the KEY EXISTS; the fallback to `id` fires only when
/// `call_id` is ABSENT — never when it is present-but-garbage (the old
/// `.get("call_id").or_else(|| get("id")).and_then(Value::as_str)` chain).
pub(crate) fn resolve_call_id(call_id: Present, fallback_id: Option<String>) -> String {
    match call_id.0 {
        Some(value) => value.as_str().unwrap_or_default().to_string(),
        None => fallback_id.unwrap_or_default(),
    }
}

/// `function_call` → `ToolUse`; `arguments` is a JSON string parsed leniently
/// (`{}` on any failure, including a non-string — models emit truncated JSON).
fn tool_use(
    call_id: Present,
    fallback_id: Option<String>,
    name: Option<String>,
    arguments: Option<String>,
) -> ContentBlock {
    ContentBlock::ToolUse {
        id: resolve_call_id(call_id, fallback_id),
        name: name.unwrap_or_default(),
        input: arguments
            .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
            .unwrap_or_else(|| json!({})),
    }
}

/// One `input[]` element in any of its three shapes (`function_call`,
/// `function_call_output`, or the message shape carrying a `role`).
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub(crate) struct InputItem {
    #[serde(rename = "type")]
    kind: Lenient<Option<String>>,
    role: Lenient<Option<String>>,
    content: Lenient<Option<InputContent>>,
    /// `function_call_output.output`: a string is kept as-is, any other JSON shape
    /// is stringified, only an ABSENT key yields the empty string.
    output: Present,
    call_id: Present,
    id: Lenient<Option<String>>,
    name: Lenient<Option<String>>,
    arguments: Lenient<Option<String>>,
}

/// `input` message content: a plain string or an array of parts; anything else is
/// no content at all (both untagged arms fail → the enclosing `Lenient` → `None`).
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum InputContent {
    Text(String),
    Parts(Vec<Lenient<InputPart>>),
}

/// One input content part; only a string `text` is modeled (the old side never
/// checked the part's `type` on the input side).
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct InputPart {
    text: Lenient<Option<String>>,
}

impl InputItem {
    /// Element → one IR message (the overall array yields one message per item).
    pub(crate) fn into_message(self) -> Message {
        let Self {
            kind,
            role,
            content,
            output,
            call_id,
            id,
            name,
            arguments,
        } = self;
        match kind.0.as_deref() {
            Some("function_call") => Message {
                role: Role::Assistant,
                content: vec![tool_use(call_id, id.0, name.0, arguments.0)],
            },
            Some("function_call_output") => Message {
                role: Role::Tool,
                content: vec![ContentBlock::ToolResult {
                    // No `id` fallback on this path: the old chain read `call_id` only.
                    tool_use_id: resolve_call_id(call_id, None),
                    content: match output.0 {
                        Some(Value::String(s)) => s,
                        Some(other) => other.to_string(),
                        None => String::new(),
                    },
                }],
            },
            // The message shape (the third input shape); items that are not objects
            // at all degrade here too, exactly like the old `_` arm.
            _ => Message {
                role: match role.0.as_deref() {
                    Some("assistant") => Role::Assistant,
                    Some("tool") => Role::Tool,
                    _ => Role::User,
                },
                content: match content.0 {
                    Some(InputContent::Text(text)) => vec![ContentBlock::Text { text }],
                    Some(InputContent::Parts(parts)) => parts
                        .into_iter()
                        // input_text, output_text and text all count as text:
                        // only the presence of a string `text` decides.
                        .filter_map(|p| p.0.text.0.map(|text| ContentBlock::Text { text }))
                        .collect(),
                    None => Vec::new(),
                },
            },
        }
    }
}

/// One response `output[]` item (`message` / `function_call` / `reasoning` /
/// unknown). The custom `Deserialize` captures the ORIGINAL JSON first so the
/// Unknown escape hatch re-emits verbatim.
#[derive(Debug, Default)]
pub(crate) struct OutputItem {
    kind: Option<String>,
    content: Vec<Lenient<OutputPart>>,
    summary: Vec<Lenient<SummaryPart>>,
    call_id: Present,
    id: Lenient<Option<String>>,
    name: Lenient<Option<String>>,
    arguments: Lenient<Option<String>>,
    raw: Value,
}

/// Typed view of an [`OutputItem`]: every field is hostile-input-proof, so parsing
/// fails only when the item is not JSON at all.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct OutputItemFields {
    #[serde(rename = "type")]
    kind: Lenient<Option<String>>,
    content: Lenient<Vec<Lenient<OutputPart>>>,
    summary: Lenient<Vec<Lenient<SummaryPart>>>,
    call_id: Present,
    id: Lenient<Option<String>>,
    name: Lenient<Option<String>>,
    arguments: Lenient<Option<String>>,
}

impl<'de> Deserialize<'de> for OutputItem {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let raw = Value::deserialize(d)?;
        let fields = serde_json::from_value::<OutputItemFields>(raw.clone()).unwrap_or_default();
        Ok(Self {
            kind: fields.kind.0,
            content: fields.content.0,
            summary: fields.summary.0,
            call_id: fields.call_id,
            id: fields.id,
            name: fields.name,
            arguments: fields.arguments,
            raw,
        })
    }
}

impl OutputItem {
    /// Scan predicate for the stop-reason rule (a `function_call` item wins).
    pub(crate) fn is_function_call(&self) -> bool {
        self.kind.as_deref() == Some("function_call")
    }

    /// Item → IR blocks in wire order (`LlmResponse::outputs`).
    pub(crate) fn into_blocks(self) -> Vec<ContentBlock> {
        let Self {
            kind,
            content,
            summary,
            call_id,
            id,
            name,
            arguments,
            raw,
        } = self;
        match kind.as_deref() {
            // Only an array content counts (old `as_array` gate): a garbage
            // `message` item yields no blocks and still does not take the hatch.
            Some("message") => content
                .into_iter()
                .filter_map(|p| p.0.into_text())
                .collect(),
            Some("function_call") => vec![tool_use(call_id, id.0, name.0, arguments.0)],
            // Summary text becomes Thinking; encrypted content is not modeled.
            Some("reasoning") => summary
                .into_iter()
                .filter_map(|s| s.0.into_thinking())
                .collect(),
            // Built-in tool calls and other unmodeled types survive via the escape
            // hatch with the original bytes.
            _ => vec![ContentBlock::Unknown { raw }],
        }
    }
}

/// One `message.content[]` part: text counts when `type` is absent, `output_text`
/// or `text` — a wrong-typed `type` counts too (the old `is_none_or` chain).
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct OutputPart {
    #[serde(rename = "type")]
    kind: Lenient<Option<String>>,
    text: Lenient<Option<String>>,
}

impl OutputPart {
    fn into_text(self) -> Option<ContentBlock> {
        let counts = self
            .kind
            .0
            .as_deref()
            .is_none_or(|k| k == "output_text" || k == "text");
        self.text
            .0
            .filter(|_| counts)
            .map(|text| ContentBlock::Text { text })
    }
}

/// One `reasoning.summary[]` entry; empty text is skipped (old contract).
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct SummaryPart {
    text: Lenient<String>,
}

impl SummaryPart {
    fn into_thinking(self) -> Option<ContentBlock> {
        let text = self.text.0;
        (!text.is_empty()).then_some(ContentBlock::Thinking { text })
    }
}

/// The Responses `usage` object: vendor counts are `input_tokens`/`output_tokens`
/// (not Chat's prompt/completion) with detail sub-objects; the IR normalizes.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub(crate) struct UsageIn {
    input_tokens: Lenient<u64>,
    output_tokens: Lenient<u64>,
    input_tokens_details: Lenient<UsageInputDetailsIn>,
    output_tokens_details: Lenient<UsageOutputDetailsIn>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct UsageInputDetailsIn {
    cached_tokens: Lenient<Option<u64>>,
    /// Cache-write tokens ride the OpenAI-compatible name (mirrors Claude's
    /// `cache_creation_input_tokens`).
    cache_write_tokens: Lenient<Option<u64>>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct UsageOutputDetailsIn {
    reasoning_tokens: Lenient<Option<u64>>,
}

impl UsageIn {
    pub(crate) fn into_usage(self) -> Usage {
        Usage {
            prompt_tokens: self.input_tokens.0,
            completion_tokens: self.output_tokens.0,
            cached_tokens: self.input_tokens_details.0.cached_tokens.0,
            cache_creation_tokens: self.input_tokens_details.0.cache_write_tokens.0,
            reasoning_tokens: self.output_tokens_details.0.reasoning_tokens.0,
        }
    }
}

/// IR usage → Responses `usage` object. `total_tokens` and
/// `input_tokens_details.cached_tokens` are ALWAYS emitted (cached at 0 default);
/// optional detail groups stay absent — key-for-key the old hand-built object.
#[derive(Debug, Serialize)]
pub(crate) struct UsageOut {
    input_tokens: u64,
    output_tokens: u64,
    total_tokens: u64,
    input_tokens_details: UsageInputDetailsOut,
    #[serde(skip_serializing_if = "Option::is_none")]
    output_tokens_details: Option<UsageReasoningOut>,
}

#[derive(Debug, Serialize)]
struct UsageInputDetailsOut {
    cached_tokens: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    cache_write_tokens: Option<u64>,
}

#[derive(Debug, Serialize)]
struct UsageReasoningOut {
    reasoning_tokens: u64,
}

impl UsageOut {
    pub(crate) fn from_ir(u: &Usage) -> Self {
        Self {
            input_tokens: u.prompt_tokens,
            output_tokens: u.completion_tokens,
            total_tokens: u.prompt_tokens + u.completion_tokens,
            input_tokens_details: UsageInputDetailsOut {
                cached_tokens: u.cached_tokens.unwrap_or(0),
                cache_write_tokens: u.cache_creation_tokens,
            },
            output_tokens_details: u
                .reasoning_tokens
                .map(|reasoning_tokens| UsageReasoningOut { reasoning_tokens }),
        }
    }
}

/// IR message → Responses input element (encode side). Tool results are standalone
/// `function_call_output` items; everything else a `message` with `input_text`.
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub(crate) enum InputItemOut<'a> {
    ToolResult(ToolResultItemOut<'a>),
    Message(MessageItemOut),
}

#[derive(Debug, Serialize)]
pub(crate) struct ToolResultItemOut<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    call_id: &'a str,
    output: &'a str,
}

#[derive(Debug, Serialize)]
pub(crate) struct MessageItemOut {
    #[serde(rename = "type")]
    kind: &'static str,
    role: &'static str,
    content: Vec<InputTextPartOut>,
}

#[derive(Debug, Serialize)]
struct InputTextPartOut {
    #[serde(rename = "type")]
    kind: &'static str,
    text: String,
}

impl<'a> InputItemOut<'a> {
    pub(crate) fn from_ir(m: &'a Message) -> Self {
        // Tool results are standalone items in Responses, not message content.
        if m.role == Role::Tool
            && let Some(ContentBlock::ToolResult {
                tool_use_id,
                content,
            }) = m.content.first()
        {
            return Self::ToolResult(ToolResultItemOut {
                kind: "function_call_output",
                call_id: tool_use_id,
                output: content,
            });
        }
        let role = match m.role {
            Role::Assistant => "assistant",
            // A Tool message without a leading tool result still travels as user
            // text (old mapping).
            Role::Tool | Role::User => "user",
        };
        let text = m
            .content
            .iter()
            .filter_map(|b| match b {
                ContentBlock::Text { text } => Some(text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("");
        Self::Message(MessageItemOut {
            kind: "message",
            role,
            content: vec![InputTextPartOut {
                kind: "input_text",
                text,
            }],
        })
    }
}
