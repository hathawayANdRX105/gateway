//! `responses_stream` — stateful Responses stream encoder (`responses` codec internals).
//!
//! Responses is an event-lifecycle protocol (`response.created` → `output_item.added` →
//! `*_delta` → `*_done` → `response.completed`), while the IR event stream has no such
//! boundaries; this encoder fills them in from state. It is used only by `stream_encoder`
//! in [`crate::responses`].

use bytes::Bytes;
use serde_json::{Value, json};
use std::collections::BTreeMap;

use crate::adaptor::AdaptorError;
use crate::format_codec::StreamEncoder;
use crate::ir::{StopReason, StreamEvent};
use crate::sse::event_frame;

/// Encoding state of one opened tool item.
///
/// `output_index` is **this format's item ordinal** (shared with the text item's
/// auto-increment sequence), not the IR event index — the two are different namespaces,
/// and mixing them directly would make text and tool items collide on numbers.
#[derive(Debug, Default, Clone)]
struct ToolState {
    opened: bool,
    item_id: String,
    call_id: String,
    name: String,
    arguments: String,
    output_index: u32,
}

/// The Responses stream encoder.
///
/// Responses is an **event-lifecycle** protocol: client parsers assume `response.created`
/// arrives first, every output item gets `output_item.added` before its deltas, and the
/// stream closes with `response.completed` (or `response.incomplete`). The IR event stream
/// has no such boundaries, so this encoder fills them in — the direct reason the encoding
/// side needs state (which items are open and how far arguments accumulated).
pub(crate) struct ResponsesStreamEncoder {
    id: String,
    model: String,
    created_sent: bool,
    text_index: Option<u32>,
    text: String,
    next_index: u32,
    tools: BTreeMap<u32, ToolState>,
    finished: bool,
}

impl ResponsesStreamEncoder {
    pub(crate) fn new() -> Self {
        Self {
            id: format!("resp_{}", uuid::Uuid::new_v4().simple()),
            model: String::new(),
            created_sent: false,
            text_index: None,
            text: String::new(),
            next_index: 0,
            tools: BTreeMap::new(),
            finished: false,
        }
    }

    /// Snapshot of the whole response (carried by both `response.created` and
    /// `response.completed`).
    // json! moves `output` into the serialized response body, so the argument is
    // genuinely consumed — borrowing it would not compile against the opaque macro.
    #[allow(clippy::needless_pass_by_value)]
    fn response_snapshot(&self, status: &str, output: Vec<Value>) -> Value {
        json!({
            "id": self.id,
            "object": "response",
            "created_at": 0,
            "status": status,
            "model": self.model,
            "output": output,
            "parallel_tool_calls": true,
        })
    }

    /// Ensure `response.created` has been sent (sent only once).
    fn ensure_created(&mut self) -> Vec<Bytes> {
        if self.created_sent {
            return Vec::new();
        }
        self.created_sent = true;
        vec![event_frame(
            "response.created",
            &json!({
                "type": "response.created",
                "response": self.response_snapshot("in_progress", Vec::new()),
            }),
        )]
    }

    /// Ensure the text item and its content part are open, returning (frames to send, the
    /// text item's `output_index`).
    fn ensure_text_item(&mut self) -> (Vec<Bytes>, u32) {
        if let Some(index) = self.text_index {
            return (Vec::new(), index);
        }
        let out = self.ensure_created();
        let index = self.next_index;
        self.next_index += 1;
        self.text_index = Some(index);

        let item_id = self.message_item_id(index);
        let mut frames = out;
        frames.push(event_frame(
            "response.output_item.added",
            &json!({
                "type": "response.output_item.added",
                "output_index": index,
                "item": {
                    "type": "message",
                    "id": item_id,
                    "role": "assistant",
                    "status": "in_progress",
                    "content": [],
                },
            }),
        ));
        frames.push(event_frame(
            "response.content_part.added",
            &json!({
                "type": "response.content_part.added",
                "item_id": item_id,
                "output_index": index,
                "content_index": 0,
                "part": {"type": "output_text", "text": "", "annotations": []},
            }),
        ));
        (frames, index)
    }

    fn message_item_id(&self, index: u32) -> String {
        format!("msg_{}_{index}", self.id)
    }

    fn tool_item_id(&self, index: u32) -> String {
        format!("fc_{}_{index}", self.id)
    }

    /// Get or create the tool item for an IR index, returning (frames to send, the entry).
    ///
    /// `call_id` / `name` only seed a newly created entry; when `update_identity` is true
    /// (`ToolCallStart`) they also fill in an existing entry's identity from non-empty
    /// inputs — a `ToolCallDelta` that arrives first must not overwrite the real identity
    /// with a placeholder id, so it passes false.
    fn ensure_tool_item(
        &mut self,
        index: u32,
        call_id: &str,
        name: &str,
        update_identity: bool,
    ) -> (Vec<Bytes>, &mut ToolState) {
        let mut out = self.ensure_created();
        // An existing entry keeps its assigned output_index; a new entry takes the next
        // value from the shared counter.
        let known_index = self.tools.get(&index).map(|state| state.output_index);
        let output_index = known_index.unwrap_or_else(|| {
            let allocated = self.next_index;
            self.next_index += 1;
            allocated
        });
        let item_id = self.tool_item_id(output_index);
        let entry = self.tools.entry(index).or_insert_with(|| ToolState {
            opened: false,
            item_id,
            call_id: call_id.to_string(),
            name: name.to_string(),
            arguments: String::new(),
            output_index,
        });
        // Later events may fill in more complete information.
        if update_identity && !call_id.is_empty() {
            entry.call_id = call_id.to_string();
        }
        if update_identity && !name.is_empty() {
            entry.name = name.to_string();
        }
        if !entry.opened {
            entry.opened = true;
            out.push(Self::tool_item_added_frame(entry));
        }
        (out, entry)
    }

    /// `response.output_item.added` (`function_call`, status `in_progress`).
    fn tool_item_added_frame(entry: &ToolState) -> Bytes {
        event_frame(
            "response.output_item.added",
            &json!({
                "type": "response.output_item.added",
                "output_index": entry.output_index,
                "item": {
                    "type": "function_call",
                    "id": entry.item_id,
                    "call_id": entry.call_id,
                    "name": entry.name,
                    "arguments": "",
                    "status": "in_progress",
                },
            }),
        )
    }

    /// The text item's closing trio: text.done → `content_part.done` → `output_item.done`.
    ///
    /// Returns (frames, item JSON reused by the final snapshot).
    fn close_text_item(&self, index: u32) -> (Vec<Bytes>, Value) {
        let item_id = self.message_item_id(index);
        let item = json!({
            "type": "message",
            "id": item_id,
            "role": "assistant",
            "status": "completed",
            "content": [{"type": "output_text", "text": self.text, "annotations": []}],
        });
        let frames = vec![
            event_frame(
                "response.output_text.done",
                &json!({
                    "type": "response.output_text.done",
                    "item_id": item_id,
                    "output_index": index,
                    "content_index": 0,
                    "text": self.text,
                }),
            ),
            event_frame(
                "response.content_part.done",
                &json!({
                    "type": "response.content_part.done",
                    "item_id": item_id,
                    "output_index": index,
                    "content_index": 0,
                    "part": {"type": "output_text", "text": self.text, "annotations": []},
                }),
            ),
            event_frame(
                "response.output_item.done",
                &json!({
                    "type": "response.output_item.done",
                    "output_index": index,
                    "item": item,
                }),
            ),
        ];
        (frames, item)
    }

    /// Closing frames for a tool item: arguments.done + `output_item.done`.
    fn close_tool_item(index: u32, state: &ToolState) -> (Vec<Bytes>, Value) {
        let item = json!({
            "type": "function_call",
            "call_id": state.call_id,
            "name": state.name,
            "arguments": state.arguments,
            "status": "completed",
        });
        let frames = vec![
            event_frame(
                "response.function_call_arguments.done",
                &json!({
                    "type": "response.function_call_arguments.done",
                    "item_id": state.item_id,
                    "output_index": index,
                    "name": state.name,
                    "arguments": state.arguments,
                }),
            ),
            event_frame(
                "response.output_item.done",
                &json!({
                    "type": "response.output_item.done",
                    "output_index": index,
                    "item": item,
                }),
            ),
        ];
        (frames, item)
    }

    /// Closing event: `response.completed`, or `response.incomplete` on truncation.
    fn completion_frame(&self, reason: &StopReason, items: Vec<Value>) -> Bytes {
        // Truncation and tool calls are different stop reasons; clients use them to decide
        // whether to continue.
        let (event_type, status) = if *reason == StopReason::Length {
            ("response.incomplete", "incomplete")
        } else {
            ("response.completed", "completed")
        };
        let mut payload = json!({
            "type": event_type,
            "response": self.response_snapshot(status, items),
        });
        if status == "incomplete" {
            payload["response"]["incomplete_details"] = json!({"reason": "max_output_tokens"});
        }
        event_frame(event_type, &payload)
    }

    /// `response.failed`: failure is terminal; it also sets `finished` so a later `finish()`
    /// emits no success events.
    fn failure_frames(&mut self, message: &str) -> Vec<Bytes> {
        let mut out = self.ensure_created();
        out.push(event_frame(
            "response.failed",
            &json!({
                "type": "response.failed",
                "response": {
                    "id": self.id,
                    "status": "failed",
                    "error": {"code": "server_error", "message": message},
                },
            }),
        ));
        self.finished = true;
        out
    }

    /// Finish: close opened items and emit `response.completed` / `response.incomplete`.
    fn finish_stream(&mut self, reason: &StopReason) -> Vec<Bytes> {
        if self.finished {
            return Vec::new();
        }
        self.finished = true;

        let mut out = self.ensure_created();
        let mut output = Vec::new();

        // Text item: complete the closing trio.
        if let Some(index) = self.text_index {
            let (frames, item) = self.close_text_item(index);
            out.extend(frames);
            output.push((index, item));
        }

        // Tool items: complete arguments.done + output_item.done.
        let tool_indices: Vec<u32> = self.tools.keys().copied().collect();
        for index in tool_indices {
            let Some(state) = self.tools.get(&index) else {
                continue;
            };
            if !state.opened {
                continue;
            }
            let (frames, item) = Self::close_tool_item(index, state);
            out.extend(frames);
            output.push((index, item));
        }

        output.sort_by_key(|(i, _)| *i);
        let items: Vec<Value> = output.into_iter().map(|(_, v)| v).collect();
        out.push(self.completion_frame(reason, items));
        out
    }
}

impl StreamEncoder for ResponsesStreamEncoder {
    fn encode_event(&mut self, ev: &StreamEvent) -> Result<Vec<Bytes>, AdaptorError> {
        match ev {
            StreamEvent::MessageStart { id, model } => {
                if !id.is_empty() {
                    self.id.clone_from(id);
                }
                self.model.clone_from(model);
                Ok(self.ensure_created())
            }
            StreamEvent::TextDelta { text, .. } => {
                let (mut out, index) = self.ensure_text_item();
                self.text.push_str(text);
                out.push(event_frame(
                    "response.output_text.delta",
                    &json!({
                        "type": "response.output_text.delta",
                        "item_id": self.message_item_id(index),
                        "output_index": index,
                        "content_index": 0,
                        "delta": text,
                    }),
                ));
                Ok(out)
            }
            StreamEvent::ThinkingDelta { .. } => {
                // The Responses reasoning item has a heavy shape (summary part + encrypted
                // content), and the IR-side ThinkingDelta is not enough to reconstruct it;
                // this opens no item and only ensures `created` was sent, so clients never
                // see a half-built item that fails to parse.
                Ok(self.ensure_created())
            }
            StreamEvent::ToolCallStart { index, id, name } => {
                let (out, _entry) = self.ensure_tool_item(*index, id, name, true);
                Ok(out)
            }
            StreamEvent::ToolCallDelta {
                index,
                arguments_delta,
            } => {
                // An item must open even without ToolCallStart: upstream may send only
                // arguments (same shape as the Claude side).
                let (mut out, entry) =
                    self.ensure_tool_item(*index, &format!("call_{index}"), "", false);
                entry.arguments.push_str(arguments_delta);
                out.push(event_frame(
                    "response.function_call_arguments.delta",
                    &json!({
                        "type": "response.function_call_arguments.delta",
                        "item_id": entry.item_id,
                        "output_index": entry.output_index,
                        "delta": arguments_delta,
                    }),
                ));
                Ok(out)
            }
            // Responses has no standalone usage event; usage appears only in the final
            // response snapshot. This encoder's snapshot has no usage field, so no frame is
            // emitted here (keeping the existing output shape).
            StreamEvent::MessageDelta { .. } | StreamEvent::Raw { .. } => Ok(Vec::new()),
            StreamEvent::MessageStop { reason } => Ok(self.finish_stream(reason)),
            // Raw vendor events have no Responses representation; ignore explicitly.
            StreamEvent::Error { message } => Ok(self.failure_frames(message)),
        }
    }

    fn finish(&mut self) -> Result<Vec<Bytes>, AdaptorError> {
        if self.finished {
            return Ok(Vec::new());
        }
        Ok(self.finish_stream(&StopReason::Stop))
    }
}
