//! `gateway-protocol-bridge` — data-plane protocol adaptation layer.
//!
//! Central home for vendor protocol compatibility. Mirrors new-api's
//! `relay/channel/*/adaptor.go`.
//!
//! ## Architecture: single-format bidirectional codecs with exactly two hops
//!
//! Each protocol implements **one** [`FormatCodec`] (its own format ↔ IR, both directions);
//! cross-format conversion is always two hops `src.decode → IR → dst.encode`. N formats need
//! only N codecs rather than N×(N-1) directed converters; adding a format means adding one
//! file.
//!
//! Streaming responses are encoded by one [`StreamEncoder`] per stream (`&mut self` holds
//! block boundaries and accumulated tool-fragment state), while the decode side
//! [`FormatCodec::decode_event`] stays stateless — Claude's `input_json_delta` to `OpenAI`'s
//! `tool_calls[].arguments` is a 1:1 per-event mapping; the side that genuinely needs
//! cross-event state is the encoder.
//!
//! ## Responsibilities
//!
//! - [`ir`] — provider-neutral intermediate representation (request / response / stream
//!   events) with an escape hatch for unknown fields
//! - [`format_codec`] — [`FormatCodec`] / [`StreamEncoder`] traits plus two-hop dispatch in
//!   [`FormatRegistry`]
//! - [`openai`] / [`claude`] / [`gemini`] / [`responses`] — single-format codecs for the four
//!   formats
//! - [`adaptor`] — shared vocabulary: [`Protocol`] format identifier and [`AdaptorError`]
//! - [`sse`] — SSE frame scanning (byte-exact passthrough + event boundaries + complete
//!   frames for conversion consumers)
//! - [`error_mapping`] — `gateway_core::error::NormalizedError` → per-protocol error shape
//! - [`stage`] — pipeline Stage 4: error mapping plus packaging the response prepared by
//!   forward into an HTTP response
//!
//! ## Boundaries with other crates
//!
//! | crate | role |
//! |-------|------|
//! | `gateway-forward` | IO orchestration + bidirectional conversion loop (request and response directions must share the same codec) |
//! | `gateway-protocol-bridge` | the conversion rules themselves (pure functions, no IO) |
//! | `gateway_core::error::NormalizedError` | single cross-crate error protocol |

pub mod adaptor;
pub mod claude;
pub mod error_mapping;
pub mod format_codec;
pub mod gemini;
pub mod ir;
pub mod openai;
pub mod responses;
mod responses_items;
mod responses_stream;
pub mod sse;
pub mod stage;

pub use adaptor::{AdaptorError, Protocol};
pub use error_mapping::map_error;
pub use format_codec::{FormatCodec, FormatRegistry, StreamEncoder};
pub use ir::{
    ContentBlock, LlmRequest, LlmResponse, Message, Role, SamplingParams, StopReason, StreamEvent,
    TextBlock, ToolChoice, ToolDef, Usage,
};
pub use stage::ProtocolBridgeStage;
