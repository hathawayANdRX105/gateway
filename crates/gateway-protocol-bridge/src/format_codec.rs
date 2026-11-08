//! Single-format bidirectional codec — the two-hop translation abstraction.
//!
//! Each format converts only to and from this crate's provider-neutral IR ([`ir`]); the registry
//! splits `src → dst` into the fixed two hops `src.decode → IR → dst.encode`.
//! Adding a format means implementing one trait, dropping the complexity from N×M to N+M.
//!
//! ## Stateless decode / stateful encode
//!
//! Per-event decode (e.g. Claude SSE `input_json_delta` → OpenAI `tool_calls[].arguments`) is a
//! 1:1 mapping and needs no cross-event state; the **encode side** is what truly needs per-stream
//! state — Claude/Responses output must add `content_block_start` / `output_item.added` boundary
//! frames per block index and accumulate IR `ToolCallDelta` fragments into complete JSON. Hence
//! only the [`StreamEncoder`] returned by [`FormatCodec::stream_encoder`] holds `&mut self` state,
//! while [`FormatCodec::decode_event`] stays stateless.
//!
//! Frame splitting is not the codec's job: `decode_event` consumes the data payload of one
//! complete SSE frame, while cross-chunk line reassembly belongs to the pipeline-layer shared
//! scanner (see [`sse`]).
//!
//! [`ir`]: crate::ir
//! [`sse`]: crate::sse

use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;

use crate::adaptor::{AdaptorError, Protocol};
use crate::ir::{LlmRequest, LlmResponse, StreamEvent};

/// A resolved pair of codecs: (src, dst).
type CodecPair = (Arc<dyn FormatCodec>, Arc<dyn FormatCodec>);

/// Single-format codec: bidirectional between this format ↔ IR. Adding a format = implementing
/// this one trait.
///
/// Every method has IR on the other side: `decode_*` parses this format's bytes into IR,
/// `encode_*` renders IR into this format's bytes. One trait per format covers both request and
/// response directions; the old split into separate request/response codecs is gone (it packed
/// direction into a `(source, target)` pair, and using a request-direction codec for responses was
/// the root of the 502 recorded in G1).
pub trait FormatCodec: Send + Sync {
    /// Protocol format handled by this codec.
    fn format(&self) -> Protocol;

    /// This format's request body → IR.
    fn decode_request(&self, body: Bytes) -> Result<LlmRequest, AdaptorError>;
    /// IR → this format's request body.
    fn encode_request(&self, req: &LlmRequest) -> Result<Bytes, AdaptorError>;

    /// This format's non-streaming response body → IR.
    fn decode_response(&self, body: Bytes) -> Result<LlmResponse, AdaptorError>;
    /// IR → this format's non-streaming response body.
    fn encode_response(&self, resp: &LlmResponse) -> Result<Bytes, AdaptorError>;

    /// One complete SSE frame's data payload → IR events (stateless: per-event 1:1 mapping).
    ///
    /// The input is a **single-frame** payload: the caller owns cross-chunk line reassembly (see
    /// the module docs).
    fn decode_event(&self, data: &str) -> Result<Vec<StreamEvent>, AdaptorError>;

    /// Create a stream encoder for this format (stateful: block boundary frames, tool-fragment
    /// accumulation).
    ///
    /// Called once per stream; the returned [`StreamEncoder`] holds its state behind `&mut self`
    /// for the stream's whole lifetime and is not shared across streams.
    fn stream_encoder(&self) -> Box<dyn StreamEncoder>;
}

/// One stream encoder per stream: `&mut self` holds the stream state (block index / tool-fragment
/// accumulation).
///
/// Lifetime: `encode_event` is called per event, `finish` once when the stream ends (to emit
/// block-end/closing frames). Calls for the same stream share state; two streams must each get
/// their own `stream_encoder()` so state never bleeds across.
pub trait StreamEncoder: Send {
    /// One IR event → zero or more SSE frame byte buffers in this format (zero when the state
    /// machine buffers).
    fn encode_event(&mut self, ev: &StreamEvent) -> Result<Vec<Bytes>, AdaptorError>;
    /// Stream end: flush buffered closing frames.
    fn finish(&mut self) -> Result<Vec<Bytes>, AdaptorError>;

    /// The upstream already sent an explicit terminal signal (OpenAI's `[DONE]` / Claude's
    /// `message_stop`).
    ///
    /// Called when the caller sees the raw terminal frame; `finish()` must not add another
    /// afterwards, or the client receives two stream terminations (OpenAI clients have undefined
    /// behavior for a duplicated `[DONE]`; in practice extra frames pollute byte-faithful
    /// assertions). Default is a no-op: formats that do not understand the terminal frame should
    /// not add one anyway.
    fn mark_done(&mut self) {}
}

/// Single-format registry: `Protocol → Arc<dyn FormatCodec>` (no more pair table).
///
/// [`Self::translate_request`] / [`Self::translate_response`] wrap `src → dst` as the fixed
/// `src.decode → IR → dst.encode` hops, so callers never see the decode/encode assembly.
pub struct FormatRegistry {
    codecs: HashMap<Protocol, Arc<dyn FormatCodec>>,
}

impl FormatRegistry {
    /// Empty registry.
    pub fn new() -> Self {
        Self {
            codecs: HashMap::new(),
        }
    }

    /// Load the four built-in format codecs.
    ///
    /// This is the entry point used by production wiring: any two formats can be converted through
    /// the two hops without a per-pair, per-direction converter.
    pub fn with_defaults() -> Self {
        let mut r = Self::new();
        r.register(Arc::new(crate::openai::OpenAiCodec::new()));
        r.register(Arc::new(crate::claude::ClaudeCodec::new()));
        r.register(Arc::new(crate::gemini::GeminiCodec::new()));
        r.register(Arc::new(crate::responses::ResponsesCodec::new()));
        r
    }

    /// Register a single-format codec; an earlier registration with the same `format()` is
    /// replaced.
    pub fn register(&mut self, codec: Arc<dyn FormatCodec>) {
        self.codecs.insert(codec.format(), codec);
    }

    /// Look up a format's codec; `None` = that format is not registered.
    pub fn resolve(&self, fmt: Protocol) -> Option<Arc<dyn FormatCodec>> {
        self.codecs.get(&fmt).cloned()
    }

    /// Two hops: src-format request body → IR → dst-format request body. When `src == dst` or
    /// either side is [`Protocol::Passthrough`], **pass through with zero conversion** (bytes are
    /// returned untouched, not parsed).
    pub fn translate_request(
        &self,
        src: Protocol,
        dst: Protocol,
        body: Bytes,
    ) -> Result<Bytes, AdaptorError> {
        if self.is_passthrough(src, dst) {
            return Ok(body);
        }
        let (src_codec, dst_codec) = self.resolve_pair(src, dst)?;
        let ir = src_codec.decode_request(body)?;
        dst_codec.encode_request(&ir)
    }

    /// Two hops: src-format response body → IR → dst-format response body. Pass-through rules
    /// match [`Self::translate_request`].
    pub fn translate_response(
        &self,
        src: Protocol,
        dst: Protocol,
        body: Bytes,
    ) -> Result<Bytes, AdaptorError> {
        if self.is_passthrough(src, dst) {
            return Ok(body);
        }
        let (src_codec, dst_codec) = self.resolve_pair(src, dst)?;
        let ir = src_codec.decode_response(body)?;
        dst_codec.encode_response(&ir)
    }

    /// Resolve the src and dst codecs; either one missing reports the same `NotRegistered`.
    fn resolve_pair(&self, src: Protocol, dst: Protocol) -> Result<CodecPair, AdaptorError> {
        let src_codec = self
            .resolve(src)
            .ok_or(AdaptorError::NotRegistered { from: src, to: dst })?;
        let dst_codec = self
            .resolve(dst)
            .ok_or(AdaptorError::NotRegistered { from: src, to: dst })?;
        Ok((src_codec, dst_codec))
    }

    /// Zero-conversion pass-through condition: same format, or either side explicitly declares
    /// passthrough.
    fn is_passthrough(&self, src: Protocol, dst: Protocol) -> bool {
        src == dst || src == Protocol::Passthrough || dst == Protocol::Passthrough
    }
}

impl Default for FormatRegistry {
    fn default() -> Self {
        Self::with_defaults()
    }
}
