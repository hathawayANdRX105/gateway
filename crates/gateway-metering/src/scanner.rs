//! Streaming token scanner — mounted in the `gateway_forward::stream` pipeline.
//!
//! usage semantics align with [`crate::usage::extract_usage`]: the same frame accepts
//! both `OpenAI` key names (`prompt_tokens`/`completion_tokens`) and Claude key names
//! (`input_tokens`/`output_tokens`), and all three cache locations are recognized;
//! fields merge across frames (last non-zero wins), and prompt/completion both zero
//! after merging means the upstream gave no real usage, so the estimate fallback runs
//! instead of passing `{0,0,0}` through as billing.

use bytes::Bytes;

use crate::usage::{frame_finish_reason, try_extract_usage};

/// Final counts at stream end.
#[derive(Debug, Clone, Copy, Default)]
pub struct TokenCounts {
    pub prompt: u64,
    pub completion: u64,
    pub cached: u64,
    /// Prompt tokens written to the cache (Claude `cache_creation_input_tokens`,
    /// `OpenAI` `prompt_tokens_details.cache_write_tokens`).
    pub cache_creation: u64,
    /// Whether `prompt` already contains `cached` (`OpenAI` `prompt_tokens`, Gemini
    /// `promptTokenCount`) rather than reporting cache tokens outside it (Claude
    /// `input_tokens`). `price_of` uses this to avoid charging the cached portion
    /// twice; the stored `usage_logs.prompt_tokens` keeps the vendor-native value.
    pub prompt_includes_cache: bool,
    /// Reasoning/thinking tokens, a **subset of `completion`** in every shape:
    /// OpenAI/Responses already include it in `completion_tokens`, and Gemini's
    /// `thoughtsTokenCount` is folded into `completion` when parsing
    /// `usageMetadata`. Diagnostic only — `price_of` prices `completion`, not this
    /// field.
    pub reasoning: u64,
}

impl TokenCounts {
    /// Merge per field across frames: a zero value means the frame did not provide that
    /// field, so an earlier non-zero value is kept.
    ///
    /// See [`StreamScanner::upstream_usage`] for the rationale: Claude's `input_tokens`
    /// appears only in the first frame, and whole-frame replacement would wipe the
    /// earlier value to 0.
    fn merge(&mut self, frame: Self) {
        if frame.prompt != 0 {
            self.prompt = frame.prompt;
        }
        if frame.completion != 0 {
            self.completion = frame.completion;
        }
        if frame.cached != 0 {
            self.cached = frame.cached;
        }
        if frame.cache_creation != 0 {
            self.cache_creation = frame.cache_creation;
        }
        if frame.reasoning != 0 {
            self.reasoning = frame.reasoning;
        }
        self.prompt_includes_cache |= frame.prompt_includes_cache;
    }

    /// Whether these counts carry real usage: at least one **primary** counter.
    ///
    /// Detail-only counts (cache/reasoning without prompt/completion) come from a
    /// malformed or truncated frame. Streaming and non-streaming extraction agree on
    /// this rule so both fall back to estimation instead of recording detail-only
    /// usage (never settle `{0,0,0}` or a cache-only row as real usage).
    pub(crate) fn has_primary_usage(self) -> bool {
        self.prompt != 0 || self.completion != 0
    }
}

#[derive(Default)]
pub struct StreamScanner {
    /// Usage merged per field across frames (the last non-zero value wins): Claude's
    /// `message_start` carries only `input_tokens` and appears once, while
    /// `message_delta`'s `output_tokens` is a cumulative value growing frame by frame
    /// (largest in the last frame). Whole-frame replacement would wipe the first frame's
    /// `input_tokens` to 0, so missing fields / zero values keep the earlier non-zero
    /// value.
    upstream_usage: Option<TokenCounts>,
    char_count: u64,
    line_buf: Vec<u8>,
    /// Finish reason captured from stream frames (later values override earlier ones; the
    /// last non-missing value wins): `OpenAI`'s last frame `choices[0].finish_reason` /
    /// Claude's `message_delta` frame `delta.stop_reason`. Values are taken only from
    /// parsed JSON frames, never by rescanning raw bytes; a stream cut mid-way or an
    /// upstream that gives no reason = None (the settlement column stays NULL, no value
    /// is invented).
    finish_reason: Option<String>,
}

impl StreamScanner {
    #[must_use]
    pub fn new() -> Self {
        Self {
            upstream_usage: None,
            char_count: 0,
            line_buf: Vec::new(),
            finish_reason: None,
        }
    }
    /// Whether the upstream reported usage in any stream frame (last non-zero
    /// merge wins); `None` when the upstream sent no usage at all.
    #[must_use]
    pub fn upstream_usage(&self) -> Option<TokenCounts> {
        self.upstream_usage
    }

    /// Ingest one stream chunk, scanning every complete line it closes.
    ///
    /// When no partial line is pending (the common case) the chunk bytes are scanned
    /// in place — no copy — and only the trailing incomplete line is stored in
    /// `line_buf`. When a partial line is pending, the chunk is appended to it once,
    /// the buffer is scanned once, and the consumed prefix is drained so the existing
    /// allocation is reused.
    ///
    /// Cost per call: O(chunk bytes) in the newline scan, plus one `scan_line` pass
    /// per line this call closes — for a line spanning chunks that pass covers its
    /// carried prefix too, but it is the line's first and only `scan_line`. Bytes
    /// carried in `line_buf` never get their newline scan redone; the scan resumes
    /// at the chunk boundary. Across a whole stream each byte is scanned exactly
    /// once per pass (amortized O(1) per byte), and an allocation happens only when
    /// a line spans chunks. This
    /// runs on every chunk of every streaming response, so it matters: the previous
    /// clone-and-rescan implementation was O(n^2) on a long line split across many
    /// chunks.
    pub fn push(&mut self, chunk: &Bytes) {
        if self.line_buf.is_empty() {
            let consumed = self.scan_complete_lines(chunk, 0);
            self.line_buf.extend_from_slice(&chunk[consumed..]);
            return;
        }

        // Take the pending buffer out so its bytes can be sliced while `scan_line`
        // holds a mutable borrow of `self`; draining the consumed prefix keeps the
        // allocation for the next partial line.
        let mut buf = std::mem::take(&mut self.line_buf);
        let pending = buf.len();
        buf.extend_from_slice(chunk);
        let consumed = self.scan_complete_lines(&buf, pending);
        buf.drain(..consumed);
        self.line_buf = buf;
    }

    /// Feed every `\n`-terminated line of `bytes` to [`Self::scan_line`] in order,
    /// and return the offset just past the last `\n` — the start of the trailing
    /// incomplete line (`bytes.len()` when `bytes` ends on a line boundary).
    ///
    /// `from` skips a prefix known to hold no `\n` (bytes scanned on earlier pushes):
    /// that prefix still opens the first line, so line starts remain correct from 0.
    /// Without it a long line spanning many chunks would rescan its carried prefix on
    /// every push — quadratic — which is exactly what the whole-stream bound in
    /// [`Self::push`] rules out.
    fn scan_complete_lines(&mut self, bytes: &[u8], from: usize) -> usize {
        let mut start = 0;
        for (i, &b) in bytes[from..].iter().enumerate() {
            if b == b'\n' {
                self.scan_line(&bytes[start..from + i]);
                start = from + i + 1;
            }
        }
        start
    }

    /// Process one complete line (stripping any trailing `\r`): only SSE payloads
    /// prefixed with `data: ` are recognized; `[DONE]` is skipped.
    fn scan_line(&mut self, line: &[u8]) {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        let Some(rest) = line.strip_prefix(b"data: ") else {
            return;
        };
        if rest.starts_with(b"[DONE]") {
            return;
        }
        self.char_count += rest.len() as u64;
        if let Some(frame) = try_extract_usage(rest) {
            self.upstream_usage.get_or_insert_default().merge(frame);
        }
        if let Some(reason) = frame_finish_reason(rest) {
            self.finish_reason = Some(reason);
        }
    }

    /// Finish scanning and return the final token counts plus the finish reason captured
    /// from the stream.
    ///
    /// `prompt` is the fallback estimate input when upstream usage is missing. Returns
    /// `(TokenCounts, Option<String>)`: the finish reason comes from the last non-missing
    /// value (see the [`Self::finish_reason`] field docs); an upstream that gives none =
    /// `None`.
    #[must_use]
    pub fn finish(self, prompt: u64) -> (TokenCounts, Option<String>) {
        // Still all-zero prompt/completion after merging = the upstream sent only empty
        // usage frames, equivalent to sending no usage; consistent with non-streaming
        // extract_usage's all-zero rule, use the estimate fallback. Never settle {0,0,0}
        // as real usage (Claude streaming once billed a whole request as 0 for this).
        let counts = match self.upstream_usage {
            Some(usage) if usage.has_primary_usage() => usage,
            _ => TokenCounts {
                prompt,
                completion: self.char_count / 4,
                cached: 0,
                cache_creation: 0,
                reasoning: 0,
                prompt_includes_cache: false,
            },
        };
        (counts, self.finish_reason)
    }
}
