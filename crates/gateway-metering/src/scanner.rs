//! Streaming token scanner — mounted in the `gateway_forward::stream` pipeline.
//!
//! usage semantics align with [`crate::settle::extract_usage`]: the same frame accepts
//! both `OpenAI` key names (`prompt_tokens`/`completion_tokens`) and Claude key names
//! (`input_tokens`/`output_tokens`), and all three cache locations are recognized;
//! fields merge across frames (last non-zero wins), and prompt/completion both zero
//! after merging means the upstream gave no real usage, so the estimate fallback runs
//! instead of passing `{0,0,0}` through as billing.

use bytes::Bytes;
use serde_json::Value as JsonValue;

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

/// Frame-level `finish_reason` extraction (lenient style, as in [`try_extract_usage`]):
/// `OpenAI` streaming's last frame `choices[0].finish_reason` (null in intermediate frames
/// is filtered out naturally by `as_str()`), Claude streaming's `message_delta` frame
/// `delta.stop_reason`; a top-level `stop_reason` covers the non-streaming Claude shape.
/// Non-JSON / missing field / non-string / empty string → None.
fn frame_finish_reason(data: &[u8]) -> Option<String> {
    let s = std::str::from_utf8(data).ok()?;
    let v: JsonValue = serde_json::from_str(s).ok()?;
    choice_finish_reason(&v)
        .or_else(|| delta_stop_reason(&v))
        .or_else(|| gemini_finish_reason(&v))
        .or_else(|| top_stop_reason(&v))
        .map(ToString::to_string)
}

/// Gemini `candidates[0].finishReason` (e.g. STOP / `MAX_TOKENS`); non-string / empty
/// string → None.
pub(crate) fn gemini_finish_reason(v: &JsonValue) -> Option<&str> {
    non_empty_str(
        v.get("candidates")
            .and_then(|c| c.get(0))
            .and_then(|c| c.get("finishReason")),
    )
}

/// Frame-level usage extraction (lenient style, as in [`frame_finish_reason`]):
/// - the usage object is at the top level (`OpenAI` / `DeepSeek` last frame, Claude
///   `message_delta`) or nested under `message.usage` (Claude `message_start`);
/// - non-JSON / no usage object / all zero → None (all zero = the frame gives no real
///   usage, so no `{0,0,0}` record is produced).
fn try_extract_usage(data: &[u8]) -> Option<TokenCounts> {
    let s = std::str::from_utf8(data).ok()?;
    let v: JsonValue = serde_json::from_str(s).ok()?;
    let usage = v
        .get("usage")
        .or_else(|| v.get("message").and_then(|m| m.get("usage")))
        // Gemini puts the usage object at `usageMetadata`.
        .or_else(|| v.get("usageMetadata"))?;
    let counts = counts_from_usage(usage);
    counts.has_primary_usage().then_some(counts)
}

/// Parse a `usage` object into counts: both `OpenAI` and Claude token key sets coexist and
/// the first present value wins; all cache locations are recognized
/// (`prompt_tokens_details.cached_tokens` / top-level `cached_tokens` /
/// `cache_read_input_tokens` / `cachedContentTokenCount`).
///
/// Completion convention: the vendor's primary counter, **with Gemini thinking folded
/// in**. Gemini reports `thoughtsTokenCount` outside `candidatesTokenCount`, and thinking
/// is billed at the output price, so `completion = candidates + thoughts` (same rule as
/// new-api and sub2api). `OpenAI`'s `reasoning_tokens` is already inside
/// `completion_tokens`, so it is only surfaced as a detail and never added again.
/// Cache-write is collected as a detail (`cache_write_tokens` /
/// `cache_creation_input_tokens`); `price_of` does not consume it.
/// Shared by streaming and non-streaming extraction.
pub(crate) fn counts_from_usage(usage: &JsonValue) -> TokenCounts {
    let get = |keys: &[&str]| {
        keys.iter()
            .find_map(|k| usage.get(*k).and_then(JsonValue::as_u64))
            .unwrap_or(0)
    };
    let detail = |group: &str, key: &str| {
        usage
            .get(group)
            .and_then(|d| d.get(key))
            .and_then(JsonValue::as_u64)
            .unwrap_or(0)
    };
    let candidates = usage
        .get("candidatesTokenCount")
        .and_then(JsonValue::as_u64);
    let thoughts = usage.get("thoughtsTokenCount").and_then(JsonValue::as_u64);
    // Gemini shape only: thinking sits outside candidatesTokenCount but is billed as
    // output. Guarded on `candidatesTokenCount` so an OpenAI-shaped usage that also
    // carries a thoughts field cannot be double-counted.
    let completion = match (candidates, thoughts) {
        (Some(c), Some(t)) => c.saturating_add(t),
        _ => get(&["completion_tokens", "output_tokens", "candidatesTokenCount"]),
    };
    // Which primary key supplied `prompt` decides whether cached tokens are a
    // subset or additive. `prompt_tokens` (OpenAI Chat) and `promptTokenCount`
    // (Gemini) are totals. `input_tokens` is shared by Claude (excludes cache) and
    // OpenAI Responses (includes cache), so Claude's cache keys disambiguate.
    let claude_cache_keys = usage.get("cache_read_input_tokens").is_some()
        || usage.get("cache_creation_input_tokens").is_some();
    let prompt_includes_cache = usage.get("prompt_tokens").is_some()
        || usage.get("promptTokenCount").is_some()
        || (usage.get("input_tokens").is_some() && !claude_cache_keys);
    TokenCounts {
        prompt: get(&["prompt_tokens", "input_tokens", "promptTokenCount"]),
        completion,
        cached: usage
            .get("prompt_tokens_details")
            .and_then(|d| d.get("cached_tokens"))
            // OpenAI Responses nests the cache detail under input_tokens_details.
            .or_else(|| {
                usage
                    .get("input_tokens_details")
                    .and_then(|d| d.get("cached_tokens"))
            })
            .or_else(|| usage.get("cached_tokens"))
            .or_else(|| usage.get("cache_read_input_tokens"))
            .or_else(|| usage.get("cachedContentTokenCount"))
            .and_then(JsonValue::as_u64)
            .unwrap_or(0),
        cache_creation: usage
            .get("cache_creation_input_tokens")
            .or_else(|| {
                usage
                    .get("prompt_tokens_details")
                    .and_then(|d| d.get("cache_write_tokens"))
            })
            .and_then(JsonValue::as_u64)
            .unwrap_or(0),
        reasoning: detail("completion_tokens_details", "reasoning_tokens")
            .max(detail("output_tokens_details", "reasoning_tokens"))
            .max(
                usage
                    .get("thoughtsTokenCount")
                    .and_then(JsonValue::as_u64)
                    .unwrap_or(0),
            )
            .max(
                usage
                    .get("reasoning_tokens")
                    .and_then(JsonValue::as_u64)
                    .unwrap_or(0),
            ),
        prompt_includes_cache,
    }
}

/// `choices[0].finish_reason` (`OpenAI` shape); non-string / empty string → None.
pub(crate) fn choice_finish_reason(v: &JsonValue) -> Option<&str> {
    non_empty_str(
        v.get("choices")
            .and_then(|choices| choices.get(0))
            .and_then(|choice| choice.get("finish_reason")),
    )
}

/// `delta.stop_reason` (Claude streaming `message_delta` shape); non-string / empty
/// string → None.
pub(crate) fn delta_stop_reason(v: &JsonValue) -> Option<&str> {
    non_empty_str(v.get("delta").and_then(|delta| delta.get("stop_reason")))
}

/// Top-level `stop_reason` (Claude non-streaming shape); non-string / empty string →
/// None.
pub(crate) fn top_stop_reason(v: &JsonValue) -> Option<&str> {
    non_empty_str(v.get("stop_reason"))
}

/// JSON string lookup: non-string or empty string is always treated as missing.
fn non_empty_str(value: Option<&JsonValue>) -> Option<&str> {
    value.and_then(JsonValue::as_str).filter(|s| !s.is_empty())
}
