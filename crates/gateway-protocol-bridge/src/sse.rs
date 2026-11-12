//! SSE frame scanner — byte-faithful forwarding plus event detection.
//!
//! Guarantees (matching new-api `scan_sse.go`):
//! - forwarded bytes are byte-identical to the upstream (vendor-private frames
//!   included), with no reordering and no dropped whitespace;
//! - no fabricated [DONE]: an upstream truncation stays a truncation and is reported
//!   as Truncated;
//! - bounded write side: buffered bytes (the incomplete-line buffer plus the
//!   accumulated `data:` payload) are flushed once they reach the configured
//!   waterline, so downstream backpressure never lets retention grow unboundedly.
//!   Passthrough bytes are returned verbatim regardless; a flush only finalizes
//!   retained state (a partial line is finalized early, so events split across a
//!   flush point may fire early or be missed — keep inputs below the waterline for
//!   the historic exact semantics).
//!
//! `Default` uses an unbounded waterline (`usize::MAX`), so existing consumers see
//! the historic behavior unchanged; callers that need bounding construct with
//! `SseScanner::new(waterline)`.
//!
//! Division of labour with `gateway_metering::StreamScanner`: this type handles only
//! **frames** (event/data/id boundaries, keepalive, termination); token counting is
//! metering's job.

use bytes::Bytes;
use serde_json::Value;

/// SSE scanner: fed one chunk at a time, returning the passthrough bytes and detected
/// events.
#[derive(Debug)]
pub struct SseScanner {
    /// Leftover line buffer (an incomplete line spanning chunks).
    line_buf: Vec<u8>,
    /// Whether `FirstToken` has been reported (idempotent, reported once).
    saw_first_token: bool,
    /// Whether [DONE] has been seen.
    saw_done: bool,
    /// Whether the stream is cut off: frames after [DONE] are dropped and never queued.
    done_cutoff: bool,
    /// `data:` payload accumulated for the current frame (multiple lines joined by
    /// newlines, per the SSE spec).
    frame_data: Vec<String>,
    /// Queue of complete frame payloads (terminated by a blank-line separator).
    ready_frames: Vec<String>,
    /// High-water mark in bytes for `line_buf` + `frame_data`. When retained bytes
    /// reach it, `push` flushes buffered frames into `ready_frames` so the upstream
    /// is never retained unboundedly. `usize::MAX` disables flushing.
    waterline: usize,
}

impl SseScanner {
    /// Create a scanner that flushes retained bytes once they reach `waterline`
    /// bytes (`line_buf` + `frame_data`). Pass `usize::MAX` to disable flushing.
    #[must_use]
    pub fn new(waterline: usize) -> Self {
        Self {
            line_buf: Vec::new(),
            saw_first_token: false,
            saw_done: false,
            done_cutoff: false,
            frame_data: Vec::new(),
            ready_frames: Vec::new(),
            waterline,
        }
    }

    /// Feed one upstream byte chunk.
    pub fn push(&mut self, chunk: &Bytes) -> (Bytes, Vec<SseEvent>) {
        let mut events = Vec::new();
        for line in self.take_lines(chunk) {
            // A trailing `\r` (CRLF upstream) is not part of the payload.
            let line = line.strip_suffix(b"\r").unwrap_or(&line);
            self.handle_line(line, &mut events);
            // Keep retention bounded: once the incomplete-line buffer plus the
            // accumulated `data:` payload reach the waterline, flush buffered
            // frames so downstream backpressure cannot let the upstream be
            // retained unboundedly. Passthrough bytes are unaffected — every
            // byte handed to `push` comes back verbatim.
            if self.buffered_bytes() >= self.waterline {
                self.flush_waterline(&mut events);
            }
        }
        // A chunk with no newline leaves everything in `line_buf`: flush it too.
        if self.buffered_bytes() >= self.waterline {
            self.flush_waterline(&mut events);
        }
        (chunk.clone(), events)
    }

    /// Split the bytes received so far into complete lines at `\n`; a trailing
    /// incomplete line stays in `line_buf`.
    fn take_lines(&mut self, chunk: &Bytes) -> Vec<Vec<u8>> {
        let mut full = std::mem::take(&mut self.line_buf);
        full.extend_from_slice(chunk);

        let mut lines = Vec::new();
        let mut start = 0;
        for (i, &b) in full.iter().enumerate() {
            if b == b'\n' {
                lines.push(full[start..i].to_vec());
                start = i + 1;
            }
        }
        self.line_buf = full[start..].to_vec();
        lines
    }
    /// Bytes currently retained by the scanner: the incomplete-line buffer plus
    /// the accumulated `data:` payload. `ready_frames` is excluded — it is the
    /// flush destination the consumer drains with `take_data_frames`.
    fn buffered_bytes(&self) -> usize {
        self.line_buf.len() + self.frame_data.iter().map(String::len).sum::<usize>()
    }

    /// Flush retained state once the high-water mark is reached.
    ///
    /// The trailing incomplete line is finalized as a complete line and any
    /// accumulated `data:` payload is sealed into `ready_frames`, so retention
    /// drops back below the waterline. A flush never invents a `[DONE]` and never
    /// alters passthrough bytes; because a partial line is finalized early, events
    /// split across the flush point may fire early or be missed.
    fn flush_waterline(&mut self, events: &mut Vec<SseEvent>) {
        if !self.line_buf.is_empty() {
            let line = std::mem::take(&mut self.line_buf);
            self.handle_line(&line, events);
        }
        if !self.frame_data.is_empty() {
            self.ready_frames.push(self.frame_data.join("\n"));
            self.frame_data.clear();
        }
    }

    /// Handle one complete line: accumulate the `data:` payload and emit events.
    fn handle_line(&mut self, line: &[u8], events: &mut Vec<SseEvent>) {
        if line.is_empty() {
            // A blank line is the frame separator: seal the accumulated data payload
            // into a complete frame.
            if !self.frame_data.is_empty() {
                self.ready_frames.push(self.frame_data.join("\n"));
                self.frame_data.clear();
            }
            return;
        }

        if line.starts_with(b":") {
            events.push(SseEvent::Ping);
            return;
        }

        let Some(rest) = line.strip_prefix(b"data") else {
            return;
        };
        let rest = if let Some(r) = rest.strip_prefix(b": ") {
            r
        } else if let Some(r) = rest.strip_prefix(b":") {
            r
        } else {
            return;
        };
        let rest = rest.trim_ascii_start();

        if rest.starts_with(b"[DONE]") {
            self.saw_done = true;
            // [DONE] terminates the stream: data frames after it in the same chunk
            // (forbidden by the SSE spec, but real upstreams do it) must not enter
            // ready_frames, or they would be picked up and encoded, sending content to
            // the client after the terminal frame. Cut off every following frame.
            self.done_cutoff = true;
            return;
        }
        // A terminated stream accepts no further data frames.
        if self.done_cutoff {
            return;
        }

        if !self.saw_first_token {
            self.saw_first_token = true;
            events.push(SseEvent::FirstToken);
        }

        if rest.windows(7).any(|w| w == b"\"usage\"") {
            events.push(SseEvent::Usage);
        }

        // Alongside the byte-faithful forwarding, keep an extra payload copy for
        // protocol conversion.
        self.frame_data
            .push(String::from_utf8_lossy(rest).into_owned());
    }

    /// Take the complete frame payloads available so far (the contents of `data:` lines,
    /// multiple lines joined by `\n`).
    ///
    /// Used for cross-format conversion: callers get a **complete frame** and need not
    /// handle half-lines spanning chunks themselves — the old
    /// `ClaudeCodec::adapt_response` guessed boundaries from "does the chunk contain a
    /// `data:` line" and silently dropped half-lines that failed JSON parsing.
    ///
    /// The queue is cleared on take; tail data that has not met a blank-line separator
    /// stays internal until the next chunk completes it.
    pub fn take_data_frames(&mut self) -> Vec<String> {
        std::mem::take(&mut self.ready_frames)
    }

    /// Take the unterminated tail frame (the stream has ended, so it will never be
    /// completed).
    ///
    /// If the upstream disconnects without a blank line after its last frame (the SSE
    /// spec does not require a trailing blank line), that frame stays in `frame_data`;
    /// without a flush the last frame would be lost — for streaming conversion that can
    /// be the closing frame of the whole response (`message_delta` / `finish_reason`).
    pub fn flush_pending_frame(&mut self) -> Vec<String> {
        if self.frame_data.is_empty() {
            return Vec::new();
        }
        let pending = self.frame_data.join("\n");
        self.frame_data.clear();
        vec![pending]
    }

    /// Whether the upstream explicitly sent `[DONE]`.
    ///
    /// Used for cross-format conversion: the target-format encoder decides from this
    /// whether `finish()` still needs to append a terminal frame (it must not when the
    /// upstream already sent one, or the client would see two stream terminations).
    #[must_use]
    pub fn saw_done(&self) -> bool {
        self.saw_done
    }

    /// Upstream end: report how it terminated.
    #[must_use]
    pub fn finish(self) -> SseEnd {
        if self.saw_done {
            SseEnd::Clean
        } else {
            SseEnd::Truncated
        }
    }
}
impl Default for SseScanner {
    fn default() -> Self {
        // Unbounded by default: existing consumers (`SseContext` in gateway-forward
        // and the pipeline codec scanner) keep the historic behavior exactly; opt
        // into bounding with `SseScanner::new(waterline)`.
        Self::new(usize::MAX)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum SseEvent {
    FirstToken,
    Usage,
    Ping,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SseEnd {
    Clean,
    Truncated,
    Errored,
}

// ==== Frame construction: SSE output shapes shared by the four codecs ====

/// SSE frame with only `data:` (`OpenAI` / Gemini shape).
pub(crate) fn data_frame(payload: &Value) -> Bytes {
    Bytes::from(format!("data: {payload}\n\n"))
}

/// SSE frame with `event:` and `data:` (Claude / Responses shape).
pub(crate) fn event_frame(kind: &str, payload: &Value) -> Bytes {
    Bytes::from(format!("event: {kind}\ndata: {payload}\n\n"))
}
