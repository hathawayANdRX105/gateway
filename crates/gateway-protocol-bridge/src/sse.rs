//! SSE frame scanner — byte-faithful forwarding plus event detection.
//!
//! Guarantees (matching new-api `scan_sse.go)`:
//! - forwarded bytes are byte-identical to the upstream (vendor-private frames
//!   included), with no reordering and no dropped whitespace;
//! - no fabricated [DONE]: an upstream truncation stays a truncation and is reported
//!   as Truncated;
//! - bounded write side: downstream backpressure never buffers the upstream unboundedly
//!   (TODO(ferrite-#502, re-anchored: #12): waterline parameter).
//!
//! Division of labour with `gateway_metering::StreamScanner`: this type handles only
//! **frames** (event/data/id boundaries, keepalive, termination); token counting is
//! metering's job.

use bytes::Bytes;
use serde_json::Value;

/// SSE scanner: fed one chunk at a time, returning the passthrough bytes and detected
/// events.
#[derive(Debug, Default)]
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
}

impl SseScanner {
    /// Feed one upstream byte chunk.
    pub fn push(&mut self, chunk: &Bytes) -> (Bytes, Vec<SseEvent>) {
        let mut events = Vec::new();
        for line in self.take_lines(chunk) {
            // A trailing `\r` (CRLF upstream) is not part of the payload.
            let line = line.strip_suffix(b"\r").unwrap_or(&line);
            self.handle_line(line, &mut events);
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
