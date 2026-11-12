//! P2 streaming resilience — buffering window / heartbeat / break classification (the Window
//! phase is wired, PR #158).
//!
//! Modelled on api-hub `_smart_stream` (:618-723) / `_drain_buffered` (:556-615), and adopts
//! Bifrost's TTFT monitoring discipline: **once bytes have been sent to the client (Live
//! phase), no destructive restart** — only flush the buffer + finish with an `event: error`
//! frame; if the stream breaks before the first byte (Window phase), replace the whole
//! candidate and retry, invisibly to the client (= api-hub `buffer_stream`, same as flo2
//! buffer-before-forward).
//!
//! Parameter health (todo/gateway-resilience.md P2): a 2s window instead of api-hub's 60s,
//! retry max 3 instead of 100, heartbeat 15s (industry 10–15s).
//!
//! ## Wiring status (Window phase)
//!
//! - Decision function [`pipe_resilient`]: the Window phase is fully implemented (chunks
//!   accumulate in buf / on EOF, drain once the completeness criterion holds / return
//!   [`ResilientChunk::WindowEofRetryable`] without a criterion);
//! - Time-driven side [`drain_stream_window`]: the attempt closure of `handle_with_retry`
//!   waits on the stream window after `forward_task` succeeds — on EOF without a criterion
//!   inside the window the whole response is discarded for another candidate; with a
//!   criterion the buffer is drained, and when the window expires (upstream still sending)
//!   [`BufferedStream`] turns the buffer plus the remaining stream into a Live passthrough
//!   (`StreamPhase::Live`);
//! - Live phase (in [`pipe_resilient`]): chunks pass straight through
//!   ([`ResilientChunk::Emit`]); carried-over buffered bytes are flushed once they reach the
//!   [`LIVE_FLUSH_THRESHOLD`] watermark; EOF with completion evidence finishes normally
//!   (`Emit` empty), EOF without it flushes the buffered bytes and returns
//!   [`ResilientChunk::Broken`] with the flushed count plus a switch-budget-exhausted flag
//!   (`cfg.max_retries`, mirroring `RetryPolicy::max_attempts`);
//! - Live-phase heartbeat (`cfg.heartbeat_ms`) is time-driven: [`pipe_resilient`] holds no
//!   clock, so keepalive expiry on upstream silence is decided on the caller side (the
//!   Live-phase counterpart of [`drain_stream_window`]); `pipe_resilient` itself stays pure.
//!
//! ferrite's [`stream::SseContext`](crate::stream::SseContext) scan chain (token counting
//! already in the pipeline) is cleaner than api-hub's byte-level hack; this module only does
//! **buffering / heartbeat / break classification**, token counting still goes through the
//! original pipeline ([`crate::stream::pipe_chunk`]), with no duplicate implementation. The
//! Window buffer is spliced back into the original byte stream before `commit_forwarded`; the
//! scan chain still sees all bytes in original order, character for character — buffering does
//! not change token counting.

use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use crate::stream::SseContext;

/// Resilience pipeline parameters. Reading them from the config `[stream]` section is later
/// wiring (P2-A); production currently enables the phase with `StreamResilience::default()`,
/// whose values are the healthy defaults.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamResilience {
    /// Duration of the transparent retry window before the first byte (ms). Inside the window
    /// chunks are only buffered, never sent; a broken stream can be retried on the whole
    /// response against another candidate.
    pub window_ms: u64,
    /// Live-phase upstream silence threshold (ms): beyond it, the caller sends a
    /// `: keepalive\n\n` SSE comment frame to the client to keep the connection alive.
    ///
    /// Time-driven: [`pipe_resilient`] holds no clock, so the keepalive decision is made on
    /// the caller side (like [`drain_stream_window`] drives the Window phase with real time);
    /// this field is read by the Live-phase drive loop, not by `pipe_resilient` itself.
    pub heartbeat_ms: u64,
    /// Candidate-switch cap for Live-phase stream breaks (guarding against api-hub's
    /// brute-force max=100 default).
    ///
    /// Mirrors `gateway_core::dispatch::RetryPolicy.max_attempts` (the authoritative
    /// attempt budget, gateway-core/src/dispatch.rs:70-73): this field is the copy the
    /// Live-phase drive loop reads to seed the `switches_remaining` counter of
    /// [`pipe_resilient`]. Each Live-phase break consumes one switch; at zero,
    /// [`ResilientChunk::Broken`] carries `budget_exhausted = true` so the caller stops
    /// switching candidates.
    pub max_retries: u32,
}

impl Default for StreamResilience {
    fn default() -> Self {
        // 2s window (not 60s), 15s heartbeat (not the 10s lower bound), retry max 3 (not 100).
        Self {
            window_ms: 2_000,
            heartbeat_ms: 15_000,
            max_retries: 3,
        }
    }
}

/// Stream phase; decides break semantics (Bifrost: once bytes have been sent, no destructive
/// restart).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamPhase {
    /// Transparent retry window before the first byte: zero bytes sent, a broken stream can be
    /// retried on the whole response against another candidate.
    Window,
    /// Bytes already sent to the client: only flush the buffer + finish with an error frame;
    /// never restart the stream.
    Live,
}

/// Action the upper layer (`commit_forwarded`) should take after the resilience layer consumes
/// one upstream chunk (or EOF).
#[derive(Debug, Clone)]
pub enum ResilientChunk {
    /// Net SSE bytes to send (flushed in the Live phase past the threshold, or drained when the
    /// Window phase turns Live).
    Emit(Bytes),
    /// Keep accumulating in buf; not sent yet (Window phase, or Live-phase buffering below
    /// the flush watermark).
    Buffered,
    /// Stream broke inside the window (EOF with no completeness criterion, zero bytes sent):
    /// the upper layer replaces the whole candidate and retries.
    WindowEofRetryable,
    /// Live-phase break: flush the buffered bytes to the client, then finish with an `event:
    /// error` frame. `flushed` = number of bytes flushed (for settlement and logs).
    /// `budget_exhausted` = whether the Live-phase candidate-switch cap
    /// (`StreamResilience.max_retries`, mirroring `RetryPolicy.max_attempts`) has been
    /// consumed — when true, the caller stops switching candidates and surfaces the error.
    Broken { flushed: usize, budget_exhausted: bool },
}

/// Completeness criterion — the only basis for deciding "is it safe to send" on EOF inside the
/// window.
///
/// Modelled on api-hub `_drain_buffered`'s byte-level heuristic: either `[DONE]` (explicit
/// termination frame of OpenAI/compat layers) or `finish_reason` (the closing field of the
/// final data frame). Both markers appear only at the tail of an SSE frame; arriving inside the
/// window means the stream still has trailing bytes, so nothing is emitted yet.
const DONE_MARK: &[u8] = b"[DONE]";
const FINISH_MARK: &[u8] = b"finish_reason";
/// Live-phase buffer flush watermark (bytes): when Live-phase buffering applies
/// (carried-over bytes between frames / during resumption), buffered bytes are flushed as
/// one [`ResilientChunk::Emit`] once they reach this size. Pure size-based decision —
/// [`pipe_resilient`] holds no clock; the heartbeat (`cfg.heartbeat_ms`) keepalive is
/// time-driven and decided on the caller side.
const LIVE_FLUSH_THRESHOLD: usize = 32 * 1024; // 32KB

/// Whether `buf` already contains the complete criterion (`[DONE]` or `finish_reason`).
///
/// Plain byte-substring scan (not JSON parsing): SSE frame boundaries inside the window may stop
/// mid-line (a chunk split in the middle of a frame), and a byte-level judgement holds for a
/// half frame as well, whereas waiting for a frame boundary would push the completeness
/// decision past the window and lose the point of retrying.
#[must_use]
pub fn has_completion_evidence(buf: &[u8]) -> bool {
    buf.windows(DONE_MARK.len()).any(|w| w == DONE_MARK)
        || buf.windows(FINISH_MARK.len()).any(|w| w == FINISH_MARK)
}

/// Single-chunk decision step of the resilience pipeline.
///
/// Semantics (modelled on api-hub `_smart_stream`/`_drain_buffered`):
///
/// - **Window phase**: chunks accumulate in `buf` and are not sent; on `chunk = None` (EOF)
///   check completeness — if the buffer contains either `[DONE]` or `finish_reason` (criterion
///   copied from `_drain_buffered`), drain the buffer and send it as a whole; otherwise return
///   [`ResilientChunk::WindowEofRetryable`];
/// - **Live phase**: no destructive restart once bytes are flowing to the client — chunks go
///   straight to [`ResilientChunk::Emit`]. When `buf` already carries bytes (Live-phase
///   buffering between frames / during resumption), the chunk is appended and the buffer is
///   flushed as one `Emit` once it reaches [`LIVE_FLUSH_THRESHOLD`] (32KB); below the
///   watermark the bytes are held ([`ResilientChunk::Buffered`]). On EOF (`chunk = None`):
///   with completion evidence in buf, return `Emit(empty)` (normal stream finish); without it
///   the stream broke mid-response — flush what is buffered and return [`ResilientChunk::Broken`]
///   with the flushed byte count. `switches_remaining` counts Live-phase breaks against the
///   candidate-switch cap seeded from `StreamResilience.max_retries` (which mirrors
///   `RetryPolicy.max_attempts`); at zero, `Broken` carries `budget_exhausted = true` so the
///   caller stops switching candidates. The caller decrements the counter on each non-exhausted
///   break; this function is stateless.
/// - Heartbeat (`cfg.heartbeat_ms`): time-driven keepalive on upstream silence — decided on
///   the caller side, since [`pipe_resilient`] holds no clock (like [`drain_stream_window`]
///   which drives the Window phase with real time); this function stays pure.
/// - `ctx` is the hook for Live-phase settlement; unused this round: the window buffer is
///   spliced back into the byte stream before entering the original scan chain, and token
///   counting is handled character by character by [`crate::stream::pipe_chunk`] as before —
///   feeding it again here would double-count.
///
/// Window expiry (`window_ms`) is switched `Window → Live` by the caller on the time-driven
/// side (see [`drain_stream_window`]); this function decides purely on the passed-in `phase`
/// and holds no clock.
pub fn pipe_resilient(
    _ctx: &mut SseContext,
    buf: &mut Vec<u8>,
    chunk: Option<&Bytes>,
    phase: StreamPhase,
    switches_remaining: u32,
) -> ResilientChunk {
    match (phase, chunk) {
        // Inside the window: only accumulate, never send; the client sees zero bytes.
        (StreamPhase::Window, Some(chunk)) => {
            buf.extend_from_slice(chunk);
            ResilientChunk::Buffered
        }
        // EOF inside the window: with a criterion → drain and send as a whole; without one →
        // replace the whole candidate and retry.
        (StreamPhase::Window, None) => {
            if has_completion_evidence(buf) {
                ResilientChunk::Emit(Bytes::from(std::mem::take(buf)))
            } else {
                ResilientChunk::WindowEofRetryable
            }
        }
        // Live phase: bytes are already flowing to the client, so no destructive restart —
        // passthrough the chunk unchanged. When `buf` already carries buffered bytes
        // (Live-phase buffering between frames / during resumption), append and flush once
        // the watermark is reached; below it, keep buffering.
        (StreamPhase::Live, Some(chunk)) => {
            if buf.is_empty() {
                return ResilientChunk::Emit(chunk.clone());
            }
            buf.extend_from_slice(chunk);
            if buf.len() >= LIVE_FLUSH_THRESHOLD {
                return ResilientChunk::Emit(Bytes::from(std::mem::take(buf)));
            }
            ResilientChunk::Buffered
        }
        // Live-phase EOF: with completion evidence → normal finish; without it the stream
        // broke mid-response — flush the buffered bytes, count the break against the switch
        // cap, and surface exhaustion when the cap reaches zero.
        (StreamPhase::Live, None) => {
            if has_completion_evidence(buf) {
                // Normal finish: no bytes left to flush (buf does not accumulate in passthrough
                // mode); return an empty Emit as the signal.
                ResilientChunk::Emit(Bytes::new())
            } else {
                let flushed = std::mem::take(buf).len();
                ResilientChunk::Broken {
                    flushed,
                    budget_exhausted: switches_remaining == 0,
                }
            }
        }
    }
}

/// Single decision result on the time-driven side of the Window phase.
///
/// Equivalent to [`pipe_resilient`]'s decision in the same phase (adding the clock-holding
/// half): the `Window` → `Live` switch happens only here; the decision function itself does not
/// read time.
#[derive(Debug)]
pub enum WindowDrain {
    /// EOF inside the window: `complete` = whether the buffer already has the full criterion
    /// ([`has_completion_evidence`]).
    Eof { complete: bool, buf: Vec<u8> },
    /// Window expired while upstream is still sending: `buf` is the window buffer; splice it
    /// with the remaining stream into a new body and switch to Live passthrough.
    Live { buf: Vec<u8> },
    /// Upstream stream error inside the window (transport error): same semantics as "EOF
    /// without criterion" — switch candidate and retry.
    Broken { err: std::io::Error, buf: Vec<u8> },
}

/// Window wait: consume `body` for `cfg.window_ms`, accumulating window bytes into buf.
///
/// Semantics (consumed directly by the `handle_with_retry` attempt closure):
/// - EOF inside the window with a complete criterion in the buffer → [`WindowDrain::Eof`]
///   `complete = true`: the caller drains buf into the whole response, the client notices
///   nothing;
/// - EOF inside the window without a criterion / stream error → the caller discards this
///   attempt and switches candidate ([`WindowDrain::Eof`] `complete = false` /
///   [`WindowDrain::Broken`]);
/// - Window expiry (upstream still sending) → [`WindowDrain::Live`]: the caller uses
///   [`BufferedStream`] to splice buf with the remaining stream into a new body, then
///   passes through (Live phase).
///
/// `biased` gives data priority over window expiry at the same instant: a frame / EOF that
/// arrives exactly on the window boundary is judged under the Window phase (data that arrived
/// early must not be mistaken for "still sending" and needlessly switch to Live).
pub async fn drain_stream_window(
    body: &mut Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>>,
    window: Duration,
) -> WindowDrain {
    let mut buf: Vec<u8> = Vec::new();
    let deadline = tokio::time::Instant::now() + window;
    loop {
        tokio::select! {
            biased;
            item = body.next() => {
                match item {
                    Some(Ok(chunk)) => buf.extend_from_slice(&chunk),
                    Some(Err(err)) => return WindowDrain::Broken { err, buf },
                    None => {
                        return WindowDrain::Eof { complete: has_completion_evidence(&buf), buf };
                    }
                }
            }
            // Expiry: upstream is still sending (or silent) at this instant — buffer + remaining
            // stream switch to Live passthrough.
            () = tokio::time::sleep_until(deadline) => {
                return WindowDrain::Live { buf };
            }
        }
    }
}

/// Response stream assembled from the window buffer and the remaining stream: emit the buffer
/// first, then the remaining stream.
///
/// The byte order is exactly the same as "unbuffered passthrough" (the window bytes are the
/// earliest bytes the upstream produced), so splicing them back is fully transparent to the
/// client; when `pending = None` it degrades to the pure remaining stream.
///
/// `inner` is not polled after termination: upstream bodies are mostly `FusedStream`
/// (`futures`' `Unfold` panics if polled after termination), while a committed Body being
/// polled repeatedly is normal (axum's `into_data_stream` does not cache the terminated
/// state).
pub struct BufferedStream<S> {
    /// Window buffer (a single chunk, emitted whole once).
    pub pending: Option<Bytes>,
    /// Remaining upstream stream.
    pub inner: S,
    /// `inner` has already yielded `None` (see the termination semantics in the type docs).
    pub terminated: bool,
}

impl<S> Stream for BufferedStream<S>
where
    S: Stream<Item = Result<Bytes, std::io::Error>> + Unpin + Send + 'static,
{
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Some(chunk) = this.pending.take() {
            return Poll::Ready(Some(Ok(chunk)));
        }
        if this.terminated {
            return Poll::Ready(None);
        }
        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Ready(None) => {
                this.terminated = true;
                Poll::Ready(None)
            }
            other => other,
        }
    }
}

impl<S> futures_util::stream::FusedStream for BufferedStream<S>
where
    S: Stream<Item = Result<Bytes, std::io::Error>> + Unpin + Send + 'static,
{
    fn is_terminated(&self) -> bool {
        self.terminated
    }
}
