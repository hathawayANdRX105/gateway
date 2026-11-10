//! Unit-level tests for the P2 streaming-resilience window phase (they target the internal
//! functions in `stream_resilience.rs`).
//!
//! The same-phase decisions of `pipe_resilient` (accumulate / drain / switch candidate /
//! passthrough) and the clock-holding half of `drain_stream_window` (EOF split inside the window /
//! expiry to Live) plus `BufferedStream`'s splice-back and termination semantics are all asserted
//! here at byte level.

use bytes::Bytes;
use futures_util::StreamExt;
use gateway_forward::stream::SseContext;
use gateway_forward::{
    BufferedStream, ResilientChunk, StreamPhase, StreamResilience, WindowDrain,
    drain_stream_window, has_completion_evidence, pipe_resilient,
};
use std::pin::Pin;
use std::time::Duration;

type Body = Pin<Box<dyn futures_util::Stream<Item = Result<Bytes, std::io::Error>> + Send>>;

fn ctx() -> SseContext {
    SseContext::new()
}

/// Window phase: chunks only accumulate in buf, and the decision is always Buffered (zero bytes
/// to the client).
#[test]
fn window_chunks_only_buffer() {
    let mut c = ctx();
    let mut buf = Vec::new();
    for frame in [b"data: a\n\n".as_slice(), b"data: b\n\n".as_slice()] {
        assert!(
            matches!(
                pipe_resilient(
                    &mut c,
                    &mut buf,
                    Some(&Bytes::from_static(frame)),
                    StreamPhase::Window,
                    &StreamResilience::default(),
                ),
                ResilientChunk::Buffered
            ),
            "window-phase chunks should only accumulate in buf, not emit to the client"
        );
    }
    assert_eq!(buf, b"data: a\n\ndata: b\n\n");
}

/// EOF inside the window: split on the criterion — with one, drain as Emit (buf is taken);
/// without one, `WindowEofRetryable`.
#[test]
fn window_eof_splits_on_completion_evidence() {
    let mut c = ctx();
    let mut buf = Vec::from(&b"data: [DONE]\n\n"[..]);
    match pipe_resilient(
        &mut c,
        &mut buf,
        None,
        StreamPhase::Window,
        &StreamResilience::default(),
    ) {
        ResilientChunk::Emit(bytes) => {
            assert_eq!(bytes, Bytes::from_static(b"data: [DONE]\n\n"));
        }
        other => panic!("EOF within window with criterion should drain Emit, got {other:?}"),
    }
    assert!(buf.is_empty(), "after drain, buf should be taken");

    let mut buf = Vec::from(&b"data: {\"delta\":{}}\n\n"[..]);
    assert!(
        matches!(
            pipe_resilient(
                &mut c,
                &mut buf,
                None,
                StreamPhase::Window,
                &StreamResilience::default(),
            ),
            ResilientChunk::WindowEofRetryable
        ),
        "EOF in window without criterion should retry the whole stream with another candidate"
    );
}

/// The Live phase is passthrough this round: chunks Emit directly, and EOF with a criterion
/// yields an empty Emit (normal finish).
#[test]
fn live_phase_passthrough_this_round() {
    let mut c = ctx();
    let mut buf = Vec::new();
    match pipe_resilient(
        &mut c,
        &mut buf,
        Some(&Bytes::from_static(b"data: x\n\n")),
        StreamPhase::Live,
        &StreamResilience::default(),
    ) {
        ResilientChunk::Emit(bytes) => assert_eq!(bytes, Bytes::from_static(b"data: x\n\n")),
        other => {
            panic!("Live-phase passthrough this round: chunk should Emit directly, got {other:?}")
        }
    }
    let mut buf = Vec::from(&b"data: [DONE]\n\n"[..]);
    match pipe_resilient(
        &mut c,
        &mut buf,
        None,
        StreamPhase::Live,
        &StreamResilience::default(),
    ) {
        ResilientChunk::Emit(bytes) => assert!(bytes.is_empty(), "normal finish sends no bytes"),
        other => panic!("Live-phase EOF with a criterion should finish normally, got {other:?}"),
    }
}

/// The criterion is a byte-level substring: a frame cut mid-line still counts as complete once
/// `[DONE]`/`finish_reason` has fully appeared.
#[test]
fn completion_evidence_is_bytes_level() {
    assert!(has_completion_evidence(b"data: [DONE]"));
    assert!(has_completion_evidence(
        b"data: {\"choices\":[{\"finish_reason\":\"stop\"}]}"
    ));
    // A criterion present only as a prefix (frame cut mid-line) is not complete.
    assert!(!has_completion_evidence(b"data: [DO"));
    assert!(!has_completion_evidence(
        b"data: {\"delta\":{\"content\":\"x\"}}"
    ));
    assert!(!has_completion_evidence(b""));
}

/// drain: EOF inside the window with a criterion → Eof{complete:true}; the whole buffer is
/// handed to the caller.
#[tokio::test]
async fn drain_window_eof_with_evidence_is_complete() {
    let mut body: Body = Box::pin(futures_util::stream::iter([
        Ok(Bytes::from_static(b"data: a\n\n")),
        Ok(Bytes::from_static(b"data: [DONE]\n\n")),
    ]));
    match drain_stream_window(&mut body, Duration::from_millis(5_000)).await {
        WindowDrain::Eof {
            complete: true,
            buf,
        } => assert_eq!(buf, b"data: a\n\ndata: [DONE]\n\n"),
        other => panic!("EOF within window with criterion should be complete, got {other:?}"),
    }
}

/// drain: EOF inside the window without a criterion → Eof{complete:false} (the upper layer
/// switches candidate).
#[tokio::test]
async fn drain_window_eof_without_evidence_is_retryable() {
    let mut body: Body = Box::pin(futures_util::stream::iter([Ok(Bytes::from_static(
        b"data: a\n\n",
    ))]));
    match drain_stream_window(&mut body, Duration::from_millis(5_000)).await {
        WindowDrain::Eof {
            complete: false,
            buf,
        } => assert_eq!(buf, b"data: a\n\n"),
        other => panic!("EOF without criterion should be retryable, got {other:?}"),
    }
}

/// drain: stream error inside the window → Broken (same semantics as EOF without a criterion).
#[tokio::test]
async fn drain_stream_error_within_window_is_broken() {
    let mut body: Body = Box::pin(futures_util::stream::iter([
        Ok(Bytes::from_static(b"data: a\n\n")),
        Err(std::io::Error::other("upstream reset")),
    ]));
    match drain_stream_window(&mut body, Duration::from_millis(5_000)).await {
        WindowDrain::Broken { buf, .. } => assert_eq!(buf, b"data: a\n\n"),
        other => panic!("stream error should be Broken, got {other:?}"),
    }
}

/// drain: window expiry (upstream still sending) → Live; the spliced byte order matches
/// passthrough exactly, and polling after termination stays safe to repeat.
#[tokio::test]
async fn drain_window_timeout_is_live_and_concat_preserves_order() {
    // The first frame arrives immediately, the last only after 200ms; a 50ms window must take
    // the Live branch (only the first frame fits in the window).
    let head: Body = Box::pin(futures_util::stream::once(async {
        Ok::<Bytes, std::io::Error>(Bytes::from_static(b"data: a\n\n"))
    }));
    let delayed: Body = Box::pin(futures_util::stream::once(async {
        tokio::time::sleep(Duration::from_millis(200)).await;
        Ok::<Bytes, std::io::Error>(Bytes::from_static(b"data: [DONE]\n\n"))
    }));
    let mut body: Body = Box::pin(head.chain(delayed));
    let buf = match drain_stream_window(&mut body, Duration::from_millis(50)).await {
        WindowDrain::Live { buf } => buf,
        other => panic!("upstream still sending should be Live, got {other:?}"),
    };
    assert_eq!(
        buf, b"data: a\n\n",
        "window period has only the first frame"
    );

    let mut joined = BufferedStream {
        pending: Some(Bytes::from(buf)),
        inner: body,
        terminated: false,
    };
    let mut all = Vec::new();
    while let Some(chunk) = joined.next().await {
        all.extend_from_slice(&chunk.expect("should not error"));
    }
    assert_eq!(
        all, b"data: a\n\ndata: [DONE]\n\n",
        "byte order after splice-back should match passthrough"
    );
    // Polling again after termination still returns None (the upstream must not be polled again:
    // unfold-style streams panic).
    assert!(
        joined.next().await.is_none(),
        "repeat poll after termination should still be None"
    );
}
