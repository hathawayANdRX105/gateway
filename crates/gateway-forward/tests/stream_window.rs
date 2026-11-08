//! P2 streaming-resilience Window-phase integration tests — buffering before the first
//! byte and switching candidates on a broken stream (#158).
//!
//! Background: streaming responses were forwarded as soon as the response headers
//! arrived; if the upstream then broke the stream (half an SSE stream, no
//! `[DONE]`/`finish_reason`), the client had already received the beginning and was left
//! with a dead stream. api-hub's `buffer_stream` (buffer before the first byte plus
//! broken-stream retry) and flo2's buffer-before-forward use the same approach: emit
//! nothing before the first byte, retry the whole request on another candidate when the
//! stream breaks, and the client notices nothing.
//!
//! Three window-wiring behaviors are verified inside the `handle_with_retry` attempt
//! closure:
//! 1. **EOF inside the window with no completeness predicate → retry another candidate
//!    successfully**: c1's stream has only one content frame before EOF; the client
//!    receives only c2's complete stream (not a single byte of c1's partial stream
//!    leaks), and the second `select` excludes c1 while health reports
//!    c1=Err(Retryable)/c2=Ok(200);
//! 2. **`[DONE]` inside the window → drain the buffer and pass through normally**: the
//!    complete stream finishes inside the window, the whole buffer is emitted, the bytes
//!    the client receives match the upstream byte for byte, and no candidate switch is
//!    triggered;
//! 3. **Window timeout switches to Live → later chunks pass through**: the upstream sends
//!    `[DONE]` only after the window expires; the buffered plus remaining stream is
//!    recombined in order and the later chunks pass through to the client.
//!
//! Mock style matches `channel_permit_stream.rs` / `retry_wiring.rs`: egress is a
//! scripted mock injected through the trait (returning an SSE frame sequence with
//! programmable timing per candidate base_url), dispatch uses the shared mock from
//! `tests/common/mod.rs`, and no real network traffic is sent.

mod common;

use parking_lot::Mutex;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_util::StreamExt;
use gateway_dispatch::RetryPolicy;
use gateway_dispatch::health::FailureClass;
use gateway_forward::ForwardStage;
use gateway_forward::StreamResilience;
use gateway_forward::egress::{Egress, ForwardedResponse, Timeouts};
use gateway_pipeline::{Stage, StageOutcome};
use gateway_protocol_bridge::format_codec::FormatRegistry;

use common::*;

/// One SSE frame and its arrival delay (delay 0 = emitted immediately after the response
/// headers).
type Frame = (Bytes, Duration);

// ---------- Test helpers ----------

/// Scripted SSE egress mock: returns a frame sequence keyed by candidate base_url; frames
/// are emitted in order after their own delay, and the end of the sequence is EOF (= the
/// upstream broke the stream).
struct ScriptedSseEgress {
    plans: Mutex<Vec<(String, Vec<Frame>)>>,
    calls: Mutex<Vec<String>>,
}

impl ScriptedSseEgress {
    fn new(plans: Vec<(&str, Vec<Frame>)>) -> Self {
        Self {
            plans: Mutex::new(plans.into_iter().map(|(k, v)| (k.to_string(), v)).collect()),
            calls: Mutex::new(Vec::new()),
        }
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().clone()
    }
}

/// Frame sequence → SSE byte stream (sleep for each frame's delay before emitting it; EOF
/// after the last).
fn sse_stream(
    frames: Vec<Frame>,
) -> impl futures_util::Stream<Item = Result<Bytes, std::io::Error>> + Send {
    futures_util::stream::unfold(frames.into_iter(), move |mut iter| async move {
        let frame = iter.next()?;
        if !frame.1.is_zero() {
            tokio::time::sleep(frame.1).await;
        }
        Some((Ok(frame.0), iter))
    })
}

impl Egress for ScriptedSseEgress {
    fn execute<'a>(
        &'a self,
        url: &'a str,
        _headers: &'a [(String, String)],
        _body: Bytes,
        _timeouts: &'a Timeouts,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<ForwardedResponse, app_contract::error::NormalizedError>>
                + Send
                + 'a,
        >,
    > {
        let marker = self
            .plans
            .lock()
            .iter()
            .find(|(key, _)| url.contains(key.as_str()))
            .map(|(key, frames)| (key.clone(), frames.clone()))
            .expect("script has no base_url marker for this candidate");
        self.calls.lock().push(marker.0);
        Box::pin(async move {
            Ok(ForwardedResponse::from_stream(
                200,
                "text/event-stream",
                None,
                sse_stream(marker.1),
            ))
        })
    }
}

/// Streaming stage in retry mode: `window_ms = None` uses the default window (2s), while
/// `Some(ms)` overrides it.
fn stream_stage(
    egress: Arc<dyn Egress>,
    dispatch: Arc<MockDispatch>,
    window_ms: Option<u64>,
) -> ForwardStage {
    let default_window_ms = StreamResilience::default().window_ms;
    ForwardStage::new(egress, Arc::new(FormatRegistry::new()))
        .with_retry(dispatch, RetryPolicy::default())
        .with_stream_resilience(Some(StreamResilience {
            window_ms: window_ms.unwrap_or(default_window_ms),
            ..StreamResilience::default()
        }))
}

/// Extract the PipeStream after a successful handle (panics with the StageError Debug value
/// on failure).
async fn expect_stream(
    stage: &ForwardStage,
    ctx: &mut gateway_pipeline::RequestCtx,
) -> gateway_pipeline::PipeStream {
    match stage.handle(ctx).await.expect("handle should succeed") {
        StageOutcome::Stream(pipe) => pipe,
        other => panic!("streaming request should return Stream, got {other:?}"),
    }
}

/// Read the PipeStream into a byte vector and drain it (consuming it means the client has
/// received the whole body).
async fn read_all(pipe: gateway_pipeline::PipeStream) -> Vec<u8> {
    // axum's into_data_stream() yields Result<Bytes, Error>; on the test path no body error
    // should appear (one appearing means window splicing/passthrough is broken).
    let mut data = pipe.into_response().into_body().into_data_stream();
    let mut all = Vec::new();
    while let Some(chunk) = data.next().await {
        all.extend_from_slice(&chunk.expect("body chunk should not error"));
    }
    all
}

// ---------- Case 1: stream breaks inside the window (no predicate) → retry another
// candidate; the client receives only the final response ----------

#[tokio::test]
async fn window_broken_stream_retries_candidate_without_client_bytes() {
    let c1 = candidate("c1");
    let c2 = candidate("c2");
    let dispatch = Arc::new(MockDispatch::new(vec![c1.clone(), c2.clone()]));
    let egress = Arc::new(ScriptedSseEgress::new(vec![
        // c1: EOF after a single partial content frame — no [DONE]/finish_reason predicate
        // inside the window.
        (
            "upstream-c1",
            vec![(
                Bytes::from_static(b"data: {\"id\":\"c1-partial\"}\n\n"),
                Duration::ZERO,
            )],
        ),
        // c2: complete stream (content frame + [DONE]), finished inside the window.
        (
            "upstream-c2",
            vec![
                (
                    Bytes::from_static(b"data: {\"id\":\"c2-1\"}\n\n"),
                    Duration::ZERO,
                ),
                (Bytes::from_static(b"data: [DONE]\n\n"), Duration::ZERO),
            ],
        ),
    ]));
    let stage = stream_stage(egress.clone(), dispatch.clone(), None);

    let mut ctx = ctx_with_body(STREAM_BODY, c1.clone());
    let all = read_all(expect_stream(&stage, &mut ctx).await).await;

    // The client must receive only c2's complete response: not a single byte of c1's
    // partial stream may leak (the whole retry semantics of the window phase = the client
    // notices nothing).
    assert_eq!(
        all,
        b"data: {\"id\":\"c2-1\"}\n\ndata: [DONE]\n\n".to_vec(),
        "stream break in window should retry whole stream with another candidate; client receives only the final response"
    );
    assert_eq!(
        egress.calls(),
        vec!["upstream-c1".to_string(), "upstream-c2".to_string()],
        "after c1 stream break, switch to c2 and resend once"
    );

    // The second select's exclude contains the already-tried c1; both attempts report health.
    let selects = dispatch.selects();
    assert_eq!(selects.len(), 2, "after c1 stream break, select again once");
    assert_eq!(selects[1], vec!["c1".to_string()]);
    assert_eq!(
        dispatch.reports(),
        vec![
            ("c1".to_string(), Err(FailureClass::Retryable)),
            ("c2".to_string(), Ok(200)),
        ],
        "stream break attempt must report Retryable to drive candidate switch"
    );
}

// ---------- Case 2: a complete predicate appears inside the window → drain the buffer and
// pass through normally ----------

#[tokio::test]
async fn window_complete_stream_drains_buffer_passthrough() {
    let c1 = candidate("c1");
    let dispatch = Arc::new(MockDispatch::new(vec![c1.clone()]));
    let egress = Arc::new(ScriptedSseEgress::new(vec![(
        "upstream-c1",
        vec![
            (
                Bytes::from_static(b"data: {\"id\":\"a\"}\n\n"),
                Duration::ZERO,
            ),
            (
                Bytes::from_static(b"data: {\"id\":\"b\"}\n\n"),
                Duration::ZERO,
            ),
            (Bytes::from_static(b"data: [DONE]\n\n"), Duration::ZERO),
        ],
    )]));
    // Default 2s window: the complete stream finishes immediately (the EOF has a predicate
    // → drain), without waiting for the window to expire.
    let stage = stream_stage(egress.clone(), dispatch.clone(), None);

    let mut ctx = ctx_with_body(STREAM_BODY, c1.clone());
    let all = read_all(expect_stream(&stage, &mut ctx).await).await;

    assert_eq!(
        all,
        b"data: {\"id\":\"a\"}\n\ndata: {\"id\":\"b\"}\n\ndata: [DONE]\n\n".to_vec(),
        "after complete stream in window is drained, pass through byte by byte to client"
    );
    assert_eq!(
        egress.calls().len(),
        1,
        "complete stream should not switch candidate and resend"
    );
    assert_eq!(
        dispatch.reports(),
        vec![("c1".to_string(), Ok(200))],
        "complete stream succeeds once"
    );
}

// ---------- Case 3: window timeout switches to Live → later chunks pass through ----------

#[tokio::test]
async fn window_timeout_switches_to_live_and_passes_rest() {
    let c1 = candidate("c1");
    let dispatch = Arc::new(MockDispatch::new(vec![c1.clone()]));
    let egress = Arc::new(ScriptedSseEgress::new(vec![(
        "upstream-c1",
        vec![
            (
                Bytes::from_static(b"data: {\"id\":\"head\"}\n\n"),
                Duration::ZERO,
            ),
            // The last frame arrives only after the window expires (1s) → the window must
            // switch to Live first.
            (
                Bytes::from_static(b"data: [DONE]\n\n"),
                Duration::from_millis(1_200),
            ),
        ],
    )]));
    // Shorten the window (1s) so "last frame arrives after window expiry" reproduces within
    // the test time limit; production defaults to 2s with the same semantics (buffer during
    // the window, switch to Live on expiry).
    let stage = stream_stage(egress.clone(), dispatch.clone(), Some(1_000));

    let mut ctx = ctx_with_body(STREAM_BODY, c1.clone());
    let all = read_all(expect_stream(&stage, &mut ctx).await).await;

    assert_eq!(
        all,
        b"data: {\"id\":\"head\"}\n\ndata: [DONE]\n\n".to_vec(),
        "after window timeout, buffered + remaining stream spliced back in original order; client sees complete byte stream"
    );
    assert_eq!(
        egress.calls().len(),
        1,
        "Live phase no longer switches candidate (bytes already emitted cannot be destructively restarted)"
    );
    assert_eq!(dispatch.reports(), vec![("c1".to_string(), Ok(200))]);
}
