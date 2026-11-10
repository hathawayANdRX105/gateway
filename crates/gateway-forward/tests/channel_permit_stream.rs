//! Response-lifetime tests for per-channel concurrency permits — the permit is held by
//! the winning attempt's response until the stream ends.
//!
//! Background: the permit used to be a local variable inside the attempt closure and was
//! returned as soon as `forward_task` returned (response headers arrived). A streaming
//! response has only just started flowing at that point, so `ChannelRecord.max_concurrency`
//! effectively limited only connection-establishment concurrency, and P2C's
//! `available_permits` did not reflect the real channel load.
//! Current state: the permit is attached to the winning attempt's
//! [`gateway_forward::Forwarded`]; for streaming it is held by `commit_forwarded`'s unfold
//! stream state and released via RAII when the stream ends (fully read / upstream
//! disconnect / client disconnect dropping the stream); for non-streaming it is released
//! with the commit scope after the whole body is collected.
//!
//! Behaviors verified (channel concurrency limit = 1, retry mode = production wiring
//! shape):
//! 1. While a streaming response is not fully consumed, `available_permits == 0` (the
//!    permit rides on the stream, not already returned);
//! 2. After reading the whole stream, `available_permits == 1`;
//! 3. After reading only the first frame and dropping the stream (client disconnect),
//!    `available_permits == 1` — no leak;
//! 4. After a non-streaming response commits and collects the whole body, the permit is
//!    returned (`available_permits == 1`);
//! 5. A failed attempt produces no `Forwarded`, and its permit is returned when that
//!    attempt ends; the winning attempt's permit is returned after the non-streaming
//!    commit completes (`ch-c1` and `ch-c2` both back to 1).
//!
//! Mock style follows `concurrency_mount.rs` / `retry_wiring.rs`: egress is a scripted mock
//! injected through the trait (two SSE frames, the second gated by `Notify` so the "stream
//! not finished" window genuinely exists), and dispatch uses the shared mock in
//! `tests/common/mod.rs`; there is zero real network traffic.

mod common;

use bytes::Bytes;
use futures_util::Stream;
use futures_util::StreamExt;
use gateway_dispatch::RetryPolicy;
use gateway_forward::ForwardStage;
use gateway_forward::egress::{Egress, ForwardedResponse, Timeouts};
use gateway_gate::concurrency::ConcurrencyState;
use gateway_pipeline::{Stage, StageOutcome};
use gateway_protocol_bridge::format_codec::FormatRegistry;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;

use common::*;

const CH_C1: &str = "ch-c1";
const CH_C2: &str = "ch-c2";

// ---------- test helpers ----------

/// Two-frame SSE egress mock: the first frame is available immediately, the second is
/// gated on `Notify` before it appears — so the "upstream still sending" window genuinely
/// exists and the stream does not end naturally before the assertion.
struct GatedSseEgress {
    gate: Arc<tokio::sync::Notify>,
    calls: Mutex<usize>,
}

impl GatedSseEgress {
    fn new() -> Self {
        Self {
            gate: Arc::new(tokio::sync::Notify::new()),
            calls: Mutex::new(0),
        }
    }

    /// Release the second frame (the first waiter is released; used by the "read to the
    /// end" case so it can reach the stream tail).
    ///
    /// `notify_one` rather than `notify_waiters`: the latter only wakes waiters that are
    /// **already waiting** and stores no permit. `gated_sse_stream` is a lazy unfold — if
    /// the release happens before step 1 is polled (when `notified()` has not registered a
    /// waiter yet), `notify_waiters` fires into the void and the permit is lost forever;
    /// the following `next()` would then wait for a notification that never comes.
    /// `notify_one` stores one permit, so both the wait-first and release-first orderings
    /// terminate.
    fn release(&self) {
        self.gate.notify_one();
    }

    fn calls(&self) -> usize {
        *self.calls.lock().unwrap()
    }
}

/// Egress mock that always returns 200 + JSON (for the non-streaming case).
struct JsonEgress;

impl Egress for JsonEgress {
    fn execute<'a>(
        &'a self,
        _url: &'a str,
        _headers: &'a [(String, String)],
        _body: Bytes,
        _timeouts: &'a Timeouts,
    ) -> Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<ForwardedResponse, gateway_core::error::NormalizedError>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            Ok(ForwardedResponse::from_stream(
                200,
                "application/json",
                None,
                futures_util::stream::iter(vec![Ok::<Bytes, std::io::Error>(Bytes::from_static(
                    b"{\"from\":\"upstream\"}",
                ))]),
            ))
        })
    }
}

impl Egress for GatedSseEgress {
    fn execute<'a>(
        &'a self,
        _url: &'a str,
        _headers: &'a [(String, String)],
        _body: Bytes,
        _timeouts: &'a Timeouts,
    ) -> Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<ForwardedResponse, gateway_core::error::NormalizedError>,
                > + Send
                + 'a,
        >,
    > {
        *self.calls.lock().unwrap() += 1;
        let gate = Arc::clone(&self.gate);
        Box::pin(async move {
            Ok(ForwardedResponse::from_stream(
                200,
                "text/event-stream",
                None,
                gated_sse_stream(gate),
            ))
        })
    }
}

/// Two-frame SSE stream: poll 0 → first frame; poll 1 → wait for the `Notify` release,
/// then yield the `[DONE]` frame; poll 2 → end of stream. The gate makes "stream not
/// finished" and "upstream still sending" structurally the same in the test.
fn gated_sse_stream(
    gate: Arc<tokio::sync::Notify>,
) -> impl Stream<Item = Result<Bytes, std::io::Error>> + Send {
    futures_util::stream::unfold(0u8, move |step| {
        let gate = Arc::clone(&gate);
        async move {
            match step {
                0 => Some((Ok(Bytes::from_static(b"data: {\"id\":\"chunk-1\"}\n\n")), 1)),
                1 => {
                    gate.notified().await;
                    Some((Ok(Bytes::from_static(b"data: [DONE]\n\n")), 2))
                }
                _ => None,
            }
        }
    })
}

/// Assert the channel's currently available slot count; `None` = channel has no registered
/// concurrency limit (unlimited).
fn available(state: &ConcurrencyState, channel_key: &str) -> usize {
    state.available_permits(channel_key).unwrap_or_else(|| {
        panic!("channel {channel_key} should have its concurrency limit registered")
    })
}

/// Streaming ctx (body carries `"stream":true`), with the route pre-seeded with the
/// candidate (the handle no longer reads it after `with_retry`, but the shared builder
/// requires it).
fn stream_ctx(candidate: gateway_dispatch::Candidate) -> gateway_pipeline::RequestCtx {
    ctx_with_body(STREAM_BODY, candidate)
}

/// Non-streaming ctx.
fn non_stream_ctx(candidate: gateway_dispatch::Candidate) -> gateway_pipeline::RequestCtx {
    ctx_with_body(NON_STREAM_BODY, candidate)
}

/// On success take the `PipeStream` (panic with the `StageError` Debug on failure).
async fn expect_stream(
    stage: &ForwardStage,
    ctx: &mut gateway_pipeline::RequestCtx,
) -> gateway_pipeline::PipeStream {
    match stage.handle(ctx).await.expect("handle must succeed") {
        StageOutcome::Stream(pipe) => pipe,
        other => panic!("streaming request should return Stream, got {other:?}"),
    }
}

/// Read the `PipeStream` as an axum data stream (consuming it = the client has received the
/// whole body).
fn data_stream(pipe: gateway_pipeline::PipeStream) -> impl Stream<Item = Bytes> + Send {
    // axum's into_data_stream() item is Result<Bytes, Error>; body errors should not occur
    // on the test path, so panic if one does (keep the error for diagnosis).
    pipe.into_response()
        .into_body()
        .into_data_stream()
        .map(|chunk| chunk.expect("body chunk must not error"))
}

// ---------- case 1: stream not fully consumed → slot still held; fully read → returned ----------

#[tokio::test]
async fn streaming_permit_released_only_after_stream_fully_consumed() {
    let c1 = candidate("c1");
    let dispatch = Arc::new(MockDispatch::new(vec![c1.clone()]));
    let egress = Arc::new(GatedSseEgress::new());
    let state = Arc::new(ConcurrencyState::default());
    state.register_channel(CH_C1, 1);
    let fwd_stage = ForwardStage::new(egress.clone(), Arc::new(FormatRegistry::new()))
        .with_retry(dispatch, RetryPolicy::default())
        .with_channel_concurrency(state.clone());

    let mut ctx = stream_ctx(c1);
    let mut data = data_stream(expect_stream(&fwd_stage, &mut ctx).await);

    // The stream was just established and no frame consumed: the permit must still be held
    // (the response stream owns the slot), otherwise returning it at connection time makes
    // max_concurrency meaningless.
    assert_eq!(
        available(&state, CH_C1),
        0,
        "while response stream is not finished, channel slot must still be held"
    );

    // Read the first frame: the stream is not finished, the permit is still held by it.
    let first = data
        .next()
        .await
        .expect("first frame must be given by upstream");
    assert_eq!(first, Bytes::from_static(b"data: {\"id\":\"chunk-1\"}\n\n"));
    assert_eq!(
        available(&state, CH_C1),
        0,
        "after first frame stream still not finished, slot must continue to be held"
    );

    // Release [DONE] and read to the stream tail: the permit is returned by RAII as the
    // stream ends.
    egress.release();
    let mut rest = Vec::new();
    while let Some(chunk) = data.next().await {
        rest.extend_from_slice(&chunk);
    }
    assert_eq!(
        rest,
        b"data: [DONE]\n\n".to_vec(),
        "stream tail must be upstream's [DONE] frame"
    );
    assert_eq!(
        available(&state, CH_C1),
        1,
        "after stream is fully read, channel slot must be returned"
    );
}

// ---------- case 2: read first frame then drop (client disconnect) → returned ----------

#[tokio::test]
async fn streaming_permit_released_when_stream_dropped_mid_flight() {
    let c1 = candidate("c1");
    let dispatch = Arc::new(MockDispatch::new(vec![c1.clone()]));
    let egress = Arc::new(GatedSseEgress::new());
    let state = Arc::new(ConcurrencyState::default());
    state.register_channel(CH_C1, 1);
    let fwd_stage = ForwardStage::new(egress.clone(), Arc::new(FormatRegistry::new()))
        .with_retry(dispatch, RetryPolicy::default())
        .with_channel_concurrency(state.clone());

    let mut ctx = stream_ctx(c1);
    let mut data = data_stream(expect_stream(&fwd_stage, &mut ctx).await);

    // Read the first frame and stop: upstream's [DONE] is still blocked on Notify, so the
    // stream is unfinished.
    let first = data
        .next()
        .await
        .expect("first frame must be given by upstream");
    assert_eq!(first, Bytes::from_static(b"data: {\"id\":\"chunk-1\"}\n\n"));
    assert_eq!(
        available(&state, CH_C1),
        0,
        "while stream not finished, channel slot must still be held"
    );

    // Client disconnect: the axum Body is dropped together with the unfold stream state →
    // the permit is returned by RAII; the slot must not leak (otherwise one disconnect
    // permanently exhausts max_concurrency).
    drop(data);
    assert_eq!(
        available(&state, CH_C1),
        1,
        "after stream dropped (client disconnect), channel slot must be returned, no leak"
    );
    assert_eq!(
        egress.calls(),
        1,
        "only one upstream request should be sent"
    );
}

// ---------- case 3: non-streaming → returned after commit collects the whole body ----------

#[tokio::test]
async fn non_streaming_permit_released_after_commit() {
    let c1 = candidate("c1");
    let dispatch = Arc::new(MockDispatch::new(vec![c1.clone()]));
    let state = Arc::new(ConcurrencyState::default());
    state.register_channel(CH_C1, 1);
    let fwd_stage = ForwardStage::new(Arc::new(JsonEgress), Arc::new(FormatRegistry::new()))
        .with_retry(dispatch, RetryPolicy::default())
        .with_channel_concurrency(state.clone());

    let mut ctx = non_stream_ctx(c1);
    let outcome = fwd_stage
        .handle(&mut ctx)
        .await
        .expect("handle must succeed");
    assert!(
        matches!(outcome, StageOutcome::Continue),
        "non-streaming success should be Continue, got {outcome:?}"
    );
    assert_eq!(
        ctx.upstream
            .as_ref()
            .expect("ctx.upstream should be written")
            .status,
        200
    );
    assert_eq!(
        available(&state, CH_C1),
        1,
        "after non-streaming commit collects full body, permit should already be returned"
    );
}

// ---------- case 4: failed attempt returned immediately (no Forwarded) ----------

#[tokio::test]
async fn failed_attempt_releases_permit_without_forwarded() {
    // c1 fails retryable with 502 → switch to c2 and succeed.
    // Assertion: c1's permit is returned when the failed attempt ends (=1), and c2's permit
    // is returned after the non-streaming commit collects the whole body (=1).
    let c1 = candidate("c1");
    let c2 = candidate("c2");
    let dispatch = Arc::new(MockDispatch::new(vec![c1.clone(), c2.clone()]));
    let egress = Arc::new(ScriptedEgress::new(vec![
        (
            "upstream-c1",
            Plan::Fail {
                status: 502,
                retryable: true,
            },
        ),
        (
            "upstream-c2",
            Plan::Ok {
                body: b"{\"from\":\"c2\"}",
            },
        ),
    ]));
    let state = Arc::new(ConcurrencyState::default());
    state.register_channel(CH_C1, 1);
    state.register_channel(CH_C2, 1);
    // The 502 from upstream c1 is surfaced as Err by ScriptedEgress; c2 goes through the
    // non-streaming path here. Asserting "the stream holds the slot" for c2 would need a
    // streaming ctx, and a single GatedSseEgress cannot fork by URL.
    // Simplification: c2 also uses ScriptedEgress's Ok plan, but the ctx uses a
    // non-streaming body — so this case asserts failed-attempt release plus winning-attempt
    // release after commit.
    let fwd_stage = ForwardStage::new(egress.clone(), Arc::new(FormatRegistry::new()))
        .with_retry(dispatch, RetryPolicy::default())
        .with_channel_concurrency(state.clone());

    let mut ctx = non_stream_ctx(c1);
    let outcome = fwd_stage
        .handle(&mut ctx)
        .await
        .expect("c2 fallback should succeed");
    assert!(
        matches!(outcome, StageOutcome::Continue),
        "winning candidate success should be Continue, got {outcome:?}"
    );
    assert_eq!(
        ctx.upstream
            .as_ref()
            .expect("ctx.upstream should be written")
            .body,
        Bytes::from_static(b"{\"from\":\"c2\"}")
    );
    assert_eq!(
        available(&state, CH_C1),
        1,
        "failed attempt's permit should be returned when that attempt ends (no Forwarded produced)"
    );
    assert_eq!(
        available(&state, CH_C2),
        1,
        "winning attempt's permit returned after non-streaming commit collects full body"
    );
    assert_eq!(
        egress.calls().len(),
        2,
        "after c1 fails, c2 should be retried"
    );
}

/// An unconsumed stream must not survive drop: the unfold state holds the permit, and after
/// the last chunk is read the stream's internal state (including the permit) is dropped, so
/// the slot is already returned synchronously when the tail is reached.
#[tokio::test]
async fn permit_state_lives_in_unfold_not_in_body_buffer() {
    use futures_util::stream::StreamExt;
    let c1 = candidate("c1");
    let dispatch = Arc::new(MockDispatch::new(vec![c1.clone()]));
    let egress = Arc::new(GatedSseEgress::new());
    let state = Arc::new(ConcurrencyState::default());
    state.register_channel(CH_C1, 1);
    let fwd_stage = ForwardStage::new(egress.clone(), Arc::new(FormatRegistry::new()))
        .with_retry(dispatch, RetryPolicy::default())
        .with_channel_concurrency(state.clone());

    let mut ctx = stream_ctx(c1);
    let mut data = data_stream(expect_stream(&fwd_stage, &mut ctx).await);

    // Driving the underlying unfold directly is impractical here: manual polling needs a
    // Tokio context to build a waker, so instead read to None inside a tokio task — already
    // covered by case 1.
    // This only verifies that the permit is visibly returned right after None is read
    // (synchronous semantics, independent of drop order).
    assert_eq!(
        available(&state, CH_C1),
        0,
        "precondition: while stream not finished, slot occupied"
    );
    // After releasing the second frame, read to the stream tail: the permit is returned
    // immediately when the stream ends (synchronous semantics, independent of drop timing).
    // After reading, the slot is stably returned — polling the same terminated stream again
    // is not a legal operation (the `unfold` contract forbids polling after `Ready(None)`),
    // so this only asserts the stable returned state.
    egress.release();
    while data.next().await.is_some() {}
    assert_eq!(
        available(&state, CH_C1),
        1,
        "after reading to stream tail, slot immediately returned"
    );
}
