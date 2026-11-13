//! Stream deadline and drop observation tests (slice A: F1 stream timeouts,
//! F3 drop logging).
//!
//! The mock egress drives upstream timing; every case runs the real streaming
//! commit unfold through `Stage::handle` in retry mode (the production wiring
//! shape, so the channel permit rides with the stream and the token
//! reservation travels with it), against loopback mocks — zero real network.
//!
//! Acceptance:
//! 1. idle arm: one frame then permanent silence, `stream_idle_ms=50ms` ⇒ the
//!    client stream ends with an `io::ErrorKind::TimedOut` item; the settle
//!    event carries status 500 / "stream idle timeout" with produced token
//!    counts; the channel permit and token reservation are returned
//!    (`available_permits` back to 1, inflight back to 0).
//! 2. max arm: upstream trickles a frame every 10 ms forever,
//!    `stream_max_ms=500ms` + `stream_idle_ms=10s` ⇒ the max arm fires first;
//!    `TimedOut` end, settle 500 / "stream max duration exceeded".
//! 3. regression: five frames ~20 ms apart then clean EOF,
//!    `stream_idle_ms=60ms` ⇒ EOF settles 200; the per-chunk idle reset means
//!    the 60 ms idle budget (which would fire mid-stream without reset) never
//!    misfires.

mod common;

use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use futures_util::Stream;
use futures_util::StreamExt;
use gateway_core::error::NormalizedError;
use gateway_core::metering::{ModelPrice, PriceTable};
use gateway_dispatch::{RetryPolicy, token_budget::TokenBudgetState};
use gateway_forward::ForwardStage;
use gateway_forward::egress::{Egress, ForwardedResponse, Timeouts};
use gateway_gate::concurrency::ConcurrencyState;
use gateway_metering::SettleSink;
use gateway_pipeline::{PipeStream, Stage, StageOutcome};
use gateway_protocol_bridge::format_codec::FormatRegistry;

use common::{MockDispatch, STREAM_BODY, VecSink, candidate, ctx_with_body};

const CH_C1: &str = "ch-c1";

type TestStream = Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>>;

/// Zero-price table: puts the settlement path in the "priced" branch so the
/// settle event is actually emitted, without affecting any asserted value
/// (cost is not asserted).
struct UnitPriceTable;

impl PriceTable for UnitPriceTable {
    fn lookup(&self, _model: &str, _group: &str) -> Option<ModelPrice> {
        Some(ModelPrice {
            input: 0.0,
            output: 0.0,
            cache: 0.0,
            group_multiplier: 1.0,
        })
    }
}

/// Mock upstream: every `execute` builds the response stream from `factory`.
struct MockEgress {
    status: u16,
    factory: Arc<dyn Fn() -> TestStream + Send + Sync>,
}

impl MockEgress {
    fn new(status: u16, factory: Arc<dyn Fn() -> TestStream + Send + Sync>) -> Self {
        Self { status, factory }
    }
}

impl Egress for MockEgress {
    fn execute<'a>(
        &'a self,
        _alias: &str,
        _url: &'a str,
        _headers: &'a [(String, String)],
        _body: Bytes,
        _timeouts: &'a Timeouts,
    ) -> Pin<Box<dyn Future<Output = Result<ForwardedResponse, NormalizedError>> + Send + 'a>> {
        let factory = Arc::clone(&self.factory);
        let status = self.status;
        Box::pin(async move {
            Ok(ForwardedResponse::from_stream(
                status,
                "text/event-stream",
                None,
                (factory)(),
            ))
        })
    }
}

/// Stage wired like production: retry loop (dispatch + channel permit riding
/// with the stream), per-channel concurrency gate, token budget, settlement
/// sink + price table, and the timeouts under test.
fn stage_with(
    egress: Arc<dyn Egress>,
    timeouts: Timeouts,
    sink: Arc<VecSink>,
    concurrency: Arc<ConcurrencyState>,
    budget: Arc<TokenBudgetState>,
) -> ForwardStage {
    let pt: Arc<dyn PriceTable> = Arc::new(UnitPriceTable);
    let sink_obj: Arc<dyn SettleSink> = sink;
    ForwardStage::new(egress, Arc::new(FormatRegistry::new()))
        .with_retry(
            Arc::new(MockDispatch::new(vec![candidate("c1")])),
            RetryPolicy::default(),
        )
        .with_timeouts(timeouts)
        .with_price_table(pt, sink_obj)
        .with_token_budget(budget)
        .with_channel_concurrency(concurrency)
}

/// Client-side view of the pipe: body errors surface as `axum::Error` items.
fn raw_data_stream(pipe: PipeStream) -> impl Stream<Item = Result<Bytes, axum::Error>> + Send {
    pipe.into_response().into_body().into_data_stream()
}

/// Walks the `source` error chain for an `io::Error` and returns its kind.
fn io_kind(source: &axum::Error) -> Option<std::io::ErrorKind> {
    let mut cursor: Option<&(dyn std::error::Error + 'static)> = Some(source);
    while let Some(err) = cursor {
        if let Some(io_err) = err.downcast_ref::<std::io::Error>() {
            return Some(io_err.kind());
        }
        cursor = err.source();
    }
    None
}

/// The terminal stream error must wrap `io::ErrorKind::TimedOut`.
fn assert_timed_out(err: &axum::Error) {
    assert_eq!(
        io_kind(err),
        Some(std::io::ErrorKind::TimedOut),
        "terminal stream error must be io::ErrorKind::TimedOut, got {err:?}"
    );
}

/// One SSE frame, then permanent silence: the idle arm must fire.
fn silent_after_one_frame() -> TestStream {
    let frame = Ok::<Bytes, std::io::Error>(Bytes::from_static(b"data: {\"id\":\"chunk-1\"}\n\n"));
    Box::pin(
        futures_util::stream::once(async move { frame })
            .chain(futures_util::stream::pending::<Result<Bytes, std::io::Error>>()),
    )
}

/// A frame every 10 ms, forever, never EOF: the max arm must fire first.
fn trickle_forever() -> TestStream {
    Box::pin(futures_util::stream::unfold(0u64, |_| async move {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        Some((
            Ok::<Bytes, std::io::Error>(Bytes::from_static(b"data: {\"id\":\"tick\"}\n\n")),
            0u64,
        ))
    }))
}

/// Five frames about 20 ms apart, then clean EOF: the idle sleep is re-armed on
/// every chunk and must not misfire before the stream ends.
fn paced_then_eof() -> TestStream {
    let frames = vec![
        Bytes::from_static(b"data: {\"id\":\"chunk-1\"}\n\n"),
        Bytes::from_static(b"data: {\"id\":\"chunk-2\"}\n\n"),
        Bytes::from_static(b"data: {\"id\":\"chunk-3\"}\n\n"),
        Bytes::from_static(b"data: {\"id\":\"chunk-4\"}\n\n"),
        Bytes::from_static(b"data: {\"id\":\"chunk-5\"}\n\n"),
    ];
    Box::pin(futures_util::stream::unfold(
        frames,
        |mut frames| async move {
            if frames.is_empty() {
                return None;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            let frame = Ok(frames.remove(0));
            Some((frame, frames))
        },
    ))
}

/// Streaming ctx: `"stream":true` body, route pre-seeded (retry mode ignores it,
/// but the builder requires one).
fn stream_ctx() -> gateway_pipeline::RequestCtx {
    ctx_with_body(STREAM_BODY, candidate("c1"))
}

/// Runs the stage and returns the sink, concurrency gate, token budget and the
/// client-side pipe for post-stream assertions.
async fn run_case(
    egress: Arc<dyn Egress>,
    timeouts: Timeouts,
) -> (
    Arc<VecSink>,
    Arc<ConcurrencyState>,
    Arc<TokenBudgetState>,
    impl Stream<Item = Result<Bytes, axum::Error>> + Send,
) {
    let sink: Arc<VecSink> = Arc::new(VecSink::default());
    let concurrency: Arc<ConcurrencyState> = Arc::new(ConcurrencyState::default());
    concurrency.register_channel(CH_C1, 1);
    let budget: Arc<TokenBudgetState> = TokenBudgetState::with_clock(move || 0u64);
    budget.register_channel(CH_C1, 10_000);
    let stage = stage_with(
        egress,
        timeouts,
        Arc::clone(&sink),
        Arc::clone(&concurrency),
        Arc::clone(&budget),
    );
    let mut ctx = stream_ctx();
    let pipe = expect_stream(&stage, &mut ctx).await;
    (sink, concurrency, budget, raw_data_stream(pipe))
}

async fn expect_stream(stage: &ForwardStage, ctx: &mut gateway_pipeline::RequestCtx) -> PipeStream {
    match stage.handle(ctx).await.expect("handle should succeed") {
        StageOutcome::Stream(pipe) => pipe,
        other => panic!("streaming request should return Stream, got {other:?}"),
    }
}

// ---------- case 1: idle arm ----------

/// One frame then permanent silence: `stream_idle_ms=50ms` fires while
/// `stream_max_ms` stays dormant; the client stream ends with a `TimedOut` error
/// item and a 500 settle event carrying produced token counts.
#[tokio::test]
async fn idle_timeout_ends_stream_with_timed_out_and_settles_500() {
    let timeouts = Timeouts {
        stream_idle_ms: 50,
        stream_max_ms: 600_000,
        ..Timeouts::default()
    };
    let egress: Arc<dyn Egress> =
        Arc::new(MockEgress::new(200, Arc::new(|| silent_after_one_frame())));
    let (sink, concurrency, budget, mut data) = run_case(egress, timeouts).await;

    // The first frame arrives immediately.
    let first = data
        .next()
        .await
        .expect("first frame should arrive")
        .unwrap();
    assert_eq!(first, Bytes::from_static(b"data: {\"id\":\"chunk-1\"}\n\n"));

    // Silence: the stream ends with a TimedOut error item, then terminates.
    let err_item = data
        .next()
        .await
        .expect("stream should end with an error item");
    assert_timed_out(&err_item.unwrap_err());
    assert!(
        data.next().await.is_none(),
        "stream should terminate after the error item"
    );

    // Settlement: exactly one event, status 500, idle-timeout file, token counts.
    let events = sink.events();
    assert_eq!(events.len(), 1, "exactly one settlement event");
    let ev = &events[0];
    assert_eq!(ev.status_code, 500);
    assert_eq!(ev.error.as_deref(), Some("stream idle timeout"));
    assert!(
        ev.completion_tokens > 0,
        "settle event should carry produced token counts, got {ev:?}"
    );
    assert_eq!(ev.group_id, "g");

    // RAII release: channel permit and token reservation returned at stream end.
    assert_eq!(
        concurrency.available_permits(CH_C1),
        Some(1),
        "channel permit should be returned after the stream ends"
    );
    assert_eq!(
        budget.inflight(CH_C1),
        0,
        "token inflight should return to 0"
    );
}

// ---------- case 2: max arm ----------

/// Trickle forever at 10 ms/frame: `stream_max_ms=500ms` fires before the
/// 10 s idle budget; identical `TimedOut` end and 500 settle. The 500 ms budget
/// keeps a 50x margin over the 10 ms frame period so even a heavily loaded CI
/// worker delivers at least one frame before the deadline.
#[tokio::test]
async fn max_duration_timeout_ends_stream_with_timed_out() {
    let timeouts = Timeouts {
        stream_idle_ms: 10_000,
        stream_max_ms: 500,
        ..Timeouts::default()
    };
    let egress: Arc<dyn Egress> = Arc::new(MockEgress::new(200, Arc::new(|| trickle_forever())));
    let (sink, concurrency, budget, mut data) = run_case(egress, timeouts).await;

    // A few frames arrive, then the max arm fires.
    let mut frames = 0u64;
    loop {
        match data.next().await {
            Some(Ok(_)) => frames += 1,
            Some(Err(err)) => {
                assert_timed_out(&err);
                break;
            }
            None => {
                panic!("stream should end with a TimedOut error, not a clean EOF")
            }
        }
    }
    // Drain once more: the terminal state (with its permit / token guards) is
    // released when the unfold observes the settled flag, i.e. on the poll
    // after the error item.
    assert!(
        data.next().await.is_none(),
        "stream should terminate after the error item"
    );
    assert!(
        frames > 0,
        "some frames should arrive before the max deadline"
    );

    let events = sink.events();
    assert_eq!(events.len(), 1);
    let ev = &events[0];
    assert_eq!(ev.status_code, 500);
    assert_eq!(ev.error.as_deref(), Some("stream max duration exceeded"));
    assert!(ev.completion_tokens > 0);

    assert_eq!(concurrency.available_permits(CH_C1), Some(1));
    assert_eq!(budget.inflight(CH_C1), 0);
}

// ---------- case 3: regression ----------

/// Five frames ~20 ms apart then clean EOF: with `stream_idle_ms=60ms` the idle
/// budget would fire mid-stream without the per-chunk reset; with it, the
/// stream settles normally at 200.
#[tokio::test]
async fn paced_frames_do_not_misfire_idle_timeout() {
    let timeouts = Timeouts {
        stream_idle_ms: 60,
        stream_max_ms: 10_000,
        ..Timeouts::default()
    };
    let egress: Arc<dyn Egress> = Arc::new(MockEgress::new(200, Arc::new(|| paced_then_eof())));
    let (sink, concurrency, budget, mut data) = run_case(egress, timeouts).await;

    let mut frames = 0u64;
    while let Some(chunk) = data.next().await {
        chunk.expect("body chunk should not error");
        frames += 1;
        if frames == 1 {
            // While the stream is mid-flight the permit is still held.
            assert_eq!(
                concurrency.available_permits(CH_C1),
                Some(0),
                "permit should be held while the stream is open"
            );
        }
    }
    assert_eq!(frames, 5, "all five frames should arrive, then a clean EOF");

    // Clean EOF settles at 200 with no error.
    let events = sink.events();
    assert_eq!(events.len(), 1);
    let ev = &events[0];
    assert_eq!(ev.status_code, 200);
    assert_eq!(ev.error, None);
    assert!(ev.completion_tokens > 0);

    assert_eq!(concurrency.available_permits(CH_C1), Some(1));
    assert_eq!(budget.inflight(CH_C1), 0);
}
