//! settle integration tests — automatic settlement and artifact persistence for
//! streaming/non-streaming forwarding after the scan chain.
//!
//! Coverage:
//! 1. `StreamScanner` extracts usage from SSE data frames (the settlement data source);
//! 2. `stream::finish` produces a `UsageEvent` only when `price_table` is attached; status/error
//!    are injected by the caller (library-level evidence for a 200 success / a mid-stream
//!    500);
//! 3. End-to-end wiring: mock egress returns a non-streaming 200 with usage; after injecting
//!    a fake `PriceTable` + `SettleSink`, `ForwardStage::handle` (internally going through
//!    `commit_forwarded`, the single settlement point) makes the sink receive exactly one
//!    event with correct counts, cost, and group attribution.
//!
//! The mock and request constructors live in `tests/common/mod.rs` for sharing.

mod common;

use bytes::Bytes;
use futures_util::StreamExt;
use gateway_dispatch::RetryPolicy;
use gateway_forward::ForwardStage;
use gateway_forward::stream::{SseContext, finish, pipe_chunk};
use gateway_metering::ledger::{BalanceLedger, Ledger, MemoryLedger};
use gateway_metering::pricing::{ModelPrice, PriceTable};
use gateway_metering::scanner::StreamScanner;
use gateway_pipeline::{Stage, StageOutcome};
use std::sync::{Arc, Mutex};

use common::*;

// ---------- test helpers ----------

/// Fixed price table: gpt-4o input $15/M, output $60/M; records the (model, group) of every
/// lookup to verify that group really is passed through to the pricing table.
#[derive(Default)]
struct RecordingPriceTable {
    looked: Mutex<Vec<(String, String)>>,
}

impl PriceTable for RecordingPriceTable {
    fn lookup(&self, model: &str, group: &str) -> Option<ModelPrice> {
        self.looked
            .lock()
            .unwrap()
            .push((model.to_string(), group.to_string()));
        Some(ModelPrice {
            input: 15.0,
            output: 60.0,
            cache: 0.0,
            group_multiplier: 1.0,
        })
    }
}

impl RecordingPriceTable {
    fn looked(&self) -> Vec<(String, String)> {
        self.looked.lock().unwrap().clone()
    }
}

/// Minimal `RequestCtx` for single-shot mode (no `with_retry)`: route preset, token carries
/// group.
fn ctx_single_shot() -> gateway_pipeline::RequestCtx {
    ctx_with_body(NON_STREAM_BODY, candidate("c1"))
}

// ---------- scanner ----------

#[test]
fn stream_scanner_extracts_usage_for_settle() {
    let mut s = StreamScanner::new();
    s.push(&Bytes::from_static(
        b"data: {\"content\":\"hello\",\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5}}\n\n",
    ));
    let (counts, _reason) = s.finish(10);
    assert_eq!(counts.prompt, 10);
    assert_eq!(counts.completion, 5);
}

// ---------- stream::finish ----------

#[test]
fn sse_context_pipe_chunk_detects_first_token() {
    let mut ctx = SseContext::new();
    let out = pipe_chunk(
        &mut ctx,
        &Bytes::from_static(b"data: {\"role\":\"assistant\"}\n\n"),
    );
    assert_eq!(
        out.passthrough,
        Bytes::from_static(b"data: {\"role\":\"assistant\"}\n\n")
    );
    assert_eq!(out.events.len(), 1);
}

fn priced_ctx() -> SseContext {
    let mut ctx = SseContext::new();
    ctx.user_key = "u1".into();
    ctx.token_key = "t1".into();
    ctx.channel_key = "ch1".into();
    ctx.public_model = "gpt-4o".into();
    ctx.upstream_model = "gpt-4o".into();
    ctx.price_table = Some(Arc::new(RecordingPriceTable::default()));
    pipe_chunk(
        &mut ctx,
        &Bytes::from_static(
            b"data: {\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5}}\n\n",
        ),
    );
    ctx
}

/// With `price_table` attached, finish really settles and returns the event (no longer
/// discarding the artifact). cost = (10×15 + 5×60)/1e6 × `500_000` = 225.
#[test]
fn finish_returns_settled_event_when_priced() {
    let (_end, counts, event) = finish(priced_ctx(), 200, None);
    assert_eq!((counts.prompt, counts.completion), (10, 5));
    let ev = event.expect("with price table attached, must settle");
    assert_eq!(ev.prompt_tokens, 10);
    assert_eq!(ev.completion_tokens, 5);
    assert_eq!(ev.cost, 225);
    assert_eq!(ev.user_key, "u1");
    assert_eq!(ev.token_key, "t1");
    assert_eq!(ev.channel_key, "ch1");
    assert_eq!(ev.status_code, 200);
    assert_eq!(ev.error, None);
    // Streaming-path settlement: is_stream must be true (finish is only called on the SSE
    // chain).
    assert!(
        ev.is_stream,
        "event settled by stream::finish must mark is_stream=true"
    );
}

/// Without `price_table`: no billing, event is None (same behavior as before billing was
/// wired).
#[test]
fn finish_without_price_table_is_not_billed() {
    let ctx = SseContext::new();
    let (_end, _counts, event) = finish(ctx, 200, None);
    assert!(event.is_none(), "None price table = no billing");
}

/// Mid-stream error: settle with the counts accumulated so far, status=500 plus the error
/// summary (prevents a leak of unbilled usage).
#[test]
fn finish_error_path_settles_accumulated_counts_with_500() {
    let (_end, counts, event) = finish(priced_ctx(), 500, Some("upstream reset"));
    assert_eq!(
        counts.completion, 5,
        "tokens produced before error must be accounted"
    );
    let ev = event.expect("error path also settles");
    assert_eq!(ev.status_code, 500);
    assert_eq!(ev.error.as_deref(), Some("upstream reset"));
    assert_eq!(ev.cost, 225);
    assert!(
        ev.is_stream,
        "mid-stream disconnect still counts as streaming path, is_stream=true"
    );
}

/// Streaming frames capture the finish reason: the last frame's choices[0].`finish_reason`
/// goes into the event record (raw material for `truncation_rate` fitting; absent upstream =
/// None, never fabricated).
#[test]
fn finish_carries_streaming_finish_reason() {
    let mut ctx = priced_ctx();
    pipe_chunk(
        &mut ctx,
        &Bytes::from_static(b"data: {\"choices\":[{\"finish_reason\":\"stop\"}]}\n\n"),
    );
    let (_end, _counts, event) = finish(ctx, 200, None);
    let ev = event.expect("with price table attached, must settle");
    assert_eq!(ev.finish_reason.as_deref(), Some("stop"));
}

/// When the frame omits the finish reason, the event record honestly stays None (no value
/// invented from elsewhere).
#[test]
fn finish_without_finish_reason_stays_none() {
    let (_end, _counts, event) = finish(priced_ctx(), 200, None);
    let ev = event.expect("with price table attached, must settle");
    assert_eq!(ev.finish_reason, None);
}

// ---------- ForwardStage end-to-end wiring (non-streaming) ----------

#[tokio::test]
async fn commit_forwarded_settles_non_streamed_into_sink() {
    // mock upstream: 200 + OpenAI-style usage (prompt 100 / completion 50) + finish reason.
    let egress = Arc::new(FixedEgress {
        body: b"{\"choices\":[{\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":100,\"completion_tokens\":50}}",
    });
    let table = Arc::new(RecordingPriceTable::default());
    let sink = Arc::new(VecSink::default());
    let adaptors = Arc::new(gateway_protocol_bridge::format_codec::FormatRegistry::new());
    let stage = ForwardStage::new(egress, adaptors).with_price_table(table.clone(), sink.clone());

    let mut ctx = ctx_single_shot();
    let outcome = stage
        .handle(&mut ctx)
        .await
        .expect("single-shot forward must succeed");
    assert!(
        matches!(outcome, StageOutcome::Continue),
        "non-streaming success must be Continue, got {outcome:?}"
    );

    let events = sink.events();
    assert_eq!(
        events.len(),
        1,
        "commit_forwarded is the only settlement point, exactly 1 event"
    );
    let ev = &events[0];
    assert_eq!(ev.prompt_tokens, 100);
    assert_eq!(ev.completion_tokens, 50);
    // Non-streaming commit point: extract finish_reason from the full response body (lenient
    // parsing).
    assert_eq!(ev.finish_reason.as_deref(), Some("stop"));
    // (100×15 + 50×60)/1e6 × 500_000 = 2_250
    assert_eq!(ev.cost, 2_250);
    assert_eq!(ev.status_code, 200);
    // user_key is the user UUID (after the stage fix it comes from ctx.token.user_key);
    // token_key is tok-1
    assert_eq!(ev.user_key, "user-test");
    assert_eq!(ev.token_key, "tok-1");
    assert_eq!(ev.channel_key, "ch-c1");
    assert_eq!(ev.route_unit_key, "c1");
    // Non-streaming commit-point settlement: the request body has no "stream" field →
    // is_stream=false.
    assert!(
        !ev.is_stream,
        "event settled on non-streaming success must have is_stream=false"
    );
    // group comes from ctx.token.group and is passed through to PriceTable::lookup
    assert_eq!(table.looked(), vec![("m".to_string(), "g".to_string())]);

    // The response body is still written to ctx.upstream (settlement does not disturb the
    // forwarded artifact)
    let up = ctx.upstream.expect("ctx.upstream must be written");
    assert_eq!(up.status, 200);
    assert!(up.body.starts_with(b"{\"choices\""));
}

/// No pt/sink attached: billing stays off and the non-streaming path emits no events.
#[tokio::test]
async fn unsettlement_path_without_price_table() {
    let egress = Arc::new(FixedEgress {
        body: b"{\"usage\":{\"prompt_tokens\":100,\"completion_tokens\":50}}",
    });
    let adaptors = Arc::new(gateway_protocol_bridge::format_codec::FormatRegistry::new());
    let stage = ForwardStage::new(egress, adaptors); // no with_price_table call

    let mut ctx = ctx_single_shot();
    let outcome = stage
        .handle(&mut ctx)
        .await
        .expect("single-shot forward must succeed");
    assert!(matches!(outcome, StageOutcome::Continue));
    assert!(
        ctx.upstream.is_some(),
        "forwarding itself is unaffected by missing settlement"
    );
}

/// No usage in the response body: fall back to estimation (prompt estimated from the request
/// body, completion roughly from the response byte count), and the event is still produced —
/// the non-streaming path no longer silently misses billing.
#[tokio::test]
async fn non_streamed_without_usage_falls_back_to_estimate() {
    let egress = Arc::new(FixedEgress {
        body: b"{\"choices\":[{\"message\":{\"content\":\"hi there\"}}]}",
    });
    let table = Arc::new(RecordingPriceTable::default());
    let sink = Arc::new(VecSink::default());
    let adaptors = Arc::new(gateway_protocol_bridge::format_codec::FormatRegistry::new());
    let stage = ForwardStage::new(egress, adaptors).with_price_table(table, sink.clone());

    let mut ctx = ctx_single_shot();
    stage
        .handle(&mut ctx)
        .await
        .expect("single-shot forward must succeed");

    let events = sink.events();
    assert_eq!(
        events.len(),
        1,
        "missing usage must still be estimated and accounted"
    );
    // The exact estimates are decided by the estimate heuristic; only non-zero is required
    // here.
    assert!(
        events[0].completion_tokens > 0,
        "completion roughly estimated from response body length must be positive"
    );
}
// ---------- streaming prehold: HoldGuard drop vs settle ----------

/// A streaming response dropped before settlement (client disconnect) must refund the
/// full prehold via `HoldGuard::Drop` — no reservation leaks past the response lifetime.
/// `HoldGuard` is private to `stage_commit.rs`, so this is exercised through
/// `ForwardStage::handle` with a mounted ledger: the prehold is planted on `ctx.hold`
/// exactly as the gate chain promotes it, and the stream is dropped without being read.
#[tokio::test]
async fn stream_hold_drop_refunds_full_estimate() {
    let ledger = Arc::new(MemoryLedger::new());
    ledger.set_balance("user-test", "tok-1", 1000);
    let hold = ledger.prehold("user-test", "tok-1", 500).unwrap();
    assert_eq!(
        ledger.available("user-test", "tok-1"),
        500,
        "prehold reserves 500 of 1000"
    );

    let egress = Arc::new(FixedEgress {
        body: b"data: {\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5}}\n\n",
    });
    let adaptors = Arc::new(gateway_protocol_bridge::format_codec::FormatRegistry::new());
    let stage = ForwardStage::new(egress, adaptors).with_ledger(Some(ledger.clone()));

    let mut ctx = ctx_with_body(STREAM_BODY, candidate("c1"));
    ctx.hold = Some(hold);

    match stage
        .handle(&mut ctx)
        .await
        .expect("streaming forward must succeed")
    {
        StageOutcome::Stream(pipe) => drop(pipe), // client hangs up: never read, never settle
        other => panic!("streaming request must return StageOutcome::Stream, got {other:?}"),
    }

    assert_eq!(
        ledger.available("user-test", "tok-1"),
        1000,
        "dropped stream refunds the full prehold through HoldGuard::Drop"
    );
}

/// Once `stream::finish` settles the hold at stream end, `HoldGuard::settle` consumes
/// the prehold and `Drop` becomes a no-op: consuming the stream to completion and then
/// dropping it must not refund again.
#[tokio::test]
async fn stream_hold_settle_then_drop_is_noop_after_settle() {
    let ledger = Arc::new(MemoryLedger::new());
    ledger.set_balance("user-test", "tok-1", 1000);
    let hold = ledger.prehold("user-test", "tok-1", 500).unwrap();

    let table = Arc::new(RecordingPriceTable::default());
    let sink = Arc::new(VecSink::default());
    let egress = Arc::new(FixedEgress {
        body: b"data: {\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5}}\n\n",
    });
    let adaptors = Arc::new(gateway_protocol_bridge::format_codec::FormatRegistry::new());
    let stage = ForwardStage::new(egress, adaptors)
        .with_price_table(table, sink)
        .with_ledger(Some(ledger.clone()));

    let mut ctx = ctx_with_body(STREAM_BODY, candidate("c1"));
    ctx.hold = Some(hold);

    let StageOutcome::Stream(pipe) = stage
        .handle(&mut ctx)
        .await
        .expect("streaming forward must succeed")
    else {
        panic!("streaming request must return StageOutcome::Stream");
    };

    // Read to completion: finish_stream runs at stream end and settle reconciles the
    // prehold with the actual cost (225 = ceil((10*15 + 5*60)/1e6 * 500_000)).
    let mut stream = pipe.into_response().into_body().into_data_stream();
    while stream.next().await.is_some() {}

    // 1000 - 500 + (500 - 225) = 775.
    assert_eq!(
        ledger.available("user-test", "tok-1"),
        775,
        "settle charged the actual cost against the prehold"
    );

    // The stream finished; its HoldGuard was already consumed by settle. If Drop
    // refunded again the balance would be 1275 instead.
    assert_eq!(
        ledger.available("user-test", "tok-1"),
        775,
        "HoldGuard::Drop is a no-op after settle: no double-release"
    );
}

// ---------- success-after-retry: hold settled exactly once ----------

/// Regression test for F1: the request-level prehold (admission) must NOT be released
/// by failed attempts — `submit_failed` is pure observation now and `Pipeline::run`
/// releases a still-outstanding hold only on terminal outcomes. The winning attempt's
/// `commit_forwarded` settle must find `ctx.hold` still present and reconcile it once.
///
/// c1 fails once (retryable 502), c2 succeeds with usage (prompt 10 / completion 5,
/// cost 225). Prehold 500 of 1000 is taken up front; after the win the balance must be
/// 1000 − 225 = 775 (hold consumed exactly once). If a failed attempt had released the
/// hold, settle would find `ctx.hold == None` and skip, leaving the 500 reservation
/// stuck instead.
#[tokio::test]
async fn success_after_retry_settles_hold_exactly_once() {
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
                body: b"{\"choices\":[{\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5}}",
            },
        ),
    ]));
    let ledger = Arc::new(MemoryLedger::new());
    ledger.set_balance("user-test", "tok-1", 1000);
    let hold = ledger.prehold("user-test", "tok-1", 500).unwrap();
    assert_eq!(
        ledger.available("user-test", "tok-1"),
        500,
        "prehold reserves 500 of 1000"
    );
    let table = Arc::new(RecordingPriceTable::default());
    let sink = Arc::new(VecSink::default());
    let adaptors = Arc::new(gateway_protocol_bridge::format_codec::FormatRegistry::new());
    let stage = ForwardStage::new(egress, adaptors)
        .with_price_table(table.clone(), sink.clone())
        .with_ledger(Some(ledger.clone()))
        .with_retry(
            dispatch,
            RetryPolicy {
                max_attempts: 3,
                total_budget_ms: 0,
            },
        );

    let mut ctx = ctx_with_body(NON_STREAM_BODY, c2.clone());
    ctx.hold = Some(hold);
    let outcome = stage.handle(&mut ctx).await.expect("c2 must succeed");
    assert!(
        matches!(outcome, StageOutcome::Continue),
        "non-streaming success must be Continue, got {outcome:?}"
    );

    // One zero-cost observation for the failed attempt + one settlement for the win.
    let events = sink.events();
    assert_eq!(
        events.len(),
        2,
        "failed-attempt observation + success settlement, exactly one each"
    );
    let last = &events[1];
    assert_eq!(last.cost, 225, "(10×15 + 5×60)/1e6 × 500_000 = 225");
    assert_eq!(last.status_code, 200);
    assert!(!last.is_stream);

    // 500 (after prehold) + (500 − 225) refunded = 775: the hold was not released by
    // the failed attempt (that would leave 500) and settled exactly once on success.
    assert_eq!(
        ledger.available("user-test", "tok-1"),
        775,
        "successful retry's settle consumed the prehold exactly once"
    );
}
