//! Failure telemetry integration tests — a failed upstream attempt lands a **zero-cost
//! observation event**.
//!
//! Coverage (the contract of `ForwardStage::submit_failed` (`pub(crate)`) in
//! `gateway_forward::stage_telemetry`):
//! 1. single-shot mode: forward_task returns `Err(NormalizedError)` → the sink receives
//!    exactly 1 event with status=502, cost=0, all-zero counts and a non-empty error,
//!    and StageError still short-circuits as usual;
//! 2. retry mode: after the budget is exhausted, **one event per failed attempt**
//!    (retries leave an observation trace too);
//! 3. Fatal 4xx: a single attempt terminates, still producing 1 observation event and
//!    never touching the second candidate;
//! 4. `is_stream` records the request intent: false for a failed non-streaming request,
//!    true for a failed streaming request;
//! 5. with no pt/sink mounted, the failure path behaves unchanged (still errors, emits
//!    no event, does not panic).
//!
//! Mock style matches retry_wiring.rs: hand-written Dispatch + scripted Egress, no real
//! network (the mock and request builders are shared in `tests/common/mod.rs`; this
//! file only builds failure plans).

mod common;

use app_contract::records::UsageEventRecord;
use gateway_dispatch::RetryPolicy;
use gateway_forward::ForwardStage;
use gateway_metering::pricing::{ModelPrice, PriceTable};
use gateway_pipeline::{Stage, StageError, UpstreamError};
use std::sync::Arc;

use common::*;

// ---------- Test helpers ----------

/// Fixed price table: proves the zero cost comes from all-zero counts, not from "no
/// price attached".
struct FixedPriceTable;

impl PriceTable for FixedPriceTable {
    fn lookup(&self, _model: &str, _group: &str) -> Option<ModelPrice> {
        Some(ModelPrice {
            input: 15.0,
            output: 60.0,
            cache: 0.0,
            group_multiplier: 1.0,
        })
    }
}

/// Stage constructor with a fake pt/sink attached; returns the stage and the sink
/// handle.
fn priced_stage(egress: Arc<ScriptedEgress>) -> (ForwardStage, Arc<VecSink>) {
    let sink = Arc::new(VecSink::default());
    let adaptors = Arc::new(gateway_protocol_bridge::format_codec::FormatRegistry::new());
    let stage = ForwardStage::new(egress, adaptors)
        .with_price_table(Arc::new(FixedPriceTable), sink.clone());
    (stage, sink)
}

/// Baseline assertions for a failure observation event: all-zero counts, cost=0, and
/// attribution keys from the same source as the success path.
fn assert_zero_cost_observation(ev: &UsageEventRecord, unit_key: &str) {
    assert_eq!(
        (ev.prompt_tokens, ev.completion_tokens, ev.cached_tokens),
        (0, 0, 0),
        "observation event counts must all be 0"
    );
    assert_eq!(ev.cost, 0, "observation events must not generate billing");
    // user_key is the user UUID (after the stage fix it comes from ctx.token.user_key);
    // token_key is tok-1
    assert_eq!(ev.user_key, "user-test");
    assert_eq!(ev.token_key, "tok-1");
    assert_eq!(ev.channel_key, format!("ch-{unit_key}"));
    assert_eq!(ev.route_unit_key, unit_key);
    assert_eq!(ev.public_model, "m");
    assert_eq!(ev.upstream_model, "m");
}

// ---------- Case 1: single-shot failure → 1 observation event + usual short-circuit ----------

#[tokio::test]
async fn single_shot_failure_emits_one_zero_cost_event() {
    let c1 = candidate("c1");
    let egress = Arc::new(ScriptedEgress::new(vec![(
        "upstream-c1",
        Plan::Fail {
            status: 502,
            retryable: true,
        },
    )]));
    let (stage, sink) = priced_stage(egress.clone());

    let mut ctx = ctx_with_body(NON_STREAM_BODY, c1);
    let err = stage
        .handle(&mut ctx)
        .await
        .expect_err("upstream 502 should short-circuit to StageError");
    match err {
        StageError::Upstream(UpstreamError::Status { code, .. }) => assert_eq!(code, 502),
        other => panic!("expected Upstream 502, got {other:?}"),
    }

    let events = sink.events();
    assert_eq!(
        events.len(),
        1,
        "failed request must leave 1 observation trace"
    );
    let ev = &events[0];
    assert_zero_cost_observation(ev, "c1");
    assert_eq!(ev.status_code, 502);
    assert_eq!(
        ev.error.as_deref(),
        Some("upstream 502"),
        "error must be non-empty"
    );
    assert!(
        !ev.is_stream,
        "failed non-streaming request: is_stream=false"
    );
    assert_eq!(egress.calls().len(), 1);
}

// ---------- Case 2: budget exhausted → one event per failed attempt ----------

#[tokio::test]
async fn retry_exhaustion_records_each_failed_attempt() {
    let dispatch = Arc::new(MockDispatch::new(vec![candidate("c1"), candidate("c2")]));
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
            Plan::Fail {
                status: 502,
                retryable: true,
            },
        ),
    ]));
    let (stage, sink) = priced_stage(egress.clone());
    let stage = stage.with_retry(
        dispatch,
        RetryPolicy {
            max_attempts: 2,
            total_budget_ms: 0,
        },
    );

    let mut ctx = ctx_with_body(NON_STREAM_BODY, candidate("c1"));
    let err = stage
        .handle(&mut ctx)
        .await
        .expect_err("budget exhaustion should error");
    assert!(
        matches!(err, StageError::Unavailable { .. }),
        "budget exhaustion (count) should map to retryable Unavailable(504), got {err:?}"
    );

    let events = sink.events();
    assert_eq!(
        events.len(),
        2,
        "every failed attempt within the retry budget must leave a trace"
    );
    assert_zero_cost_observation(&events[0], "c1");
    assert_zero_cost_observation(&events[1], "c2");
    assert!(
        events
            .iter()
            .all(|e| e.status_code == 502 && e.error.is_some()),
        "both should be 502 and carry an error summary: {events:?}"
    );
    assert!(
        events.iter().all(|e| !e.is_stream),
        "both failures of the non-streaming request should be is_stream=false"
    );
    assert_eq!(egress.calls().len(), 2);
}

// ---------- Case 3: Fatal 4xx also leaves a trace without touching the second candidate ----------

#[tokio::test]
async fn fatal_4xx_records_one_event_and_short_circuits() {
    let dispatch = Arc::new(MockDispatch::new(vec![candidate("c1"), candidate("c2")]));
    let egress = Arc::new(ScriptedEgress::new(vec![
        (
            "upstream-c1",
            Plan::Fail {
                status: 400,
                retryable: false,
            },
        ),
        (
            "upstream-c2",
            Plan::Fail {
                status: 502,
                retryable: true,
            },
        ),
    ]));
    let (stage, sink) = priced_stage(egress.clone());
    let stage = stage.with_retry(dispatch, RetryPolicy::default());

    let mut ctx = ctx_with_body(NON_STREAM_BODY, candidate("c1"));
    let err = stage
        .handle(&mut ctx)
        .await
        .expect_err("fatal 4xx should propagate the error");
    match err {
        StageError::Upstream(UpstreamError::Status { code, .. }) => assert_eq!(code, 400),
        other => panic!("expected Upstream 400, got {other:?}"),
    }

    let events = sink.events();
    assert_eq!(
        events.len(),
        1,
        "fatal termination still leaves an observation trace"
    );
    assert_zero_cost_observation(&events[0], "c1");
    assert_eq!(events[0].status_code, 400);
    assert_eq!(events[0].error.as_deref(), Some("upstream 400"));
    assert_eq!(egress.calls().len(), 1, "fatal must not touch c2");
}

// ---------- Case 4: streaming request failure → is_stream=true ----------

#[tokio::test]
async fn streaming_request_failure_marks_is_stream_true() {
    let egress = Arc::new(ScriptedEgress::new(vec![(
        "upstream-c1",
        Plan::Fail {
            status: 502,
            retryable: true,
        },
    )]));
    let (stage, sink) = priced_stage(egress.clone());

    let mut ctx = ctx_with_body(STREAM_BODY, candidate("c1"));
    let err = stage
        .handle(&mut ctx)
        .await
        .expect_err("should short-circuit with an error");
    assert!(
        matches!(
            err,
            StageError::Upstream(UpstreamError::Status { code: 502, .. })
        ),
        "got {err:?}"
    );

    let events = sink.events();
    assert_eq!(events.len(), 1);
    assert!(
        events[0].is_stream,
        "is_stream records request intent: failed streaming request should still be true"
    );
    assert_eq!(events[0].cost, 0);
    assert_eq!(events[0].status_code, 502);
}

// ---------- Case 5: no pt/sink mounted → behavior matches pre-billing ----------

#[tokio::test]
async fn failure_without_sink_still_short_circuits() {
    let egress = Arc::new(ScriptedEgress::new(vec![(
        "upstream-c1",
        Plan::Fail {
            status: 502,
            retryable: true,
        },
    )]));
    let adaptors = Arc::new(gateway_protocol_bridge::format_codec::FormatRegistry::new());
    let stage = ForwardStage::new(egress.clone(), adaptors); // no with_price_table

    let mut ctx = ctx_with_body(NON_STREAM_BODY, candidate("c1"));
    let err = stage
        .handle(&mut ctx)
        .await
        .expect_err("no pt/sink mounted: error semantics unchanged");
    assert!(
        matches!(
            err,
            StageError::Upstream(UpstreamError::Status { code: 502, .. })
        ),
        "got {err:?}"
    );
    assert_eq!(egress.calls().len(), 1);
}
