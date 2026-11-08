//! `ForwardStage::with_retry` wiring integration test — the retry loop + health reporting on the
//! production path.
//!
//! The core retry contract comes down to three behaviors:
//! 1. Switch candidates after a retryable failure: the second select's exclude must contain the
//!    tried candidate's key, the response comes from the winning candidate, and every attempt
//!    reports health via Dispatch::report;
//! 2. Budget exhaustion: all candidates fail retryably → Unavailable, which maps to 503 without
//!    Retry-After when no channel is cooling;
//! 3. Fatal (retryable=false) does not switch channels: a single attempt ends the request and
//!    the upstream status is passed through.
//!
//! Dispatch uses a hand-written mock (recording select's exclude sets and the report call
//! sequence); egress uses a scripted mock (returning fixed success bodies / classified errors per
//! candidate base_url). The mocks and request builders are shared in `tests/common/mod.rs`
//! (egress is injected through the Egress trait; no real network is used).

mod common;

use bytes::Bytes;
use gateway_dispatch::RetryPolicy;
use gateway_dispatch::health::FailureClass;
use gateway_forward::ForwardStage;
use gateway_pipeline::{Stage, StageError, StageOutcome, UpstreamError, error_to_response};
use std::sync::Arc;

use common::*;

// ---------- Test helpers ----------

fn stage_with_retry(
    egress: Arc<ScriptedEgress>,
    dispatch: Arc<MockDispatch>,
    max_attempts: u32,
) -> ForwardStage {
    let adaptors = Arc::new(gateway_protocol_bridge::format_codec::FormatRegistry::new());
    ForwardStage::new(egress, adaptors).with_retry(
        dispatch,
        RetryPolicy {
            max_attempts,
            total_budget_ms: 0,
        },
    )
}

// ---------- Case 1: c1 fails retryably → switch to c2 and succeed ----------

#[tokio::test]
async fn retryable_failure_switches_candidate_and_reports_health() {
    // Behavior assertion: c1 returns 502 (retryable) → the second select must put c1's key in
    // exclude; the final response comes from c2; report sequence = [c1: Err(Retryable),
    // c2: Ok(200)].
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
    let stage = stage_with_retry(egress.clone(), dispatch.clone(), 3);

    // route points at c2: if handle still read ctx.route, it would skip c1.
    let mut ctx = ctx_with_body(NON_STREAM_BODY, c2.clone());
    let outcome = stage.handle(&mut ctx).await.expect("c2 must succeed");
    assert!(
        matches!(outcome, StageOutcome::Continue),
        "non-streaming success must be Continue, got {outcome:?}"
    );

    let up = ctx.upstream.expect("ctx.upstream must be written");
    assert_eq!(
        up.status, 200,
        "winning attempt's status code must land in ctx"
    );
    assert_eq!(
        up.body,
        Bytes::from_static(b"{\"from\":\"c2\"}"),
        "response body must come from c2, not direct route-preset target hit"
    );

    let selects = dispatch.selects();
    assert_eq!(selects.len(), 2, "after c1 fails, must select again");
    assert!(selects[0].is_empty(), "first pick has no exclude set");
    assert_eq!(
        selects[1],
        vec!["c1".to_string()],
        "second select's exclude must contain tried c1's unit key"
    );

    assert_eq!(
        dispatch.reports(),
        vec![
            ("c1".to_string(), Err(FailureClass::Retryable)),
            ("c2".to_string(), Ok(200)),
        ],
        "every attempt must report health"
    );
}

// ---------- Case 2: all retryable failures → budget exhausted, Unavailable (503) ----------

#[tokio::test]
async fn all_retryable_failures_exhaust_budget_to_504() {
    // Both candidates return 502; after max_attempts=2 runs out, BudgetExhausted maps to
    // Unavailable with retry_after_ms=0 (no channel cooling), i.e. 503 without Retry-After; report
    // records two Err(Retryable).
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
            Plan::Fail {
                status: 502,
                retryable: true,
            },
        ),
    ]));
    let stage = stage_with_retry(egress.clone(), dispatch.clone(), 2);

    let mut ctx = ctx_with_body(NON_STREAM_BODY, c1.clone());
    let err = stage
        .handle(&mut ctx)
        .await
        .expect_err("budget exhaustion must error, not silently succeed");
    match err {
        StageError::Unavailable { retry_after_ms, .. } => {
            // Budget exhaustion (by attempt count) → Unavailable; both candidates failed without
            // cooling, so the cooling probe finds none and retry_after_ms = 0 (router: 503 without
            // Retry-After, resend immediately).
            assert_eq!(
                retry_after_ms, 0,
                "Retry-After must be 0 when no cooling channel"
            );
        }
        other => panic!("expected Unavailable(504), got {other:?}"),
    }
    assert!(
        ctx.upstream.is_none(),
        "failure path must not write ctx.upstream"
    );

    assert_eq!(
        dispatch.reports(),
        vec![
            ("c1".to_string(), Err(FailureClass::Retryable)),
            ("c2".to_string(), Err(FailureClass::Retryable)),
        ],
        "every failed attempt must report Retryable"
    );
    assert_eq!(
        egress.calls().len(),
        2,
        "two attempts each hit upstream once"
    );
}

// ---------- Case 3: c1 returns 4xx (Fatal) → no channel switch, status passed through ----------

#[tokio::test]
async fn fatal_4xx_does_not_switch_candidate() {
    // 4xx is not retryable → a channel switch cannot fix a client/protocol problem: a single
    // attempt ends the request, the upstream 400 passes through as Upstream(400), c2 is never
    // touched, and report has a single Fatal entry.
    let c1 = candidate("c1");
    let c2 = candidate("c2");
    let dispatch = Arc::new(MockDispatch::new(vec![c1.clone(), c2.clone()]));
    let egress = Arc::new(ScriptedEgress::new(vec![
        (
            "upstream-c1",
            Plan::Fail {
                status: 400,
                retryable: false,
            },
        ),
        ("upstream-c2", Plan::Ok { body: b"{}" }),
    ]));
    let stage = stage_with_retry(egress.clone(), dispatch.clone(), 3);

    let mut ctx = ctx_with_body(NON_STREAM_BODY, c1.clone());
    let err = stage
        .handle(&mut ctx)
        .await
        .expect_err("4xx must map to Upstream error");
    match err {
        StageError::Upstream(UpstreamError::Status { code, body_preview }) => {
            assert_eq!(
                code, 400,
                "upstream 4xx status must pass through, not swallowed into 502"
            );
            assert_eq!(body_preview, "upstream 400");
        }
        other => panic!("expected Upstream 400, got {other:?}"),
    }

    assert_eq!(
        dispatch.selects().len(),
        1,
        "Fatal must not trigger second route selection"
    );
    assert_eq!(
        dispatch.reports(),
        vec![("c1".to_string(), Err(FailureClass::Fatal))],
        "Fatal report must be recorded, otherwise health table stays empty forever"
    );
    let calls = egress.calls();
    assert_eq!(calls.len(), 1, "only one touch of c1 allowed");
    assert!(calls[0].contains("upstream-c1"), "c2 must not be requested");
}

// ---------- Case 4: write back ctx attribution after a failover win ----------

#[tokio::test]
async fn failover_winner_writes_back_ctx_attribution() {
    // Attribution contract (#140 chain): the channel_key persisted with usage comes from the
    // RouteAttribution that Pipeline::run packs from ctx.route/selected_channel_*. After retry
    // switches candidates, if ctx still held DispatchStage's initial pick (c1), usage would be
    // recorded against the failed channel — this case pins down that after a win, ctx.route /
    // selected_channel_key / selected_channel_name must all equal the winning candidate c2 (route
    // is pre-set to failed c1 to amplify any mismatch).
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
    let stage = stage_with_retry(egress.clone(), dispatch.clone(), 3);

    // route is pre-set to failed c1: if handle does not write back attribution, the assertion
    // catches it immediately.
    let mut ctx = ctx_with_route(c1.clone());
    let outcome = stage.handle(&mut ctx).await.expect("c2 must succeed");
    assert!(matches!(outcome, StageOutcome::Continue));

    // All three attribution fields must point at the winning candidate c2.
    assert_eq!(
        ctx.selected_channel_key.as_deref(),
        Some("ch-c2"),
        "winning candidate's channel_key must be written back to ctx, otherwise usage lands on the failed initial channel"
    );
    assert_eq!(
        ctx.selected_channel_name.as_deref(),
        Some("name-c2"),
        "display name comes from Dispatcher snapshot lookup (MockDispatch::channel_name)"
    );
    assert_eq!(
        ctx.route.as_ref().map(|r| r.unit.meta.key.as_str()),
        Some("c2"),
        "ctx.route must be replaced with the winning candidate; Pipeline::run's RouteAttribution takes it as authoritative"
    );
}

// ---------- Case 5: hot-updating the retry budget (ArcSwap hot handle, #311) ----------

#[tokio::test]
async fn update_retry_policy_hot_swaps_max_attempts() {
    // MockDispatch rotates through 5 candidates that all return 502 (retryable): with the default
    // max_attempts=5, budget exhaustion takes 5 upstream calls; after a hot swap to max_attempts=1
    // the same scenario is exhausted in one — proving the loop reads the hot handle's current
    // value, not a boot-time copy.
    let candidates = (0..5)
        .map(|i| candidate(&format!("c{i}")))
        .collect::<Vec<_>>();
    let dispatch = Arc::new(MockDispatch::new(candidates.clone()));
    let egress = Arc::new(ScriptedEgress::new(vec![
        (
            "upstream-c0",
            Plan::Fail {
                status: 502,
                retryable: true,
            },
        ),
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
        (
            "upstream-c3",
            Plan::Fail {
                status: 502,
                retryable: true,
            },
        ),
        (
            "upstream-c4",
            Plan::Fail {
                status: 502,
                retryable: true,
            },
        ),
    ]));
    let stage = stage_with_retry(egress.clone(), dispatch.clone(), 5);

    let mut ctx = ctx_with_body(NON_STREAM_BODY, candidates[0].clone());
    let _ = stage.handle(&mut ctx).await;
    assert_eq!(
        egress.calls().len(),
        5,
        "default budget of 5 attempts must hit upstream 5 times"
    );

    // Hot-swap the budget to 1: the next request exhausts it in a single attempt.
    stage.update_retry_policy(RetryPolicy {
        max_attempts: 1,
        total_budget_ms: 0,
    });

    let mut ctx = ctx_with_body(NON_STREAM_BODY, candidates[0].clone());
    let _ = stage.handle(&mut ctx).await;
    assert_eq!(
        egress.calls().len(),
        6,
        "after hot update, same scenario adds only 1 attempt (5+1)"
    );
}

// ---------- Case 6 (b+c e2e): all channels cooling → Unavailable{ra>0} → 504 upstream ----------

/// End to end: every candidate is cooling (select always returns
/// AllUnavailable{retry_after_ms=8000}, empty candidate pool). Under the b+c split this is
/// "cooling = worth waiting for", so the forward stage must surface
/// `StageError::Unavailable { retry_after_ms: 8000 }` (the router maps it to 504 + Retry-After:
/// 8s), not 503 (503 is only for no cooling). Also verifies S2 termination: with the budget off
/// (the helper's total_budget_ms=0) the gateway does not block waiting for cooling but returns
/// immediately (no infinite loop), with 0 upstream calls (select fails first).
#[tokio::test]
async fn all_cooling_channels_surface_unavailable_with_retry_after() {
    let c1 = candidate("c1");
    // Empty candidate pool + cooling ra=8000: select immediately returns AllUnavailable{ra:8000}.
    let dispatch = Arc::new(MockDispatch::new(Vec::new()).with_cooling_ra(8_000));
    let egress = Arc::new(ScriptedEgress::new(Vec::new()));
    let stage = stage_with_retry(egress.clone(), dispatch.clone(), 3);

    let mut ctx = ctx_with_body(NON_STREAM_BODY, c1.clone());
    let err = stage
        .handle(&mut ctx)
        .await
        .expect_err("all-cooling must error, not silently succeed");
    match err {
        StageError::Unavailable { retry_after_ms, .. } => {
            assert_eq!(
                retry_after_ms, 8_000,
                "cooling → Unavailable.retry_after_ms=8000 (upper: 504 + Retry-After: 8s)"
            );
        }
        other => panic!("expected Unavailable(cooldown, 504), got {other:?}"),
    }
    // S2 termination: budget off + cooling means the gateway does not block (no sleep waiting for
    // cooling); 0 upstream calls.
    assert_eq!(
        egress.calls().len(),
        0,
        "select fails first, never hit upstream"
    );
    assert!(
        ctx.upstream.is_none(),
        "failure path must not write ctx.upstream"
    );
}

// ---------- Case 7 (b+c e2e full chain): request → forward stage → StageError → 503/504 ----------

/// End to end (fills the Jev test_gap): a real `ForwardStage` (retry mode) request hits an **empty
/// candidate pool**; the forward stage emits `StageError::Unavailable`, then
/// `gateway_pipeline::error_to_response` maps it to HTTP, pinning down the whole **503/504 split**
/// chain:
/// - no cooling (`retry_after_ms=0`: all disabled/rate-limited/genuinely no model) → **503
///   unavailable**, no Retry-After;
/// - cooling (`retry_after_ms>0`: channels cooling) → **504 worth waiting + Retry-After:
///   ceil(ra/1000)**.
/// With the budget off (total_budget_ms=0) the gateway does not block waiting for cooling (S2
/// termination), so 0ms is non-blocking.
#[tokio::test]
async fn unavailable_end_to_end_maps_503_and_504() {
    let c1 = candidate("c1");
    let adaptors = Arc::new(gateway_protocol_bridge::format_codec::FormatRegistry::new());

    // (1) No cooling (empty pool + cooled_ra default 0) → 503 unavailable.
    let dispatch = Arc::new(MockDispatch::new(Vec::new()));
    let egress = Arc::new(ScriptedEgress::new(Vec::new()));
    let stage = ForwardStage::new(egress.clone(), adaptors.clone()).with_retry(
        dispatch.clone(),
        RetryPolicy {
            max_attempts: 3,
            total_budget_ms: 0,
        },
    );
    let mut ctx = ctx_with_body(NON_STREAM_BODY, c1.clone());
    let err = stage
        .handle(&mut ctx)
        .await
        .expect_err("all unavailable (no candidates) must error");
    let ra = match &err {
        StageError::Unavailable { retry_after_ms, .. } => retry_after_ms,
        other => panic!("expected Unavailable, got {other:?}"),
    };
    assert_eq!(*ra, 0, "no cooling → forward stage's retry_after_ms=0");
    assert_eq!(
        egress.calls().len(),
        0,
        "never hit upstream when no candidates"
    );
    let resp = error_to_response(err);
    assert_eq!(
        resp.status().as_u16(),
        503,
        "no cooling → 503 unavailable (not 504)"
    );
    assert!(
        resp.headers().get(http::header::RETRY_AFTER).is_none(),
        "no cooling → no Retry-After promised"
    );

    // (2) Cooling (empty pool + cooled_ra=8000) → 504 worth waiting + Retry-After: 8.
    let dispatch = Arc::new(MockDispatch::new(Vec::new()).with_cooling_ra(8_000));
    let egress = Arc::new(ScriptedEgress::new(Vec::new()));
    let stage = ForwardStage::new(egress.clone(), adaptors).with_retry(
        dispatch.clone(),
        RetryPolicy {
            max_attempts: 3,
            total_budget_ms: 0,
        },
    );
    let mut ctx = ctx_with_body(NON_STREAM_BODY, c1);
    let err = stage
        .handle(&mut ctx)
        .await
        .expect_err("all-cooling (budget off, non-blocking) must error");
    let ra = match &err {
        StageError::Unavailable { retry_after_ms, .. } => retry_after_ms,
        other => panic!("expected Unavailable, got {other:?}"),
    };
    assert_eq!(*ra, 8_000, "cooling → forward stage's retry_after_ms=8000");
    assert_eq!(
        egress.calls().len(),
        0,
        "select fails first, never hit upstream"
    );
    let resp = error_to_response(err);
    assert_eq!(resp.status().as_u16(), 504, "cooling → 504 worth waiting");
    assert_eq!(
        resp.headers()
            .get(http::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok()),
        Some("8"),
        "cooling → Retry-After = ceil(8000/1000)=8s"
    );
}
