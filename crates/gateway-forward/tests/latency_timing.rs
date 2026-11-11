//! Regression tests for **per-attempt timing** in `ForwardStage::with_retry`.
//!
//! Background bug: `handle_with_retry` created `started` before the retry loop began,
//! and both the success and failure health reports used `started.elapsed()` (loop-level
//! cumulative). During failover, the latency the second (successful) attempt reported to
//! dispatch health (the P2C latency factor) included the time of the **first slow failed
//! attempt** (e.g. a 200ms slow attempt followed by an instant success → ~200ms
//! reported), polluting the channel's peak-latency EWMA and making P2C wrongly avoid the
//! genuinely faster channel.
//!
//! After the fix: the `latency_ms` in success/failure reports covers only **this**
//! attempt's duration (an `Instant` created at the top of the attempt closure).
//! `submit_failed`'s failure observation still uses the loop-level cumulative value,
//! unchanged in semantics (this file covers only the health-report path).
//!
//! Every case uses a hand-written mock (no real network): the custom egress uses
//! candidate markers to produce the timing "first attempt drags `SLOW_ATTEMPT` then fails /
//! later attempts are instant", and the custom dispatch records (unit key, health result,
//! latency sample) for each report.
//! `common::MockDispatch` drops `latency_ms` in its report, so the latency dimension cannot
//! be asserted there; this file keeps its own dispatch mock that records latency.

mod common;

use bytes::Bytes;
use common::{NON_STREAM_BODY, candidate, ctx_with_body};
use futures_util::stream;
use gateway_core::error::NormalizedError;
use gateway_core::error::code::UPSTREAM_ERROR;
use gateway_dispatch::RetryPolicy;
use gateway_dispatch::health::{AttemptReport, FailureClass, TpsSample};
use gateway_dispatch::{Candidate, Dispatch, DispatchError};
use gateway_forward::ForwardStage;
use gateway_forward::egress::{Egress, ForwardedResponse, Timeouts};
use gateway_pipeline::{Stage, StageError, StageOutcome};
use parking_lot::Mutex;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

/// How long a slow attempt deliberately drags (simulating a slow upstream) before
/// throwing a retryable failure.
const SLOW_ATTEMPT: Duration = Duration::from_millis(200);

/// Lower bound for a slow attempt: real elapsed time never falls below this (50ms of
/// headroom left for scheduler jitter).
const SLOW_FLOOR_MS: u32 = 150;

/// Alias for the boxed future returned by `Egress::execute` (the signature is long).
type EgressFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ForwardedResponse, NormalizedError>> + Send + 'a>>;

/// Retryable 502 failure (isomorphic to the real `egress::classify_status` output for
/// 502).
fn retryable_502() -> NormalizedError {
    NormalizedError {
        code: UPSTREAM_ERROR,
        status: 502,
        retryable: true,
        channel_scoped: false,
        message: "upstream 502".into(),
        retry_after_ms: None,
    }
}

/// Always-successful response body (returned instantly).
fn ok_response() -> ForwardedResponse {
    let body = Bytes::from_static(b"{\"from\":\"c2\"}");
    ForwardedResponse::from_stream(
        200,
        "application/json",
        None,
        stream::iter(vec![Ok::<Bytes, std::io::Error>(body)]),
    )
}

// ---------- egress mock ----------

/// egress mock: decides fast or slow from the candidate marker in the url.
/// `SlowOnFirst` makes only the first attempt (`upstream-c1`) drag `SLOW_ATTEMPT`;
/// `upstream-c2` succeeds instantly. `AlwaysSlow` makes every attempt drag
/// `SLOW_ATTEMPT`.
#[derive(Clone, Copy)]
enum Pace {
    SlowOnFirst,
    AlwaysSlow,
}

struct PaceEgress {
    pace: Pace,
    calls: Mutex<Vec<String>>,
}

impl PaceEgress {
    fn new(pace: Pace) -> Self {
        Self {
            pace,
            calls: Mutex::new(Vec::new()),
        }
    }

    /// Sequence of requested upstream urls (for asserting attempt count and chosen
    /// candidates).
    fn calls(&self) -> Vec<String> {
        self.calls.lock().clone()
    }
}

impl Egress for PaceEgress {
    fn execute<'a>(
        &'a self,
        url: &'a str,
        _headers: &'a [(String, String)],
        _body: Bytes,
        _timeouts: &'a Timeouts,
    ) -> EgressFuture<'a> {
        self.calls.lock().push(url.to_string());
        let slow = match self.pace {
            Pace::SlowOnFirst => url.contains("upstream-c1"),
            Pace::AlwaysSlow => true,
        };
        if slow {
            // Slow attempt: async sleep yields the runtime, then throws a retryable
            // failure.
            Box::pin(async move {
                tokio::time::sleep(SLOW_ATTEMPT).await;
                Err(retryable_502())
            })
        } else {
            Box::pin(async move { Ok(ok_response()) })
        }
    }
}

// ---------- dispatch mock ----------

/// Hand-written mock Dispatch: yields the first candidate not in `exclude` and records
/// each report's (unit key, health result, latency sample) verbatim, so the latency
/// dimension reported to health can be asserted.
/// One health report's (unit key, health result, latency sample). The type alias avoids
/// `clippy::type_complexity` (CI compiles with -D warnings).
type HealthReport = (String, Result<u16, FailureClass>, Option<u32>);

struct LatencyRecordingDispatch {
    candidates: Vec<Candidate>,
    reports: Mutex<Vec<HealthReport>>,
}

impl LatencyRecordingDispatch {
    fn new(candidates: Vec<Candidate>) -> Self {
        Self {
            candidates,
            reports: Mutex::new(Vec::new()),
        }
    }

    /// Sequence of (unit key, result, latency sample) for each health report.
    fn reports(&self) -> Vec<HealthReport> {
        self.reports.lock().clone()
    }

    /// Latency sample for one unit key (panics when absent, listing the keys seen for
    /// diagnosis).
    fn latency_of(&self, unit_key: &str) -> u32 {
        let reports = self.reports();
        reports
            .iter()
            .find(|(k, ..)| k == unit_key)
            .unwrap_or_else(|| {
                panic!(
                    "unit {unit_key} did not report health; actual report sequence = {:?}",
                    reports.iter().map(|(k, ..)| k).collect::<Vec<_>>()
                )
            })
            .2
            .unwrap_or_else(|| panic!("{unit_key} health report must include a latency sample"))
    }
}

impl Dispatch for LatencyRecordingDispatch {
    fn select(
        &self,
        group: &str,
        model: &str,
        exclude: &[String],
        _sticky_channel_key: Option<&str>,
        _min_context: Option<u64>,
    ) -> Result<Candidate, DispatchError> {
        self.candidates
            .iter()
            .find(|c| !exclude.contains(&c.unit.meta.key))
            .cloned()
            .ok_or_else(|| DispatchError::AllUnavailable {
                group: group.to_string(),
                model: model.to_string(),
                retry_after_ms: 0,
            })
    }

    fn report(&self, unit_key: &str, report: AttemptReport) {
        self.reports
            .lock()
            .push((unit_key.to_string(), report.outcome, report.latency_ms));
    }

    fn report_tps(&self, _unit_key: &str, _sample: TpsSample) {}

    fn channel_name(&self, channel_key: &str) -> Option<String> {
        let key = channel_key.strip_prefix("ch-")?;
        Some(format!("name-{key}"))
    }
}

/// Assemble a `ForwardStage` with retry (`max_attempts` includes the first attempt).
fn stage_with_retry(
    egress: Arc<PaceEgress>,
    dispatch: Arc<LatencyRecordingDispatch>,
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

// ---------- cases ----------

/// Core regression: the first attempt (c1) is slow and fails retryably, the second (c2)
/// succeeds instantly. The c2 latency reported to health must contain only the second
/// (instant) attempt, not c1's 200ms.
#[tokio::test]
async fn per_attempt_latency_excludes_prior_slow_attempts() {
    let c1 = candidate("c1");
    let c2 = candidate("c2");
    let dispatch = Arc::new(LatencyRecordingDispatch::new(vec![c1.clone(), c2.clone()]));
    let egress = Arc::new(PaceEgress::new(Pace::SlowOnFirst));
    let stage = stage_with_retry(egress.clone(), dispatch.clone(), 3);

    // route is pre-set to the failing c1: if handle still read ctx.route it would skip
    // c1 and the timing would not hold.
    let mut ctx = ctx_with_body(NON_STREAM_BODY, c1.clone());
    let outcome = stage.handle(&mut ctx).await.expect("c2 should succeed");
    assert!(
        matches!(outcome, StageOutcome::Continue),
        "non-streaming success should be Continue, got {outcome:?}"
    );
    assert_eq!(
        egress.calls().len(),
        2,
        "each of the two attempts sends one upstream request"
    );

    // Health results: c1 retryable failure → Err(Retryable); c2 success → Ok(200).
    let reports = dispatch.reports();
    assert_eq!(reports.len(), 2, "each attempt must report to health once");
    assert_eq!(reports[0].0, "c1");
    assert_eq!(
        reports[0].1,
        Err(FailureClass::Retryable),
        "c1 retryable failure"
    );
    assert_eq!(reports[1].0, "c2");
    assert_eq!(reports[1].1, Ok(200), "c2 success should report Ok(200)");

    // Core assertion: the second attempt's latency sample is tiny (instant success) and
    // does **not** include the first attempt's 200ms. Before the fix this was loop-level
    // cumulative → ≈200ms; after the fix it covers only this attempt → ≈0ms.
    let c2_latency = dispatch.latency_of("c2");
    assert!(
        c2_latency < 100,
        "the second attempt's latency reported to health should include only this attempt's (instant) duration, not the first attempt's {}ms slow attempt; got {c2_latency}ms",
        SLOW_ATTEMPT.as_millis()
    );

    // Control: the first attempt really did drag ~200ms. It measures its own duration
    // both before and after the fix, proving the test actually produced a slow first
    // attempt — which is what gives the assertion above its discriminating power.
    let c1_latency = dispatch.latency_of("c1");
    assert!(
        c1_latency >= SLOW_FLOOR_MS,
        "the first (slow) attempt's latency sample should ≈ its actual duration; got {c1_latency}ms"
    );
}

/// Per-attempt timing on the failure branch: with two slow failures and an exhausted
/// budget, each reported latency contains only **its own** attempt's duration. The key
/// is the second report (c2): it must not accumulate to ~400ms merely by "following c1",
/// but stay ≈ one slow attempt's duration.
#[tokio::test]
async fn per_attempt_latency_on_all_failures() {
    let c1 = candidate("c1");
    let c2 = candidate("c2");
    let dispatch = Arc::new(LatencyRecordingDispatch::new(vec![c1.clone(), c2.clone()]));
    let egress = Arc::new(PaceEgress::new(Pace::AlwaysSlow));
    let stage = stage_with_retry(egress, dispatch.clone(), 2);

    let mut ctx = ctx_with_body(NON_STREAM_BODY, c1.clone());
    let err = stage
        .handle(&mut ctx)
        .await
        .expect_err("budget exhaustion must error rather than silently succeed");
    match err {
        StageError::Unavailable { .. } => {}
        other => panic!("expected Unavailable(504) budget exhausted, got {other:?}"),
    }
    assert!(
        ctx.upstream.is_none(),
        "failure path must not write ctx.upstream"
    );

    let reports = dispatch.reports();
    assert_eq!(
        reports.len(),
        2,
        "each of the two failures reports to health once"
    );
    assert_eq!(reports[0].0, "c1");
    assert_eq!(reports[0].1, Err(FailureClass::Retryable));
    assert_eq!(reports[1].0, "c2");
    assert_eq!(reports[1].1, Err(FailureClass::Retryable));

    let c1_latency = dispatch.latency_of("c1");
    let c2_latency = dispatch.latency_of("c2");
    assert!(
        c1_latency >= SLOW_FLOOR_MS,
        "the first slow failure should ≈ its own duration; got {c1_latency}ms"
    );
    assert!(
        c2_latency < 2 * SLOW_FLOOR_MS,
        "the second slow failure's reported latency should not accumulate to ~{}ms (bug: loop-level timing adds the first attempt); got {c2_latency}ms (should ≈ one slow attempt's duration)",
        2 * SLOW_ATTEMPT.as_millis()
    );
}
