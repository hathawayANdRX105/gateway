//! Lifecycle tests for attempt-level token pre-charge and settle refunds.
//!
//! The wiring shape is identical to `channel_permit_stream.rs`: every attempt (the attempt
//! closure in retry mode / `handle` in single-pass mode) calls `admit_window` + `preclaim`
//! before `forward_task`; the handle is attached to the winning attempt's
//! [`gateway_forward::Forwarded`] and released with the unfold stream state for streaming, or
//! with the commit-scope RAII for non-streaming; the metering settlement commit point calls
//! `settle_actual` to refund the difference (idempotent, independent of Drop order). No path
//! leaks.
//!
//! Behaviour covered (mock egress, zero real network):
//! 1. Streaming: stream not consumed ⇒ `inflight == est`; read the whole stream ⇒ `== 0`; read
//!    only the first frame then drop ⇒ `== 0` (leaks / double refunds turn this red: a double
//!    refund would underflow the atomic counter and fail the assertion);
//! 2. Non-streaming with usage: after settle `inflight == 0` and `completion_ema` moves toward
//!    the measured sample (missing wiring leaves the EMA untouched; double accounting
//!    underflows the inflight counter);
//! 3. Small `cap` (100) + a large-estimate request filling the window ⇒ the second concurrent
//!    request is rejected as 429 retryable; the mock records are asserted to show health did
//!    **not** record Fatal (a full token window ≠ a bad channel; a false Fatal turns this red);
//! 4. Not calling `with_token_budget` ⇒ the paths above behave = status quo (no inflight / EMA
//!    change; breaking the regression invariant turns this red).
//!
//! Mock style follows `channel_permit_stream.rs`: egress uses a scripted mock injected through
//! the trait (two SSE frames, the second gated by a `Notify` so the "stream not finished"
//! window genuinely exists), and dispatch uses the shared mock from `tests/common/mod.rs`.

mod common;

use bytes::Bytes;
use futures_util::Stream;
use futures_util::StreamExt;
use gateway_dispatch::RetryPolicy;
use gateway_dispatch::health::{AttemptReport, FailureClass, TpsSample};
use gateway_dispatch::token_budget::TokenBudgetState;
use gateway_forward::ForwardStage;
use gateway_forward::egress::{Egress, ForwardedResponse, Timeouts};
use gateway_pipeline::{Stage, StageOutcome};
use gateway_protocol_bridge::format_codec::FormatRegistry;
use parking_lot::Mutex;
use std::pin::Pin;
use std::sync::Arc;

use common::*;

const CH_C1: &str = "ch-c1";

/// `ForwardedResponse` future returned by egress (a narrowed alias for the trait signature,
/// saving the fully-qualified path everywhere).
type ExecFuture<'a> = Pin<
    Box<
        dyn Future<Output = Result<ForwardedResponse, gateway_core::error::NormalizedError>>
            + Send
            + 'a,
    >,
>;

// ---------- test helpers ----------

/// Egress mock for a two-frame SSE response: the first frame is available immediately, the
/// second is gated on a `Notify` before it appears — so the "upstream still sending" window
/// genuinely exists and the stream does not end naturally before the assertion.
///
/// (Same shape as the mock of the same name in `channel_permit_stream.rs`; not reused because
/// each test binary compiles separately and `common` only carries request builders and the
/// dispatch mock.)
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

    /// Release the second frame (the first waiter is released; used by the "read to the end"
    /// case so the stream can reach its tail).
    ///
    /// `notify_one` rather than `notify_waiters`: the latter only wakes waiters that are
    /// **already waiting** and stores no permit; `gated_sse_stream` is a lazy unfold, so an
    /// early release would lose the permit forever.
    fn release(&self) {
        self.gate.notify_one();
    }

    fn calls(&self) -> usize {
        *self.calls.lock()
    }
}

impl Egress for GatedSseEgress {
    fn execute<'a>(
        &'a self,
        _alias: &str,
        _url: &'a str,
        _headers: &'a [(String, String)],
        _body: Bytes,
        _timeouts: &'a Timeouts,
    ) -> ExecFuture<'a> {
        *self.calls.lock() += 1;
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

/// Mock egress that always returns 200 + usage (for the non-streaming settle case).
struct UsageEgress;

impl Egress for UsageEgress {
    fn execute<'a>(
        &'a self,
        _alias: &str,
        _url: &'a str,
        _headers: &'a [(String, String)],
        _body: Bytes,
        _timeouts: &'a Timeouts,
    ) -> ExecFuture<'a> {
        Box::pin(async move {
            Ok(ForwardedResponse::from_stream(
                200,
                "application/json",
                None,
                futures_util::stream::iter(vec![Ok::<Bytes, std::io::Error>(Bytes::from_static(
                    b"{\"usage\":{\"prompt_tokens\":100,\"completion_tokens\":50}}",
                ))]),
            ))
        })
    }
}

/// Two-frame SSE stream: poll 0 → first frame; poll 1 → `[DONE]` frame after waiting for the
/// `Notify` release; poll 2 → end of stream. The gate makes "stream not finished" and "upstream
/// still sending" structurally the same in the test.
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

/// Streaming ctx (body carries `"stream":true`), route pre-populated with a candidate (unused
/// by `handle` after `with_retry`, but required by the shared builder).
fn stream_ctx(candidate: gateway_dispatch::Candidate) -> gateway_pipeline::RequestCtx {
    ctx_with_body(STREAM_BODY, candidate)
}

/// Extract the `PipeStream` after a successful handle (panics with `StageError` Debug on failure).
async fn expect_stream(
    stage: &ForwardStage,
    ctx: &mut gateway_pipeline::RequestCtx,
) -> gateway_pipeline::PipeStream {
    match stage.handle(ctx).await.expect("handle should succeed") {
        StageOutcome::Stream(pipe) => pipe,
        other => panic!("streaming request should return Stream, got {other:?}"),
    }
}

/// Read a `PipeStream` as an axum data stream (consuming it = the client received the whole
/// body).
fn data_stream(pipe: gateway_pipeline::PipeStream) -> impl Stream<Item = Bytes> + Send {
    // axum's into_data_stream() yields Result<Bytes, Error>; a body error should not occur on
    // the test path, so panic if it does (keeping the error message for diagnosis).
    pipe.into_response()
        .into_body()
        .into_data_stream()
        .map(|chunk| chunk.expect("body chunk should not error"))
}

/// Estimated tokens for this request (same basis as `stage.rs::estimate_forward_tokens`:
/// `estimate_prompt_tokens(body) + min(max_tokens, completion_ema)`; when the bound field is
/// absent it is the EMA). The test recomputes with the real metering functions instead of
/// hard-coding literals — a basis drift turns this red.
fn est_for(body: &'static [u8], max_tokens: Option<u64>, ema: u64) -> u64 {
    let prompt = gateway_metering::estimate::estimate_prompt_tokens(&Bytes::from_static(body));
    prompt + max_tokens.unwrap_or(ema).min(ema)
}

// ---------- case 1a: unconsumed stream holds inflight; full read returns it ----------

#[tokio::test]
async fn streaming_token_held_until_stream_fully_consumed() {
    let c1 = candidate("c1");
    let dispatch = Arc::new(MockDispatch::new(vec![c1.clone()]));
    let egress = Arc::new(GatedSseEgress::new());
    let token = TokenBudgetState::new();
    token.register_channel(CH_C1, 10_000);
    let stage = ForwardStage::new(egress.clone(), Arc::new(FormatRegistry::new()))
        .with_retry(dispatch, RetryPolicy::default())
        .with_token_budget(token.clone());

    let mut ctx = stream_ctx(c1);
    let mut data = data_stream(expect_stream(&stage, &mut ctx).await);

    // Stream just established, no frame consumed yet: the handle lives in the unfold stream
    // state, so the in-flight estimate must still be held (otherwise inflight would only
    // reflect connection establishment and a long stream could exhaust the channel TPM
    // unseen).
    let est = est_for(STREAM_BODY, None, token.completion_ema(CH_C1));
    assert!(
        est > 0,
        "precondition: estimate should be positive (prompt estimate + completion EMA), got {est}"
    );
    assert_eq!(
        token.inflight(CH_C1),
        est,
        "inflight token should remain occupied while stream unfinished (streaming state, not connect-and-immediately-release)"
    );

    // Read the first frame: the stream is still unfinished, the handle is still held by the
    // stream.
    let first = data
        .next()
        .await
        .expect("first frame should be provided by upstream");
    assert_eq!(first, Bytes::from_static(b"data: {\"id\":\"chunk-1\"}\n\n"));
    assert_eq!(
        token.inflight(CH_C1),
        est,
        "stream still not finished after first frame, inflight should remain occupied"
    );

    // Release [DONE] and read to the end: the handle is dropped with the stream state and refunds
    // the whole estimate (this case mounts no price table, so no settle runs).
    egress.release();
    let mut rest = Vec::new();
    while let Some(chunk) = data.next().await {
        rest.extend_from_slice(&chunk);
    }
    assert_eq!(
        rest,
        b"data: [DONE]\n\n".to_vec(),
        "stream end should be [DONE] frame"
    );
    assert_eq!(
        token.inflight(CH_C1),
        0,
        "after stream read to end, inflight token should be returned (settle refunds difference + drop releases)"
    );
}

// ---------- case 1b: drop after only the first frame (client disconnect) ⇒ full refund ----------

#[tokio::test]
async fn streaming_token_refunded_when_stream_dropped_mid_flight() {
    let c1 = candidate("c1");
    let dispatch = Arc::new(MockDispatch::new(vec![c1.clone()]));
    let egress = Arc::new(GatedSseEgress::new());
    let token = TokenBudgetState::new();
    token.register_channel(CH_C1, 10_000);
    let stage = ForwardStage::new(egress.clone(), Arc::new(FormatRegistry::new()))
        .with_retry(dispatch, RetryPolicy::default())
        .with_token_budget(token.clone());

    let mut ctx = stream_ctx(c1);
    let mut data = data_stream(expect_stream(&stage, &mut ctx).await);

    let est = est_for(STREAM_BODY, None, token.completion_ema(CH_C1));
    let first = data
        .next()
        .await
        .expect("first frame should be provided by upstream");
    assert_eq!(first, Bytes::from_static(b"data: {\"id\":\"chunk-1\"}\n\n"));
    assert_eq!(
        token.inflight(CH_C1),
        est,
        "precondition: inflight should be occupied while stream unfinished"
    );

    // Client disconnect: the axum Body is dropped along with the unfold stream state (handle
    // included) → the un-settled estimate is fully refunded by TokenHandle::drop and must not
    // leak (otherwise a single disconnect would permanently exhaust the channel TPM).
    drop(data);
    assert_eq!(
        token.inflight(CH_C1),
        0,
        "after stream dropped (client disconnect), inflight token should be fully returned, must not leak"
    );
    assert_eq!(egress.calls(), 1, "should only send one upstream request");
}

// ---------- case 2: non-streaming with usage ⇒ settle refund + EMA calibration ----------

#[tokio::test]
async fn non_streaming_settle_refunds_delta_and_calibrates_ema() {
    // Upstream returns usage (prompt 100 / completion 50); the settlement commit point refunds
    // the difference and feeds the EMA. Single-pass mode (no with_retry) goes through handle's
    // single mounting point with the same pre-charge/refund; non-streaming token refund shares
    // the metering settlement gate (settle only when pt+sink are mounted together, with the
    // handle's Drop as the full-refund backstop), so this case must mount `with_price_table`.
    let egress = Arc::new(UsageEgress);
    let token = TokenBudgetState::new();
    token.register_channel(CH_C1, 10_000);
    let stage = ForwardStage::new(egress, Arc::new(FormatRegistry::new()))
        .with_price_table(Arc::new(FlatPriceTable), Arc::new(VecSink::default()))
        .with_token_budget(token.clone());

    let ema_before = token.completion_ema(CH_C1);
    assert_ne!(
        ema_before, 0,
        "precondition: EMA should be TokenBudgetState's default initial value"
    );
    let est = est_for(NON_STREAM_BODY, None, ema_before);
    assert!(
        est > 100 + 50,
        "precondition: estimate should be greater than measured ({est} ≤ 150, refund difference needs observable effect)"
    );

    let mut ctx = ctx_with_body(NON_STREAM_BODY, candidate("c1"));
    let outcome = stage
        .handle(&mut ctx)
        .await
        .expect("single-pass forward should succeed");
    assert!(
        matches!(outcome, StageOutcome::Continue),
        "non-streaming success should be Continue, got {outcome:?}"
    );

    // Refund: settle corrects inflight from est to the measured 150; the handle returns 150
    // when the commit scope drops → net zero.
    assert_eq!(
        token.inflight(CH_C1),
        0,
        "nothing should remain after settle refund + scope release"
    );
    // EMA calibration: α=0.3 fed the measured completion 50 (`TokenHandle::settle_actual`
    // basis: `(old·7 + sample·3) / 10`). Missing settle wiring leaves the EMA untouched.
    let ema_after = token.completion_ema(CH_C1);
    assert_ne!(
        ema_after, ema_before,
        "after settle EMA should move toward measured completion"
    );
    assert_eq!(
        ema_after,
        (ema_before * 7 + 50 * 3) / 10,
        "EMA should move toward measured sample at α=0.3"
    );
}

// ---------- case 3: window full ⇒ second concurrent request rejected as 429 retryable ----------

#[tokio::test]
async fn token_window_full_rejects_second_request_retryable() {
    const CAP: u64 = 100;
    /// Body carries `max_tokens:50` ⇒ output-side estimate 50 (capped by the EMA), prompt
    /// estimate ≈ 11, so a single request est ≈ 61 ≤ cap=100 is admitted; two concurrent
    /// requests need 122 > 100 ⇒ the window is full.
    const CAPPED_BODY: &[u8] = b"{\"model\":\"m\",\"stream\":true,\"max_tokens\":50}";
    let c1 = candidate("c1");
    // Deterministic clock (fixed 0ms): both handle calls land in the same 60s window bucket, so
    // the window occupancy is not wiped by a window boundary — rejection behaviour is determined
    // purely by cap and est.
    let token = TokenBudgetState::with_clock(|| 0);
    token.register_channel(CH_C1, CAP);

    let est = est_for(CAPPED_BODY, Some(50), token.completion_ema(CH_C1));
    assert!(
        est <= CAP && 2 * est > CAP,
        "precondition: single request can occupy window (est={est} ≤ cap={CAP}), two concurrent exceed (2·est > cap)"
    );

    // select exhaustion takes the "all candidates rate-limited" exit (same shape as production
    // Dispatcher::select: RateLimited → 429); common's MockDispatch yields AllUnavailable → 503
    // on exhaustion, which has the wrong shape, so a local mock that changes only this one
    // semantic point is configured here.
    let dispatch = Arc::new(RateLimitedDispatch::new(vec![c1.clone()]));
    let egress = Arc::new(GatedSseEgress::new());
    let stage = ForwardStage::new(egress.clone(), Arc::new(FormatRegistry::new()))
        .with_retry(dispatch.clone(), RetryPolicy::default())
        .with_token_budget(token.clone());

    // First concurrent request: admitted and holds the window (the response stream has not
    // been consumed yet; the pre-charge handle lives in the stream state).
    let mut ctx_first = ctx_with_body(CAPPED_BODY, c1.clone());
    let data_first = data_stream(expect_stream(&stage, &mut ctx_first).await);
    assert_eq!(
        token.inflight(CH_C1),
        est,
        "precondition: after first request pre-occupies, inflight = est"
    );

    // Second concurrent request: the window is already full ⇒ rejected (same shape as a full
    // concurrency slot).
    let mut ctx_second = ctx_with_body(CAPPED_BODY, c1.clone());
    let err = stage
        .handle(&mut ctx_second)
        .await
        .expect_err("window full, this request must be rejected");
    assert!(
        matches!(err, gateway_pipeline::StageError::RateLimited),
        "window-full rejection should ultimately map to 429 retryable (RateLimited), got {err:?}"
    );

    // The health side records no real upstream failure: `Fatal` would blacklist the channel as
    // broken, while a full token window ≠ a bad channel (design §3.6) — health reports go
    // through the existing 429/Throttled channel (Ok(429)); any Fatal in the mock records means
    // collateral damage.
    let reports = dispatch.reports();
    assert!(
        !reports.is_empty(),
        "rejected attempt should report health (drives candidate switch), got empty records"
    );
    for (unit, outcome) in &reports {
        assert_ne!(
            *outcome,
            Err(FailureClass::Fatal),
            "token window full must not mark channel as Fatal ({unit})"
        );
    }

    // Rejection happens before preclaim ⇒ the rejected request has no in-flight accounting and
    // never hits upstream.
    assert_eq!(
        token.inflight(CH_C1),
        est,
        "rejected request should not pre-occupy additionally"
    );
    assert_eq!(
        egress.calls(),
        1,
        "rejected request should not actually hit upstream (only first request did)"
    );

    // Single-pass mode (no with_retry) rejects at the same point: the same 429 upstream error
    // shape as a full slot (`normalized_to_stage_error` passes it through, no submit_failed, no
    // health-table entry).
    let stage_single = ForwardStage::new(
        Arc::new(GatedSseEgress::new()),
        Arc::new(FormatRegistry::new()),
    )
    .with_token_budget(token.clone());
    let mut ctx_single = ctx_with_body(CAPPED_BODY, c1);
    let err_single = stage_single
        .handle(&mut ctx_single)
        .await
        .expect_err("window still full, single-pass mode must also reject");
    let gateway_pipeline::StageError::Upstream(gateway_pipeline::UpstreamError::Status {
        code,
        body_preview,
    }) = err_single
    else {
        panic!(
            "single-mode full window should take 429 upstream semantics (same shape as full slot), got {err_single:?}"
        );
    };
    assert_eq!(
        code, 429,
        "single-pass mode token window full should carry 429 semantics"
    );
    assert!(
        body_preview.contains("token"),
        "error message should point to token budget (distinguishable for troubleshooting), got {body_preview:?}"
    );

    // Drop the first request's response stream (client disconnect): the un-settled handle is
    // fully refunded by Drop.
    drop(data_first);
    assert_eq!(
        token.inflight(CH_C1),
        0,
        "first request handle drop should fully refund"
    );
}

// ---------- case 4: token budget unmounted ⇒ status quo (regression invariant) ----------

#[tokio::test]
async fn unmounted_token_budget_keeps_current_behavior() {
    const CAP: u64 = 100;
    let c1 = candidate("c1");
    let egress = Arc::new(GatedSseEgress::new());
    // Only build the state, do not call `with_token_budget`: the entire token path should be
    // skipped.
    let token = TokenBudgetState::new();
    token.register_channel(CH_C1, CAP);
    let stage = ForwardStage::new(egress.clone(), Arc::new(FormatRegistry::new()));

    let mut ctx = stream_ctx(c1);
    let mut data = data_stream(expect_stream(&stage, &mut ctx).await);
    // Even with a full window (est > cap) it forwards as usual when not mounted (status quo: no
    // token-window admission check).
    let first = data
        .next()
        .await
        .expect("first frame should be provided by upstream");
    assert_eq!(first, Bytes::from_static(b"data: {\"id\":\"chunk-1\"}\n\n"));
    egress.release();
    let mut rest = Vec::new();
    while let Some(chunk) = data.next().await {
        rest.extend_from_slice(&chunk);
    }
    assert_eq!(rest, b"data: [DONE]\n\n".to_vec());

    // Counts and share unchanged: no pre-charge, no settle, no EMA calibration.
    assert_eq!(
        token.inflight(CH_C1),
        0,
        "when not mounted there must be no inflight accounting (behavior = status quo)"
    );
    assert_ne!(
        token.completion_ema(CH_C1),
        0,
        "EMA retains default initial value (not calibrated by settle)"
    );
    assert_eq!(egress.calls(), 1, "should only send one upstream request");
}

/// Dispatch mock shaped like the production `Dispatcher::select` "all candidates rate limited"
/// exit: on candidate exhaustion it returns `RateLimited` (handle maps 429) instead of
/// `AllUnavailable` (503), and records every health report for assertions (a full token window
/// ≠ a bad channel: only 429/Retryable-class outcomes are allowed, no Fatal).
struct RateLimitedDispatch {
    cands: Vec<gateway_dispatch::Candidate>,
    reports: Mutex<Vec<(String, Result<u16, FailureClass>)>>,
}

impl RateLimitedDispatch {
    fn new(cands: Vec<gateway_dispatch::Candidate>) -> Self {
        Self {
            cands,
            reports: Mutex::new(Vec::new()),
        }
    }

    fn reports(&self) -> Vec<(String, Result<u16, FailureClass>)> {
        self.reports.lock().clone()
    }
}

impl gateway_dispatch::Dispatch for RateLimitedDispatch {
    fn select(
        &self,
        group: &str,
        public_model: &str,
        exclude: &[String],
        _sticky_channel_key: Option<&str>,
        _min_context: Option<u64>,
    ) -> Result<gateway_dispatch::Candidate, gateway_dispatch::DispatchError> {
        self.cands
            .iter()
            .find(|c| !exclude.contains(&c.unit.meta.key))
            .cloned()
            .ok_or(gateway_dispatch::DispatchError::RateLimited {
                group: group.to_string(),
                model: public_model.to_string(),
            })
    }

    fn report(&self, unit_key: &str, report: AttemptReport) {
        self.reports
            .lock()
            .push((unit_key.to_string(), report.outcome));
    }

    fn report_tps(&self, _unit_key: &str, _sample: TpsSample) {}
}

/// Flat price table (minimal `PriceTable`: the token-refund cases only need the metering
/// settlement gate opened; bill amounts are not asserted).
struct FlatPriceTable;

impl gateway_metering::pricing::PriceTable for FlatPriceTable {
    fn lookup(&self, _model: &str, _group: &str) -> Option<gateway_metering::pricing::ModelPrice> {
        Some(gateway_metering::pricing::ModelPrice {
            input: 1.0,
            output: 1.0,
            cache: 0.0,
            group_multiplier: 1.0,
        })
    }
}
