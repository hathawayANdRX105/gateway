//! `ForwardStage` concurrency-gate mounting (v2) integration tests — the global Semaphore
//! takes effect inside the forward path.
//!
//! Contracts verified (concurrency-gate mounting v2):
//! 1. `with_concurrency(2)` + 3 concurrent requests (single-shot mode): the mock egress
//!    delays to hold the slots and create a race window, so the third request is rejected
//!    at `try_acquire` before `forward_task` → the client sees `rate_limited` (429
//!    semantics) and the request **never touches egress**;
//! 2. `with_concurrency(2)` + retry mode (production assembly shape): the rejected
//!    request stays rejected through the retry loop → once the budget is exhausted the
//!    terminal state is Unavailable (504 + Retry-After), with the 429 concurrency
//!    diagnostic going only to tracing;
//! 3. `with_concurrency(4)` + 3 concurrent: enough slots → all succeed;
//! 4. `with_concurrency` not called (`concurrency == None`): behaviour is exactly as
//!    before mounting and all 3 concurrent requests succeed — zero breakage of the
//!    existing path.
//!
//! Mock style follows `retry_wiring.rs`: egress is a scripted mock injected through the
//! trait (with `tokio::time::sleep` added here to simulate upstream latency and hold the
//! slots); no real network. The concurrency gate is a **global** semaphore (an overall
//! cap, not per channel), so retrying another candidate contends for the same lock; the
//! Dispatch mock therefore always returns the same candidate (ignoring exclude) — the
//! same outcome as "every retry rejected" in a real multi-candidate scenario.

use bytes::Bytes;
use gateway_core::records::{ChannelSettings, RouteUnitRecord, SyncMeta};
use gateway_dispatch::health::{AttemptReport, TpsSample};
use gateway_dispatch::{Candidate, Dispatch, DispatchError, RetryPolicy};
use gateway_forward::ForwardStage;
use gateway_forward::egress::{Egress, ForwardedResponse, Timeouts};
use gateway_pipeline::{
    BodySource, ProtocolKind, RequestMeta, SelectedRoute, Stage, StageError, StageOutcome,
    StreamedAccum, TokenInfo, UpstreamError,
};
use std::sync::{Arc, Mutex};
use std::time::Duration;

// ---------- test helpers ----------

fn candidate(key: &str) -> Candidate {
    SelectedRoute {
        unit: RouteUnitRecord {
            meta: SyncMeta {
                key: key.to_string(),
                schema_version: 1,
                logical_version: 1,
                origin: "test".to_string(),
                updated_at: chrono::Utc::now(),
            },
            group: "g".to_string(),
            public_model: "m".to_string(),
            channel_key: format!("ch-{key}"),
            key_index: 0,
            upstream_model: "m".to_string(),
            weight: 10,
            status: 1,
        },
        secret: "sk-test".to_string(),
        base_url: format!("http://upstream-{key}.invalid"),
        upstream_model: "m".to_string(),
        provider_type: "openai".to_string(),
        settings: ChannelSettings::default(),
    }
}

/// Dispatch mock that always returns the same candidate: the concurrency gate is a
/// global semaphore, so "retry another candidate" hits the same lock and exclude
/// semantics are irrelevant here; always returning it guarantees the retry budget is
/// truly exhausted (respecting exclude would exclude the only candidate after the first
/// failure, making select return `AllUnavailable` at once and ending the loop before the
/// budget is spent).
struct SingleDispatch {
    candidate: Candidate,
}

impl Dispatch for SingleDispatch {
    fn select(
        &self,
        _group: &str,
        _model: &str,
        _exclude: &[String],
        _sticky_channel_key: Option<&str>,
        _min_context: Option<u64>,
    ) -> Result<Candidate, DispatchError> {
        Ok(self.candidate.clone())
    }

    fn report(&self, _unit_key: &str, _report: AttemptReport) {}

    fn report_tps(&self, _unit_key: &str, _sample: TpsSample) {}
}

/// Delayed egress mock: sleeps `delay` to simulate upstream latency (holding the
/// concurrency slots to create a race window), then returns a fixed 200 success body.
/// Every call is recorded so the test can assert "rejected requests never touch egress".
struct DelayedEgress {
    delay: Duration,
    calls: Mutex<Vec<String>>,
}

impl DelayedEgress {
    fn new(delay: Duration) -> Self {
        Self {
            delay,
            calls: Mutex::new(Vec::new()),
        }
    }

    fn call_count(&self) -> usize {
        self.calls.lock().unwrap().len()
    }
}

impl Egress for DelayedEgress {
    fn execute<'a>(
        &'a self,
        url: &'a str,
        _headers: &'a [(String, String)],
        _body: Bytes,
        _timeouts: &'a Timeouts,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<ForwardedResponse, gateway_core::error::NormalizedError>,
                > + Send
                + 'a,
        >,
    > {
        self.calls.lock().unwrap().push(url.to_string());
        let delay = self.delay;
        Box::pin(async move {
            tokio::time::sleep(delay).await;
            let body = Bytes::from_static(b"{\"ok\":true}");
            let stream = futures_util::stream::iter(vec![Ok::<Bytes, std::io::Error>(body)]);
            Ok(ForwardedResponse::from_stream(
                200,
                "application/json",
                None,
                stream,
            ))
        })
    }
}

/// Non-streaming request ctx (same shape as `retry_wiring.rs`: route pre-set to the
/// candidate, token attribution complete).
fn ctx_with_route(route: Candidate) -> gateway_pipeline::RequestCtx {
    let meta = RequestMeta {
        method: "POST".to_string(),
        path: "/v1/chat/completions".to_string(),
        headers: http::HeaderMap::new(),
        body: BodySource::InMemory(Bytes::from_static(b"{\"model\":\"m\"}")),
        client_ip: "127.0.0.1".parse().unwrap(),
        request_id: uuid::Uuid::now_v7(),
        inbound_protocol: ProtocolKind::OpenAI,
    };
    gateway_pipeline::RequestCtx {
        hold: None,
        request: meta,
        token: Some(TokenInfo {
            id: "tok-1".into(),
            user_key: "user-test".to_string(),
            group: "g".to_string(),
            enabled: true,
            allowed_models: None,
            auth_version: 1,
        }),
        requested_model: Some("m".to_string()),
        requested_max_tokens: None,
        route: Some(route),
        selected_channel_key: None,
        selected_channel_name: None,
        session_hint: None,
        upstream: None,
        streamed: StreamedAccum::default(),
        error: None,
        drop_guards: Vec::new(),
    }
}

/// Fire 3 concurrent requests (`tokio::spawn` + a shared Arc<ForwardStage>) and collect
/// `(stage result, upstream status code written to ctx)`.
async fn run_three(
    stage: Arc<ForwardStage>,
    candidate: Candidate,
) -> Vec<(Result<StageOutcome, StageError>, Option<u16>)> {
    let mut handles = Vec::new();
    for _ in 0..3 {
        let stage = Arc::clone(&stage);
        let candidate = candidate.clone();
        handles.push(tokio::spawn(async move {
            let mut ctx = ctx_with_route(candidate);
            let outcome = stage.handle(&mut ctx).await;
            let upstream_status = ctx.upstream.map(|u| u.status);
            (outcome, upstream_status)
        }));
    }
    futures_util::future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.expect("spawned test task panicked"))
        .collect()
}

/// Assert "exactly `n_ok` successes, every other request rejected with 429 semantics".
fn assert_split(
    results: &[(Result<StageOutcome, StageError>, Option<u16>)],
    n_ok: usize,
    why: &str,
) {
    let ok = results
        .iter()
        .filter(|(r, _)| matches!(r, Ok(StageOutcome::Continue)))
        .count();
    assert_eq!(
        ok, n_ok,
        "{why}: success count should be {n_ok}, results = {results:?}"
    );
    assert_eq!(
        results.len() - ok,
        3 - n_ok,
        "{why}: remaining requests should all be rejected by the concurrency gate",
    );
}

// ---------- case 1: single-shot mode, 2 slots + 3 concurrent → the third gets 429
// ---------- without touching egress ----------

#[tokio::test]
async fn single_mode_concurrency_limit_rejects_third_request() {
    // Behaviour assertion: two requests each hold one slot (egress sleeps 200ms to
    // occupy it), and the third request is rejected at try_acquire before forward_task →
    // StageError carries rate_limited 429 semantics; the rejected request sends no
    // upstream call (egress call count == 2).
    let egress = Arc::new(DelayedEgress::new(Duration::from_millis(200)));
    let adaptors = Arc::new(gateway_protocol_bridge::format_codec::FormatRegistry::new());
    let stage = Arc::new(ForwardStage::new(egress.clone(), adaptors).with_concurrency(2));
    let results = run_three(stage, candidate("c1")).await;

    assert_split(&results, 2, "with 2 slots the 3rd request must be rejected");
    for (outcome, _) in &results {
        if let Err(StageError::Upstream(UpstreamError::Status { code, body_preview })) = outcome {
            assert_eq!(
                *code, 429,
                "concurrency rejection should carry 429 semantics"
            );
            assert_eq!(
                body_preview.as_str(),
                "concurrency limit reached",
                "single-shot path passes concurrency-gate error straight through"
            );
        }
    }
    assert_eq!(
        egress.call_count(),
        2,
        "rejected requests must not touch egress (acquire happens before forward_task)"
    );
}

// ---------- case 2: retry mode (production assembly shape), 2 slots + 3 concurrent →
// ---------- budget exhausted, 504 (Unavailable) ----------

#[tokio::test]
async fn retry_mode_concurrency_limit_exhausts_budget_to_unavailable() {
    // Production assembly = with_retry + with_concurrency. The rejected request fails
    // acquire on every attempt of the retry loop (the global semaphore stays full even
    // when switching candidates) → budget exhausted → terminal Unavailable (504 +
    // Retry-After); the 429 concurrency diagnostic goes to tracing.
    let egress = Arc::new(DelayedEgress::new(Duration::from_millis(200)));
    let adaptors = Arc::new(gateway_protocol_bridge::format_codec::FormatRegistry::new());
    let dispatch = Arc::new(SingleDispatch {
        candidate: candidate("c1"),
    });
    let stage = Arc::new(
        ForwardStage::new(egress.clone(), adaptors)
            .with_retry(dispatch, RetryPolicy::default())
            .with_concurrency(2),
    );
    let results = run_three(stage, candidate("c1")).await;

    assert_split(
        &results,
        2,
        "in retry mode the global gate also lets only 2 through",
    );
    // The rejected third request: the concurrency rejection persists across the whole
    // budget → budget exhausted → retryable 504 (Unavailable). The 429 concurrency
    // diagnostic goes to tracing and no longer appears in the 4xx response body.
    let unavailable = results
        .iter()
        .filter(|(o, _)| matches!(o, Err(StageError::Unavailable { .. })))
        .count();
    assert_eq!(
        unavailable, 1,
        "in retry mode, a concurrency-rejected request with exhausted budget maps to Unavailable(504)"
    );
    assert_eq!(
        egress.call_count(),
        2,
        "retries of a rejected request never touch egress"
    );
}

// ---------- case 3: 4 slots + 3 concurrent → all succeed ----------

#[tokio::test]
async fn concurrency_headroom_allows_all_requests() {
    // Slots (4) outnumber concurrent requests (3): every acquire succeeds and no
    // rejection path is taken.
    let egress = Arc::new(DelayedEgress::new(Duration::from_millis(100)));
    let adaptors = Arc::new(gateway_protocol_bridge::format_codec::FormatRegistry::new());
    let dispatch = Arc::new(SingleDispatch {
        candidate: candidate("c1"),
    });
    let stage = Arc::new(
        ForwardStage::new(egress.clone(), adaptors)
            .with_retry(dispatch, RetryPolicy::default())
            .with_concurrency(4),
    );
    let results = run_three(stage, candidate("c1")).await;

    assert_split(
        &results,
        3,
        "with enough slots all 3 concurrent requests succeed",
    );
    for upstream_status in results.iter().map(|(_, s)| s) {
        assert_eq!(
            *upstream_status,
            Some(200),
            "successful requests write back upstream 200"
        );
    }
    assert_eq!(
        egress.call_count(),
        3,
        "each request makes one upstream call"
    );
}

// ---------- case 4: not mounted (concurrency == None) → behaviour matches the
// ---------- pre-mounting path ----------

#[tokio::test]
async fn without_concurrency_mount_behaviour_unchanged() {
    // with_concurrency is not called: acquire is skipped and all 3 concurrent requests
    // succeed — direct evidence that the existing test/production path (when the builder
    // is not called) suffers zero breakage.
    let egress = Arc::new(DelayedEgress::new(Duration::from_millis(100)));
    let adaptors = Arc::new(gateway_protocol_bridge::format_codec::FormatRegistry::new());
    let dispatch = Arc::new(SingleDispatch {
        candidate: candidate("c1"),
    });
    let stage = Arc::new(
        ForwardStage::new(egress.clone(), adaptors).with_retry(dispatch, RetryPolicy::default()),
    );
    let results = run_three(stage, candidate("c1")).await;

    assert_split(
        &results,
        3,
        "without the concurrency gate mounted, behavior matches pre-mounting exactly",
    );
    assert_eq!(egress.call_count(), 3);
}
