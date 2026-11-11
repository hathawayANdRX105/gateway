//! forward→dispatch tps wiring contract test (design §5 — the test gap jev called out).
//!
//! What it pins is that the **cross-crate signal really arrives**, not just that the two ends
//! work: a real `ForwardStage` (`with_retry` mounting a recording `Dispatch` that wraps a real
//! `Dispatcher` + a real `MemoryHealthTable`), with egress as a scripted mock (zero real network).
//! Assertions observe both places at once: the recording mock's `report_tps` call sequence and
//! the real change of `tps_ema` in the health table.
//!
//! Coverage matrix (each case names which bug turns it red):
//! 1. Normal streaming read to the end ⇒ `report_tps` exactly once, `Measured` variant, tok/s in
//!    a plausible range, health-table `tps_ema` written (red: late report dropped / wrong variant
//!    / wrong denominator / reaching only the mock and not the table).
//! 2. No usage frame (metering falls back to bytes/4 estimation) ⇒ sample is `Estimated` (red:
//!    wrong variant → estimation noise mixed into the main EMA at α=0.2).
//! 3. Ultra-short burst with completion < 8 ⇒ 0 reports; when it follows a normal stream
//!    `tps_ema` does not move (red: threshold broken → `max_tokens=1` noise pollutes the EMA).
//! 4. Upstream stream break (mid-stream Err) ⇒ 0 reports (red: the broken-stream arm misreports →
//!    `finish_at` at the break moment is too small and a low throughput is written high). Design
//!    §3.2.
//! 5. Non-streaming request ⇒ 0 reports (red: non-streaming misreports → the factor is no longer
//!    always neutral).
//! 6. A late report only merges into `tps_ema` and does not touch cooldown/streak/401 counters
//!    (pins that the existing health ledger is not polluted by the side-channel signal).
//! 7. Sub-millisecond short stream (content frame and terminal frame in the same chunk ⇒ the 1 ms
//!    floor rounds it up) ⇒ `report_tps` 0 times (red: no ceiling ⇒ a 120 token ÷ 1 ms =
//!    120000 tok/s bogus value becomes the first `tps_ema` sample, and then `pool_max_tps` taking
//!    the pool maximum pins the whole pool's factor at the 0.2 floor).
//! 8. Artificial sample above the ceiling but **not** sub-millisecond ⇒ 0 reports, and `tps_ema`
//!    stays 0 (red: clamping instead ⇒ the fake `10_000` tok/s is written into the EMA, worse than
//!    not reporting at all).
//! 9. Normal sample above the floor and below the ceiling ⇒ exactly 1 report (red: ceiling set
//!    too low / misjudged ⇒ normal throughput filtered out).
//! 10. Value at the ceiling (10 token ÷ 1 ms floor = `10_000` tok/s) ⇒ still reported (red:
//!     criterion written as `>=` ⇒ the ceiling itself is wrongly dropped).

mod common;

use bytes::Bytes;
use futures_util::Stream;
use futures_util::StreamExt;
use gateway_core::records::{ChannelKey, ChannelRecord, RouteUnitRecord, SyncMeta};
use gateway_dispatch::health::{HealthTable, MemoryHealthTable, TpsSample};
use gateway_dispatch::{
    AttemptReport, Candidate, Dispatch, DispatchError, Dispatcher, RetryPolicy, Snapshot,
};
use gateway_forward::ForwardStage;
use gateway_forward::egress::{Egress, ForwardedResponse, Timeouts};
use gateway_pipeline::{PipeStream, RequestCtx, Stage, StageOutcome};
use gateway_protocol_bridge::format_codec::FormatRegistry;
use parking_lot::Mutex;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use common::*;

const UNIT_KEY: &str = "u1";
const CHANNEL_KEY: &str = "ch-c1";

type ExecFuture<'a> = Pin<
    Box<
        dyn Future<Output = Result<ForwardedResponse, gateway_core::error::NormalizedError>>
            + Send
            + 'a,
    >,
>;

// ---------- Recording Dispatch (real Dispatcher + recording report_tps) ----------

/// Recording Dispatch: `select` / `report` / `report_tps` all forward to the real `Dispatcher`
/// (health accounting takes the real path), and `report_tps` additionally keeps a call record —
/// this is the observation point for "the signal arrived", not a stand-in for the production
/// implementation.
struct RecordingDispatch {
    inner: Dispatcher,
    tps_calls: Mutex<Vec<(String, TpsSample)>>,
}

impl RecordingDispatch {
    fn tps_calls(&self) -> Vec<(String, TpsSample)> {
        self.tps_calls.lock().clone()
    }
}

impl Dispatch for RecordingDispatch {
    fn select(
        &self,
        group: &str,
        public_model: &str,
        exclude: &[String],
        sticky_channel_key: Option<&str>,
        min_context: Option<u64>,
    ) -> Result<Candidate, DispatchError> {
        self.inner.select(
            group,
            public_model,
            exclude,
            sticky_channel_key,
            min_context,
        )
    }

    fn report(&self, unit_key: &str, report: AttemptReport) {
        self.inner.report(unit_key, report);
    }

    fn report_tps(&self, unit_key: &str, sample: TpsSample) {
        self.tps_calls.lock().push((unit_key.to_string(), sample));
        self.inner.report_tps(unit_key, sample);
    }

    fn channel_name(&self, channel_key: &str) -> Option<String> {
        self.inner.channel_name(channel_key)
    }
}

// ---------- Snapshot / stage assembly ----------

fn unit() -> RouteUnitRecord {
    RouteUnitRecord {
        meta: SyncMeta {
            key: UNIT_KEY.to_string(),
            schema_version: 1,
            logical_version: 1,
            origin: "test".to_string(),
            updated_at: chrono::Utc::now(),
        },
        group: "g".to_string(),
        public_model: "m".to_string(),
        channel_key: CHANNEL_KEY.to_string(),
        key_index: 0,
        upstream_model: "m".to_string(),
        weight: 10,
        status: 1,
    }
}

fn channel() -> ChannelRecord {
    ChannelRecord {
        meta: SyncMeta {
            key: CHANNEL_KEY.to_string(),
            schema_version: 1,
            logical_version: 1,
            origin: "test".to_string(),
            updated_at: chrono::Utc::now(),
        },
        name: CHANNEL_KEY.to_string(),
        provider_type: "openai".to_string(),
        base_url: "http://upstream.invalid".to_string(),
        keys: vec![ChannelKey {
            index: 0,
            secret: "sk-test".to_string(),
            rpm_limit: 0,
        }],
        max_concurrency: 10,
        status: 1,
        groups: vec!["g".to_string()],
        settings: serde_json::Value::Null,
    }
}

/// Assembly: real Dispatcher (real health table) + recording wrapper + mock-egress `ForwardStage`.
/// Returns `(stage, recording dispatch, health)` (the test also holds health to observe table-side
/// changes).
fn build_stage(
    egress: Arc<dyn Egress>,
) -> (ForwardStage, Arc<RecordingDispatch>, Arc<MemoryHealthTable>) {
    let health = Arc::new(MemoryHealthTable::new());
    let dispatcher = Dispatcher::new(
        Some(Arc::new(Snapshot {
            units: vec![unit()],
            channels: std::collections::HashMap::from([(CHANNEL_KEY.to_string(), channel())]),
        })),
        Arc::clone(&health),
        Arc::new(gateway_gate::concurrency::ConcurrencyState::default()),
    );
    let recording = Arc::new(RecordingDispatch {
        inner: dispatcher,
        tps_calls: Mutex::new(Vec::new()),
    });
    let stage = ForwardStage::new(egress, Arc::new(FormatRegistry::new())).with_retry(
        Arc::clone(&recording) as Arc<dyn Dispatch>,
        RetryPolicy::default(),
    );
    (stage, recording, health)
}

// ---------- Scripted egress ----------

/// One upstream response script: frame sequence + inter-frame gap (ms). `gap_ms` makes the
/// "generation segment duration" measurable so tps values land in a realistic magnitude instead of
/// being pushed sky-high by the 1 ms floor.
#[derive(Clone)]
struct SseScript {
    frames: Vec<Result<Bytes, String>>,
    gap_ms: u64,
}

impl SseScript {
    fn stream(&self) -> impl Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static {
        let gap = Duration::from_millis(self.gap_ms);
        let frames = self.frames.clone();
        futures_util::stream::unfold((frames, 0usize), move |(frames, i)| async move {
            // Error frames are carried as String (`io::Error` is not Clone, and the script must be
            // reusable in order); only here do they become real `io::Error` fed into the upstream
            // body stream.
            let frame = frames.get(i)?.clone().map_err(std::io::Error::other);
            if i > 0 {
                tokio::time::sleep(gap).await;
            }
            Some((frame, (frames, i + 1)))
        })
    }
}

/// Egress mock that always returns a single scripted SSE stream.
struct ScriptedSseEgress(SseScript);

impl Egress for ScriptedSseEgress {
    fn execute<'a>(
        &'a self,
        _url: &'a str,
        _headers: &'a [(String, String)],
        _body: Bytes,
        _timeouts: &'a Timeouts,
    ) -> ExecFuture<'a> {
        let script = self.0.clone();
        Box::pin(async move {
            Ok(ForwardedResponse::from_stream(
                200,
                "text/event-stream",
                None,
                script.stream(),
            ))
        })
    }
}

/// Egress mock that always returns a single JSON body (non-streaming cases).
struct ScriptedJsonEgress(&'static [u8]);

impl Egress for ScriptedJsonEgress {
    fn execute<'a>(
        &'a self,
        _url: &'a str,
        _headers: &'a [(String, String)],
        _body: Bytes,
        _timeouts: &'a Timeouts,
    ) -> ExecFuture<'a> {
        let body = self.0;
        Box::pin(async move {
            Ok(ForwardedResponse::from_stream(
                200,
                "application/json",
                None,
                futures_util::stream::once(async move {
                    Ok::<Bytes, std::io::Error>(Bytes::from_static(body))
                }),
            ))
        })
    }
}

/// OpenAI-shaped content frame (payload length determines the estimated completion).
fn content_frame(text: &str) -> Bytes {
    Bytes::from(format!(
        "data: {{\"choices\":[{{\"delta\":{{\"content\":\"{text}\"}}}}]}}\n\n"
    ))
}

/// OpenAI-shaped usage frame (the tail frame supplies the measured completion).
fn usage_frame(prompt: u64, completion: u64) -> Bytes {
    Bytes::from(format!(
        "data: {{\"usage\":{{\"prompt_tokens\":{prompt},\"completion_tokens\":{completion}}}}}\n\n"
    ))
}

const DONE: &str = "data: [DONE]\n\n";

/// Normal long stream: content frames + usage frame + `[DONE]` (measured source ⇒ `Measured`).
fn measured_script(gap_ms: u64) -> SseScript {
    SseScript {
        frames: vec![
            Ok(content_frame("alpha beta gamma delta")),
            Ok(content_frame("epsilon zeta eta theta")),
            Ok(usage_frame(10, 120)),
            Ok(Bytes::from_static(DONE.as_bytes())),
        ],
        gap_ms,
    }
}

/// Normal long stream without a usage frame (metering falls back to bytes/4 estimation ⇒
/// `Estimated`).
fn estimated_script(gap_ms: u64) -> SseScript {
    SseScript {
        frames: vec![
            Ok(content_frame("alpha beta gamma delta epsilon zeta")),
            Ok(content_frame("eta theta iota kappa lambda mu")),
            Ok(Bytes::from_static(DONE.as_bytes())),
        ],
        gap_ms,
    }
}

/// Ultra-short burst: usage reports completion=7 (< threshold 8) ⇒ must not report.
fn short_script(gap_ms: u64) -> SseScript {
    SseScript {
        frames: vec![
            Ok(content_frame("alpha beta gamma delta")),
            Ok(usage_frame(10, 7)),
            Ok(Bytes::from_static(DONE.as_bytes())),
        ],
        gap_ms,
    }
}

// ---------- Helpers to run one request ----------

/// Streaming ctx (body carries `"stream":true`). route is pre-set with a candidate (after
/// `with_retry` the handle does not read it).
fn stream_ctx() -> RequestCtx {
    ctx_with_body(STREAM_BODY, candidate("c1"))
}

/// Non-streaming ctx.
fn json_ctx() -> RequestCtx {
    ctx_with_body(NON_STREAM_BODY, candidate("c1"))
}

/// Read a `PipeStream` as an axum data stream (consuming it means the client has received the full
/// body).
fn data_stream(pipe: PipeStream) -> impl Stream + Send {
    pipe.into_response().into_body().into_data_stream()
}

/// Run one streaming request and read the response body to the end of the stream (reading to the
/// end = the settlement point really ran).
async fn run_stream(stage: &ForwardStage, ctx: &mut RequestCtx) {
    let outcome = stage
        .handle(ctx)
        .await
        .expect("streaming request handle should succeed");
    let StageOutcome::Stream(pipe) = outcome else {
        panic!("streaming request should return Stream outcome");
    };
    let mut data = data_stream(pipe);
    while let Some(chunk) = data.next().await {
        // Body errors (e.g. an upstream break passed through) are expected in this case and are not
        // asserted — the settlement point has already run inside unfold; here we only drive it by
        // consuming the stream to the end.
        let _ = chunk;
    }
}

// ---------- Case 1: normal long stream ⇒ Measured sample reaches the health table ----------

/// One successful streaming round ⇒ `report_tps` exactly once, `Measured` variant + plausible
/// tok/s, and the health table's `tps_ema` really written (crossing crates into the table, not just
/// the mock).
///
/// bug: late report dropped (signal reaches Dispatch but not `HealthTable`) / variant judged as
/// Estimated / value miscalculated (using the first-packet instant or the whole request duration as
/// denominator) ⇒ red.
#[tokio::test]
async fn streaming_completion_reaches_health_table_as_measured() {
    let (stage, recording, health) = build_stage(Arc::new(ScriptedSseEgress(measured_script(20))));

    let mut ctx = stream_ctx();
    run_stream(&stage, &mut ctx).await;

    let calls = recording.tps_calls();
    assert_eq!(
        calls.len(),
        1,
        "normal complete read round should report tps exactly once, actual {calls:?}"
    );
    assert_eq!(
        calls[0].0, UNIT_KEY,
        "report key should be winning candidate's route unit key"
    );
    let value = match &calls[0].1 {
        TpsSample::Measured(v) => *v,
        TpsSample::Estimated(v) => {
            panic!("stream with usage frame should be a Measured sample, actual Estimated({v})")
        }
    };
    // 4 frames × 20ms ⇒ generation segment ~60ms, completion=120 ⇒ tps ≈ 2000.
    // The range is wide enough to absorb CI scheduling jitter (even if the generation segment is
    // several times slower it is still inside), and narrow enough to catch "denominator mistakenly
    // taken as the 1ms floor" (that would compute ~120000). The lower bound catches the systematic
    // underestimate caused by "denominator taken as the whole response including TTFT or client
    // backpressure".
    assert!(
        (300.0..=30_000.0).contains(&value),
        "tok/s should fall in reasonable range (approx 120 token / 60ms), actual {value:.1}"
    );

    let ema = health.get(UNIT_KEY).tps_ema;
    assert!(
        ema > 0.0,
        "health table tps_ema should be written by record_tps (cross-crate signal arrived), actual {ema}"
    );
    assert_eq!(
        ema, value,
        "first sample directly becomes tps_ema (health side semantics), mock and health table observations must agree"
    );
}

/// A late report only merges into `tps_ema`: cooldown/streak/401 counters/cooldown deadline are not
/// driven by the late signal (pins the forward-side expression of "the late channel does not
/// pollute existing health accounting").
///
/// bug: late report wrongly goes through the `report` channel (incidentally changing
/// cooldown/streak) ⇒ red.
#[tokio::test]
async fn late_report_does_not_touch_other_health_ledger() {
    let (stage, recording, health) = build_stage(Arc::new(ScriptedSseEgress(measured_script(20))));

    let mut ctx = stream_ctx();
    run_stream(&stage, &mut ctx).await;

    let health_state = health.get(UNIT_KEY);
    assert_eq!(
        recording.tps_calls().len(),
        1,
        "precondition: exactly one late report"
    );
    assert_eq!(
        health_state.cooldown_streak, 0,
        "late tps should not record channel failure streak"
    );
    assert_eq!(
        health_state.unauthorized_run, 0,
        "late tps should not record 401 escalation count"
    );
    assert_eq!(
        health_state.cooldown_until_ms, 0,
        "late tps should not enter cooldown"
    );
}

// ---------- Case 2: no usage ⇒ Estimated sample ----------

/// Upstream provides no usage (metering falls back to bytes/4 estimation) ⇒ the sample that
/// arrives is `Estimated`.
///
/// bug: wrong variant (estimation fed to α=0.2 as if measured) ⇒ noise mixed into the main EMA,
/// red.
#[tokio::test]
async fn stream_without_usage_reports_estimated_sample() {
    let (stage, recording, health) = build_stage(Arc::new(ScriptedSseEgress(estimated_script(20))));

    let mut ctx = stream_ctx();
    run_stream(&stage, &mut ctx).await;

    let calls = recording.tps_calls();
    assert_eq!(
        calls.len(),
        1,
        "normal long stream without usage should still report once, actual {calls:?}"
    );
    match &calls[0].1 {
        TpsSample::Estimated(v) => assert!(
            *v > 0.0,
            "estimated sample value should be positive (estimation fallback completion ≥ 8 to pass threshold), actual {v}"
        ),
        TpsSample::Measured(v) => {
            panic!("stream without usage frame should not be Measured, actual Measured({v})")
        }
    }
    assert!(
        health.get(UNIT_KEY).tps_ema > 0.0,
        "estimated sample also merged into tps_ema (just α halved)"
    );
}

// ---------- Case 3: completion < 8 not reported, and does not pollute the EMA ----------

/// Ultra-short burst (completion=7) ⇒ 0 reports; when inserted after a normal stream `tps_ema`
/// does not move at all (pins "noise samples do not pollute").
///
/// bug: completion<8 threshold broken (missed check or wrong threshold) ⇒ report count non-zero /
/// ema dragged, red.
#[tokio::test]
async fn short_burst_is_dropped_and_keeps_ema_intact() {
    // egress emits scripts in call order: normal long stream (build baseline) → ultra-short burst
    // ×2.
    let scripted =
        SequencedEgress::new(vec![measured_script(20), short_script(5), short_script(5)]);
    let (stage, recording, health) = build_stage(Arc::new(scripted));

    let mut ctx = stream_ctx();
    run_stream(&stage, &mut ctx).await;
    let baseline = health.get(UNIT_KEY).tps_ema;
    assert!(
        baseline > 0.0,
        "precondition: first normal stream should establish tps_ema baseline"
    );

    for _ in 0..2 {
        let mut ctx = stream_ctx();
        run_stream(&stage, &mut ctx).await;
    }

    assert_eq!(
        recording.tps_calls().len(),
        1,
        "ultra-short burst should not report (only the first normal stream's once), actual {:?}",
        recording.tps_calls()
    );
    assert_eq!(
        health.get(UNIT_KEY).tps_ema,
        baseline,
        "noise sample (completion<8) must not modify tps_ema"
    );
}

// ---------- Case 4: broken stream not reported ----------

/// Upstream breaks mid-stream (Err inside the stream) ⇒ `report_tps` 0 times: at the break moment
/// `finish_at` would write a low throughput high, and it must never enter the EMA (design §3.2).
///
/// bug: the broken-stream arm misreports ⇒ red.
#[tokio::test]
async fn truncated_stream_reports_nothing() {
    let script = SseScript {
        frames: vec![
            Ok(content_frame("alpha beta gamma delta")),
            Ok(content_frame("epsilon zeta eta theta")),
            Err("upstream stream broken".to_string()),
        ],
        gap_ms: 5,
    };
    let (stage, recording, health) = build_stage(Arc::new(ScriptedSseEgress(script)));

    let mut ctx = stream_ctx();
    run_stream(&stage, &mut ctx).await;

    assert_eq!(
        recording.tps_calls().len(),
        0,
        "truncated stream round tps report should be 0 times, actual {:?}",
        recording.tps_calls()
    );
    assert_eq!(
        health.get(UNIT_KEY).tps_ema,
        0.0,
        "broken stream produces no throughput sample, tps_ema should remain 0"
    );
}

// ---------- Case 5: non-streaming not reported ----------

/// Non-streaming request ⇒ `report_tps` 0 times: the factor must always be neutral (design §3.2 /
/// §4 invariant 2).
///
/// bug: non-streaming also reports ⇒ red.
#[tokio::test]
async fn non_streaming_request_reports_nothing() {
    let (stage, recording, health) = build_stage(Arc::new(ScriptedJsonEgress(
        br#"{"usage":{"prompt_tokens":100,"completion_tokens":400}}"#,
    )));

    let mut ctx = json_ctx();
    stage
        .handle(&mut ctx)
        .await
        .expect("non-streaming handle should succeed");

    assert_eq!(
        recording.tps_calls().len(),
        0,
        "non-streaming should not report tps, actual {:?}",
        recording.tps_calls()
    );
    assert_eq!(
        health.get(UNIT_KEY).tps_ema,
        0.0,
        "non-streaming does not write tps_ema"
    );
}

// ---------- Egress mock that rotates scripts by call order ----------

struct SequencedEgress {
    scripts: Mutex<Vec<SseScript>>,
}

impl SequencedEgress {
    fn new(scripts: Vec<SseScript>) -> Self {
        Self {
            scripts: Mutex::new(scripts),
        }
    }
}

impl Egress for SequencedEgress {
    fn execute<'a>(
        &'a self,
        _url: &'a str,
        _headers: &'a [(String, String)],
        _body: Bytes,
        _timeouts: &'a Timeouts,
    ) -> ExecFuture<'a> {
        // Consume in declaration order (the first request uses the first script): `remove(0)`
        // rather than `pop` — in this test the scripts are ordered "normal stream builds the
        // baseline first, ultra-short bursts later".
        let mut scripts = self.scripts.lock();
        let script = scripts
            .first()
            .cloned()
            .expect("scripts exhausted: request count exceeds preset rounds");
        scripts.remove(0);
        Box::pin(async move {
            Ok(ForwardedResponse::from_stream(
                200,
                "text/event-stream",
                None,
                script.stream(),
            ))
        })
    }
}

// ---------- Ceiling group: sub-ms bogus / oversized / normal / boundary equality ----------

/// Test-side mirror of the production ceiling (the production constant
/// `stage_telemetry::TPS_SAMPLE_MAX` is module-private (no `pub`), so integration tests cannot
/// reach it; this is the same physical quantity, 10000 tok/s, and the two must agree).
const TPS_SAMPLE_MAX: f64 = 10_000.0;

/// Ceiling-group script: the usage frame supplies the measured `completion`, frame gap is `gap_ms`.
///
/// `gap_ms == 0` ⇒ content and terminal frames arrive back-to-back in the same chunk ⇒ the
/// generation segment is sub-millisecond ⇒ rounded up to 1 ms by the floor ⇒ value =
/// `completion * 1000` tok/s (the generation path of sub-millisecond bogus values);
/// `gap_ms >= 1` ⇒ each frame sleeps at least gap, the generation segment is real elapsed time and
/// does not hit the floor.
fn ceiling_script(completion: u64, gap_ms: u64) -> SseScript {
    SseScript {
        frames: vec![
            Ok(content_frame("alpha beta gamma delta")),
            Ok(usage_frame(10, completion)),
            Ok(Bytes::from_static(DONE.as_bytes())),
        ],
        gap_ms,
    }
}

/// Sub-millisecond short stream ⇒ 0 reports, the bogus value never touches the EMA.
///
/// 120 token ÷ 1ms floor = 120000 tok/s. Without the ceiling, this would directly become the
/// channel's first `tps_ema` (the health side does no EMA smoothing for the first sample); after
/// `pool_max_tps` takes the pool maximum, the factors of the other real channels are pinned at the
/// 0.2 floor by `clamp(50/120000, 0.2, 1.0)`.
///
/// bug: missing `TPS_SAMPLE_MAX` early return ⇒ `tps_calls` non-zero / `tps_ema` written with
/// 120000, red.
#[tokio::test]
async fn sub_millisecond_stream_reports_nothing() {
    let (stage, recording, health) =
        build_stage(Arc::new(ScriptedSseEgress(ceiling_script(120, 0))));

    let mut ctx = stream_ctx();
    run_stream(&stage, &mut ctx).await;

    assert_eq!(
        recording.tps_calls().len(),
        0,
        "sub-millisecond short stream's 120000 tok/s bogus value should not be reported, actual {:?}",
        recording.tps_calls()
    );
    assert_eq!(
        health.get(UNIT_KEY).tps_ema,
        0.0,
        "sub-millisecond bogus value must not enter tps_ema (otherwise poisons the whole pool pool_max_tps)"
    );
}

/// Artificial sample above the ceiling but whose generation segment is real elapsed time (not
/// sub-millisecond) ⇒ still 0 reports, EMA still 0.
///
/// 1000 token ÷ 3 frames 1ms apart ≈ 500000 tok/s; the generation segment is above the floor —
/// what this pins is not "only sub-millisecond blows up" but that any non-physical value must be
/// dropped; `tps_ema` staying 0 pins **drop rather than clamp**: clamping would write the fake
/// `10_000` throughput into the EMA.
///
/// bug: ceiling missed / ceiling written as a clamp (`value.min(TPS_SAMPLE_MAX)`) ⇒ count non-zero
/// or `tps_ema` equals `TPS_SAMPLE_MAX`, red.
#[tokio::test]
async fn oversized_non_submillisecond_sample_is_dropped_not_clamped() {
    let (stage, recording, health) =
        build_stage(Arc::new(ScriptedSseEgress(ceiling_script(1000, 1))));

    let mut ctx = stream_ctx();
    run_stream(&stage, &mut ctx).await;

    assert_eq!(
        recording.tps_calls().len(),
        0,
        "over-ceiling sample (≈500000 tok/s) should not be reported, actual {:?}",
        recording.tps_calls()
    );
    assert_eq!(
        health.get(UNIT_KEY).tps_ema,
        0.0,
        "over-ceiling sample must be discarded: clamping to {TPS_SAMPLE_MAX} would write fake throughput into EMA"
    );
}

/// Normal throughput above the floor and below the ceiling ⇒ exactly 1 report (guardrail: the
/// ceiling must not incidentally filter out genuine samples).
///
/// 120 token ÷ 2 intervals of 20ms ≈ 3000 tok/s, within the realistic single-stream generation
/// magnitude (2000–4000 tok/s) and an order of magnitude below the ceiling.
///
/// bug: ceiling set too low / criterion wrong ⇒ normal throughput dropped as a bogus value,
/// `tps_calls` is 0, red.
#[tokio::test]
async fn normal_sample_between_floor_and_ceiling_reported_once() {
    let (stage, recording, health) =
        build_stage(Arc::new(ScriptedSseEgress(ceiling_script(120, 20))));

    let mut ctx = stream_ctx();
    run_stream(&stage, &mut ctx).await;

    let calls = recording.tps_calls();
    assert_eq!(
        calls.len(),
        1,
        "normal throughput (above floor, below ceiling) should report exactly once, actual {calls:?}"
    );
    let value = match &calls[0].1 {
        TpsSample::Measured(v) => *v,
        TpsSample::Estimated(v) => {
            panic!("stream with usage frame should be Measured, actual Estimated({v})")
        }
    };
    assert!(
        (0.0..=TPS_SAMPLE_MAX).contains(&value),
        "normal sample value should fall in (0, {TPS_SAMPLE_MAX}], actual {value:.1}"
    );
    assert_eq!(
        health.get(UNIT_KEY).tps_ema,
        value,
        "normal sample still first sample passes straight through to tps_ema (health side semantics not changed by ceiling)"
    );
}

/// Value in the ceiling neighborhood (≤ ceiling) ⇒ still reported (the criterion is `>` not `>=`,
/// the boundary is not dropped).
///
/// A 10-token short burst (`gap_ms=0`, generation segment as short as possible) ⇒ the computed value
/// lands near `10_000` and **does not exceed** the ceiling (the denominator is held up by the 1ms
/// floor, value = 10 ÷ ≥1ms ≤ `10_000`). On the production side `report_stream_tps`'s criterion is
/// `value > TPS_SAMPLE_MAX` — equal to the ceiling is not wrongly dropped. This pins the reporting
/// side: `>=` or a ceiling set too low (e.g. the 10_000-1 notch) both wrongly drop
/// boundary-neighborhood samples. Together with the drop side pinned by the oversized case, the
/// threshold position is squeezed into the interval (this case's value, value above the ceiling).
///
/// bug: criterion written as `>=` and the value raised to ≥ ceiling / ceiling set too low ⇒
/// boundary sample wrongly dropped, `tps_calls` is 0, red.
#[tokio::test]
async fn sample_at_or_below_ceiling_is_still_reported() {
    let (stage, recording, health) =
        build_stage(Arc::new(ScriptedSseEgress(ceiling_script(10, 0))));

    let mut ctx = stream_ctx();
    run_stream(&stage, &mut ctx).await;

    let calls = recording.tps_calls();
    assert_eq!(
        calls.len(),
        1,
        "sample with value in ceiling neighborhood (≤ {TPS_SAMPLE_MAX}) should still report once, actual {calls:?}"
    );
    let value = match &calls[0].1 {
        TpsSample::Measured(v) => *v,
        TpsSample::Estimated(v) => {
            panic!("stream with usage frame should be Measured, actual Estimated({v})")
        }
    };
    assert!(
        value <= TPS_SAMPLE_MAX,
        "this sample value should not exceed ceiling (10 token ÷ ≥1ms floor), actual {value:.6}"
    );
    assert_eq!(
        health.get(UNIT_KEY).tps_ema,
        value,
        "boundary/neighborhood sample merged into tps_ema (ceiling only drops > unphysical values)"
    );
}
