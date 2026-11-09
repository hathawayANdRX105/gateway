//! Lifecycle tests for prefix-cache sticky binding writes (design
//! prompt-cache-sticky-routing §3.3).
//!
//! Wiring shape: `ForwardStage::with_sticky_table` and the read side share one
//! `Arc<StickyTable>` — `DispatchStage::with_sticky_table` in these tests and, in
//! production, the forwarding stage's own retry path. Settlement commit points —
//! the streaming read-to-end arm + the non-streaming settlement path — bind the
//! winning channel into the table only when `counts.cached > 0` is **measured**;
//! a cold request with `cached == 0`, a response without usage, an upstream stream
//! break (500 arm), and a client disconnect (stream dropped, neither arm runs)
//! never write; an unmounted table skips the whole write side (settlement
//! semantics unchanged, the single charge write point unchanged).
//!
//! **Same-source contract** (hard ticket constraint): the write-side key and the
//! read side's `DispatchStage::sticky_channel_for` share one source — the public
//! model is `ctx.requested_model`, the session hint is `ctx.session_hint` → the
//! `x-session-id` header, and the fingerprint is the **raw bytes** of
//! `ctx.request.body`'s `BodySource::InMemory`. The protocol-adapted body is never
//! used: the read side (selection happens before `Dispatch::select`) physically
//! cannot obtain the adapted body, and different sources on the two sides would
//! make stickiness never hit.
//!
//! Covered behaviour (mock egress, zero real network; TTL uses a fake clock and
//! does not depend on the wall clock):
//! 1. non-streaming 2xx + measured `cached_tokens > 0` ⇒ a binding appears in the
//!    table, and the read side DispatchStage (same table) finds the same channel on
//!    the original ctx — write/read round-trip cross-check (key-construction drift ⇒
//!    lookup miss; wrong bound channel ⇒ channel key mismatch);
//! 2. binding TTL: invalid on expiry, valid before expiry (fake clock advanced; the
//!    boundary semantics "valid only when `expire_at > now`" are pinned);
//! 3. `session_hint` wins over the body fingerprint: the table holds only `sid:`
//!    keys; a read side without a prefilled hint falls back to the `x-session-id`
//!    header and still hits the same binding (a broken header path or reversed
//!    priority turns this red);
//! 4. `cached == 0` (no cache field) ⇒ no write (binding a cold request's channel
//!    would turn this red);
//! 5. response without usage ⇒ no write (writing the table from estimated counts
//!    would turn this red);
//! 6. upstream stream break (error inside the stream, 500 arm) ⇒ no write (writing
//!    on the failure path would turn this red);
//! 7. client disconnect (drop the stream after reading the first frame) ⇒ no write
//!    (writing from a drop hook would turn this red);
//! 8. table not mounted ⇒ forwarding behaviour matches the status quo (status code
//!    + body pass back unchanged), the write side is skipped entirely.
//! 9. streaming 2xx read to the end + cache hit measured within frames (OpenAI
//!    shape) ⇒ the "read-to-end arm" commit point writes the table, and the read
//!    side finds the same channel on the original ctx (a write/read round trip
//!    isomorphic to non-streaming case 1);
//! 10. streaming Claude two-frame shape (`message_start` carrying `input_tokens` +
//!     `cache_read_input_tokens`, `message_delta` carrying `output_tokens`) ⇒ the
//!     `cached` merged across frames is also written to the table;
//! 11. streaming cold request (usage but no cache signal, measured `cached == 0`) ⇒
//!     no write (guards the streaming arm's `cached > 0` gate, isomorphic to
//!     non-streaming case 4);
//! 12. production-entry read-side wiring (`with_retry_handle` →
//!     `handle_with_retry` → `run_retry_loop`): the channel bound by the write side
//!     gets a prior boost in retry selection and is chosen (read-side key source and
//!     write-side commit point share one source);
//! 13. the same entry's session-hint key (`sid:`) also hits ⇒ the bound channel is
//!     chosen (entry-level proof that key-source priority shares one source with the
//!     write side);
//! 14. table not mounted (= production `sticky_enabled=false`) ⇒ no panic, selection
//!     matches the status quo (the degraded mode of design §4 invariant 2).
//!
//! Trigger conditions of the streaming-arm commit point (pinned by cases 9-11):
//! streaming-frame usage is merged across frames by metering
//! (`metering/src/scanner.rs`), `cached` recognizes OpenAI `prompt_tokens_details` /
//! top-level `cached_tokens` / Claude `cache_read_input_tokens`, so when a successful
//! stream finishes the "read-to-end arm" commit point writes the table with measured
//! values; a cold stream (no cache signal in the frames ⇒ `cached == 0`) still writes
//! nothing. Cases 6/7 pin "no write on stream break / drop" independently of in-frame
//! fields and do not change with this section.
//!
//! Production-entry read-side wiring (pinned by cases 12-14): the production
//! pipeline only pushes ForwardStage (which drives its own retry loop); the
//! DispatchStage read side is not in the pipeline — so the read-side table lookup
//! happens in `handle_with_retry`: before calling `run_retry_loop` it looks up the
//! mounted sticky table once per request (key source shared with the write-side
//! commit point) and passes the sticky channel key through unchanged as the prior
//! for every `select` round.
//! The mock folds the boost into a deterministic choice (the sticky channel wins
//! when it is in the pool and not excluded); the real selector's weighted-sampling
//! semantics are covered by the dispatch crate's tests.

mod common;

use arc_swap::ArcSwap;
use bytes::Bytes;
use futures_util::Stream;
use futures_util::StreamExt;
use gateway_dispatch::health::AttemptReport;
use gateway_dispatch::{
    Candidate, Dispatch, DispatchError, DispatchStage, RetryPolicy, StickyKey, StickyTable,
    TpsSample,
};
use gateway_forward::ForwardStage;
use gateway_forward::egress::{Egress, ForwardedResponse, Timeouts};
use gateway_pipeline::{PipeStream, RequestCtx, Stage, StageOutcome};
use gateway_protocol_bridge::format_codec::FormatRegistry;
use parking_lot::Mutex;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use common::*;

/// Winning channel key (generated as `ch-{key}` by `common::candidate`).
const CH_C1: &str = "ch-c1";

/// TTL of the test table (milliseconds). Matching the magnitude of
/// [`gateway_dispatch::sticky::DEFAULT_TTL_MS`] is enough — these cases assert the
/// boundary semantics of "invalid on expiry", not a specific number of seconds.
const TTL_MS: u64 = 300_000;

/// The future returned by egress's `ForwardedResponse` (alias for the narrowed
/// trait signature).
type ExecFuture<'a> = Pin<
    Box<
        dyn Future<Output = Result<ForwardedResponse, gateway_core::error::NormalizedError>>
            + Send
            + 'a,
    >,
>;

// ---------- test helpers ----------

/// Test table + an advanceable fake clock ([`StickyTable::with_clock`]).
///
/// Production uses the `chrono::Utc` wall clock; tests inject a clock instead, so
/// TTL expiry assertions are unaffected by real time and need no sleep.
fn sticky_table() -> (Arc<StickyTable>, Arc<AtomicU64>) {
    let now = Arc::new(AtomicU64::new(1_000_000));
    let clock = Arc::clone(&now);
    let table = StickyTable::with_clock(move || clock.load(Ordering::Relaxed), TTL_MS, 64);
    (table, now)
}

/// Non-streaming mock egress: always returns 200 + the injected JSON body (usage
/// shape chosen by the case).
struct JsonEgress {
    body: &'static [u8],
}

impl Egress for JsonEgress {
    fn execute<'a>(
        &'a self,
        _url: &'a str,
        _headers: &'a [(String, String)],
        _body: Bytes,
        _timeouts: &'a Timeouts,
    ) -> ExecFuture<'a> {
        let body = self.body;
        Box::pin(async move {
            Ok(ForwardedResponse::from_stream(
                200,
                "application/json",
                None,
                futures_util::stream::iter(vec![Ok::<Bytes, std::io::Error>(Bytes::from_static(
                    body,
                ))]),
            ))
        })
    }
}

/// SSE mock usage frame (OpenAI shape: cache in `prompt_tokens_details`).
///
/// Case 9 uses it to prove "successful stream + measured cache hit ⇒ table write";
/// the stream-break / drop cases (6/7) use it to prove the failure path does not
/// write just because the frame carries a cache field.
const CACHED_USAGE_FRAME: &[u8] =
    b"data: {\"usage\":{\"prompt_tokens\":90,\"completion_tokens\":30,\"prompt_tokens_details\":{\"cached_tokens\":40}}}\n\n";

/// SSE terminal frame (OpenAI convention). A successful stream read to the end =
/// the client first receives `[DONE]`, then the stream ends.
const DONE_FRAME: &[u8] = br#"data: [DONE]

"#;

/// OpenAI-shape usage frame but **without a cache field**: a cold stream (the frame
/// carries measured usage ⇒ prompt / completion non-zero, but `cached == 0`).
const COLD_USAGE_FRAME: &[u8] = br#"data: {"usage":{"prompt_tokens":100,"completion_tokens":10}}

"#;

/// Claude `message_start` frame: `message.usage` carries `input_tokens` and
/// `cache_read_input_tokens` (the cache read hits in this frame).
const CLAUDE_MESSAGE_START_FRAME: &[u8] = br#"data: {"type":"message_start","message":{"usage":{"input_tokens":90,"cache_read_input_tokens":40}}}

"#;

/// Claude `message_delta` frame: the top-level `usage` carries only `output_tokens`.
/// The first frame's `input_tokens` / `cache_read_input_tokens` survive by field-wise
/// merging across frames.
const CLAUDE_MESSAGE_DELTA_FRAME: &[u8] =
    br#"data: {"type":"message_delta","usage":{"output_tokens":30}}

"#;

/// Upstream stream-break mock: one normal frame + an error on the last frame (Err
/// inside the stream).
struct BrokenSseEgress;

impl Egress for BrokenSseEgress {
    fn execute<'a>(
        &'a self,
        _url: &'a str,
        _headers: &'a [(String, String)],
        _body: Bytes,
        _timeouts: &'a Timeouts,
    ) -> ExecFuture<'a> {
        Box::pin(async move {
            Ok(ForwardedResponse::from_stream(
                200,
                "text/event-stream",
                None,
                futures_util::stream::iter(vec![
                    Ok(Bytes::from_static(CACHED_USAGE_FRAME)),
                    Err(std::io::Error::new(
                        std::io::ErrorKind::ConnectionAborted,
                        "upstream broke mid-stream",
                    )),
                ]),
            ))
        })
    }
}

/// Two-frame SSE mock: the first frame is emitted immediately, and before the second
/// the stream parks on a `Notify` waiting for release — so the "upstream is still
/// sending" window really exists and the client-disconnect case can stop mid-stream.
struct GatedSseEgress {
    gate: Arc<tokio::sync::Notify>,
}

impl GatedSseEgress {
    fn new() -> Self {
        Self {
            gate: Arc::new(tokio::sync::Notify::new()),
        }
    }
}

impl Egress for GatedSseEgress {
    fn execute<'a>(
        &'a self,
        _url: &'a str,
        _headers: &'a [(String, String)],
        _body: Bytes,
        _timeouts: &'a Timeouts,
    ) -> ExecFuture<'a> {
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

/// N-frame SSE mock: emits frames in order (one chunk per frame, matching a real
/// upstream's frame granularity) and ends the stream normally after the last one —
/// used by cases where "the client reads the whole stream" walks the read-to-end
/// arm's settlement commit point. Mid-stream break / client disconnect have dedicated
/// mocks (see [`BrokenSseEgress`] / [`GatedSseEgress`]); this one covers only
/// "successful read to end".
struct SseFramesEgress {
    frames: &'static [&'static [u8]],
}

impl Egress for SseFramesEgress {
    fn execute<'a>(
        &'a self,
        _url: &'a str,
        _headers: &'a [(String, String)],
        _body: Bytes,
        _timeouts: &'a Timeouts,
    ) -> ExecFuture<'a> {
        let frames = self.frames;
        Box::pin(async move {
            Ok(ForwardedResponse::from_stream(
                200,
                "text/event-stream",
                None,
                futures_util::stream::iter(
                    frames
                        .iter()
                        .copied()
                        .map(|f| Ok::<Bytes, std::io::Error>(Bytes::from_static(f))),
                ),
            ))
        })
    }
}

/// First frame = usage frame; before the second frame it waits for a `Notify` release,
/// then emits `[DONE]`; the next poll ends the stream.
fn gated_sse_stream(
    gate: Arc<tokio::sync::Notify>,
) -> impl Stream<Item = Result<Bytes, std::io::Error>> + Send {
    futures_util::stream::unfold(0u8, move |step| {
        let gate = Arc::clone(&gate);
        async move {
            match step {
                0 => Some((Ok(Bytes::from_static(CACHED_USAGE_FRAME)), 1)),
                1 => {
                    gate.notified().await;
                    Some((Ok(Bytes::from_static(b"data: [DONE]\n\n")), 2))
                }
                _ => None,
            }
        }
    })
}

/// Streaming ctx (body carries `"stream":true`), route prefilled with a candidate
/// (read by single mode).
fn stream_ctx() -> RequestCtx {
    ctx_with_body(STREAM_BODY, candidate("c1"))
}

/// Runs one `handle` round in single mode; panics with the StageError Debug on
/// failure.
async fn run_single(stage: &ForwardStage, ctx: &mut RequestCtx) -> StageOutcome {
    stage
        .handle(ctx)
        .await
        .expect("single forward should succeed")
}

/// Extracts the PipeStream after a successful handle (panics with the StageError
/// Debug on failure).
async fn expect_stream(stage: &ForwardStage, ctx: &mut RequestCtx) -> PipeStream {
    match stage.handle(ctx).await.expect("handle should succeed") {
        StageOutcome::Stream(pipe) => pipe,
        other => panic!("streaming request should return Stream, got {other:?}"),
    }
}

/// Reads the PipeStream as an axum data stream (consuming it = the client has
/// received the whole body).
fn data_stream(pipe: PipeStream) -> impl Stream<Item = Bytes> + Send {
    pipe.into_response()
        .into_body()
        .into_data_stream()
        .map(|chunk| chunk.expect("body chunk should not error"))
}

/// Stream-break cases only: when the upstream errors mid-stream the error item must
/// be **passed through** to the client, and the read side must not treat it as a test
/// failure panic — keep the `Result` here and let each case assert on it.
fn raw_data_stream(pipe: PipeStream) -> impl Stream<Item = Result<Bytes, axum::Error>> + Send {
    pipe.into_response().into_body().into_data_stream()
}

/// Read-side DispatchStage (shares the same table as the write side): production
/// mounts one `Arc` used by both the write and read sides.
fn reader(table: &Arc<StickyTable>) -> DispatchStage {
    DispatchStage::new(Arc::new(MockDispatch::new(vec![candidate("c1")])))
        .with_sticky_table(Arc::clone(table))
}

/// This request's key under the read-side rules (raw-body fingerprint, no session
/// hint).
fn fingerprint_key() -> StickyKey {
    StickyKey::from_request("m", None, Some(NON_STREAM_BODY))
        .expect("non-empty raw body must have fingerprint key")
}

/// Streaming request's key under the read-side rules (raw-body fingerprint, no
/// session hint).
fn stream_fingerprint_key() -> StickyKey {
    StickyKey::from_request("m", None, Some(STREAM_BODY))
        .expect("non-empty raw body must have fingerprint key")
}

// ---------- case 1 + 2: table write + read-side round trip + TTL boundary ----------

/// Measured cache hit ⇒ write the table, and the read side finds the same channel on
/// the original ctx; TTL expiry invalidates it.
///
/// Which bug turns this red:
/// - write-side key source drift (using the protocol-adapted body / the candidate's
///   `public_model` instead of `ctx.requested_model`) ⇒ the read side misses the table
///   lookup by raw body;
/// - wrong bound channel (binding `upstream_model` / `route_unit_key` instead of
///   `channel_key`) ⇒ channel keys do not match;
/// - the write mistakenly hanging off the billing gate (nothing written without a
///   `price_table`) ⇒ empty table.
#[tokio::test]
async fn cached_hit_binds_channel_and_reader_round_trips() {
    let (table, now) = sticky_table();
    let egress = Arc::new(JsonEgress {
        body: br#"{"usage":{"prompt_tokens":100,"completion_tokens":10,"prompt_tokens_details":{"cached_tokens":50}}}"#,
    });
    let stage = ForwardStage::new(egress, Arc::new(FormatRegistry::new()))
        .with_sticky_table(Arc::clone(&table));

    let mut ctx = ctx_with_body(NON_STREAM_BODY, candidate("c1"));
    run_single(&stage, &mut ctx).await;

    // Write/read round-trip cross-check: the read side finds the winning channel by
    // (public model × raw-body fingerprint).
    assert_eq!(
        reader(&table).sticky_channel_for(&ctx).as_deref(),
        Some(CH_C1),
        "binding written by settle must be found by read side (same table, same source key)"
    );

    // TTL boundary (fake clock, no sleep): valid only when `expire_at > now`.
    now.fetch_add(TTL_MS - 1, Ordering::Relaxed);
    assert_eq!(
        table.lookup(&fingerprint_key(), table.now_ms()).as_deref(),
        Some(CH_C1),
        "should still be valid 1ms before expiry"
    );
    now.fetch_add(1, Ordering::Relaxed);
    assert_eq!(
        table.lookup(&fingerprint_key(), table.now_ms()),
        None,
        "expires at the moment of expiry (`expire_at <= now` read-time expired)"
    );
}

// ---------- case 3: session_hint priority + header fallback sharing the source ----------

/// The session-hint key wins over the body fingerprint, and the read side still hits
/// the same binding through the `x-session-id` header fallback.
///
/// Which bug turns this red: the write side ignores `ctx.session_hint` (writes the
/// fingerprint key) ⇒ `sid:` lookup misses; the write side treats an empty/blank hint
/// as a valid session (all sessions collapse into one key) ⇒ wrong key content; the
/// read-side header fallback breaks ⇒ a header-only ctx cannot find the binding.
#[tokio::test]
async fn session_hint_key_wins_over_body_fingerprint() {
    let (table, _now) = sticky_table();
    let egress = Arc::new(JsonEgress {
        body: br#"{"usage":{"prompt_tokens":100,"completion_tokens":10,"cached_tokens":50}}"#,
    });
    let stage = ForwardStage::new(egress, Arc::new(FormatRegistry::new()))
        .with_sticky_table(Arc::clone(&table));

    let mut ctx = ctx_with_body(NON_STREAM_BODY, candidate("c1"));
    ctx.session_hint = Some("s-1".into());
    run_single(&stage, &mut ctx).await;

    let sid = StickyKey::from_request("m", Some("s-1"), Some(NON_STREAM_BODY)).unwrap();
    assert_eq!(
        table.lookup(&sid, table.now_ms()).as_deref(),
        Some(CH_C1),
        "request with session hint should write `sid:` key"
    );
    assert_eq!(
        table.lookup(&fingerprint_key(), table.now_ms()),
        None,
        "with session hint present, must not write body fingerprint key (otherwise same session splits into two keys)"
    );

    // Read side with no prefilled hint, taking the `x-session-id` header fallback ⇒
    // hits the same binding.
    let mut ctx_header = ctx_with_body(NON_STREAM_BODY, candidate("c1"));
    ctx_header.request.headers.insert(
        http::HeaderName::from_static("x-session-id"),
        "s-1".parse().unwrap(),
    );
    assert_eq!(
        reader(&table).sticky_channel_for(&ctx_header).as_deref(),
        Some(CH_C1),
        "header fallback should share source with write side's sid key"
    );
}

// ---------- case 4: cached == 0 ⇒ no write ----------

/// A cold request (usage without a cache field ⇒ measured `cached == 0`) does not
/// write the table.
///
/// Which bug turns this red: writing on "2xx success" alone, missing `cached > 0` ⇒
/// every cold request pins its channel (the shape design §3.3 explicitly avoids).
#[tokio::test]
async fn cold_request_does_not_bind() {
    let (table, _now) = sticky_table();
    let egress = Arc::new(JsonEgress {
        body: br#"{"usage":{"prompt_tokens":100,"completion_tokens":10}}"#,
    });
    let stage = ForwardStage::new(egress, Arc::new(FormatRegistry::new()))
        .with_sticky_table(Arc::clone(&table));

    let mut ctx = ctx_with_body(NON_STREAM_BODY, candidate("c1"));
    run_single(&stage, &mut ctx).await;

    assert!(
        table.is_empty(),
        "cold request with cached == 0 does not write table (avoid pinning a channel that produced no cache benefit)"
    );
}

// ---------- case 5: no usage ⇒ no write ----------

/// A response without usage (extracted value None, takes the estimate fallback with
/// `cached = 0`) does not write the table.
///
/// Which bug turns this red: judging "cache hit" from fallback estimated counts ⇒
/// channels that never return usage get pinned indiscriminately.
#[tokio::test]
async fn response_without_usage_does_not_bind() {
    let (table, _now) = sticky_table();
    let egress = Arc::new(JsonEgress {
        body: br#"{"choices":[{"message":{"content":"hi"}}]}"#,
    });
    let stage = ForwardStage::new(egress, Arc::new(FormatRegistry::new()))
        .with_sticky_table(Arc::clone(&table));

    let mut ctx = ctx_with_body(NON_STREAM_BODY, candidate("c1"));
    run_single(&stage, &mut ctx).await;

    assert!(
        table.is_empty(),
        "no usage (treated as miss) does not write table"
    );
}

// ---------- case 6: upstream stream break ⇒ no write ----------

/// An error inside the stream (the `finish(500)` arm) does not write the table, even
/// if the frames carry a cache field.
///
/// Which bug turns this red: widening the write point from the read-to-end arm to the
/// stream-break arm ⇒ a half-finished response also binds its channel, pinning later
/// requests to a half-dead channel.
#[tokio::test]
async fn broken_stream_does_not_bind() {
    let (table, _now) = sticky_table();
    let egress = Arc::new(BrokenSseEgress);
    let stage = ForwardStage::new(egress, Arc::new(FormatRegistry::new()))
        .with_sticky_table(Arc::clone(&table));

    let mut ctx = stream_ctx();
    let mut data = raw_data_stream(expect_stream(&stage, &mut ctx).await);
    assert!(
        data.next()
            .await
            .is_some_and(|c| c.is_ok_and(|b| !b.is_empty())),
        "before stream break, first frame should be emitted normally"
    );
    assert!(
        data.next().await.is_some_and(|c| c.is_err()),
        "stream break should end with an error item (error passed through to client as-is)"
    );
    assert!(
        table.is_empty(),
        "upstream stream break (500 arm) does not write table"
    );
}

// ---------- case 7: client disconnect ⇒ no write ----------

/// A client that disconnects right after the first frame (stream dropped) does not
/// write the table.
///
/// Which bug turns this red: adding a Drop hook on the unfold stream state that writes
/// the table ⇒ requests cut off by the client also write, sticking the user to a
/// channel it never finished.
#[tokio::test]
async fn dropped_stream_does_not_bind() {
    let (table, _now) = sticky_table();
    let egress = Arc::new(GatedSseEgress::new());
    let stage = ForwardStage::new(egress, Arc::new(FormatRegistry::new()))
        .with_sticky_table(Arc::clone(&table));

    let mut ctx = stream_ctx();
    let mut data = data_stream(expect_stream(&stage, &mut ctx).await);
    assert!(data.next().await.is_some_and(|c| !c.is_empty()));
    // The second frame is never released: the client disconnects now, dropping the
    // stream.
    drop(data);

    assert!(
        table.is_empty(),
        "client disconnect (stream dropped) does not write table"
    );
}

// ---------- case 8: table not mounted ⇒ behaviour matches the status quo ----------

/// Without `with_sticky_table` ⇒ the same cache-hit response passes back as usual and
/// the write side is skipped entirely.
///
/// Which bug turns this red: the write side not short-circuiting on the `Option`
/// (panicking when unmounted / falling back to a default table) ⇒ the rollback switch
/// (`sticky_enabled=false`) is no longer equivalent to the status quo.
#[tokio::test]
async fn unmounted_table_keeps_current_behavior() {
    let egress = Arc::new(JsonEgress {
        body: br#"{"usage":{"prompt_tokens":100,"completion_tokens":10,"prompt_tokens_details":{"cached_tokens":50}}}"#,
    });
    let stage = ForwardStage::new(egress, Arc::new(FormatRegistry::new()));

    let mut ctx = ctx_with_body(NON_STREAM_BODY, candidate("c1"));
    assert!(matches!(
        run_single(&stage, &mut ctx).await,
        StageOutcome::Continue
    ));

    let upstream = ctx.upstream.expect("success path must write ctx.upstream");
    assert_eq!(upstream.status, 200);
    assert!(
        serde_json::from_slice::<serde_json::Value>(&upstream.body).is_ok(),
        "upstream response body should pass back unchanged (write-side bypass does not change settlement/return semantics)"
    );
}

// ---------- case 9: successful stream + measured cache hit ⇒ table write +
// read-side round trip ----------

/// Streaming 2xx read to the end with a measured prefix-cache hit in frames (OpenAI
/// shape) ⇒ the "read-to-end arm" settlement commit point writes the table, and the
/// read side (same table, same-source key) finds the winning channel on the original
/// ctx — a write/read round trip isomorphic to non-streaming case 1, with the commit
/// point moved to the streaming arm.
///
/// Which bug turns this red:
/// - streaming-arm commit point missing / mistakenly on the stream-break arm ⇒ the
///   stream finishes normally but the table stays empty (this case proves a successful
///   stream really triggers the write, rather than "not yet");
/// - write-side key source drift (wrong fingerprint / using the adapted body) ⇒ the
///   read side misses the table lookup by raw body;
/// - wrong bound channel ⇒ the channel key does not match `ch-c1`.
#[tokio::test]
async fn successful_stream_with_cache_hit_binds_channel() {
    let (table, _now) = sticky_table();
    let egress = Arc::new(SseFramesEgress {
        frames: &[CACHED_USAGE_FRAME, DONE_FRAME],
    });
    let stage = ForwardStage::new(egress, Arc::new(FormatRegistry::new()))
        .with_sticky_table(Arc::clone(&table));

    let mut ctx = stream_ctx();
    let chunks: Vec<Bytes> = data_stream(expect_stream(&stage, &mut ctx).await)
        .collect()
        .await;
    assert_eq!(
        chunks,
        [
            Bytes::from_static(CACHED_USAGE_FRAME),
            Bytes::from_static(DONE_FRAME)
        ],
        "client should receive usage frames + [DONE] terminal frame frame by frame (stream read to end gives commit point a chance to execute)"
    );

    // Write/read round-trip cross-check: exactly one binding, its channel key is the
    // winning channel, and the read side finds it by the same-source key.
    assert_eq!(
        table.len(),
        1,
        "successful stream cache hit should write exactly one binding"
    );
    assert_eq!(
        table
            .lookup(&stream_fingerprint_key(), table.now_ms())
            .as_deref(),
        Some(CH_C1),
        "binding entry's channel key should be winning channel ch-c1"
    );
    assert_eq!(
        reader(&table).sticky_channel_for(&ctx).as_deref(),
        Some(CH_C1),
        "binding written by streaming arm must be found by read side (same table, same source key)"
    );
}

// ---------- case 10: streaming Claude two-frame shape ⇒ cached merged across
// frames is also written ----------

/// Streaming Claude two-frame shape (`message_start` carrying `message.usage`'s
/// `input_tokens` + `cache_read_input_tokens`, `message_delta` carrying
/// `usage.output_tokens`) ⇒ the "read-to-end arm" commit point writes the table with
/// counts merged across frames, and the read side finds the same channel on the
/// original ctx.
///
/// Which bug turns this red:
/// - cross-frame merge replaced by whole-frame replacement (`message_delta` wipes the
///   first frame's `input_tokens` to 0) or the `cache_read_input_tokens` key not
///   recognized ⇒ merged `cached == 0`, empty table;
/// - write-side key source drift / wrong bound channel ⇒ read-side miss or mismatched
///   channel key.
#[tokio::test]
async fn claude_streamed_cache_hit_binds_channel() {
    let (table, _now) = sticky_table();
    let egress = Arc::new(SseFramesEgress {
        frames: &[CLAUDE_MESSAGE_START_FRAME, CLAUDE_MESSAGE_DELTA_FRAME],
    });
    let stage = ForwardStage::new(egress, Arc::new(FormatRegistry::new()))
        .with_sticky_table(Arc::clone(&table));

    let mut ctx = stream_ctx();
    let chunks: Vec<Bytes> = data_stream(expect_stream(&stage, &mut ctx).await)
        .collect()
        .await;
    assert_eq!(
        chunks,
        [
            Bytes::from_static(CLAUDE_MESSAGE_START_FRAME),
            Bytes::from_static(CLAUDE_MESSAGE_DELTA_FRAME)
        ],
        "client should receive Claude's two frames frame by frame (stream read to end gives commit point a chance to execute)"
    );

    assert_eq!(
        table.len(),
        1,
        "successful stream cache hit should write exactly one binding"
    );
    assert_eq!(
        table
            .lookup(&stream_fingerprint_key(), table.now_ms())
            .as_deref(),
        Some(CH_C1),
        "binding entry's channel key should be winning channel ch-c1"
    );
    assert_eq!(
        reader(&table).sticky_channel_for(&ctx).as_deref(),
        Some(CH_C1),
        "binding written by Claude two-frame shape must be found by read side (same table, same source key)"
    );
}

// ---------- case 11: streaming cold request (cached == 0) ⇒ no write ----------

/// A successful stream read to the end but no cache signal in frames (usage has
/// prompt/completion, no cached field ⇒ measured `cached == 0`) does not write the
/// table — isomorphic to non-streaming case 4, guarding the streaming arm's
/// `cached > 0` gate.
///
/// Which bug turns this red: `bind_sticky`'s early return on `counts.cached == 0` is
/// removed ⇒ cold streams also bind channels, sticking users to channels that produced
/// no cache benefit.
#[tokio::test]
async fn successful_stream_without_cache_hit_does_not_bind() {
    let (table, _now) = sticky_table();
    let egress = Arc::new(SseFramesEgress {
        frames: &[COLD_USAGE_FRAME],
    });
    let stage = ForwardStage::new(egress, Arc::new(FormatRegistry::new()))
        .with_sticky_table(Arc::clone(&table));

    let mut ctx = stream_ctx();
    let chunks: Vec<Bytes> = data_stream(expect_stream(&stage, &mut ctx).await)
        .collect()
        .await;
    assert_eq!(
        chunks,
        [Bytes::from_static(COLD_USAGE_FRAME)],
        "cold stream itself should be read to end normally (not writing table is `cached == 0` semantics, not a failure path)"
    );

    assert!(
        table.is_empty(),
        "successful stream but cold request with measured cached == 0 does not write table (avoid pinning a channel that produced no cache benefit)"
    );
}

// ---------- cases 12-14: read-side wiring (production entry) ----------

/// Read-side wiring mock: folds the "sticky prior boost" into a **deterministic
/// choice** per the real selector contract — the sticky channel is chosen directly
/// when it is in the pool and not excluded, otherwise the first candidate in the pool
/// is returned — and records the sticky key each `select` round receives, unchanged.
///
/// The mock only drives **wiring** assertions (whether the sticky key really reaches
/// `select`, whether the hit channel is chosen): the real selector's weighted sampling
/// and boost-factor semantics are covered by the dispatch crate's tests.
/// The selection order deliberately puts c2 before c1, so both "not wired" and
/// "wired but no hit" show up as choosing c2, distinguishable with one assertion.
struct StickyAwareDispatch {
    candidates: Vec<Candidate>,
    sticky_keys: Mutex<Vec<Option<String>>>,
}

impl StickyAwareDispatch {
    fn new(candidates: Vec<Candidate>) -> Self {
        Self {
            candidates,
            sticky_keys: Mutex::new(Vec::new()),
        }
    }

    /// Sticky key received by each `select` round (`None` = not sticky).
    fn sticky_keys(&self) -> Vec<Option<String>> {
        self.sticky_keys.lock().clone()
    }
}

impl Dispatch for StickyAwareDispatch {
    fn select(
        &self,
        group: &str,
        model: &str,
        exclude: &[String],
        sticky_channel_key: Option<&str>,
    ) -> Result<Candidate, DispatchError> {
        self.sticky_keys
            .lock()
            .push(sticky_channel_key.map(str::to_string));
        let pool: Vec<&Candidate> = self
            .candidates
            .iter()
            .filter(|c| !exclude.contains(&c.unit.meta.key))
            .collect();
        // Stickiness only raises the prior; it does not exempt exclude: a sticky
        // channel that has been tried is already out of the pool.
        sticky_channel_key
            .and_then(|key| pool.iter().find(|c| c.unit.channel_key == key))
            .or_else(|| pool.first())
            .map(|c| (*c).clone())
            .ok_or_else(|| DispatchError::AllUnavailable {
                group: group.to_string(),
                model: model.to_string(),
                retry_after_ms: 0,
            })
    }

    fn report(&self, _unit_key: &str, _report: AttemptReport) {}

    fn report_tps(&self, _unit_key: &str, _sample: TpsSample) {}
}

/// Stage shaped like the production entry: `with_sticky_table` (mounts the same table
/// as the write side; the read-side lookup depends on it) and `with_retry_handle` (the
/// injection method of the bin/api assembly layer), after which `Stage::handle` itself
/// drives `handle_with_retry` → `run_retry_loop`.
///
/// Deliberately does not go through `DispatchStage`: the production pipeline only pushes
/// ForwardStage, and the read side is in forward's retry loop (hard ticket constraint:
/// assertions must hit the production entry).
fn retry_stage(
    egress: Arc<JsonEgress>,
    dispatch: Arc<StickyAwareDispatch>,
    table: Option<Arc<StickyTable>>,
) -> ForwardStage {
    let stage = ForwardStage::new(egress, Arc::new(FormatRegistry::new()));
    let stage = match table {
        Some(table) => stage.with_sticky_table(table),
        None => stage,
    };
    let policy = Arc::new(ArcSwap::from_pointee(RetryPolicy {
        max_attempts: 3,
        total_budget_ms: 0,
    }));
    stage.with_retry_handle(dispatch, policy)
}

/// The production entry reads the binding written by the write side ⇒ the sticky key
/// enters `select` as a prior and the bound channel is chosen.
///
/// Which bug turns this red: `run_retry_loop`'s sticky argument hard-coded to `None`
/// (the read side never wired) ⇒ select receives `None` and the first candidate c2 in
/// the pool is chosen; key source not shared with the write side (using the adapted
/// body / another model name) ⇒ lookup misses and c2 is chosen as well.
#[tokio::test]
async fn production_retry_path_reads_sticky_table_into_select() {
    let (table, _now) = sticky_table();
    // Simulate a binding written by a previous request's write side: (public model m
    // × raw-body fingerprint) → ch-c1.
    table.bind(fingerprint_key(), CH_C1);

    let dispatch = Arc::new(StickyAwareDispatch::new(vec![
        candidate("c2"),
        candidate("c1"),
    ]));
    let egress = Arc::new(JsonEgress {
        body: br#"{"ok":true}"#,
    });
    let stage = retry_stage(egress, Arc::clone(&dispatch), Some(Arc::clone(&table)));

    // Prefill route with c2: retry mode ignores it, but on a mismatch it shows
    // immediately who got the attribution.
    let mut ctx = ctx_with_body(NON_STREAM_BODY, candidate("c2"));
    let outcome = stage
        .handle(&mut ctx)
        .await
        .expect("retry path request should succeed");
    assert!(matches!(outcome, StageOutcome::Continue));

    assert_eq!(
        dispatch.sticky_keys(),
        vec![Some(CH_C1.to_string())],
        "production entry (with_retry_handle → handle_with_retry → run_retry_loop) must pass through the sticky key obtained from the table lookup to select"
    );
    assert_eq!(
        ctx.selected_channel_key.as_deref(),
        Some(CH_C1),
        "bound channel should be chosen due to prior boost, not fall back to the first candidate c2 in the pool"
    );
}

/// Same-source session-hint key at the production entry: a binding under
/// `ctx.session_hint` hits as well.
///
/// Which bug turns this red: the read side builds the key from the body fingerprint
/// only (drops hint priority) ⇒ the `sid:` write key is not found ⇒ c2 is chosen.
#[tokio::test]
async fn production_retry_path_honors_session_hint_sticky_key() {
    let (table, _now) = sticky_table();
    let sid = StickyKey::from_request("m", Some("s-1"), Some(NON_STREAM_BODY)).unwrap();
    table.bind(sid, CH_C1);

    let dispatch = Arc::new(StickyAwareDispatch::new(vec![
        candidate("c2"),
        candidate("c1"),
    ]));
    let egress = Arc::new(JsonEgress {
        body: br#"{"ok":true}"#,
    });
    let stage = retry_stage(egress, Arc::clone(&dispatch), Some(Arc::clone(&table)));

    let mut ctx = ctx_with_body(NON_STREAM_BODY, candidate("c2"));
    ctx.session_hint = Some("s-1".into());
    stage
        .handle(&mut ctx)
        .await
        .expect("retry path request should succeed");

    assert_eq!(
        ctx.selected_channel_key.as_deref(),
        Some(CH_C1),
        "when session hint key shares source with write side, production entry should choose the bound channel"
    );
}

/// Table not mounted (production = `sticky_enabled=false`) ⇒ no panic, selection
/// matches the status quo.
///
/// Which bug turns this red: the read side not short-circuiting on `Option` (panicking
/// when unmounted / fabricating a default table that finds a channel) ⇒ the rollback
/// switch (`sticky_enabled=false`) is no longer equivalent to the status quo.
#[tokio::test]
async fn unmounted_sticky_table_keeps_retry_path_selection() {
    let dispatch = Arc::new(StickyAwareDispatch::new(vec![
        candidate("c2"),
        candidate("c1"),
    ]));
    let egress = Arc::new(JsonEgress {
        body: br#"{"ok":true}"#,
    });
    // Do not call with_sticky_table: same shape as a sticky_enabled=false production
    // assembly.
    let stage = retry_stage(egress, Arc::clone(&dispatch), None);

    let mut ctx = ctx_with_body(NON_STREAM_BODY, candidate("c2"));
    let outcome = stage
        .handle(&mut ctx)
        .await
        .expect("unmounted table must not panic; request should succeed as usual");
    assert!(matches!(outcome, StageOutcome::Continue));
    assert_eq!(
        ctx.selected_channel_key.as_deref(),
        Some("ch-c2"),
        "unmounted table ⇒ degraded to pure P2C: first candidate in pool selected; selection matches status quo"
    );
}
