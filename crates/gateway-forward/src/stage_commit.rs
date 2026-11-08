//! Response commit: non-streaming collects the full body into `ctx.upstream`;
//! streaming flows straight back through the SseScanner + StreamScanner
//! scan-chain unfold orchestration.
//!
//! All settle / tps / sticky hook invocations live inside
//! [`ForwardStage::commit_forwarded`] (unfold state machine for streaming,
//! sequential calls at the commit point for non-streaming).

use crate::egress::ByteStream;
use crate::stage::ForwardStage;
use crate::stage_sticky::{StickyBind, bind_sticky};
use crate::stage_telemetry::{
    StreamTps, counts_or_estimate, report_stream_tps, settlement_identity, submit_event,
};
use crate::stream::{self, pipe_chunk};
use bytes::Bytes;
use futures_util::{StreamExt, TryStreamExt};
use gateway_metering::ledger::{Hold, Ledger};
use gateway_pipeline::{
    PipeStream, RequestCtx, SelectedRoute, StageError, StageOutcome, UpstreamResponse,
};
use gateway_protocol_bridge::sse::SseEvent;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

impl ForwardStage {
    /// Commits the upstream response of one successful attempt.
    ///
    /// Non-streaming → collect the whole body into `ctx.upstream`; streaming →
    /// unfold the SseScanner + StreamScanner scan chain into
    /// [`StageOutcome::Stream`] and stream directly back. `candidate` is the winning
    /// candidate (the streaming context needs its channel_key/upstream_model).
    ///
    /// When `with_price_table` has mounted a price table, both paths produce the
    /// UsageEvent for the sink inside this function (the single commit point):
    /// streaming settles via [`stream::finish`] when the stream ends or fails
    /// midway; non-streaming goes through `settle_non_stream` once the full body
    /// is collected.
    ///
    /// `cache_key` is the request's response-cache key (`None` = not cacheable /
    /// cache not mounted): on the non-streaming path it stores the settled
    /// response. Streaming never caches and ignores it.
    pub(crate) async fn commit_forwarded(
        &self,
        ctx: &mut RequestCtx,
        forwarded: crate::Forwarded,
        candidate: &SelectedRoute,
        stream: bool,
        req_body: &Bytes,
        cache_key: Option<&gateway_cache::CacheKey>,
    ) -> Result<StageOutcome, StageError> {
        if stream {
            return Ok(self.commit_stream(ctx, forwarded, candidate, req_body));
        }
        self.commit_non_stream(ctx, forwarded, candidate, req_body, cache_key)
            .await
    }

    /// Streaming commit: the scan-chain unfold streams straight back, and
    /// settlement happens when the stream ends.
    fn commit_stream(
        &self,
        ctx: &mut RequestCtx,
        forwarded: crate::Forwarded,
        candidate: &SelectedRoute,
        req_body: &Bytes,
    ) -> StageOutcome {
        // Pass the upstream content-type back to the client unchanged: SSE
        // clients use it to decide they must read an event stream.
        let content_type = forwarded.content_type.clone();
        let sink = self.sink.clone();
        let hold = ctx.hold.take();
        let ledger = self.ledger.clone();
        // The scan context is carried in an Option: on stream completion or a
        // mid-stream error, take() yields it and settles exactly once, preventing
        // re-entry and double billing after an error item.
        let idle_dur = Duration::from_millis(self.timeouts.stream_idle_ms);
        let max_deadline =
            tokio::time::Instant::now() + Duration::from_millis(self.timeouts.stream_max_ms);
        let state = StreamCommitState {
            upstream: forwarded.body,
            sse: Some(self.sse_context(ctx, candidate)),
            // The channel slot permit and token reservation handle travel with
            // the unfold stream state: released via RAII when the stream ends
            // (completed / upstream disconnected / dropped after client
            // disconnect) — P2C's available_permits / inflight therefore reflect
            // the real number of concurrent streams, not just established ones.
            _guards: (
                forwarded.channel_permit,
                forwarded.token_handle,
                hold.map(|h| HoldGuard::new(ledger, h)),
            ),
            // Stream deadlines: `idle` is re-armed per chunk, `max` is set once
            // (both derived from the stage-wide `timeouts`, like every other
            // attempt of this request).
            idle: Box::pin(tokio::time::sleep(idle_dur)),
            max: Box::pin(tokio::time::sleep_until(max_deadline)),
            idle_dur,
            settled: false,
            // Late-arriving generation throughput signal (design §3.2): an
            // observation endpoint that moves with the unfold stream state.
            // Dispatcher handle and winning-candidate unit key (the anchor for
            // report_tps, same `meta.key` basis as the health side; dispatch is
            // None in single-shot mode ⇒ the whole block reports nothing).
            tps: StreamTps {
                dispatch: self.dispatch.clone(),
                unit_key: candidate.unit.meta.key.clone(),
                first_token_at: None,
                usage_seen: false,
            },
        };
        // Effective group (backfilled into settlement events; same basis as
        // settle_non_stream).
        let group_id = candidate.unit.group.clone();
        // Sticky-write binding inputs (table + key + target channel) are computed
        // **before** entering the response stream: streaming settlement happens
        // inside the unfold, where `ctx` is no longer available (`BoxStream` is
        // `'static`). Table not mounted / no key source ⇒ None, write side skipped.
        let sticky_bind = self.sticky_bind(ctx, candidate, req_body);
        let mapped = futures_util::stream::unfold(state, move |state| {
            let sink = sink.clone();
            let group_id = group_id.clone();
            let sticky_bind = sticky_bind.clone();
            next_stream_item(state, sink, group_id, sticky_bind)
        });
        let stream: futures_util::stream::BoxStream<'static, Result<Bytes, std::io::Error>> =
            Box::pin(mapped);
        StageOutcome::Stream(PipeStream::with_content_type(
            axum::body::Body::from_stream(stream),
            content_type,
        ))
    }

    /// Streaming settlement context: attribution keys come from the user/token
    /// metadata written by AuthGate; the price table is mounted with the stage.
    fn sse_context(&self, ctx: &RequestCtx, candidate: &SelectedRoute) -> stream::SseContext {
        // Missing attribution falls back to empty strings (same as the old
        // behavior): events are still produced, just without an owner key.
        let (user_key, token_key, group) = match settlement_identity(ctx) {
            Some(identity) => identity,
            None => {
                tracing::warn!("streaming settlement missing attribution, using empty identity");
                Default::default()
            }
        };
        let mut sse_ctx = stream::SseContext::new();
        sse_ctx.user_key = user_key;
        sse_ctx.token_key = token_key;
        sse_ctx.channel_key = candidate.unit.channel_key.clone();
        sse_ctx.public_model = candidate.unit.public_model.clone();
        sse_ctx.upstream_model = candidate.upstream_model.clone();
        sse_ctx.group = group;
        // Group ratio is injected by apps; the library layer defaults to 1.0
        sse_ctx.group_ratio = 1.0;
        // When the price table is not mounted (None), finish does not settle;
        // behavior matches the pre-billing era.
        sse_ctx.price_table = self.price_table.clone();
        sse_ctx
    }

    /// Non-streaming commit: collect the full body → refund the token
    /// difference → write sticky binding → settle → write `ctx.upstream`.
    async fn commit_non_stream(
        &self,
        ctx: &mut RequestCtx,
        forwarded: crate::Forwarded,
        candidate: &SelectedRoute,
        req_body: &Bytes,
        cache_key: Option<&gateway_cache::CacheKey>,
    ) -> Result<StageOutcome, StageError> {
        // The channel permit (if any) and token reservation handle (if any) live
        // with the `forwarded` struct: after the full body is collected they are
        // released via RAII when this function's scope ends, not returned early
        // inside commit.
        // Read the whole body (non-streaming response bodies are usually small).
        let mut buf = Vec::new();
        let mut body_stream = forwarded.body;
        while let Some(chunk) = body_stream
            .try_next()
            .await
            .map_err(|e| StageError::Internal(anyhow::anyhow!("read upstream body: {e}")))?
        {
            buf.extend_from_slice(&chunk);
        }
        // Upstream usage extraction (measured counts): the token refund and the
        // sticky-binding write share this one parse — the write side only looks at
        // `cached` (only a real prefix-cache hit this time makes the channel
        // eligible for binding).
        let usage = gateway_metering::extract_usage(&buf);
        // Non-streaming token reservation settlement commit point (co-located with
        // the metering settlement point below): when billing is mounted (pt+sink as
        // a pair, the same settlement gate as settle_non_stream), refund with the
        // extracted usage values (same estimate fallback when usage is absent) and
        // calibrate the EMA; when not mounted, do not settle and the handle Drop
        // refunds the full estimate (design §3.5: settle missing ⇒ Drop fallback,
        // no path leaks).
        if self.price_table.is_some()
            && self.sink.is_some()
            && let Some(handle) = forwarded.token_handle.as_ref()
        {
            let counts = counts_or_estimate(usage, req_body, &buf);
            handle.settle_actual(counts.prompt + counts.completion, counts.completion);
        }
        // Sticky-binding write at the non-streaming settlement commit point
        // (design §3.3, side path, the single billing write point is unchanged):
        // the commit point's input is always 2xx (`forward_once`'s status guard
        // rejects non-2xx earlier, so they never reach here — an existing decision
        // point), so bind only when the measured `cached > 0`; no usage ⇒ fallback
        // count `cached = 0`, treated as a miss, no write.
        bind_sticky(self.sticky_bind(ctx, candidate, req_body).as_ref(), usage);
        // Non-streaming settlement point (before writing ctx.upstream; skipped
        // entirely when pt is not mounted).
        self.settle_non_stream(ctx, req_body, &buf, forwarded.status, candidate);
        // The body is materialized once as `Bytes`: the response-cache store
        // clones it by reference count (no payload copy), and `ctx.upstream`
        // takes the original.
        let resp_body = Bytes::from(buf);
        // Response-cache store at the settlement commit point (opt-in): the
        // settled counts + the winning attempt's attribution travel with the
        // body, so a replay is byte-identical and bills identically.
        if let Some(key) = cache_key {
            let counts = counts_or_estimate(usage, req_body, &resp_body);
            self.store_response_cache(key, &resp_body, forwarded.status, counts, candidate);
        }
        ctx.upstream = Some(UpstreamResponse {
            status: forwarded.status,
            body: resp_body,
        });
        Ok(StageOutcome::Continue)
    }
}

/// Mutable part carried by the streaming commit inside the unfold state.
/// RAII guard for a prehold traveling with a streaming response.
///
/// - Explicit `settle(actual)` reconciles the reservation with the metered actual
///   cost (refund the difference, or charge the extra on a negative delta) and
///   consumes the hold, so `Drop` becomes a no-op — settle and Drop never
///   double-account.
/// - If the stream is dropped before settlement (client disconnect), `Drop`
///   releases the full preheld amount, mirroring `TokenHandle`'s contract so no
///   prehold leaks.
struct HoldGuard {
    ledger: Option<Arc<dyn Ledger>>,
    hold: Option<Hold>,
}

impl HoldGuard {
    fn new(ledger: Option<Arc<dyn Ledger>>, hold: Hold) -> Self {
        Self {
            ledger,
            hold: Some(hold),
        }
    }

    /// Reconcile the hold with the metered actual cost: refund the difference or
    /// charge the extra on a negative delta. Consumes the hold so `Drop` no longer
    /// releases it.
    fn settle(&mut self, actual: i64) {
        let Some(hold) = self.hold.take() else {
            return;
        };
        if let Some(ledger) = &self.ledger {
            ledger.settle(&hold, actual);
        }
    }
}

impl Drop for HoldGuard {
    fn drop(&mut self) {
        if let Some(hold) = self.hold.take()
            && let Some(ledger) = &self.ledger
        {
            ledger.release(&hold);
        }
    }
}

struct StreamCommitState {
    /// Upstream response stream.
    upstream: ByteStream,
    /// Settlement scan context; taken and set to None after settlement (bytes
    /// keep passing through unchanged).
    sse: Option<stream::SseContext>,
    /// RAII guards whose lifetime follows the stream: (per-channel concurrency
    /// permit, token reservation handle).
    _guards: (
        Option<tokio::sync::OwnedSemaphorePermit>,
        Option<gateway_dispatch::token_budget::TokenHandle>,
        Option<HoldGuard>,
    ),
    /// Throughput observation endpoint.
    tps: StreamTps,
    /// Per-chunk idle sleep: re-armed to `now + idle_dur` on every chunk; fires
    /// when the upstream stays silent for `idle_dur`.
    idle: Pin<Box<tokio::time::Sleep>>,
    /// Max-duration sleep: set once at construction to `now + stream_max_ms` and
    /// never re-armed; fires when the stream outlives the hard ceiling.
    max: Pin<Box<tokio::time::Sleep>>,
    /// Idle duration re-armed on every chunk.
    idle_dur: Duration,
    /// Set by any terminal arm (EOF / upstream error / idle / max) after
    /// `finish_stream` ran. `Drop` reports a `debug!` when the state is dropped
    /// without ever settling (client disconnect or shutdown): `debug`, not
    /// `info`, because graceful shutdown drops all in-flight streams and an
    /// info-level log would false-alarm in bulk.
    settled: bool,
}

/// One step of the streaming commit: consume one upstream chunk or finish.
///
/// Settlement happens exactly once, in one of the terminal arms: EOF -> 200,
/// upstream error -> 500, stream idle timeout -> 500, stream max duration ->
/// 500; `sse` is taken and set to None to prevent re-entry; afterwards bytes
/// keep passing through unchanged.
async fn next_stream_item(
    mut state: StreamCommitState,
    sink: Option<Arc<dyn gateway_metering::SettleSink>>,
    group_id: String,
    sticky_bind: Option<Arc<StickyBind>>,
) -> Option<(Result<Bytes, std::io::Error>, StreamCommitState)> {
    // Already settled: a previous terminal arm already ran `finish_stream` and
    // took `sse`; the unfold may still poll once more after we returned a
    // terminal item — end the stream instead of re-settling or re-firing an
    // already-expired sleep.
    if state.settled {
        return None;
    }
    // The select only observes: each arm yields an owned `Step` because the
    // sibling futures keep borrowing `state` while a handler runs, so all
    // settlement work happens in the match below instead.
    enum Step {
        Chunk(Bytes),
        Eof,
        UpstreamErr(std::io::Error),
        IdleTimeout,
        MaxTimeout,
    }
    let step = tokio::select! {
        biased;
        // Data: one upstream chunk (the two existing settlement arms, EOF ->
        // 200 / upstream error -> 500, keep their exact semantics below).
        item = state.upstream.next() => match item {
            Some(Ok(chunk)) => Step::Chunk(chunk),
            Some(Err(e)) => Step::UpstreamErr(e),
            None => Step::Eof,
        },
        // Idle: upstream silent for `idle_dur`.
        () = state.idle.as_mut() => Step::IdleTimeout,
        // Max: total stream duration exceeded.
        () = state.max.as_mut() => Step::MaxTimeout,
    };
    match step {
        Step::Chunk(chunk) => {
            let out = match state.sse.as_mut() {
                Some(sse) => {
                    let piped = pipe_chunk(sse, &chunk);
                    // First-token / usage frames are reported only once by
                    // SseScanner and folded into two `tps` booleans (the hot
                    // path neither allocates nor rescans bytes).
                    for event in &piped.events {
                        match event {
                            SseEvent::FirstToken => {
                                state
                                    .tps
                                    .first_token_at
                                    .get_or_insert_with(std::time::Instant::now);
                            }
                            SseEvent::Usage => state.tps.usage_seen = true,
                            _ => {}
                        }
                    }
                    piped.passthrough
                }
                // Already settled: discard the scan context; bytes keep passing
                // through unchanged
                None => chunk,
            };
            // Re-arm idle: the next chunk must arrive within idle_dur.
            let next = tokio::time::Instant::now() + state.idle_dur;
            state.idle.as_mut().reset(next);
            Some((Ok(out), state))
        }
        Step::UpstreamErr(e) => {
            // Upstream disconnect: settle with the accumulated counts
            // (status=500), otherwise a mid-stream failure would leak the token
            // bill already incurred; the error item is still returned.
            finish_stream(
                &mut state,
                &sink,
                &group_id,
                sticky_bind.as_ref(),
                500,
                Some(&e.to_string()),
            );
            state.settled = true;
            Some((Err(e), state))
        }
        Step::Eof => {
            // Stream completed = settlement commit point (status 200).
            finish_stream(
                &mut state,
                &sink,
                &group_id,
                sticky_bind.as_ref(),
                200,
                None,
            );
            state.settled = true;
            None
        }
        Step::IdleTimeout => settle_timeout(
            state,
            &sink,
            &group_id,
            sticky_bind.as_ref(),
            "stream idle timeout",
        ),
        Step::MaxTimeout => settle_timeout(
            state,
            &sink,
            &group_id,
            sticky_bind.as_ref(),
            "stream max duration exceeded",
        ),
    }
}

/// Shared tail of the two timeout arms: settle at 500 with `reason`, mark the
/// state settled, and end the client stream with a `TimedOut` error item.
/// `reason` feeds both the settle event and the io error so the two views
/// never drift.
fn settle_timeout(
    mut state: StreamCommitState,
    sink: &Option<Arc<dyn gateway_metering::SettleSink>>,
    group_id: &str,
    sticky_bind: Option<&Arc<StickyBind>>,
    reason: &'static str,
) -> Option<(Result<Bytes, std::io::Error>, StreamCommitState)> {
    finish_stream(&mut state, sink, group_id, sticky_bind, 500, Some(reason));
    state.settled = true;
    Some((
        Err(std::io::Error::new(std::io::ErrorKind::TimedOut, reason)),
        state,
    ))
}

/// Streaming settlement commit point (shared by the completed / disconnected arms).
///
/// Both arms produce a settlement event and refund the token reservation; only a
/// normal completion (`status_code == 200`) produces a throughput sample and
/// writes the sticky binding — a disconnect is negative evidence about the channel
/// and must not be a success sample.
fn finish_stream(
    state: &mut StreamCommitState,
    sink: &Option<Arc<dyn gateway_metering::SettleSink>>,
    group_id: &str,
    sticky_bind: Option<&Arc<StickyBind>>,
    status_code: u16,
    error: Option<&str>,
) {
    let Some(sse) = state.sse.take() else {
        return;
    };
    let priced = sse.price_table.is_some();
    let (_end, counts, event) = stream::finish(sse, status_code, error);
    let cost = event.as_ref().map(|ev| ev.cost);
    submit_event(sink, event, group_id);
    // Token reservation settlement commit point: reconcile the prehold with the
    // metered actual cost (refund the difference or charge the extra on a negative
    // delta). Settle consumes the hold, so Drop becomes a no-op; when the stream
    // is dropped before settlement, Drop refunds the full estimate.
    if priced && let Some(guard) = state._guards.2.as_mut() {
        // priced only holds when a price table is mounted, in which case finish
        // produces an event with a cost; fall back to 0 (full refund) defensively.
        guard.settle(cost.unwrap_or(0));
    }
    if status_code == 200 {
        // Normal completion = the only settlement point that produces a throughput
        // sample. The disconnect arm and client drop (the whole unfold is dropped)
        // report nothing, design §3.2.
        report_stream_tps(&state.tps, counts, std::time::Instant::now());
        // Sticky binding write (design §3.3, side path): bind only when "this
        // attempt succeeded and actually hit the prefix cache" — not on the
        // disconnect arm (500 settlement), not when the client disconnects (the
        // stream is dropped, neither arm runs), and the whole block is skipped when
        // no table is mounted.
        bind_sticky(sticky_bind, Some(counts));
    }
}

impl Drop for StreamCommitState {
    fn drop(&mut self) {
        if !self.settled {
            tracing::debug!("stream dropped before settlement (client disconnect or shutdown)");
        }
    }
}
