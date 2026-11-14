//! `stage_telemetry` — metering/settlement telemetry for `ForwardStage`.
//!
//! Success path: [`ForwardStage::settle_non_stream`] emits a settlement event at
//! the non-streaming commit point (streaming settles via `stream::finish` when the
//! stream ends or fails midway); failure path: [`ForwardStage::submit_failed`]
//! emits a **zero-cost observation event** for a failed attempt that had a
//! candidate. Both paths only take effect once `with_price_table` is mounted; when
//! pt/sink are not mounted or attribution is missing they are skipped silently —
//! never emitting an ownerless bill and never changing the caller's control flow,
//! which still returns `StageError` as usual.

use bytes::Bytes;
use gateway_core::error::NormalizedError;
use gateway_dispatch::{Dispatch, TpsSample};
use gateway_metering::ledger::Hold;
use gateway_pipeline::{RequestCtx, SelectedRoute, StageError};
use std::sync::Arc;
use std::time::Duration;

use crate::stage::ForwardStage;

impl ForwardStage {
    /// Non-streaming settlement — extract usage from the full response body (falling
    /// back to estimation when absent), price it, and hand the `UsageEvent` to the
    /// sink.
    ///
    /// Returns immediately when the price table or sink is not mounted
    /// (`with_price_table` not called), preserving the existing no-billing behavior.
    /// Estimation basis: prompt uses the request body's structure-aware estimate
    /// (`gateway_metering::estimate::estimate_prompt_tokens`) and completion is
    /// roughly response bytes / 4 (the same order as `StreamScanner`'s no-usage
    /// fallback).
    pub(crate) fn settle_non_stream(
        &self,
        ctx: &mut RequestCtx,
        req_body: &Bytes,
        resp_body: &[u8],
        status: u16,
        candidate: &SelectedRoute,
    ) {
        let usage = gateway_metering::extract_usage(resp_body);
        if let Some(usage) = usage {
            let estimated = gateway_metering::estimate::estimate_prompt_tokens(req_body);
            record_calibration(
                gateway_metering::calibration::global().as_ref(),
                estimated,
                usage.prompt,
            );
        }
        let (Some(pt), Some(sink)) = (self.price_table.as_ref(), self.sink.as_ref()) else {
            return;
        };
        let counts = counts_or_estimate(usage, req_body, resp_body);
        // Take the prehold out of ctx before the attribution check: QuotaGate reserved the
        // estimated cost at admission, so a missing-attribution exit must still release the
        // reservation instead of letting it stick until the next reseed.
        let hold = ctx.hold.take();
        // Attribution keys come from ctx.token, written by the gate chain; missing
        // attribution means we cannot bill — release the hold and skip, never emit an
        // ownerless bill.
        let Some((user_key, token_key, group)) = settlement_identity(ctx) else {
            if let Some(h) = hold
                && let Some(ledger) = &self.ledger
            {
                ledger.release(&h);
            }
            return;
        };
        // Group ratio is injected by apps; the library layer defaults to 1.0
        let group_ratio = 1.0;
        let fallback = Hold {
            id: 0,
            amount: 0,
            user_key: user_key.clone(),
            token_key: token_key.clone(),
        };
        let hold_ref = hold.as_ref().unwrap_or(&fallback);
        let mut event = gateway_metering::settle_event(
            counts,
            // Only called at the non-streaming commit point (streaming settles via
            // stream::finish).
            false,
            &group,
            group_ratio,
            hold_ref,
            pt.as_ref(),
            &candidate.unit.channel_key,
            &candidate.unit.meta.key,
            &candidate.unit.public_model,
            &candidate.upstream_model,
            0,
            0,
            status,
            None,
            // Non-streaming commit point: extract the finish reason from the full
            // response body (lenient parse; missing / invalid JSON = None, never
            // invented).
            gateway_metering::extract_finish_reason(resp_body).as_deref(),
        );
        // Backfill the effective group: the winning candidate unit's group (the
        // routing-semantic authority). Settlement only happens on paths that already
        // have a candidate (commit_forwarded passes the winner here, and
        // submit_failed has a candidate per failed attempt), so there is no "failed
        // and never selected a unit" case and no fallback to ctx.token.group.
        event.group_id.clone_from(&candidate.unit.group);
        // Reconcile the prehold with the metered actual: refund the difference or
        // charge the extra on a negative delta. No hold means no ledger mounted,
        // hence nothing to settle.
        if let Some(h) = &hold
            && let Some(ledger) = &self.ledger
        {
            ledger.settle(h, event.cost);
        }
        sink.submit(event);
    }

    /// Settlement for a response-cache replay: the recorded counts and the
    /// recorded channel attribution price and land exactly as the original
    /// call's settlement did — a replay is billed like the response it replays.
    ///
    /// Group authority: the recorded group, not a re-read of the requester's
    /// current membership — the cached answer was priced under that group, so
    /// the replay prices under it too. Token attribution (who pays) is the
    /// replaying request's identity, same as `settle_non_stream`. Skipped
    /// silently when pt/sink are not mounted or attribution is missing — never
    /// emitting an ownerless bill.
    pub(crate) fn settle_cache_hit(
        &self,
        ctx: &mut RequestCtx,
        entry: &gateway_cache::CachedResponse,
    ) {
        let (Some(pt), Some(sink)) = (self.price_table.as_ref(), self.sink.as_ref()) else {
            return;
        };
        // Take the prehold out of ctx exactly like `settle_non_stream`: the
        // replaying request took a prehold at admission, and the replay's
        // settlement is its settle-exactly-once point.
        let hold = ctx.hold.take();
        let Some((user_key, token_key, _group)) = settlement_identity(ctx) else {
            if let Some(h) = hold
                && let Some(ledger) = &self.ledger
            {
                ledger.release(&h);
            }
            return;
        };
        // No prehold (no ledger mounted at the gate) → zero fallback, same as
        // `settle_non_stream`.
        let fallback = Hold {
            id: 0,
            amount: 0,
            user_key: user_key.clone(),
            token_key: token_key.clone(),
        };
        let hold_ref = hold.as_ref().unwrap_or(&fallback);
        let counts = gateway_metering::scanner::TokenCounts {
            prompt: entry.usage.prompt_tokens,
            completion: entry.usage.completion_tokens,
            cached: entry.usage.cached_tokens,
            cache_creation: entry.usage.cache_creation_tokens,
            reasoning: entry.usage.reasoning_tokens,
            prompt_includes_cache: entry.usage.prompt_includes_cache,
        };
        let mut event = gateway_metering::settle_event(
            counts,
            // Only at the non-streaming hit path (streaming never hits the cache).
            false,
            &entry.group,
            // Library-layer default ratio, same as `settle_non_stream`.
            1.0,
            hold_ref,
            pt.as_ref(),
            &entry.channel_key,
            &entry.unit_key,
            &entry.public_model,
            &entry.upstream_model,
            0,
            0,
            entry.status,
            None,
            // The stored body is the full client-facing answer; the finish
            // reason extracts exactly as it did on the original call.
            gateway_metering::extract_finish_reason(&entry.body).as_deref(),
        );
        event.group_id.clone_from(&entry.group);
        // Reconcile the prehold with the metered actual, exactly like a live
        // call: the replay's recorded counts price to the same cost the original
        // call settled at.
        if let Some(h) = &hold
            && let Some(ledger) = &self.ledger
        {
            ledger.settle(h, event.cost);
        }
        sink.submit(event);
    }

    /// Failure observation — when upstream responded (4xx/5xx) or transport failed
    /// but a **candidate exists**, emit a **zero-cost observation event** to the
    /// sink, so `usage_logs` records "this request happened and which candidate
    /// rejected it".
    ///
    /// The prehold is NOT released here: the prehold is request-scoped admission
    /// state and must be settled exactly once (successful commit) or released exactly
    /// once (terminal failure). `Pipeline::run` releases a still-outstanding hold
    /// centrally on any terminal outcome, so a failed attempt never settles a hold
    /// that an earlier attempt already released — in retry mode the hold survives
    /// every failed attempt and is consumed by the winning attempt's settle.
    ///
    /// Pure observation, not billed: counts are all 0 → cost must be 0;
    /// status/error come from [`NormalizedError`]; `is_stream` records the request
    /// intent (same source as the success path). Attribution keys
    /// (user/token/group) match `settle_non_stream`, all from `ctx.token`. Skipped
    /// silently when pt/sink are not mounted or attribution is missing — never
    /// emitting an ownerless bill and never changing the caller's control flow,
    /// which still returns [`StageError`] as usual.
    pub(crate) fn submit_failed(
        &self,
        ctx: &RequestCtx,
        candidate: &SelectedRoute,
        err: &NormalizedError,
        stream: bool,
        elapsed: Duration,
    ) {
        let (Some(pt), Some(sink)) = (self.price_table.as_ref(), self.sink.as_ref()) else {
            return;
        };
        let Some((user_key, token_key, group)) = settlement_identity(ctx) else {
            // No attribution: nothing to observe, never emit an ownerless bill.
            return;
        };
        let fallback = Hold {
            id: 0,
            amount: 0,
            user_key: user_key.clone(),
            token_key: token_key.clone(),
        };
        let hold_ref = &fallback;
        // All-zero counts → price_of yields 0, so the observation event creates no
        // bill. Failure elapsed → duration_ms (u32 milliseconds, saturating).
        let duration_ms = millis_saturating(elapsed);
        let mut event = gateway_metering::settle_event(
            gateway_metering::scanner::TokenCounts::default(),
            stream,
            &group,
            // Group ratio is injected by apps; the library layer defaults to 1.0
            // (same as settle_non_stream).
            1.0,
            hold_ref,
            pt.as_ref(),
            &candidate.unit.channel_key,
            &candidate.unit.meta.key,
            &candidate.unit.public_model,
            &candidate.upstream_model,
            0, // first_token_ms: failure observations have no first-token timing
            duration_ms,
            err.status,
            Some(&err.message),
            // Failure observation row: this call site has no upstream response body
            // (NormalizedError only carries the status and a summary), so
            // finish_reason is structurally absent → None.
            None,
        );
        // Backfill the effective group: the failure observation row is attributed to
        // the **failed attempt's candidate** unit group (same as settle_non_stream;
        // an attempt always has a candidate, so there is no token-group fallback).
        event.group_id.clone_from(&candidate.unit.group);
        sink.submit(event);
    }
}

/// Settlement attribution keys (`user_key` / `token_key` / group), all from `ctx.token`.
///
/// A missing token (in theory the gates guarantee Some) → `None`: the caller skips
/// settlement / observation silently and never emits an ownerless bill.
pub(crate) fn settlement_identity(ctx: &RequestCtx) -> Option<(String, String, String)> {
    let token = ctx.token.as_ref()?;
    Some((
        token.user_key.clone(),
        token.id.clone(),
        token.group.clone(),
    ))
}

/// Upstream usage wins; when absent, estimate prompt from the request body and
/// completion as response bytes / 4 (the same order as `StreamScanner`'s no-usage
/// fallback).
pub(crate) fn counts_or_estimate(
    usage: Option<gateway_metering::scanner::TokenCounts>,
    req_body: &Bytes,
    resp_body: &[u8],
) -> gateway_metering::scanner::TokenCounts {
    usage.unwrap_or_else(|| {
        tracing::debug!("usage unavailable, falling back to token estimate");
        gateway_metering::scanner::TokenCounts {
            prompt: gateway_metering::estimate::estimate_prompt_tokens(req_body).tokens,
            completion: resp_body.len() as u64 / 4,
            cached: 0,
            cache_creation: 0,
            reasoning: 0,
            prompt_includes_cache: false,
        }
    })
}
/// Side-record an estimate-vs-reported prompt pair for calibration when the
/// upstream reported usage. A no-op when the log is not installed or usage is
/// absent; never disturbs the settlement flow it wraps.
pub(crate) fn record_calibration(
    log: Option<&Arc<gateway_metering::calibration::CalibrationLog>>,
    estimate: gateway_metering::estimate::PromptEstimate,
    reported_prompt: u64,
) {
    let Some(log) = log else {
        return;
    };
    // A reported prompt of 0 means the upstream gave no usage; `CalibrationLog::record`
    // ignores such samples (no calibration signal, avoids division by zero).
    log.record(estimate, reported_prompt);
}
/// Milliseconds duration → `u32`, saturating at the limit (about 49.7 days).
pub(crate) fn millis_saturating(elapsed: Duration) -> u32 {
    u32::try_from(elapsed.as_millis()).unwrap_or(u32::MAX)
}

/// Hand a settlement product to the sink (skipped silently when the sink is not
/// mounted or there is no event) and backfill the effective group.
///
/// `group_id` = the winning candidate unit's group (the routing-semantic authority,
/// same as `settle_non_stream`). Streaming settlement happens inside the unfold
/// closure (the event only exists after `stream::finish` returns), and
/// `commit_forwarded` captures the winning candidate's group to carry back with the
/// event.
pub(crate) fn submit_event(
    sink: Option<&Arc<dyn gateway_metering::SettleSink>>,
    event: Option<gateway_core::records::UsageEvent>,
    group_id: &str,
) {
    if let (Some(mut event), Some(sink)) = (event, sink) {
        event.group_id = group_id.to_string();
        sink.submit(event);
    }
}

/// `NormalizedError` → `StageError::Upstream` (passes through the upstream status code
/// and message preview).
pub(crate) fn normalized_to_stage_error(e: NormalizedError) -> StageError {
    StageError::Upstream(gateway_pipeline::UpstreamError::Status {
        code: e.status,
        body_preview: e.message,
    })
}

/// Minimum `completion_tokens` for a throughput sample — smaller samples are dropped
/// outright (design §3.2): ultra-short bursts such as `max_tokens=1` carry no
/// throughput information, while their denominator (the time of one generation) is
/// exactly the noisiest tier; feeding them into the EMA only skews the channel-level
/// mean.
const TPS_MIN_COMPLETION_TOKENS: u64 = 8;

/// Upper bound on tok/s for generated throughput samples: real single-stream
/// generation peaks around 2000–4000 tok/s, so this leaves ample headroom. Samples
/// over the bound are **measurement artifacts** (a sub-millisecond generation span
/// inflated by the 1ms floor), not real channel throughput — drop them rather than
/// clamping (see the filter comment in `report_stream_tps`).
const TPS_SAMPLE_MAX: f64 = 10_000.0;

/// In-stream generation-throughput observation endpoint — moves as a whole with the
/// unfold stream state (the dispatcher handle and unit key move once with the state
/// instead of cloning an `Arc` per chunk).
///
/// - `first_token_at`: arrival time of the **first data frame**
///   (`SseEvent::FirstToken`; `:` heartbeat lines do not count), the start of the
///   generation-span timing;
/// - `usage_seen`: whether any frame with `usage` appeared in this stream. When the
///   upstream supplies usage, metered `completion` is measured; otherwise it falls
///   back to the bytes/4 estimate — the two have different confidence, so the
///   health side uses different α buckets by source (design §3.2).
pub(crate) struct StreamTps {
    /// Landing point for `report_tps`; `None` = no dispatcher mounted (single-shot
    /// mode) ⇒ nothing is reported at all.
    pub(crate) dispatch: Option<Arc<dyn Dispatch>>,
    /// Routing unit key of the winning candidate (`RouteUnitRecord.meta.key`, same
    /// basis as the health table).
    pub(crate) unit_key: String,
    pub(crate) first_token_at: Option<std::time::Instant>,
    pub(crate) usage_seen: bool,
}

/// Late throughput-sample report when the stream finishes normally (the only
/// emission point, design §3.2).
///
/// Filter conditions (failing any one means no report ⇒ zero disturbance to
/// existing traffic):
/// - no dispatcher mounted (single-shot mode) or no unit key;
/// - no first-token timestamp (the stream broke before the upstream's first frame,
///   or was empty);
/// - `completion < TPS_MIN_COMPLETION_TOKENS` (ultra-short bursts carry no
///   information);
/// - the computed value is non-finite or non-positive (dirty data stays out of the
///   DB);
/// - the computed value `> TPS_SAMPLE_MAX` (physically implausible artifact, **drop
///   rather than clamp**).
///
/// The generation span is `finish_at - first_token_at` with a 1ms floor. The floor
/// only prevents division by zero — rounding a sub-millisecond span up to 1ms
/// inflates an N-token sample to N*1000 tok/s, so it must not produce physically
/// implausible values; the upper bound is responsible for those.
pub(crate) fn report_stream_tps(
    tps: &StreamTps,
    counts: gateway_metering::scanner::TokenCounts,
    finish_at: std::time::Instant,
) {
    let Some(dispatch) = tps.dispatch.as_deref() else {
        return;
    };
    let Some(first_token_at) = tps.first_token_at else {
        return;
    };
    if counts.completion < TPS_MIN_COMPLETION_TOKENS {
        return;
    }
    let seconds = finish_at
        .saturating_duration_since(first_token_at)
        .max(Duration::from_millis(1))
        .as_secs_f64();
    // Completion token counts are bounded far below f64's 52-bit mantissa, so
    // this conversion cannot lose meaningful precision for any realistic stream.
    #[allow(clippy::cast_precision_loss)]
    let value = counts.completion as f64 / seconds;
    if !value.is_finite() || value <= 0.0 {
        return;
    }
    // Drop over-limit values (do not clamp): after the 1ms floor inflates a
    // sub-millisecond generation span, a short 120-token stream computes to a
    // 120000 tok/s measurement artifact. The health side's first-sample branch
    // (`ema == 0.0`) takes the sample value directly without EMA smoothing, and the
    // selector's `pool_max_tps` takes the maximum in the pool — that artifact would
    // poison the whole pool: every other real channel's factor is pinned to the 0.2
    // floor by `clamp(max/artifact, 0.2, 1.0)` and needs more than ten samples of
    // α=0.2 decay to recover. An artifact carries no throughput information, so drop
    // it; clamping to 10_000 would instead inject a fake "extremely fast" signal into
    // the EMA.
    if value > TPS_SAMPLE_MAX {
        return;
    }
    // Bucket samples by source: upstream usage measured ⇒ Measured; metering used the
    // bytes/4 estimate fallback ⇒ Estimated (the health side halves α, isolating noise
    // outside the main EMA).
    let sample = if tps.usage_seen {
        TpsSample::Measured(value)
    } else {
        TpsSample::Estimated(value)
    };
    dispatch.report_tps(&tps.unit_key, sample);
}
