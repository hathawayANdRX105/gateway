//! `stage_retry` — `ForwardStage` retry mode: `with_retry` mounting + `handle_with_retry` main loop.
//!
//! Calls `gateway_dispatch::retry::run_retry_loop` to drive "select candidate → single attempt →
//! health report → exclude tried → select again"; the attempt closure is pure IO (on success it
//! stores `Forwarded` in the slot; on failure it reports per arm by
//! `retryable`/`degraded`/`channel_scoped`), and once the loop ends the hit structure commits.

use arc_swap::ArcSwap;
use bytes::Bytes;
use gateway_core::error::NormalizedError;
use gateway_dispatch::retry::{AttemptOutcome, RealTimeControl, run_retry_loop};
use gateway_dispatch::{AttemptReport, Dispatch, DispatchError, FailureClass, RetryPolicy};
use gateway_pipeline::{RequestCtx, StageError, StageOutcome};
use std::sync::{Arc, Mutex};

use crate::egress::upstream_error;
use crate::stage::{AttemptError, ForwardStage};
use crate::stage_concurrency::try_acquire_concurrency;
use crate::stage_telemetry::{millis_saturating, normalized_to_stage_error};

impl ForwardStage {
    /// Enable the failover retry loop: handle no longer reads `ctx.route` (it can coexist with
    /// `DispatchStage` but does not depend on it) and instead drives
    /// `gateway_dispatch::retry::run_retry_loop` itself — after each attempt it reports health to
    /// `dispatch`, and a retryable failure switches candidates for another round.
    #[must_use]
    pub fn with_retry(mut self, dispatch: Arc<dyn Dispatch>, policy: RetryPolicy) -> Self {
        self.dispatch = Some(dispatch);
        self.retry_policy = Arc::new(ArcSwap::from_pointee(policy));
        self
    }

    /// Same as [`Self::with_retry`], but injects the hot-reload handle the caller already holds
    /// (the bin/api assembly layer hands the same `ArcSwap` to both the options polling
    /// side and this stage so the two share one budget; use `with_retry` when sharing is not
    /// needed).
    #[must_use]
    pub fn with_retry_handle(
        mut self,
        dispatch: Arc<dyn Dispatch>,
        policy: Arc<ArcSwap<RetryPolicy>>,
    ) -> Self {
        self.dispatch = Some(dispatch);
        self.retry_policy = policy;
        self
    }

    /// Hot-swap the retry budget: storing a new value is enough; the retry loop reads
    /// it the next time it is entered.
    pub fn update_retry_policy(&self, policy: RetryPolicy) {
        self.retry_policy.store(Arc::new(policy));
    }

    /// Clone the hot-reload handle (the assembly layer hands the same handle to the options polling
    /// side, see [`Self::with_retry_handle`]).
    #[must_use]
    pub fn retry_policy_handle(&self) -> Arc<ArcSwap<RetryPolicy>> {
        Arc::clone(&self.retry_policy)
    }

    /// Retry-mode main loop: calls `gateway_dispatch::retry::run_retry_loop` to drive
    /// "select candidate → single attempt → health report → exclude tried → select again".
    ///
    /// Before the loop, [`ForwardStage::sticky_channel_for`] looks up this stage's mounted sticky
    /// table once (read side, design §3.2), and the resulting channel key is passed to every
    /// `select` round as a prior boost; table not mounted ⇒ `None`, and routing is byte-identical
    /// to no stickiness.
    /// The attempt closure is pure IO: on success it stores `Forwarded` in the result slot (taken
    /// out and committed by [`Self::finish_retry`] after the loop), on failure it stashes the
    /// `NormalizedError` in the error slot and returns Retryable/Fatal based on `retryable`. The
    /// report closure forwards the same result to `Dispatch::report`, driving the dispatch-side
    /// health state machine.
    pub(crate) async fn handle_with_retry(
        &self,
        ctx: &mut RequestCtx,
        dispatch: Arc<dyn Dispatch>,
        body: Bytes,
        stream: bool,
    ) -> Result<StageOutcome, StageError> {
        let RetryInputs {
            started,
            group,
            model,
            path,
            inbound_format,
            sticky_channel_key,
            cache_key,
            min_context,
        } = self.retry_inputs(ctx, &body, stream)?;

        // The winning attempt's Forwarded and the most recent failure; short critical section
        // with a std Mutex (the lock is not held across await).
        let slots = Arc::new(AttemptSlots {
            result: Mutex::new(None),
            error: Mutex::new(None),
        });
        // The attempt closure borrows ctx by shared reference for the whole loop
        // (attribution reads only: `submit_failed` no longer touches the hold — the
        // pipeline releases a still-outstanding hold exactly once at the terminal
        // outcome), so `commit_forwarded` can keep using `&mut ctx` afterwards.
        let ctx_ref: &RequestCtx = ctx;

        let dsel = Arc::clone(&dispatch);
        let drep = Arc::clone(&dispatch);
        // The attempt closure moves `body`; clone one copy for the commit after the loop
        // (settlement needs it).
        let commit_body = body.clone();

        // Read the retry budget's current value once (hot-reload handle): the budget is
        // fixed for one request; a new value takes effect on the next request (loop semantics in
        // gateway_dispatch::retry::run_retry_loop).
        let retry_policy = self.retry_policy.load();

        let runner = Arc::new(AttemptRunner {
            stage: self,
            ctx: ctx_ref,
            path,
            body,
            stream,
            inbound_format,
            slots: Arc::clone(&slots),
            started,
        });

        let loop_result = run_retry_loop(
            &group,
            &model,
            retry_policy.as_ref(),
            // Sticky channel key (`None` = not sticky): passed verbatim to select's prior layer
            // by the loop every round.
            sticky_channel_key.as_deref(),
            // Initial context-window floor (prompt estimate + output budget): raised by
            // the loop when an attempt overflows the selected channel's window.
            min_context,
            move |g, m, exclude, sticky, min_context| {
                dsel.select(g, m, exclude, sticky, min_context)
            },
            move |candidate| {
                let task = runner.build_task(candidate);
                let runner = Arc::clone(&runner);
                async move { runner.run(task).await }
            },
            move |key, report: AttemptReport| drep.report(key, report),
            &RealTimeControl,
        )
        .await;

        self.finish_retry(
            ctx,
            dispatch.as_ref(),
            RetryCommit {
                stream,
                body: &commit_body,
                cache_key,
            },
            &slots,
            loop_result,
        )
        .await
    }

    /// Request-level inputs for the retry loop (timing origin / group / model / path / inbound
    /// protocol / sticky key).
    ///
    /// Computed once per request: not recomputed inside the loop, and the sticky key does not
    /// change per attempt. The failure-observation timing origin is taken here — `duration_ms` =
    /// wall-clock ms from "entering retry → that failure" (including earlier attempts and routing
    /// time); the health report's `latency_ms` uses the attempt-local timer (`attempt_started`) and
    /// does not share this origin.
    fn retry_inputs(
        &self,
        ctx: &RequestCtx,
        body: &Bytes,
        stream: bool,
    ) -> Result<RetryInputs, StageError> {
        // Timing origin: accumulation starts as soon as the retry loop is entered (before routing
        // inputs and the sticky lookup).
        let started = std::time::Instant::now();
        let group = ctx
            .token
            .as_ref()
            .map(|t| t.group.clone())
            .unwrap_or_default();
        // The model name is lifted to ctx by gate::model; without it no candidate can be selected,
        // which is an assembly error.
        let Some(model) = ctx.requested_model.clone() else {
            return Err(StageError::Internal(anyhow::anyhow!(
                "model not resolved before forward"
            )));
        };
        // Read-side stickiness (design §3.2): routing reads the table that the write-side
        // settlement point writes, closing the loop — the sticky channel key is **computed once per
        // request** (not in the loop body, not recomputed per attempt); table not mounted / no key
        // source / no hit ⇒ None, degrading to non-sticky (behavior byte-identical to before).
        let sticky_channel_key = self.sticky_channel_for(ctx, body);
        // Response-cache key, computed once per request (streaming never caches).
        // A hit short-circuits in `handle` before the loop; a miss stores its
        // response at the winning attempt's commit point.
        let cache_key = if stream {
            None
        } else {
            self.response_cache_key(ctx, body)
        };
        // Context fit (data-driven via channel settings.context_window): required
        // tokens = prompt estimate (O(n) body walk on the client body, which predates
        // the protocol bridge) + the requested output budget. A spilled body skips
        // the estimate — reactive escalation still covers it.
        let min_context = match &ctx.request.body {
            gateway_pipeline::BodySource::InMemory(bytes) => {
                let prompt = gateway_metering::estimate::estimate_prompt_tokens(bytes);
                Some(
                    prompt
                        .tokens
                        .saturating_add(u64::from(ctx.requested_max_tokens.unwrap_or(4096))),
                )
            }
            gateway_pipeline::BodySource::OnDisk { .. } => None,
        };
        Ok(RetryInputs {
            started,
            group,
            model,
            // The inbound protocol is taken out before the closure: once the closure moves `path`,
            // ctx can no longer be borrowed.
            path: ctx.request.path.clone(),
            inbound_format: ctx.request.inbound_protocol,
            sticky_channel_key,
            cache_key,
            min_context,
        })
    }

    /// Run one attempt: concurrency gate → token pre-charge → forward → stream window → result
    /// classification.
    async fn run_attempt(
        &self,
        task: &crate::ForwardTask,
        ctx: &RequestCtx,
        started: std::time::Instant,
    ) -> AttemptResult {
        // This attempt is timed independently: the latency_ms reported to dispatch health (the P2C
        // latency factor) on success/failure counts only **this** attempt's duration, not earlier
        // failed attempts or routing — otherwise during failover (e.g. a 30s timeout followed by a
        // 1s success) the earlier slowness would accumulate and the genuinely faster channel would
        // look slower and slower in the EWMA. submit_failed still uses the loop-level started (see
        // above).
        let attempt_started = std::time::Instant::now();

        // Concurrency gate (v2 mounting): try_acquire before forward_task; the permit stays in
        // this function's scope and is dropped when the attempt ends (Ok or Err), releasing the
        // slot — it does **not** go into ctx.drop_guards (the attempt ends then, not the whole
        // response). Slot full: a concurrency rejection is not an upstream error, so no
        // submit_failed; health follows the Retryable arm with degraded semantics (same as P1-C:
        // drives candidate switching, the health table records no real upstream failure); the
        // error is stashed for a 429 passthrough when the budget is exhausted.
        let _permit = match try_acquire_concurrency(self.concurrency.as_ref()) {
            Ok(permit) => permit,
            Err(e) => {
                let outcome = if e.retryable {
                    AttemptOutcome::Retryable(FailureClass::Retryable, None)
                } else {
                    // Theoretically unreachable arm (semaphore Closed): terminate as Fatal.
                    AttemptOutcome::Fatal(FailureClass::Fatal, None)
                };
                return AttemptResult::failed(outcome, e);
            }
        };

        // Per-channel concurrency gate: occupies one slot of this attempt's channel. The permit is
        // attached to the winning attempt's `Forwarded` (RAII release at the end of the stream
        // state / non-streaming commit); a failed attempt returns it when this function ends;
        // unregistered channels have no concurrency limit.
        let channel_permit = match self.try_channel_permit(&task.candidate.unit.channel_key) {
            Ok(permit) => permit,
            Err(e) => {
                return AttemptResult::failed(
                    AttemptOutcome::Retryable(FailureClass::Retryable, None),
                    e,
                );
            }
        };

        // Channel token budget pre-charge (v3 mounting, at the same point as the per-channel
        // concurrency gate): not mounted = the whole section is skipped (behavior = current).
        // Window full (`admit_window` rejects) = token full ≠ channel broken: a 429 retryable
        // rejection shaped like the full-slot case — **no `submit_failed`** (not an upstream error,
        // no status_code to report); health goes through the existing 429/Throttled channel (the
        // 429 ladder applies a light weight reduction, no Fatal, no channel failure cooldown,
        // design §3.6); the error is stashed for diagnostics when the budget is exhausted. The
        // handle is attached to the winning attempt's `Forwarded` (same point as channel_permit);
        // a failed attempt returns everything by drop when this function ends.
        let token_handle = match self.preclaim_token_budget(task) {
            Ok(handle) => handle,
            Err(e) => {
                let outcome = AttemptOutcome::Throttled {
                    latency_ms: millis_saturating(attempt_started.elapsed()),
                    retry_after_ms: None,
                };
                return AttemptResult::failed(outcome, e);
            }
        };

        let permits = AttemptPermits {
            channel: channel_permit,
            token: token_handle,
        };
        // This attempt uses the **normal** egress timeout (not clamped to the failover time budget
        // — S2: a successful connection is not cut by the budget; timeout semantics in
        // `forward_task`'s three-stage timeout).
        match self.forward_task(task, &self.timeouts).await {
            Ok(forwarded) => {
                self.success_result(task, ctx, started, attempt_started, forwarded, permits)
                    .await
            }
            Err(AttemptError { error: e, degraded }) => {
                self.failure_result(task, ctx, started, attempt_started, e, degraded)
            }
        }
    }

    /// Finish a successful attempt: stream window decision → attach permits → assemble the
    /// result.
    async fn success_result(
        &self,
        task: &crate::ForwardTask,
        ctx: &RequestCtx,
        started: std::time::Instant,
        attempt_started: std::time::Instant,
        mut forwarded: crate::Forwarded,
        permits: AttemptPermits,
    ) -> AttemptResult {
        let status = forwarded.status;
        // P2 streaming resilience (Window phase, #158): for streaming requests, after response
        // headers arrive the upstream bytes are not sent out immediately; within the
        // `cfg.window_ms` window they are only buffered — the client still has zero bytes at this
        // point, so a broken stream can be retried on the whole response against another candidate
        // without the client noticing.
        if let Some(reason) = self.apply_stream_window(task.stream, &mut forwarded).await {
            // A broken stream inside the window = this upstream response was abandoned halfway:
            // retry the whole response on another candidate. A non-HTTP error (no status code to
            // pass through), classified as retryable 502, stashed for diagnostics when the budget
            // is exhausted. The permits were not moved into the discarded Forwarded and are
            // returned when this function returns.
            let error = upstream_error(502, true, reason);
            self.submit_failed(ctx, &task.candidate, &error, task.stream, started.elapsed());
            // This attempt is timed independently (including the window wait): a broken stream is
            // real evidence about the channel's current state; report it to the health table to
            // drive candidate switching.
            let outcome = AttemptOutcome::Retryable(
                FailureClass::Retryable,
                Some(millis_saturating(attempt_started.elapsed())),
            );
            return AttemptResult::failed(outcome, error);
        }
        // This attempt is timed independently (see attempt_started), excluding earlier failed
        // attempts.
        let latency_ms = millis_saturating(attempt_started.elapsed());
        // The channel permit travels with Forwarded into commit_forwarded: streaming attaches it to
        // the unfold stream state, non-streaming releases it after collecting the full body — P2C's
        // available_permits therefore reflects real concurrent responses. The token pre-charge
        // handle is at the same point as the permit: streaming with the unfold stream state,
        // non-streaming released by RAII in the commit scope (the settlement commit point calls
        // settle_actual first to refund the difference).
        forwarded.channel_permit = permits.channel;
        forwarded.token_handle = permits.token;
        AttemptResult {
            outcome: AttemptOutcome::Done { status, latency_ms },
            forwarded: Some(forwarded),
            error: None,
        }
    }

    /// Per-channel concurrency gate acquire: unregistered channel → `Ok(None)`; slot full → 429
    /// retryable.
    fn try_channel_permit(
        &self,
        channel_key: &str,
    ) -> Result<Option<tokio::sync::OwnedSemaphorePermit>, NormalizedError> {
        let Some(state) = self.channel_concurrency.as_ref() else {
            return Ok(None);
        };
        match state.try_acquire(channel_key) {
            Some(Some(permit)) => Ok(Some(permit)),
            Some(None) => Err(crate::rate_limited("channel concurrency limit reached")),
            None => Ok(None),
        }
    }

    /// P2 streaming-resilience window phase: when streaming and a window config is mounted, wait
    /// for the window before sending the first byte.
    ///
    /// Returns `Some(reason)` = stream broke inside the window (no completion criterion / stream
    /// error), and this attempt should switch candidates wholesale; `None` = no switch needed (the
    /// window collected the whole response, or the window expired and it switched to Live
    /// passthrough).
    ///
    /// Three outcomes (the clock-holding half of `drain_stream_window`):
    /// - EOF inside the window with a completion criterion ([DONE]/`finish_reason`) → the buffer is
    ///   drained as the whole response (the client gets everything at once, zero perception);
    /// - EOF inside the window without a criterion / stream error → discard this attempt and switch
    ///   candidates;
    /// - window expired (upstream still sending) → buffer and remaining stream are stitched into a
    ///   new body and switched to the Live phase passthrough (byte order identical to unbuffered
    ///   pass-through).
    async fn apply_stream_window(
        &self,
        stream: bool,
        forwarded: &mut crate::Forwarded,
    ) -> Option<String> {
        use crate::stream_resilience::WindowDrain;
        if !stream {
            return None;
        }
        let cfg = self.stream_resilience?;
        let drain = crate::stream_resilience::drain_stream_window(
            &mut forwarded.body,
            std::time::Duration::from_millis(cfg.window_ms),
        )
        .await;
        match drain {
            // The complete stream was collected inside the window: the buffer is the whole response
            // body, nothing left to wait for, go straight to Live.
            WindowDrain::Eof {
                complete: true,
                buf,
            } => {
                let bytes = Bytes::from(buf);
                forwarded.body = Box::pin(futures_util::stream::once(async move {
                    Ok::<Bytes, std::io::Error>(bytes)
                }));
                None
            }
            // Broken inside the window without a completion criterion = this upstream response was
            // abandoned halfway: retry the whole response on another candidate.
            WindowDrain::Eof {
                complete: false,
                buf,
            } => {
                tracing::debug!(
                    buffered = buf.len(),
                    "stream window: EOF without completion marker; switching candidate"
                );
                Some(
                    "upstream stream ended inside retry window without completion marker"
                        .to_string(),
                )
            }
            // A stream error inside the window (transport error) has the same semantics as "EOF
            // without criterion".
            WindowDrain::Broken { err, buf } => {
                tracing::debug!(
                    buffered = buf.len(),
                    %err,
                    "stream window: upstream stream error; switching candidate"
                );
                Some(format!("upstream stream error inside retry window: {err}"))
            }
            // Window expired: the buffer goes out first and the remaining stream follows; the
            // client cannot tell it was buffered.
            WindowDrain::Live { buf } => {
                let inner =
                    std::mem::replace(&mut forwarded.body, Box::pin(futures_util::stream::empty()));
                forwarded.body = Box::pin(crate::stream_resilience::BufferedStream {
                    pending: (!buf.is_empty()).then(|| Bytes::from(buf)),
                    inner,
                    terminated: false,
                });
                None
            }
        }
    }

    /// Failed-attempt classification: 429 → Throttled; retryable/degraded → Retryable;
    /// channel-scoped 4xx → `FatalButSwitchable`; everything else → Fatal.
    fn failure_result(
        &self,
        task: &crate::ForwardTask,
        ctx: &RequestCtx,
        started: std::time::Instant,
        attempt_started: std::time::Instant,
        e: NormalizedError,
        degraded: bool,
    ) -> AttemptResult {
        let retryable = e.retryable;
        let switchable = e.channel_scoped;
        let upstream_429 = e.status == 429 && retryable;
        let retry_after_hint = e.retry_after_ms;
        // Context-window overflow (400/413 + provider phrasing): the request does not
        // fit this channel's window. The channel is not broken (health stays Neutral
        // via Ok(400)) — the retry loop answers by raising the dispatch floor, so the
        // next selection lands on a strictly larger window.
        let window_overflow =
            gateway_dispatch::failure_scope::is_context_overflow(e.status, &e.message);
        // Each failed attempt emits one zero-cost observation event: when the budget is exhausted /
        // Fatal terminates, the troubleshooting side still needs a trace that the request happened.
        // Must be called before `e` is moved into the return struct.
        self.submit_failed(ctx, &task.candidate, &e, task.stream, started.elapsed());
        // A failure slow enough to time out is real evidence about the channel's current state
        // (this attempt is timed independently, excluding earlier attempts): dropping it would let
        // P2C learn the channel got slower only after a success.
        let latency_ms = millis_saturating(attempt_started.elapsed());
        let outcome = if upstream_429 {
            // Upstream actively rate limited (RATE_LIMITED): the channel itself is not broken —
            // health is reported with the 429 status code + Retry-After hint (drives the separate
            // 429 cooldown ladder + hint override), and the loop switches candidates (rate limiting
            // often has capacity on another candidate immediately). Switching/budget semantics are
            // the same as Retryable.
            AttemptOutcome::Throttled {
                latency_ms,
                retry_after_ms: retry_after_hint,
            }
        } else if window_overflow {
            // Handled before the retryable/switchable arms: an overflow must not be
            // classified Retryable (same-window retries are pointless) nor channel-scoped
            // Fatal (another channel with a larger window can still serve the request).
            AttemptOutcome::WindowOverflow
        } else if retryable || degraded {
            // P1-C dual-ledger bridge: degraded = all proxy nodes bound to the channel are in node
            // cooldown and this attempt actually failed on the direct-connection fallback. Health
            // records Retryable against the route unit (drives the failure streak; after N
            // consecutive failures the unit enters cooldown and select stops repeatedly choosing a
            // channel whose "proxies are all down"), and switching is forced.
            AttemptOutcome::Retryable(FailureClass::Retryable, Some(latency_ms))
        } else if switchable {
            // Channel-scoped 4xx (P1-B demotion): the health payload goes through Fatal —
            // health::classify(Err(Fatal)) lands on Neutral, no score change and no streak.
            AttemptOutcome::FatalButSwitchable(FailureClass::Fatal, Some(latency_ms))
        } else {
            AttemptOutcome::Fatal(FailureClass::Fatal, Some(latency_ms))
        };
        AttemptResult::failed(outcome, e)
    }

    /// Finish after the retry loop ends: write attribution back + commit the winning response /
    /// pass through the failure.
    async fn finish_retry(
        &self,
        ctx: &mut RequestCtx,
        dispatch: &dyn Dispatch,
        commit: RetryCommit<'_>,
        slots: &AttemptSlots,
        loop_result: Result<(gateway_dispatch::Attempt, AttemptOutcome), DispatchError>,
    ) -> Result<StageOutcome, StageError> {
        let (attempt, outcome) = match loop_result {
            Ok(pair) => pair,
            // Mapping for budget exhausted / no candidate / snapshot not ready / all rate-limited
            // (E1, distinguished from DispatchStage's single-shot mapping: here RetriesExhausted →
            // 502, RateLimited → 429).
            Err(e) => {
                tracing::error!(error = ?e, "request failed after dispatch attempts");
                return Err(dispatch_error_to_stage_error(e, slots));
            }
        };

        // Attribution write-back: retry may have switched candidates, but ctx still carries
        // DispatchStage's initial pick; Pipeline::run packages RouteAttribution from it, and
        // without write-back usage would be recorded against the failed initial channel. The name
        // is looked up against the snapshot; when not found, degrade to key-only (same discipline
        // as DispatchStage).
        let channel_key = attempt.candidate.unit.channel_key.clone();
        let channel_name = dispatch.channel_name(&channel_key);
        if channel_name.is_none() {
            tracing::debug!(
                channel_key = %channel_key,
                "channel name not found in dispatch snapshot; retry attribution carries key only"
            );
        }
        ctx.route = Some(attempt.candidate.clone());
        ctx.selected_channel_key = Some(channel_key);
        ctx.selected_channel_name = channel_name;

        let forwarded = lock(&slots.result).take();
        match (forwarded, outcome) {
            (Some(f), _) => {
                self.commit_forwarded(
                    ctx,
                    f,
                    &attempt.candidate,
                    commit.stream,
                    commit.body,
                    commit.cache_key.as_ref(),
                )
                .await
            }
            // Fatal (request-related problem) and FatalButSwitchable (the last candidate, no
            // channel left to switch to — the retry loop brings it back as a terminal state) both
            // pass through the stashed NormalizedError's upstream status code; there is no
            // successful response to commit.
            (
                None,
                AttemptOutcome::Fatal(_, _)
                | AttemptOutcome::FatalButSwitchable(_, _)
                | AttemptOutcome::WindowOverflow,
            ) => {
                let e = lock(&slots.error).take().ok_or_else(|| {
                    StageError::Internal(anyhow::anyhow!("forward produced no result"))
                })?;
                Err(normalized_to_stage_error(e))
            }
            (None, _) => Err(StageError::Internal(anyhow::anyhow!(
                "retry loop finished without a forwarded response"
            ))),
        }
    }
}

/// Product of one attempt: outcome + optional winning response + optional diagnostic error.
///
/// The two Options are written back to [`AttemptSlots`] by the attempt closure: the winning
/// response is committed after the loop, the error is passed through as diagnostics on budget
/// exhaustion / Fatal (both may be None, see the branches).
struct AttemptResult {
    outcome: AttemptOutcome,
    forwarded: Option<crate::Forwarded>,
    error: Option<NormalizedError>,
}

impl AttemptResult {
    /// Failed attempt with no winning response: outcome + diagnostic error.
    fn failed(outcome: AttemptOutcome, error: NormalizedError) -> Self {
        Self {
            outcome,
            forwarded: None,
            error: Some(error),
        }
    }
}

/// The two result slots shared by the attempt closure and the finishing step (short critical
/// section with a std Mutex, not held across await).
struct AttemptSlots {
    /// The winning attempt's response, for [`ForwardStage::finish_retry`] to commit.
    result: Mutex<Option<crate::Forwarded>>,
    /// The most recent failure, for diagnostics passthrough on budget exhaustion / Fatal.
    error: Mutex<Option<NormalizedError>>,
}

/// Closure environment of a single attempt: request-level inputs + result slots, reused for each
/// call inside the loop.
struct AttemptRunner<'a> {
    stage: &'a ForwardStage,
    ctx: &'a RequestCtx,
    path: String,
    body: Bytes,
    stream: bool,
    inbound_format: gateway_pipeline::ProtocolKind,
    slots: Arc<AttemptSlots>,
    started: std::time::Instant,
}

impl AttemptRunner<'_> {
    /// Assemble the `ForwardTask` for this attempt.
    ///
    /// Called in the closure's synchronous section: the returned future only owns the task and does
    /// not borrow `candidate`, satisfying `run_retry_loop`'s lifetime bound on
    /// `FnMut(&Candidate) -> Fut`.
    fn build_task(&self, candidate: &gateway_dispatch::Candidate) -> crate::ForwardTask {
        ForwardStage::build_task(
            candidate,
            self.path.clone(),
            self.body.clone(),
            self.stream,
            self.inbound_format,
        )
    }

    async fn run(&self, task: crate::ForwardTask) -> AttemptOutcome {
        // `AttemptRunner` is created per-request and used within a single async task, and
        // `ctx` is a shared borrow for the runner's whole lifetime with no concurrent
        // access — no reborrow or cast is needed.
        let result = self.stage.run_attempt(&task, self.ctx, self.started).await;
        // The two slots are only read after the loop ends (the loop awaits on a single thread), so
        // write order does not matter.
        if let Some(forwarded) = result.forwarded {
            *lock(&self.slots.result) = Some(forwarded);
        }
        if let Some(error) = result.error {
            *lock(&self.slots.error) = Some(error);
        }
        result.outcome
    }
}

/// The two kinds of permits this attempt acquired: per-channel concurrency permit + token
/// pre-charge handle.
///
/// Attached to the response's `Forwarded` on success; a failed attempt returns everything by scope
/// drop.
struct AttemptPermits {
    channel: Option<tokio::sync::OwnedSemaphorePermit>,
    token: Option<gateway_dispatch::token_budget::TokenHandle>,
}

/// Request-level inputs of the retry loop: computed once per request, reused inside the loop.
struct RetryInputs {
    /// Loop-level timing origin for failure observation.
    started: std::time::Instant,
    /// Effective group (for routing).
    group: String,
    /// Public model name (for routing).
    model: String,
    /// Client request path (reused by each attempt's `ForwardTask`).
    path: String,
    /// Inbound protocol (decode on the request direction, encode on the response direction).
    inbound_format: gateway_pipeline::ProtocolKind,
    /// Read-side sticky key (None = not sticky).
    sticky_channel_key: Option<String>,
    /// Response-cache key (None = streaming / not cacheable / cache not mounted);
    /// consumed by the winning attempt's commit point.
    cache_key: Option<gateway_cache::CacheKey>,
    /// Initial context-window floor: prompt estimate + requested output budget.
    /// `None` = the body was spilled to disk and could not be estimated (reactive
    /// escalation still applies; only the pre-dispatch fit filter is skipped).
    min_context: Option<u64>,
}

/// Commit inputs for the winning attempt (`finish_retry` →
/// `commit_forwarded`): streaming intent, the materialized request body
/// (settlement basis), and the response-cache key (`None` = streaming / not
/// cacheable / cache not mounted).
struct RetryCommit<'a> {
    stream: bool,
    body: &'a Bytes,
    cache_key: Option<gateway_cache::CacheKey>,
}

/// Retry-loop error → `StageError` (E1 mapping: distinguished from `DispatchStage`'s single-shot
/// mapping).
fn dispatch_error_to_stage_error(e: DispatchError, slots: &AttemptSlots) -> StageError {
    match e {
        DispatchError::RateLimited { .. } => StageError::RateLimited,
        DispatchError::ModelUnknown { group, model } => StageError::NoRoute { group, model },
        DispatchError::AllUnavailable {
            group,
            model,
            retry_after_ms,
        }
        | DispatchError::BudgetExhausted {
            group,
            model,
            retry_after_ms,
        } => {
            // All cooling/disabled or S2 failover budget exhausted → 503 without cooldown / 504 +
            // Retry-After with cooldown. Carries the last upstream diagnostic (error slot) for
            // troubleshooting; semantics unchanged.
            if let Some(last) = lock(&slots.error).take() {
                tracing::warn!(
                    group,
                    model,
                    last_status = last.status,
                    retry_after_ms,
                    "failover ended without a usable candidate; client should retry after {retry_after_ms}ms"
                );
            }
            StageError::Unavailable {
                group,
                model,
                retry_after_ms,
            }
        }
        DispatchError::SnapshotNotReady => StageError::NotReady,
    }
}

/// Take the Mutex value; poisoning can only come from a panic propagation path, and recovering the
/// inner value is enough (both slots are plain assignments, so no logically inconsistent state
/// exists).
fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}
