//! Failover retry orchestration — the state-machine data for one request's lifetime.
//!
//! References: one-api controller/relay.go (retries ignore the first priority),
//! new-api `transport/handler/handle_relay.go` (reset body / refund / health report before retry),
//! sub2api `backend/internal/handler/failover_loop.go` (`FailoverState`: `FailedAccountIDs`
//! exclusion set + `MaxSwitches` budget).
//!
//! Upper-layer loop shape:
//! ```text
//! failover = Failover::new(policy)
//! while let Some(attempt_no) = failover.next_attempt() {
//!   candidate = dispatch.select(group, model, failover.exclude(), sticky)
//!   on success: dispatch.report(ok); metering.settle(); return
//!   on FailureClass::Fatal: report; metering.settle(as_error); return 4xx
//!   on FailureClass::Retryable: report; failover.mark_tried(&candidate); continue
//!   on 429 (Throttled): report Ok(429)+Retry-After hint; mark_tried; continue
//! }
//! return 502   # budget exhausted
//! ```
//!
//! sticky = the sticky channel bound to this request (None = not sticky): passed through
//! verbatim to every select round, and **stickiness never bypasses exclude** — exclude only
//! grows while candidates are switched, so once a sticky channel has been tried it naturally
//! stops receiving the boost (design §4 invariant 1, "stickiness never hard-pins a channel").
//!
//! Key invariants:
//! - pre-charge happens once (no stacking across retries), settlement happens once;
//! - the request body must be **replayable** before a retry (forward's replayable body guarantee);
//! - every attempt's health report is mandatory (cross-candidate statistics);
//! - the exclusion set is keyed by unit key (unit.meta.key); a key is never selected twice
//!   (sub2api `FailedAccountIDs`).
//! - the sticky key is passed through every round and never exempts a channel from exclude
//!   (stickiness never hard-pins a channel, design §4 invariant 1).

use std::collections::HashMap;

use crate::candidate::Candidate;
use crate::health::{AttemptReport, FailureClass};
use async_trait::async_trait;

/// Retry policy parameters — defined on the injection surface so the admin plane can
/// read the budget from options and hot-swap it without depending on this engine.
pub use gateway_core::dispatch::RetryPolicy;

/// Time control for the retry loop — cooldown-recovery waits go through here. Production =
/// real `tokio` sleep; tests inject [`FakeTimeControl`] so a cooldown wait takes 0 ms of real
/// time and never blocks (virtual advance). The S2 failover time accumulator sleeps only for
/// cooldown waits; failed-attempt time comes from forward's `AttemptOutcome::latency_ms()`
/// (already measured), so this loop reads no extra clock.
#[async_trait]
pub trait TimeControl: Send + Sync {
    /// Cooldown-recovery wait (ms). With `ms == 0` neither implementation waits.
    async fn sleep_ms(&self, ms: u64);
}

/// Production implementation: real `tokio::time::sleep`.
#[derive(Debug, Default, Clone, Copy)]
pub struct RealTimeControl;

#[async_trait]
impl TimeControl for RealTimeControl {
    async fn sleep_ms(&self, ms: u64) {
        if ms > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
        }
    }
}

/// Test implementation: never actually sleeps; it adds the requested cooldown time to
/// [`FakeTimeControl::cooled_ms`] (so tests can assert how much was or was not waited) and
/// yields via `yield_now` — 0 ms of wall clock, no blocking in CI.
#[derive(Debug, Default)]
pub struct FakeTimeControl {
    /// Accumulated requested cooldown wait (ms).
    pub cooled_ms: std::sync::atomic::AtomicU64,
}

#[async_trait]
impl TimeControl for FakeTimeControl {
    async fn sleep_ms(&self, ms: u64) {
        self.cooled_ms
            .fetch_add(ms, std::sync::atomic::Ordering::SeqCst);
        tokio::task::yield_now().await;
    }
}

/// Result of a single attempt — the report from forward → retry.
#[derive(Debug)]
pub enum AttemptOutcome {
    /// Upstream 2xx/4xx-with-usage (billing is normal).
    Done {
        status: u16,
        /// Upstream round-trip time (ms). Successful samples are the only source of the P2C
        /// latency factor.
        latency_ms: u32,
    },
    /// Retryable failure.
    Retryable(FailureClass, Option<u32>),
    /// Non-retryable failure.
    Fatal(FailureClass, Option<u32>),
    /// Channel-scoped 4xx: this candidate failed but another channel might work. Health records
    /// Neutral (no EWMA change) and the loop switches candidates; only the last candidate's
    /// result is passed through.
    FatalButSwitchable(FailureClass, Option<u32>),
    /// Upstream 429 active rate limiting: the channel itself is not broken, so health is
    /// reported as `Ok(429)` (Throttled has its own cooldown ladder); `retry_after_ms` is the
    /// wait parsed from the upstream `Retry-After` header (`None` = absent/unparseable). Loop
    /// semantics match [`Self::Retryable`]: `mark_tried`, then continue with another candidate
    /// (same switching/budget accounting) — on a 429 a different candidate often has capacity
    /// immediately.
    Throttled {
        /// Upstream round-trip time of this attempt (ms).
        latency_ms: u32,
        /// Upstream `Retry-After` hint (ms); `None` = absent or unparseable.
        retry_after_ms: Option<u64>,
    },
    /// Upstream rejected the request because it does not fit the model's context
    /// window (context-window-exceeded family, detected by
    /// `failure_scope::is_context_overflow`). The retry loop answers by
    /// **raising the dispatch floor** to the failed candidate's window, so the
    /// next selection can only land on strictly larger windows; when the failed
    /// channel has no window bound (unlimited), the loop passes the stashed
    /// error through instead — escalation has no direction to grow in.
    WindowOverflow,
}

impl AttemptOutcome {
    /// Upstream latency observed by this attempt (`None` = not measured).
    ///
    /// Failures can carry latency too: a failure slow enough to time out is **real** evidence
    /// about the channel's current state; discarding it would leave P2C blind to the slowdown
    /// until after a success.
    #[must_use]
    pub fn latency_ms(&self) -> Option<u32> {
        match self {
            AttemptOutcome::Done { latency_ms, .. }
            | AttemptOutcome::Throttled { latency_ms, .. } => Some(*latency_ms),
            AttemptOutcome::Retryable(_, latency)
            | AttemptOutcome::Fatal(_, latency)
            | AttemptOutcome::FatalButSwitchable(_, latency) => *latency,
            AttemptOutcome::WindowOverflow => None,
        }
    }

    /// Upstream `Retry-After` hint (ms) — carried only by the 429 rate-limit case; always
    /// `None` on the other variants.
    #[must_use]
    pub fn retry_after_ms(&self) -> Option<u64> {
        match self {
            AttemptOutcome::Throttled { retry_after_ms, .. } => *retry_after_ms,
            _ => None,
        }
    }

    /// `Result<status code, failure class>` for the health table to consume.
    ///
    /// # Errors
    ///
    /// `Err(class)` for every failure outcome (`Retryable`, `Fatal`, `FatalButSwitchable`)
    /// carrying the attempt's classified failure class; successes (`Done`, `Throttled`) return
    /// `Ok` (the status code, or 429 for throttling).
    pub fn as_health_result(&self) -> Result<u16, FailureClass> {
        match self {
            AttemptOutcome::Done { status, .. } => Ok(*status),
            AttemptOutcome::Retryable(class, _)
            | AttemptOutcome::Fatal(class, _)
            | AttemptOutcome::FatalButSwitchable(class, _) => Err(*class),
            AttemptOutcome::Throttled { .. } => Ok(429),
            // Context overflow: the request does not fit the model — the channel is not
            // broken and the client did nothing wrong. Health stays untouched (Neutral
            // semantics): the fit failure says nothing about the channel's quality, and
            // escalation is driven by the retry loop's floor, not by health scoring.
            AttemptOutcome::WindowOverflow => Ok(400),
        }
    }
}

/// Default tier + exact per-model override tier — P3 per-model retry tiers: api-hub's
/// `[retry."<model>"]` configuration surface (default first, model merged over it; the default
/// is sanitized to max 3, not 100).
#[derive(Debug, Clone)]
pub struct ModelRetryPolicies {
    default_policy: RetryPolicy,
    per_model: HashMap<String, RetryPolicy>,
}

impl ModelRetryPolicies {
    /// Constructor.
    #[must_use]
    pub fn new(default_policy: RetryPolicy, per_model: HashMap<String, RetryPolicy>) -> Self {
        Self {
            default_policy,
            per_model,
        }
    }

    /// Exact `per_model` hit, otherwise default.
    #[must_use]
    pub fn policy_for(&self, model: &str) -> &RetryPolicy {
        self.per_model.get(model).unwrap_or(&self.default_policy)
    }
}

/// Orchestration trait — implemented by bin/api's forward handler (needs forward+metering
/// access).
pub trait RetryLoop: Send + Sync {
    fn run(
        &self,
        group: &str,
        model: &str,
        policy: &RetryPolicy,
    ) -> impl Future<Output = Result<AttemptOutcome, crate::DispatchError>> + Send;
}

/// Context of one attempt (the minimal information retry passes to forward).
///
/// The loop returns it with the outcome on success so the caller knows **which candidate won**
/// (streaming passthrough needs the winning candidate's `channel_key` / `upstream_model` to
/// assemble the SSE context).
#[derive(Debug)]
pub struct Attempt {
    pub candidate: Candidate,
    pub attempt_no: u32,
}

/// Failover state machine — tried-exclusion set + attempt budget (the equivalent of sub2api
/// `FailoverState`).
///
/// This type guarantees only "do not reselect tried candidates, do not exceed the budget";
/// same-channel backoff and other timing are controlled by the upper loop (sub2api
/// sameAccountRetryDelay/exponential backoff).
#[derive(Debug, Default, Clone)]
pub struct Failover {
    policy: RetryPolicy,
    // ponytail: Vec rather than HashSet — tried length is bounded (max_attempts-1, 4 by
    // default), a linear scan and hashing are the same order at n<=4, and Vec keeps order
    // without allocation jitter (ocr #12 rejected).
    /// Tried candidate keys (unit.meta.key), passed to `Dispatch::select` as exclude.
    tried: Vec<String>,
    /// Completed attempt count.
    attempts: u32,
    /// Minimum context window (tokens) the next selection must provide. Raised by
    /// `raise_floor` when an attempt fails with a context-window overflow; `None` =
    /// unconstrained (the request fit estimate was not computed, or nothing has
    /// overflowed yet).
    min_context: Option<u64>,
}

impl Failover {
    #[must_use]
    pub fn new(policy: RetryPolicy, min_context: Option<u64>) -> Self {
        Self {
            policy,
            tried: Vec::new(),
            attempts: 0,
            min_context,
        }
    }

    /// Current exclusion set (lent to `Dispatch::select`).
    #[must_use]
    pub fn exclude(&self) -> &[String] {
        &self.tried
    }

    /// Current context-window floor (lent to `Dispatch::select`; `None` =
    /// unconstrained).
    #[must_use]
    pub fn min_context(&self) -> Option<u64> {
        self.min_context
    }

    /// Escalate past a context-window overflow: the next selection may only land
    /// on channels with a **strictly larger** window than the one that just
    /// overflowed (the fit estimate undershot; going smaller cannot help). The
    /// floor never lowers — a request that already needed `n` tokens does not
    /// become smaller by retrying.
    pub fn raise_floor(&mut self, overflowed_window: u64) {
        let next = overflowed_window.saturating_add(1);
        self.min_context = Some(self.min_context.map_or(next, |current| current.max(next)));
    }

    /// Advance to the next attempt; attempt budget exhausted → None (the upper layer returns 502).
    pub fn next_attempt(&mut self) -> Option<u32> {
        if self.attempts >= self.policy.max_attempts {
            return None;
        }
        self.attempts += 1;
        Some(self.attempts)
    }

    /// Record a tried candidate (called after a failure; the exclusion set dedups). Takes an
    /// owned value to avoid another allocation (ocr #11: callers usually already hold a String).
    pub fn mark_tried(&mut self, key: impl Into<String>) {
        let key = key.into();
        if !self.tried.contains(&key) {
            self.tried.push(key);
        }
    }

    /// Clear the tried exclusion set — "connection-time tolerance" semantics: after waiting out
    /// a cooldown, give tried channels one chance to become selectable again (otherwise recovered
    /// candidates stay blocked by exclude and the wait is pointless). Called only from the
    /// recovery-wait path; capped by both `max_attempts` and the time budget, so it cannot spin
    /// forever.
    pub fn reset_tried(&mut self) {
        self.tried.clear();
    }

    /// Whether the attempt budget is exhausted (the time budget is tracked separately by
    /// `RetryBudget`).
    #[must_use]
    pub fn is_exhausted(&self) -> bool {
        self.attempts >= self.policy.max_attempts
    }
}

/// Full retry loop — the orchestration logic owned by dispatch.
///
/// User requirement: the retry mechanism lives in dispatch, not scattered through forward.
/// This function hard-codes the "select candidate → attempt → health report → exclude tried →
/// select again" loop in `gateway_dispatch::retry`; forward only supplies a single-attempt
/// closure (pure IO, no retry decisions).
///
/// Loop shape (sub2api `FailoverState` / new-api relay retry semantics):
/// ```text
/// while let Some(attempt_no) = failover.next_attempt() {
///   candidate = select(group, model, failover.exclude(), sticky)?   // exclude tried
///   outcome   = attempt(candidate).await                     // one forward
///   report(key, AttemptReport)                                // health report
///   match outcome {
///     Done { .. } | Fatal(_) => return Ok((attempt, outcome)) // carry the winner out
///     Retryable(_)           => mark_tried; continue          // switch candidate, retry
///     FatalButSwitchable(_)  => mark_tried; stash the result; continue
///       # select has no candidate left (NoCandidate) → bring the stashed result back and
///       # pass it through (the last candidate's channel-scoped 4xx reaches the client
///       # as-is, not disguised as 503)
///   }
/// }
/// Err(RetriesExhausted)
/// ```
///
/// Attempt-count budget (`max_attempts`) or time budget (`total_budget_ms`) exhausted without a
/// success → `DispatchError::BudgetExhausted` (upper layer: 503 without cooldown / 504 +
/// Retry-After with cooldown; the client resends later).
///
/// P1-C (dual health ledger bridge) note: node cooldown on the `proxy::manager` side is not
/// visible to this loop — when "all bound nodes cooling → direct-connection fallback fails",
/// forward's attempt closure classifies the result as `Retryable(Retryable)` directly (degraded
/// semantics = the existing Retryable arm: report(Err(Retryable)) drives the unit failure
/// streak + `mark_tried` + continue), so the loop body needs no separate degraded branch; once
/// the streak reaches the threshold the unit is ejected by `health`/`selector` and a channel
/// whose proxies are all down is no longer selected repeatedly.
// ponytail: every parameter is one independent input of the loop (the three routing inputs +
// three closures + the clock); packing them into a struct would only move the parameters
// around (same convention as gateway_metering::settle / control-plane).
//
/// # Errors
///
/// [`DispatchError::SnapshotNotReady`], [`DispatchError::ModelUnknown`],
/// [`DispatchError::AllUnavailable`] or [`DispatchError::RateLimited`] from
/// [`Dispatch::select`], and [`DispatchError::BudgetExhausted`] when the attempt or time
/// budget is exhausted without a success.
#[allow(clippy::too_many_arguments)]
pub async fn run_retry_loop<Sel, Attempt, Fut>(
    group: &str,
    model: &str,
    policy: &RetryPolicy,
    // Sticky key (`None` = not sticky): passed through verbatim as each select round's prior
    // boost; it takes no part in exclude accounting — once a sticky channel is mark_tried'd into
    // exclude, the selector naturally stops boosting it (stickiness never hard-pins a channel,
    // design §4).
    sticky_channel_key: Option<&str>,
    // Initial context-window floor for every selection round (the request's prompt
    // estimate + output budget); raised by the loop when an attempt overflows the
    // selected channel's window. `None` = unconstrained.
    min_context: Option<u64>,
    mut select: Sel,
    mut attempt: Attempt,
    mut report: impl FnMut(&str, AttemptReport),
    time: &dyn TimeControl,
) -> Result<(crate::Attempt, AttemptOutcome), crate::DispatchError>
where
    Sel: FnMut(
        &str,
        &str,
        &[String],
        Option<&str>,
        Option<u64>,
    ) -> Result<Candidate, crate::DispatchError>,
    // The attempt closure performs one forward only (pure IO): it uses the **normal** egress
    // timeout, not clamped to the remaining budget.
    // S2 semantics: a successful connection is not cut by the failover time budget, and its
    // duration does not count toward the budget (P1).
    Attempt: FnMut(&Candidate) -> Fut,
    Fut: Future<Output = AttemptOutcome>,
{
    let mut failover = Failover::new(*policy, min_context);
    // Most recent channel-scoped 4xx (FatalButSwitchable): preferred for passthrough when the
    // count/time budget is exhausted (the client gets the upstream's real 4xx rather than it
    // being masked by 503/504; P1-B passthrough).
    let mut last_switchable: Option<(crate::Attempt, AttemptOutcome)> = None;
    // S2 time budget: accumulates failed-attempt time + cooldown wait only; success does not
    // count (P1: success spends no budget). Accounted lazily — accumulated only at consumption
    // events, no per-attempt clock reads / egress clamps.
    let mut budget = RetryBudget::new(policy.total_budget_ms); // 0 = off (legacy behavior: max_attempts only)

    loop {
        // Each round passes the sticky key through verbatim; when everything is cooling and the
        // budget allows, sleep until the shortest recovery and select again.
        let candidate = match select_candidate(
            &mut select,
            group,
            model,
            &mut failover,
            sticky_channel_key,
            &mut budget,
            time,
        )
        .await
        {
            Ok(candidate) => candidate,
            // AllUnavailable can fall back to the stashed channel-scoped 4xx; other errors are
            // propagated as-is.
            Err(SelectError::AllUnavailable(e)) => {
                tracing::warn!(
                    group,
                    model,
                    excluded = failover.exclude().len(),
                    "retry loop: all candidates unavailable"
                );
                return finish_with_fallback(&mut last_switchable, || e);
            }
            Err(SelectError::Fatal(e)) => {
                tracing::error!(
                    error = ?e,
                    group,
                    model,
                    "retry loop: unrecoverable dispatch error"
                );
                return Err(e);
            }
        };
        // Only when we are really about to hit upstream does an attempt count get consumed
        // (waiting/switching does not count).
        let Some(attempt_no) = failover.next_attempt() else {
            // Attempt budget exhausted: prefer the stashed 4xx, otherwise BudgetExhausted
            // (upper layer 503/504).
            tracing::warn!(group, model, "retry loop: attempt budget exhausted");
            return finish_with_fallback(&mut last_switchable, || {
                budget_exhausted(
                    &mut select,
                    group,
                    model,
                    failover.exclude(),
                    sticky_channel_key,
                    failover.min_context(),
                )
            });
        };
        // This attempt uses the **normal** egress timeout (not clamped to the remaining budget).
        let outcome = run_attempt(&mut attempt, &candidate, &mut report).await;
        // S2: read the failed attempt's duration before moving the outcome (successful
        // Done/Fatal do not enter this path).
        let failed_latency_ms = u64::from(outcome.latency_ms().unwrap_or(0));
        if let Some(finished) = settle_attempt(
            outcome,
            candidate,
            attempt_no,
            &mut failover,
            &mut last_switchable,
        ) {
            return Ok(finished);
        }
        // (Reaching here = this attempt failed and failover must continue) S2: the failure
        // duration counts toward the budget; over budget → pass through the stashed 4xx if
        // present, otherwise BudgetExhausted (lazy: accounted only at the failure site).
        budget.consume(failed_latency_ms);
        if budget.exceeded() {
            tracing::warn!(group, model, "retry loop: time budget exhausted");
            return finish_with_fallback(&mut last_switchable, || {
                budget_exhausted(
                    &mut select,
                    group,
                    model,
                    failover.exclude(),
                    sticky_channel_key,
                    failover.min_context(),
                )
            });
        }
    }
}

/// Run one upstream attempt and write the health report back (the `attempt` → `report` order is
/// unchanged).
async fn run_attempt<Fut>(
    attempt: &mut impl FnMut(&Candidate) -> Fut,
    candidate: &Candidate,
    report: &mut impl FnMut(&str, AttemptReport),
) -> AttemptOutcome
where
    Fut: Future<Output = AttemptOutcome>,
{
    let outcome = attempt(candidate).await;
    report(
        &candidate.unit.meta.key,
        AttemptReport::new(
            outcome.as_health_result(),
            outcome.latency_ms(),
            outcome.retry_after_ms(),
        ),
    );
    outcome
}

/// Finish one attempt: on termination (Done / Fatal) return the result to carry out;
/// `FatalButSwitchable` stashes that result, other failures record the exclusion set, and `None`
/// means failover continues.
fn settle_attempt(
    outcome: AttemptOutcome,
    candidate: Candidate,
    attempt_no: u32,
    failover: &mut Failover,
    last_switchable: &mut Option<(crate::Attempt, AttemptOutcome)>,
) -> Option<(crate::Attempt, AttemptOutcome)> {
    match outcome {
        // Done (success / 4xx-with-usage) and Fatal (client problem, no channel switch) terminate
        // the loop and carry out the candidate context of the winning (or terminating) attempt.
        // Success does not count toward the failover budget.
        AttemptOutcome::Done { .. } | AttemptOutcome::Fatal(_, _) => Some((
            crate::Attempt {
                candidate,
                attempt_no,
            },
            outcome,
        )),
        // Context-window overflow: exclude the failed candidate and **raise the
        // dispatch floor** past its window, so the next selection can only land on
        // strictly larger windows (the fit estimate undershot). A channel without a
        // window bound (unlimited) has no escalation direction — the overflow passes
        // through to the client as terminal.
        AttemptOutcome::WindowOverflow => {
            failover.mark_tried(candidate.unit.meta.key.clone());
            let window = candidate
                .settings
                .get("context_window")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0);
            if window > 0 {
                failover.raise_floor(window);
                None
            } else {
                Some((
                    crate::Attempt {
                        candidate,
                        attempt_no,
                    },
                    outcome,
                ))
            }
        }
        AttemptOutcome::Retryable(_, _) | AttemptOutcome::Throttled { .. } => {
            // A final failure that is no longer a channel-scoped 4xx → clear the stash and pass
            // through the "last" candidate only.
            // 429 is synonymous with Retryable: switch candidates and continue (rate limiting
            // often regains capacity on another candidate).
            *last_switchable = None;
            failover.mark_tried(candidate.unit.meta.key.clone());
            None
        }
        AttemptOutcome::FatalButSwitchable(class, _) => {
            failover.mark_tried(candidate.unit.meta.key.clone());
            *last_switchable = Some((
                crate::Attempt {
                    candidate,
                    attempt_no,
                },
                AttemptOutcome::FatalButSwitchable(class, None),
            ));
            None
        }
    }
}

/// Lazy accumulator for the S2 request-level time budget: accumulates only at failed attempts
/// and cooldown waits; `total_ms == 0` = off (legacy behavior: `max_attempts` only).
struct RetryBudget {
    total_ms: u64,
    consumed_ms: u64,
}

impl RetryBudget {
    fn new(total_ms: u64) -> Self {
        Self {
            total_ms,
            consumed_ms: 0,
        }
    }

    /// Whether we are still within budget after waiting `wait_ms` (always false with the budget
    /// off: no blocking on the gateway side).
    fn can_wait(&self, wait_ms: u64) -> bool {
        wait_ms > 0 && self.total_ms > 0 && self.consumed_ms + wait_ms <= self.total_ms
    }

    /// Record consumed time.
    fn consume(&mut self, ms: u64) {
        self.consumed_ms += ms;
    }

    /// Budget on and overspent.
    fn exceeded(&self) -> bool {
        self.total_ms > 0 && self.consumed_ms > self.total_ms
    }
}

/// The two failure faces of [`select_candidate`]: `AllUnavailable` can fall back to the stashed
/// channel-scoped 4xx (pass through the real 4xx); other errors are propagated as-is (the stash is
/// not consumed).
enum SelectError {
    AllUnavailable(crate::DispatchError),
    Fatal(crate::DispatchError),
}

/// Routing phase: ModelUnknown/other errors are propagated immediately; `AllUnavailable`, when the
/// budget allows, sleeps until the shortest cooldown recovery and selects again (no attempt
/// consumed, no upstream touched), otherwise propagates as-is.
async fn select_candidate<Sel>(
    select: &mut Sel,
    group: &str,
    model: &str,
    failover: &mut Failover,
    sticky_channel_key: Option<&str>,
    budget: &mut RetryBudget,
    time: &dyn TimeControl,
) -> Result<Candidate, SelectError>
where
    Sel: FnMut(
        &str,
        &str,
        &[String],
        Option<&str>,
        Option<u64>,
    ) -> Result<Candidate, crate::DispatchError>,
{
    loop {
        // The sticky key is passed through verbatim every round: exclude growth and boost are
        // independent, and stickiness does not exempt a channel from the exclusion set.
        // The context-window floor travels with it: an overflow raised it before the
        // round, so the next selection can only land on strictly larger windows.
        match select(
            group,
            model,
            failover.exclude(),
            sticky_channel_key,
            failover.min_context(),
        ) {
            Ok(candidate) => return Ok(candidate),
            // The model really does not exist (missing configuration) → terminal 404, no wait and
            // no passthrough.
            Err(crate::DispatchError::ModelUnknown { .. }) => {
                return Err(SelectError::Fatal(crate::DispatchError::ModelUnknown {
                    group: group.to_string(),
                    model: model.to_string(),
                }));
            }
            // All cooling/disabled: there is a cooldown AND the budget is **on** (budget>0) AND
            // the wait still fits → sleep until the shortest recovery (no upstream hit, no attempt
            // consumed; the wait counts toward the budget). With the budget off (0), do **not**
            // block on the gateway side waiting for cooldown — return AllUnavailable directly and
            // let the client wait with 504+Retry-After, avoiding an endless spin in the
            // "permanently cooling, no budget cap" case. Otherwise pass through the stashed 4xx,
            // and if there is none give AllUnavailable (upper layer 503/504: no cooldown = 503
            // unavailable / with cooldown = 504).
            Err(crate::DispatchError::AllUnavailable { retry_after_ms, .. }) => {
                if wait_for_cooldown(retry_after_ms, budget, time, failover).await {
                    continue;
                }
                return Err(SelectError::AllUnavailable(
                    crate::DispatchError::AllUnavailable {
                        group: group.to_string(),
                        model: model.to_string(),
                        retry_after_ms,
                    },
                ));
            }
            Err(e) => return Err(SelectError::Fatal(e)),
        }
    }
}

/// Cooldown wait: when the budget allows, sleep until the shortest recovery and clear the tried
/// exclusion set. Returns whether a wait happened (`false` = no wait; the caller passes through /
/// fails).
async fn wait_for_cooldown(
    retry_after_ms: u64,
    budget: &mut RetryBudget,
    time: &dyn TimeControl,
    failover: &mut Failover,
) -> bool {
    if !budget.can_wait(retry_after_ms) {
        return false;
    }
    time.sleep_ms(retry_after_ms).await;
    budget.consume(retry_after_ms);
    // Clear the exclusion set after waiting out the recovery: recovered tried channels can be
    // selected again (connection-time tolerance).
    failover.reset_tried();
    true
}

/// "Pass through the stashed 4xx, otherwise return `err`" — shared by the three exits:
/// all-unavailable / attempt budget / time budget.
///
/// `err` is lazily evaluated: constructed only when there really is no stashed result
/// (`probe_retry_after` calls select again, so it must not run for nothing).
fn finish_with_fallback(
    last_switchable: &mut Option<(crate::Attempt, AttemptOutcome)>,
    err: impl FnOnce() -> crate::DispatchError,
) -> Result<(crate::Attempt, AttemptOutcome), crate::DispatchError> {
    last_switchable.take().map_or_else(|| Err(err()), Ok)
}

/// Construct `BudgetExhausted` (`retry_after_ms` probed via [`probe_retry_after`]).
fn budget_exhausted<Sel>(
    select: &mut Sel,
    group: &str,
    model: &str,
    exclude: &[String],
    sticky_channel_key: Option<&str>,
    min_context: Option<u64>,
) -> crate::DispatchError
where
    Sel: FnMut(
        &str,
        &str,
        &[String],
        Option<&str>,
        Option<u64>,
    ) -> Result<Candidate, crate::DispatchError>,
{
    crate::DispatchError::BudgetExhausted {
        group: group.to_string(),
        model: model.to_string(),
        retry_after_ms: probe_retry_after(
            select,
            group,
            model,
            exclude,
            sticky_channel_key,
            min_context,
        ),
    }
}

/// Probe "the current shortest cooldown recovery time" when the chain breaks (time/attempt budget
/// exhausted), to feed the 504's `Retry-After`. Only `AllUnavailable.retry_after_ms` (a real
/// cooldown) counts; other cases (no cooling channel / rate limited / model really absent) return
/// 0 (the client resends immediately).
fn probe_retry_after<Sel>(
    select: &mut Sel,
    group: &str,
    model: &str,
    exclude: &[String],
    sticky_channel_key: Option<&str>,
    min_context: Option<u64>,
) -> u64
where
    Sel: FnMut(
        &str,
        &str,
        &[String],
        Option<&str>,
        Option<u64>,
    ) -> Result<Candidate, crate::DispatchError>,
{
    match select(group, model, exclude, sticky_channel_key, min_context) {
        Err(crate::DispatchError::AllUnavailable { retry_after_ms, .. }) => retry_after_ms,
        _ => 0,
    }
}
