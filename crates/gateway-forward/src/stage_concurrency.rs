//! `stage_concurrency` — mounting and acquiring the ForwardStage concurrency gates
//! (global gate + per-channel gate).
//!
//! A full slot always rejects the attempt as `rate_limited` (429, retryable, degraded):
//! a concurrency rejection is not an upstream error, so no `submit_failed` observation
//! event is emitted and the attempt reaches dispatch as a retryable outcome (driving
//! candidate switching) rather than as an upstream-status failure.
//! Assembly convention: `max = 0` means unlimited concurrency, i.e.
//! [`ForwardStage::with_concurrency`] is not called.

use gateway_core::error::NormalizedError;
use gateway_gate::concurrency::ConcurrencyState;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

use crate::stage::ForwardStage;

impl ForwardStage {
    /// Mount the global concurrency gate: the whole ForwardStage shares one
    /// `Semaphore(max)` — an overall cap, not a per-channel one; per-channel gating has its
    /// own [`ForwardStage::with_channel_concurrency`], which shares the dispatcher's
    /// `ConcurrencyState`.
    ///
    /// The acquire point precedes `forward_task` on every forwarding path, with `try`
    /// semantics and no queueing: in retry mode it sits inside the attempt closure (the
    /// permit is released when the attempt ends), in single-shot mode inside `handle` (the
    /// permit is held until `commit_forwarded` completes). A full slot rejects the attempt
    /// immediately as `rate_limited` (429, retryable, degraded) — no `submit_failed`
    /// observation event is emitted, since a concurrency rejection is not an upstream
    /// error.
    ///
    /// Note: a `max = 0` semaphore rejects every request; the assembly convention treats
    /// 0 as unlimited, i.e. it does not call this builder (`concurrency` stays `None`).
    pub fn with_concurrency(mut self, max: usize) -> Self {
        self.concurrency = Some(Arc::new(Semaphore::new(max)));
        self
    }

    /// Mount the per-channel concurrency gate: it shares one `ConcurrencyState` with the
    /// `Dispatcher`.
    ///
    /// The acquire point is inside the attempt closure, before `forward_task`; the permit
    /// goes into the winning attempt's [`crate::Forwarded`] and is released by RAII over
    /// the response lifetime (streaming: when the stream ends / is dropped; non-streaming:
    /// after commit has collected the whole body), while a failed attempt returns it as soon
    /// as the closure ends. A full slot rejects the attempt as `rate_limited` (429,
    /// retryable, degraded): no `submit_failed` event is emitted, since a concurrency
    /// rejection is not an upstream error. The dispatcher reads the same available-slot
    /// count so P2C actively avoids saturated channels — sharing one `Arc` is what makes
    /// that signal effective.
    pub fn with_channel_concurrency(mut self, state: Arc<ConcurrencyState>) -> Self {
        self.channel_concurrency = Some(state);
        self
    }
}

/// Concurrency-gate acquire (v2 mount point): `concurrency` not mounted → `Ok(None)`,
/// behaviour unchanged.
///
/// Full slot (`NoPermits`) → `Err(NormalizedError)`: `rate_limited` / 429 / `retryable`
/// / not channel-scoped — the caller therefore takes the degraded path (same as P1-C:
/// a retryable outcome that drives candidate switching, not an upstream-status failure),
/// and **does not call `submit_failed`** (a concurrency rejection is not an upstream
/// error, and there is no status_code to report). The permit is owned (it holds the
/// `Arc<Semaphore>`), so it can be held across await points and returns the slot when
/// dropped at scope end.
pub(crate) fn try_acquire_concurrency(
    concurrency: &Option<Arc<Semaphore>>,
) -> Result<Option<OwnedSemaphorePermit>, NormalizedError> {
    let Some(sem) = concurrency.as_ref() else {
        return Ok(None);
    };
    match sem.clone().try_acquire_owned() {
        Ok(permit) => Ok(Some(permit)),
        Err(TryAcquireError::NoPermits) => Err(crate::rate_limited("concurrency limit reached")),
        // This module never close()s a semaphore, so Closed is theoretically unreachable;
        // terminate as an internal error as a fallback.
        Err(e) => Err(NormalizedError {
            code: gateway_core::error::code::INTERNAL,
            status: 500,
            retryable: false,
            channel_scoped: false,
            message: format!("semaphore closed: {e}"),
            retry_after_ms: None,
        }),
    }
}
