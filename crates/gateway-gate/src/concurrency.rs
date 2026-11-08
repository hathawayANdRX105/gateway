//! `concurrency` — gate 7: post-dispatch concurrency slots (one `Semaphore` per channel)
//!
//! Unlike the other gates, this one runs after dispatch (it needs the selected channel
//! before it can take a slot), and it must be mounted after the dispatch stages in the
//! pipeline.
//!
//! The index is `ChannelRecord.meta.key` (i.e. `RouteUnitRecord.channel_key`), the
//! channel-level key exposed by the catalog snapshot; channels have no numeric id.
//!
//! ## Permit lifecycle (RAII)
//!
//! [`Stage::handle`] grabs a slot with `try_acquire_owned`: on success the permit goes
//! straight into [`RequestCtx::drop_guards`] and drops with the ctx, returning the slot
//! (for a streaming response the ctx drops when the stream is set up, so the slot is
//! released there rather than at stream end — see the `DropGuards` caveat). There is no
//! intermediate hold_id bookkeeping, so no path can leak a slot by dropping the handle.
//! When the channel is full the stage returns `Err(StageError::RateLimited)` immediately
//! (mapped to 429) and **never blocks or queues**.
//!
//! Unregistered channels are **not concurrency-limited**: they pass through with
//! `Continue` (one warn per channel). The gate never does
//! `or_insert_with(Semaphore::new(0))` — a zero-capacity semaphore would deadlock the
//! first request forever.

use std::sync::Arc;

use async_trait::async_trait;
use dashmap::{DashMap, DashSet};
use gateway_pipeline::{RequestCtx, Stage, StageError, StageOutcome};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Concurrency-slot state shared across requests.
#[derive(Default)]
pub struct ConcurrencyState {
    /// channel_key → Semaphore
    pub semaphores: DashMap<String, Arc<Semaphore>>,
}

impl ConcurrencyState {
    /// Register a channel's concurrency cap.
    ///
    /// An already-registered channel keeps its existing semaphore so permits already
    /// issued are not lost on replacement; runtime downsizing is still handled by the
    /// upper layer's rebuild flow.
    pub fn register_channel(&self, channel_key: &str, max_concurrency: u32) {
        self.semaphores
            .entry(channel_key.to_string())
            .or_insert_with(|| Arc::new(Semaphore::new(max_concurrency as usize)));
    }

    /// Current available slot count for a channel; unregistered channels return `None`,
    /// distinctly different from the `Some(0)` of a registered but full channel.
    pub fn available_permits(&self, channel_key: &str) -> Option<usize> {
        self.semaphores
            .get(channel_key)
            .map(|semaphore| semaphore.available_permits())
    }

    /// Try to take one channel slot.
    ///
    /// `None` means the channel has no registered concurrency cap and is treated as
    /// unlimited; `Some(None)` means it is registered but full, and the caller should
    /// reject this attempt as `rate_limited`. Dropping the returned permit returns the
    /// slot.
    pub fn try_acquire(&self, channel_key: &str) -> Option<Option<OwnedSemaphorePermit>> {
        let semaphore = self.semaphores.get(channel_key).map(|held| held.clone())?;
        Some(semaphore.try_acquire_owned().ok())
    }
}

pub struct ConcurrencyGate {
    state: Arc<ConcurrencyState>,
    /// Warn dedup marker for unregistered channels: warn once per channel instead of
    /// flooding the log on every request.
    warned_unregistered: DashSet<String>,
    /// Warn dedup marker for saturated channels: warn once per channel instead of
    /// flooding the log while a channel stays at its concurrency cap.
    warned_saturated: DashSet<String>,
}

impl ConcurrencyGate {
    pub fn new(state: Arc<ConcurrencyState>) -> Self {
        Self {
            state,
            warned_unregistered: DashSet::new(),
            warned_saturated: DashSet::new(),
        }
    }

    /// Register / update a channel's concurrency cap.
    pub fn register_channel(&self, channel_key: &str, max_concurrency: u32) {
        self.state.register_channel(channel_key, max_concurrency);
    }

    /// Current available slots; `None` for an unregistered channel, `Some(0)` when
    /// registered but full.
    pub fn available_permits(&self, channel_key: &str) -> Option<usize> {
        self.state.available_permits(channel_key)
    }
}

#[async_trait]
impl Stage for ConcurrencyGate {
    fn name(&self) -> &'static str {
        "concurrency"
    }

    async fn handle(&self, ctx: &mut RequestCtx) -> Result<StageOutcome, StageError> {
        // 1. A SelectedRoute is required to know which channel's slot to take.
        let channel_key = ctx
            .route
            .as_ref()
            .ok_or_else(|| {
                StageError::Internal(anyhow::anyhow!("concurrency gate requires SelectedRoute"))
            })?
            .unit
            .channel_key
            .clone();

        // 2. Unregistered channel: unlimited concurrency, pass straight through (one warn
        //    per channel).
        let Some(sem) = self.state.semaphores.get(&channel_key).map(|s| s.clone()) else {
            if self.warned_unregistered.insert(channel_key.clone()) {
                tracing::warn!(
                    channel_key = %channel_key,
                    "concurrency gate: channel not registered, passing through unlimited"
                );
            }
            return Ok(StageOutcome::Continue);
        };

        // 3. Take the slot synchronously: success → the permit is attached to ctx as an
        //    RAII guard and returns the slot when ctx drops; full → RateLimited (router
        //    maps it to 429).
        match sem.try_acquire_owned() {
            Ok(permit) => {
                ctx.drop_guards.push(Box::new(permit));
                Ok(StageOutcome::Continue)
            }
            Err(tokio::sync::TryAcquireError::NoPermits) => {
                if self.warned_saturated.insert(channel_key.clone()) {
                    tracing::warn!(
                        channel_key = %channel_key,
                        "concurrency gate: channel saturated, request rate limited"
                    );
                }
                Err(StageError::RateLimited)
            }
            // This module never close()s a semaphore, so the Closed arm is theoretically
            // unreachable; fall back to Internal.
            Err(e) => Err(StageError::Internal(anyhow::anyhow!(
                "semaphore closed: {e}"
            ))),
        }
    }
}
