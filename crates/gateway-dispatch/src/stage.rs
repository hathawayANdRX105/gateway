//! `stage` — `DispatchStage`: wires the dispatcher into the pipeline
//!
//! Second stage of the pipeline hot path: select candidates from the catalog snapshot
//! (Dispatcher) and write them into ctx. Failure mapping: `ModelUnknown` → `NoRoute` (404,
//! the model really is absent), AllUnavailable/BudgetExhausted → Unavailable (no cooling:
//! 503 unavailable / cooling: 504 + Retry-After), `SnapshotNotReady` → `NotReady` (503),
//! `RateLimited` → 429.

use crate::sticky::{self, StickyKey, StickyTable};
use crate::{Dispatch, DispatchError};
use async_trait::async_trait;
use gateway_pipeline::{BodySource, RequestCtx, Stage, StageError, StageOutcome};
use std::sync::Arc;

/// Dispatch stage — takes an admitted request (`RequestCtx`) and writes the selected
/// route into ctx.route.
pub struct DispatchStage {
    dispatch: Arc<dyn Dispatch>,
    /// Prefix-cache sticky table (design prompt-cache-sticky-routing §3.1):
    /// not mounted (`None`) = no stickiness, and route selection degrades byte for byte to
    /// the status quo. Table assembly lives in the relay builder (`app-relay`, ticket 3).
    sticky: Option<Arc<StickyTable>>,
}

impl DispatchStage {
    pub fn new(dispatch: Arc<dyn Dispatch>) -> Self {
        Self {
            dispatch,
            sticky: None,
        }
    }

    /// Mount the sticky table (builder). Not calling it = no stickiness on the whole
    /// path.
    #[must_use]
    pub fn with_sticky_table(mut self, table: Arc<StickyTable>) -> Self {
        self.sticky = Some(table);
        self
    }

    /// Sticky lookup (design prompt-cache-sticky-routing §3.2, first step):
    /// returns the channel key this request should stick to; `None` = table not mounted /
    /// no key source / no hit, and the whole path degrades to no stickiness.
    ///
    /// Key source priority: `ctx.session_hint` → `x-session-id` request header → raw body
    /// fingerprint. Route selection consumes the return value as the sticky prior boost key
    /// for `Dispatch::select`; on a hit, with the channel inside the candidate pool, it only
    /// raises the prior and does not bypass exclude/cooldown (design §4). Public because
    /// `tests/sticky_table.rs` needs a cross-crate assertion that "no table ⇒ None".
    pub fn sticky_channel_for(&self, ctx: &RequestCtx) -> Option<String> {
        let table = self.sticky.as_ref()?;
        let public_model = ctx.requested_model.as_deref()?;
        let session_hint = ctx
            .session_hint
            .as_deref()
            .map(str::trim)
            .filter(|h| !h.is_empty())
            .or_else(|| {
                ctx.request
                    .headers
                    .get(sticky::SESSION_HEADER)
                    .and_then(|v| v.to_str().ok())
                    .map(str::trim)
                    .filter(|h| !h.is_empty())
            });
        let raw_body = match &ctx.request.body {
            BodySource::InMemory(bytes) => Some(bytes.as_ref()),
            BodySource::OnDisk { .. } => None,
        };
        let key = StickyKey::from_request(public_model, session_hint, raw_body)?;
        table.lookup(&key, table.now_ms())
    }
}

#[async_trait]
impl Stage for DispatchStage {
    fn name(&self) -> &'static str {
        "dispatch"
    }

    async fn handle(&self, ctx: &mut RequestCtx) -> Result<StageOutcome, StageError> {
        let group = ctx
            .token
            .as_ref()
            .map(|t| t.group.clone())
            .unwrap_or_default();
        // The model name is parsed from the request body by gate::model and promoted to
        // ctx through GateChain.
        let public_model = ctx.requested_model.clone().unwrap_or_default();
        // Sticky lookup (design §3.2, first step): returns the channel key this request
        // should stick to, as select's prior boost; None = table not mounted / no key
        // source / no hit, and route selection degrades byte for byte to the status quo.
        let sticky_channel_key = self.sticky_channel_for(ctx);
        // Context fit (data-driven via channel settings.context_window): required
        // tokens = prompt estimate (O(n) body walk on the client body, which predates
        // the protocol bridge; OpenAI / Claude shapes are recognized) + the requested
        // output budget. A spilled body skips the estimate — reactive escalation still
        // covers it. Channels without a window are unlimited.
        let min_context = match &ctx.request.body {
            BodySource::InMemory(bytes) => {
                let prompt = gateway_metering::estimate::estimate_prompt_tokens(bytes);
                let max_tokens = u64::from(ctx.requested_max_tokens.unwrap_or(4096));
                Some(prompt.saturating_add(max_tokens))
            }
            BodySource::OnDisk { .. } => None,
        };
        match self.dispatch.select(
            &group,
            &public_model,
            &[],
            sticky_channel_key.as_deref(),
            min_context,
        ) {
            Ok(candidate) => {
                // The candidate itself is a SelectedRoute: secret / upstream_model /
                // provider_type / settings all enter ctx with it, and forward no longer
                // fabricates them.
                let channel_key = candidate.unit.channel_key.clone();
                ctx.route = Some(candidate);
                // Channel attribution: the key comes from the unit; the display name is
                // looked up in the Dispatcher snapshot. SelectedRoute carries no name
                // (dispatch's type is unchanged), so a miss (e.g. the snapshot hot-reloaded
                // the channel away) degrades to key only.
                let channel_name = self.dispatch.channel_name(&channel_key);
                if channel_name.is_none() {
                    tracing::debug!(
                        channel_key = %channel_key,
                        "channel name not found in dispatch snapshot; usage attribution carries key only"
                    );
                }
                ctx.selected_channel_key = Some(channel_key);
                ctx.selected_channel_name = channel_name;
                Ok(StageOutcome::Continue)
            }
            Err(DispatchError::SnapshotNotReady) => Err(StageError::NotReady),
            Err(DispatchError::ModelUnknown { group, model }) => {
                Err(StageError::NoRoute { group, model })
            }
            Err(
                DispatchError::AllUnavailable {
                    group,
                    model,
                    retry_after_ms,
                }
                | DispatchError::BudgetExhausted {
                    group,
                    model,
                    retry_after_ms,
                },
            ) => Err(StageError::Unavailable {
                group,
                model,
                retry_after_ms,
            }),
            Err(DispatchError::RateLimited { .. }) => Err(StageError::RateLimited),
        }
    }
}
