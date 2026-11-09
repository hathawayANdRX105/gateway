//! `pipeline` — chained stage orchestrator
//!
//! Usage:
//! ```ignore
//! let pipe = Pipeline::new()
//!     .push(AdmissionStage::new(...))
//!     .push(DispatchStage::new(...))
//!     .push(ForwardStage::new(...))
//!     .push(ProtocolBridgeStage::new(...));
//! let resp = pipe.run(ctx).await?;
//! ```

use gateway_core::metering::Ledger;

use crate::ctx::{RequestCtx, RouteAttribution};
use crate::stage::{Stage, StageError, StageOutcome};
use axum::body::Body;
use http::Response;
use std::sync::Arc;

/// Chained stage container
pub struct Pipeline {
    stages: Vec<Arc<dyn Stage>>,
    /// Prehold ledger shared with the admission track (`QuotaGate`). `None` in shapes
    /// without preholds (coding face, personal shape, gate/dispatch tests) — `None`
    /// means no release is performed and the field is simply absent.
    ///
    /// Stages settle or release `ctx.hold` themselves at their commit points; the
    /// pipeline only releases a hold still outstanding at a terminal outcome
    /// (short circuit / stage error / ran off the end), exactly once, via
    /// [`Self::release_pending_hold`].
    ledger: Option<Arc<dyn Ledger>>,
}

impl Pipeline {
    pub fn new() -> Self {
        Self {
            stages: vec![],
            ledger: None,
        }
    }

    /// Mounts the prehold ledger so terminal pipeline outcomes can release a request's
    /// outstanding admission hold exactly once. `None` stays valid: no release is
    /// performed and no hold exists in shapes without preholds.
    pub fn with_ledger(mut self, ledger: Option<Arc<dyn Ledger>>) -> Self {
        self.ledger = ledger;
        self
    }

    /// Append one stage to the chain
    pub fn push<S: Stage + 'static>(mut self, s: S) -> Self {
        self.stages.push(Arc::new(s));
        self
    }

    /// Append a shared stage object — the facade builder's custom-stage slots
    /// store `Arc<dyn Stage>` and push them here.
    pub fn push_shared(mut self, stage: Arc<dyn Stage>) -> Self {
        self.stages.push(stage);
        self
    }

    pub fn len(&self) -> usize {
        self.stages.len()
    }

    pub fn is_empty(&self) -> bool {
        self.stages.is_empty()
    }

    /// Run all stages in order
    ///
    /// Returns immediately on `ShortCircuit` / `Stream` / `Err`.
    ///
    /// Before returning an `Ok` response, if Dispatch has selected a route, pack the channel
    /// attribution into [`RouteAttribution`] and insert it into the response extensions (both
    /// the non-streaming and streaming exits must be covered; see `attach_attribution`).
    pub async fn run(&self, mut ctx: RequestCtx) -> Result<Response<Body>, StageError> {
        for (i, stage) in self.stages.iter().enumerate() {
            tracing::trace!(
                stage = stage.name(),
                idx = i,
                req_id = %ctx.request.request_id,
                "pipeline::run stage start"
            );
            let outcome = match stage.handle(&mut ctx).await {
                Ok(outcome) => outcome,
                // A stage error is terminal: the request-level prehold taken at admission
                // is still outstanding unless a gate released it on its own rejection
                // (GateChain takes the hold, so `take` below is a no-op there). Release
                // exactly once so the reservation does not stick until the next reseed.
                Err(e) => {
                    self.release_pending_hold(&mut ctx);
                    return Err(e);
                }
            };
            match outcome {
                StageOutcome::Continue => continue,
                StageOutcome::ShortCircuit(resp) => {
                    // Short circuit (e.g. a later gate rejection): release the admission
                    // hold exactly once before exiting. GateChain's own rejections already
                    // took the hold, so this is a no-op there; any other short circuit
                    // would otherwise leak the reservation until reseed.
                    self.release_pending_hold(&mut ctx);
                    return Ok(attach_attribution(ctx, resp));
                }
                StageOutcome::Stream(stream) => {
                    // Streaming: the hold already moved into the HoldGuard traveling with
                    // the body (stream::finish settles it at stream end; Drop refunds on
                    // disconnect). Nothing to release here — on the forward success path
                    // commit_forwarded already consumed `ctx.hold` at the settlement commit
                    // point, so it is `None`.
                    return Ok(attach_attribution(ctx, stream.into_response()));
                }
            }
        }
        // Ran every stage without producing a response: release the still-outstanding
        // hold once before surfacing the error.
        self.release_pending_hold(&mut ctx);
        Err(StageError::Internal(anyhow::anyhow!(
            "pipeline ended without producing a response (stages={})",
            self.stages.len()
        )))
    }

    /// Release the request-level prehold exactly once when the pipeline exits without
    /// settling it at the commit point (stage error, short circuit, or ran off the end).
    ///
    /// The forward success path is untouched: `commit_forwarded`'s settle consumed
    /// `ctx.hold` (it is `None`), and streaming already moved it into the `HoldGuard`.
    /// `take` is a no-op on `None`, so a gate that released the hold itself (GateChain)
    /// cannot be double-released here.
    fn release_pending_hold(&self, ctx: &mut RequestCtx) {
        if let Some(hold) = ctx.hold.take()
            && let Some(ledger) = &self.ledger
        {
            ledger.release(&hold);
        }
    }
}

/// Pack the channel attribution selected by Dispatch from ctx into the response extensions.
///
/// ctx is consumed by value inside `run`: after the streaming branch hands the body to
/// SsePipe, ctx can no longer be recovered from the handler (`Pipeline::run` returns only the
/// response), so packaging must finish before run returns — that is what lets streaming
/// requests carry channel attribution too. A consumer outside the chain can read it back from
/// `response.extensions()`.
///
/// When no route was selected (short-circuited before dispatch, e.g. 401 / quota rejection),
/// nothing is inserted: there is no "channel actually hit", and the usage side treats it as
/// missing.
#[inline]
fn attach_attribution(ctx: RequestCtx, mut resp: Response<Body>) -> Response<Body> {
    // ctx is consumed by value here, so move the attribution fields out instead of
    // cloning them. `selected_channel_key` normally carries DispatchStage's pick; if a
    // custom stage only wrote `route`, fall back to the key owned by that route.
    // Either way no clone is needed (the owned fields are moved, and `route` is dropped
    // right after this block).
    let RequestCtx {
        route,
        selected_channel_key,
        selected_channel_name,
        requested_model,
        ..
    } = ctx;
    if let Some(route) = route {
        let channel_key = selected_channel_key.unwrap_or(route.unit.channel_key);
        resp.extensions_mut().insert(RouteAttribution {
            channel_key,
            channel_name: selected_channel_name.unwrap_or_default(),
            model: requested_model.unwrap_or_default(),
        });
    }
    resp
}

impl Default for Pipeline {
    fn default() -> Self {
        Self::new()
    }
}
