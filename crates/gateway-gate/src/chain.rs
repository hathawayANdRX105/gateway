//! `chain` — `GateChain`: chains multiple gates into a single stage.
//!
//! It looks like one stage from the outside; internally it runs the gates in order and
//! returns immediately on the first failure.

use std::sync::Arc;

use async_trait::async_trait;
use gateway_metering::ledger::{Hold, Ledger};
use gateway_pipeline::{RequestCtx, Stage, StageError, StageOutcome};

use crate::error::{Rejection, rejection_to_response};
use crate::{TokenInfo, UserInfo};

/// Gate check context (per-request intermediates shared between gates).
#[derive(Debug, Clone)]
pub struct GateCtx {
    /// Immutable request input.
    pub request_meta: gateway_pipeline::RequestMeta,
    /// Plaintext key extracted by the auth stage.
    pub raw_key: Option<String>,
    /// Written by the auth stage: contract `UserRecord.meta.key` (used by the state gate).
    pub user_key: Option<String>,
    /// Populated after auth passes.
    pub token: Option<TokenInfo>,
    /// Populated after state passes.
    pub user: Option<UserInfo>,
    /// Effective group (token.group wins; user.group is the fallback).
    pub group: Option<String>,
    /// Cost computed by the quota stage.
    pub estimated_cost: Option<i64>,
    /// Model parsed from the request body by the model stage.
    pub requested_model: Option<String>,
    /// `max_tokens` parsed from the request body by the model stage.
    pub requested_max_tokens: Option<u32>,
    /// Prehold taken by the quota gate at admission; promoted into `RequestCtx.hold` when
    /// the whole chain passes, released by the chain when a later gate rejects.
    pub hold: Option<Hold>,
}

impl GateCtx {
    /// Ensure the request body has been parsed for `model` / `max_tokens`; parsing is skipped
    /// once `requested_model` is set.
    ///
    /// # Errors
    ///
    /// Propagates the [`Rejection`] produced by body parsing.
    pub fn ensure_model_parsed(&mut self) -> Result<(), Rejection> {
        if self.requested_model.is_none() {
            let body = self.request_meta.body.clone();
            crate::model::parse_body_model_into(&body, self)?;
        }
        Ok(())
    }

    /// Fetch the public model and token for this request (shared by the `model` and
    /// `quota` gates).
    ///
    /// Requires [`Self::ensure_model_parsed`] first.
    ///
    /// # Errors
    ///
    /// Yields [`Rejection::AuthSkipped`] when the token has not passed the auth gate,
    /// and [`Rejection::ModelNotSpecified`] when the request body carries no model.
    pub fn model_and_token(&self) -> Result<(&str, &TokenInfo), Rejection> {
        let token = self.token.as_ref().ok_or(Rejection::AuthSkipped)?;
        let model = self
            .requested_model
            .as_deref()
            .ok_or(Rejection::ModelNotSpecified)?;
        Ok((model, token))
    }
}

/// Final output after the gates pass (reserved; the chain currently promotes only the token
/// and model name into `RequestCtx`).
#[derive(Debug)]
pub struct Gated {
    pub token: TokenInfo,
    pub user: UserInfo,
    pub group: String,
}

/// Gate trait
#[async_trait]
pub trait Gate: Send + Sync {
    fn name(&self) -> &'static str;
    async fn check(&self, ctx: &mut GateCtx) -> Result<(), Rejection>;
}

/// Serial orchestration of multiple gates.
#[derive(Clone)]
pub struct GateChain {
    gates: Vec<Arc<dyn Gate>>,
    /// Ledger handle for releasing preholds taken by the quota gate when a later gate
    /// rejects the request. `None` when no ledger is mounted (no preholds exist).
    ledger: Option<Arc<dyn Ledger>>,
}

impl GateChain {
    #[must_use]
    pub fn new() -> Self {
        Self {
            gates: vec![],
            ledger: None,
        }
    }

    #[must_use]
    pub fn push<G: Gate + 'static>(mut self, g: G) -> Self {
        self.gates.push(Arc::new(g));
        self
    }
    /// Mounts the ledger so a prehold taken by an earlier gate (quota) can be released
    /// when a later gate rejects the request.
    #[must_use]
    pub fn with_ledger(mut self, ledger: Arc<dyn Ledger>) -> Self {
        self.ledger = Some(ledger);
        self
    }

    /// Drop the gate with this name (see [`Gate::name`]); returns whether one was
    /// removed. Composition support for callers that assemble their own chain from
    /// a preset.
    pub fn remove(&mut self, name: &str) -> bool {
        let before = self.gates.len();
        self.gates.retain(|gate| gate.name() != name);
        self.gates.len() != before
    }

    /// Whether a gate with this name is currently mounted.
    #[must_use]
    pub fn contains(&self, name: &str) -> bool {
        self.gates.iter().any(|gate| gate.name() == name)
    }

    /// Append a boxed gate — the facade builder stores appended gates as trait
    /// objects and pushes them here.
    pub fn push_gate_shared(&mut self, gate: Arc<dyn Gate>) {
        self.gates.push(gate);
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.gates.len()
    }
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.gates.is_empty()
    }
}

impl Default for GateChain {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Stage for GateChain {
    fn name(&self) -> &'static str {
        "gate"
    }

    async fn handle(&self, ctx: &mut RequestCtx) -> Result<StageOutcome, StageError> {
        let mut gate_ctx = GateCtx {
            request_meta: ctx.request.clone(),
            raw_key: None,
            user_key: None,
            token: None,
            user: None,
            group: None,
            estimated_cost: None,
            requested_model: None,
            requested_max_tokens: None,
            hold: None,
        };

        for gate in &self.gates {
            if let Err(rej) = gate.check(&mut gate_ctx).await {
                tracing::warn!(?rej, "gate chain rejected request");
                // A later gate rejected the request (e.g. rate limit after quota): release
                // any prehold taken by an earlier gate so the reservation does not stick
                // until the next reload reseeds the ledger.
                if let Some(hold) = gate_ctx.hold.take()
                    && let Some(ledger) = &self.ledger
                {
                    ledger.release(&hold);
                }
                return Ok(StageOutcome::ShortCircuit(rejection_to_response(&rej)));
            }
        }

        // All gates passed: promote the token and model name into `RequestCtx`.
        // The model name is overridden only when ModelGate parsed one (assemblies without
        // ModelGate keep the preset value).
        if let Some(model) = gate_ctx.requested_model.take() {
            ctx.requested_model = Some(model);
        }
        if let Some(token) = gate_ctx.token {
            // Use the effective group computed by StateGate (token.group wins, falling back
            // to user.group when empty). Using token.group directly would send an empty group
            // into dispatch for tokens without an explicit group, which cannot match
            // route_unit's group → always 404.
            let effective_group = gate_ctx
                .group
                .clone()
                .filter(|g| !g.is_empty())
                .unwrap_or_else(|| token.group.clone());
            ctx.token = Some(gateway_pipeline::TokenInfo {
                id: token.id.clone(),
                user_key: token.user_key.clone(),
                group: effective_group,
                enabled: token.enabled,
                allowed_models: token.allowed_models.clone(),
                auth_version: token.auth_version,
            });
        }
        // Promote the prehold into `RequestCtx` so the forward stage can settle it
        // (actual cost) or release it (rejection / drop).
        if let Some(hold) = gate_ctx.hold.take() {
            ctx.hold = Some(hold);
        }
        Ok(StageOutcome::Continue)
    }
}
