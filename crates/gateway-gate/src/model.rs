//! `model` — gate 5: model allowlist (`token.allowed_models`) plus request-body
//! model/max_tokens parsing.
//!
//! Also hosts [`GroupModelGate`]: a group-level model allowlist (backed by
//! [`crate::snapshot::GroupSnapshot`]), an additional gate parallel to the token-level
//! [`ModelGate`].

use async_trait::async_trait;
use gateway_pipeline::BodySource;
use serde::Deserialize;

use super::chain::{Gate, GateCtx};
use super::error::Rejection;
use super::snapshot::SharedGroupSnapshot;

pub struct ModelGate;

#[async_trait]
impl Gate for ModelGate {
    fn name(&self) -> &'static str {
        "model"
    }

    async fn check(&self, ctx: &mut GateCtx) -> Result<(), Rejection> {
        // 0. Parse model + max_tokens from the body (when not yet parsed), then fetch token
        //    and model.
        ctx.ensure_model_parsed()?;
        let (model, token) = ctx.model_and_token()?;

        // 1. Allowlist match.
        if let Some(allowed) = &token.allowed_models
            && !allowed.is_empty()
            && !allowed.iter().any(|m| match_model(m, model))
        {
            return Err(Rejection::ModelForbidden {
                model: model.into(),
            });
        }
        Ok(())
    }
}

/// Body `model` field and max_tokens, using field names shared by OpenAI and Anthropic.
///
/// A gate whose only job is to parse the model: assemblies that run no
/// authentication (see the facade's `Shape::Open`) still need `requested_model`
/// in the context before forwarding, so parsing must be expressible without a
/// token. It runs `ensure_model_parsed` and nothing else — no allowlist, no
/// token lookup.
#[derive(Debug, Default, Clone, Copy)]
pub struct ModelParseGate;

#[async_trait]
impl Gate for ModelParseGate {
    fn name(&self) -> &'static str {
        "model-parse"
    }

    async fn check(&self, ctx: &mut GateCtx) -> Result<(), Rejection> {
        ctx.ensure_model_parsed()
    }
}

#[derive(Debug, Deserialize)]
pub struct BodyModel {
    pub model: Option<String>,
    pub max_tokens: Option<u32>,
    /// Anthropic uses variants such as max_completion_tokens.
    #[serde(default)]
    pub max_completion_tokens: Option<u32>,
}

/// Shared body-parsing entry point (used by both quota and model).
pub fn parse_body_model_into(body: &BodySource, ctx: &mut GateCtx) -> Result<(), Rejection> {
    let bytes = match body {
        BodySource::InMemory(b) => b.as_ref(),
        BodySource::OnDisk { .. } => return Ok(()), // on-disk bodies are not parsed yet
    };
    let bm: BodyModel = match serde_json::from_slice(bytes) {
        Ok(b) => b,
        Err(_) => return Ok(()), // not JSON / parse failure → skip
    };
    if let Some(m) = bm.model {
        ctx.requested_model = Some(m);
    }
    ctx.requested_max_tokens = bm.max_tokens.or(bm.max_completion_tokens);
    Ok(())
}

/// Model-name matching: exact match, or `*` wildcard (e.g. `gpt-4*` matches `gpt-4o`).
pub fn match_model(pattern: &str, model: &str) -> bool {
    if pattern == model {
        return true;
    }
    if let Some(prefix) = pattern.strip_suffix('*') {
        return model.starts_with(prefix);
    }
    false
}

/// Group-level model gate: matches the group allowlist for `ctx.group`, looked up in
/// [`GroupSnapshot`](crate::snapshot::GroupSnapshot).
///
/// An **additional** gate parallel to the token-level [`ModelGate`] (mount order: preferably
/// after ModelGate, since it relies on that gate having parsed `requested_model` into the
/// ctx; this gate does not parse the request body itself).
///
/// fail-open policy — "not configured" never blocks:
/// - missing `ctx.group` / group absent from the snapshot / empty allowlist → allow;
/// - only when the group exists with a non-empty allowlist and no pattern (including the
///   `gpt-4*` wildcard) matches does it return [`Rejection::ModelNotAllowedForGroup`].
///
/// The disabled-group semantics are an exception to fail-open: when the group exists in the
/// snapshot and was disabled in the admin console (`enabled = false`), the **whole group**
/// is rejected with [`Rejection::GroupDisabled`] (see the `api_groups.status` comment in
/// migration 0001: disabled group → whole-group rejection at the gate layer).
pub struct GroupModelGate {
    groups: SharedGroupSnapshot,
}

impl GroupModelGate {
    pub fn new(groups: SharedGroupSnapshot) -> Self {
        Self { groups }
    }
}

#[async_trait]
impl Gate for GroupModelGate {
    fn name(&self) -> &'static str {
        "group_model"
    }

    async fn check(&self, ctx: &mut GateCtx) -> Result<(), Rejection> {
        let model = ctx
            .requested_model
            .as_deref()
            .ok_or(Rejection::ModelNotSpecified)?;

        // No group / empty group name = no group-level gating configured → fail-open.
        let Some(group) = ctx.group.as_deref().filter(|g| !g.is_empty()) else {
            return Ok(());
        };
        let snapshot = self.groups.load();
        // A group explicitly disabled in the admin console (status≠1) is rejected as a whole,
        // regardless of its allowlist.
        if snapshot.is_disabled(group) {
            return Err(Rejection::GroupDisabled {
                group: group.into(),
            });
        }
        // Group missing (unconfigured) → fail-open; an existing group with an empty allowlist
        // is likewise unrestricted.
        let Some(allowed) = snapshot.allowed_models(group) else {
            return Ok(());
        };
        if allowed.is_empty() || allowed.iter().any(|p| match_model(p, model)) {
            return Ok(());
        }
        Err(Rejection::ModelNotAllowedForGroup {
            model: model.into(),
            group: group.into(),
        })
    }
}
