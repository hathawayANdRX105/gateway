//! `stage_cache` — response-cache wiring for [`ForwardStage`]: key derivation,
//! the hit short-circuit, and the store at the settlement commit point.
//!
//! ## Read path (top of `ForwardStage::handle`, both drive modes)
//!
//! The lookup runs after the gates (auth / model / quota are decided — so the
//! key has a settlement identity) and before any upstream work. A hit replays
//! the stored answer and settles it exactly like the original call
//! ([`Self::commit_cache_hit`] + `settle_cache_hit`); it never reports health,
//! writes sticky, or touches concurrency / token accounting, because none of
//! those signals exist for a replay. Streaming requests never hit the cache.
//!
//! ## Write path (non-streaming settlement commit point, `commit_non_stream`)
//!
//! The store happens after settlement, so the stored body is the client-facing
//! answer and the stored counts are the settled ones — a replay is
//! byte-identical and bills identically. The recorded channel attribution is
//! the winning attempt's, so `usage_logs` names the channel that actually
//! produced the content, on the original call and on every replay.

use bytes::Bytes;
use gateway_cache::{CacheKey, CacheScope, CachedResponse, CachedUsage, response_key};
use gateway_pipeline::{ProtocolKind, RequestCtx, SelectedRoute, StageOutcome, UpstreamResponse};

use crate::stage::ForwardStage;

impl ForwardStage {
    /// Derive this request's response-cache key; `None` = not cacheable (cache
    /// not mounted / no settlement identity / no model resolved by the gate).
    ///
    /// The key is the isolation-scoped identity: user + group (billing context
    /// decides model access and the price multiplier) + public model + inbound
    /// protocol + raw request body (the filtered bytes — the same bytes every
    /// attempt and the settlement see).
    pub(crate) fn response_cache_key(&self, ctx: &RequestCtx, body: &Bytes) -> Option<CacheKey> {
        self.response_cache.as_ref()?;
        let token = ctx.token.as_ref()?;
        let scope = CacheScope {
            user_key: token.user_key.clone(),
            group: token.group.clone(),
        };
        let model = ctx.requested_model.as_deref()?;
        Some(response_key(
            &scope,
            model,
            protocol_tag(ctx.request.inbound_protocol),
            body,
        ))
    }

    /// Response-cache lookup; cache not mounted / miss / expired ⇒ `None`.
    pub(crate) fn lookup_response_cache(
        &self,
        key: &CacheKey,
    ) -> Option<std::sync::Arc<CachedResponse>> {
        self.response_cache.as_ref()?.lookup(key)
    }

    /// Cache-hit short-circuit: replay the stored answer and settle it exactly
    /// like the original call. No upstream interaction happened: no health
    /// report, no sticky write, no concurrency / token accounting.
    pub(crate) fn commit_cache_hit(
        &self,
        ctx: &mut RequestCtx,
        hit: &CachedResponse,
    ) -> StageOutcome {
        self.settle_cache_hit(ctx, hit);
        ctx.upstream = Some(UpstreamResponse {
            status: hit.status,
            body: hit.body.clone(),
        });
        StageOutcome::Continue
    }

    /// Response-cache write at the settlement commit point (opt-in). The
    /// client-facing body plus the settled counts and the winning attempt's
    /// attribution — a replay is byte-identical and bills identically.
    ///
    /// `body` is cloned by reference count (`Bytes`), so the store never copies
    /// the response payload.
    pub(crate) fn store_response_cache(
        &self,
        key: &CacheKey,
        body: &Bytes,
        status: u16,
        counts: gateway_metering::scanner::TokenCounts,
        candidate: &SelectedRoute,
    ) {
        let Some(cache) = self.response_cache.as_ref() else {
            return;
        };
        cache.store(
            key.clone(),
            CachedResponse {
                status,
                body: body.clone(),
                usage: CachedUsage {
                    prompt_tokens: counts.prompt,
                    completion_tokens: counts.completion,
                    cached_tokens: counts.cached,
                    cache_creation_tokens: counts.cache_creation,
                    reasoning_tokens: counts.reasoning,
                    prompt_includes_cache: counts.prompt_includes_cache,
                },
                channel_key: candidate.unit.channel_key.clone(),
                unit_key: candidate.unit.meta.key.clone(),
                public_model: candidate.unit.public_model.clone(),
                upstream_model: candidate.upstream_model.clone(),
                group: candidate.unit.group.clone(),
            },
        );
    }
}

/// Stable protocol tag for the cache key: the inbound protocol decides the
/// response body's shape (each protocol serializes the same completion its own
/// way), so two protocols never share an entry.
fn protocol_tag(kind: ProtocolKind) -> &'static str {
    match kind {
        ProtocolKind::OpenAI => "openai",
        ProtocolKind::Anthropic => "anthropic",
        ProtocolKind::Gemini => "gemini",
        ProtocolKind::OpenAIResp => "openai-resp",
    }
}
