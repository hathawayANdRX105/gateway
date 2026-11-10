//! Sticky-routing write side: key construction (`sticky_key`), hit lookup (the read side
//! calls the symmetric methods), and sticky-binding writes at the settlement commit point
//! (`bind_sticky`). The table structure plus the write/lookup primitives live in the
//! `gateway_dispatch::sticky` data plane; this side only constructs keys and decides writes
//! at the `ForwardStage` settlement point.

use bytes::Bytes;
use gateway_dispatch::sticky::{SESSION_HEADER, StickyKey, StickyTable};
use gateway_pipeline::RequestCtx;
use std::sync::Arc;

/// Sticky-binding input for the settlement commit point: the table, this request's key, and
/// the target channel (the winning attempt's channel).
///
/// The key must be computed **before** entering the response stream (streaming settlement
/// happens inside the unfold, where `ctx` is unavailable), so it travels with the table and
/// channel inside one `Arc` carried by the stream state: one extra `Arc` clone per chunk, and
/// the write happens only once the stream has been fully read.
pub(crate) struct StickyBind {
    pub(crate) table: Arc<StickyTable>,
    pub(crate) key: StickyKey,
    pub(crate) channel_key: String,
}

/// Sticky key (design §3.1/§3.3): **same source** as the read side's
/// `DispatchStage::sticky_channel_for` — the public model comes from `ctx.requested_model`,
/// the session hint from `ctx.session_hint` → the `x-session-id` header, and the fingerprint
/// from the **raw request-body bytes** of `raw_body`.
///
/// The adapted body is deliberately not used: the read side performs its lookup before
/// `Dispatch::select` and cannot physically obtain the adapted body, and differing sources on
/// the two sides would make stickiness never hit (the ruling in design §3.1). `None` = not
/// sticky for this request (no public model resolved, or neither a session hint nor a raw
/// body).
pub(crate) fn sticky_key(ctx: &RequestCtx, raw_body: &Bytes) -> Option<StickyKey> {
    let public_model = ctx.requested_model.as_deref()?;
    let session_hint = ctx
        .session_hint
        .as_deref()
        .map(str::trim)
        .filter(|h| !h.is_empty())
        .or_else(|| {
            ctx.request
                .headers
                .get(SESSION_HEADER)
                .and_then(|v| v.to_str().ok())
                .map(str::trim)
                .filter(|h| !h.is_empty())
        });
    StickyKey::from_request(public_model, session_hint, Some(raw_body.as_ref()))
}

/// Sticky-binding write at the settlement commit point (design §3.3, side path — the single
/// billing write point is unchanged): bind the winning channel into the table only when the
/// measured prefix cache was hit (`counts.cached > 0`).
///
/// Cold requests with `cached == 0` are not written (avoid pinning a channel that has not yet
/// produced cache benefit); a broken stream / failure / client disconnect (the stream is
/// dropped and neither arm runs) never reaches this write point and writes nothing either.
/// Table not mounted / no key source / no measured counts ⇒ skip the whole block.
pub(crate) fn bind_sticky(
    bind: Option<&Arc<StickyBind>>,
    counts: Option<gateway_metering::scanner::TokenCounts>,
) {
    let (Some(bind), Some(counts)) = (bind, counts) else {
        return;
    };
    if counts.cached == 0 {
        return;
    }
    bind.table.bind(bind.key.clone(), &bind.channel_key);
}
