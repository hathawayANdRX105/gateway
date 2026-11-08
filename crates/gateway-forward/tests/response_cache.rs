//! Response-cache integration tests — the data-plane layer of "skip the repeat
//! LLM call".
//!
//! Coverage (single-shot mode, mock egress, fake price table + sink):
//! 1. A miss stores the settled response; a byte-identical second request
//!    replays it without touching the upstream, and the replay settles exactly
//!    like the original call (same counts, same channel attribution);
//! 2. A different user never replays another user's answer (isolation);
//! 3. Streaming requests never interact with the cache;
//! 4. Without the cache mounted, behavior is byte-for-byte the status quo
//!    (every identical request goes upstream).
//!
//! The mock and request constructors live in `tests/common/mod.rs` for sharing.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use gateway_forward::ForwardStage;
use gateway_metering::pricing::{ModelPrice, PriceTable};
use gateway_pipeline::{Stage, StageOutcome};

/// Upstream answer with OpenAI-style usage (prompt 100 / completion 50) — the
/// same shape the settle tests use, so the settled counts are deterministic.
const UPSTREAM_BODY: &[u8] =
    b"{\"choices\":[{\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":100,\"completion_tokens\":50}}";

/// Fixed price table: gpt-4o input $15/M, output $60/M (same as the settle
/// tests' RecordingPriceTable, without the recording).
struct FlatPriceTable;

impl PriceTable for FlatPriceTable {
    fn lookup(&self, _model: &str, _group: &str) -> Option<ModelPrice> {
        Some(ModelPrice {
            input: 15.0,
            output: 60.0,
            cache: 0.0,
            group_multiplier: 1.0,
        })
    }
}

fn cache_mount() -> Arc<gateway_cache::ResponseCache> {
    gateway_cache::ResponseCache::with_limits(
        Duration::from_secs(60),
        64 * 1024 * 1024,
        1024 * 1024,
    )
}

/// Minimal RequestCtx for single-shot mode (no with_retry): route preset, token
/// carries user/group (same shape as the settle tests' helper).
fn ctx_single_shot() -> gateway_pipeline::RequestCtx {
    ctx_with_body(NON_STREAM_BODY, candidate("c1"))
}

// ---------- replay ----------

/// A miss stores the settled answer; an identical second request replays it
/// without touching the upstream, and the replay settles exactly like the
/// original call. Bug it catches: a hit that skips settlement (replays would be
/// free — a billing hole), or a hit that settles under the wrong channel.
#[tokio::test]
async fn identical_request_replays_from_cache_and_settles() {
    let egress = Arc::new(ScriptedEgress::new(vec![(
        "",
        Plan::Ok {
            body: UPSTREAM_BODY,
        },
    )]));
    let sink = Arc::new(VecSink::default());
    let cache = cache_mount();
    let adaptors = Arc::new(gateway_protocol_bridge::format_codec::FormatRegistry::new());
    let stage = ForwardStage::new(egress.clone(), adaptors)
        .with_price_table(Arc::new(FlatPriceTable), sink.clone())
        .with_response_cache(cache);

    // First request: miss → upstream, settled, stored.
    let mut ctx1 = ctx_single_shot();
    let outcome = stage
        .handle(&mut ctx1)
        .await
        .expect("first forward succeeds");
    assert!(matches!(outcome, StageOutcome::Continue));
    assert_eq!(egress.calls().len(), 1, "miss goes upstream");
    let up1 = ctx1.upstream.expect("first response written");
    assert_eq!(up1.status, 200);

    // Second identical request: hit → replay, no upstream, settled from the
    // recorded counts and attribution.
    let mut ctx2 = ctx_single_shot();
    let outcome = stage.handle(&mut ctx2).await.expect("replay succeeds");
    assert!(matches!(outcome, StageOutcome::Continue));
    assert_eq!(
        egress.calls().len(),
        1,
        "the replay must not touch the upstream"
    );

    let up2 = ctx2.upstream.expect("replayed response written");
    assert_eq!(up2.status, up1.status);
    assert_eq!(
        up2.body, up1.body,
        "a replay is byte-identical to the original"
    );

    // Both the original call and the replay settle exactly once.
    let events = sink.events();
    assert_eq!(events.len(), 2, "original + replay both settle");
    let (first, replay) = (&events[0], &events[1]);
    assert_eq!(replay.prompt_tokens, first.prompt_tokens);
    assert_eq!(replay.completion_tokens, first.completion_tokens);
    assert_eq!(
        replay.cost, first.cost,
        "a replay is billed like the original"
    );
    assert_eq!(
        replay.channel_key, first.channel_key,
        "attribution names the channel that produced the content"
    );
    assert_eq!(replay.route_unit_key, first.route_unit_key);
    assert_eq!(replay.status_code, 200);
    assert_eq!(
        replay.finish_reason.as_deref(),
        first.finish_reason.as_deref()
    );
    assert!(!replay.is_stream);
}

/// A different user never replays another user's answer: the key is
/// isolation-scoped, so the second request goes upstream again. Bug it
/// catches: dropping user/group from the key — user A reading user B's
/// response is the worst possible cache bug on a billing plane.
#[tokio::test]
async fn different_user_never_replays() {
    let egress = Arc::new(ScriptedEgress::new(vec![(
        "",
        Plan::Ok {
            body: UPSTREAM_BODY,
        },
    )]));
    let cache = cache_mount();
    let adaptors = Arc::new(gateway_protocol_bridge::format_codec::FormatRegistry::new());
    let sink = Arc::new(VecSink::default());
    let stage = ForwardStage::new(egress.clone(), adaptors)
        .with_price_table(Arc::new(FlatPriceTable), sink)
        .with_response_cache(cache);

    let mut ctx1 = ctx_single_shot();
    stage
        .handle(&mut ctx1)
        .await
        .expect("first forward succeeds");

    let mut ctx2 = ctx_single_shot();
    ctx2.token.as_mut().unwrap().user_key = "user-b".to_string();
    stage
        .handle(&mut ctx2)
        .await
        .expect("second forward succeeds");

    assert_eq!(
        egress.calls().len(),
        2,
        "another user's identical request goes upstream again"
    );
}

// ---------- streaming bypass ----------

/// Streaming requests never interact with the cache: two identical streaming
/// requests both go upstream, and no entry is stored. Bug it catches: caching
/// an SSE byte stream (a replay would break SSE clients and double-settle on
/// the stream path).
#[tokio::test]
async fn streaming_requests_never_hit_cache() {
    let egress = Arc::new(ScriptedEgress::new(vec![(
        "",
        Plan::Ok {
            body: UPSTREAM_BODY,
        },
    )]));
    let cache = cache_mount();
    let adaptors = Arc::new(gateway_protocol_bridge::format_codec::FormatRegistry::new());
    let sink = Arc::new(VecSink::default());
    let stage = ForwardStage::new(egress.clone(), adaptors)
        .with_price_table(Arc::new(FlatPriceTable), sink)
        .with_response_cache(cache);

    let mut ctx1 = ctx_with_body(STREAM_BODY, candidate("c1"));
    let outcome = stage
        .handle(&mut ctx1)
        .await
        .expect("stream forward succeeds");
    assert!(
        matches!(outcome, StageOutcome::Stream(_)),
        "streaming intent returns a stream, got {outcome:?}"
    );

    let mut ctx2 = ctx_with_body(STREAM_BODY, candidate("c1"));
    let outcome = stage
        .handle(&mut ctx2)
        .await
        .expect("second stream succeeds");
    assert!(matches!(outcome, StageOutcome::Stream(_)));

    assert_eq!(
        egress.calls().len(),
        2,
        "streaming requests always go upstream"
    );
}

// ---------- regression: not mounted = status quo ----------

/// Without `with_response_cache`, identical requests all go upstream. Bug it
/// catches: an accidental default mount — the data plane must stay
/// byte-for-byte identical when the switch is off (the same invariant as the
/// sticky switch's rollback path).
#[tokio::test]
async fn unmounted_cache_keeps_status_quo() {
    let egress = Arc::new(ScriptedEgress::new(vec![(
        "",
        Plan::Ok {
            body: UPSTREAM_BODY,
        },
    )]));
    let adaptors = Arc::new(gateway_protocol_bridge::format_codec::FormatRegistry::new());
    let sink = Arc::new(VecSink::default());
    let stage = ForwardStage::new(egress.clone(), adaptors)
        .with_price_table(Arc::new(FlatPriceTable), sink);

    let mut ctx1 = ctx_single_shot();
    stage
        .handle(&mut ctx1)
        .await
        .expect("first forward succeeds");
    let mut ctx2 = ctx_single_shot();
    stage
        .handle(&mut ctx2)
        .await
        .expect("second forward succeeds");

    assert_eq!(
        egress.calls().len(),
        2,
        "no cache: every request goes upstream"
    );
}
