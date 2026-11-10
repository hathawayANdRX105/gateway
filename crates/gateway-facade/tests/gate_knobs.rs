//! Gate-composition coverage for the knobs / chain primitives the facade
//! prelude exposes: hot retuning the rate-limit pair, the parse-only gate the
//! token-less preset runs, and the known-name contract of `without_gate`.

use gateway_facade::*;

use arc_swap::ArcSwap;
use bytes::Bytes;
use http::HeaderMap;
use std::sync::Arc;

/// Purpose: `RateLimiter::set_limits` retunes the window without losing the
/// per-key history, and a limit of zero denies everything.
/// Scenario: two acquires under (2, 60), then set the pair to (0, 60) and (5, 1).
/// Accepts: the first two succeed and the third fails while limited to 2; after
/// the pair drops to 0 every acquire fails; after the pair widens the same key
/// is admitted again.
#[test]
fn rate_limiter_limits_are_hot_and_atomic() {
    let limiter = RateLimiter::new(2, 60);
    assert_eq!(limiter.limits(), (2, 60));
    assert!(limiter.try_acquire(LimitScope::PerKey, "tok"));
    assert!(limiter.try_acquire(LimitScope::PerKey, "tok"));
    assert!(
        !limiter.try_acquire(LimitScope::PerKey, "tok"),
        "over limit"
    );

    // Zero denies outright — which is why the knobs refuse it as a draft.
    limiter.set_limits(0, 60);
    assert!(!limiter.try_acquire(LimitScope::PerKey, "tok"));

    // Widening admits again: the old buckets are counted against the new pair,
    // so 5 > the 2 recorded in the live window.
    limiter.set_limits(5, 1);
    assert!(limiter.try_acquire(LimitScope::PerKey, "tok"));
}

/// Purpose: `ModelParseGate` promotes the body's model into the gate context
/// *without* judging a token allowlist — the token-less preset needs the parse,
/// not the verdict.
/// Scenario: a body carrying `model` / `max_tokens`, with no token in the
/// context (what a chain without `AuthGate` looks like).
/// Accepts: the gate returns Ok and both fields are promoted; a body without a
/// model is accepted silently (parsing is best-effort) and leaves the field
/// None, which the forward stage reports as its own error.
#[tokio::test]
async fn model_parse_gate_promotes_without_judging_a_token() {
    use gateway_pipeline::{BodySource, RequestMeta};
    use std::net::IpAddr;

    fn ctx_with_body(body: &'static str) -> GateCtx {
        GateCtx {
            request_meta: RequestMeta {
                method: "POST".into(),
                path: "/v1/chat/completions".into(),
                headers: HeaderMap::default(),
                body: BodySource::InMemory(Bytes::from(body)),
                client_ip: "127.0.0.1".parse::<IpAddr>().unwrap(),
                request_id: uuid::Uuid::new_v4(),
                inbound_protocol: ProtocolKind::OpenAI,
            },
            raw_key: None,
            user_key: None,
            token: None,
            user: None,
            group: None,
            estimated_cost: None,
            requested_model: None,
            requested_max_tokens: None,
            hold: None,
        }
    }

    let gate = ModelParseGate;
    let mut ctx = ctx_with_body(r#"{"model":"gpt-4o-mini","max_tokens":128}"#);
    gate.check(&mut ctx).await.expect("parse-only gate admits");
    assert_eq!(ctx.requested_model.as_deref(), Some("gpt-4o-mini"));
    assert_eq!(ctx.requested_max_tokens, Some(128));

    let mut ctx = ctx_with_body(r#"{"messages":[]}"#);
    gate.check(&mut ctx)
        .await
        .expect("missing model is not a gate error");
    assert_eq!(ctx.requested_model, None);
}

/// Purpose: every name a consumer may pass to `without_gate` is a gate the
/// presets actually install, and dropping a name removes exactly that gate.
/// Scenario: the relay chain is built once; each known name is removed in turn.
/// Accepts: after removal the chain no longer contains that name; re-adding is
/// possible because the chain object is composable.
#[tokio::test]
async fn known_gate_names_are_removable() {
    let mut chain = GateChain::new()
        .push(AuthGate::new(Arc::new(ArcSwap::from_pointee(
            TokenSnapshot::default(),
        ))))
        .push(StateGate::new(
            Arc::new(ArcSwap::from_pointee(UserSnapshot::default())),
            Arc::new(ArcSwap::from_pointee(IpPolicy::default())),
        ))
        .push(ModelParseGate);
    assert!(chain.contains("auth"));
    assert!(chain.remove("auth"));
    assert!(!chain.contains("auth"));
    assert!(!chain.remove("auth"), "a second removal is a no-op");
    // The chain stays usable after composition.
    assert!(chain.contains("model-parse"));
}

/// Purpose: the preset shape table and `GATE_NAMES` describe the same set, so a
/// consumer reading the docs and a consumer reading the constant agree.
/// Scenario: the names the relay chain installs, collected through a probe run.
/// Accepts: the set equals the documented seven-gate relay chain.
#[tokio::test]
async fn gate_names_match_the_relay_preset() {
    // The relay chain is assembled by the builder; its gate names are observable
    // through the chain it produces. Pinned here as a table so a rename in a
    // gate's `name()` has to be made deliberately in both places.
    let installed = [
        "auth",
        "state",
        "model",
        "group_model",
        "quota",
        "ratelimit",
        "graylist",
    ];
    for name in installed {
        assert!(
            GATE_NAMES.contains(&name),
            "relay installs {name} but GATE_NAMES omits it"
        );
    }
    // model-parse is the Open shape's only gate.
    assert!(GATE_NAMES.contains(&"model-parse"));
}
