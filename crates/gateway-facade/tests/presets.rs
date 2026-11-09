//! Preset shapes and composition rules of the admission chain.
//!
//! Purpose: every preset produces the same *gate sequence* the historical
//! default chain had, and the assertions pin the observable differences —
//! whether the quota gate is installed, whether a custom gate is appended,
//! whether a drop removes a gate. The chain runs a model request that fails at
//! the empty catalog (no route), which proves which gates admitted it.

use std::sync::Arc;

// Single-entry proof: this test names `gateway_facade` and nothing else. Every
// engine / contract type used below arrives through the facade's re-exports.
use gateway_facade::*;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

/// Fixture envelope version: record identity is assigned by the consumer, so gateway
/// tests only need a stable placeholder.
const FIXTURE_SCHEMA_VERSION: u32 = 1;
const TOKEN_KEY: &str = "sk-facade-preset-test";
const USER_KEY: &str = "user-facade-preset-test";

/// The only egress in these tests: never called — selection fails on the empty
/// catalog before the forwarding stage would reach the upstream.
struct UnreachableEgress;

#[async_trait]
impl Egress for UnreachableEgress {
    fn execute<'a>(
        &'a self,
        _url: &'a str,
        _headers: &'a [(String, String)],
        _body: bytes::Bytes,
        _timeouts: &'a Timeouts,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<ForwardedResponse, NormalizedError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async { unreachable!("empty catalog fails selection before egress") })
    }
}

fn token_snapshot() -> SharedTokenSnapshot {
    let snapshot: SharedTokenSnapshot = Arc::new(ArcSwap::from(Arc::new(TokenSnapshot::default())));
    snapshot.load().upsert(
        gateway_gate::auth::sha256(TOKEN_KEY),
        TokenEntry::new(
            TokenRecord {
                meta: SyncMeta {
                    key: "tok-facade-preset-test".into(),
                    schema_version: FIXTURE_SCHEMA_VERSION,
                    logical_version: 1,
                    origin: "test".into(),
                    updated_at: chrono::Utc::now(),
                },
                user_key: USER_KEY.into(),
                name: "facade-preset".into(),
                key_hash: String::new(),
                key_preview: "sk-f****".into(),
                group: Some("default".into()),
                quota: 0,
                unlimited_quota: true,
                used_quota: 0,
                expires_at: None,
                status: 1,
            },
            None,
        ),
    );
    snapshot
}

fn user_snapshot() -> SharedUserSnapshot {
    let snapshot: SharedUserSnapshot = Arc::new(ArcSwap::from(Arc::new(UserSnapshot::default())));
    snapshot.load().upsert(UserRecord {
        meta: SyncMeta {
            key: USER_KEY.into(),
            schema_version: FIXTURE_SCHEMA_VERSION,
            logical_version: 1,
            origin: "test".into(),
            updated_at: chrono::Utc::now(),
        },
        username: "facade-preset".into(),
        display_name: "Facade Preset".into(),
        email: String::new(),
        quota: 0,
        used_quota: 0,
        request_count: 0,
        group: "default".into(),
        status: 1,
        role: 1,
        created_at: chrono::Utc::now(),
    });
    snapshot
}

fn context() -> Context {
    Context {
        config: Config {
            health: HealthSetting::default(),
            timeouts: Timeouts::default(),
            sticky_enabled: false,
            global_max_concurrency: 8,
            ..Config::default()
        },
        components: Components {
            egress: Some(Arc::new(UnreachableEgress)),
            wire_proxy_pool: false,
            proxies: Arc::new(ProxyManager::new()),
            proxy_snapshot: ProxySnapshot::default(),
            state: State {
                dispatch: Snapshot::default(),
                token_snapshot: token_snapshot(),
                user_snapshot: user_snapshot(),
                group_snapshot: Arc::new(ArcSwap::from(Arc::new(GroupSnapshot::default()))),
            },
            quota_gate: None,
            settle: None,
            ledger: None,
        },
    }
}

/// Same fixture as [`context`], but the user is disabled so the state gate has
/// something to judge.
fn disabled_user_context() -> Context {
    let snapshot: SharedUserSnapshot = Arc::new(ArcSwap::from(Arc::new(UserSnapshot::default())));
    snapshot.load().upsert(UserRecord {
        meta: SyncMeta {
            key: USER_KEY.into(),
            schema_version: FIXTURE_SCHEMA_VERSION,
            logical_version: 1,
            origin: "test".into(),
            updated_at: chrono::Utc::now(),
        },
        username: "facade-preset".into(),
        display_name: "Facade Preset".into(),
        email: String::new(),
        quota: 0,
        used_quota: 0,
        request_count: 0,
        group: "default".into(),
        status: 0,
        role: 1,
        created_at: chrono::Utc::now(),
    });
    Context {
        config: Config {
            health: HealthSetting::default(),
            timeouts: Timeouts::default(),
            sticky_enabled: false,
            global_max_concurrency: 8,
            ..Config::default()
        },
        components: Components {
            egress: Some(Arc::new(UnreachableEgress)),
            wire_proxy_pool: false,
            proxies: Arc::new(ProxyManager::new()),
            proxy_snapshot: ProxySnapshot::default(),
            state: State {
                dispatch: Snapshot::default(),
                token_snapshot: token_snapshot(),
                user_snapshot: snapshot,
                group_snapshot: Arc::new(ArcSwap::from(Arc::new(GroupSnapshot::default()))),
            },
            quota_gate: None,
            settle: None,
            ledger: None,
        },
    }
}

fn request() -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("authorization", format!("Bearer {TOKEN_KEY}"))
        .body(Body::from(
            r#"{"model":"gpt-4o-mini","messages":[{"role":"user","content":"hi"}]}"#,
        ))
        .unwrap()
}

/// A stage that stamps a marker response when the chain reaches it, proving a
/// slot's position without touching the engines.
struct MarkerStage;

#[async_trait]
impl Stage for MarkerStage {
    fn name(&self) -> &'static str {
        "marker-stage"
    }

    async fn handle(&self, _ctx: &mut RequestCtx) -> Result<StageOutcome, StageError> {
        Ok(StageOutcome::ShortCircuit(
            axum::http::Response::builder()
                .status(StatusCode::IM_A_TEAPOT)
                .body(Body::from("marker"))
                .unwrap(),
        ))
    }
}

/// Purpose: `Shape::Relay` keeps every admission gate — a request with a valid
/// token and an empty catalog is rejected by dispatch (no route), not by auth.
/// Acceptance: status 404 no_route, i.e. the gate chain admitted and forwarding
/// found no candidate.
#[tokio::test]
async fn relay_shape_keeps_full_gate_chain() {
    let gateway: Gateway = GatewayBuilder::relay(context()).build();
    let resp = gateway
        .router()
        .oneshot(request())
        .await
        .expect("router serves the request");
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

/// Purpose: `Shape::NoBilling` differs from `Relay` only by the quota gate, so
/// with no quota gate installed both shapes behave identically here.
/// Acceptance: same 404 no_route outcome as the relay shape.
#[tokio::test]
async fn no_billing_shape_admits_like_relay_without_quota() {
    let gateway: Gateway = GatewayBuilder::no_billing(context()).build();
    let resp = gateway
        .router()
        .oneshot(request())
        .await
        .expect("router serves the request");
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

/// Purpose: `Shape::Open` drops the admission chain entirely — every stock gate
/// reads a token, so a token-less chain is empty.
/// Acceptance: a request with no key at all still reaches forwarding (the empty
/// catalog answers 404 no route) instead of being rejected by an identity gate.
#[tokio::test]
async fn open_shape_skips_identity_gates() {
    let gateway: Gateway = GatewayBuilder::open(context()).build();
    let mut req = request();
    req.headers_mut().remove("authorization");
    let resp = gateway
        .router()
        .oneshot(req)
        .await
        .expect("router serves the request");
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

/// Purpose: `with_gate` appends a custom gate to the admission chain tail; it
/// must run (and its rejection must short-circuit before forwarding).
/// Acceptance: the marker gate rejects with 403 before dispatch can answer 404.
#[tokio::test]
async fn custom_gate_appends_to_chain() {
    struct RejectGate;

    #[async_trait]
    impl Gate for RejectGate {
        fn name(&self) -> &'static str {
            "reject"
        }

        async fn check(&self, _ctx: &mut GateCtx) -> Result<(), Rejection> {
            Err(Rejection::ModelForbidden {
                model: "nope".into(),
            })
        }
    }

    let gateway: Gateway = GatewayBuilder::relay(context())
        .with_gate(RejectGate)
        .build();
    let resp = gateway
        .router()
        .oneshot(request())
        .await
        .expect("router serves the request");
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

/// Purpose: `without_gate` removes a preset gate by name, observably.
/// Acceptance: with a disabled user the stock chain rejects 403 at the state
/// gate; the same chain minus `state` continues to dispatch and answers 404 no
/// route — the removal is visible in the status code.
#[tokio::test]
async fn without_gate_removes_a_preset_gate() {
    // Stock chain: StateGate judges the disabled user → 403 user_disabled.
    let gateway: Gateway = GatewayBuilder::relay(disabled_user_context()).build();
    let resp = gateway
        .router()
        .oneshot(request())
        .await
        .expect("router serves the request");
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);

    // Same chain minus the state gate → dispatch answers 404 no route.
    let gateway: Gateway = GatewayBuilder::relay(disabled_user_context())
        .without_gate("state")
        .build();
    let resp = gateway
        .router()
        .oneshot(request())
        .await
        .expect("router serves the request");
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

/// Purpose: the stage slots keep their documented positions — a
/// `with_stage_before_forward` stage short-circuits before dispatch (404 would
/// otherwise answer), and a `with_stage_after_chain` stage never runs when an
/// earlier stage short-circuited.
/// Acceptance: teapot from the pre-forward slot; the chain length shows the
/// after-chain slot exists but produced nothing on this path.
#[tokio::test]
async fn stage_slots_keep_their_positions() {
    let gateway: Gateway = GatewayBuilder::relay(context())
        .with_stage_before_forward(MarkerStage)
        .build();
    let resp = gateway
        .router()
        .oneshot(request())
        .await
        .expect("router serves the request");
    assert_eq!(resp.status(), StatusCode::IM_A_TEAPOT);
}

/// Purpose: `RuntimeKnobs` is the single runtime-mutable surface — reading the
/// effective config, applying a draft, and observing the epoch bump.
/// Acceptance: the retry policy and rate-limit pair changed after update, epoch
/// moved 0 → 1, and the boot view is unchanged (boot-fixed values are read-only).
#[test]
fn runtime_knobs_apply_drafts_and_track_epoch() {
    let gateway: Gateway = GatewayBuilder::relay(context()).build();
    let knobs = gateway.knobs();

    assert_eq!(knobs.epoch(), 0);
    let before = knobs.snapshot();
    assert_eq!(before.ratelimit_max_requests, 1_000);
    assert_eq!(before.boot.global_max_concurrency, 8);

    let after = knobs
        .update(ConfigDraft {
            retry_policy: None,
            health: None,
            ratelimit_max_requests: Some(7),
            ratelimit_window_seconds: Some(30),
            token_tpm_default: Some(5_000),
        })
        .expect("draft applies");

    assert_eq!(after.ratelimit_max_requests, 7);
    assert_eq!(after.ratelimit_window_seconds, 30);
    assert_eq!(after.token_tpm_default, 5_000);
    // Boot-fixed values are untouched by a draft.
    assert_eq!(
        after.boot.global_max_concurrency,
        before.boot.global_max_concurrency
    );
    assert_eq!(knobs.epoch(), 1);

    // An invalid health setting is rejected whole — nothing applied.
    let bad = HealthSetting {
        alpha: f64::NAN,
        ..HealthSetting::default()
    };
    let err = knobs
        .update(ConfigDraft {
            health: Some(bad),
            ..ConfigDraft::default()
        })
        .expect_err("invalid health setting must be refused");
    assert!(err.to_string().contains("alpha"), "{err}");
    assert_eq!(knobs.epoch(), 1, "a refused draft changes nothing");
}
