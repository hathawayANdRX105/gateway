//! Insertion-point semantics of [`gateway_facade::GatewayBuilder`]: a consumer stage
//! added with `with_stage_before_forward` runs after the gate chain and before
//! the forwarding stage; a stage added with `with_stage_after_chain` runs only
//! when the whole default chain completes. The token / user snapshots carry one
//! enabled pair so the gates admit the request; the empty dispatch catalog makes
//! the forwarding stage fail, which is exactly the precondition the assertions
//! need (the pre-forward stage ran, the after-chain stage did not).

use std::sync::Arc;
use std::sync::Mutex;

use arc_swap::ArcSwap;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

// Single-entry discipline: this test names `gateway_facade` and nothing else —
// the vocabulary and the engine types it touches arrive through the re-exports.
use gateway_facade::*;

/// The only egress in this test: never called — selection fails on the empty
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

/// Records its tag into a shared log when (and only when) the chain reaches it.
struct Recorder {
    tag: &'static str,
    log: Arc<Mutex<Vec<&'static str>>>,
}

impl Recorder {
    fn new(tag: &'static str, log: Arc<Mutex<Vec<&'static str>>>) -> Self {
        Self { tag, log }
    }
}

#[async_trait]
impl Stage for Recorder {
    fn name(&self) -> &'static str {
        self.tag
    }

    async fn handle(&self, _ctx: &mut RequestCtx) -> Result<StageOutcome, StageError> {
        self.log.lock().unwrap().push(self.tag);
        Ok(StageOutcome::Continue)
    }
}

const TOKEN_KEY: &str = "sk-facade-test";
const USER_KEY: &str = "user-facade-test";

/// Consumer-loaded values for a minimal relay: one enabled token (hash-keyed) and
/// its user, empty group / dispatch catalogs.
/// Fixture envelope version: record identity is assigned by the consumer, so gateway
/// tests only need a stable placeholder.
const FIXTURE_SCHEMA_VERSION: u32 = 1;

fn facade_context() -> Context {
    let token_snapshot: SharedTokenSnapshot =
        Arc::new(ArcSwap::from(Arc::new(TokenSnapshot::default())));
    token_snapshot.load().upsert(
        sha256(TOKEN_KEY),
        TokenEntry::new(
            TokenRecord {
                meta: SyncMeta {
                    key: "tok-facade-test".into(),
                    schema_version: FIXTURE_SCHEMA_VERSION,
                    logical_version: 1,
                    origin: "test".into(),
                    updated_at: chrono::Utc::now(),
                },
                user_key: USER_KEY.into(),
                name: "facade-test".into(),
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
    let user_snapshot: SharedUserSnapshot =
        Arc::new(ArcSwap::from(Arc::new(UserSnapshot::default())));
    user_snapshot.load().upsert(UserRecord {
        meta: SyncMeta {
            key: USER_KEY.into(),
            schema_version: FIXTURE_SCHEMA_VERSION,
            logical_version: 1,
            origin: "test".into(),
            updated_at: chrono::Utc::now(),
        },
        username: "facade".into(),
        display_name: "Facade Test".into(),
        email: String::new(),
        quota: 0,
        used_quota: 0,
        request_count: 0,
        group: "default".into(),
        status: 1,
        role: 1,
        created_at: chrono::Utc::now(),
    });
    let group_snapshot: SharedGroupSnapshot =
        Arc::new(ArcSwap::from(Arc::new(GroupSnapshot::default())));

    Context {
        config: Config {
            health: HealthSetting::default(),
            retry_policy: RetryPolicy::default(),
            timeouts: Timeouts::default(),
            digest_enabled: false,
            sticky_enabled: false,
            sticky_boost: 1.0,
            token_tpm_default: 0,
            ratelimit_max_requests: 1_000,
            ratelimit_window_seconds: 60,
            global_max_concurrency: 8,
            response_cache: Some(ResponseCacheTuning {
                ttl_secs: 300,
                max_total_bytes: 1 << 20,
                max_entry_bytes: 1 << 18,
            }),
            ..Config::default()
        },
        components: Components {
            egress: Arc::new(UnreachableEgress),
            wire_proxy_pool: false,
            proxies: Arc::new(ProxyManager::new()),
            proxy_snapshot: ProxySnapshot::default(),
            state: State {
                dispatch: Snapshot::default(),
                token_snapshot,
                user_snapshot,
                group_snapshot,
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
        .body(Body::from(r#"{"model":"gpt-4o"}"#))
        .unwrap()
}

/// A stage inserted before forward runs once the gates admit the request; the
/// after-chain stage does not run because the forwarding stage fails first
/// (empty catalog). This pins the documented slot positions.
#[tokio::test]
async fn inserted_stages_run_at_their_documented_slots() {
    let log: Arc<Mutex<Vec<&'static str>>> = Arc::new(Mutex::new(Vec::new()));
    let app = GatewayBuilder::relay(facade_context())
        .with_stage_before_forward(Recorder::new("pre_forward", log.clone()))
        .with_stage_after_chain(Recorder::new("after_chain", log.clone()))
        .build();

    let resp = app
        .router()
        .oneshot(request())
        .await
        .expect("router responds");

    assert_eq!(
        log.lock().unwrap().as_slice(),
        &["pre_forward"],
        "the before-forward stage ran, the after-chain stage did not: the chain stopped at the failing forwarding stage"
    );
    assert!(
        resp.status().is_client_error() || resp.status().is_server_error(),
        "forwarding the empty catalog fails, so the request surfaces an error"
    );
    assert_ne!(resp.status(), StatusCode::OK);
}

/// The default chain is gates → forwarding stage → protocol bridge; each custom
/// stage adds one link. This pins the chain shape independent of execution.
#[tokio::test]
async fn default_chain_has_three_stages_plus_insertions() {
    let app = GatewayBuilder::relay(facade_context()).build();
    assert_eq!(
        app.pipeline().len(),
        3,
        "gate chain + forwarding + protocol bridge"
    );

    let app = GatewayBuilder::relay(facade_context())
        .with_stage_before_forward(Recorder::new("pre", Arc::new(Mutex::new(Vec::new()))))
        .with_stage_after_chain(Recorder::new("post", Arc::new(Mutex::new(Vec::new()))))
        .build();
    assert_eq!(
        app.pipeline().len(),
        5,
        "two inserted stages extend the chain"
    );
}

/// Non-data-plane paths get the scoped 404 from the path guard, never the
/// `AuthGate`'s 401.
#[tokio::test]
async fn path_guard_serves_only_data_plane_paths() {
    let app = GatewayBuilder::relay(facade_context()).build();
    let resp = app
        .router()
        .oneshot(
            Request::builder()
                .uri("/api/anything")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("router responds");
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}
