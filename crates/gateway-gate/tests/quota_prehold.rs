//! `gateway-gate` prehold regression tests — `QuotaGate` backed by the admission ledger.
//!
//! These pin the prehold wiring (`QuotaGate` + ledger + `GateChain` release on later-gate
//! rejection):
//!
//! 1. `quota_concurrent_overdraft_only_one_passes` — N requests against ONE token whose
//!    balance covers only ONE request's estimated cost: exactly one passes `QuotaGate`, the
//!    rest get `Rejection::InsufficientQuota` observing the drained ledger balance. The
//!    checks run sequentially (the atomicity is inside the ledger's mutex, so a sequential
//!    loop that re-reads the ledger is a faithful proxy for the concurrent case).
//! 2. `quota_hold_released_when_later_gate_rejects` — `QuotaGate` followed by a gate that
//!    always rejects: after rejection the ledger balance must be fully restored
//!    (available == original), i.e. the prehold does not stick.
use gateway_gate::snapshot::TokenView;
use std::net::IpAddr;
use std::sync::Arc;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use gateway_core::records::{SyncMeta, TokenRecord, UserRecord};
use gateway_metering::ledger::{BalanceLedger, MemoryLedger};
use gateway_pipeline::{BodySource, ProtocolKind, RequestCtx, RequestMeta, Stage, StageOutcome};
use http::HeaderMap;
use uuid::Uuid;

use gateway_gate::chain::{Gate, GateCtx};
use gateway_gate::snapshot::{
    IpPolicy, PriceRow, PricingSnapshot, QuotaSnapshot, TokenEntry, TokenSnapshot, UserSnapshot,
};
use gateway_gate::{AuthGate, GateChain, QuotaGate, Rejection, StateGate, sha256};

// ---------------------------------------------------------------------------
// helpers (mirror crates/gateway-gate/tests/gate.rs idioms)
// ---------------------------------------------------------------------------

fn meta(key: &str) -> SyncMeta {
    SyncMeta {
        key: key.to_string(),
        schema_version: 1,
        logical_version: 1,
        origin: "test".into(),
        updated_at: Utc::now(),
    }
}

fn user_record(key: &str, group: &str, enabled: bool) -> UserRecord {
    UserRecord {
        meta: meta(key),
        username: format!("u-{key}"),
        display_name: format!("User {key}"),
        email: format!("{key}@test.io"),
        quota: 100_000_000,
        used_quota: 0,
        request_count: 0,
        group: group.to_string(),
        status: if enabled { 1 } else { 2 },
        role: 1,
        created_at: Utc::now(),
    }
}

fn token_record(key: &str, user_key: &str, group: Option<&str>) -> TokenRecord {
    TokenRecord {
        meta: meta(key),
        user_key: user_key.to_string(),
        name: format!("tok-{key}"),
        key_hash: "abcd".into(),
        key_preview: "sk-ab****cd".into(),
        group: group.map(String::from),
        quota: 10_000_000,
        unlimited_quota: false,
        used_quota: 0,
        expires_at: None,
        status: 1,
    }
}

fn make_meta(headers: HeaderMap, body: Vec<u8>) -> RequestMeta {
    let client_ip: IpAddr = "10.0.0.5".parse().unwrap();
    RequestMeta {
        method: "POST".into(),
        path: "/v1/chat/completions".into(),
        headers,
        body: BodySource::InMemory(Bytes::from(body)),
        client_ip,
        request_id: Uuid::now_v7(),
        inbound_protocol: ProtocolKind::OpenAI,
    }
}

fn bearer_header(key: &str) -> HeaderMap {
    let mut h = HeaderMap::new();
    h.insert("authorization", format!("Bearer {key}").parse().unwrap());
    h
}

fn ctx_with_token(raw: &str, user_key: &str, snapshot: &TokenSnapshot) -> GateCtx {
    let hash = sha256(raw);
    let entry = snapshot.lookup(&hash).expect("token present in snapshot");
    let token = entry.record;
    let mut ctx = make_ctx(make_meta(bearer_header(raw), b"{}".to_vec()));
    ctx.raw_key = Some(raw.into());
    ctx.user_key = Some(user_key.into());
    ctx.token = Some(gateway_gate::TokenInfo {
        id: token.meta.key.clone(),
        user_key: "user-test".to_string(),
        user_id: 0,
        id_hash: hash,
        group: token.group().unwrap_or("").to_string(),
        enabled: token.is_enabled(),
        expires_at: token.expires_at_unix(),
        allowed_models: None,
        auth_version: token.auth_version(),
    });
    ctx
}

fn make_ctx(meta: RequestMeta) -> GateCtx {
    GateCtx {
        request_meta: meta,
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

fn quota_ctx(raw: &str, user_key: &str) -> GateCtx {
    let snapshot = TokenSnapshot::default();
    let hash = sha256(raw);
    snapshot.upsert(
        hash,
        TokenEntry::new(token_record("tok-1", user_key, None), None),
    );
    ctx_with_token(raw, user_key, &snapshot)
}

// ---------------------------------------------------------------------------
// 1. concurrent-overdraft regression
// ---------------------------------------------------------------------------

/// One token whose balance covers exactly one request's estimated cost (`15_360` =
/// `ceil(15_000_000` * 1024 / 1e6)). Five sequential admission checks against the shared
/// ledger: exactly one passes, four are rejected with `Rejection::InsufficientQuota` whose
/// `remaining` is the drained (0) ledger balance — the pre-wiring bug let every request
/// observe the same snapshot `remaining` and overdraft.
#[tokio::test]
async fn quota_concurrent_overdraft_only_one_passes() {
    let pricing = PricingSnapshot::default();
    pricing.upsert(
        "gpt-4o".into(),
        "default".into(),
        PriceRow {
            input_per_m: 0.0,
            output_per_m: 15_000_000.0,
            cache_per_m: 0.0,
        },
    );
    let cost: i64 = 15_360; // ceil(15_000_000 * 1024 / 1_000_000)
    let quotas = QuotaSnapshot::default();
    // Snapshot `remaining` still covers every request (stale fast filter); the ledger is
    // authoritative — only the first prehold can succeed.
    quotas.upsert("tok-1".into(), cost);

    let ledger = Arc::new(MemoryLedger::new());
    ledger.set_balance("user-test", "tok-1", cost);
    let gate = QuotaGate::new(
        Arc::new(ArcSwap::from_pointee(quotas)),
        Arc::new(ArcSwap::from_pointee(pricing)),
        ledger.clone(),
    );

    let mut passes = 0;
    let mut failures = 0;
    for _ in 0..5 {
        let mut ctx = quota_ctx("sk-tok-1", "user-test");
        ctx.requested_model = Some("gpt-4o".into());
        ctx.requested_max_tokens = Some(1024);
        match gate.check(&mut ctx).await {
            Ok(()) => {
                passes += 1;
                assert_eq!(
                    ctx.estimated_cost,
                    Some(cost),
                    "admission prices the request at one estimate"
                );
                assert_eq!(
                    ctx.hold.as_ref().map(|h| h.amount),
                    Some(cost),
                    "the passing request holds the full estimate"
                );
            }
            Err(Rejection::InsufficientQuota {
                remaining,
                cost: rejected_cost,
            }) => {
                assert_eq!(
                    remaining, 0,
                    "rejection must observe the ledger balance drained by the first prehold"
                );
                assert_eq!(
                    rejected_cost, cost,
                    "rejection prices at the same estimate as the passing request"
                );
                failures += 1;
            }
            Err(other) => panic!("unexpected rejection: {other:?}"),
        }
    }

    assert_eq!(
        passes, 1,
        "only one request may prehold against a balance covering one"
    );
    assert_eq!(failures, 4);
    assert_eq!(
        ledger.available("user-test", "tok-1"),
        0,
        "ledger balance fully drained: the concurrent-overdraft hole is closed"
    );
}

// ---------------------------------------------------------------------------
// 2. hold released when a LATER gate rejects
// ---------------------------------------------------------------------------

/// A gate that always rejects, used as the later gate in the chain.
struct AlwaysReject;

#[async_trait]
impl Gate for AlwaysReject {
    fn name(&self) -> &'static str {
        "always_reject"
    }

    async fn check(&self, _ctx: &mut GateCtx) -> Result<(), Rejection> {
        Err(Rejection::RateLimited)
    }
}

/// `QuotaGate` preholds the estimate, then `AlwaysReject` rejects: the chain must release
/// the prehold via the shared ledger, so the balance is fully restored.
#[tokio::test]
async fn quota_hold_released_when_later_gate_rejects() {
    let raw = "sk-chain-1";
    let hash = sha256(raw);
    let cost: i64 = 3840; // ceil(15_000_000 * 256 / 1_000_000)

    let tokens = Arc::new(ArcSwap::from_pointee(TokenSnapshot::default()));

    tokens.load().upsert(
        hash,
        TokenEntry::new(token_record("1", "u1", Some("default")), None),
    );
    let users = Arc::new(ArcSwap::from_pointee(UserSnapshot::default()));
    users.load().upsert(user_record("u1", "default", true));

    let pricing = Arc::new(ArcSwap::from_pointee(PricingSnapshot::default()));
    pricing.load().upsert(
        "gpt-4o".into(),
        "default".into(),
        PriceRow {
            input_per_m: 0.0,
            output_per_m: 15_000_000.0,
            cache_per_m: 0.0,
        },
    );

    let quotas = Arc::new(ArcSwap::from_pointee(QuotaSnapshot::default()));
    quotas.load().upsert("1".into(), cost);

    let ip_policy = Arc::new(ArcSwap::from_pointee(IpPolicy::allows_everyone()));

    // One ledger shared by QuotaGate (takes the prehold) and the chain (releases it when a
    // later gate rejects).
    let ledger = Arc::new(MemoryLedger::new());
    ledger.set_balance("u1", "1", cost);

    let chain = GateChain::new()
        .push(AuthGate::new(tokens))
        .push(StateGate::new(users, ip_policy))
        .push(QuotaGate::new(quotas, pricing, ledger.clone()))
        .push(AlwaysReject)
        .with_ledger(ledger.clone());

    let mut headers = HeaderMap::new();
    headers.insert("authorization", format!("Bearer {raw}").parse().unwrap());
    let req = make_meta(headers, br#"{"model":"gpt-4o","max_tokens":256}"#.to_vec());
    let mut ctx = RequestCtx::new(req);

    let outcome = chain.handle(&mut ctx).await.expect("stage ok");
    let resp = match outcome {
        StageOutcome::ShortCircuit(resp) => resp,
        other => {
            panic!("expected ShortCircuit from the rejecting later gate, got {other:?}")
        }
    };
    assert_eq!(
        resp.status(),
        429,
        "RateLimited from the later gate maps to HTTP 429"
    );

    assert_eq!(
        ledger.available("u1", "1"),
        cost,
        "hold released when a later gate rejects: available balance fully restored"
    );
}
