//! Assembly integration test for gate + dispatch + forward + protocol egress.
//!
//! Verifies that the shortest pipeline chain — GateChain (empty) → DispatchStage →
//! ForwardStage → ProtocolBridgeStage, wired into a Pipeline — lets one request run
//! through to completion and produce a response.
//!
//! A mock egress returns a fake upstream response; no real network request is made
//! (injection through the Egress trait).

use bytes::Bytes;
use gateway_core::records::{ChannelKey, ChannelRecord, RouteUnitRecord, SyncMeta};
use gateway_dispatch::stage::DispatchStage;
use gateway_forward::egress::{Egress, ForwardedResponse, Timeouts};
use gateway_forward::stage::ForwardStage;
use gateway_gate::concurrency::ConcurrencyState;
use gateway_pipeline::{BodySource, Pipeline, ProtocolKind, RequestMeta, StreamedAccum};
use gateway_protocol_bridge::stage::ProtocolBridgeStage;
use std::sync::Arc;

// ---------- mock egress ----------

/// Fake upstream: always returns 200 + a fixed JSON body.
struct MockEgress;

impl Egress for MockEgress {
    fn execute<'a>(
        &'a self,
        _url: &'a str,
        _headers: &'a [(String, String)],
        _body: Bytes,
        _timeouts: &'a Timeouts,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<ForwardedResponse, gateway_core::error::NormalizedError>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let stream = futures_util::stream::iter(vec![Ok::<Bytes, std::io::Error>(
                Bytes::from_static(b"{\"choices\":[]}"),
            )]);
            Ok(ForwardedResponse::from_stream(
                200,
                "application/json",
                None,
                stream,
            ))
        })
    }
}

// ---------- assembly ----------

fn mk_ctx() -> gateway_pipeline::RequestCtx {
    let meta = RequestMeta {
        method: "POST".to_string(),
        path: "/v1/chat/completions".to_string(),
        headers: http::HeaderMap::new(),
        body: BodySource::InMemory(Bytes::from_static(b"{\"model\":\"gpt-4o\"}")),
        client_ip: "127.0.0.1".parse().unwrap(),
        request_id: uuid::Uuid::now_v7(),
        inbound_protocol: ProtocolKind::OpenAI,
    };
    gateway_pipeline::RequestCtx {
        request: meta,
        token: Some(gateway_pipeline::TokenInfo {
            id: "tok-1".into(),
            user_key: "user-test".to_string(),
            group: "default".to_string(),
            enabled: true,
            allowed_models: None,
            auth_version: 1,
        }),
        requested_model: Some("gpt-4o".to_string()),
        route: None,
        selected_channel_key: None,
        selected_channel_name: None,
        session_hint: None,
        upstream: None,
        streamed: StreamedAccum::default(),
        error: None,
        drop_guards: Vec::new(),
        hold: None,
    }
}

fn mk_snapshot() -> Arc<gateway_dispatch::Snapshot> {
    let unit = RouteUnitRecord {
        meta: SyncMeta {
            key: "u1".into(),
            schema_version: 1,
            logical_version: 1,
            origin: "test".into(),
            updated_at: chrono::Utc::now(),
        },
        group: "default".into(),
        public_model: "gpt-4o".into(),
        channel_key: "ch1".into(),
        key_index: 0,
        upstream_model: "gpt-4o".into(),
        weight: 10,
        status: 1,
    };
    let channel = ChannelRecord {
        meta: SyncMeta {
            key: "ch1".into(),
            schema_version: 1,
            logical_version: 1,
            origin: "test".into(),
            updated_at: chrono::Utc::now(),
        },
        name: "ch1".into(),
        provider_type: "openai".into(),
        base_url: "https://upstream.example".into(),
        keys: vec![ChannelKey {
            index: 0,
            secret: "sk-test".into(),
            rpm_limit: 0,
        }],
        max_concurrency: 10,
        status: 1,
        groups: vec!["default".into()],
        settings: serde_json::Value::Null,
    };
    let mut channels = std::collections::HashMap::new();
    channels.insert("ch1".into(), channel);
    Arc::new(gateway_dispatch::Snapshot {
        units: vec![unit],
        channels,
    })
}

// ---------- tests ----------

#[tokio::test]
async fn full_chain_runs_to_protocol_bridge() {
    // Assemble the four-stage chain
    let gate = gateway_gate::chain::GateChain::new(); // empty gate → pass-through
    let dispatcher = Arc::new(gateway_dispatch::Dispatcher::new(
        Some(mk_snapshot()),
        Arc::new(gateway_dispatch::health::MemoryHealthTable::new()),
        Arc::new(ConcurrencyState::default()),
    ));
    let dispatch_stage = DispatchStage::new(dispatcher);
    let formats = Arc::new(gateway_protocol_bridge::format_codec::FormatRegistry::with_defaults());
    let forward_stage = ForwardStage::new(Arc::new(MockEgress), formats.clone());
    let bridge = ProtocolBridgeStage::new(formats);

    let pipe = Pipeline::new()
        .push(gate)
        .push(dispatch_stage)
        .push(forward_stage)
        .push(bridge);

    let ctx = mk_ctx();
    let result = pipe.run(ctx).await;
    let resp = result.expect("full chain should produce a response");
    assert_eq!(resp.status(), 200, "mock upstream 200 should pass through");

    // run must pack the channel selected by Dispatch into extensions before the
    // response is returned (covering both the streaming and non-streaming exits) —
    // the out-of-chain usage middleware reads channel attribution from here for
    // persistence.
    let attr = resp
        .extensions()
        .get::<gateway_pipeline::RouteAttribution>()
        .expect("run should bring channel attribution back in response extensions");
    assert_eq!(
        attr.channel_key, "ch1",
        "attribution key = matched channel UUID"
    );
    assert_eq!(
        attr.channel_name, "ch1",
        "attribution name = channel display name in snapshot"
    );
    assert_eq!(attr.model, "gpt-4o", "attribution model = public alias");
}

#[tokio::test]
async fn chain_stops_at_gate_on_rejection() {
    // Gate that rejects (AuthGate with an empty token snapshot → InvalidApiKey
    // short-circuits to 401)
    let tokens = Arc::new(arc_swap::ArcSwap::from_pointee(
        gateway_gate::snapshot::TokenSnapshot::default(),
    ));
    let auth = gateway_gate::auth::AuthGate::new(tokens);
    let gate = gateway_gate::chain::GateChain::new().push(auth);

    let pipe = Pipeline::new().push(gate);
    let ctx = mk_ctx();
    let resp = pipe.run(ctx).await.expect("gate short-circuit is Ok");
    assert_eq!(resp.status(), 401, "no key → AuthGate rejects");
}
