//! `gateway-gate` group-level model gate integration tests — GroupSnapshot queries +
//! GroupModelGate decisions
//!
//! Covers: group allowlist hit / wildcard hit, rejection on miss, fail-open when
//! unconfigured, whole-group rejection for a disabled group, missing model, snapshot query
//! fallbacks, and the HTTP mapping of the new Rejection variants (403 + code).

use std::net::IpAddr;
use std::sync::Arc;

use arc_swap::ArcSwap;
use bytes::Bytes;
use gateway_gate::chain::GateCtx;
use gateway_gate::snapshot::{GroupEntry, GroupSnapshot};
use gateway_gate::{Gate, GroupModelGate, Rejection, SharedGroupSnapshot, rejection_to_response};
use gateway_pipeline::{BodySource, ProtocolKind, RequestMeta};
use http::HeaderMap;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// Build the test snapshot: "vip" allows only `gpt-4*` (multiplier 2.5), "public" has an
/// empty allowlist (unrestricted models).
fn group_snapshot() -> SharedGroupSnapshot {
    let mut groups = GroupSnapshot::default();
    groups.upsert(
        "vip".into(),
        GroupEntry {
            allowed_models: vec!["gpt-4*".into()],
            multiplier: 2.5,
            enabled: true,
        },
    );
    groups.upsert(
        "public".into(),
        GroupEntry {
            allowed_models: vec![],
            multiplier: 1.0,
            enabled: true,
        },
    );
    Arc::new(ArcSwap::from_pointee(groups))
}

/// Minimal GateCtx: only the group / requested_model input fields matter.
fn ctx(group: Option<&str>, model: Option<&str>) -> GateCtx {
    let client_ip: IpAddr = "10.0.0.5".parse().unwrap();
    GateCtx {
        request_meta: RequestMeta {
            method: "POST".into(),
            path: "/v1/chat/completions".into(),
            headers: HeaderMap::new(),
            body: BodySource::InMemory(Bytes::from_static(b"{}")),
            client_ip,
            request_id: Uuid::now_v7(),
            inbound_protocol: ProtocolKind::OpenAI,
        },
        raw_key: None,
        user_key: None,
        token: None,
        user: None,
        group: group.map(str::to_string),
        estimated_cost: None,
        requested_model: model.map(str::to_string),
        requested_max_tokens: None,
        hold: None,
    }
}

// ---------------------------------------------------------------------------
// GroupModelGate::check
// ---------------------------------------------------------------------------

#[tokio::test]
async fn vip_group_allows_wildcard_match() {
    let gate = GroupModelGate::new(group_snapshot());
    // `gpt-4*` wildcard matches gpt-4o → allow
    let mut c = ctx(Some("vip"), Some("gpt-4o"));
    gate.check(&mut c)
        .await
        .expect("gpt-4o allowed via wildcard");
}

#[tokio::test]
async fn vip_group_rejects_non_matching_model() {
    let gate = GroupModelGate::new(group_snapshot());
    let mut c = ctx(Some("vip"), Some("claude-x"));
    let r = gate.check(&mut c).await.unwrap_err();
    // The rejection body should carry the concrete model + group for troubleshooting.
    assert!(
        matches!(
            r,
            Rejection::ModelNotAllowedForGroup { ref model, ref group }
                if model == "claude-x" && group == "vip"
        ),
        "unexpected rejection: {r:?}"
    );
}

#[tokio::test]
async fn empty_whitelist_group_fails_open() {
    let gate = GroupModelGate::new(group_snapshot());
    // "public" empty allowlist = the group does not restrict models
    let mut c = ctx(Some("public"), Some("anything-at-all"));
    gate.check(&mut c)
        .await
        .expect("empty whitelist = unrestricted");
}

#[tokio::test]
async fn unknown_group_and_missing_group_fail_open() {
    let gate = GroupModelGate::new(group_snapshot());
    // Group absent (unconfigured) → fail-open
    let mut c = ctx(Some("no-such-group"), Some("claude-x"));
    gate.check(&mut c).await.expect("unknown group fail-open");
    // ctx without group → fail-open as well
    let mut c = ctx(None, Some("claude-x"));
    gate.check(&mut c).await.expect("no group fail-open");
    // An empty-string group counts as unconfigured (aligned with chain.rs's group
    // promotion semantics)
    let mut c = ctx(Some(""), Some("claude-x"));
    gate.check(&mut c).await.expect("empty group fail-open");
}

#[tokio::test]
async fn disabled_group_rejects_even_with_open_whitelist() {
    // Contract (migration 0001 api_groups.status comment): a disabled group with status≠1 →
    // the gate rejects the whole group.
    let mut groups = GroupSnapshot::default();
    groups.upsert(
        "blocked".into(),
        GroupEntry {
            allowed_models: vec![],
            multiplier: 1.0,
            enabled: false,
        },
    );
    // Premise check: this group's allowlist is empty = by allowlist alone any model would
    // pass, so the rejection can only come from the disabled bit (not
    // ModelNotAllowedForGroup).
    assert_eq!(
        groups.allowed_models("blocked").map(<[String]>::len),
        Some(0),
        "a disabled group's whitelist must be empty; the rejection must come from the disabled bit"
    );
    assert!(groups.is_disabled("blocked"));

    let gate = GroupModelGate::new(Arc::new(ArcSwap::from_pointee(groups)));
    let mut c = ctx(Some("blocked"), Some("any-model-at-all"));
    let r = gate.check(&mut c).await.unwrap_err();
    assert!(
        matches!(&r, Rejection::GroupDisabled { group } if group == "blocked"),
        "unexpected rejection: {r:?}"
    );
}

#[tokio::test]
async fn missing_requested_model_is_model_not_specified() {
    let gate = GroupModelGate::new(group_snapshot());
    let mut c = ctx(Some("vip"), None);
    let r = gate.check(&mut c).await.unwrap_err();
    assert!(matches!(r, Rejection::ModelNotSpecified), "got {r:?}");
}

// ---------------------------------------------------------------------------
// GroupSnapshot queries
// ---------------------------------------------------------------------------

#[test]
fn snapshot_queries_and_fallbacks() {
    let mut groups = GroupSnapshot::default();
    groups.upsert(
        "vip".into(),
        GroupEntry {
            allowed_models: vec!["gpt-4*".into()],
            multiplier: 2.5,
            enabled: true,
        },
    );

    // Hit: allowlist returned as-is (zero-copy borrow)
    assert_eq!(
        groups.allowed_models("vip"),
        Some(&["gpt-4*".to_string()][..])
    );
    assert_eq!(groups.multiplier("vip"), 2.5);
    // Missing fallback: None / 1.0 (neutral multiplier)
    assert!(groups.allowed_models("missing").is_none());
    assert_eq!(groups.multiplier("missing"), 1.0);
    // is_disabled: enabled group false, unknown group false (fail-open semantics unchanged)
    assert!(!groups.is_disabled("vip"));
    assert!(!groups.is_disabled("missing"));

    // upsert overwrites a group with the same name
    groups.upsert("vip".into(), GroupEntry::default());
    assert_eq!(groups.allowed_models("vip").map(<[String]>::len), Some(0));
    assert_eq!(groups.multiplier("vip"), 1.0);
    // Confirm the default values themselves (enabled=true: the default group seed / anything
    // not explicitly disabled takes the enabled path)
    let d = GroupEntry::default();
    assert!(d.allowed_models.is_empty());
    assert_eq!(d.multiplier, 1.0);
    assert!(d.enabled);
    assert!(!groups.is_disabled("vip"));

    // Explicitly disabled group: is_disabled hits; allowlist/multiplier queries are
    // unaffected
    groups.upsert(
        "vip".into(),
        GroupEntry {
            allowed_models: vec!["gpt-4*".into()],
            multiplier: 2.5,
            enabled: false,
        },
    );
    assert!(groups.is_disabled("vip"));
    assert_eq!(groups.multiplier("vip"), 2.5);
}

// ---------------------------------------------------------------------------
// Rejection → HTTP mapping
// ---------------------------------------------------------------------------

#[tokio::test]
async fn rejection_maps_to_403_with_code() {
    let resp = rejection_to_response(Rejection::ModelNotAllowedForGroup {
        model: "claude-x".into(),
        group: "vip".into(),
    });
    assert_eq!(resp.status(), 403);
    let bytes = axum::body::to_bytes(resp.into_body(), 4096)
        .await
        .expect("read body");
    let v: serde_json::Value = serde_json::from_slice(&bytes).expect("json body");
    assert_eq!(v["error"]["code"], "model_not_allowed_for_group");
    // message should carry both model and group for client troubleshooting
    let msg = v["error"]["message"].as_str().unwrap();
    assert!(
        msg.contains("claude-x") && msg.contains("vip"),
        "msg = {msg}"
    );
}

#[tokio::test]
async fn group_disabled_maps_to_403_with_code() {
    let resp = rejection_to_response(Rejection::GroupDisabled {
        group: "blocked".into(),
    });
    assert_eq!(resp.status(), 403);
    let bytes = axum::body::to_bytes(resp.into_body(), 4096)
        .await
        .expect("read body");
    let v: serde_json::Value = serde_json::from_slice(&bytes).expect("json body");
    assert_eq!(v["error"]["code"], "group_disabled");
    // message should carry the group name so clients/troubleshooting can locate which group
    // was disabled
    let msg = v["error"]["message"].as_str().unwrap();
    assert!(msg.contains("blocked"), "msg = {msg}");
}
