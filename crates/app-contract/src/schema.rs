//! # schema — version compatibility rules and fixtures
//!
//! ## Compatibility rules (required reading when changing the contract)
//!
//! | Change | Compatible | Requirement |
//! |------|----------|------|
//! | Add a field | Yes | Must carry `#[serde(default)]`, with correct default-value semantics |
//! | Add an enum variant | Caution | Frontend must fail soft on unknown variants (show "unknown") |
//! | Delete/rename/retype a field | No | Stop-write window (see below) |
//! | Bump SCHEMA_VERSION | - | Only for field deletion/type changes; record migration notes |
//!
//! Stop-write window: keep the old field for one release under `#[serde(skip_serializing)]`,
//! then remove it after dual-writing.
//!
//! ## fixtures
//!
//! Fixtures are intended for three consumers: field blueprints for web mocks, round-trip tests
//! for the store codec, and console integration tests. `crates/mock` (handwritten fake data) no
//! longer exists; frontend demo mode should reference these fixtures directly so shapes stay
//! exactly aligned with the contract and no longer drift.

use crate::records::{
    ChannelKey, ChannelRecord, GroupRecord, RouteUnitRecord, SyncMeta, UserRecord,
};
use chrono::Utc;

fn meta(key: &str) -> SyncMeta {
    SyncMeta {
        key: key.into(),
        schema_version: crate::SCHEMA_VERSION,
        logical_version: 1,
        origin: "center".into(),
        updated_at: Utc::now(),
    }
}

/// Test channel (two keys, OpenAI protocol).
pub fn fixture_channel() -> ChannelRecord {
    ChannelRecord {
        meta: meta("channel/fixture-1"),
        name: "fixture-openai".into(),
        provider_type: "openai".into(),
        base_url: "https://api.openai.com".into(),
        keys: vec![
            ChannelKey {
                index: 0,
                secret: "sk-fixture-a".into(),
                rpm_limit: 0,
            },
            ChannelKey {
                index: 1,
                secret: "sk-fixture-b".into(),
                rpm_limit: 300,
            },
        ],
        max_concurrency: 10,
        status: 1,
        groups: vec!["default".into()],
        settings: serde_json::json!({}),
    }
}

/// Test group.
pub fn fixture_group() -> GroupRecord {
    GroupRecord {
        meta: meta("group/default"),
        id: "default".into(),
        display_name: "Default group".into(),
        rate_multiplier: 1.0,
        allowed_models: vec![],
    }
}

/// Test user (root role, svip group).
pub fn fixture_user() -> UserRecord {
    UserRecord {
        meta: meta("user/fixture-1"),
        username: "hathaway".into(),
        display_name: "Hathaway".into(),
        email: "hathaway@wildtoken.com".into(),
        quota: 50_000_000,
        used_quota: 12_400_000,
        request_count: 18_420,
        group: "svip".into(),
        status: 1,
        role: 100,
        created_at: Utc::now(),
    }
}

/// Test routing unit: gpt-4o → channel fixture-1 / key 0 / gpt-4o-2024-08-06.
pub fn fixture_route_unit() -> RouteUnitRecord {
    RouteUnitRecord {
        meta: meta("route-unit/fixture-1"),
        group: "default".into(),
        public_model: "gpt-4o".into(),
        channel_key: "channel/fixture-1".into(),
        key_index: 0,
        upstream_model: "gpt-4o-2024-08-06".into(),
        weight: 10,
        status: 1,
    }
}

// TODO(#240): Add serde round-trip tests for every Record type (serde_json → original type →
//             equality) so web/PG/turso encoding and decoding stay consistent. This is one of
//             the acceptance criteria for 02-b D0.
