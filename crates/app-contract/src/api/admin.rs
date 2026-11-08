//! Admin-plane endpoint DTOs — for the console admin panel (page-admin / page-users).
//!
//! Auth: role >= 10 (admin). The endpoint registry table is at the end of this file.
//! Reference: new-api's /api/channel|user|group admin routes (including the status/group
//! filter enums).

use crate::records::{ChannelRecord, GroupRecord, UserRecord};
use serde::{Deserialize, Serialize};

/// POST/PUT /api/channel — channel create/update (admin).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminChannelUpsert {
    /// None = create.
    pub key: Option<String>,
    pub name: String,
    pub provider_type: String,
    pub base_url: String,
    /// Plaintext keys (encrypted server-side before storage); responses never return plaintext.
    pub keys: Vec<String>,
    pub max_concurrency: u32,
    pub groups: Vec<String>,
    pub settings: serde_json::Value,
}

impl From<&ChannelRecord> for AdminChannelUpsert {
    fn from(r: &ChannelRecord) -> Self {
        Self {
            key: Some(r.meta.key.clone()),
            name: r.name.clone(),
            provider_type: r.provider_type.clone(),
            base_url: r.base_url.clone(),
            // Ciphertext never leaves the database — on edit the frontend receives a mask, and
            // leaving keys blank on save means unchanged (TODO(#209))
            keys: vec![],
            max_concurrency: r.max_concurrency,
            groups: r.groups.clone(),
            settings: r.settings.clone(),
        }
    }
}

/// GET /api/user → data: AdminUserPage (admin user list).
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminUserPage {
    pub items: Vec<AdminUserDto>,
    pub total: u64,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminUserDto {
    pub key: String,
    pub username: String,
    pub display_name: String,
    pub email: String,
    pub quota: i64,
    pub used_quota: i64,
    /// Request count. The backend `UserView` has no such column (measured with curl on
    /// 2026-09-09), so wire values default to 0; the frontend "request count" card renders
    /// whatever the backend actually returns.
    #[serde(default)]
    pub request_count: u64,
    /// Effective group (`auth_users.groups[1]`, default "default").
    #[serde(default)]
    pub group: String,
    /// All effective groups (multi-value since migration 0016); `groups[1]` is
    /// [`group`](Self::group).
    #[serde(default)]
    pub groups: Vec<String>,
    pub status: u8,
    pub role: u16,
    pub created_at: String,
}

impl From<&UserRecord> for AdminUserDto {
    fn from(r: &UserRecord) -> Self {
        // The snapshot side only has a single-valued group, so groups reuses the same clone
        let group = r.group.clone();
        Self {
            key: r.meta.key.clone(),
            username: r.username.clone(),
            display_name: r.display_name.clone(),
            email: r.email.clone(),
            quota: r.quota,
            used_quota: r.used_quota,
            request_count: r.request_count,
            group: group.clone(),
            groups: vec![group],
            status: r.status,
            role: r.role,
            created_at: r.created_at.format("%Y-%m-%d").to_string(),
        }
    }
}

/// POST /api/user/manage — admin user management
/// (enable/disable/set_role/adjust_quota/reset_password)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ManageUserRequest {
    pub key: String,
    pub action: String,
    #[serde(default)]
    pub value: Option<String>,
}

/// Channel view DTO — mirrors the admin-api /api/channel response
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelDto {
    pub key: String,
    pub name: String,
    pub channel_type: String,
    pub base_url: String,
    pub key_count: i64,
    #[serde(default)]
    pub keys: Option<Vec<String>>,
    pub models: serde_json::Value,
    pub groups: Vec<String>,
    pub weight: i32,
    pub status: i16,
    /// `api_channels.settings` configuration as stored (JSON blob); `settings.headers` holds
    /// outbound override headers (a JSON object with string keys and values), shared by the
    /// gateway hot path and the admin health-check/model-list calls.
    #[serde(default)]
    pub settings: serde_json::Value,
    #[serde(default)]
    pub test_model: Option<String>,
    #[serde(default)]
    pub remark: String,
    #[serde(default)]
    pub created_at: String,
    #[serde(default)]
    pub updated_at: String,
}

/// Channel create/update request
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelUpsertRequest {
    pub name: String,
    pub channel_type: String,
    pub base_url: String,
    #[serde(default)]
    pub keys: Vec<String>,
    pub models: serde_json::Value,
    pub groups: Vec<String>,
    pub weight: i32,
    /// Same as `ChannelDto.settings` — create/update requests may carry `settings.headers`
    /// custom headers.
    #[serde(default)]
    pub settings: serde_json::Value,
    #[serde(default)]
    pub test_model: Option<String>,
    #[serde(default)]
    pub remark: String,
}

/// Request-header preset view DTO — corresponds to `GET /api/header-preset`.
///
/// `headers` is a JSON object that can be copied into a channel's `settings.headers`. The
/// `passthrough` preset uses an empty object, meaning client request headers are not overridden.
/// Preset selection uses copy semantics, so editing a preset does not retroactively rewrite
/// saved channel configuration.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HeaderPresetDto {
    pub key: String,
    pub name: String,
    #[serde(default)]
    pub headers: serde_json::Value,
    #[serde(default)]
    pub remark: String,
    #[serde(default)]
    pub builtin: bool,
    #[serde(default)]
    pub sort: i32,
    #[serde(default)]
    pub created_at: String,
    #[serde(default)]
    pub updated_at: String,
}

/// User group DTO — mirrors the admin-api /api/group response
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupDto {
    pub key: String,
    pub name: String,
    /// Default-group flag (`api_groups.is_default`; at most one row in the whole table is true,
    /// see migration 0027).
    #[serde(default)]
    pub is_default: bool,
    pub ratio: f64,
    pub model_whitelist: serde_json::Value,
    #[serde(default)]
    pub remark: String,
    pub status: i16,
    #[serde(default)]
    pub created_at: String,
    #[serde(default)]
    pub updated_at: String,
}

/// User group create/update request
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupUpsertRequest {
    /// Group name: on update, `None` = keep the current value (server-side COALESCE; absent on
    /// the wire); required on create.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub ratio: f64,
    pub model_whitelist: serde_json::Value,
    #[serde(default)]
    pub remark: String,
    /// Default-group flag: `None` = no change (absent on the wire); `Some(true)` = make this the
    /// default (the server clears the old default in the same transaction); `Some(false)` = unset
    /// (another default group must remain, otherwise 400).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_default: Option<bool>,
}

/// PUT /api/group — group edit (admin).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminGroupUpsert {
    pub id: String,
    pub display_name: String,
    pub rate_multiplier: f64,
    pub allowed_models: Vec<String>,
}

impl From<&GroupRecord> for AdminGroupUpsert {
    fn from(r: &GroupRecord) -> Self {
        Self {
            id: r.id.clone(),
            display_name: r.display_name.clone(),
            rate_multiplier: r.rate_multiplier,
            allowed_models: r.allowed_models.clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// Admin-plane endpoint registry (for the console implementation; auth role >= 10)
// ---------------------------------------------------------------------------
//
// | Method | Path                    | Request body          | data type      |
// |--------|-------------------------|-----------------------|----------------|
// | GET    | /api/channel/           | -                     | Vec<ChannelRecord> projection |
// | POST   | /api/channel/           | AdminChannelUpsert    | ()             |
// | DEL    | /api/channel/{key}      | -                     | ()             |
// | GET    | /api/user/              | ?page=&group=&status= | AdminUserPage  |
// | PUT    | /api/user/{key}/quota   | { delta: i64 }        | { balance }    |
// | GET    | /api/group/             | -                     | Vec<AdminGroupUpsert> |
// | PUT    | /api/group/             | AdminGroupUpsert      | ()             |
//
// TODO(#209): key retention semantics when editing a channel — an empty `keys` field from the
// frontend means keep the existing keys unchanged.
