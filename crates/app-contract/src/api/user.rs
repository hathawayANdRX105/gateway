//! User endpoint DTOs.
//! Reference: new-api /api/user/ responses.

use crate::records::UserRecord;
use serde::{Deserialize, Serialize};

/// GET /api/user/self → the user projection seen by the frontend.
///
/// Fields align one-for-one with the wire shape of the backend
/// `api_auth::service::UserView` (camelCase): no storage details such as password
/// hashes. `role` is the u16 wire value (1/10/100); semantic labels come from
/// [`role_label`]. `request_count` is optional because the real `/self` does not return
/// it; `auth_version` increments on password change and stays unfilled on the
/// Record→Dto path (defaults to 0).
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserDto {
    pub key: String,
    pub username: String,
    pub display_name: String,
    pub email: String,
    pub quota: i64,
    pub used_quota: i64,
    /// Request count. The real `/self` (UserView) does not carry this field, hence
    /// `Option` + `#[serde(default)]`.
    #[serde(default)]
    pub request_count: Option<u64>,
    pub group: String,
    /// 1=user | 10=admin | 100=root (wire value; use [`role_label`] for the label).
    pub role: u16,
    pub status: u8,
    /// Incremented on password change to invalidate old access/refresh tokens. Defaults
    /// to 0 on the Record→Dto path.
    #[serde(default)]
    pub auth_version: i64,
    pub created_at: String,
}

impl From<&UserRecord> for UserDto {
    fn from(r: &UserRecord) -> Self {
        Self {
            key: r.meta.key.clone(),
            username: r.username.clone(),
            display_name: r.display_name.clone(),
            email: r.email.clone(),
            quota: r.quota,
            used_quota: r.used_quota,
            request_count: Some(r.request_count),
            group: r.group.clone(),
            role: r.role,
            status: r.status,
            auth_version: 0,
            created_at: r.created_at.format("%Y-%m-%d").to_string(),
        }
    }
}

/// role wire value → semantic label: 100→root, 10→admin, everything else→user.
pub fn role_label(role: u16) -> &'static str {
    match role {
        100 => "root",
        10 => "admin",
        _ => "user",
    }
}

/// GET /api/user/self/sessions → a single session.
///
/// Wire shape of the backend `api_auth::service::SessionView`; the backend serializes
/// time fields as RFC3339 strings, carried as `String` on the frontend wire to keep
/// chrono out of the wasm side.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionDto {
    pub sid: String,
    pub user_agent: String,
    pub ip: String,
    pub login_method: String,
    pub created_at: String,
    pub last_active: String,
    pub expires_at: String,
    /// Whether this is the current device's session (the backend marks it from the sid
    /// in the access token).
    pub current: bool,
}

/// PUT /api/user/self — change display name / change password (a password change needs
/// the old and new password supplied together).
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateSelfRequest {
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub original_password: Option<String>,
    #[serde(default)]
    pub new_password: Option<String>,
}

/// Self-service profile update body (display name/email; password goes through a
/// separate endpoint). The live `PUT /api/user/self` handler takes [`UpdateSelfRequest`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateProfileRequest {
    pub display_name: String,
    pub email: String,
}

/// PUT /api/user/password
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChangePasswordRequest {
    pub old_password: String,
    pub new_password: String,
}

/// POST /api/user/topup — redeem-code top-up request body.
///
/// Contract projection of the backend handler's local `TopupRequest { key }` struct
/// (billing redeem.rs, no serde rename): the field name aligns verbatim as the single
/// field `key`, whose value is the redeem-code plaintext (the `fx-` + 32 hex string
/// returned once when an admin generates it). A successful response is bare JSON
/// `{quota, success}`, where quota is the credited amount in internal quota units
/// (500_000 ≈ $1).
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct UserTopupRequest {
    /// Redeem-code plaintext (aligns verbatim with the backend `TopupRequest.key`, no
    /// rename).
    pub key: String,
}
