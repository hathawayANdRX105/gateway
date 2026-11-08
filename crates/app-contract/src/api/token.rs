//! Key-management endpoint DTOs.
//! Aligned with the wire shape of the backend `control-plane` tokens (camelCase;
//! lists use an `{items}` envelope).

use crate::records::TokenRecord;
use serde::{Deserialize, Serialize};

/// GET /api/token → data: TokenList (the backend list response uses an
/// `{ "items": [...] }` envelope).
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenList {
    pub items: Vec<TokenDto>,
}

/// View of a single API key (maps to the backend TokenView wire shape, camelCase).
///
/// `plain_key` appears only in the create response [`CreateTokenResult`]; it is always None
/// in lists (the frontend shows a mask).
/// `status`: 1 = enabled, 2 = disabled (aligned with new-api).
/// `expires_at` / `created_at`: RFC3339 strings; `expires_at` None means the key never expires.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenDto {
    /// Key UUID (the path parameter for update/delete).
    pub key: String,
    /// Owning user UUID (equal to the current user in owner mode; may differ in
    /// admin_all listings).
    #[serde(default)]
    pub user_key: String,
    pub name: String,
    /// Plaintext appears only in the create response; it is always None in lists
    /// (the frontend shows a mask).
    #[serde(default)]
    pub plain_key: Option<String>,
    /// Masked preview returned by the backend (like `sk-ab****ef`); listings
    /// display it instead of assembling the mask client-side.
    #[serde(default)]
    pub key_preview: String,
    /// Group alias; None = inherit the user's group.
    pub group: Option<String>,
    /// Independent quota (quota units, 500_000 ≈ $1); has no effect when
    /// `unlimited_quota` is true.
    pub quota: i64,
    pub unlimited_quota: bool,
    pub used_quota: i64,
    /// 1 = enabled / 2 = disabled.
    pub status: u8,
    /// RFC3339; None = never expires.
    pub expires_at: Option<String>,
    /// Creation time as RFC3339.
    #[serde(default)]
    pub created_at: String,
}

impl From<&TokenRecord> for TokenDto {
    fn from(r: &TokenRecord) -> Self {
        Self {
            key: r.meta.key.clone(),
            user_key: r.user_key.clone(),
            name: r.name.clone(),
            plain_key: None,
            key_preview: r.key_preview.clone(),
            group: r.group.clone(),
            quota: r.quota,
            unlimited_quota: r.unlimited_quota,
            used_quota: r.used_quota,
            status: r.status,
            expires_at: r.expires_at.map(|t| t.format("%Y-%m-%d %H:%M").to_string()),
            // Record has no created_at, so the Record→Dto path leaves it empty;
            // the wire path fills it from the backend.
            created_at: String::new(),
        }
    }
}

/// POST /api/token (create) request body.
/// `quota`/`unlimited_quota` have backend defaults (0 / false), so the frontend may omit them.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateTokenRequest {
    /// Key name (non-empty).
    pub name: String,
    /// Group alias; None = inherit the user's group.
    #[serde(default)]
    pub group: Option<String>,
    #[serde(default)]
    pub quota: i64,
    #[serde(default)]
    pub unlimited_quota: bool,
    /// RFC3339; None = never expires.
    #[serde(default)]
    pub expires_at: Option<String>,
}

/// POST /api/token response: one-time plaintext key plus the key view.
/// The plaintext appears only once, in the create response; every later endpoint returns only
/// `key_preview`.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateTokenResult {
    /// One-time plaintext `sk-…` (appears once in the successful create response).
    #[serde(default)]
    pub plaintext: String,
    pub token: TokenDto,
}

/// PUT /api/token/{key} (edit).
/// All fields are Option: absent fields are not sent (`skip_serializing_if`), and the backend
/// treats absence as "no change".
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateTokenRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Note: the backend group is `Option<Option<String>>` (None = no change, Some(None) = follow
    /// the user group); the contract layer does not express "follow" semantics, so sending None
    /// here means no change.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quota: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unlimited_quota: Option<bool>,
    /// 1 = enabled / 2 = disabled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<String>,
}
