//! Identity records — users and API tokens.
//!
//! References: new-api internal/identity, one-api user/token tables,
//! domain two of 07-database-schema.md.

use super::SyncMeta;
use serde::{Deserialize, Serialize};

/// User.
///
/// `quota` follows new-api semantics (the smallest internal billing unit, `500_000` = $1).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserRecord {
    pub meta: SyncMeta,
    pub username: String,
    pub display_name: String,
    pub email: String,
    /// Remaining quota (internal units).
    pub quota: i64,
    /// Consumed quota.
    pub used_quota: i64,
    pub request_count: u64,
    pub group: String,
    /// 1 enabled / 2 disabled (aligned with new-api).
    pub status: u8,
    /// 1 user / 10 admin / 100 root.
    pub role: u16,
    /// User creation timestamp. TODO(#207): auto-group policy — new-api `resolve_group.go`.
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// Token used by the frontend/client to call the API (sk-...).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenRecord {
    pub meta: SyncMeta,
    pub user_key: String, // points to UserRecord::meta.key
    pub name: String,
    /// SHA-256(plaintext key). The plaintext appears only once in the creation response;
    /// the contract layer never stores plaintext.
    pub key_hash: String,
    /// Masked preview "sk-ab****ef" (for list display; no decryption needed).
    pub key_preview: String,
    /// Fixed group for the token; None = follow the user's group (new-api token group
    /// semantics).
    pub group: Option<String>,
    /// Token-specific quota limit; ignored when `unlimited_quota` = true.
    pub quota: i64,
    pub unlimited_quota: bool,
    pub used_quota: i64,
    /// Optional expiration time.
    pub expires_at: Option<chrono::DateTime<chrono::Utc>>,
    pub status: u8,
}
