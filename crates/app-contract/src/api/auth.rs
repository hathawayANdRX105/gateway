//! Login/session endpoint DTOs.
//! Reference: the auth page's form fields, new-api `/api/user/login`, and `transport`'s one-shot 401
//! refresh.

use crate::records::UserRecord;
use crate::records::identity::TokenRecord;
use serde::{Deserialize, Serialize};

use super::user::UserDto;

/// POST /api/user/login
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

/// POST /api/user/register
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RegisterRequest {
    pub username: String,
    pub password: String,
    #[serde(default)]
    pub email: Option<String>,
    /// Invitation code = the inviter's `user_key` (UUID). The registration link passes it
    /// directly as `?invite=<uuid>`.
    ///
    /// The backend silently discards it when parsing fails or the inviter does not exist —
    /// invitations are only a secondary benefit of registration, and an invalid code must not
    /// affect account creation itself. The default `None` means a normal registration.
    #[serde(default)]
    pub invite: Option<String>,
}
/// Returned on successful login. `access_token` is short-lived and `refresh_token` long-lived
/// (the web side already has a one-shot 401 refresh).
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoginResponse {
    pub user: UserDto,
    pub access_token: String,
    pub refresh_token: String,
    /// Number of seconds `access_token` stays valid; the frontend schedules silent refresh
    /// accordingly.
    pub expires_in: u64,
}

/// POST /api/user/reset — password recovery request.
/// TODO(#205): email verification-code flow — enable once the console mail channel (ops::notify)
/// is ready.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResetPasswordRequest {
    pub email: String,
}

/// DELETE /api/user/logout — revokes the refresh token (server-side denylist).
/// TODO(#208): refresh-token rotation + denylist table — requires storage support (identity
/// domain).
pub struct LogoutRequest {
    pub refresh_token: String,
}

/// Test helper: builds a login response from records (for console unit tests).
pub fn login_response_from(_user: &UserRecord, _tokens: &[TokenRecord]) -> LoginResponse {
    todo!("TODO(#205): provide real construction when console lands")
}
