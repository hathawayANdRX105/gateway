//! Verifies that the contract DTOs parse a real backend login response.
//!
//! Wire shape of the backend `UserView`: `role` is a u16 value (1/10/100), and `auth_users`
//! has no `request_count` column, so the response carries no `requestCount`. The contract
//! `UserDto` mirrors the wire fields it covers: `role: u16`, with semantic labels via
//! [`app_contract::api::user::role_label`], and `request_count: Option<u64>` (missing =
//! None). These tests pin that compatibility against JSON captured from the real backend.

use app_contract::api::auth::LoginResponse;
use app_contract::api::user::role_label;

/// Login response body captured from a real running backend (`http://127.0.0.1:3211`) with
/// role=1 and no requestCount; the `role` argument substitutes the captured value.
fn real_login_body(role: u32) -> String {
    format!(
        r#"{{
            "accessToken": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.payload.sig",
            "expiresIn": 900,
            "refreshToken": "1009c75c-144d-443f-a8ed-c4ff25812838.f92a4d57fc05ac81b9debcb2af248f85c1dcba8ca74a41a9657970be49584359",
            "user": {{
                "authVersion": 1,
                "createdAt": "2026-09-08T17:55:28.579140Z",
                "displayName": "probe_user",
                "email": "",
                "group": "default",
                "key": "d9334bb6-1652-4e00-ade8-75fe5d5fbc45",
                "quota": 0,
                "role": {role},
                "status": 1,
                "usedQuota": 0,
                "username": "probe_user"
            }}
        }}"#
    )
}

#[test]
fn login_response_parses_with_integer_role_and_missing_request_count() {
    let body = real_login_body(1);
    let resp: LoginResponse =
        serde_json::from_str(&body).expect("后端真实登录响应必须能被 LoginResponse 解析");

    assert_eq!(
        resp.access_token,
        "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.payload.sig"
    );
    assert!(!resp.refresh_token.is_empty());
    assert_eq!(resp.expires_in, 900);

    // role=1 → the u16 value is kept verbatim and the semantic label comes from role_label; the
    // backend omits requestCount → None.
    assert_eq!(resp.user.role, 1);
    assert_eq!(role_label(resp.user.role), "user");
    assert_eq!(resp.user.request_count, None);
    assert_eq!(resp.user.username, "probe_user");
    assert_eq!(resp.user.status, 1);
}

#[test]
fn login_response_maps_root_role_100_to_semantic_label() {
    let body = real_login_body(100);
    let resp: LoginResponse = serde_json::from_str(&body).expect("role=100 响应必须能解析");
    assert_eq!(resp.user.role, 100);
    assert_eq!(role_label(resp.user.role), "root");
}

#[test]
fn login_response_maps_admin_role_10_to_semantic_label() {
    let body = real_login_body(10);
    let resp: LoginResponse = serde_json::from_str(&body).expect("role=10 响应必须能解析");
    assert_eq!(resp.user.role, 10);
    assert_eq!(role_label(resp.user.role), "admin");
}
