//! In-memory session cache (native test double).
//!
//! The browser stores session/token state in `page_auth::model::session` (localStorage /
//! sessionStorage, wasm-only). Native environments (tests-http integration tests, command-line
//! tools) have no browser storage, so this module offers an in-process double with the same
//! signatures: after a successful login `set_cached_session` records `(access_token, user)`,
//! requests call `get_cached_token` to retrieve it, and `clear_cached_session` ends the session.
//!
//! Semantics match the browser version (the two-tier store collapses to one in-memory slot on
//! native):
//! - writes are immediately readable;
//! - after clearing, `get_cached_token` returns `None`;
//! - all threads share one copy (`Mutex`), so concurrent tests never observe a stale slot.

use std::sync::Mutex;

use crate::api::user::UserDto;

/// In-process session slot: `access_token + current user`.
static CACHE: Mutex<Option<(String, UserDto)>> = Mutex::new(None);

/// Writes the session slot (simulates the client cache after a successful login).
// A poisoned cache mutex is unrecoverable; this infallible test-double setter must not become
// fallible.
#[allow(unknown_lints)] // rustc/clippy do not know the dylint lint name
#[allow(expect_in_non_result)]
pub fn set_cached_session(token: &str, user: &UserDto) {
    *CACHE.lock().expect("session cache poisoned") = Some((token.to_string(), user.clone()));
}

/// Returns the current access token; `None` when there is no session or it has been cleared.
pub fn get_cached_token() -> Option<String> {
    CACHE
        .lock()
        .expect("session cache poisoned")
        .as_ref()
        .map(|(t, _)| t.clone())
}

/// Clears the session slot (simulates logout).
// A poisoned cache mutex is unrecoverable; this infallible test-double clear must not become
// fallible.
#[allow(unknown_lints)] // rustc/clippy do not know the dylint lint name
#[allow(expect_in_non_result)]
pub fn clear_cached_session() {
    *CACHE.lock().expect("session cache poisoned") = None;
}
