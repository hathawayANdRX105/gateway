//! `gateway-gate` — filtering layer (auth / state / quota / ratelimit / model / graylist /
//! concurrency)
//!
//! The request main path's **filtering layer**, designed after the Tower / Axum middleware
//! onion model. Any gate failure → `ShortCircuit(4xx)`, never entering dispatch.
//!
//! ## Module map (seven gates, all traversed in one call)
//!
//! | Module | Gate | Failure code |
//! |--------|------|--------------|
//! | [`auth`]        | ① key-hash lookup + auth_version | INVALID_API_KEY / TOKEN_EXPIRED |
//! | [`state`]       | ② enabled / expiry / IP allowlist / user disabled | USER_DISABLED / IP_NOT_ALLOWED |
//! | [`quota`]       | ③ balance vs estimated cost | INSUFFICIENT_QUOTA |
//! | [`ratelimit`]   | ④ per-key / per-channel RPM/TPM | RATE_LIMITED |
//! | [`model`]       | ⑤ token.allowed_models allowlist | MODEL_FORBIDDEN |
//! | [`graylist`]    | ⑥ consecutive-failure ban | GRAYLISTED |
//! | [`concurrency`] | ⑦ post-dispatch, per-channel Semaphore (full slots do not queue; returned on ctx drop) | RATE_LIMITED (429) |
//! | [`snapshot`]    | (data source) holds identity / quota / ip-policy | CATALOG_NOT_READY |
//!
//! ```text
//! request → auth → state → quota → ratelimit → model → graylist
//!                                              ↓ (post-dispatch)
//!                                       concurrency
//!          ↑ all lookups hit the local in-memory snapshot, zero cross-network calls
//! ```

pub mod auth;
pub mod chain;
pub mod concurrency;
pub mod error;
pub mod graylist;
pub mod model;
pub mod quota;
pub mod ratelimit;
pub mod rewrite;
pub mod snapshot;
pub mod state;

pub use auth::{AuthGate, sha256};
pub use chain::{Gate, GateChain, GateCtx, Gated};
pub use concurrency::{ConcurrencyGate, ConcurrencyState};
pub use error::{Rejection, rejection_to_response};
pub use graylist as graylist_mod;
pub use graylist::{
    BLOCK_DURATION, FAIL_STREAK_THRESHOLD, FailEntry, GrayListGate, GrayListState, STREAK_WINDOW,
};
pub use model::{GroupModelGate, ModelGate, ModelParseGate};
pub use quota::QuotaGate;
pub use ratelimit::{LimitScope, RateLimitGate, RateLimiter};
pub use rewrite::{InjectSpec, rewrite_body};
pub use state::StateGate;

// snapshot re-exports
pub use snapshot::{
    IpPolicy, PriceRow, PricingSnapshot, QuotaSnapshot, SharedGroupSnapshot, SharedIpPolicy,
    SharedPricing, SharedQuota, SharedTokenSnapshot, SharedUserSnapshot, TokenEntry, TokenSnapshot,
    UserSnapshot,
};

/// Token metadata produced by auth (filled inside chain, finally promoted to RequestCtx.token)
#[derive(Debug, Clone)]
pub struct TokenInfo {
    /// Token identity primary key: `TokenRecord.meta.key` (a UUID string); quota and
    /// rate-limit buckets are keyed by it directly
    pub id: String,
    /// The UUID string of the user owning the token (`TokenRecord.user_key`). Settlement
    /// debits by looking up user_balances with the user key — forward once had only
    /// token.id available and debited the wrong key (the wallet was never charged,
    /// confirmed by e2e); this field closes that attribution chain.
    pub user_key: String,
    pub user_id: i64,
    pub id_hash: [u8; 32], // sha256(raw_key)
    pub group: String,
    pub enabled: bool,
    pub expires_at: Option<i64>,
    pub allowed_models: Option<Vec<String>>,
    pub auth_version: u64,
}

/// User metadata produced by auth
#[derive(Debug, Clone)]
pub struct UserInfo {
    pub id: i64,
    pub enabled: bool,
    pub group: String,
    pub auth_version: u64,
}
