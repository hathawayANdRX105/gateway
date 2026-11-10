//! `gateway-pipeline` — the gateway orchestration core
//!
//! Defines the Stage trait / RequestCtx / Pipeline / axum integration, and is the common
//! dependency of the gateway crates that participate in the pipeline (gate / dispatch /
//! forward / protocol-bridge). It does not depend on any concrete stage crate in return,
//! keeping the dependency direction one-way.
//!
//! ## File layout
//!
//! - `ctx` — request context: `RequestCtx` / `RequestMeta` / `BodySource` / `PipeStream` /
//!   `RouteAttribution`
//! - `stage` — `Stage` trait + `StageOutcome` + `StageError` / `UpstreamError`
//! - `pipeline` — chained `Pipeline` orchestrator
//! - `router` — axum integration (`build_router` + `error_to_response`)
//!
//! Shared contract types keep only [`TokenInfo`], which cross-crate consumers require; the
//! remaining snapshots (token / user / pricing / quota / ip-policy / sensitive-words) are
//! implemented by their respective owners:
//! - `gate`'s `snapshot.rs` (TokenSnapshot / UserSnapshot live there)
//! - `gate`'s `quota.rs` / `state.rs` / `ratelimit.rs`
//! - `security`'s `wordlist.rs`
//! - `dispatch`'s `health.rs` / `ratelimit.rs`
//! - `metering`'s `ledger.rs` / `pricing.rs`

mod ctx;
mod pipeline;
mod router;
mod stage;

pub use ctx::{
    BodyReader, BodySource, DropGuards, PipeStream, RequestCtx, RequestMeta, RouteAttribution,
    SelectedRoute, StreamedAccum, UpstreamResponse,
};
// Inbound protocol vocabulary lives in the shared vocabulary (gateway-core); this facade
// keeps the historical `gateway_pipeline::ProtocolKind` path working.
pub use gateway_core::ProtocolKind;
pub use pipeline::Pipeline;
pub use router::{build_router, data_plane_guard, error_to_response};
pub use stage::{Stage, StageError, StageOutcome, UpstreamError};

/// Product of token authentication (Admission writes it into `RequestCtx.token`)
#[derive(Debug, Clone)]
pub struct TokenInfo {
    /// token identity primary key: `TokenRecord.meta.key` (UUID string)
    pub id: String,
    /// Owning user's UUID string (settlement looks it up in user_balances; see gate::TokenInfo)
    pub user_key: String,
    pub group: String,
    pub enabled: bool,
    pub allowed_models: Option<Vec<String>>,
    pub auth_version: u64,
}
