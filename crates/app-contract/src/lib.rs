//! # contract — the logical contract layer for the whole workspace
//!
//! Single source of truth. Center PostgreSQL, edge Fjall, web (wasm), and bin/api
//! (native) all depend on this crate to get identical type definitions.
//!
//! ## Module map
//!
//! | Module | Contents | Consumers |
//! |------|------|--------|
//! | [`api`]       | REST DTOs between frontend and console (auth/user/token/usage/admin) | web, console |
//! | [`records`]   | Domain entities (channel/routing/identity/usage/billing submodules) | everything |
//! | [`error`]     | Cross-surface error-code table + gateway error body | gateway, console, web |
//! | [`session_cache`] | In-memory session cache (native test double, no runtime dependency) | tests-http |
//!
//! ## Hard rules (violation = PR rejected)
//!
//! 1. This crate must not depend on any runtime (tokio/sqlx/axum/reqwest/dioxus);
//! 2. No SQL DDL (feature crates create their tables inline) and no storage implementation;
//! 3. Web DTOs are converted from records (the `From` impls live in this crate);
//! 4. New fields must carry a compatible default (`#[serde(default)]`); removing a field
//!    goes through a stop-write window.

pub mod api;
pub mod error;
pub mod mutations;
pub mod records;
pub mod schema;
pub mod session_cache;

/// Contract schema version. Bump it on a field-level incompatible change and register the
/// migration rule in schema.rs.
pub const SCHEMA_VERSION: u32 = 1;
