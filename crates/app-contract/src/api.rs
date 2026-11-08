//! # api — Web/Console REST contract (frontend ↔ console)
//!
//! ## Submodule map
//!
//! | Module | Endpoint domain |
//! |------|--------|
//! | [`auth`]    | Login/session/refresh |
//! | [`user`]    | User self-read/self-update |
//! | [`token`]   | Key management |
//! | [`usage`]   | Usage queries |
//! | [`billing`] | Subscriptions/redemption/model aliases |
//! | [`admin`]   | Admin surface: channel/route/user/group CRUD |
//!
//! The unified envelope [`Envelope`] matches the web side's `client::Envelope`.

pub mod admin;
pub mod auth;
pub mod billing;
pub mod token;
pub mod usage;
pub mod user;

use serde::{Deserialize, Serialize};

/// Unified response envelope — matches the web side's `client::Envelope`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Envelope<T> {
    pub success: bool,
    pub message: String,
    pub data: Option<T>,
}

impl<T> Envelope<T> {
    pub fn ok(data: T) -> Self {
        Self {
            success: true,
            message: "ok".into(),
            data: Some(data),
        }
    }
    pub fn err(message: impl Into<String>) -> Self {
        Self {
            success: false,
            message: message.into(),
            data: None,
        }
    }
}
