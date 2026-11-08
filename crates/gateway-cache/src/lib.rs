//! # gateway-cache — cache substrate for the gateway data plane
//!
//! One bounded concurrent store primitive ([`ttl::TtlCache`], moka-backed) plus
//! the response cache ([`response::ResponseCache`], the data-plane layer of
//! "skip the repeat LLM call").
//!
//! ## Module map
//!
//! | Module | Responsibility |
//! |---|---|
//! | [`ttl`] | The bounded store primitive: capacity (entry count or weighted size), TTL / time-to-idle, hit/miss counters. Every gateway cache sits on it. |
//! | [`response`] | The response cache: isolation-scoped keys, cached entries, store/lookup with per-entry and total byte guards. |
//!
//! ## Placement rules
//!
//! - The store primitive holds no policy of its own: expiry semantics, value
//!   shape and key derivation belong to the owning feature (sticky bindings in
//!   `gateway-dispatch`, client pools in `gateway-proxy`, responses here).
//! - Response-cache lookup / store call sites live in `gateway-forward`; this
//!   crate never names the pipeline or the routing types.
//! - Consumers depend on this crate laterally (`crates/gateway-*` →
//!   `crates/gateway-*`), the same direction as `gateway-forward` →
//!   `gateway-dispatch`.

pub mod response;
pub mod ttl;

pub use response::{
    CacheKey, CacheScope, CachedResponse, CachedUsage, ResponseCache, response_key,
};
pub use ttl::{CacheStats, TtlCache};
