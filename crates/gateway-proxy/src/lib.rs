//! `gateway-proxy` — per-channel node selection + egress Client leases
//!
//! The HTTP CONNECT / SOCKS5 handshake is performed by `reqwest` (the workspace has the
//! `socks` feature enabled). [`ProxyManager::acquire`] selects a node by `channel_key`
//! and returns a `reqwest::Client` with the node's `reqwest::Proxy` already injected.
//!
//! Protocols reqwest cannot cover (vless/vmess/ss/trojan) are mapped by the [`adapter`]
//! module to a meow `ProxyAdapter`, with the protocol handshake completed inside
//! `dial_tcp`; `ProxyManager` produces `ProxyClient::Adapter` for them.
//!
//! ## What is vocabulary and what is engine
//!
//! Nothing here is vocabulary. Node types, the URL / share-link parsers, the snapshot
//! type, and the admin-plane port live in `gateway-core::proxy` (the vocabulary surface);
//! this crate re-exports them at the crate root so existing call sites keep
//! resolving. Everything that dials, leases, caches, or probes stays here.
//!
//! ## File layout
//!
//! - [`probe`] — active probing (liveness via TCP dial, reusing meow `ProxyHealth`)
//! - [`node`] — `reqwest::Proxy` projection of a node
//! - [`adapter`] — node → meow `ProxyAdapter` mapping (protocol handshake inside
//!   `dial_tcp`)
//! - [`pool`] — `ProxyPool`: indexed by `channel_key` + priority-tiered node selection
//! - [`ssrf`] — SSRF protection: dual validation of IP literals and DNS results
//! - [`manager`] — leases / health cooldowns / per-node Client cache (both reqwest and
//!   adapter paths)

pub mod manager;
pub mod node;
pub mod pool;
pub mod probe;
pub mod ssrf;

pub use probe::{ProbeResult, probe_node};

pub use manager::{Lease, ProxyManager};
pub use pool::{ProxyPool, ProxySnapshot};
pub use ssrf::validate_url;

pub mod adapter;

pub use adapter::{adapter_for, tcp_metadata};
pub use gateway_core::proxy::node::{BasicAuth, NodeOpts, ParseError, ProxyNode, ProxyScheme};
pub use gateway_core::proxy::sharelink::{
    ShareLinkBatch, VmessShareLink, mask_link, parse_share_link, parse_share_links,
};
pub use meow_common::ProxyAdapter;
