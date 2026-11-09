//! `snapshot` — data sources: identity / quota / ip-policy / pricing storage
//!
//! Every gate's read-only snapshot comes from here, pushed by the snapshot reload in
//! `api-gateway-state`.
//!
//! The snapshot containers themselves are vocabulary shared with the admin plane and
//! live on the injection surface (`gateway_core::gate`); this module re-exports
//! them so existing `gateway_gate::snapshot::…` paths keep resolving, and keeps the
//! gate-local view helpers: the IP allowlist and the contract-record projections the
//! gates read.
//!
//! `TokenEntry` wraps the contract's [`TokenRecord`] + the gate-only `allowed_models`;
//! it does not reuse the contract TokenRecord because the latter does not list allowed
//! models (a gate-only concept).

use std::net::IpAddr;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use gateway_core::records::{TokenRecord, UserRecord};

pub use gateway_core::gate::{
    GroupEntry, GroupSnapshot, PriceRow, PricingSnapshot, QuotaSnapshot, SharedGroupSnapshot,
    SharedPricing, SharedQuota, SharedTokenSnapshot, SharedUserSnapshot, TokenEntry, TokenSnapshot,
    UserSnapshot, estimate_cost,
};

// ============================================================================
// IpPolicy — CIDR allowlist (IPv4 / IPv6 prefixes)
// ============================================================================

/// IP allowlist (set of CIDR prefixes). Empty set = everyone allowed.
///
/// Internally stores raw CIDR strings (serde-friendly) and parses on demand.
/// ponytail: string parsing saves a pile of derives compared with serializing
/// CidrRange directly; the list is short, so the cost is negligible.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct IpPolicy {
    cidrs: Vec<String>,
}

impl IpPolicy {
    pub fn allows_everyone() -> Self {
        Self { cidrs: vec![] }
    }

    pub fn from_cidrs(cidrs: &[String]) -> Self {
        Self {
            cidrs: cidrs.to_vec(),
        }
    }

    pub fn cidrs(&self) -> &[String] {
        &self.cidrs
    }

    pub fn allows(&self, ip: &IpAddr) -> bool {
        if self.cidrs.is_empty() {
            return true;
        }
        self.cidrs
            .iter()
            .any(|c| parse_cidr(c).is_ok_and(|r| r.contains(ip)))
    }
}

/// CIDR prefix: works for IPv4 / IPv6, bitwise prefix matching.
/// ponytail: hand-written prefix matching avoids the ipnet dependency; the use case is
/// just allowlist comparison.
#[derive(Debug, Clone)]
struct CidrRange {
    bytes: [u8; 16],
    prefix_len: u8, // 0..=128
    is_v4: bool,
}

fn parse_cidr(s: &str) -> Result<CidrRange, String> {
    let (addr_part, plen_part) = match s.split_once('/') {
        Some((a, p)) => (a, Some(p)),
        None => (s, None),
    };
    let ip: IpAddr = addr_part.parse().map_err(|e| format!("bad ip: {e}"))?;
    let plen: u8 = match plen_part {
        Some(p) => p.parse().map_err(|e| format!("bad prefix: {e}"))?,
        None => match ip {
            IpAddr::V4(_) => 32,
            IpAddr::V6(_) => 128,
        },
    };
    let mut bytes = [0u8; 16];
    let is_v4 = match ip {
        IpAddr::V4(v4) => {
            bytes[0..4].copy_from_slice(&v4.octets());
            true
        }
        IpAddr::V6(v6) => {
            bytes.copy_from_slice(&v6.octets());
            false
        }
    };
    let max_plen = if is_v4 { 32 } else { 128 };
    if plen > max_plen {
        return Err(format!("prefix > {max_plen}"));
    }
    Ok(CidrRange {
        bytes,
        prefix_len: plen,
        is_v4,
    })
}

impl CidrRange {
    fn contains(&self, ip: &IpAddr) -> bool {
        match ip {
            IpAddr::V4(v4) => {
                let mut b = [0u8; 16];
                b[0..4].copy_from_slice(&v4.octets());
                self.contains_bytes(&b, true)
            }
            IpAddr::V6(v6) => self.contains_bytes(&v6.octets(), false),
        }
    }

    fn contains_bytes(&self, other: &[u8; 16], other_is_v4: bool) -> bool {
        if self.is_v4 != other_is_v4 {
            return false;
        }
        let full_bytes = (self.prefix_len / 8) as usize;
        let rem_bits = self.prefix_len % 8;
        if full_bytes > 0 && self.bytes[..full_bytes] != other[..full_bytes] {
            return false;
        }
        if rem_bits == 0 {
            return true;
        }
        let mask = !((1u8 << (8 - rem_bits)) - 1);
        self.bytes[full_bytes] & mask == other[full_bytes] & mask
    }
}

// ============================================================================
// ArcSwap handle aliases (gate-local)
// ============================================================================

pub type SharedIpPolicy = std::sync::Arc<arc_swap::ArcSwap<IpPolicy>>;

// ============================================================================
// View extensions — contract record → gate view
// ============================================================================

/// `UserRecord` → gate-view fields
pub trait UserView {
    /// Whether the user is enabled (status == 1)
    fn is_enabled(&self) -> bool;
    /// auth_version monotonicity — used for "old tokens become invalid immediately after a
    /// password/2FA change". Here `meta.schema_version` is used as a coarse proxy; the real
    /// auth_version is maintained by the sync layer.
    fn auth_version(&self) -> u64;
    /// User group string
    fn group(&self) -> &str;
}

impl UserView for UserRecord {
    fn is_enabled(&self) -> bool {
        self.status == 1
    }
    fn auth_version(&self) -> u64 {
        self.meta.schema_version as u64
    }
    fn group(&self) -> &str {
        &self.group
    }
}

/// `TokenRecord` → gate-view fields
pub trait TokenView {
    fn is_enabled(&self) -> bool;
    fn auth_version(&self) -> u64;
    fn group(&self) -> Option<&str>;
    /// unix timestamp; None = never expires.
    fn expires_at_unix(&self) -> Option<i64>;
    fn user_key(&self) -> &str;
    /// The token's own remaining quota (i64::MAX when unlimited_quota).
    fn quota_remaining(&self) -> i64;
}

impl TokenView for TokenRecord {
    fn is_enabled(&self) -> bool {
        self.status == 1
    }
    fn auth_version(&self) -> u64 {
        self.meta.schema_version as u64
    }
    fn group(&self) -> Option<&str> {
        self.group.as_deref()
    }
    fn expires_at_unix(&self) -> Option<i64> {
        self.expires_at.as_ref().map(|t| t.timestamp())
    }
    fn user_key(&self) -> &str {
        &self.user_key
    }
    fn quota_remaining(&self) -> i64 {
        if self.unlimited_quota {
            i64::MAX
        } else {
            self.quota - self.used_quota
        }
    }
}

/// Current unix timestamp.
pub fn now_unix() -> i64 {
    Utc::now().timestamp()
}

/// `DateTime<Utc>` → unix (kept public for tests / boundary callers).
pub fn to_unix(t: &DateTime<Utc>) -> i64 {
    t.timestamp()
}
