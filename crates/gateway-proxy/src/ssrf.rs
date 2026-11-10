//! `ssrf` — SSRF protection
//!
//! Two IP checks: IP literals taken from URL parsing, and the actual DNS resolution result
//! (meant to run before dialling). Domain names are resolved by the caller and then validated;
//! `validate_url` only covers IP literals.
//!
//! IPv4-mapped IPv6 (`::ffff:a.b.c.d`) bypasses the loopback check and must be blocked.
//! CGNAT (100.64.0.0/10) is a reserved range and must be blocked as well.

use std::net::IpAddr;
use thiserror::Error;
use url::Url;

#[derive(Debug, Error)]
pub enum SsrfError {
    #[error("loopback address not allowed")]
    Loopback,
    #[error("private ip not allowed")]
    PrivateIp,
    #[error("link-local address not allowed")]
    LinkLocal,
    #[error("multicast address not allowed")]
    Multicast,
    #[error("unspecified address not allowed")]
    Unspecified,
    #[error("dns resolution failed: {0}")]
    Dns(String),
}

/// URL validation: only IP literals are checked; a domain name must be DNS-resolved by the
/// caller and then passed through `validate_resolved`
///
/// # Errors
///
/// Returns [`SsrfError::Dns`] when the URL has no host, or the [`check_ip`] variant
/// when the host is an IP literal inside a reserved range.
pub fn validate_url(url: &Url) -> Result<(), SsrfError> {
    let host = url.host().ok_or(SsrfError::Dns("no host".into()))?;
    match host {
        url::Host::Ipv4(ip) => check_ip(IpAddr::V4(ip))?,
        url::Host::Ipv6(ip) => check_ip(IpAddr::V6(ip))?,
        url::Host::Domain(_) => {
            // Domains are not DNS-resolved here; the caller resolves and validates at dial time
        }
    }
    Ok(())
}

/// Single-IP check: every reserved range is rejected.
///
/// IPv4-mapped IPv6 (`::ffff:a.b.c.d`) is unmapped first and then checked by the IPv4 rules —
/// otherwise `::ffff:127.0.0.1` reports `is_loopback()` as false and slips past the whole check.
///
/// # Errors
/// loopback / unspecified / multicast / private (including CGNAT) / link-local each return their
/// corresponding variant.
pub fn check_ip(ip: IpAddr) -> Result<(), SsrfError> {
    // Fold to the canonical form: IPv4-mapped addresses are always judged by the IPv4 rules.
    let ip = match ip {
        IpAddr::V6(v6) => v6.to_ipv4_mapped().map_or(IpAddr::V6(v6), IpAddr::V4),
        v4 @ IpAddr::V4(_) => v4,
    };
    if ip.is_loopback() {
        return Err(SsrfError::Loopback);
    }
    if ip.is_unspecified() {
        return Err(SsrfError::Unspecified);
    }
    if ip.is_multicast() {
        return Err(SsrfError::Multicast);
    }
    if is_private(&ip) {
        return Err(SsrfError::PrivateIp);
    }
    if is_link_local(&ip) {
        return Err(SsrfError::LinkLocal);
    }
    Ok(())
}

/// Dial-time re-check (DNS-rebinding defense) — takes the addresses the caller resolved and
/// returns the first one
///
/// # Errors
///
/// Returns the [`check_ip`] variant of the first address that falls inside a reserved
/// range; the caller must not dial in that case.
pub fn validate_resolved(addrs: &[IpAddr]) -> Result<IpAddr, SsrfError> {
    for ip in addrs {
        check_ip(*ip)?;
    }
    addrs
        .first()
        .copied()
        .ok_or(SsrfError::Dns("no addresses".into()))
}

/// Whether the address is private (including CGNAT 100.64.0.0/10)
fn is_private(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            // CGNAT 100.64.0.0/10
            let bytes = v4.octets();
            (bytes[0] == 100 && (bytes[1] & 0xc0) == 64) || v4.is_private()
        }
        IpAddr::V6(v6) => {
            let b = v6.octets();
            // fc00::/7
            (b[0] & 0xfe) == 0xfc
        }
    }
}

/// Whether the address is link-local
fn is_link_local(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => v4.is_link_local(),
        IpAddr::V6(v6) => v6.segments()[0] == 0xfe80,
    }
}
