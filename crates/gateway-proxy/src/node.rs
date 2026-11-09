//! `node` — the reqwest projection of a [`ProxyNode`]
//!
//! The node type and its URL parser live in `gateway-contract::proxy::node`: the admin plane
//! parses and validates stored links, and it must be able to do so without linking an
//! egress engine. What stays here is the part that needs `reqwest` — turning a node into a
//! `reqwest::Proxy` for `ClientBuilder::proxy()` — plus the re-export that keeps the
//! historical `gateway_proxy::node::*` paths resolving.
//!
//! The protocol handshakes (vless/vmess/ss/trojan/hysteria2/anytls/snell) are not here
//! either: reqwest cannot perform them, so [`super::adapter`] maps the node onto a meow
//! `ProxyAdapter` and the handshake happens inside its `dial_tcp`.

pub use gateway_core::proxy::node::{BasicAuth, NodeOpts, ParseError, ProxyNode, ProxyScheme};

/// Convert into a `reqwest::Proxy` (for injection via `ClientBuilder::proxy()`)
///
/// - Direct -> None
/// - Http/Socks5 -> `reqwest::Proxy::all(...)` + `basic_auth` (if any)
/// - Vless/Vmess/Shadowsocks/Trojan/Hysteria2/AnyTls/Snell -> `Err(ProxyConvertError)`
///   (these dial through the meow adapter in [`super::adapter`])
/// - auth goes through `.basic_auth()` rather than into the URL, avoiding special-character
///   escaping issues
pub fn to_reqwest_proxy(node: &ProxyNode) -> Result<Option<reqwest::Proxy>, ProxyConvertError> {
    match node.scheme {
        ProxyScheme::Direct => Ok(None),
        ProxyScheme::Http | ProxyScheme::Socks5 => {
            let prefix = if node.scheme == ProxyScheme::Http {
                "http"
            } else {
                "socks5"
            };
            let url = format!("{prefix}://{}:{}", node.host, node.port);
            let mut proxy =
                reqwest::Proxy::all(url).map_err(|e| ProxyConvertError(e.to_string()))?;
            if let Some(auth) = &node.auth {
                proxy = proxy.basic_auth(&auth.user, &auth.pass);
            }
            Ok(Some(proxy))
        }
        ProxyScheme::Vless
        | ProxyScheme::Vmess
        | ProxyScheme::Shadowsocks
        | ProxyScheme::Trojan
        | ProxyScheme::Hysteria2
        | ProxyScheme::AnyTls
        | ProxyScheme::Snell => Err(ProxyConvertError(format!(
            "scheme {:?} does not go through reqwest (dial with adapter's meow ProxyAdapter)",
            node.scheme
        ))),
    }
}

/// `reqwest::Proxy` conversion failure (`reqwest::Error` has no public constructor, hence this
/// lightweight custom error).
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct ProxyConvertError(pub String);
