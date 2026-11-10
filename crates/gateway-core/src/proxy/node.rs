//! `node` — `ProxyNode`: a parsed proxy node
//!
//! Parsing is configuration mapping only, never dialing: a share link or URL becomes a
//! [`ProxyNode`] value. The protocol handshakes (HTTP CONNECT / SOCKS5 via reqwest,
//! vless/vmess/ss/trojan via meow adapters) belong to `gateway-proxy` — precisely so the
//! admin plane can name a node without pulling an egress engine into its closure.
//!
//! The reqwest projection of a node (`to_reqwest_proxy`) stays in `gateway-proxy` for the
//! same reason: it is a `reqwest::Proxy`, an engine type.

use url::Url;

/// Proxy protocol
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProxyScheme {
    Direct,
    Http,
    Socks5,
    Vless,
    Vmess,
    Shadowsocks,
    Trojan,
    Hysteria2,
    AnyTls,
    Snell,
}

/// Basic auth for HTTP/SOCKS proxies
#[derive(Debug, Clone)]
pub struct BasicAuth {
    pub user: String,
    pub pass: String,
}

/// Node transport options (parsed from the URL query)
///
/// Shared by the five protocols vless / vmess / hysteria2 / anytls / snell; always `None` for
/// others.
/// Originally named with a Vless prefix and with a field called `vless` — a name that mentioned
/// only vless was misleading, so the 0.1 series did a purely symbolic rename to `NodeOpts` / `opts`
/// with unchanged field semantics and defaults (public struct fields make it breaking).
///
/// Query keys follow common share-link conventions:
/// - `flow=xtls-rprx-vision` — XTLS-Vision inner stream mode (VLESS)
/// - `sni=<domain>` — SNI for TLS / REALITY / Hysteria2 / `AnyTLS`
/// - `pbk=` — REALITY server X25519 public key (64 hex or 43 base64url; presence marks a
///   REALITY node)
/// - `sid=<0-16hex>` — REALITY short id (all zeros by default)
/// - `insecure=1` / `allowInsecure=1` — skip certificate verification
///   (Hysteria2/AnyTLS/Trojan compatibility)
/// - `version=v4|v5` — Snell version (v5 by default)
/// - `obfs=http|tls` — Snell obfuscation (none by default)
/// - `obfs-uri=` — Snell obfs host/uri
/// - `type=ws` / `path=/xxx` — WebSocket transport (the presence of path marks a WS node)
/// - `host=<sni domain>` — WebSocket Host header (optional; defaults to SNI or host)
/// - `fp=` / `fingerprint=` — uTLS fingerprint (requires the engine's `utls` feature)
#[derive(Debug, Clone, Default)]
pub struct NodeOpts {
    /// XTLS flow; currently only `xtls-rprx-vision` is recognized
    pub flow: Option<String>,
    /// TLS / REALITY SNI
    pub sni: Option<String>,
    /// REALITY public key (64 hex or 43 base64url), paired with `sid`
    pub pbk: Option<String>,
    /// REALITY short id (hex, 0-16 characters)
    pub sid: Option<String>,
    /// Skip TLS certificate verification
    pub insecure: bool,
    /// Snell version
    pub version: Option<String>,
    /// Snell obfs type
    pub obfs: Option<String>,
    /// Snell obfs host/uri
    pub obfs_uri: Option<String>,
    /// WebSocket path (query key "path")
    pub ws_path: Option<String>,
    /// WebSocket host header (query key "host")
    pub ws_host: Option<String>,
    /// uTLS fingerprint (`fingerprint=chrome` / `fp=chrome`); requires the engine's `utls` feature
    pub fingerprint: Option<String>,
}
#[derive(Debug, Clone)]
pub struct ProxyNode {
    pub id: i64,
    pub scheme: ProxyScheme,
    pub host: String,
    pub port: u16,
    pub auth: Option<BasicAuth>,
    /// Node transport options (shared by vless/vmess/hysteria2/anytls/snell); `None` for other
    /// protocols
    pub opts: Option<NodeOpts>,
    pub channel_keys: Vec<String>,
    pub priority: i32,
}

impl ProxyNode {
    /// Parse a raw proxy URL string (accepts `http://user:pass@host:port` / `socks5://...` /
    /// `socks5h://...` / `vless://...` / `vmess://...` / `ss://...` / `trojan://...` /
    /// `hysteria2://...` / `anytls://...` / `snell://...`)
    ///
    /// - scheme mapping: http/https -> Http, socks5/socks5h -> Socks5, vless:// -> Vless,
    ///   vmess:// -> Vmess, ss:// -> Shadowsocks, trojan:// -> Trojan, hysteria2:// -> Hysteria2,
    ///   anytls:// -> `AnyTls`, snell:// -> Snell
    /// - host is required; a missing host is an error
    /// - port: vless/vmess/trojan/hysteria2/anytls/snell default to 443, socks5 to 1080, http to
    ///   8080; ss has no standard default (an explicit port is required; missing port is an error)
    /// - auth: `username()`/`password()` fill `auth` when non-empty; with only a user, pass is the
    ///   empty string
    /// - the returned id=0, `channel_keys`=[], priority=0 are filled in later by the caller
    ///
    /// # Errors
    ///
    /// Returns [`ParseError::Invalid`] when the URL does not parse, has no host,
    /// or carries an unusable port; returns the scheme-specific error when the
    /// scheme is unknown or a required field is missing.
    pub fn parse_url(url: &str) -> Result<Self, ParseError> {
        let url_obj =
            Url::parse(url).map_err(|e| ParseError::Invalid(format!("url parse failed: {e}")))?;
        let scheme = scheme_from_str(url_obj.scheme())?;
        let host = url_obj
            .host_str()
            .ok_or_else(|| ParseError::Invalid("missing host".to_string()))?;
        let port = port_for(&url_obj, scheme)?;
        let opts = opts_from_query(&url_obj, scheme);
        let auth = auth_from_url(&url_obj);
        Ok(Self {
            id: 0,
            scheme,
            host: host.to_string(),
            port,
            auth,
            opts,
            channel_keys: vec![],
            priority: 0,
        })
    }
}

/// URL scheme → [`ProxyScheme`]; an unknown scheme is an error.
fn scheme_from_str(scheme: &str) -> Result<ProxyScheme, ParseError> {
    match scheme {
        "http" | "https" => Ok(ProxyScheme::Http),
        "socks5" | "socks5h" => Ok(ProxyScheme::Socks5),
        "vless" => Ok(ProxyScheme::Vless),
        "vmess" => Ok(ProxyScheme::Vmess),
        "ss" => Ok(ProxyScheme::Shadowsocks),
        "trojan" => Ok(ProxyScheme::Trojan),
        "hysteria2" => Ok(ProxyScheme::Hysteria2),
        "anytls" => Ok(ProxyScheme::AnyTls),
        "snell" => Ok(ProxyScheme::Snell),
        _ => Err(ParseError::Invalid(format!(
            "unsupported proxy scheme: {scheme}"
        ))),
    }
}

/// Port defaults: vless/vmess/trojan/hysteria2/anytls/snell 443, socks5 1080, http 8080
/// (compatibility), ss has no standard default (explicit port required, missing is an error).
fn port_for(url: &Url, scheme: ProxyScheme) -> Result<u16, ParseError> {
    match scheme {
        ProxyScheme::Shadowsocks => url
            .port()
            .ok_or_else(|| ParseError::Invalid("ss scheme requires explicit port".to_string())),
        ProxyScheme::Socks5 => Ok(url.port().unwrap_or(1080)),
        ProxyScheme::Vless
        | ProxyScheme::Vmess
        | ProxyScheme::Trojan
        | ProxyScheme::Hysteria2
        | ProxyScheme::AnyTls
        | ProxyScheme::Snell => Ok(url.port().unwrap_or(443)),
        // Http (Direct never reaches the parser)
        _ => Ok(url.port().unwrap_or(8080)),
    }
}

/// Parse query parameters: VLESS / `VMess` (ws transport) / Hysteria2 / `AnyTLS` / Snell share
/// [`NodeOpts`]; all other protocols always get `None`.
fn opts_from_query(url: &Url, scheme: ProxyScheme) -> Option<NodeOpts> {
    let needs_opts = matches!(
        scheme,
        ProxyScheme::Vless
            | ProxyScheme::Vmess
            | ProxyScheme::Hysteria2
            | ProxyScheme::AnyTls
            | ProxyScheme::Snell
    );
    needs_opts.then(|| {
        let mut opts = NodeOpts::default();
        for (k, v) in url.query_pairs() {
            match k.as_ref() {
                "flow" => opts.flow = Some(v.into_owned()),
                "sni" => opts.sni = Some(v.into_owned()),
                "pbk" => opts.pbk = Some(v.into_owned()),
                "sid" => opts.sid = Some(v.into_owned()),
                "insecure" | "allowInsecure" => {
                    // share links conventionally use insecure=1, not a Rust bool literal
                    opts.insecure = v == "1" || v.eq_ignore_ascii_case("true");
                }
                "version" => opts.version = Some(v.into_owned()),
                "obfs" => opts.obfs = Some(v.into_owned()),
                "obfs-uri" => opts.obfs_uri = Some(v.into_owned()),
                "fp" | "fingerprint" => opts.fingerprint = Some(v.into_owned()),
                "path" => opts.ws_path = Some(v.into_owned()),
                "host" => opts.ws_host = Some(v.into_owned()),
                _ => {}
            }
        }
        opts
    })
}

/// Take auth from the URL userinfo. `Url::username()` / `password()` return percent-encoded
/// text that must be decoded: a username containing `@` arrives as `user%40domain`, and using it
/// directly makes proxy authentication fail.
fn auth_from_url(url: &Url) -> Option<BasicAuth> {
    let user = url.username();
    if user.is_empty() {
        return None;
    }
    Some(BasicAuth {
        user: percent_decode(user),
        pass: percent_decode(url.password().unwrap_or("")),
    })
}

/// Percent-decode a URL component; invalid UTF-8 sequences are handled lossily.
pub(crate) fn percent_decode(s: &str) -> String {
    percent_encoding::percent_decode_str(s)
        .decode_utf8_lossy()
        .into_owned()
}

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("invalid proxy url: {0}")]
    Invalid(String),
}
