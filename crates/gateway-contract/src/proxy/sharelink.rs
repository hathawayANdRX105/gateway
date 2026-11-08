//! `sharelink` — share-link dialect parser
//!
//! [`ProxyNode::parse_url`] is the standard URL parser, but cannot handle two mainstream
//! dialects:
//!
//! | Dialect | Shape | Why `parse_url` cannot handle it |
//! |---|---|---|
//! | VMess v2rayN | `vmess://BASE64(JSON)` | The payload is entirely base64; `Url::parse` treats it as the host |
//! | SS legacy | `ss://BASE64(method:pass@host:port)#remark` | The **whole string** (including host:port) is inside the base64 |
//! | SS SIP002 | `ss://BASE64(method:pass)@host:port#remark` | Only userinfo is base64; host is plaintext |
//! | SS plaintext | `ss://method:pass@host:port` | Already supported; delegate directly |
//!
//! Mixing dialects into `parse_url` would degrade it from a URL parser into a dialect dispatcher,
//! so this is a separate module: dispatch by scheme to dedicated parsers, and delegate every
//! other scheme unchanged to `parse_url`.
//!
//! This lives with the vocabulary rather than with the egress engine because the admin plane
//! parses stored links during CRUD validation and bulk import; requiring the engine (and its
//! TLS/protocol stack) just to parse a string is what the injection surface exists to avoid.
//!
//! VMess JSON field mapping to [`ProxyNode`] (consistent with the auth semantics the engine's
//! adapter assembly expects; mistakes cause authentication failures after assembly):
//!
//! - `id` → `auth.user` (UUID)
//! - `scy` → `auth.pass` (VMess security); `auto` / empty stays blank so the adapter uses its
//!   default
//! - `add` → `host`, `port` → port (accepts both numeric and string forms)
//! - `net=ws`: `path` → [`NodeOpts::ws_path`], `host` → `ws_host`
//! - `sni` → `NodeOpts::sni`; with `tls=true` and no `sni`, fall back to `add` (TLS requires SNI)
//! - `fp` → `NodeOpts::fingerprint`
//! - `aid` (alterId): obsolete after VMess AEAD; the adapter ignores this key, so it is discarded
//! - `net=grpc`/`h2`/`httpupgrade`: [`NodeOpts`] has no fields for these transports; **error instead
//!   of silently degrading** — degrading to tcp would dial successfully but use the wrong
//!   transport, making diagnosis far more expensive than failing at import time

use base64::Engine;
use base64::engine::general_purpose::{STANDARD_NO_PAD, URL_SAFE_NO_PAD};
use serde::Deserialize;
use serde_json::Value as JsonValue;

use crate::proxy::node::{BasicAuth, NodeOpts, ParseError, ProxyNode, ProxyScheme, percent_decode};

/// JSON structure of the v2rayN `vmess://` base64 payload.
///
/// Field names are v2rayN's historical abbreviations. `port` / `aid` may be numbers or strings
/// across clients (v2rayN writes numbers, some Android clients write strings), so [`JsonValue`]
/// captures them for local parsing.
#[derive(Debug, Clone, Deserialize)]
pub struct VmessShareLink {
    /// Protocol version, fixed `"2"`. Recorded only; not mapped.
    #[serde(rename = "v", default)]
    pub v: String,
    /// Remark / label.
    #[serde(rename = "ps", default)]
    pub ps: Option<String>,
    /// Server address.
    #[serde(rename = "add")]
    pub add: String,
    /// Port, number or string.
    #[serde(rename = "port")]
    pub port: JsonValue,
    /// User UUID.
    #[serde(rename = "id")]
    pub id: String,
    /// alterId, obsolete after VMess AEAD; discarded after parsing.
    #[serde(rename = "aid", default)]
    pub aid: JsonValue,
    /// Cipher (security).
    #[serde(rename = "scy", default)]
    pub scy: String,
    /// Transport: `tcp` / `ws` / `grpc` / `h2` / `httpupgrade`.
    #[serde(rename = "net", default)]
    pub net: String,
    /// Camouflage type (`type`); currently not mapped.
    #[serde(rename = "type", default)]
    pub type_: String,
    /// WebSocket Host header.
    #[serde(rename = "host", default)]
    pub host: Option<String>,
    /// WebSocket path.
    #[serde(rename = "path", default)]
    pub path: Option<String>,
    /// Whether TLS is enabled. v2rayN writes `"tls"` / `""` strings; some clients write a bool.
    #[serde(rename = "tls", default)]
    pub tls: Option<JsonValue>,
    /// TLS SNI.
    #[serde(rename = "sni", default)]
    pub sni: Option<String>,
    /// uTLS fingerprint.
    #[serde(rename = "fp", default)]
    pub fp: Option<String>,
    /// ALPN list; currently not mapped (`NodeOpts` has no such field).
    #[serde(rename = "alpn", default)]
    pub alpn: Option<JsonValue>,
}

/// Parse result for a batch of share links: successes and failures collected separately.
///
/// Why not `Vec<Result<..>>`: callers (subscriptions / bulk import) need separate counts and
/// display; splitting them avoids a partition pass. The first element of `failures` is the
/// **masked** original line — originals contain passwords and UUIDs and must not reach logs or
/// the frontend.
pub struct ShareLinkBatch {
    /// Successfully parsed nodes (`id` = 0, `channel_keys` empty, `priority` = 0; filled by the
    /// caller).
    pub nodes: Vec<ProxyNode>,
    /// Failed lines: `(masked original line, failure reason)`.
    pub failures: Vec<(String, String)>,
}

/// Parse a single share link → [`ProxyNode`].
///
/// `vmess://` uses the base64-JSON dialect; `ss://` supports legacy / SIP002 / plaintext; all
/// other schemes (vless / trojan / hysteria2 / anytls / snell / http / socks5) are delegated
/// unchanged to [`ProxyNode::parse_url`].
///
/// Scheme detection uses a string prefix rather than `Url::parse` first: the vmess base64 payload
/// is not a valid URL and parsing it first would fail outright.
pub fn parse_share_link(link: &str) -> Result<ProxyNode, ParseError> {
    let lower = link.trim().to_ascii_lowercase();
    if lower.starts_with("vmess://") {
        parse_vmess_dialect(link.trim())
    } else if lower.starts_with("ss://") {
        parse_ss_dialect(link.trim())
    } else {
        ProxyNode::parse_url(link.trim())
    }
}

/// Multi-line batch parsing: one link per line; skip blank lines and comment lines starting with
/// `#`.
///
/// A single-line failure goes only into `failures` and does not affect the batch — one bad link
/// in a subscription is normal.
pub fn parse_share_links(text: &str) -> ShareLinkBatch {
    let mut batch = ShareLinkBatch {
        nodes: Vec::new(),
        failures: Vec::new(),
    };
    for raw in text.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        match parse_share_link(line) {
            Ok(node) => batch.nodes.push(node),
            Err(e) => batch.failures.push((mask_link(line), e.to_string())),
        }
    }
    batch
}

/// Mask one share link for logs and error display: keep only the scheme and a host skeleton.
///
/// **vmess needs a special case**: a `vmess://BASE64` payload is all alphanumeric and
/// `Url::parse` treats it as a valid host and preserves it — the masked result would carry the
/// entire decodable payload, defeating masking. The vmess authority is the payload itself, so do
/// not parse it; return `vmess://***`.
///
/// Other schemes are masked as URLs: drop userinfo, path, and query (userinfo and query can hold
/// credentials); keep the host and port for locating. Must handle **invalid** raw lines (missing
/// host, too long, odd chars) — the fallback is only `<scheme>://***`, never the original text.
pub fn mask_link(link: &str) -> String {
    let link = link.trim();
    let scheme = link
        .split_once("://")
        .map(|(s, _)| s.to_ascii_lowercase())
        .unwrap_or_else(|| "?".to_string());
    if scheme == "vmess" {
        return "vmess://***".to_string();
    }
    match url::Url::parse(link) {
        Ok(u) => u.host_str().map_or_else(
            || format!("{scheme}://***"),
            |host| {
                let port = u.port().map(|p| format!(":{p}")).unwrap_or_default();
                format!("{scheme}://***@{host}{port}")
            },
        ),
        Err(_) => format!("{scheme}://***"),
    }
}

/// `vmess://BASE64(JSON)` → [`ProxyNode`] (mapping conventions in the module header).
fn parse_vmess_dialect(link: &str) -> Result<ProxyNode, ParseError> {
    let v = decode_vmess_share_link(link)?;

    if v.add.trim().is_empty() {
        return Err(invalid("vmess payload missing add (server address)"));
    }
    if v.id.trim().is_empty() {
        return Err(invalid("vmess payload missing id (UUID)"));
    }
    let port = json_port(&v.port).ok_or_else(|| invalid("vmess payload has invalid port"))?;
    let opts = vmess_opts(&v)?;

    // When scy=auto / empty, leave it blank so the adapter uses its own default.
    let scy = v.scy.trim();
    let cipher = if scy.is_empty() || scy.eq_ignore_ascii_case("auto") {
        String::new()
    } else {
        scy.to_string()
    };

    Ok(ProxyNode {
        id: 0,
        scheme: ProxyScheme::Vmess,
        host: v.add.trim().to_string(),
        port,
        auth: Some(BasicAuth {
            user: v.id.trim().to_string(),
            pass: cipher,
        }),
        opts: Some(opts),
        channel_keys: vec![],
        priority: 0,
    })
}

/// Decode the `vmess://` base64(JSON) payload; strip `#remark` only once.
fn decode_vmess_share_link(link: &str) -> Result<VmessShareLink, ParseError> {
    let payload = link
        .get("vmess://".len()..)
        .ok_or_else(|| invalid("vmess link missing payload"))?;
    // Share links often append `#remark` after the base64.
    let payload = payload.split('#').next().unwrap_or(payload);
    let json = decode_base64(payload).map_err(|e| invalid(&format!("vmess payload {e}")))?;
    serde_json::from_str(&json)
        .map_err(|e| invalid(&format!("vmess payload is not valid JSON: {e}")))
}

/// Map vmess transport / TLS / fingerprint fields into [`NodeOpts`].
fn vmess_opts(v: &VmessShareLink) -> Result<NodeOpts, ParseError> {
    // Per v2rayN convention, an empty net string is treated as tcp.
    let net = v.net.trim().to_ascii_lowercase();
    let net = if net.is_empty() { "tcp" } else { net.as_str() };
    let tls = json_truthy(v.tls.as_ref());

    let mut opts = NodeOpts::default();
    match net {
        "tcp" => {}
        "ws" => {
            // path defaults to `/`: clash's ws-opts requires a path, but share links often omit
            // it.
            opts.ws_path = Some(
                v.path
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .unwrap_or("/")
                    .to_string(),
            );
            opts.ws_host = v
                .host
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string);
        }
        other => {
            return Err(invalid(&format!(
                "vmess transport `{other}` is not supported (NodeOpts has no corresponding field; silent degradation would use the wrong transport)"
            )));
        }
    }
    opts.sni = v
        .sni
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    if tls && opts.sni.is_none() {
        // TLS requires SNI; when the share link omits it, fall back to the server address
        // (consistent with mainstream clients).
        opts.sni = Some(v.add.trim().to_string());
    }
    opts.fingerprint =
        v.fp.as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
    Ok(opts)
}

/// `ss://` three forms → [`ProxyNode`].
///
/// Detection order: first treat the link as SIP002 / plaintext. An `@` outside the base64
/// separates userinfo from host, and splitting on the last `@` yields the hostpart. Only when
/// there is no such `@` decode the whole string as legacy base64 and reparse. Doing it the other
/// way would decode SIP002's base64 userinfo as the whole string and produce garbage.
fn parse_ss_dialect(link: &str) -> Result<ProxyNode, ParseError> {
    let body = link
        .get("ss://".len()..)
        .ok_or_else(|| invalid("ss link missing payload"))?;
    let body = body.split('#').next().unwrap_or(body);

    if let Some((userinfo, hostpart)) = body.rsplit_once('@') {
        // SIP002: userinfo is base64(method:password); in plaintext form it is method:password
        // directly.
        let creds = match decode_base64(userinfo) {
            Ok(decoded) if decoded.contains(':') => decoded,
            // If decoding fails or yields no `:`, it was plaintext userinfo to begin with.
            _ => percent_decode(userinfo),
        };
        let (cipher, password) = split_creds(&creds)?;
        // query (plugin=, etc.) follows host; strip it before splitting host:port.
        let hostpart = hostpart.split('?').next().unwrap_or(hostpart);
        let (host, port) = split_host_port(hostpart)?;
        return Ok(ss_node(cipher, password, host, port));
    }

    // legacy: the whole string is base64(method:password@host:port)
    let decoded = decode_base64(body).map_err(|e| invalid(&format!("ss payload {e}")))?;
    let (creds, hostpart) = decoded
        .rsplit_once('@')
        .ok_or_else(|| invalid("ss legacy payload missing `@host:port`"))?;
    let (cipher, password) = split_creds(creds)?;
    let (host, port) = split_host_port(hostpart)?;
    Ok(ss_node(cipher, password, host, port))
}

/// Assemble an ss node: `auth.user` = cipher, `auth.pass` = password (the engine's adapter
/// assembly reads them in this order).
fn ss_node(cipher: &str, password: &str, host: &str, port: u16) -> ProxyNode {
    ProxyNode {
        id: 0,
        scheme: ProxyScheme::Shadowsocks,
        host: host.to_string(),
        port,
        auth: Some(BasicAuth {
            user: cipher.to_string(),
            pass: password.to_string(),
        }),
        opts: None,
        channel_keys: vec![],
        priority: 0,
    }
}

/// Split `method:password`. The password may contain `:`, so split only at the first colon.
fn split_creds(creds: &str) -> Result<(&str, &str), ParseError> {
    let (cipher, password) = creds
        .split_once(':')
        .ok_or_else(|| invalid("ss credentials are not in `method:password` form"))?;
    if cipher.is_empty() {
        return Err(invalid("ss missing cipher method"));
    }
    Ok((cipher, password))
}

/// Split `host:port`, tolerating IPv6 literals. ss has no standard default port, so a missing
/// port is an error.
fn split_host_port(hostpart: &str) -> Result<(&str, u16), ParseError> {
    let (host, port) = hostpart
        .rsplit_once(':')
        .ok_or_else(|| invalid("ss missing explicit port"))?;
    let host = host.strip_prefix('[').unwrap_or(host);
    let host = host.strip_suffix(']').unwrap_or(host);
    if host.is_empty() {
        return Err(invalid("ss missing server address"));
    }
    let port: u16 = port
        .parse()
        .map_err(|_| invalid(&format!("ss port `{port}` invalid")))?;
    Ok((host, port))
}

/// Decode base64: tolerate missing padding and try both the standard and URL-safe alphabets.
///
/// Share links commonly omit padding (`=` must be escaped in URLs), and clients differ on the
/// alphabet, so both must be tried. Decoded bytes must be valid UTF-8 (the payload is JSON or
/// ASCII credentials).
fn decode_base64(s: &str) -> Result<String, String> {
    let trimmed = s.trim().trim_end_matches('=');
    let bytes = STANDARD_NO_PAD
        .decode(trimmed)
        .or_else(|_| URL_SAFE_NO_PAD.decode(trimmed))
        .map_err(|e| format!("not valid base64: {e}"))?;
    String::from_utf8(bytes).map_err(|_| "base64-decoded content is not valid UTF-8".to_string())
}

/// v2rayN's `port` field may be a number or a string; both decode to `u16`.
fn json_port(v: &JsonValue) -> Option<u16> {
    match v {
        JsonValue::Number(n) => u16::try_from(n.as_u64()?).ok(),
        JsonValue::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}

/// Truthiness of the `tls` field: v2rayN writes `"tls"` / `""`; some clients write `true` /
/// `false`.
fn json_truthy(v: Option<&JsonValue>) -> bool {
    match v {
        Some(JsonValue::Bool(b)) => *b,
        Some(JsonValue::String(s)) => {
            let s = s.trim();
            !s.is_empty() && !s.eq_ignore_ascii_case("none")
        }
        _ => false,
    }
}

fn invalid(msg: &str) -> ParseError {
    ParseError::Invalid(msg.to_string())
}
