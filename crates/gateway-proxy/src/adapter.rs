//! `adapter` — the mapping layer from [`ProxyNode`] to meow egress adapters.
//!
//! Protocol assembly is delegated to [`meow_config::proxy_parser::parse_proxy`]: it takes
//! a clash-style config map and assembles the protocol itself
//! (ss/trojan/vless/vmess/hysteria2/anytls/snell), the transport chain
//! (ws/grpc/h2/httpupgrade), and TLS (including REALITY and uTLS fingerprints).
//!
//! Only **field mapping** happens here: `ProxyNode` → clash map. meow-config absorbs the
//! combinatorial explosion of protocols and transports; no `TransportChain::push` or
//! `TlsConfig` assembly appears here — that was exactly where the previous 400-line
//! hand-written branch came from.
//!
//! scheme → auth field semantics (consistent with the `ss://` parsing convention, see PR
//! #81):
//! - Shadowsocks: `auth.user` = cipher method name, `auth.pass` = password
//! - VLESS / VMess: `auth.user` = UUID; for VMess, `auth.pass` = security (the cipher)
//! - Trojan / Hysteria2 / AnyTLS / Snell: the password lives in `auth.user`
//!   (`trojan://pass@host`)

use std::collections::HashMap;
use std::sync::Arc;

use meow_common::{ConnType, DnsMode, Metadata, Network, ProxyAdapter};
use serde_yaml::Value as Yaml;
use smol_str::SmolStr;

use gateway_core::proxy::node::{BasicAuth, NodeOpts, ProxyNode, ProxyScheme};

/// Build the TCP egress [`Metadata`] pointing at `host:port`.
///
/// `Metadata` implements `Default`, but the egress path sets every field explicitly: the
/// destination (`host` / `dst_port`) is required, while the remaining fields are
/// ingress-side source statistics (src_geo_ip / process / in_user, etc.) and stay empty for
/// the egress direction.
pub fn tcp_metadata(host: &str, port: u16) -> Metadata {
    Metadata {
        network: Network::Tcp,
        conn_type: ConnType::Http,
        src_ip: None,
        dst_ip: None,
        src_port: 0,
        dst_port: port,
        host: SmolStr::from(host),
        dns_mode: DnsMode::Normal,
        process: SmolStr::default(),
        process_path: SmolStr::default(),
        uid: None,
        dscp: None,
        src_geo_ip: Vec::new(),
        dst_geo_ip: Vec::new(),
        sniff_host: SmolStr::default(),
        in_name: SmolStr::default(),
        in_port: 0,
        in_user: None,
        special_proxy: SmolStr::default(),
    }
}

/// Build the meow egress adapter for a node.
///
/// # Returns
/// - `Direct` / `Http` / `Socks5` return `None`: `Http` / `Socks5` are handled by reqwest
///   directly (`ProxyNode::to_reqwest_proxy`) and `Direct` has no adapter, so callers use
///   `ProxyClient::Reqwest` for all three
/// - Invalid config (missing auth, malformed UUID, unknown cipher, bad REALITY
///   parameters, …): `tracing::warn!` then return `None`, and the caller falls back to
///   direct — a bad node config must not panic the gateway. The concrete decision is made
///   by meow-config and its error message is passed through to the log verbatim
pub fn adapter_for(node: &ProxyNode) -> Option<Arc<dyn ProxyAdapter>> {
    // http/socks5 are handled by reqwest's socks feature and never enter meow; direct has
    // no adapter
    let proxy_type = match node.scheme {
        ProxyScheme::Direct | ProxyScheme::Http | ProxyScheme::Socks5 => return None,
        ProxyScheme::Shadowsocks => "ss",
        ProxyScheme::Trojan => "trojan",
        ProxyScheme::Vless => "vless",
        ProxyScheme::Vmess => "vmess",
        ProxyScheme::Hysteria2 => "hysteria2",
        ProxyScheme::AnyTls => "anytls",
        ProxyScheme::Snell => "snell",
    };
    let config = clash_config(node, proxy_type);
    match meow_config::proxy_parser::parse_proxy(&config) {
        Ok(proxy) => Some(proxy),
        Err(e) => {
            tracing::warn!(
                host = %node.host,
                port = node.port,
                scheme = ?node.scheme,
                error = %e,
                "invalid node config, falling back to direct"
            );
            None
        }
    }
}

/// `ProxyNode` → clash-style config map (the input to `parse_proxy`).
///
/// Key names follow the clash/mihomo convention and correspond one-to-one with
/// meow-config's parser; omitted items are never written so meow-config applies its own
/// defaults (for example `udp: false`, `skip-cert-verify: false`).
fn clash_config(node: &ProxyNode, proxy_type: &str) -> HashMap<String, Yaml> {
    let mut config = HashMap::new();
    put(
        &mut config,
        "name",
        str_val(&format!("node-{}-{proxy_type}", node.id)),
    );
    put(&mut config, "type", str_val(proxy_type));
    put(&mut config, "server", str_val(&node.host));
    put(&mut config, "port", Yaml::Number(node.port.into()));

    if let Some(auth) = &node.auth {
        put_auth(&mut config, auth, proxy_type);
    }

    let Some(opts) = node.opts.as_ref() else {
        return config;
    };
    put_tls(&mut config, opts);
    if let Some(flow) = opts.flow.as_deref().filter(|s| !s.is_empty()) {
        put(&mut config, "flow", str_val(flow));
    }
    put_ws(&mut config, opts, &node.host);
    put_snell(&mut config, opts);
    config
}

/// Auth field mapping: `parse_url` splits URL userinfo into user/pass — for protocols with
/// a `scheme://pass@host` shape (trojan/hysteria2/anytls/snell/vless/vmess) the password
/// or UUID lands in user and pass is empty; only `ss://cipher:pass@` has both fields set.
/// Clash-side key names: ss→cipher+password, vless/vmess→uuid, snell→psk, others→password.
fn put_auth(config: &mut HashMap<String, Yaml>, auth: &BasicAuth, proxy_type: &str) {
    match proxy_type {
        "ss" => {
            put(config, "cipher", str_val(&auth.user));
            put(config, "password", str_val(&auth.pass));
        }
        "vless" | "vmess" => put(config, "uuid", str_val(&auth.user)),
        "snell" => put(config, "psk", str_val(&auth.user)),
        _ => put(config, "password", str_val(&auth.user)),
    }
    // VMess encryption reuses auth.pass (legacy convention); an empty value lets meow use
    // its default, auto
    if proxy_type == "vmess" && !auth.pass.is_empty() {
        put(config, "cipher", str_val(&auth.pass));
    }
}

/// TLS / REALITY / uTLS fingerprint fields.
fn put_tls(config: &mut HashMap<String, Yaml>, opts: &NodeOpts) {
    if let Some(sni) = opts.sni.as_deref().filter(|s| !s.is_empty()) {
        put(config, "tls", Yaml::Bool(true));
        put(config, "sni", str_val(sni));
        put(config, "servername", str_val(sni));
    }
    if opts.insecure {
        put(config, "skip-cert-verify", Yaml::Bool(true));
    }
    if let Some(pbk) = opts.pbk.as_deref().filter(|s| !s.is_empty()) {
        // REALITY must sit on top of TLS; share links often provide pbk without security=tls
        put(config, "tls", Yaml::Bool(true));
        let mut reality = serde_yaml::Mapping::new();
        reality.insert(str_val("public-key"), str_val(pbk));
        if let Some(sid) = opts.sid.as_deref().filter(|s| !s.is_empty()) {
            reality.insert(str_val("short-id"), str_val(sid));
        }
        put(config, "reality-opts", Yaml::Mapping(reality));
        // meow-config requires client-fingerprint with REALITY, defaulting to chrome
        put(
            config,
            "client-fingerprint",
            str_val(opts.fingerprint.as_deref().unwrap_or("chrome")),
        );
    } else if let Some(fp) = opts.fingerprint.as_deref().filter(|s| !s.is_empty()) {
        put(config, "client-fingerprint", str_val(fp));
    }
}

/// WebSocket transport: a present path is treated as ws (matching the share-link
/// convention).
fn put_ws(config: &mut HashMap<String, Yaml>, opts: &NodeOpts, node_host: &str) {
    let Some(path) = opts.ws_path.as_deref().filter(|s| !s.is_empty()) else {
        return;
    };
    put(config, "network", str_val("ws"));
    let mut ws = serde_yaml::Mapping::new();
    ws.insert(str_val("path"), str_val(path));
    let host = opts
        .ws_host
        .as_deref()
        .or(opts.sni.as_deref())
        .unwrap_or(node_host);
    let mut headers = serde_yaml::Mapping::new();
    headers.insert(str_val("Host"), str_val(host));
    ws.insert(str_val("headers"), Yaml::Mapping(headers));
    put(config, "ws-opts", Yaml::Mapping(ws));
}

/// Snell version and obfuscation fields (other protocols lack these opts, so they never
/// match).
fn put_snell(config: &mut HashMap<String, Yaml>, opts: &NodeOpts) {
    if let Some(v) = opts.version.as_deref().filter(|s| !s.is_empty()) {
        // clash uses numeric versions (3/4/5), while links often write v5
        let digits: String = v.chars().filter(char::is_ascii_digit).collect();
        if let Ok(n) = digits.parse::<u64>() {
            put(config, "version", Yaml::Number(n.into()));
        }
    }
    if let Some(obfs) = opts.obfs.as_deref().filter(|s| !s.is_empty()) {
        let mut m = serde_yaml::Mapping::new();
        m.insert(str_val("mode"), str_val(obfs));
        if let Some(uri) = opts.obfs_uri.as_deref().filter(|s| !s.is_empty()) {
            m.insert(str_val("host"), str_val(uri));
        }
        put(config, "obfs-opts", Yaml::Mapping(m));
    }
}

/// Insert one clash config entry (keys are uniformly converted to `String`).
fn put(config: &mut HashMap<String, Yaml>, key: &str, value: Yaml) {
    config.insert(key.to_string(), value);
}

fn str_val(s: &str) -> Yaml {
    Yaml::String(s.to_string())
}
