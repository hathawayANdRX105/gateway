//! Behavior tests for share-link dialect parsing.
//!
//! Base64 payloads are always encoded inline rather than hard-coded as long strings — a
//! hard-coded blob hides what the test covers, and changing one field would require
//! manual re-encoding. Credentials use obviously fake values (`test-pass`, an obviously
//! fake UUID).

use base64::Engine;
use base64::engine::general_purpose::{STANDARD, STANDARD_NO_PAD, URL_SAFE_NO_PAD};
use gateway_proxy::node::ProxyScheme;
use gateway_proxy::{parse_share_link, parse_share_links};

const UUID: &str = "550e8400-e29b-41d4-a716-446655440000";
const PASS: &str = "test-pass";

/// Build `vmess://BASE64(JSON)`, with the JSON assembled from field fragments.
fn vmess_link(extra: &str) -> String {
    let json = format!(
        r#"{{"v":"2","ps":"node","add":"example.com","port":443,"id":"{UUID}","aid":0,"scy":"auto"{extra}}}"#
    );
    format!("vmess://{}", STANDARD.encode(json))
}

/// `VMess` payload mapping: the UUID lands in `auth.user` and host/port are in place.
///
/// The UUID slot is the easiest place to get wrong — `adapter.rs::clash_config` reads
/// `auth.user` as the `uuid` for vmess; writing it to `auth.pass` would make the assembled
/// adapter fail authentication (#98 hit the same class of bug on trojan).
#[test]
fn vmess_maps_uuid_into_auth_user() {
    let node = parse_share_link(&vmess_link(r#","net":"tcp""#)).expect("must parse successfully");
    assert_eq!(node.scheme, ProxyScheme::Vmess);
    assert_eq!(node.host, "example.com");
    assert_eq!(node.port, 443);
    let auth = node.auth.expect("vmess must have auth");
    assert_eq!(auth.user, UUID, "UUID must land in auth.user");
    assert_eq!(
        auth.pass, "",
        "scy=auto must stay empty so meow takes the default cipher"
    );
}

/// When `scy` names a concrete cipher it is written to `auth.pass` (clash's `cipher`
/// key).
///
/// The payload is built by hand without the default `scy`: the helper's `"scy":"auto"`
/// would create a duplicate JSON key, and serde derive **errors** on duplicate fields
/// rather than taking the latter (unlike Map's overwrite semantics).
#[test]
fn vmess_explicit_cipher_lands_in_auth_pass() {
    let json = format!(
        r#"{{"v":"2","add":"example.com","port":443,"id":"{UUID}","scy":"chacha20-poly1305","net":"tcp"}}"#
    );
    let node = parse_share_link(&format!("vmess://{}", STANDARD.encode(&json)))
        .expect("must parse successfully");
    assert_eq!(node.auth.unwrap().pass, "chacha20-poly1305");
}

/// A string-shaped `port` (as some Android clients emit) must also parse.
#[test]
fn vmess_accepts_string_port() {
    let json = format!(
        r#"{{"v":"2","add":"example.com","port":"8443","id":"{UUID}","net":"tcp","scy":"auto"}}"#
    );
    let node = parse_share_link(&format!("vmess://{}", STANDARD.encode(json)))
        .expect("must parse successfully");
    assert_eq!(node.port, 8443);
}

/// `net=ws` maps to `ws_path` / `ws_host`; `sni` and `fp` land in their own fields.
#[test]
fn vmess_ws_transport_maps_path_host_sni_fp() {
    let link = vmess_link(
        r#","net":"ws","path":"/vm","host":"cdn.example.com","tls":"tls","sni":"sni.example.com","fp":"chrome""#,
    );
    let opts = parse_share_link(&link)
        .expect("must parse successfully")
        .opts
        .expect("ws node must have NodeOpts");
    assert_eq!(opts.ws_path.as_deref(), Some("/vm"));
    assert_eq!(opts.ws_host.as_deref(), Some("cdn.example.com"));
    assert_eq!(opts.sni.as_deref(), Some("sni.example.com"));
    assert_eq!(opts.fingerprint.as_deref(), Some("chrome"));
}

/// When `net=ws` omits the path it defaults to `/` — clash's ws-opts requires a path,
/// while share links often leave it out.
#[test]
fn vmess_ws_without_path_defaults_to_slash() {
    let opts = parse_share_link(&vmess_link(r#","net":"ws""#))
        .expect("must parse successfully")
        .opts
        .expect("must have NodeOpts");
    assert_eq!(opts.ws_path.as_deref(), Some("/"));
}

/// With `tls=true` but no `sni`, fall back to the server address: a TLS handshake needs
/// an SNI.
#[test]
fn vmess_tls_without_sni_falls_back_to_server_address() {
    let opts = parse_share_link(&vmess_link(r#","net":"tcp","tls":"tls""#))
        .expect("must parse successfully")
        .opts
        .expect("must have NodeOpts");
    assert_eq!(opts.sni.as_deref(), Some("example.com"));
}

/// `net=grpc` errors instead of silently degrading.
///
/// `NodeOpts` has no grpc field, and degrading to tcp would dial successfully but use the
/// wrong transport — that failure shows up at runtime as "connected but no response",
/// far harder to diagnose than an import-time error.
#[test]
fn vmess_unsupported_transport_errors() {
    let err = parse_share_link(&vmess_link(r#","net":"grpc""#))
        .expect_err("grpc transport layer should error")
        .to_string();
    assert!(
        err.contains("grpc"),
        "error should name grpc, actual: {err}"
    );
}

/// A payload without a UUID errors (authentication is impossible without an id).
#[test]
fn vmess_missing_uuid_errors() {
    let json = r#"{"v":"2","add":"example.com","port":443,"id":"","net":"tcp"}"#;
    assert!(parse_share_link(&format!("vmess://{}", STANDARD.encode(json))).is_err());
}

/// Bad base64 returns an error without panicking: subscription content is untrusted.
#[test]
fn vmess_bad_base64_errors_without_panic() {
    assert!(parse_share_link("vmess://!!!not-base64!!!").is_err());
}

/// When base64 decodes but the result is not JSON, it errors.
#[test]
fn vmess_non_json_payload_errors() {
    let link = format!("vmess://{}", STANDARD.encode("this is not JSON"));
    assert!(parse_share_link(&link).is_err());
}

/// SS legacy: the **whole string** is base64 (including host:port).
///
/// This is the key difference from SIP002 — legacy encodes `@host:port` into the base64
/// blob too, and reversing the detection order would decode SIP002's userinfo as a
/// whole-string payload and produce garbage.
#[test]
fn ss_legacy_whole_string_base64() {
    let payload = STANDARD.encode(format!("aes-128-gcm:{PASS}@example.com:8388"));
    let node =
        parse_share_link(&format!("ss://{payload}#remark")).expect("must parse successfully");
    assert_eq!(node.scheme, ProxyScheme::Shadowsocks);
    assert_eq!(node.host, "example.com");
    assert_eq!(node.port, 8388);
    let auth = node.auth.expect("ss must have auth");
    assert_eq!(auth.user, "aes-128-gcm", "cipher lands in auth.user");
    assert_eq!(auth.pass, PASS, "password lands in auth.pass");
}

/// SS SIP002: only the userinfo is base64; host:port stays in plain text.
#[test]
fn ss_sip002_base64_userinfo_with_plain_host() {
    let userinfo = STANDARD.encode(format!("chacha20-ietf-poly1305:{PASS}"));
    let node = parse_share_link(&format!("ss://{userinfo}@example.com:8389"))
        .expect("must parse successfully");
    assert_eq!(node.host, "example.com");
    assert_eq!(node.port, 8389);
    let auth = node.auth.unwrap();
    assert_eq!(auth.user, "chacha20-ietf-poly1305");
    assert_eq!(auth.pass, PASS);
}

/// A SIP002 `?plugin=` query must not be treated as part of the host.
#[test]
fn ss_sip002_strips_plugin_query_from_host() {
    let userinfo = STANDARD_NO_PAD.encode(format!("aes-256-gcm:{PASS}"));
    let node = parse_share_link(&format!(
        "ss://{userinfo}@example.com:8390?plugin=obfs-local"
    ))
    .expect("must parse successfully");
    assert_eq!(node.host, "example.com");
    assert_eq!(node.port, 8390);
}

/// Plain-text userinfo form (`ss://method:pass@host:port`).
#[test]
fn ss_plain_userinfo() {
    let node = parse_share_link(&format!("ss://aes-128-gcm:{PASS}@example.com:8391"))
        .expect("must parse successfully");
    let auth = node.auth.unwrap();
    assert_eq!(auth.user, "aes-128-gcm");
    assert_eq!(auth.pass, PASS);
}

/// When the password contains `:`, split only at the first colon (a colon is legal inside
/// a password).
#[test]
fn ss_password_containing_colon_is_preserved() {
    let userinfo = STANDARD.encode("aes-128-gcm:pa:ss:word");
    let node = parse_share_link(&format!("ss://{userinfo}@example.com:8392"))
        .expect("must parse successfully");
    assert_eq!(node.auth.unwrap().pass, "pa:ss:word");
}

/// Base64 without padding must decode: `=` needs escaping in a URL, so share links
/// commonly omit it.
#[test]
fn ss_base64_without_padding_decodes() {
    let payload = STANDARD_NO_PAD.encode(format!("aes-128-gcm:{PASS}@example.com:8393"));
    assert!(!payload.ends_with('='), "this case tests no-padding input");
    let node = parse_share_link(&format!("ss://{payload}")).expect("no-padding should decode");
    assert_eq!(node.port, 8393);
}

/// Base64 in the URL-safe alphabet must also decode (clients are not consistent).
#[test]
fn ss_url_safe_alphabet_decodes() {
    // `?` and `~` encode to `-` / `_`, exactly where the two alphabets differ.
    let creds = "aes-128-gcm:p?a~ss@example.com:8394";
    let payload = URL_SAFE_NO_PAD.encode(creds);
    let node = parse_share_link(&format!("ss://{payload}")).expect("URL-safe should decode");
    assert_eq!(node.host, "example.com");
    assert_eq!(node.port, 8394);
}

/// ss has no standard default port; a missing port must error rather than guess one.
#[test]
fn ss_missing_port_errors() {
    let payload = STANDARD.encode(format!("aes-128-gcm:{PASS}@example.com"));
    assert!(parse_share_link(&format!("ss://{payload}")).is_err());
}

/// Non-dialect schemes delegate to `parse_url`: vless query options must be parsed.
#[test]
fn non_dialect_scheme_delegates_to_parse_url() {
    let node = parse_share_link(&format!(
        "vless://{UUID}@example.com:443?sni=a.example.com&type=ws&path=/x"
    ))
    .expect("vless should be handled by parse_url");
    assert_eq!(node.scheme, ProxyScheme::Vless);
    let opts = node.opts.expect("vless must have opts");
    assert_eq!(opts.sni.as_deref(), Some("a.example.com"));
    assert_eq!(opts.ws_path.as_deref(), Some("/x"));
}

/// Batch parsing: bad lines go to failures while good lines still enter nodes.
#[test]
fn batch_isolates_bad_lines() {
    let good = format!(
        "ss://{}",
        STANDARD.encode(format!("aes-128-gcm:{PASS}@a.example.com:1"))
    );
    let text = format!("{good}\nvmess://!!!broken!!!\n{good}");
    let batch = parse_share_links(&text);
    assert_eq!(batch.nodes.len(), 2, "both good lines should become nodes");
    assert_eq!(batch.failures.len(), 1, "one bad line should be isolated");
}

/// Blank lines and `#` comment lines are ignored: they produce no node and no failure.
#[test]
fn batch_ignores_blank_and_comment_lines() {
    let good = format!(
        "ss://{}",
        STANDARD.encode(format!("aes-128-gcm:{PASS}@a.example.com:1"))
    );
    let batch = parse_share_links(&format!("\n# this is a comment\n  \n{good}\n"));
    assert_eq!(batch.nodes.len(), 1);
    assert!(
        batch.failures.is_empty(),
        "comments and blank lines must not count as failures"
    );
}

/// **Security assertion**: the raw line echoed for a failure must be masked and must not
/// leak the password.
///
/// `failures` goes to logs and back to the frontend. A broken vmess payload yields no
/// host, so the only option is `vmess://***` — never put the original text containing the
/// password back.
#[test]
fn batch_failure_source_never_leaks_credentials() {
    // The payload base64 decodes but the JSON lacks an id; on failure the original line
    // contains the password in clear.
    let json = format!(r#"{{"add":"example.com","port":443,"id":"","net":"tcp","scy":"{PASS}"}}"#);
    let link = format!("vmess://{}", STANDARD.encode(&json));
    let batch = parse_share_links(&link);
    assert_eq!(batch.failures.len(), 1);
    let (masked, _reason) = &batch.failures[0];
    assert!(
        !masked.contains(PASS),
        "must not contain password after masking, actual: {masked}"
    );
    assert!(
        !masked.contains(&STANDARD.encode(&json)),
        "must not contain the original base64 payload (it can be decoded), actual: {masked}"
    );
    assert!(masked.starts_with("vmess://"), "retain scheme for locating");
}

/// Masking keeps the host for identification but drops the userinfo.
#[test]
fn masked_link_keeps_host_drops_userinfo() {
    let batch = parse_share_links(&format!("ss://aes-128-gcm:{PASS}@example.com"));
    assert_eq!(batch.failures.len(), 1, "missing port should fail");
    let (masked, _) = &batch.failures[0];
    assert!(
        !masked.contains(PASS),
        "must not contain password: {masked}"
    );
    assert!(
        masked.contains("example.com"),
        "retain host for locating: {masked}"
    );
}
