//! Upstream request preparation — URL / auth header / channel policy.
//!
//! Covers the non-protocol part of the new-api adaptor five-method contract
//! (relay/adaptor/interface.go): GetRequestURL + SetupRequestHeader + ConvertRequest.
//! Protocol body conversion lives in the protocol crate; this module only does
//! **addressing and authentication**.

use gateway_dispatch::Candidate;
use gateway_pipeline::ProtocolKind;

/// Preparation output — the direct input to egress::execute.
#[derive(Debug, Clone)]
pub struct PreparedRequest {
    /// Complete upstream URL (base_url + upstream endpoint path).
    pub url: String,
    /// Auth header (such as Authorization: Bearer sk-...).
    pub auth_header: (String, String),
    /// Channel-level override headers (ChannelRecord.settings.headers).
    pub extra_headers: Vec<(String, String)>,
}

/// Auth header name (provider → header name). Path rules live in [`build_url`].
///
/// openai → Bearer (`Authorization`)
/// claude → `x-api-key`
/// gemini → `x-goog-api-key`
/// passthrough → `Authorization: Bearer` (fallback without secrets, overridable by
/// settings)
fn auth_header_for(provider: &str, secret: &str) -> (String, String) {
    let normalized = provider.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "claude" => ("x-api-key".into(), secret.into()),
        "gemini" => ("x-goog-api-key".into(), secret.into()),
        // openai / passthrough / unknown — Bearer fallback (settings can override).
        _ => ("Authorization".into(), format!("Bearer {secret}")),
    }
}

/// Build the upstream URL — the **upstream protocol** selects the endpoint template and
/// the **inbound format** distinguishes multiple endpoints of the same protocol.
///
/// The conversion layer has already encoded the request body in the upstream format, so
/// the URL must address that format's endpoint; the client path only matters under
/// passthrough (passthrough semantics: path and body are both decided by the client):
/// - `openai` → `/v1/chat/completions`; an inbound Responses request goes to
///   `/v1/responses` (a Responses client's body is encoded as Responses, so it must hit
///   the matching endpoint)
/// - `claude` → `/v1/messages` (forced; the client path is irrelevant)
/// - `gemini` → `/v1beta/models/{model}:{verb}`, verb follows stream
///   (model is the upstream real name after channel mapping; falls back to the client
///   path when there is no addressable model)
/// - `passthrough` / unknown → `{base}{path}` (the client path fully decides the target)
///
/// A trailing `/` on base and a leading `/` on path are normalized to avoid doubled or
/// missing slashes.
///
/// A trailing version prefix (`/v1`) on base is stripped during normalization: existing
/// channels in the database with `/v1` (copied from vendor docs) and the canonical form
/// without it then build the same correct path instead of degrading to `/v1/v1/...`.
/// `/v1beta` is kept (the gemini branch builds the authoritative prefix itself).
fn build_url(
    provider: &str,
    base_url: &str,
    path: &str,
    inbound: ProtocolKind,
    model: &str,
    stream: bool,
) -> String {
    let base = base_url.trim_end_matches('/');
    let normalized = provider.trim().to_ascii_lowercase();
    // Accommodate existing bases carrying `/v1` (copied from vendor docs): the
    // openai/claude/gemini branches build their own version prefix, so stripping a
    // trailing `/v1` makes both spellings yield the same path; the passthrough/unknown
    // branch forwards `{base}{path}` verbatim and does not strip (the client path carries
    // its own version segment).
    let base = match normalized.as_str() {
        "openai" | "claude" | "gemini" => base.strip_suffix("/v1").unwrap_or(base),
        _ => base,
    };
    match normalized.as_str() {
        "claude" => format!("{base}/v1/messages"),
        "gemini" => {
            let verb = if stream {
                "streamGenerateContent"
            } else {
                "generateContent"
            };
            if model.is_empty() {
                // No addressable model (non-chat request or non-JSON body): fall back
                // to the client path and let the upstream judge. Keep the old prefix
                // rule to avoid a regression.
                let trimmed = path.trim_start_matches('/');
                return if trimmed.starts_with("v1beta/") {
                    format!("{base}/{trimmed}")
                } else {
                    format!("{base}/v1beta/{trimmed}")
                };
            }
            format!("{base}/v1beta/models/{model}:{verb}")
        }
        "openai" => {
            // Responses client → Responses endpoint; every other inbound format is
            // encoded as Chat. Uses match rather than == so a new ProtocolKind variant
            // forces a decision here.
            match inbound {
                ProtocolKind::OpenAIResp => format!("{base}/v1/responses"),
                _ => format!("{base}/v1/chat/completions"),
            }
        }
        // passthrough / unknown — pass through; the client path fully decides the target.
        _ => {
            let trimmed = path.trim_start_matches('/');
            if trimmed.is_empty() {
                base.to_string()
            } else {
                format!("{base}/{trimmed}")
            }
        }
    }
}

/// Parse override headers from ChannelRecord.settings.headers (a JSON object).
///
/// Invalid structure → empty list. Header names are lowercased to match the
/// case-insensitive comparison in the later sanitize step.
pub fn extra_headers_from_settings(settings: &serde_json::Value) -> Vec<(String, String)> {
    let Some(obj) = settings.get("headers").and_then(|v| v.as_object()) else {
        return Vec::new();
    };
    let mut out = Vec::with_capacity(obj.len());
    for (k, v) in obj {
        let Some(s) = v.as_str() else { continue };
        out.push((k.to_ascii_lowercase(), s.to_string()));
    }
    out
}

/// Prepare a request by provider_type — the adapter factory.
///
/// `inbound` / `stream` / `upstream_model` decide the upstream endpoint (see
/// [`build_url`]): the upstream URL must match the converted request body's format, not
/// the path the client called. The auth header follows the provider protocol family;
/// channel settings.headers overrides can rewrite the auth header name.
pub fn prepare(
    candidate: &Candidate,
    path: &str,
    provider_type: &str,
    inbound: ProtocolKind,
    stream: bool,
    upstream_model: &str,
    extra_headers: Vec<(String, String)>,
) -> PreparedRequest {
    let url = build_url(
        provider_type,
        &candidate.base_url,
        path,
        inbound,
        upstream_model,
        stream,
    );
    let auth = auth_header_for(provider_type, &candidate.secret);
    PreparedRequest {
        url,
        auth_header: auth,
        extra_headers,
    }
}

/// Header filtering — strip hop-by-hop, credential, and conflicting headers (new-api
/// api_request.go rules).
/// - Always stripped (HTTP/1.1 RFC 7230 §6.1 hop-by-hop): `connection`, `keep-alive`,
///   `transfer-encoding`, `upgrade`, `proxy-*`
/// - Always stripped as credentials: `authorization`, `cookie`, `x-api-key`,
///   `x-goog-api-key`
/// - Always stripped as conflicting: `host`, `content-length` (reqwest recomputes it)
/// - Case-insensitive; everything else passes through (including Accept, User-Agent,
///   and so on).
pub fn sanitize_client_headers(headers: &[(String, String)]) -> Vec<(String, String)> {
    const STRIPPED: &[&str] = &[
        "connection",
        "keep-alive",
        "transfer-encoding",
        "upgrade",
        "proxy-authorization",
        "proxy-authenticate",
        "authorization",
        "cookie",
        "x-api-key",
        "x-goog-api-key",
        "host",
        "content-length",
    ];

    headers
        .iter()
        .filter(|(name, _)| {
            let lower = name.trim().to_ascii_lowercase();
            !STRIPPED.iter().any(|s| lower == *s) && !lower.starts_with("proxy-")
        })
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}
