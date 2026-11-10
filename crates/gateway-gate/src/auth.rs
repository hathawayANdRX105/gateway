//! `auth` — gate 1: token extraction + hash lookup + `auth_version` check

use std::sync::Arc;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use sha2::{Digest, Sha256};

use super::TokenInfo;
use super::chain::{Gate, GateCtx};
use super::error::Rejection;
use super::snapshot::TokenSnapshot;
use super::snapshot::TokenView;

pub struct AuthGate {
    tokens: Arc<ArcSwap<TokenSnapshot>>,
}

impl AuthGate {
    pub fn new(tokens: Arc<ArcSwap<TokenSnapshot>>) -> Self {
        Self { tokens }
    }
}

#[async_trait]
impl Gate for AuthGate {
    fn name(&self) -> &'static str {
        "auth"
    }

    async fn check(&self, ctx: &mut GateCtx) -> Result<(), Rejection> {
        // 1. extract the key
        let raw = extract_key(&ctx.request_meta)?;

        // 2. sha256 hash lookup (hash first, then hand over the plaintext key — saves a clone)
        let hash = sha256(&raw);
        ctx.raw_key = Some(raw);
        let entry = self
            .tokens
            .load()
            .lookup(&hash)
            .ok_or(Rejection::InvalidApiKey)?;
        let token_record = entry.record;

        // 3. write user_key (used by the state gate) + token info
        ctx.user_key = Some(token_record.user_key().to_string());
        ctx.token = Some(TokenInfo {
            id: token_record.meta.key.clone(),
            user_key: token_record.user_key().to_string(),
            user_id: 0, // placeholder: the gate chain never resolves the real user id
            id_hash: hash,
            group: token_record.group().unwrap_or("").to_string(),
            enabled: token_record.is_enabled(),
            expires_at: token_record.expires_at_unix(),
            allowed_models: entry.allowed_models,
            auth_version: token_record.auth_version(),
        });
        Ok(())
    }
}

/// Extract the key from the three standard headers: Authorization: Bearer, x-api-key,
/// x-goog-api-key.
///
/// # Errors
///
/// Returns [`Rejection::InvalidApiKey`] when no recognized header carries a key.
pub fn extract_key(meta: &gateway_pipeline::RequestMeta) -> Result<String, Rejection> {
    let header = |name: &str| meta.headers.get(name).and_then(|h| h.to_str().ok());
    if let Some(rest) = header("authorization").and_then(|s| s.strip_prefix("Bearer ")) {
        return Ok(rest.to_string());
    }
    header("x-api-key")
        .or_else(|| header("x-goog-api-key"))
        .map(str::to_string)
        .ok_or(Rejection::InvalidApiKey)
}

/// SHA-256 → 32 bytes. ponytail: a single `Sha256::new()` + `update` + `finalize` round,
/// no heap allocation.
#[must_use]
pub fn sha256(input: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    let out = hasher.finalize();
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&out);
    arr
}
