//! Shared harness for the hash-candidate comparison.
//!
//! One place defines the candidate algorithms, the exact byte layout fed to each
//! hash, and the sample corpus, so `benches/hash_candidates.rs` (speed) and
//! `crates/gateway-cache/tests/hash_consistency.rs` (correctness + stability)
//! cannot drift apart.
//!
//! Every candidate hashes the same pre-encoded buffer: isolation fields and the
//! request body, each length-prefixed with 8 big-endian bytes. That prefix is what
//! prevents boundary collisions (`("u-1","23gpt-4o")` vs `("u-123","gpt-4o")`), so it
//! must stay identical across candidates — otherwise the comparison measures
//! different semantics, not different hash functions.

#![allow(dead_code)] // Each consumer binary uses a different subset of this module.

use rand::RngCore;
use rand::SeedableRng;
use rand::rngs::StdRng;

/// Digest widths the response cache would store, per candidate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Algorithm {
    /// Production baseline: SHA-256, 32-byte digest.
    Sha256,
    /// BLAKE3, 32-byte digest.
    Blake3,
    /// BLAKE3 extended output, first 16 bytes kept.
    Blake3Truncated128,
    /// XXH3 128-bit, kept as two little-endian words.
    Xxh3_128,
    /// XXH3 64-bit. Collision margin too small for cross-user isolation keys; kept
    /// only to show the speed/collision trade-off.
    Xxh3_64,
    /// SipHash-2-4 with a fixed key: deterministic across processes (unlike the
    /// randomized std hasher), so it is measurable as a stable fingerprint.
    SipHash24,
    /// ahash with fixed seeds. Included to show it is not reproducible across
    /// builds/platforms, which disqualifies it as a shared cache key.
    AHashFixed,
    /// foldhash with fixed seeds. Same reproducibility caveat as ahash.
    FoldHashFixed,
}

impl Algorithm {
    /// Every candidate, in reporting order (production baseline first).
    pub const ALL: [Algorithm; 8] = [
        Algorithm::Sha256,
        Algorithm::Blake3,
        Algorithm::Blake3Truncated128,
        Algorithm::Xxh3_128,
        Algorithm::Xxh3_64,
        Algorithm::SipHash24,
        Algorithm::AHashFixed,
        Algorithm::FoldHashFixed,
    ];

    /// Short stable label used in benchmark ids and reports.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Algorithm::Sha256 => "sha256",
            Algorithm::Blake3 => "blake3_256",
            Algorithm::Blake3Truncated128 => "blake3_128",
            Algorithm::Xxh3_128 => "xxh3_128",
            Algorithm::Xxh3_64 => "xxh3_64",
            Algorithm::SipHash24 => "siphash24_128",
            Algorithm::AHashFixed => "ahash_fixed",
            Algorithm::FoldHashFixed => "foldhash_fixed",
        }
    }

    /// Digest width in bytes that this candidate would store in a cache key.
    #[must_use]
    pub fn digest_len(self) -> usize {
        match self {
            Algorithm::Sha256 | Algorithm::Blake3 => 32,
            Algorithm::Blake3Truncated128 | Algorithm::Xxh3_128 | Algorithm::SipHash24 => 16,
            Algorithm::Xxh3_64 | Algorithm::AHashFixed | Algorithm::FoldHashFixed => 8,
        }
    }

    /// Whether the digest is expected to be identical across processes and rebuilds.
    ///
    /// `false` means the value is only valid inside one process: using it as a shared
    /// cache key would silently miss across workers or after a restart.
    #[must_use]
    pub fn is_cross_process_stable(self) -> bool {
        match self {
            Algorithm::Sha256
            | Algorithm::Blake3
            | Algorithm::Blake3Truncated128
            | Algorithm::Xxh3_128
            | Algorithm::Xxh3_64
            | Algorithm::SipHash24 => true,
            Algorithm::AHashFixed | Algorithm::FoldHashFixed => false,
        }
    }

    /// Compute the digest of `input` with this candidate.
    #[must_use]
    pub fn digest(self, input: &[u8]) -> Vec<u8> {
        match self {
            Algorithm::Sha256 => {
                use sha2::{Digest, Sha256};
                Sha256::digest(input).to_vec()
            }
            Algorithm::Blake3 => blake3::hash(input).as_bytes().to_vec(),
            Algorithm::Blake3Truncated128 => {
                let out = blake3::hash(input);
                out.as_bytes()[..16].to_vec()
            }
            Algorithm::Xxh3_128 => xxhash_rust::xxh3::xxh3_128(input).to_le_bytes().to_vec(),
            Algorithm::Xxh3_64 => xxhash_rust::xxh3::xxh3_64(input).to_le_bytes().to_vec(),
            Algorithm::SipHash24 => {
                // 128-bit output variant: the 64-bit `sip::SipHasher24` would give only a
                // 2^32 birthday bound, which is the same weakness as XXH3-64.
                // A fixed key keeps this deterministic; that is the point being measured
                // (a keyed hash with a random key could not be a shared cache key).
                use core::hash::Hasher as _;
                use siphasher::sip128::Hasher128 as _;
                let mut hasher = siphasher::sip128::SipHasher24::new_with_keys(
                    0x0700_0900_0500_0b00,
                    0x0d00_0f00_1100_1300,
                );
                hasher.write(input);
                u128::from(hasher.finish128()).to_le_bytes().to_vec()
            }
            Algorithm::AHashFixed => {
                use core::hash::{BuildHasher as _, Hasher as _};
                let state = ahash::RandomState::with_seeds(
                    0x0123_4567_89ab_cdef,
                    0x0fed_cba9_8765_4321,
                    0x1111_2222_3333_4444,
                    0x5555_6666_7777_8888,
                );
                let mut hasher = state.build_hasher();
                hasher.write(input);
                hasher.finish().to_le_bytes().to_vec()
            }
            Algorithm::FoldHashFixed => {
                use core::hash::{BuildHasher as _, Hasher as _};
                let mut hasher = foldhash::fast::FixedState::default().build_hasher();
                hasher.write(input);
                hasher.finish().to_le_bytes().to_vec()
            }
        }
    }
}

/// The four isolation fields plus the request body, as the production key sees them.
#[derive(Debug, Clone)]
pub struct RequestCase {
    pub user_key: String,
    pub group: String,
    pub model: String,
    pub protocol: String,
    pub body: Vec<u8>,
}

impl RequestCase {
    /// Encode exactly the byte layout `response_key` feeds to the hash.
    ///
    /// Each component carries an 8-byte big-endian length prefix; the body does too.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let parts: [&[u8]; 5] = [
            self.user_key.as_bytes(),
            self.group.as_bytes(),
            self.model.as_bytes(),
            self.protocol.as_bytes(),
            self.body.as_slice(),
        ];
        let mut out = Vec::with_capacity(parts.iter().map(|part| part.len() + 8).sum());
        for part in parts {
            out.extend_from_slice(&(part.len() as u64).to_be_bytes());
            out.extend_from_slice(part);
        }
        out
    }
}

/// Body sizes that cover the observed request distribution.
pub const BODY_SIZES: [usize; 3] = [512, 4 * 1024, 32 * 1024];

/// Build a corpus of distinct requests with a fixed seed so every run is identical.
///
/// Samples cycle through body sizes and vary every isolation field, which is what a
/// collision test needs: near-duplicate inputs that differ in exactly one component.
#[must_use]
pub fn corpus(count: usize) -> Vec<RequestCase> {
    let mut rng = StdRng::seed_from_u64(0x5eed_1e55);
    let models = [
        "gpt-4o",
        "gpt-4o-mini",
        "claude-3-5-sonnet",
        "gemini-1.5-pro",
    ];
    let protocols = ["openai", "anthropic", "gemini", "responses"];
    let groups = ["default", "premium", "internal"];

    (0..count)
        .map(|i| {
            let size = BODY_SIZES[i % BODY_SIZES.len()];
            let mut body = vec![0u8; size];
            rng.fill_bytes(&mut body);
            RequestCase {
                user_key: format!("user-{i}"),
                group: groups[i % groups.len()].to_string(),
                model: models[i % models.len()].to_string(),
                protocol: protocols[i % protocols.len()].to_string(),
                body,
            }
        })
        .collect()
}

/// Pairs that must never collide: each differs in exactly one component.
///
/// Returns `(description, left, right)` so a failing assertion names the boundary.
#[must_use]
pub fn boundary_pairs() -> Vec<(&'static str, RequestCase, RequestCase)> {
    let base = RequestCase {
        user_key: "u-1".to_string(),
        group: "default".to_string(),
        model: "gpt-4o".to_string(),
        protocol: "openai".to_string(),
        body: b"{\"q\":\"hi\"}".to_vec(),
    };

    let mut other_body = base.clone();
    other_body.body = b"{\"q\":\"hi \"}".to_vec();

    // Concatenation trap: ("u-1","23gpt-4o") and ("u-123","gpt-4o") join to the same
    // bytes without a length prefix, so only the prefix keeps them distinct.
    let mut left = base.clone();
    left.model = "23gpt-4o".to_string();
    let mut right = base.clone();
    right.user_key = "u-123".to_string();
    right.model = "gpt-4o".to_string();

    let mut other_user = base.clone();
    other_user.user_key = "u-2".to_string();
    let mut other_group = base.clone();
    other_group.group = "premium".to_string();
    let mut other_protocol = base.clone();
    other_protocol.protocol = "anthropic".to_string();

    vec![
        (
            "body differs by one trailing byte",
            base.clone(),
            other_body,
        ),
        ("user+model boundary trap", left, right),
        ("different user", base.clone(), other_user),
        ("different group", base.clone(), other_group),
        ("different protocol", base.clone(), other_protocol),
    ]
}
