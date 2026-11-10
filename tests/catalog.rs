//! Catalog file → snapshot building.
//!
//! Purpose: the standalone process has no database, so the TOML catalog is the only source
//! of channels, tokens and prices. A wrong expansion here silently produces "no route" or
//! "unknown key" at request time, which is why the build is asserted directly.
//!
//! Scenarios: one channel with two keys and two models in one group (four route units), a
//! token hashed into the snapshot with its quota, price conversion from $/M to internal
//! units, and the fallback from a group-specific price row to `default`.
//!
//! Acceptance: counts and lookups match the document; a token pointing at a missing user is
//! rejected instead of loading a half-broken catalog.

use gateway_daemon::catalog::Catalog;
use gateway_facade::{PriceTable, sha256};

const CATALOG: &str = r#"
[[groups]]
name = "default"
allowed_models = ["gpt-4o*"]
multiplier = 1.5

[[users]]
key = "user-1"
username = "alice"
email = "alice@example.com"
group = "default"

[[tokens]]
key = "sk-test-key"
user = "user-1"
name = "main"
quota = 2_000_000
allowed_models = ["gpt-4o"]

[[channels]]
key = "ch-1"
name = "primary"
provider_type = "openai"
base_url = "http://127.0.0.1:9"
groups = ["default"]

[[channels.keys]]
secret = "upstream-a"

[[channels.keys]]
secret = "upstream-b"

[[channels.models]]
name = "gpt-4o"

[[channels.models]]
name = "gpt-4o-mini"
upstream = "gpt-4o-mini-2024-07-18"
weight = 2

[[prices]]
model = "gpt-4o"
input = 2.5
output = 10.0
cache = 1.25
"#;

#[test]
fn expands_units_and_hashes_tokens() {
    let catalog = Catalog::from_toml(CATALOG).expect("catalog must build");

    // Two models × one group × two keys = four dispatch units.
    assert_eq!(catalog.counts.route_units, 4);
    assert_eq!(catalog.counts.channels, 1);
    assert_eq!(catalog.counts.tokens, 1);

    let hash = sha256("sk-test-key");
    let entry = catalog
        .token_snapshot
        .lookup(&hash)
        .expect("the plaintext key must be indexed by its sha256");

    // The token's own group is unset, so the user's group is the effective one at the gate;
    // the record itself keeps None and the quota lives under the token key.
    assert_eq!(entry.record.group, None);
    assert_eq!(entry.record.user_key, "user-1");
    assert_eq!(
        catalog.quota_snapshot.remaining(&entry.record.meta.key),
        2_000_000
    );

    let unit = catalog
        .dispatch
        .units
        .iter()
        .find(|unit| unit.public_model == "gpt-4o-mini")
        .expect("the mini model must be routable");
    assert_eq!(unit.upstream_model, "gpt-4o-mini-2024-07-18");
    assert_eq!(unit.weight, 2);
}

#[test]
fn converts_prices_and_falls_back_to_default_group() {
    let catalog = Catalog::from_toml(CATALOG).expect("catalog must build");

    // $/M → internal units (500_000 units = $1).
    let row = catalog
        .pricing_snapshot
        .lookup("gpt-4o", "any-other-group")
        .expect("the model-only row must be reachable through the default fallback");
    assert_eq!(row.output_per_m, 10.0 * 500_000.0);

    let settle_price = catalog
        .price_table
        .lookup("gpt-4o", "vip")
        .expect("settlement must use the same fallback");
    assert_eq!(settle_price.output, 10.0);
}

#[test]
fn rejects_token_with_unknown_user() {
    let text = r#"
[[tokens]]
key = "sk-orphan"
user = "missing"

[[channels]]
key = "ch-1"
name = "primary"
base_url = "http://127.0.0.1:9"
groups = ["default"]
[[channels.keys]]
secret = "upstream"
"#;
    let error = Catalog::from_toml(text).expect_err("an orphan token must fail the build");
    assert!(
        error.to_string().contains("unknown user"),
        "unexpected error: {error}"
    );
}
