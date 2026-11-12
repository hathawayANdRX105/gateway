//! Catalog load validation for channel `settings` (issue #15).
//!
//! A channel's `settings` table is parsed into [`gateway_core::ChannelSettings`] at
//! catalog load. A negative `priority` or a `context_window` above
//! [`MAX_CONTEXT_WINDOW`] refuses the whole record — never silently clamped.
//!
//! `Catalog::from_toml` is the load entry point, so these tests drive the real load
//! path end to end.

use gateway_daemon::catalog::Catalog;

/// Minimal valid catalog with one channel; `settings` is the only variable part.
fn catalog(settings: &str) -> String {
    format!(
        r#"
[[groups]]
name = "default"
allowed_models = ["gpt-4o*"]
multiplier = 1.0
enabled = true

[[users]]
key = "user-1"
username = "alice"
group = "default"
status = 1
role = 1

[[tokens]]
key = "sk-replace-me"
user = "user-1"
quota = 2_000_000
status = 1

[[channels]]
key = "ch-1"
name = "primary"
provider_type = "openai"
base_url = "https://api.example.com"
max_concurrency = 32
status = 1
groups = ["default"]
settings = {settings}

[[channels.keys]]
secret = "sk-upstream"
rpm_limit = 0

[[channels.models]]
name = "gpt-4o"

[[prices]]
model = "gpt-4o"
group = "default"
input = 2.5
output = 10.0
"#
    )
}

/// Purpose: a channel with a settings table of known keys loads.
/// Scenario: `settings = { priority = 5, context_window = 2048 }`.
/// Accepts: `Catalog::from_toml` succeeds.
#[test]
fn channel_with_settings_loads() {
    let text = catalog("{ priority = 5, context_window = 2048 }");
    Catalog::from_toml(&text).expect("valid settings must load");
}

/// Purpose: unknown settings keys do not fail load (they are preserved in `extra`).
/// Scenario: `settings = { priority = 2, custom_key = "custom" }`.
/// Accepts: load succeeds — unknown keys are not rejected.
#[test]
fn unknown_settings_keys_do_not_fail_load() {
    let text = catalog("{ priority = 2, custom_key = \"custom\" }");
    Catalog::from_toml(&text).expect("unknown settings keys must load");
}

/// Purpose: a negative priority refuses the channel at load.
/// Scenario: `settings = { priority = -1 }`.
/// Accepts: load fails with an error naming priority.
#[test]
fn negative_priority_refuses_channel_at_load() {
    let text = catalog("{ priority = -1 }");
    let err = Catalog::from_toml(&text).unwrap_err().to_string();
    assert!(
        err.contains("priority"),
        "load error must name priority: {err}"
    );
    assert!(
        err.contains("ch-1"),
        "load error must name the channel: {err}"
    );
}

/// Purpose: a `context_window` above the bound refuses the channel at load.
/// Scenario: `settings = { context_window = 10000001 }`.
/// Accepts: load fails with an error naming `context_window` and the bound.
#[test]
fn oversized_context_window_refuses_channel_at_load() {
    let text = catalog("{ context_window = 10000001 }");
    let err = Catalog::from_toml(&text).unwrap_err().to_string();
    assert!(
        err.contains("context_window"),
        "load error must name context_window: {err}"
    );
    assert!(
        err.contains("10000000"),
        "load error must report the bound: {err}"
    );
}

/// Purpose: `context_window` exactly at the bound loads.
/// Scenario: `settings = { context_window = 10000000 }`.
/// Accepts: load succeeds (only *above* the bound is refused).
#[test]
fn context_window_at_bound_loads() {
    let text = catalog("{ context_window = 10000000 }");
    Catalog::from_toml(&text).expect("context_window at MAX_CONTEXT_WINDOW must load");
}
