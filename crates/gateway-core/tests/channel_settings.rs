//! `ChannelSettings` — typed promotion of `ChannelRecord.settings` (issue #15).
//!
//! Old settings JSON is a bag of arbitrary keys; known keys are promoted to typed
//! fields with documented defaults, and everything else lands in `extra`. Catalog
//! load rejects a negative `priority` and an oversized `context_window` instead of
//! silently clamping.

use gateway_core::{ChannelSettings, MAX_CONTEXT_WINDOW};

/// Purpose: deserializing old settings JSON promotes known keys to typed fields and
/// fills omitted keys from their documented defaults.
/// Scenario: `{"priority": 7, "context_window": 4096}` plus an object with no keys
/// at all.
/// Accepts: present keys are read verbatim; absent keys default to priority 0,
/// `context_window` 0, fallback false, `tpm_limit` 0, empty headers, `fallback_key_index`
/// None.
#[test]
fn known_keys_deserialize_to_typed_fields_defaults_for_the_rest() {
    let set: ChannelSettings =
        serde_json::from_value(serde_json::json!({ "priority": 7, "context_window": 4096 }))
            .unwrap();
    assert_eq!(set.priority, 7);
    assert_eq!(set.context_window, 4096);
    assert!(!set.fallback);
    assert_eq!(set.tpm_limit, 0);
    assert!(set.headers.is_empty());
    assert_eq!(set.fallback_key_index, None);

    let defaults: ChannelSettings = serde_json::from_str("{}").unwrap();
    assert_eq!(defaults, ChannelSettings::default());
    assert_eq!(defaults.priority, 0);
    assert_eq!(defaults.context_window, 0);
    assert!(!defaults.fallback);
    assert_eq!(defaults.tpm_limit, 0);
    assert_eq!(defaults.fallback_key_index, None);
}

/// Purpose: unknown settings keys must not break load and must round-trip verbatim
/// (the ir.rs `extra`/`flatten` escape-hatch precedent).
/// Scenario: settings with known keys plus arbitrary unknown keys, including a
/// nested object.
/// Accepts: unknown keys live in `extra`, typed keys are not in `extra`, and
/// re-serialization emits every key unchanged.
#[test]
fn unknown_keys_land_in_extra_and_round_trip() {
    let settings: ChannelSettings = serde_json::from_value(serde_json::json!({
        "priority": 1,
        "custom_header": "whatever",
        "nested": { "a": [1, 2] },
    }))
    .unwrap();
    assert_eq!(settings.extra.get("custom_header"), Some(&serde_json::json!("whatever")));
    assert_eq!(
        settings.extra.get("nested"),
        Some(&serde_json::json!({ "a": [1, 2] }))
    );
    assert!(!settings.extra.contains_key("priority"));

    let reserialized = serde_json::to_value(&settings).unwrap();
    assert_eq!(reserialized.get("custom_header"), Some(&serde_json::json!("whatever")));
    assert_eq!(reserialized.get("nested"), Some(&serde_json::json!({ "a": [1, 2] })));
    assert_eq!(reserialized.get("priority"), Some(&serde_json::json!(1)));
}

/// Purpose: load refuses a negative priority instead of clamping it to 0.
/// Scenario: `{"priority": -3}`.
/// Accepts: `validate` returns an error naming the key and the value.
#[test]
fn validate_refuses_negative_priority() {
    let settings = ChannelSettings::from_value(serde_json::json!({ "priority": -3 })).unwrap();
    let err = settings.validate().unwrap_err();
    assert!(
        err.contains("priority"),
        "error must name the offending key: {err}"
    );
    assert!(err.contains("-3"), "error must report the value: {err}");
}

/// Purpose: load refuses a `context_window` above the documented bound instead of
/// clamping it; the bound itself is still accepted.
/// Scenario: `context_window` at MAX + 1 and exactly at MAX.
/// Accepts: above the bound the error names the key, the value, and the bound; at
/// the bound validation passes.
#[test]
fn validate_refuses_context_window_above_max() {
    let too_big =
        ChannelSettings::from_value(serde_json::json!({ "context_window": MAX_CONTEXT_WINDOW + 1 }))
            .unwrap();
    let err = too_big.validate().unwrap_err();
    assert!(
        err.contains("context_window"),
        "error must name the offending key: {err}"
    );
    assert!(
        err.contains(&(MAX_CONTEXT_WINDOW + 1).to_string()),
        "error must report the value: {err}"
    );
    assert!(
        err.contains(&MAX_CONTEXT_WINDOW.to_string()),
        "error must report the bound: {err}"
    );

    let at_bound = ChannelSettings::from_value(serde_json::json!({ "context_window": MAX_CONTEXT_WINDOW }))
        .unwrap();
    at_bound
        .validate()
        .expect("context_window == MAX_CONTEXT_WINDOW must load");
}

/// Purpose: wrongly typed known keys are rejected at parse rather than silently
/// coerced, with the offending key named in the error.
/// Scenario: `priority` as a string.
/// Accepts: `from_value` returns an error naming `priority`.
#[test]
fn from_value_rejects_wrongly_typed_known_keys() {
    let err = ChannelSettings::from_value(serde_json::json!({ "priority": "high" }))
        .unwrap_err();
    assert!(err.contains("priority"), "error must name the key: {err}");
}

/// Purpose: header values stay generic JSON values — the old bag read was lenient
/// (non-string values are skipped by the consumer, `extra_headers_from_settings`),
/// so parse must not reject them.
/// Scenario: `headers` with a non-string value.
/// Accepts: `from_value` keeps the value verbatim in the typed `headers` map.
#[test]
fn headers_keep_lenient_json_values() {
    let settings = ChannelSettings::from_value(serde_json::json!({ "headers": { "X-Key": 7 } }))
        .expect("non-string header values are preserved, matching the old lenient bag read");
    assert_eq!(settings.headers.get("X-Key"), Some(&serde_json::json!(7)));
}
