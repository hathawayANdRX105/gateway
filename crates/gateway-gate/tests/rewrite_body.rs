//! `gateway-gate` integration tests for `rewrite_body` — channel-level body inject/drop.
//!
//! Purpose: pin the implemented semantics of [`gateway_gate::rewrite_body`] — inject default
//! keys only where the client did not send explicitly, then remove `drop_fields`, and pass
//! non-JSON-object bodies through byte-for-byte.
//!
//! Acceptance (issue #11 sub-deliverable (d)):
//! 1. missing keys are injected; client-sent keys stay untouched
//! 2. `drop_fields` entries are removed
//! 3. empty / malformed / array (any non-object) bodies pass through byte-for-byte
//! 4. a key in both `fields` and `drop_fields` follows the documented order (inject first,
//!    drop last → the key ends up dropped)
//! 5. nested object values inject fine

use bytes::Bytes;
use gateway_gate::{InjectSpec, rewrite_body};

/// Build an [`InjectSpec`] from `(key, value)` injection pairs and drop-field names.
/// Pairs are taken by value — the `json!()` values at the call sites are moved into the
/// map without a per-pair deep clone.
fn spec(
    pairs: impl IntoIterator<Item = (&'static str, serde_json::Value)>,
    drop_fields: impl IntoIterator<Item = &'static str>,
) -> InjectSpec {
    InjectSpec {
        fields: pairs
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect(),
        drop_fields: drop_fields.into_iter().map(str::to_string).collect(),
    }
}

/// Rewrite `body` with `spec` and parse the result — the output must always be valid JSON.
fn rewritten(body: &[u8], spec: &InjectSpec) -> serde_json::Value {
    serde_json::from_slice(rewrite_body(&Bytes::copy_from_slice(body), spec).as_ref())
        .expect("rewrite_body must always produce valid JSON")
}

/// 1. Missing keys are injected; client-sent keys win (client values are never overwritten).
#[test]
fn injects_missing_keys_while_client_values_win() {
    // Scenario: the client sent model + temperature; the channel spec defaults temperature
    // and adds max_tokens.
    // Acceptance: client temperature (0.8) wins over the default (0.7), max_tokens is filled,
    // model passes through verbatim.
    let body = br#"{"model": "gpt-4", "temperature": 0.8}"#;
    let spec = spec(
        [
            ("temperature", serde_json::json!(0.7)),
            ("max_tokens", serde_json::json!(1024)),
        ],
        [],
    );
    let got = rewritten(body, &spec);
    assert_eq!(
        got,
        serde_json::json!({"model": "gpt-4", "temperature": 0.8, "max_tokens": 1024})
    );
}

/// 2. `drop_fields` entries are removed; absent drop names are ignored.
#[test]
fn drop_fields_removed() {
    // Scenario: body carries a, b, c; spec drops b plus a name that is not present.
    // Acceptance: b is gone, a and c keep their values, unknown drop names are no-ops.
    let body = br#"{"a": 1, "b": 2, "c": 3}"#;
    let spec = spec([], ["b", "nonexistent"]);
    let got = rewritten(body, &spec);
    assert_eq!(got, serde_json::json!({"a": 1, "c": 3}));
}

/// 3. Non-JSON-object bodies pass through byte-for-byte, even with an active spec.
#[test]
fn non_object_bodies_pass_through_byte_for_byte() {
    // Scenario: empty body, malformed JSON, array, scalar, JSON string, null, whitespace —
    // none of these is a JSON object.
    // Acceptance: each input is returned unchanged, byte for byte — best-effort adaptation
    // never turns an unparseable body into a 500.
    let spec = spec([("temperature", serde_json::json!(0.7))], []);
    for body in [
        b"" as &[u8],
        b"{unclosed" as &[u8],
        b"[1, 2, 3]" as &[u8],
        b"42" as &[u8],
        b"\"hello\"" as &[u8],
        b"null" as &[u8],
        b"   " as &[u8],
    ] {
        let out = rewrite_body(&Bytes::copy_from_slice(body), &spec);
        assert_eq!(
            out.as_ref(),
            body,
            "body must pass through unchanged: {body:?}"
        );
    }
}

/// 4. A key in both `fields` and `drop_fields` follows the documented order (inject then
///    drop → dropped), whether or not the client sent it.
#[test]
fn field_and_drop_on_same_key_drop_wins() {
    // Documented order: injection runs before `drop_fields` removal (drop runs last).
    // Acceptance: the key ends up removed in both cases.
    let spec = spec([("timeout", serde_json::json!(30))], ["timeout"]);

    // Client did not send the key: injected with the default, then dropped.
    let got = rewritten(b"{}", &spec);
    assert_eq!(got, serde_json::json!({}));

    // Client sent the key: the client value wins injection, then drop removes it anyway.
    let got = rewritten(br#"{"timeout": 5}"#, &spec);
    assert_eq!(got, serde_json::json!({}));
}

/// 5. Nested object values inject fine; a client-sent nested object is untouched.
#[test]
fn nested_object_values_inject() {
    // Scenario: the channel defaults a whole nested `config` object; the client may override
    // it wholesale.
    // Acceptance: `config` is injected when missing; a client-sent `config` stays verbatim.
    let default_config = serde_json::json!({"temperature": 0.7, "max_tokens": 1024});

    // Injected case: build the expectation from the same literal shape the spec injects.
    let got = rewritten(
        br#"{"model": "gpt-4"}"#,
        &spec(
            [(
                "config",
                serde_json::json!({"temperature": 0.7, "max_tokens": 1024}),
            )],
            [],
        ),
    );
    assert_eq!(
        got,
        serde_json::json!({"model": "gpt-4", "config": default_config})
    );

    // Client-sent case: the same spec shape, a fresh Value — the client's own object wins.
    let got = rewritten(
        br#"{"config": {"temperature": 0.9}}"#,
        &spec([("config", default_config)], []),
    );
    assert_eq!(got, serde_json::json!({"config": {"temperature": 0.9}}));
}
