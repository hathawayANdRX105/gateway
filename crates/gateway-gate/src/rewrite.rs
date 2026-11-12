//! P4 channel-level body rewriting: inject fills defaults, drop removes fields.
//!
//! Mirrors the semantics of api-hub `_inject_body` (:257-287): **fields the client sent
//! explicitly take priority; injection only fills defaults**; `drop_fields` lists fields the
//! channel does not support, and they are removed **after** injection — so a key in both
//! `fields` and `drop_fields` ends up removed, and drop never resurrects a key the channel
//! does not support.
//!
//! This module provides only the pure rewriting step: given a body and an [`InjectSpec`],
//! [`rewrite_body`] returns the adapted body. The config surface (`inject` /
//! `drop_fields` channel settings) and the call site (picking the spec from the selected
//! channel and applying it before forwarding) belong to the contract/apps side and land
//! separately. Nothing here touches the network, so a rewrite failure cannot surface as a
//! 500.

use std::collections::HashMap;

use bytes::Bytes;

/// Channel-level body-rewrite spec (plain data, passed through with the candidate).
#[derive(Debug, Clone, Default)]
pub struct InjectSpec {
    /// Injection fields: fill only keys the client did not send explicitly (client values
    /// always win).
    pub fields: HashMap<String, serde_json::Value>,
    /// Field names the channel does not support; removed after injection, before sending.
    pub drop_fields: Vec<String>,
}

/// Apply [`InjectSpec`] to the upstream request body.
///
/// Best-effort channel adaptation: a body that is not a JSON object (empty body, malformed
/// JSON, array, scalar) is returned unchanged byte-for-byte — an unparseable request body
/// must never become a 500.
///
/// For JSON objects, entries of [`InjectSpec::fields`] whose key the client did not send
/// explicitly are injected (client values always win), then every name in
/// [`InjectSpec::drop_fields`] is removed — drop runs after injection, so a key present in
/// both lists ends up dropped. The object is re-serialized as valid JSON and untouched
/// fields keep their values. Re-serialization sorts keys (`serde_json`'s default `Map` is
/// ordered by key), so byte equality with the input is guaranteed for non-object bodies only.
pub fn rewrite_body(body: &Bytes, spec: &InjectSpec) -> Bytes {
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(body) else {
        return body.clone();
    };
    let serde_json::Value::Object(mut map) = value else {
        return body.clone();
    };

    // Injection first: `or_insert_with` never overwrites, so client-sent keys keep their
    // values and only missing keys get the channel default.
    for (key, value) in &spec.fields {
        map.entry(key.clone()).or_insert_with(|| value.clone());
    }
    // Drop last: fields the channel does not support are removed after injection, so a key
    // in both `fields` and `drop_fields` ends up dropped even if it was just injected.
    for key in &spec.drop_fields {
        map.remove(key);
    }

    match serde_json::to_vec(&map) {
        Ok(bytes) => Bytes::from(bytes),
        // Serializing a `Value` cannot fail in practice; on the off chance it does, fall
        // back to passthrough — rewriting must never turn an adaptable body into a 500.
        Err(_) => body.clone(),
    }
}
