//! settle / sink tests — group-aware pricing chain plus the channel that lands settled
//! products.
//!
//! Covers three behaviors:
//! 1. `settle_event` passes `group` through to `PriceTable::lookup` and multiplies
//!    `group_ratio` into cost (library-side evidence that the previously dead
//!    group-multiplier chain is wired through);
//! 2. unknown models use the default free price but still produce an event (the floor
//!    semantics);
//! 3. [`SettleSink`] can be implemented by an in-memory mock on the apps side and receives
//!    events.

use gateway_core::records::UsageEvent;
use gateway_metering::ledger::Hold;
use gateway_metering::pricing::{ModelPrice, PriceTable};
use gateway_metering::scanner::TokenCounts;
use gateway_metering::settle::{extract_finish_reason, extract_usage};
use gateway_metering::{SettleSink, settle_event};
use std::sync::Mutex;

/// Fake price table: returns a different price per group for the same model — proves that
/// group really reaches lookup.
struct GroupTable;

impl PriceTable for GroupTable {
    fn lookup(&self, model: &str, group: &str) -> Option<ModelPrice> {
        match (model, group) {
            ("gpt-4o", "vip") => Some(ModelPrice {
                input: 30.0,
                output: 60.0,
                cache: 0.0,
                group_multiplier: 1.0,
            }),
            ("gpt-4o", _) => Some(ModelPrice {
                input: 15.0,
                output: 60.0,
                cache: 0.0,
                group_multiplier: 1.0,
            }),
            _ => None,
        }
    }
}

fn hold() -> Hold {
    Hold {
        id: 0,
        amount: 0,
        user_key: "u1".into(),
        token_key: "t1".into(),
    }
}

/// Group selects the price and group_ratio multiplies it: 1M prompt at the vip price
/// ($30/M) × ratio 2 → $60.
#[test]
fn settle_event_uses_group_and_group_ratio() {
    let counts = TokenCounts {
        prompt: 1_000_000,
        completion: 0,
        cached: 0,
        cache_creation: 0,
        reasoning: 0,
        prompt_includes_cache: false,
    };
    let ev = settle_event(
        counts,
        true,
        "vip",
        2.0,
        &hold(),
        &GroupTable,
        "ch",
        "ru",
        "gpt-4o",
        "gpt-4o",
        0,
        0,
        200,
        None,
        None,
    );
    // $30/M × 1M × ratio 2 = $60 → 60 * 500_000 = 30_000_000
    assert_eq!(ev.cost, 30_000_000);
    // is_stream is passed through into the record verbatim (evidence of the streaming path).
    assert!(
        ev.is_stream,
        "is_stream=true must be forwarded to the event"
    );

    // The default group falls back to $15/M with ratio 1.0 → 7_500_000
    let ev = settle_event(
        counts,
        false,
        "default",
        1.0,
        &hold(),
        &GroupTable,
        "ch",
        "ru",
        "gpt-4o",
        "gpt-4o",
        0,
        0,
        200,
        None,
        None,
    );
    assert_eq!(ev.cost, 7_500_000);
    assert!(
        !ev.is_stream,
        "is_stream=false must be forwarded to the event"
    );

    // Event attribution keys come from the hold.
    assert_eq!(ev.user_key, "u1");
    assert_eq!(ev.token_key, "t1");
}

/// Unknown model → default free price (0), but the event is still produced; error/status land
/// in the event verbatim.
#[test]
fn settle_event_unknown_model_free_and_error_fields_carried() {
    let counts = TokenCounts {
        prompt: 1000,
        completion: 500,
        cached: 0,
        cache_creation: 0,
        reasoning: 0,
        prompt_includes_cache: false,
    };
    let ev = settle_event(
        counts,
        false,
        "vip",
        2.0,
        &hold(),
        &GroupTable,
        "ch",
        "ru",
        "free-model",
        "free",
        0,
        0,
        200,
        None,
        None,
    );
    assert_eq!(ev.cost, 0, "priceless model is free but still accounted");

    let ev = settle_event(
        counts,
        false,
        "default",
        1.0,
        &hold(),
        &GroupTable,
        "ch",
        "ru",
        "gpt-4o",
        "gpt-4o",
        0,
        0,
        500,
        Some("upstream exploded"),
        None,
    );
    assert_eq!(ev.status_code, 500);
    assert_eq!(ev.error.as_deref(), Some("upstream exploded"));
}

/// In-memory sink mock (same shape as the apps-side implementation): settled products can be
/// collected.
#[derive(Default)]
struct VecSink(Mutex<Vec<UsageEvent>>);

impl SettleSink for VecSink {
    fn submit(&self, event: UsageEvent) {
        self.0.lock().unwrap().push(event);
    }
}

#[test]
fn sink_receives_settled_events() {
    let sink = VecSink::default();
    let counts = TokenCounts {
        prompt: 100,
        completion: 50,
        cached: 0,
        cache_creation: 0,
        reasoning: 0,
        prompt_includes_cache: false,
    };
    let ev = settle_event(
        counts,
        false,
        "default",
        1.0,
        &hold(),
        &GroupTable,
        "ch",
        "ru",
        "gpt-4o",
        "gpt-4o",
        0,
        0,
        200,
        None,
        None,
    );
    sink.submit(ev);

    let events = sink.0.lock().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].prompt_tokens, 100);
    assert_eq!(events[0].completion_tokens, 50);
    // (100×15 + 50×60)/1e6 × 500_000 = 2_250
    assert_eq!(events[0].cost, 2_250);
}

/// Usage extraction from non-streaming response bodies: both the OpenAI / Claude shapes;
/// absent or all-zero usage falls back to None.
#[test]
fn extract_usage_handles_openai_claude_and_absent() {
    // OpenAI: prompt/completion + details.cached_tokens
    let body = br#"{"choices":[],"usage":{"prompt_tokens":11,"completion_tokens":7,"prompt_tokens_details":{"cached_tokens":3}}}"#;
    let c = extract_usage(body).expect("openai usage");
    assert_eq!((c.prompt, c.completion, c.cached), (11, 7, 3));

    // Claude: input/output_tokens + cache_read_input_tokens
    let body = br#"{"usage":{"input_tokens":5,"output_tokens":9,"cache_read_input_tokens":2}}"#;
    let c = extract_usage(body).expect("claude usage");
    assert_eq!((c.prompt, c.completion, c.cached), (5, 9, 2));
    // Missing usage / non-JSON / all zero → None, and the caller falls back to estimation.
    assert!(extract_usage(b"{}").is_none());
    assert!(extract_usage(b"not json").is_none());
    assert!(extract_usage(br#"{"usage":{"prompt_tokens":0,"completion_tokens":0}}"#).is_none());
}

/// Finish-reason extraction from non-streaming responses: OpenAI's
/// `choices[0].finish_reason` / Claude's top-level `stop_reason`; missing, non-string, empty
/// string, or invalid JSON → None (never fabricated).
#[test]
fn extract_finish_reason_handles_shapes_and_absent() {
    // OpenAI shape
    let body = br#"{"choices":[{"finish_reason":"stop"}],"usage":{}}"#;
    assert_eq!(extract_finish_reason(body).as_deref(), Some("stop"));

    // Claude shape
    let body = br#"{"stop_reason":"max_tokens"}"#;
    assert_eq!(extract_finish_reason(body).as_deref(), Some("max_tokens"));

    // Missing / null / non-string / empty string / invalid JSON → None
    assert!(extract_finish_reason(b"{}").is_none());
    assert!(extract_finish_reason(br#"{"choices":[{"finish_reason":null}]}"#).is_none());
    assert!(extract_finish_reason(br#"{"choices":[{"finish_reason":42}]}"#).is_none());
    assert!(extract_finish_reason(br#"{"choices":[{"finish_reason":""}]}"#).is_none());
    assert!(extract_finish_reason(b"not json").is_none());
}

/// settle_event passes finish_reason through: supplied means it lands in the record, absent
/// means None.
#[test]
fn settle_event_carries_finish_reason() {
    let counts = TokenCounts {
        prompt: 1,
        completion: 1,
        cached: 0,
        cache_creation: 0,
        reasoning: 0,
        prompt_includes_cache: false,
    };
    let ev = settle_event(
        counts,
        false,
        "default",
        1.0,
        &hold(),
        &GroupTable,
        "ch",
        "ru",
        "gpt-4o",
        "gpt-4o",
        0,
        0,
        200,
        None,
        Some("stop"),
    );
    assert_eq!(ev.finish_reason.as_deref(), Some("stop"));

    let ev = settle_event(
        counts,
        false,
        "default",
        1.0,
        &hold(),
        &GroupTable,
        "ch",
        "ru",
        "gpt-4o",
        "gpt-4o",
        0,
        0,
        200,
        None,
        None,
    );
    assert_eq!(ev.finish_reason, None);
}

/// Quantity details are carried through to the event but do not enter `cost`:
/// the pricing layer must remain untouched by the new columns.
#[test]
fn settle_event_carries_quantity_details_without_pricing_them() {
    let base = TokenCounts {
        prompt: 100,
        completion: 40,
        cached: 30,
        cache_creation: 0,
        reasoning: 0,
        prompt_includes_cache: false,
    };
    let detailed = TokenCounts {
        cache_creation: 20,
        reasoning: 12,
        prompt_includes_cache: false,
        ..base
    };

    let plain = settle_event(
        base,
        true,
        "default",
        1.0,
        &hold(),
        &GroupTable,
        "ch",
        "ru",
        "gpt-4o",
        "gpt-4o",
        0,
        0,
        200,
        None,
        None,
    );
    let with_details = settle_event(
        detailed,
        true,
        "default",
        1.0,
        &hold(),
        &GroupTable,
        "ch",
        "ru",
        "gpt-4o",
        "gpt-4o",
        0,
        0,
        200,
        None,
        None,
    );

    assert_eq!(with_details.cache_creation_tokens, 20);
    assert_eq!(with_details.reasoning_tokens, 12);
    assert_eq!(
        plain.cost, with_details.cost,
        "new quantity fields must not change cost"
    );
}

/// Detail-only usage is malformed (cache/reasoning are subsets of the primary
/// counters), so the non-streaming path must fall back to estimation instead of
/// recording a cache-only row — the same rule the streaming `finish()` applies.
#[test]
fn extract_usage_rejects_detail_only_usage() {
    let body =
        br#"{"usage":{"cached_tokens":80,"cache_creation_input_tokens":20,"reasoning_tokens":5}}"#;
    assert!(gateway_metering::extract_usage(body).is_none());
}
