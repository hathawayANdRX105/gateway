//! `RequestPath::parse` / `protocol` — inbound URL classification.
//!
//! Acceptance: every guarded data-plane path shape maps to the dialect it speaks;
//! the fallback stays `OpenAI`; Gemini URLs expose the path-carried model and the
//! streaming verb; unrelated paths are no longer misclassified by substring luck.

use gateway_core::{ProtocolKind, RequestPath};

/// Purpose: pin the shape + dialect pair for every recognized family in one table.
/// Scenario: representative paths of the guarded scope (`/v1*`, `/v1beta*`).
/// Accepts: each path parses to the expected variant and `protocol()` maps it to the
/// expected `ProtocolKind`.
#[test]
fn parses_each_dialect_family() {
    let cases = [
        (
            "/v1/chat/completions",
            RequestPath::ChatCompletions,
            ProtocolKind::OpenAI,
        ),
        (
            "/v1/messages",
            RequestPath::Messages,
            ProtocolKind::Anthropic,
        ),
        (
            "/v1/messages/count_tokens",
            RequestPath::Messages,
            ProtocolKind::Anthropic,
        ),
        ("/v1/models", RequestPath::Models, ProtocolKind::OpenAI),
        (
            "/v1beta/models",
            RequestPath::GeminiOther,
            ProtocolKind::Gemini,
        ),
    ];
    for (path, shape, protocol) in cases {
        let parsed = RequestPath::parse(path);
        assert_eq!(parsed, shape, "path={path}");
        assert_eq!(parsed.protocol(), protocol, "path={path}");
    }
}

/// Purpose: regression lock for segment anchoring — the old substring prefixes
/// matched junk continuations (`/v1/messages2xyz`, `/v1betafoo`) and the tightened
/// rules must not.
/// Scenario: near-miss paths that the old `starts_with`/`contains` rules swallowed.
/// Accepts: `OpenAI` fallback for the junk shapes; empty-segment-only paths (which the
/// entry guard rejects before the parser sees them) still classify by segments.
#[test]
fn near_miss_paths_fall_back_instead_of_guessing() {
    let cases = [
        // Old: starts_with("/v1/messages") matched this junk.
        "/v1/messages2xyz",
        // Old: starts_with("/v1/responses") matched this junk.
        "/v1/responses2",
        // Old: contains("/v1beta") matched this junk.
        "/v1betafoo",
        // Only the full chat/completions operation is that shape.
        "/v1/chat",
    ];
    for path in cases {
        assert_eq!(
            RequestPath::parse(path).protocol(),
            ProtocolKind::OpenAI,
            "path={path}"
        );
    }
    // Shape layer too: loosening the chat arm back would flip this without moving
    // the dialect.
    assert_eq!(RequestPath::parse("/v1/chat"), RequestPath::Unknown);
    // Defense in depth: the parser ignores empty segments; the guard 404s this
    // shape before any stage runs, so the classification is never acted upon.
    assert_eq!(RequestPath::parse("//v1//messages"), RequestPath::Messages);
}

/// Purpose: Gemini URLs address the model in the path, and the verb decides streaming.
/// Scenario: `models/{model}:generateContent` and `:streamGenerateContent`, plus the
/// bare `models/{model}` form without a verb.
/// Accepts: model extracted verbatim; `stream` true only for the stream verb.
#[test]
fn gemini_model_and_stream_come_from_the_url() {
    let parsed = RequestPath::parse("/v1beta/models/gemini-2.5-pro:generateContent");
    assert_eq!(
        parsed,
        RequestPath::GeminiGenerate {
            model: Some("gemini-2.5-pro".into()),
            stream: false,
        }
    );

    let parsed = RequestPath::parse("/v1beta/models/gemini-2.5-pro:streamGenerateContent");
    assert_eq!(
        parsed,
        RequestPath::GeminiGenerate {
            model: Some("gemini-2.5-pro".into()),
            stream: true,
        }
    );
    assert_eq!(parsed.protocol(), ProtocolKind::Gemini);

    let parsed = RequestPath::parse("/v1beta/models/gemini-2.5-pro");
    assert_eq!(
        parsed,
        RequestPath::GeminiGenerate {
            model: Some("gemini-2.5-pro".into()),
            stream: false,
        }
    );
}

/// Purpose: model extraction must not be polluted by extra segments, and non-generate
/// verbs still address the model resource.
/// Scenario: a junk segment between model and verb; a `:countTokens` verb.
/// Accepts: the polluted shape yields no model extraction (`GeminiOther`); the colon
/// verb form extracts the model with `stream: false`.
#[test]
fn deeper_gemini_shapes_extract_no_model() {
    assert_eq!(
        RequestPath::parse("/v1beta/models/a/b:generateContent"),
        RequestPath::GeminiOther
    );
    assert_eq!(
        RequestPath::parse("/v1beta/models/gemini-2.5-pro:countTokens"),
        RequestPath::GeminiGenerate {
            model: Some("gemini-2.5-pro".into()),
            stream: false,
        }
    );
}

/// Purpose: some relays mount the Gemini dialect under `/v1` without the v1beta prefix.
/// Scenario: `/v1/models/{model}:generateContent` / `:streamGenerateContent`.
/// Accepts: classified as Gemini by the anchored colon verb, not by substring luck.
#[test]
fn gemini_verb_is_recognized_under_v1_too() {
    let parsed = RequestPath::parse("/v1/models/gemini-2.5-pro:streamGenerateContent");
    assert_eq!(
        parsed,
        RequestPath::GeminiGenerate {
            model: Some("gemini-2.5-pro".into()),
            stream: true,
        }
    );
    assert_eq!(parsed.protocol(), ProtocolKind::Gemini);
}

/// Purpose: parsing is robust to the URI decorations callers actually pass.
/// Scenario: a query string and a trailing slash on otherwise-recognized paths.
/// Accepts: identical classification to the clean path.
#[test]
fn query_and_trailing_slash_are_normalized() {
    assert_eq!(
        RequestPath::parse("/v1/chat/completions?alt=genai"),
        RequestPath::ChatCompletions
    );
    assert_eq!(RequestPath::parse("/v1/messages/"), RequestPath::Messages);
}

/// Purpose: regression lock for the tightened rule — the old substring sniffing
/// classified any path containing "/v1/" + "gemini" as Gemini.
/// Scenario: unrelated OpenAI/unknown paths whose segments carry the word "gemini".
/// Accepts: `OpenAI` dialect; only the anchored Gemini shapes (v1beta prefix or
/// `:generateContent` verb) classify as Gemini.
#[test]
fn gemini_substring_no_longer_hijacks_other_paths() {
    for path in ["/v1/embeddings-gemini", "/v1/chat/completions", "/v1", ""] {
        assert_eq!(
            RequestPath::parse(path).protocol(),
            ProtocolKind::OpenAI,
            "path={path}"
        );
    }
}
