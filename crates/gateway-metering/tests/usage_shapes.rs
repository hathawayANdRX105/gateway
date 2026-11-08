//! Vendor-shape tests for `counts_from_usage`: which counter carries the prompt and
//! whether cached tokens are already inside it. This flag decides whether `price_of`
//! subtracts them from the input charge (OpenAI/Gemini) or charges them additively on
//! top of an already-uncached prompt (Claude).

use bytes::Bytes;
use gateway_metering::StreamScanner;

fn scan(frame: &'static str) -> gateway_metering::scanner::TokenCounts {
    let mut s = StreamScanner::new();
    s.push(&Bytes::from_static(frame.as_bytes()));
    let (counts, _) = s.finish(0);
    counts
}

#[test]
fn claude_input_tokens_excludes_cache() {
    let counts = scan(
        "data: {\"usage\":{\"input_tokens\":10,\"output_tokens\":3,\
         \"cache_read_input_tokens\":4,\"cache_creation_input_tokens\":6}}\n\n",
    );
    assert_eq!(counts.prompt, 10);
    assert_eq!(counts.cached, 4);
    assert!(
        !counts.prompt_includes_cache,
        "Claude input_tokens already excludes cache"
    );
}

#[test]
fn responses_input_tokens_includes_cache() {
    let counts = scan(
        "data: {\"usage\":{\"input_tokens\":10,\
         \"input_tokens_details\":{\"cached_tokens\":4},\"output_tokens\":3}}\n\n",
    );
    assert_eq!(counts.prompt, 10);
    assert_eq!(
        counts.cached, 4,
        "Responses caches live under input_tokens_details"
    );
    assert!(
        counts.prompt_includes_cache,
        "Responses input_tokens includes cached tokens"
    );
}

#[test]
fn openai_chat_prompt_tokens_includes_cache() {
    let counts = scan(
        "data: {\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":3,\
         \"prompt_tokens_details\":{\"cached_tokens\":4}}}\n\n",
    );
    assert!(counts.prompt_includes_cache);
    assert_eq!(counts.cached, 4);
}
