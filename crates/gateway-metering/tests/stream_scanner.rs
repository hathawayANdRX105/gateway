use bytes::Bytes;
use gateway_metering::scanner::StreamScanner;

#[test]
fn stream_scanner_extracts_openai_usage() {
    let mut s = StreamScanner::new();
    s.push(&Bytes::from_static(
        b"data: {\"content\":\"hello\",\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5}}\n\n",
    ));
    let (counts, reason) = s.finish(10);
    assert_eq!(counts.prompt, 10);
    assert_eq!(counts.completion, 5);
    assert_eq!(
        reason, None,
        "frames without a finish field must not invent a finish reason"
    );
}

#[test]
fn stream_scanner_fallback_estimation() {
    let mut s = StreamScanner::new();
    s.push(&Bytes::from_static(
        b"data: {\"content\":\"hello world\"}\n\n",
    ));
    let (counts, _reason) = s.finish(5);
    assert_eq!(counts.prompt, 5);
    assert!(counts.completion > 0);
}

#[test]
fn stream_scanner_done_line_ignored() {
    let mut s = StreamScanner::new();
    s.push(&Bytes::from_static(b"data: [DONE]\n\n"));
    let (counts, _reason) = s.finish(0);
    assert_eq!(counts.completion, 0);
}

/// Streaming finish-reason capture (raw material for fitting `truncation_rate)`:
/// - `OpenAI` last frame `choices[0].finish_reason` (a null intermediate frame does not
///   affect the "last non-missing value wins" rule — later frames override earlier ones);
/// - Claude `message_delta` frame `delta.stop_reason`;
/// - streams without a finish field stay None (no value is invented).
#[test]
fn stream_scanner_captures_openai_finish_reason() {
    let mut s = StreamScanner::new();
    s.push(&Bytes::from_static(
        b"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"},\"finish_reason\":null}]}\n\n",
    ));
    s.push(&Bytes::from_static(
        b"data: {\"choices\":[{\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":1}}\n\n",
    ));
    let (_, reason) = s.finish(0);
    assert_eq!(reason.as_deref(), Some("stop"));
}

#[test]
fn stream_scanner_captures_claude_delta_stop_reason() {
    let mut s = StreamScanner::new();
    s.push(&Bytes::from_static(
        b"data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"max_tokens\"}}\n\n",
    ));
    let (_, reason) = s.finish(0);
    assert_eq!(reason.as_deref(), Some("max_tokens"));
}

/// `OpenAI` streaming: intermediate frames carry no usage, the last frame gives real usage
/// ⇒ settle from the last frame, not the prompt estimate.
#[test]
fn stream_scanner_openai_final_frame_counts() {
    let mut s = StreamScanner::new();
    s.push(&Bytes::from_static(
        b"data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\n",
    ));
    s.push(&Bytes::from_static(
        b"data: {\"usage\":{\"prompt_tokens\":100,\"completion_tokens\":50}}\n\n",
    ));
    let (counts, _) = s.finish(7);
    assert_eq!(counts.prompt, 100);
    assert_eq!(counts.completion, 50);
    assert_eq!(counts.cached, 0);
}

/// `DeepSeek` shape: the cache field lives under `prompt_tokens_details.cached_tokens`.
#[test]
fn stream_scanner_deepseek_cached_tokens_in_details() {
    let mut s = StreamScanner::new();
    s.push(&Bytes::from_static(
        b"data: {\"usage\":{\"prompt_tokens\":100,\"completion_tokens\":50,\"prompt_tokens_details\":{\"cached_tokens\":80}}}\n\n",
    ));
    let (counts, _) = s.finish(7);
    assert_eq!(counts.prompt, 100);
    assert_eq!(counts.completion, 50);
    assert_eq!(counts.cached, 80);
}

/// Claude core regression: `message_start` gives only `input_tokens` (nested in message.usage),
/// while `message_delta`'s `output_tokens` is cumulative. The old implementation returned
/// {0,0,0} for every frame; the old code must fail this case.
#[test]
fn stream_scanner_claude_message_start_plus_delta() {
    let mut s = StreamScanner::new();
    s.push(&Bytes::from_static(
        b"data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":25,\"output_tokens\":1}}}\n\n",
    ));
    s.push(&Bytes::from_static(
        b"data: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":50}}\n\n",
    ));
    let (counts, _) = s.finish(7);
    // input_tokens appears only once: the last frame does not carry it, so the first
    // frame's 25 must not be wiped to 0; output_tokens accumulates frame by frame: the last
    // frame's 50 overrides the first frame's 1.
    assert_eq!(counts.prompt, 25);
    assert_eq!(counts.completion, 50);
}

/// Claude's multiple `message_delta` frames grow `output_tokens` (1 → 30 → 50): take the last
/// non-zero value 50, neither summing to 81 nor keeping only the first frame's 1.
#[test]
fn stream_scanner_claude_delta_output_tokens_grows() {
    let mut s = StreamScanner::new();
    for n in [1, 30, 50] {
        let frame =
            format!("data: {{\"type\":\"message_delta\",\"usage\":{{\"output_tokens\":{n}}}}}\n\n");
        s.push(&Bytes::copy_from_slice(frame.as_bytes()));
    }
    let (counts, _) = s.finish(7);
    assert_eq!(counts.completion, 50);
}

/// Cache key coverage: top-level `cached_tokens` (OpenAI-compatible).
#[test]
fn stream_scanner_top_level_cached_tokens() {
    let mut s = StreamScanner::new();
    s.push(&Bytes::from_static(
        b"data: {\"usage\":{\"prompt_tokens\":100,\"completion_tokens\":50,\"cached_tokens\":80}}\n\n",
    ));
    let (counts, _) = s.finish(7);
    assert_eq!(counts.cached, 80);
}

/// Cache key coverage: Claude `cache_read_input_tokens`.
#[test]
fn stream_scanner_claude_cache_read_input_tokens() {
    let mut s = StreamScanner::new();
    s.push(&Bytes::from_static(
        b"data: {\"usage\":{\"input_tokens\":100,\"output_tokens\":50,\"cache_read_input_tokens\":80}}\n\n",
    ));
    let (counts, _) = s.finish(7);
    assert_eq!(counts.prompt, 100);
    assert_eq!(counts.completion, 50);
    assert_eq!(counts.cached, 80);
}

/// An all-zero usage frame is not real usage: after merging prompt/completion are still 0,
/// so the estimate fallback must run instead of passing {0,0,0} through to settlement.
#[test]
fn stream_scanner_all_zero_usage_falls_back_to_estimation() {
    let mut s = StreamScanner::new();
    s.push(&Bytes::from_static(
        b"data: {\"usage\":{\"prompt_tokens\":0,\"completion_tokens\":0}}\n\n",
    ));
    let (counts, _) = s.finish(7);
    // Fallback: prompt uses the input argument 7; completion = 51 payload bytes / 4 = 12.
    assert_eq!(counts.prompt, 7);
    assert_eq!(counts.completion, 12);
    assert_eq!(counts.cached, 0);
}

/// Stream where the upstream gives no usage at all: estimate fallback, prompt from the
/// input argument, completion from payload bytes / 4. A 23-byte payload ⇒ 5.
#[test]
fn stream_scanner_no_usage_stream_estimates() {
    let mut s = StreamScanner::new();
    s.push(&Bytes::from_static(
        b"data: {\"delta\":\"hello world\"}\n\n",
    ));
    let (counts, _) = s.finish(12);
    assert_eq!(counts.prompt, 12);
    assert_eq!(counts.completion, 5);
    assert_eq!(counts.cached, 0);
}

/// Non-JSON frames produce no usage counts, but their payload still counts toward the
/// estimate fallback (8 bytes ⇒ 2).
#[test]
fn stream_scanner_non_json_frame_ignored_for_usage() {
    let mut s = StreamScanner::new();
    s.push(&Bytes::from_static(b"data: not json\n\n"));
    let (counts, _) = s.finish(3);
    assert_eq!(counts.prompt, 3);
    assert_eq!(counts.completion, 2);
    assert_eq!(counts.cached, 0);
}

/// Stream with only cached and all-zero prompt/completion: finish's all-zero check must
/// fire — a {0,0,cached} shape with cached as the only live field must not be passed
/// through as real usage.
#[test]
fn stream_scanner_cached_only_stream_falls_back_to_estimation() {
    let mut s = StreamScanner::new();
    s.push(&Bytes::from_static(
        b"data: {\"usage\":{\"cached_tokens\":80}}\n\n",
    ));
    let (counts, _) = s.finish(7);
    // Fallback: prompt uses the input argument 7; completion = 29 payload bytes / 4 = 7;
    // the isolated cached 80 does not enter billing.
    assert_eq!(counts.prompt, 7);
    assert_eq!(counts.completion, 7);
    assert_eq!(counts.cached, 0);
}

#[test]
fn stream_scanner_extracts_cache_write_and_reasoning() {
    let mut s = StreamScanner::new();
    s.push(&Bytes::from_static(
        b"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":100,\"completion_tokens\":40,\
          \"prompt_tokens_details\":{\"cached_tokens\":30,\"cache_write_tokens\":20},\
          \"completion_tokens_details\":{\"reasoning_tokens\":12}}}\n\n",
    ));
    let (counts, _) = s.finish(0);
    assert_eq!(counts.prompt, 100);
    // OpenAI already includes reasoning inside completion_tokens: no double add.
    assert_eq!(counts.completion, 40);
    assert_eq!(counts.cached, 30);
    assert_eq!(counts.cache_creation, 20);
    assert_eq!(counts.reasoning, 12);
}

#[test]
fn stream_scanner_extracts_claude_cache_creation() {
    let mut s = StreamScanner::new();
    s.push(&Bytes::from_static(
        b"data: {\"type\":\"message_start\",\"message\":{\"usage\":\
          {\"input_tokens\":10,\"cache_creation_input_tokens\":6,\"cache_read_input_tokens\":4}}}\n\n",
    ));
    let (counts, _) = s.finish(0);
    assert_eq!(counts.prompt, 10);
    assert_eq!(counts.cache_creation, 6);
    assert_eq!(counts.cached, 4);
}

#[test]
fn stream_scanner_extracts_gemini_thoughts() {
    let mut s = StreamScanner::new();
    s.push(&Bytes::from_static(
        b"data: {\"candidates\":[],\"usageMetadata\":{\"promptTokenCount\":10,\
          \"candidatesTokenCount\":20,\"cachedContentTokenCount\":3,\"thoughtsTokenCount\":7}}\n\n",
    ));
    let (counts, _) = s.finish(0);
    assert_eq!(counts.prompt, 10);
    // Thinking is billed as output: completion = candidates 20 + thoughts 7.
    assert_eq!(counts.completion, 27);
    assert_eq!(counts.cached, 3);
    assert_eq!(counts.reasoning, 7);
}

#[test]
fn stream_scanner_merges_quantities_across_frames() {
    let mut s = StreamScanner::new();
    s.push(&Bytes::from_static(
        b"data: {\"type\":\"message_start\",\"message\":{\"usage\":\
          {\"input_tokens\":10,\"cache_creation_input_tokens\":6}}}\n\n",
    ));
    s.push(&Bytes::from_static(
        b"data: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":9}}\n\n",
    ));
    let (counts, _) = s.finish(0);
    assert_eq!(counts.prompt, 10);
    assert_eq!(counts.cache_creation, 6);
    assert_eq!(counts.completion, 9);
}

/// The cache-shape flag drives billing: OpenAI/Gemini include cached tokens in the
/// prompt counter, Claude reports them outside it.
#[test]
fn stream_scanner_tracks_whether_prompt_includes_cache() {
    let cases: [(&[u8], bool); 3] = [
        (
            b"data: {\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":1}}\n\n",
            true,
        ),
        (
            b"data: {\"usage\":{\"input_tokens\":10,\"output_tokens\":1,\"cache_read_input_tokens\":2}}\n\n",
            false,
        ),
        (
            b"data: {\"usageMetadata\":{\"promptTokenCount\":10,\"candidatesTokenCount\":1}}\n\n",
            true,
        ),
    ];
    for (frame, expected) in cases {
        let mut s = StreamScanner::new();
        s.push(&Bytes::copy_from_slice(frame));
        let (counts, _) = s.finish(0);
        assert_eq!(
            counts.prompt_includes_cache,
            expected,
            "frame: {}",
            String::from_utf8_lossy(frame)
        );
    }
}

/// Purpose: prove `push` carries a partial line across chunks byte-identically.
/// Scenario: one valid usage-carrying SSE payload (`OpenAI` shape, `\r\n\r\n`
/// terminator) is fed in six pushes split at awkward boundaries — mid `data: `
/// prefix, mid-JSON body, and inside the `\r\n` line terminator — so most calls
/// must carry a pending partial line over to the next chunk.
/// Acceptance: `finish()` returns exactly the same `TokenCounts` fields and finish
/// reason as feeding the same bytes in a single `push`, and the literal usage in
/// the payload is really parsed (guards against both scanners silently agreeing on
/// an empty result). A `push` rewrite that drops or double-scans carried-over bytes
/// across chunks corrupts the JSON line (losing `usage/finish_reason`) or miscounts
/// payload bytes, and turns this red.
#[test]
fn stream_scanner_chunk_splits_match_single_push() {
    let chunks: [&[u8]; 6] = [
        b"dat",                                                         // mid `data: ` prefix
        b"a: {\"choices\":[{\"finish_",                                 // mid-JSON
        b"reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":11,\"compl", // still mid-JSON
        b"etion_tokens\":7}}\r",                                        // ends before the line's \n
        b"\n", // splits inside the `\r\n` terminator
        b"\n", // closes the empty second line
    ];

    // Reference: the same bytes in one push.
    let mut whole = StreamScanner::new();
    whole.push(&Bytes::from(chunks.concat()));
    let (want, want_reason) = whole.finish(0);

    let mut split = StreamScanner::new();
    for chunk in chunks {
        split.push(&Bytes::from_static(chunk));
    }
    let (got, got_reason) = split.finish(0);

    assert_eq!(got.prompt, want.prompt);
    assert_eq!(got.completion, want.completion);
    assert_eq!(got.cached, want.cached);
    assert_eq!(got.cache_creation, want.cache_creation);
    assert_eq!(got.reasoning, want.reasoning);
    assert_eq!(got.prompt_includes_cache, want.prompt_includes_cache);
    assert_eq!(got_reason, want_reason);

    // Literal expectations: the split feed must really parse the payload, not just
    // agree with the reference on a degenerate all-zero result.
    assert_eq!(got.prompt, 11);
    assert_eq!(got.completion, 7);
    assert_eq!(got_reason.as_deref(), Some("stop"));
}
