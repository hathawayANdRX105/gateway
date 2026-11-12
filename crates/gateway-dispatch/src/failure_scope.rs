//! P1-B channel relevance classification. 401/403 (credentials) and 404 (this
//! channel lacks the model) may succeed on another channel → Channel;
//! 400/413/422 (the request itself is bad) fail on any channel → Request.
//! Reference: api-hub's deterministic error demotion (:466-474) and sub2api's
//! three-state `NextAccountAction`.

/// Whether a single 4xx failure is worth retrying on another channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureScope {
    /// Another channel may succeed.
    Channel,
    /// The request itself is faulty; switching channels cannot help.
    Request,
}

/// Classification limited to "channel-scoped 4xx". 5xx/429 do not go through this
/// function (retryable classification already lives in `egress::classify_status`).
#[must_use]
pub fn classify_channel_scope(status: u16) -> FailureScope {
    match status {
        401 | 403 | 404 => FailureScope::Channel,
        _ => FailureScope::Request,
    }
}

/// Detects the context-window-exceeded family of upstream errors (the request does
/// not fit the selected model's context window).
///
/// Matched case-insensitively against the upstream error **message preview** (the
/// 2048-byte lossy decode forward already carries); providers phrase it differently
/// — `OpenAI` returns the `context_length_exceeded` code, Anthropic "prompt is too
/// long", Gemini "exceeds the maximum number of tokens". Extend the list when a new
/// provider phrasing shows up in production logs; a false negative degrades to the
/// pre-existing behavior (the error passes through, no escalation), a false positive
/// only costs one extra dispatch round.
#[must_use]
pub fn is_context_overflow(status: u16, message: &str) -> bool {
    if !matches!(status, 400 | 413) {
        return false;
    }
    let message = message.to_ascii_lowercase();
    [
        "context_length_exceeded",
        "maximum context length",
        "context length",
        "prompt is too long",
        "exceeds the maximum number of tokens",
        "context window",
        "too many input tokens",
        "input tokens exceed",
    ]
    .iter()
    .any(|needle| message.contains(needle))
}
