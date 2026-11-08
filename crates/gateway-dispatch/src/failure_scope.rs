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
pub fn classify_channel_scope(status: u16) -> FailureScope {
    match status {
        401 | 403 | 404 => FailureScope::Channel,
        _ => FailureScope::Request,
    }
}
