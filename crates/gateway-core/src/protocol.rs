//! Inbound protocol vocabulary: the client-facing protocol kind plus the
//! classification of a request path into a recognized URL shape.
//!
//! This is ingress grammar — "which vendor dialect is the client speaking, and what
//! does the URL itself address" — not byte conversion (that lives in the codec
//! engines). It sits in the shared surface because the classification result must be
//! readable by every stage crate (gate needs the path-carried model before forward
//! runs) while the engines that act on it stay one layer up.
//!
//! Parsing is pure and segment-based: the path is split on `/`, empty segments are
//! dropped, the version segment decides the dialect family. Deliberately **no**
//! substring matching — the old path sniffing judged `contains("gemini")` anywhere in
//! the path, which misclassified unrelated paths that merely carried the word.

/// Protocol used by the client request
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProtocolKind {
    OpenAI,
    Anthropic,
    Gemini,
    OpenAIResp,
}

/// A recognized inbound data-plane URL shape.
///
/// Built once per request by `RequestCtx::from_axum` (via [`RequestPath::parse`]).
/// Each variant is one URL dialect; [`RequestPath::protocol`] is the single place
/// that maps a shape to its [`ProtocolKind`]. Shapes outside the four protocol
/// families (and unrecognized `/v1` operations) collapse to
/// [`RequestPath::Unknown`] and keep the `OpenAI` treatment, matching the long-standing
/// fallback behavior.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestPath {
    /// `/v1/chat/completions` — `OpenAI` Chat Completions.
    ChatCompletions,
    /// `/v1/responses` — `OpenAI` Responses.
    Responses,
    /// `/v1/messages` (and subpaths such as `count_tokens`) — Anthropic Messages.
    Messages,
    /// `/v1/models` (and subpaths) — model listing; served outside the pipeline,
    /// kept as a shape so the classification is total over the guarded path scope.
    Models,
    /// Targeting one Gemini model resource (`/v1beta/models/{model}[:verb]`, or the
    /// same shape mounted under `/v1`).
    ///
    /// Gemini carries the model in the URL, not the body, and the stream flag in
    /// the verb (`:streamGenerateContent`) — the only inbound shape where the path
    /// alone decides streaming. Any colon verb still extracts the model (countTokens
    /// and friends address the same model resource); `stream` is set only by the
    /// stream verb.
    GeminiGenerate {
        /// Real model name from the `models/{model}` segment; absent when the URL
        /// does not address one model.
        model: Option<String>,
        /// True when the verb is `:streamGenerateContent`.
        stream: bool,
    },
    /// Any other Gemini-dialect path (model listing, embeddings, deeper or junker
    /// shapes such as `/v1beta/models/a/b:generateContent`): the dialect is known,
    /// no single model resource is addressed, so no model is extracted.
    GeminiOther,
    /// Anything else, including `/v1` itself — treated as `OpenAI` dialect.
    Unknown,
}

impl RequestPath {
    /// Classify a raw request path by segments.
    ///
    /// The query string and a trailing slash are normalized away first, so callers
    /// may pass the URI path as-is.
    #[must_use]
    pub fn parse(raw_path: &str) -> Self {
        let path = raw_path.split_once('?').map_or(raw_path, |(path, _)| path);
        let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
        match segments.as_slice() {
            ["v1", "messages", ..] => Self::Messages,
            ["v1", "responses", ..] => Self::Responses,
            ["v1", "chat", "completions", ..] => Self::ChatCompletions,
            // Exactly three segments: a deeper shape (extra path between the model
            // and the verb) addresses no single model, so it must not yield a
            // polluted model extraction once the gate starts consuming it.
            ["v1beta", "models", marker] => Self::gemini_at(marker),
            ["v1beta", ..] => Self::GeminiOther,
            // A `:generateContent` verb addresses Gemini even without the v1beta
            // prefix; some relays mount the dialect under /v1.
            ["v1", "models", marker] if is_gemini_verb(marker) => Self::gemini_at(marker),
            ["v1", "models", ..] => Self::Models,
            _ => Self::Unknown,
        }
    }

    /// The protocol dialect this URL shape belongs to.
    #[must_use]
    pub const fn protocol(&self) -> ProtocolKind {
        match self {
            Self::Messages => ProtocolKind::Anthropic,
            Self::Responses => ProtocolKind::OpenAIResp,
            Self::GeminiGenerate { .. } | Self::GeminiOther => ProtocolKind::Gemini,
            Self::ChatCompletions | Self::Models | Self::Unknown => ProtocolKind::OpenAI,
        }
    }

    /// Split a `models/{model}[:verb]` segment: without a colon the whole segment is
    /// the model name; the verb decides streaming.
    fn gemini_at(marker: &str) -> Self {
        let (model, verb) = marker.split_once(':').unwrap_or((marker, ""));
        Self::GeminiGenerate {
            model: (!model.is_empty()).then(|| model.to_string()),
            stream: verb == "streamGenerateContent",
        }
    }
}

/// Whether a `models/{model}[:verb]` segment names a generateContent target.
/// (`:streamGenerateContent` does not end with `:generateContent` — the `:` anchors
/// the version prefix boundary.)
fn is_gemini_verb(marker: &str) -> bool {
    marker.ends_with(":generateContent") || marker.ends_with(":streamGenerateContent")
}
