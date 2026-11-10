//! Filter-word scanning — one-shot text ([`WordFilter`]) plus cross-chunk streaming
//! ([`StreamFilter`])
//!
//! Wiring location and configuration format are documented in the crate root.

use std::borrow::Cow;
use std::sync::Arc;

use aho_corasick::AhoCorasick;

/// One-shot text filter: ASCII case-insensitive word-list scanning plus replacement.
///
/// An empty word list builds successfully but never matches; without a match
/// [`filter`](Self::filter) returns `Cow::Borrowed` (zero-allocation hot path).
pub struct WordFilter {
    ac: AhoCorasick,
    /// `replace_all` requires one replacement string per pattern; all of them point at
    /// the same `replacement` here.
    replacements: Vec<String>,
    max_pattern_len: usize,
}

impl WordFilter {
    /// Build from [`FilterConfig`](crate::wordlist::FilterConfig).
    ///
    /// `ascii_case_insensitive(true)`: the filter-word scenario ignores ASCII case by
    /// default. CJK has no notion of case and is unaffected.
    ///
    /// Empty entries are dropped: `aho-corasick` matches an empty pattern at every
    /// character gap, so a typo such as `words = [""]` would turn `"a bad"` into
    /// `"XaX XbXaXdX"`, make `is_match` always true, and defeat the zero-allocation
    /// fast path. Configuration is a trust boundary, so this must be rejected.
    ///
    /// # Panics
    /// Panics when the word list pushes the automaton past `aho-corasick`'s state limit.
    /// Filtering is a safety feature, so crashing at startup beats silently running
    /// unfiltered — returning `Result` would let callers ignore the error and keep
    /// running a gateway that does not filter.
    // Deliberate fail-fast: a filter that silently fails to build would leave the
    // gateway unfiltered (see the # Panics note above), so we panic rather than
    // return Result and let callers ignore the error.
    #[allow(unknown_lints)]
    #[allow(expect_in_non_result)]
    pub fn new(config: &crate::wordlist::FilterConfig) -> Self {
        let words: Vec<&str> = config
            .words
            .iter()
            .map(String::as_str)
            .filter(|w| !w.is_empty())
            .collect();
        let ac = AhoCorasick::builder()
            .ascii_case_insensitive(true)
            .build(&words)
            .expect("filter word list must build an Aho-Corasick automaton");
        Self {
            replacements: vec![config.replacement.clone(); words.len()],
            max_pattern_len: words.iter().map(|w| w.len()).max().unwrap_or(0),
            ac,
        }
    }

    /// Filter text (ASCII case-insensitive), replacing matches with the configured
    /// `replacement`.
    ///
    /// Returns `Cow::Borrowed` when nothing matches or the word list is empty — most
    /// requests do not match and this is the hot path, so it must not allocate.
    #[must_use]
    pub fn filter<'a>(&self, text: &'a str) -> Cow<'a, str> {
        if self.replacements.is_empty() || !self.ac.is_match(text) {
            return Cow::Borrowed(text);
        }
        Cow::Owned(self.ac.replace_all(text, &self.replacements))
    }

    /// Emit boundary for streaming filtering: the number of bytes in `text` safe to
    /// output.
    ///
    /// Rule: the last `max_pattern_len - 1` bytes may be the first half of a word
    /// spanning chunks and must be held; a completed match need not wait, so the
    /// boundary is the larger of "end of the last match" and "`len - hold`". The
    /// boundary never falls inside a match.
    ///
    /// The returned value is backed off to a UTF-8 character boundary.
    fn emit_boundary(&self, text: &str) -> usize {
        let hold = self.max_pattern_len.saturating_sub(1);
        let by_len = text.len().saturating_sub(hold);
        let by_match = self.ac.find_iter(text).last().map_or(0, |m| m.end());
        let at = by_len.max(by_match);
        // The slice must fall on a character boundary: a CJK character is 3 bytes and
        // cutting through it panics. by_match comes from a match end and is always on a
        // boundary; only by_len can land inside a character. floor_char_boundary is an
        // O(1) lookup (at most a 3-byte backwalk, and 0 stays 0).
        text.floor_char_boundary(at)
    }

    /// Whether the word list is empty (callers can skip filtering entirely).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.replacements.is_empty()
    }

    /// Check for a match, for callers that only need a yes/no answer.
    #[must_use]
    pub fn has_match(&self, text: &str) -> bool {
        self.ac.is_match(text)
    }
}

/// Cross-chunk streaming filter: handles streaming text (such as SSE) split into
/// arbitrary lengths by the network layer.
///
/// A filter word can straddle a chunk boundary (`"my sec"` + `"ret here"`), matching in
/// neither chunk alone. `push` therefore outputs only the prefix **known to no longer
/// participate in matching** and keeps the rest in `pending` for the next chunk.
/// Splitting happens in the pre-replacement source coordinate system and only the prefix
/// to emit is replaced — the reverse (replace first, then split by source length) would
/// misalign and could even panic past the end as replacement changes lengths.
///
/// ponytail: holding the tail delays that content by one chunk; with very long words in
/// the list the first-character latency grows accordingly. For something finer, use
/// aho-corasick's low-level `Automaton` API to read the state-machine position and hold
/// only the bytes actually mid-match.
pub struct StreamFilter {
    filter: Arc<WordFilter>,
    /// **Source text** not yet confirmed emittable (unreplaced).
    pending: String,
}

impl StreamFilter {
    /// Create a streaming filter from a shared [`WordFilter`].
    #[must_use]
    pub fn new(filter: Arc<WordFilter>) -> Self {
        Self {
            filter,
            pending: String::new(),
        }
    }

    /// Push one chunk and return the (filtered) text confirmed safe to output.
    ///
    /// With an empty word list the chunk passes through unbuffered; buffering would add
    /// first-character latency for no reason.
    ///
    /// Contract: concatenating all `push` return values in order and then appending
    /// [`flush`](Self::flush) equals one [`WordFilter::filter`] call on the full source
    /// text. How many bytes a single call returns is an internal hold policy.
    pub fn push(&mut self, chunk: &str) -> String {
        if self.filter.is_empty() {
            return chunk.to_string();
        }

        self.pending.push_str(chunk);

        let at = self.filter.emit_boundary(&self.pending);
        if at == 0 {
            return String::new();
        }

        let tail = self.pending.split_off(at);
        let emit = std::mem::replace(&mut self.pending, tail);
        self.filter.filter(&emit).into_owned()
    }

    /// Emit everything left in `pending` (filtered) when the stream ends.
    ///
    /// State resets afterwards, so the same instance can serve the next stream.
    pub fn flush(&mut self) -> String {
        let out = self.filter.filter(&self.pending).into_owned();
        self.pending.clear();
        out
    }
}
