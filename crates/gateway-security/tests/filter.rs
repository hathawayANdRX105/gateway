//! Word-filter tests — `WordFilter` one-shot filtering plus `StreamFilter` cross-chunk
//! streaming filtering.
//!
//! What is asserted is the consumer-observable contract: the filtered output and the
//! zero-allocation fast path.
//! How many bytes a single `push` returns is an internal hold policy and is not pinned down.

use std::borrow::Cow;
use std::sync::Arc;

use gateway_security::{FilterConfig, StreamFilter, WordFilter};

/// FilterConfig's default behavior in `wordlist.rs` agrees with its serde behavior.
#[test]
fn test_filter_config_default() {
    // With only words given, replacement should be "***" and both switches true.
    let config = FilterConfig {
        words: vec!["secret".to_string()],
        ..Default::default()
    };
    assert_eq!(config.replacement, "***");
    assert!(config.filter_request);
    assert!(config.filter_response);
}

/// With an empty wordlist, WordFilter::is_empty returns true and filter returns the text
/// unchanged (zero allocation).
#[test]
fn test_empty_wordlist() {
    let config = FilterConfig {
        words: vec![],
        replacement: "***".to_string(),
        filter_request: true,
        filter_response: true,
    };
    let filter = WordFilter::new(&config);
    assert!(filter.is_empty());

    // With no match, filter returns Cow::Borrowed (zero allocation).
    let text = "Hello world";
    match filter.filter(text) {
        Cow::Borrowed(s) => assert_eq!(s, text),
        Cow::Owned(_) => panic!("empty word table should return Borrowed"),
    }

    // has_match should be false.
    assert!(!filter.has_match(text));
}

/// Empty entries in the configuration must be discarded.
///
/// Regression test: `aho-corasick` matches an empty pattern at every character boundary, so
/// `words = [""]` would turn `"a bad thing"` into `"XaX XbXaXdX XtXhXiXnXgX"` and make
/// `is_match` always true, defeating the zero-allocation fast path. Configuration is a trust
/// boundary.
#[test]
fn empty_pattern_in_config_is_discarded() {
    let only_empty = WordFilter::new(&FilterConfig {
        words: vec![String::new()],
        replacement: "X".into(),
        ..Default::default()
    });
    assert!(
        only_empty.is_empty(),
        "empty-only config equals no word list"
    );
    assert!(!only_empty.has_match("anything"));
    assert!(matches!(only_empty.filter("a bad thing"), Cow::Borrowed(_)));

    // Empty entries mixed with real ones: real entries still apply, empty ones do not
    // pollute the output.
    let mixed = WordFilter::new(&FilterConfig {
        words: vec![String::new(), "bad".into()],
        replacement: "***".into(),
        ..Default::default()
    });
    assert_eq!(mixed.filter("a bad thing").as_ref(), "a *** thing");
    assert!(!mixed.has_match("clean text"));
    assert!(matches!(mixed.filter("clean text"), Cow::Borrowed(_)));
}

/// Case-insensitive replacement works: "Secret" with mixed case must become "***".
#[test]
fn test_case_insensitive_match() {
    let config = FilterConfig {
        words: vec!["secret".to_string()],
        replacement: "***".to_string(),
        ..Default::default()
    };
    let filter = WordFilter::new(&config);
    assert!(filter.has_match("Secret"));
    assert_eq!(filter.filter("Secret").as_ref(), "***");
    assert_eq!(filter.filter("my secret text").as_ref(), "my *** text");
}

/// Multiple and overlapping matches are replaced correctly (AhoCorasick guarantees
/// correctness).
#[test]
fn test_multiple_matches() {
    let config = FilterConfig {
        words: vec!["bad".to_string(), "word".to_string()],
        replacement: "***".to_string(),
        ..Default::default()
    };
    let filter = WordFilter::new(&config);
    let text = "This is a bad word and another bad.";
    assert_eq!(
        filter.filter(text).as_ref(),
        "This is a *** *** and another ***."
    );
}

/// Feed a chunk sequence into `StreamFilter` and return the concatenation of every push
/// output plus flush.
///
/// This is the only contract of `StreamFilter`: clients see the concatenated stream, while
/// how many bytes a single `push` returns is an internal hold policy that tests must not pin
/// down.
fn drive(words: &[&str], replacement: &str, chunks: &[&str]) -> String {
    let config = FilterConfig {
        words: words.iter().map(|w| w.to_string()).collect(),
        replacement: replacement.to_string(),
        ..Default::default()
    };
    let filter = Arc::new(WordFilter::new(&config));
    let mut sf = StreamFilter::new(filter);
    let mut out = String::new();
    for c in chunks {
        out.push_str(&sf.push(c));
    }
    out.push_str(&sf.flush());
    out
}

/// Cross-chunk match: "secret" is split into "my sec" + "ret here", and neither chunk
/// matches on its own. This is the sole reason `StreamFilter` exists — calling `WordFilter`
/// per chunk would let the word through.
#[test]
fn stream_filter_catches_word_split_across_chunks() {
    assert_eq!(
        drive(&["secret"], "***", &["my sec", "ret here"]),
        "my *** here"
    );
    // Control: one-shot filtering per chunk lets it through, proving the cross-chunk logic
    // is not redundant.
    let one_shot = WordFilter::new(&FilterConfig {
        words: vec!["secret".into()],
        replacement: "***".into(),
        ..Default::default()
    });
    let leaked: String = ["my sec", "ret here"]
        .iter()
        .map(|c| one_shot.filter(c).into_owned())
        .collect();
    assert_eq!(
        leaked, "my secret here",
        "per-chunk filtering inevitably leaks it"
    );
}

/// UTF-8 boundaries: one CJK character is 3 bytes, so a byte-based `hold_len` lands in the
/// middle of a character and must fall back to a character boundary; slicing outside
/// `is_char_boundary` panics outright.
///
/// A two-character CJK word is 6 bytes → `hold_len = 5`, so feeding it one character (3
/// bytes) at a time forces a fallback on every split.
#[test]
fn stream_filter_handles_utf8_split_mid_character() {
    let text = "this is sensitive content";
    let chunks: Vec<String> = text.chars().map(|c| c.to_string()).collect();
    let refs: Vec<&str> = chunks.iter().map(String::as_str).collect();
    assert_eq!(drive(&["sensitive"], "***", &refs), "this is *** content");

    // Mixed split: the two characters of the CJK word land in different chunks, with ASCII
    // on both sides.
    assert_eq!(drive(&["sensitive"], "#", &["absens", "itivecd"]), "ab#cd");
}

/// The word spans three chunks: the basic case feeds 2-byte chunks, and the variant pads ASCII
/// around the same split.
#[test]
fn stream_filter_catches_word_split_across_three_chunks() {
    assert_eq!(drive(&["abcdef"], "***", &["ab", "cd", "ef"]), "***");
    assert_eq!(drive(&["abcdef"], "***", &["xab", "cd", "efy"]), "x***y");
}

/// Adjacent repeated words: `ab` repeats back to back in `abababab`, and a hold boundary
/// computed from a fixed length alone would split the word apart and let it through.
/// This is a regression test — a previous implementation both panicked and leaked here.
#[test]
fn stream_filter_handles_adjacent_repeated_words() {
    assert_eq!(drive(&["ab"], "", &["ababababab"]), "");
    assert_eq!(drive(&["ab"], "-", &["aba", "bab", "ab"]), "----");
}

/// State resets after `flush`, so the same instance can serve the next stream.
#[test]
fn stream_filter_resets_after_flush() {
    let filter = Arc::new(WordFilter::new(&FilterConfig {
        words: vec!["secret".into()],
        replacement: "***".into(),
        ..Default::default()
    }));
    let mut sf = StreamFilter::new(filter);

    let mut first = sf.push("secret");
    first.push_str(&sf.flush());
    assert_eq!(first, "***");

    // No bytes from the previous stream remain after flush.
    let mut second = sf.push("plain");
    second.push_str(&sf.flush());
    assert_eq!(second, "plain");
}

/// A replacement longer or shorter than the word must not shift anything: length changes do
/// not affect hold-boundary computation.
#[test]
fn stream_filter_handles_replacement_length_change() {
    // Shortening: 6 bytes → 0 bytes
    assert_eq!(drive(&["secret"], "", &["a secret b"]), "a  b");
    // Lengthening: 1 byte → 4 bytes
    assert_eq!(drive(&["x"], "LONG", &["axbxc"]), "aLONGbLONGc");
}

/// Empty wordlist: no buffering, no allocation, pass through unchanged. Buffering would add
/// first-token latency for nothing.
#[test]
fn stream_filter_passes_through_when_wordlist_empty() {
    assert_eq!(drive(&[], "***", &["hello ", "world"]), "hello world");
}
