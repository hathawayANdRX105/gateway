use bytes::Bytes;
use gateway_protocol_bridge::sse::{SseEnd, SseEvent, SseScanner};

#[test]
fn sse_scanner_first_data_line_triggers_first_token() {
    let mut s = SseScanner::default();
    let (pass, events) = s.push(&Bytes::from_static(b"data: {\"role\":\"assistant\"}\n\n"));
    assert_eq!(
        pass,
        Bytes::from_static(b"data: {\"role\":\"assistant\"}\n\n")
    );
    assert_eq!(events, vec![SseEvent::FirstToken]);
}

#[test]
fn sse_scanner_second_data_line_no_first_token() {
    let mut s = SseScanner::default();
    let _ = s.push(&Bytes::from_static(b"data: {\"role\":\"assistant\"}\n\n"));
    let (_, events) = s.push(&Bytes::from_static(b"data: {\"content\":\"hello\"}\n\n"));
    assert!(events.is_empty());
}

#[test]
fn sse_scanner_done_triggers_clean_end() {
    let mut s = SseScanner::default();
    let _ = s.push(&Bytes::from_static(b"data: {\"role\":\"assistant\"}\n\n"));
    let (_, events) = s.push(&Bytes::from_static(b"data: [DONE]\n\n"));
    assert!(events.is_empty());
    assert_eq!(s.finish(), SseEnd::Clean);
}

#[test]
fn sse_scanner_truncated_when_no_done() {
    let mut s = SseScanner::default();
    let _ = s.push(&Bytes::from_static(b"data: {\"role\":\"assistant\"}\n\n"));
    assert_eq!(s.finish(), SseEnd::Truncated);
}

#[test]
fn sse_scanner_ping_line_triggers_ping_event() {
    let mut s = SseScanner::default();
    let (_, events) = s.push(&Bytes::from_static(b": keepalive\n\n"));
    assert_eq!(events, vec![SseEvent::Ping]);
}

#[test]
fn sse_scanner_usage_field_triggers_usage_event() {
    let mut s = SseScanner::default();
    let (_, events) = s.push(&Bytes::from_static(
        b"data: {\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5}}\n\n",
    ));
    assert_eq!(events.len(), 2);
    assert_eq!(events[0], SseEvent::FirstToken);
    assert_eq!(events[1], SseEvent::Usage);
}

#[test]
fn sse_scanner_cross_chunk_line_reassembly() {
    let mut s = SseScanner::default();
    let (p1, e1) = s.push(&Bytes::from_static(b"data: {\"role\""));
    assert!(e1.is_empty());
    let (p2, e2) = s.push(&Bytes::from_static(b":\"assistant\"}\n\n"));
    assert_eq!(e2, vec![SseEvent::FirstToken]);
    // Passthrough must be byte-faithful: the two chunks reassemble into the original
    // bytes, with nothing swallowed or altered.
    let mut passthrough = p1.to_vec();
    passthrough.extend_from_slice(&p2);
    assert_eq!(
        passthrough.as_slice(),
        b"data: {\"role\":\"assistant\"}\n\n"
    );
}

#[test]
fn sse_scanner_empty_lines_are_frame_separators() {
    let mut s = SseScanner::default();
    let (_, events) = s.push(&Bytes::from_static(b"\n\n\n"));
    assert!(events.is_empty());
}

#[test]
fn sse_scanner_crlf_handling() {
    let mut s = SseScanner::default();
    let (_, events) = s.push(&Bytes::from_static(
        b"data: {\"role\":\"assistant\"}\r\n\r\n",
    ));
    assert_eq!(events, vec![SseEvent::FirstToken]);
}
