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
    assert_eq!(events, [] as [gateway_protocol_bridge::sse::SseEvent; 0]);
}

#[test]
fn sse_scanner_done_triggers_clean_end() {
    let mut s = SseScanner::default();
    let _ = s.push(&Bytes::from_static(b"data: {\"role\":\"assistant\"}\n\n"));
    let (_, events) = s.push(&Bytes::from_static(b"data: [DONE]\n\n"));
    assert_eq!(events, [] as [gateway_protocol_bridge::sse::SseEvent; 0]);
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
    assert_eq!(e1, [] as [gateway_protocol_bridge::sse::SseEvent; 0]);
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
    assert_eq!(events, [] as [gateway_protocol_bridge::sse::SseEvent; 0]);
}

#[test]
fn sse_scanner_crlf_handling() {
    let mut s = SseScanner::default();
    let (_, events) = s.push(&Bytes::from_static(
        b"data: {\"role\":\"assistant\"}\r\n\r\n",
    ));
    assert_eq!(events, vec![SseEvent::FirstToken]);
}
#[test]
fn sse_scanner_no_newline_stream_flushes_in_bounded_pieces() {
    // Scenario: a pathological no-newline stream longer than the waterline.
    // Acceptance: every push past the waterline flushes its retained payload into
    // a frame the consumer can drain immediately, so retention never grows with
    // the stream length; passthrough stays byte-faithful.
    let mut s = SseScanner::new(32);
    // `data: ` is a 6-byte prefix; the rest is the retained payload.
    let payload = Bytes::from_static(b"data: xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
    let mut flushed = 0usize;
    let mut passthrough = Vec::new();
    for _ in 0..50 {
        let (pass, _events) = s.push(&payload);
        passthrough.extend_from_slice(&pass);
        let frames = s.take_data_frames();
        assert!(
            !frames.is_empty(),
            "every push over the waterline must flush a frame"
        );
        let piece: usize = frames.iter().map(String::len).sum();
        assert!(
            piece <= payload.len(),
            "a flush piece must not exceed the chunk that filled it"
        );
        flushed += piece;
    }
    assert_eq!(flushed, 50 * (payload.len() - 6));
    assert_eq!(passthrough, payload.repeat(50).clone());
    // Nothing is retained after the flushes.
    assert_eq!(s.take_data_frames(), Vec::<String>::new());
    assert_eq!(s.flush_pending_frame(), Vec::<String>::new());
    assert_eq!(s.finish(), SseEnd::Truncated);
}

#[test]
fn sse_scanner_events_and_done_still_fire_across_a_flush() {
    // Scenario: flush happens at a line boundary (a safe point), mid-stream.
    // Acceptance: FirstToken still fires on the flushed frame, the flushed frame
    // is drainable immediately, passthrough stays byte-faithful, and a later
    // [DONE] still terminates the stream cleanly.
    let mut s = SseScanner::new(16);
    let mut passthrough = Vec::new();

    // Below the waterline: an incomplete line is retained, nothing flushed.
    let (p1, e1) = s.push(&Bytes::from_static(b"data: {\"role\""));
    passthrough.extend_from_slice(&p1);
    assert_eq!(e1, [] as [SseEvent; 0]);
    assert_eq!(s.take_data_frames(), Vec::<String>::new());

    // This chunk completes the line and pushes retained bytes over the waterline,
    // so the frame is flushed mid-stream instead of being held for EOF.
    let (p2, e2) = s.push(&Bytes::from_static(b":\"assistant\"}\n\n"));
    passthrough.extend_from_slice(&p2);
    assert_eq!(e2, vec![SseEvent::FirstToken]);
    assert_eq!(s.take_data_frames(), vec![r#"{"role":"assistant"}"#]);

    let mut expected = b"data: {\"role\"".to_vec();
    expected.extend_from_slice(b":\"assistant\"}\n\n");
    assert_eq!(passthrough, expected);

    // [DONE] after a flush is still detected and ends the stream cleanly.
    let (_, e3) = s.push(&Bytes::from_static(b"data: [DONE]\n\n"));
    assert_eq!(e3, [] as [SseEvent; 0]);
    assert_eq!(s.finish(), SseEnd::Clean);
}

#[test]
fn sse_scanner_below_waterline_is_byte_identical() {
    // Scenario: input smaller than the waterline, with one complete frame and a
    // [DONE] terminator.
    // Acceptance: identical to the pre-waterline scanner — byte-faithful
    // passthrough, Ping then FirstToken events, frame released at its blank-line
    // separator, Clean end.
    let mut s = SseScanner::new(4096);
    let input = b": keepalive\ndata: {\"role\":\"assistant\"}\n\ndata: [DONE]\n\n";
    let (pass, events) = s.push(&Bytes::from_static(input));
    assert_eq!(pass, Bytes::from_static(input));
    assert_eq!(
        events,
        vec![SseEvent::Ping, SseEvent::FirstToken]
    );
    assert_eq!(s.take_data_frames(), vec![r#"{"role":"assistant"}"#]);
    assert_eq!(s.finish(), SseEnd::Clean);
}

#[test]
fn sse_scanner_default_keeps_unbounded_behavior() {
    // Scenario: `Default` (unbounded waterline) fed 2000 chunks with no blank
    // separator anywhere.
    // Acceptance: nothing is flushed mid-stream — retention behaves exactly as
    // before the waterline existed; passthrough stays byte-faithful and
    // end-of-stream releases the whole retained payload at once.
    let mut s = SseScanner::default();
    let chunk = Bytes::from_static(b"data: {\"role\":\"assistant\"}\n");
    let payload = br#"{"role":"assistant"}"#;
    let mut passthrough = Vec::new();
    for _ in 0..2000 {
        let (pass, _events) = s.push(&chunk);
        passthrough.extend_from_slice(&pass);
    }
    assert_eq!(s.take_data_frames(), Vec::<String>::new());
    let mut expected_payload = Vec::new();
    for i in 0..2000 {
        if i > 0 {
            expected_payload.push(b'\n');
        }
        expected_payload.extend_from_slice(payload);
    }
    assert_eq!(
        s.flush_pending_frame(),
        vec![String::from_utf8(expected_payload).unwrap()]
    );
    let mut expected_pass = Vec::new();
    for _ in 0..2000 {
        expected_pass.extend_from_slice(chunk.as_ref());
    }
    assert_eq!(passthrough, expected_pass);
    assert_eq!(s.finish(), SseEnd::Truncated);
}
