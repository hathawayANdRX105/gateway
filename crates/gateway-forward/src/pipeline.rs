//! Single-attempt forwarding pipeline — the core orchestration of forward.
//!
//! Pipeline (one request, one response):
//! ```text
//! ForwardTask
//!   → adapter::prepare (URL assembly / auth headers / channel parameter overrides)
//!   → request transform: inbound format → IR → upstream format
//!   → egress::execute (send with timeouts)
//!   → [response]
//!       ├─ non-streaming: upstream format → IR → inbound format, returned whole
//!       └─ streaming:     decode each SSE frame into IR events → re-encode with the
//!                         inbound format's StreamEncoder
//! ```
//!
//! ## Why translation closes the loop here (WP6 final)
//!
//! The request and response directions must cooperate through the same codecs, and
//! the inbound format is decided by the request path while the upstream format is
//! decided by the channel's `provider_type` — both are only visible together in
//! this function. The old implementation put response translation in a
//! `protocol-bridge` stage and hardcoded "response source protocol = `OpenAI`"; the
//! streaming path did not translate at all (`ctx.upstream` was only written in the
//! non-streaming branch), so streaming Claude/Gemini clients always received
//! `OpenAI` event shapes (G2).
//!
//! Failures funnel through `gateway_core::error::NormalizedError` →
//! `gateway_dispatch::FailureClass`.

use crate::ForwardTask;
use crate::adapter::{self, PreparedRequest};
use crate::egress::{ByteStream, Egress, ForwardedResponse, body_preview, upstream_error};
use bytes::Bytes;
use futures_util::{StreamExt, TryStreamExt};
use gateway_core::error::NormalizedError;
use gateway_protocol_bridge::adaptor::Protocol;
use gateway_protocol_bridge::format_codec::{FormatCodec, FormatRegistry, StreamEncoder};
use gateway_protocol_bridge::ir::StreamEvent;
use gateway_protocol_bridge::sse::SseScanner;
use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

/// Merged request headers (adapter auth + channel overrides + already-filtered
/// client headers).
///
/// The **first** header of a given name wins (case-insensitive): a channel using
/// `settings.headers` to override the auth header name must not send both, and this
/// is where adapter.rs's documented "settings can override" promise is kept.
#[must_use]
pub fn merge_headers(
    prepared: &PreparedRequest,
    client_headers: &[(String, String)],
) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> =
        Vec::with_capacity(2 + client_headers.len() + prepared.extra_headers.len());
    let mut seen: HashSet<String> = HashSet::with_capacity(out.capacity());
    for (k, v) in prepared
        .extra_headers
        .iter()
        .chain(std::iter::once(&prepared.auth_header))
        .chain(client_headers.iter())
    {
        if !seen.insert(k.to_ascii_lowercase()) {
            continue;
        }
        out.push((k.clone(), v.clone()));
    }
    out
}

/// Read the whole response body — used by the non-streaming path only.
///
/// The whole read is bounded by `stream_max_ms` so a hung upstream cannot block
/// the commit forever, and each chunk re-arms the `stream_idle_ms` idle sleep so
/// a trickling upstream is cut after the silence budget. Either timeout fails
/// with `ErrorKind::TimedOut`.
async fn read_all_body(
    resp: ForwardedResponse,
    timeouts: &crate::egress::Timeouts,
) -> Result<Bytes, std::io::Error> {
    let mut body = resp.into_body_stream();
    let mut buf = Vec::new();
    let max_dur = Duration::from_millis(timeouts.stream_max_ms);
    let idle_dur = Duration::from_millis(timeouts.stream_idle_ms);
    let max_sleep = tokio::time::sleep(max_dur);
    let idle_sleep = tokio::time::sleep(idle_dur);
    tokio::pin!(max_sleep);
    tokio::pin!(idle_sleep);
    loop {
        tokio::select! {
            biased;
            chunk = body.try_next() => match chunk {
                Ok(Some(chunk)) => {
                    buf.extend_from_slice(&chunk);
                    idle_sleep.as_mut().reset(tokio::time::Instant::now() + idle_dur);
                }
                Ok(None) => return Ok(Bytes::from(buf)),
                Err(e) => return Err(e),
            },
            () = max_sleep.as_mut() => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "stream max duration exceeded",
                ));
            }
            () = idle_sleep.as_mut() => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "stream idle timeout",
                ));
            }
        }
    }
}

/// Request-side preparation: upstream URL / auth header / override headers, plus
/// the result of merging them with client headers.
fn prepare_request(task: &ForwardTask) -> (PreparedRequest, Vec<(String, String)>) {
    // The upstream endpoint is decided by the upstream protocol + inbound format
    // (the client path only matters under passthrough).
    let url_model = resolve_upstream_model(&task.body, &task.candidate.upstream_model);
    let prepared = adapter::prepare(
        &task.candidate,
        &task.path,
        &task.provider_type,
        task.inbound_format,
        task.stream,
        &url_model,
        task.extra_headers.clone(),
    );
    let merged = merge_headers(&prepared, &task.headers);
    (prepared, merged)
}

/// Request-side transform: public alias → upstream real name, then encode the
/// inbound format into the upstream format.
fn translate_request_body(
    task: &ForwardTask,
    formats: &FormatRegistry,
    inbound: Protocol,
    upstream_format: Protocol,
) -> Result<Bytes, NormalizedError> {
    // Public alias → upstream real name: must be rewritten before the protocol
    // transform; otherwise each codec has already moved `model` into its own field
    // location, and rewriting would need per-protocol handling.
    let body = rewrite_upstream_model(&task.body, &task.candidate.upstream_model);
    let body = formats
        .translate_request(inbound, upstream_format, body)
        .map_err(|e| protocol_bridge_error(&e, 400, false))?;
    Ok(capture_prompt_body(&body))
}

/// Core orchestration of a single forward — prepare + bidirectional protocol
/// transforms + execute.
///
/// Streaming and non-streaming both return `Forwarded`: non-streaming responses are
/// also shaped as a byte stream (a single chunk).
///
/// `task.stream` decides the response shape:
/// - streaming (`stream=true`) → upstream SSE is translated frame by frame as it flows
/// - non-streaming (`stream=false`) → `read_all_body` collects it once, then
///   translates it whole
///
///
/// # Errors
///
/// Propagates upstream `NormalizedError` for non-2xx responses, transport
/// failures, and protocol-conversion errors. The error carries the upstream
/// status code and a bounded body preview.
pub async fn forward_once(
    task: &ForwardTask,
    egress: &dyn Egress,
    formats: &FormatRegistry,
    timeouts: &crate::egress::Timeouts,
) -> Result<crate::Forwarded, NormalizedError> {
    let (prepared, merged) = prepare_request(task);

    // Inbound format (the protocol the client speaks) → upstream format (the vendor
    // protocol decided by the channel's provider_type).
    let inbound = Protocol::from_kind(task.inbound_format);
    let upstream_format = Protocol::from_provider_type(&task.provider_type);
    let body = translate_request_body(task, formats, inbound, upstream_format)?;

    let resp = egress
        .execute(
            &task.candidate.unit.public_model,
            &prepared.url,
            &merged,
            body,
            timeouts,
        )
        .await?;

    let status = resp.status();
    let content_type = resp.content_type().to_string();
    // Take the Retry-After hint out before the response moves into the body stream
    // (Copy semantics); the non-2xx guard below still needs it for classify_status.
    let retry_after = resp.retry_after_ms();

    // Response direction: upstream format → inbound format. Streaming translates
    // frame by frame, non-streaming translates the whole body.
    if task.stream {
        // Defense in depth: both production Egress implementations already classify
        // non-2xx as Err inside execute, so this is normally unreachable; it catches
        // "Ok(non-2xx) passthrough" — ForwardedResponse's public constructor lets any
        // Egress do that, and if the streaming branch fed an error body into
        // passthrough/SseScanner the client would get a 4xx plus an empty/broken SSE.
        // The guard sits at the convergence point of all exits and before
        // passthrough, so error bodies take the same classify_status chain as the
        // non-streaming path (the error-shape contract locked by #238).
        if !(200..=299).contains(&status) {
            let preview = read_error_preview(resp.into_body_stream(), timeouts.total_ms).await;
            return Err(crate::egress::classify_status(
                status,
                &preview,
                retry_after,
            ));
        }
        // Same format = zero-conversion passthrough: no encoder is built at all
        // (when client and channel speak the same protocol the bytes must not be
        // rewritten, and no termination frame may be appended — the upstream [DONE]
        // is already passed through verbatim, and adding another would mean two
        // stream terminations; an e2e byte-fidelity assertion proved this trap).
        if inbound == upstream_format {
            return Ok(forwarded_from_stream(
                status,
                content_type,
                resp.into_body_stream(),
            ));
        }

        // One encoder per stream (`&mut self` holds block boundaries and tool-fragment
        // state) and it must live and die with this request — reuse would mix state.
        let encoder = formats.resolve(inbound).map(|c| c.stream_encoder());
        let decoder = formats.resolve(upstream_format);
        return Ok(forwarded_from_stream(
            status,
            content_type,
            translate_sse_stream(resp.into_body_stream(), encoder, decoder),
        ));
    }

    // Non-streaming: collect the whole upstream body, then do the two-hop transform
    // in one pass.
    let bytes = read_all_body(resp, timeouts)
        .await
        .map_err(|e| upstream_error(502, true, format!("read upstream body: {e}")))?;
    let bytes = formats
        .translate_response(upstream_format, inbound, bytes)
        .map_err(|e| protocol_bridge_error(&e, 502, true))?;

    Ok(forwarded_from_stream(
        status,
        content_type,
        Box::pin(futures_util::stream::iter(std::iter::once(Ok(bytes)))),
    ))
}

/// Assemble [`crate::Forwarded`] from a response byte stream (permit/token handles
/// are attached afterwards by the caller).
fn forwarded_from_stream(status: u16, content_type: String, body: ByteStream) -> crate::Forwarded {
    crate::Forwarded {
        status,
        channel_permit: None,
        token_handle: None,
        body,
        content_type,
    }
}

/// Bounded error preview for non-2xx streaming responses.
///
/// Reads at most 2048 bytes, bounded by the `total_ms` timeout; a timeout or read
/// error only degrades the preview to empty and does not affect status
/// classification.
async fn read_error_preview(mut body: ByteStream, total_ms: u64) -> String {
    let read = async {
        let mut buf = Vec::new();
        while let Some(chunk) = body.try_next().await? {
            buf.extend_from_slice(&chunk);
            if buf.len() >= 2048 {
                break;
            }
        }
        Ok::<Vec<u8>, std::io::Error>(buf)
    };
    match tokio::time::timeout(Duration::from_millis(total_ms), read).await {
        Ok(Ok(buf)) => body_preview(&buf),
        _ => String::new(),
    }
}

/// protocol-bridge error → `gateway_core::error::NormalizedError`, the single exit.
fn protocol_bridge_error(
    e: &gateway_protocol_bridge::adaptor::AdaptorError,
    status: u16,
    retryable: bool,
) -> NormalizedError {
    upstream_error(status, retryable, e.to_string())
}

/// Decode one `data:` payload frame into IR events.
///
/// A single-frame decode failure returns an empty list instead of an error: SSE
/// streams contain non-JSON control frames, and one unparsable frame must not break
/// the whole stream. (The old implementation parsed half a line as JSON, failed, and
/// **silently dropped** it — but even then it never got the frame boundaries right;
/// now [`SseScanner`] guarantees the boundaries, so skipping here only covers
/// genuinely valid non-content frames.)
fn decode_events(decoder: Option<&dyn FormatCodec>, frame: &str) -> Vec<StreamEvent> {
    decoder.map_or_else(Vec::new, |d| d.decode_event(frame).unwrap_or_default())
}

/// Tail-frame encoding: failure only warns and never breaks the stream (the client
/// should still get the finish frame and the complete stream tail).
fn encode_event_lossy(
    encoder: &mut Option<Box<dyn StreamEncoder>>,
    ev: &StreamEvent,
    out: &mut Vec<Bytes>,
) {
    let Some(enc) = encoder.as_mut() else {
        return;
    };
    match enc.encode_event(ev) {
        Ok(bytes) => out.extend(bytes),
        Err(e) => tracing::warn!("encode tail frame event failed: {e}"),
    }
}

/// Regular frame encoding: failures propagate and are turned into a stream error by
/// the caller.
fn encode_event_strict(
    encoder: &mut Option<Box<dyn StreamEncoder>>,
    ev: &StreamEvent,
    out: &mut Vec<Bytes>,
) -> Result<(), gateway_protocol_bridge::adaptor::AdaptorError> {
    let Some(enc) = encoder.as_mut() else {
        return Ok(());
    };
    out.extend(enc.encode_event(ev)?);
    Ok(())
}

/// Decode a batch of SSE data frames into IR events and encode them into
/// the inbound format. The first encode error aborts the batch.
fn encode_frames(
    encoder: &mut Option<Box<dyn StreamEncoder>>,
    decoder: Option<&dyn FormatCodec>,
    frames: Vec<String>,
    out: &mut Vec<Bytes>,
) -> Result<(), gateway_protocol_bridge::adaptor::AdaptorError> {
    for frame in frames {
        for ev in decode_events(decoder, &frame) {
            encode_event_strict(encoder, &ev, out)?;
        }
    }
    Ok(())
}

/// Mutable state of a cross-format SSE translation stream (one per stream, alive
/// with the request).
struct SseTranslateState {
    upstream: ByteStream,
    scanner: SseScanner,
    /// `Some` = the encoder still owes trailing frames; cleared to None after
    /// finishing to prevent duplicates.
    encoder: Option<Box<dyn StreamEncoder>>,
    decoder: Option<Arc<dyn FormatCodec>>,
}

/// Translate the upstream SSE byte stream frame by frame into the inbound format.
fn translate_sse_stream(
    upstream: ByteStream,
    encoder: Option<Box<dyn StreamEncoder>>,
    decoder: Option<Arc<dyn FormatCodec>>,
) -> ByteStream {
    Box::pin(futures_util::stream::unfold(
        SseTranslateState {
            upstream,
            scanner: SseScanner::default(),
            encoder,
            decoder,
        },
        translate_next_chunk,
    ))
}

/// Produce the next translated chunk; `None` = the upstream stream ended and all
/// trailing frames have been emitted.
async fn translate_next_chunk(
    mut state: SseTranslateState,
) -> Option<(Result<Bytes, std::io::Error>, SseTranslateState)> {
    loop {
        let chunk = match state.upstream.next().await {
            Some(Ok(chunk)) => chunk,
            Some(Err(e)) => return Some((Err(e), state)),
            None => return finish_translation(state),
        };

        // Cross-format: the scanner reassembles complete lines, each `data:` payload
        // is decoded into IR events and handed to the encoder as inbound-format
        // frames.
        let (_passthrough, _events) = state.scanner.push(&chunk);
        // Take and encode this chunk's data frames first — [DONE] often shares a
        // chunk with preceding content frames, so draining the scanner wholesale
        // first would throw the valid content away.
        let frames = state.scanner.take_data_frames();
        if !frames.is_empty() {
            let mut out = Vec::new();
            if let Err(e) = encode_frames(
                &mut state.encoder,
                state.decoder.as_deref(),
                frames,
                &mut out,
            ) {
                return Some((Err(std::io::Error::other(e.to_string())), state));
            }
            if !out.is_empty() {
                return Some((Ok(Bytes::from(out.concat())), state));
            }
        }

        // Upstream terminated explicitly: tell the encoder not to add another
        // termination frame (two [DONE]s mean two stream terminations).
        if state.scanner.saw_done() {
            if let Some(enc) = state.encoder.as_mut() {
                enc.mark_done();
            }
            // Upstream sent data frames after [DONE] — the SSE spec forbids this, and
            // encoding them would make the client receive content after the
            // termination frame. Drain and discard them without encode_event.
            let stray = state.scanner.take_data_frames();
            if !stray.is_empty() {
                tracing::warn!(
                    frames = stray.len(),
                    "SSE frames after upstream [DONE], discarding"
                );
            }
        }
    }
}

/// Upstream stream end: emit the trailing unterminated frame first (together with
/// the encoder finish), then append the finish frames separately.
fn finish_translation(
    mut state: SseTranslateState,
) -> Option<(Result<Bytes, std::io::Error>, SseTranslateState)> {
    // The last upstream frame may arrive without a trailing blank line; emit it
    // before finishing.
    let mut tail_frames = state.scanner.flush_pending_frame();
    if !tail_frames.is_empty() {
        let mut out = Vec::new();
        for frame in tail_frames.drain(..) {
            for ev in decode_events(state.decoder.as_deref(), &frame) {
                encode_event_lossy(&mut state.encoder, &ev, &mut out);
            }
        }
        if let Some(enc) = state.encoder.as_mut()
            && let Ok(mut bytes) = enc.finish()
        {
            out.append(&mut bytes);
        }
        // Finish frames went out with the tail; clear the encoder so they are not
        // emitted twice.
        state.encoder = None;
        return Some((Ok(Bytes::from(out.concat())), state));
    }

    // No unterminated tail frame: only let the encoder append its finish frames.
    let finished = state
        .encoder
        .as_mut()
        .map_or_else(|| Ok(Vec::new()), |enc| enc.finish());
    match finished {
        Ok(frames) if !frames.is_empty() => {
            // finish has emitted everything; clear it to avoid a second finish.
            state.encoder = None;
            Some((Ok(Bytes::from(frames.concat())), state))
        }
        Ok(_) => None,
        Err(e) => {
            state.encoder = None;
            Some((Err(std::io::Error::other(e.to_string())), state))
        }
    }
}

/// Pipeline execution trait — bin/api implements it with reqwest.
pub trait Pipeline: Send + Sync {
    /// Run one forwarding attempt. No retries (retrying belongs to
    /// `gateway_dispatch::retry`).
    fn execute(
        &self,
        task: &ForwardTask,
        timeouts: &crate::egress::Timeouts,
    ) -> impl Future<Output = Result<crate::Forwarded, NormalizedError>> + Send;
}

/// Default reqwest pipeline — owns the `Egress` instance.
///
/// ponytail: bin/api could pass a custom Egress later (proxy pool / rate-limited
/// Client); a single global client is enough for now.
#[derive(Clone)]
pub struct ReqwestPipeline {
    egress: Arc<dyn Egress>,
    /// Single-format protocol registry; empty = passthrough (same format is not
    /// converted).
    formats: Arc<FormatRegistry>,
}

impl std::fmt::Debug for ReqwestPipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReqwestPipeline").finish_non_exhaustive()
    }
}

impl ReqwestPipeline {
    pub fn new(egress: Arc<dyn Egress>, formats: Arc<FormatRegistry>) -> Self {
        Self { egress, formats }
    }
}

impl Pipeline for ReqwestPipeline {
    async fn execute(
        &self,
        task: &ForwardTask,
        timeouts: &crate::egress::Timeouts,
    ) -> Result<crate::Forwarded, NormalizedError> {
        forward_once(task, self.egress.as_ref(), &self.formats, timeouts).await
    }
}

/// Pre-scan hook on the transformed request body, kept as the body-level insertion
/// point for metering.
///
/// Called once after the request-side transform and before sending (only the
/// transformed body is the real upstream body).
///
/// Currently a pass-through: it returns that body unchanged. Prompt-token estimation
/// is not wired here — the stages that need it read the request body they already hold
/// and call `gateway_metering::estimate` (telemetry / retry / commit points).
pub fn capture_prompt_body(body: &bytes::Bytes) -> bytes::Bytes {
    body.clone()
}

/// Resolve the model name actually sent upstream — used for addressing by
/// "model-in-path" protocols such as Gemini.
///
/// Channel mapping wins (`upstream_model` non-empty); when `upstream_model` is
/// empty it falls back to the client model (i.e. the zero-rewrite path of
/// `rewrite_upstream_model`: the upstream model is left untouched).
///
/// The model goes straight into a URL path segment and clients can send arbitrary
/// strings — `/`, `..` or control characters would rewrite the upstream path (a
/// trust boundary). Only single-segment safe characters pass; otherwise addressing
/// is skipped and [`build_url`](crate::adapter::build_url) falls back to the client
/// path, leaving rejection to the upstream.
pub fn resolve_upstream_model(body: &Bytes, upstream_model: &str) -> String {
    let resolved = if upstream_model.is_empty() {
        serde_json::from_slice::<serde_json::Value>(body)
            .ok()
            .and_then(|v| v.get("model").and_then(|m| m.as_str()).map(str::to_string))
            .unwrap_or_default()
    } else {
        upstream_model.to_string()
    };
    if is_safe_model_segment(&resolved) {
        resolved
    } else {
        String::new()
    }
}

/// Whether the model can be a single URL segment: only alphanumerics and `.` `-` `_`.
///
/// `.` is allowed because `..:verb` is still a single segment (traversal needs a
/// standalone `/..` segment), and dot-separated model names (`gemini-1.5-pro`) are
/// common.
fn is_safe_model_segment(s: &str) -> bool {
    !s.is_empty()
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_'))
}

/// Rewrite the request body's `model` to the upstream real name.
///
/// The routing unit maps a public alias (`gpt-4`) to the upstream real name
/// (`gpt-4-0613`); clients send the alias and upstreams only accept the real name,
/// so this one field must be rewritten before sending.
///
/// Returned as-is (zero-copy clone, no re-serialization) when:
/// - `upstream_model` is empty — there is no target real name;
/// - the body is not JSON or its top level is not an object — no reliable place to
///   rewrite;
/// - there is no `model` field — not a chat-style request;
/// - `model` already equals the real name — nothing to rewrite, and byte fidelity
///   must be preserved (passthrough semantics).
pub fn rewrite_upstream_model(body: &bytes::Bytes, upstream_model: &str) -> bytes::Bytes {
    if upstream_model.is_empty() {
        return body.clone();
    }
    let Ok(mut json) = serde_json::from_slice::<serde_json::Value>(body) else {
        return body.clone();
    };
    let Some(obj) = json.as_object_mut() else {
        return body.clone();
    };
    // Already the real name (including the common "public name == real name" case) →
    // leave the bytes alone to keep passthrough fidelity.
    match obj.get("model").and_then(|v| v.as_str()) {
        Some(current) if current == upstream_model => return body.clone(),
        Some(_) => {}
        // No model field = not a chat-style request; do not insert one out of thin
        // air.
        None => return body.clone(),
    }
    obj.insert(
        "model".to_string(),
        serde_json::Value::String(upstream_model.to_string()),
    );
    match serde_json::to_vec(&json) {
        Ok(v) => bytes::Bytes::from(v),
        Err(_) => body.clone(),
    }
}
