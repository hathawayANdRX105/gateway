//! Streaming bidirectional pipeline — upstream SSE → scan chain → client.

use bytes::Bytes;
use futures_util::{FutureExt, Stream, StreamExt};
use std::pin::Pin;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub struct PipedChunk {
    pub passthrough: Bytes,
    pub events: Vec<gateway_protocol_bridge::sse::SseEvent>,
}

pub struct SseContext {
    pub scanner: gateway_protocol_bridge::sse::SseScanner,
    pub token_scanner: gateway_metering::scanner::StreamScanner,
    /// Streaming metadata (used for settlement).
    pub user_key: String,
    pub token_key: String,
    pub channel_key: String,
    pub public_model: String,
    pub upstream_model: String,
    /// Request group name (passed to `PriceTable::lookup`), empty string by default.
    pub group: String,
    /// Group rate multiplier (GroupRecord.rate_multiplier); the library default is 1.0 and
    /// the real value is injected by the apps layer.
    pub group_ratio: f64,
    /// Pricing table reference (optional; None = no billing).
    pub price_table: Option<std::sync::Arc<dyn gateway_metering::pricing::PriceTable>>,
}

impl SseContext {
    pub fn new() -> Self {
        Self {
            scanner: gateway_protocol_bridge::sse::SseScanner::default(),
            token_scanner: gateway_metering::scanner::StreamScanner::new(),
            user_key: String::new(),
            token_key: String::new(),
            channel_key: String::new(),
            public_model: String::new(),
            upstream_model: String::new(),
            group: String::new(),
            group_ratio: 1.0,
            price_table: None,
        }
    }
}

impl Default for SseContext {
    fn default() -> Self {
        Self::new()
    }
}

pub fn pipe_chunk(ctx: &mut SseContext, chunk: &Bytes) -> PipedChunk {
    let (passthrough, events) = ctx.scanner.push(chunk);
    ctx.token_scanner.push(chunk);
    PipedChunk {
        passthrough,
        events,
    }
}

/// Terminates the scanners and returns SseEnd + TokenCounts; when `price_table` is `Some`
/// this **actually** settles and returns the product to the caller (`None` = no billing,
/// matching the behavior before billing existed).
///
/// The caller injects the terminal state through `status_code` / `error`: pass `200, None`
/// when the stream finishes normally; on a mid-stream failure (upstream disconnect) pass
/// `500, Some(summary)` and settle with the counts accumulated so far, so the bill for
/// tokens already produced is not leaked.
pub fn finish(
    ctx: SseContext,
    status_code: u16,
    error: Option<&str>,
) -> (
    gateway_protocol_bridge::sse::SseEnd,
    gateway_metering::scanner::TokenCounts,
    Option<gateway_core::records::UsageEvent>,
) {
    let end = ctx.scanner.finish();
    // Finish reason captured from stream frames (the last frame's value when present;
    // upstream absent = None, never fabricated).
    let (counts, finish_reason) = ctx.token_scanner.finish(0);

    let event = ctx.price_table.as_ref().map(|pt| {
        // The library layer does no prehold (prehold belongs to the admission track);
        // amount=0 means "post-hoc settlement, not a prehold": settle_event consumes only the
        // hold's attribution keys user_key/token_key and computes cost from counts × price;
        // the amount field plays no part in this settlement.
        let hold = gateway_metering::ledger::Hold {
            id: 0,
            amount: 0,
            user_key: ctx.user_key.clone(),
            token_key: ctx.token_key.clone(),
        };
        gateway_metering::settle_event(
            counts,
            // Reaching finish always means the streaming path (SSE scan-chain settlement).
            true,
            &ctx.group,
            ctx.group_ratio,
            &hold,
            pt.as_ref(),
            &ctx.channel_key,
            "",
            &ctx.public_model,
            &ctx.upstream_model,
            0,
            0,
            status_code,
            error,
            finish_reason.as_deref(),
        )
    });

    (end, counts, event)
}

#[derive(Debug)]
pub struct AbortGuard {
    cancel: CancellationToken,
    _stop_tx: Option<oneshot::Sender<()>>,
}

impl AbortGuard {
    pub fn new() -> (Self, oneshot::Receiver<()>) {
        let (tx, rx) = oneshot::channel();
        let guard = Self {
            cancel: CancellationToken::new(),
            _stop_tx: Some(tx),
        };
        (guard, rx)
    }

    pub fn cancel(&self) {
        self.cancel.cancel();
    }

    pub async fn cancelled(&self) {
        self.cancel.cancelled().await;
    }
}

impl Default for AbortGuard {
    fn default() -> Self {
        Self {
            cancel: CancellationToken::new(),
            _stop_tx: None,
        }
    }
}

impl Drop for AbortGuard {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

pub fn abortable_stream<S>(inner: S, stop: oneshot::Receiver<()>) -> AbortableStream<S>
where
    S: Stream<Item = Result<Bytes, std::io::Error>> + Unpin,
{
    AbortableStream { inner, stop }
}

pub struct AbortableStream<S> {
    inner: S,
    stop: oneshot::Receiver<()>,
}

impl<S> Stream for AbortableStream<S>
where
    S: Stream<Item = Result<Bytes, std::io::Error>> + Unpin,
{
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.inner.poll_next_unpin(cx) {
            std::task::Poll::Ready(item) => std::task::Poll::Ready(item),
            std::task::Poll::Pending => match self.stop.poll_unpin(cx) {
                std::task::Poll::Ready(_) => std::task::Poll::Ready(None),
                std::task::Poll::Pending => std::task::Poll::Pending,
            },
        }
    }
}

/// Whether the request body asks for a streaming response — reads the top-level `stream`
/// boolean field.
///
/// OpenAI / Claude / Gemini (OpenAI-compatible layer) all express streaming intent with this
/// field. Non-JSON or a missing field → false (treated as non-streaming: collect the whole
/// body, then reply).
///
/// The URL path cannot decide this: `/v1/chat/completions` with `"stream": true` is
/// streaming, while a "stream" substring in the path does not mean the client wants SSE.
pub(crate) fn body_wants_stream(body: &Bytes) -> bool {
    #[derive(serde::Deserialize)]
    struct StreamFlag {
        #[serde(default)]
        stream: bool,
    }
    serde_json::from_slice::<StreamFlag>(body)
        .map(|f| f.stream)
        .unwrap_or(false)
}
