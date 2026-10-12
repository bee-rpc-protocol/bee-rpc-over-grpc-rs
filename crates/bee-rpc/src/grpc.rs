//! A generic Bee-RPC call over tonic (`client.client_grpc`). Feature `grpc`.
//!
//! Each Bee-RPC method is `rpc M(stream buffer.Buffer) returns (stream buffer.Buffer)`.
//! No generated stubs: the call uses a prost codec for `Buffer` on any path.
//!
//! Rules:
//!
//! - The request direction sends the parts with [`crate::frame::serialize_part`]
//!   and waits on the [`crate::signal::Signal`] before each Buffer.
//! - The response direction goes through a [`crate::frame::Parser`] that shares
//!   that signal.
//! - With `block_skip`, the request direction stays open after the input, and sends
//!   the skip requests of the parse side until the response ends. Without it, the
//!   request direction ends after the input (the reference default).
//! - No blocking call on the async runtime.
//! - The request direction runs in a task of its own, so that the response can be
//!   read while the input is still being sent. The task ends with the call.

use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tonic::codegen::http::uri::PathAndQuery;

use crate::buffer::Buffer;
use crate::frame::{serialize_part, BlockStore, ParseOptions, Parser, Part};
use crate::signal::Signal;
use crate::skip::{SkipFilter, StreamControl};
use crate::Error;

/// How long the held-open request direction waits between two looks at the skip
/// queue (`client_grpc` uses 0.05 s).
const HOLD_POLL: Duration = Duration::from_millis(20);

/// Options of [`call`].
#[derive(Clone, Debug, Default)]
pub struct CallOptions {
    /// Parser options for the response.
    pub parse: ParseOptions,
    /// Hold the request direction open for skip requests (`client_grpc(block_skip=True)`).
    pub block_skip: bool,
    /// A deadline for the whole call.
    pub timeout: Option<std::time::Duration>,
}

/// Call the bidi method at `path` (for example `/bee.WireHarness/Exchange`) on
/// `channel`, send `parts`, and return all response parts.
pub async fn call(
    channel: tonic::transport::Channel,
    path: &str,
    parts: Vec<Part>,
    options: CallOptions,
) -> Result<Vec<Part>, Error> {
    run(channel, path, parts, options, None).await
}

/// [`call`], with a [`BlockStore`] for the response: blocks that the store holds
/// come from it, and with `block_skip` the peer is asked to skip them.
pub async fn call_with_store(
    channel: tonic::transport::Channel,
    path: &str,
    parts: Vec<Part>,
    options: CallOptions,
    store: Arc<dyn BlockStore>,
) -> Result<Vec<Part>, Error> {
    run(channel, path, parts, options, Some(store)).await
}

fn transport(e: impl std::fmt::Display) -> Error {
    Error::Transport(e.to_string())
}

async fn run(
    channel: tonic::transport::Channel,
    path: &str,
    parts: Vec<Part>,
    options: CallOptions,
    store: Option<Arc<dyn BlockStore>>,
) -> Result<Vec<Part>, Error> {
    let timeout = options.timeout;
    let work = exchange(channel, path, parts, options, store);
    match timeout {
        None => work.await,
        Some(limit) => tokio::time::timeout(limit, work)
            .await
            .map_err(|_| Error::Transport("deadline exceeded".to_string()))?,
    }
}

/// The request direction task. It stops when the call ends (dropped).
struct SendTask {
    done: Arc<AtomicBool>,
    handle: JoinHandle<()>,
}

impl Drop for SendTask {
    fn drop(&mut self) {
        self.done.store(true, Ordering::Release);
        self.handle.abort();
    }
}

async fn exchange(
    channel: tonic::transport::Channel,
    path: &str,
    parts: Vec<Part>,
    options: CallOptions,
    store: Option<Arc<dyn BlockStore>>,
) -> Result<Vec<Part>, Error> {
    let path = PathAndQuery::from_str(path).map_err(transport)?;
    let signal = Signal::new();
    let control = options
        .block_skip
        .then(|| StreamControl::new(options.parse.algorithm));

    let mut parser = Parser::new(options.parse).with_signal(signal.clone());
    if let Some(store) = store {
        parser = parser.with_store(store);
    }
    if let Some(control) = &control {
        parser = parser.with_control(control.clone());
    }

    let (tx, rx) = mpsc::channel::<Buffer>(1);
    let done = Arc::new(AtomicBool::new(false));
    let handle = tokio::spawn(send_side(parts, signal, control, tx, done.clone()));
    let _task = SendTask { done, handle };

    let mut request = tonic::Request::new(ReceiverStream::new(rx));
    if let Some(limit) = options.timeout {
        request.set_timeout(limit);
    }
    let mut client = tonic::client::Grpc::new(channel);
    client.ready().await.map_err(transport)?;
    let codec = tonic_prost::ProstCodec::<Buffer, Buffer>::default();
    let mut response = client
        .streaming(request, path, codec)
        .await
        .map_err(transport)?
        .into_inner();

    let mut out = Vec::new();
    while let Some(buffer) = response.message().await.map_err(transport)? {
        out.extend(parser.push(buffer)?);
    }
    out.extend(parser.finish()?);
    Ok(out)
}

/// The request direction (`serialize_to_buffer` with a signal and a control, and
/// the hold-open loop of `client_grpc`). It ends when `tx` is closed.
async fn send_side(
    parts: Vec<Part>,
    signal: Signal,
    control: Option<StreamControl>,
    tx: mpsc::Sender<Buffer>,
    done: Arc<AtomicBool>,
) {
    let mut filter = control.clone().map(SkipFilter::new);
    for part in &parts {
        for buffer in serialize_part(part) {
            // Our own skip requests leave before each Buffer.
            if !forward_skips(control.as_ref(), &tx).await {
                return;
            }
            signal.wait_open().await;
            let buffer = match filter.as_mut() {
                Some(filter) => match filter.push(buffer) {
                    Some(buffer) => buffer,
                    None => continue,
                },
                None => buffer,
            };
            if tx.send(buffer).await.is_err() {
                return;
            }
        }
    }
    // Hold the direction open: a skip request can only come while the response
    // is parsed. Without a control, the direction ends with the input.
    if control.is_some() {
        while forward_skips(control.as_ref(), &tx).await && !done.load(Ordering::Acquire) {
            tokio::time::sleep(HOLD_POLL).await;
        }
    }
}

/// Send the queued skip requests. False if the call is gone.
async fn forward_skips(control: Option<&StreamControl>, tx: &mpsc::Sender<Buffer>) -> bool {
    let Some(control) = control else { return true };
    for buffer in control.take_outbound() {
        if tx.send(buffer).await.is_err() {
            return false;
        }
    }
    true
}
