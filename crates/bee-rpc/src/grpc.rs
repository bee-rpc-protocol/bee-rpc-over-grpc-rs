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
//!
//! Two more calls move Buffers that are already framed, for a relay (a client that
//! takes the Buffers of one call or of a .bee file and gives them to another call):
//!
//! - [`call_buffers`] sends a stream of Buffers as they are: block markers, chunks
//!   and heads are not changed. The request direction waits on the signal before
//!   each Buffer, and with `block_skip` it drops the blocks that the peer asks to
//!   skip, the same as [`call`]. If the input stream gives an error, the request
//!   direction sends the cancel markers, [`cancel_markers`], and then ends, and
//!   the call returns that error. A peer must not take a cut input for a complete
//!   one: at a clean end the reference yields the message that the end cuts, as it
//!   is, and a separator or a new head does not help, because the parser closes the
//!   open message with a separator and ignores a head while a message is open. The
//!   markers are blocks that intersect: block A opens, block B opens, block A
//!   closes. The reference refuses intersecting blocks in disk mode, and this crate
//!   always, at any point of a stream: no message open, a message open, or inside a
//!   block. In message mode the reference does not check blocks, so a cut message
//!   that it parses in message mode can still come out cut.
//! - [`call_raw`] sends parts and gives the Buffers of the response as they are, one
//!   at a time, without the parser: a block is not rebuilt in memory, and its hash
//!   is not checked (the next receiver checks it). A Buffer with `signal: true`
//!   toggles the signal of the request direction, the same as in the parser.

use std::pin::Pin;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use tokio::sync::{mpsc, Notify};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tonic::codegen::http::uri::PathAndQuery;

use crate::buffer::Buffer;
use crate::frame::{serialize_part, BlockStore, ParseOptions, Parser, Part};
use crate::hash::HashAlgorithm;
use crate::pointer::block_pointer;
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

/// A request that is already framed: each item is one Buffer as it goes on the wire.
pub type BufferStream = Pin<Box<dyn Stream<Item = Result<Buffer, Error>> + Send>>;

/// [`call`] for a request that is already framed (see the module docs): send
/// `input` as it is, and return all response parts.
pub async fn call_buffers(
    channel: tonic::transport::Channel,
    path: &str,
    input: BufferStream,
    options: CallOptions,
) -> Result<Vec<Part>, Error> {
    let timeout = options.timeout;
    let work = exchange(channel, path, Input::Buffers(input), options, None);
    match timeout {
        None => work.await,
        Some(limit) => tokio::time::timeout(limit, work)
            .await
            .map_err(|_| Error::Transport("deadline exceeded".to_string()))?,
    }
}

/// The response of [`call_raw`]: its Buffers, unchanged.
pub struct RawResponse {
    response: tonic::Streaming<Buffer>,
    signal: Signal,
    _task: SendTask,
}

impl std::fmt::Debug for RawResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RawResponse").finish_non_exhaustive()
    }
}

impl RawResponse {
    /// The next Buffer of the response, or `None` at its end. A Buffer with
    /// `signal: true` is given too, after it toggled the signal of the request
    /// direction.
    pub async fn next(&mut self) -> Result<Option<Buffer>, Error> {
        let buffer = self.response.message().await.map_err(transport)?;
        if buffer.as_ref().is_some_and(|b| b.signal == Some(true)) {
            self.signal.change();
        }
        Ok(buffer)
    }
}

/// Call the bidi method at `path`, send `parts`, and give the response Buffers as
/// they are (see the module docs). With `options.timeout`, the deadline is set on
/// the request; the caller limits the time to read the response. `options.parse`
/// and `options.block_skip` are not used: there is no parser.
pub async fn call_raw(
    channel: tonic::transport::Channel,
    path: &str,
    parts: Vec<Part>,
    options: CallOptions,
) -> Result<RawResponse, Error> {
    let path = PathAndQuery::from_str(path).map_err(transport)?;
    let signal = Signal::new();
    let (tx, rx) = mpsc::channel::<Buffer>(1);
    let done = Arc::new(AtomicBool::new(false));
    let failed = Arc::new(InputFailure::default());
    let handle = tokio::spawn(send_side(
        Input::Parts(parts),
        signal.clone(),
        None,
        tx,
        done.clone(),
        failed.clone(),
    ));
    let task = SendTask { done, handle };

    let mut request = tonic::Request::new(ReceiverStream::new(rx));
    if let Some(limit) = options.timeout {
        request.set_timeout(limit);
    }
    let mut client = tonic::client::Grpc::new(channel);
    client.ready().await.map_err(transport)?;
    let codec = tonic_prost::ProstCodec::<Buffer, Buffer>::default();
    let response = client
        .streaming(request, path, codec)
        .await
        .map_err(transport)?
        .into_inner();
    Ok(RawResponse {
        response,
        signal,
        _task: task,
    })
}

/// The Buffers that [`call_buffers`] sends when its input fails, before it ends the
/// request direction (see the module docs): block A opens, block B opens, block A
/// closes. The ids are SHA3-256 hashes of fixed texts; no content follows them.
pub fn cancel_markers() -> Vec<Buffer> {
    let id = |text: &[u8]| HashAlgorithm::Sha3_256.digest(text);
    let marker = |block_id: &[u8]| Buffer {
        block: Some(block_pointer(block_id, HashAlgorithm::Sha3_256, false)),
        ..Default::default()
    };
    let a = id(b"bee-rpc: the input of this call failed (A)");
    let b = id(b"bee-rpc: the input of this call failed (B)");
    vec![marker(&a), marker(&b), marker(&a)]
}

/// How long a failed call waits for its cancel markers to leave before it returns.
const MARKER_WAIT: Duration = Duration::from_secs(5);

/// What the request direction sends.
enum Input {
    /// Parts, framed here.
    Parts(Vec<Part>),
    /// Buffers that are already framed.
    Buffers(BufferStream),
}

/// The error of an input stream, for the response side, which cancels the call.
#[derive(Default)]
struct InputFailure {
    error: Mutex<Option<Error>>,
    notify: Notify,
    /// True when the cancel markers left (or could not leave: the call is gone).
    markers_sent: AtomicBool,
    markers: Notify,
}

impl InputFailure {
    fn set(&self, error: Error) {
        *self.error.lock().unwrap_or_else(PoisonError::into_inner) = Some(error);
        self.notify.notify_one();
    }

    fn has_error(&self) -> bool {
        self.error
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some()
    }

    fn markers_done(&self) {
        self.markers_sent.store(true, Ordering::Release);
        self.markers.notify_waiters();
    }

    /// Wait until the cancel markers left, at most [`MARKER_WAIT`].
    async fn wait_markers(&self) {
        let wait = async {
            loop {
                let notified = self.markers.notified();
                if self.markers_sent.load(Ordering::Acquire) {
                    return;
                }
                notified.await;
            }
        };
        let _ = tokio::time::timeout(MARKER_WAIT, wait).await;
    }

    fn take(&self) -> Option<Error> {
        self.error
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }
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
    let work = exchange(channel, path, Input::Parts(parts), options, store);
    match timeout {
        None => work.await,
        Some(limit) => tokio::time::timeout(limit, work)
            .await
            .map_err(|_| Error::Transport("deadline exceeded".to_string()))?,
    }
}

/// From this many bytes of chunk, or at a block marker, parsing hashes enough data to
/// stall the runtime thread. Such a Buffer is parsed on a blocking thread, so that
/// the request direction (skip requests, flow control) keeps running.
const OFFLOAD_BYTES: usize = 64 * 1024;

/// [`Parser::push`], off the async threads when the Buffer is heavy. Gives the
/// parser back.
async fn push(
    mut parser: Parser,
    buffer: Buffer,
) -> Result<(Parser, Result<Vec<Part>, Error>), Error> {
    let heavy = buffer.block.is_some()
        || buffer
            .chunk
            .as_ref()
            .is_some_and(|c| c.len() >= OFFLOAD_BYTES);
    if !heavy {
        let parsed = parser.push(buffer);
        return Ok((parser, parsed));
    }
    tokio::task::spawn_blocking(move || {
        let parsed = parser.push(buffer);
        (parser, parsed)
    })
    .await
    .map_err(transport)
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
    input: Input,
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
    let failed = Arc::new(InputFailure::default());
    let handle = tokio::spawn(send_side(
        input,
        signal,
        control,
        tx,
        done.clone(),
        failed.clone(),
    ));
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
    loop {
        let next = tokio::select! {
            next = response.message() => next.map_err(transport)?,
            // The input failed: return its error once the markers left.
            _ = failed.notify.notified() => None,
        };
        if failed.has_error() {
            failed.wait_markers().await;
            if let Some(error) = failed.take() {
                return Err(error);
            }
        }
        let Some(buffer) = next else { break };
        let (back, parsed) = push(parser, buffer).await?;
        parser = back;
        out.extend(parsed?);
    }
    out.extend(parser.finish()?);
    Ok(out)
}

/// The request direction (`serialize_to_buffer` with a signal and a control, and
/// the hold-open loop of `client_grpc`). It ends when `tx` is closed.
async fn send_side(
    input: Input,
    signal: Signal,
    control: Option<StreamControl>,
    tx: mpsc::Sender<Buffer>,
    done: Arc<AtomicBool>,
    failed: Arc<InputFailure>,
) {
    let mut filter = control.clone().map(SkipFilter::new);
    match input {
        Input::Parts(parts) => {
            for part in &parts {
                for buffer in serialize_part(part) {
                    if !send_one(buffer, &signal, control.as_ref(), filter.as_mut(), &tx).await {
                        return;
                    }
                }
            }
        }
        Input::Buffers(mut stream) => {
            while let Some(item) = stream.next().await {
                match item {
                    Ok(buffer) => {
                        if !send_one(buffer, &signal, control.as_ref(), filter.as_mut(), &tx).await
                        {
                            return;
                        }
                    }
                    Err(error) => {
                        // The error first, so that a response that ends now still
                        // gives it; the response side waits for the markers before it
                        // returns, because the call stops this task when it returns.
                        failed.set(error);
                        for marker in cancel_markers() {
                            if tx.send(marker).await.is_err() {
                                break;
                            }
                        }
                        drop(tx);
                        failed.markers_done();
                        return;
                    }
                }
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

/// Send one Buffer of the input: first our own skip requests, then wait for the
/// signal, then drop it if the peer asked to skip its block. False if the call is
/// gone.
async fn send_one(
    buffer: Buffer,
    signal: &Signal,
    control: Option<&StreamControl>,
    filter: Option<&mut SkipFilter>,
    tx: &mpsc::Sender<Buffer>,
) -> bool {
    if !forward_skips(control, tx).await {
        return false;
    }
    signal.wait_open().await;
    let buffer = match filter {
        Some(filter) => match filter.push(buffer) {
            Some(buffer) => buffer,
            None => return true,
        },
        None => buffer,
    };
    tx.send(buffer).await.is_ok()
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
