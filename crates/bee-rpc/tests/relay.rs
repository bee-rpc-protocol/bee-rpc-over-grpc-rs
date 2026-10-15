//! Calls that move Buffers that are already framed (`call_buffers`, `call_raw`), for a
//! client that relays the Buffers of one call or of a .bee file to another call.
//!
//! An in-process gRPC server parses what it gets with the parser of this crate, so a
//! block that is relayed must still hash to its id. The cases come from the parse
//! vectors of the Python reference. With `BEE_RPC_PYTHON`, `call_raw` also reads the
//! streams of the Python peer.
#![cfg(feature = "grpc")]

mod common;

use std::convert::Infallible;
use std::path::PathBuf;
use std::pin::Pin;
use std::process::Stdio;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use bee_rpc::buffer::Buffer;
use bee_rpc::frame::{serialize_part, ParseOptions, Parser, Part};
use bee_rpc::grpc::{call_buffers, call_raw, cancel_markers, BufferStream, CallOptions};
use bee_rpc::Error;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::mpsc;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tokio_stream::Stream;
use tonic::codegen::{http, Body, BoxFuture, Service, StdError};
use tonic::{Status, Streaming};

const PATH: &str = "/bee.Relay/Call";

type Reply = Pin<Box<dyn Stream<Item = Result<Buffer, Status>> + Send>>;
type Handler = Arc<dyn Fn(Streaming<Buffer>) -> Reply + Send + Sync>;

/// A gRPC server with one bidi method, `PATH`, that runs `handler`.
#[derive(Clone)]
struct Server(Handler);

impl tonic::server::NamedService for Server {
    const NAME: &'static str = "bee.Relay";
}

struct Method(Handler);

impl tonic::server::StreamingService<Buffer> for Method {
    type Response = Buffer;
    type ResponseStream = Reply;
    type Future = BoxFuture<tonic::Response<Reply>, Status>;

    fn call(&mut self, request: tonic::Request<Streaming<Buffer>>) -> Self::Future {
        let reply = (self.0)(request.into_inner());
        Box::pin(async move { Ok(tonic::Response::new(reply)) })
    }
}

impl<B> Service<http::Request<B>> for Server
where
    B: Body + Send + 'static,
    B::Error: Into<StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        let handler = self.0.clone();
        Box::pin(async move {
            let codec = tonic_prost::ProstCodec::<Buffer, Buffer>::default();
            let mut grpc = tonic::server::Grpc::new(codec);
            Ok(grpc.streaming(Method(handler), request).await)
        })
    }
}

/// Serve `handler` on the loopback and give a channel to it.
async fn serve(handler: Handler) -> tonic::transport::Channel {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(Server(handler))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap()
}

/// A reply of the Buffers that `make` sends on its channel.
fn reply(make: impl FnOnce(mpsc::Sender<Result<Buffer, Status>>) + Send + 'static) -> Reply {
    let (tx, rx) = mpsc::channel(64);
    make(tx);
    Box::pin(ReceiverStream::new(rx))
}

/// One index 0 part as Buffers.
async fn send_part(tx: &mpsc::Sender<Result<Buffer, Status>>, data: Vec<u8>) {
    for buffer in serialize_part(&Part::new(0, data)) {
        let _ = tx.send(Ok(buffer)).await;
    }
}

fn opts() -> CallOptions {
    CallOptions {
        timeout: Some(Duration::from_secs(60)),
        ..Default::default()
    }
}

fn input(buffers: Vec<Buffer>) -> BufferStream {
    Box::pin(tokio_stream::iter(buffers.into_iter().map(Ok)))
}

/// The summaries (len, sha256) of the parts that a server parsed, as JSON.
fn summaries(parts: &[Part]) -> Vec<u8> {
    let list: Vec<_> = parts
        .iter()
        .map(|p| serde_json::json!({ "len": p.data.len(), "sha256": common::sha256_hex(&p.data) }))
        .collect();
    serde_json::to_vec(&list).unwrap()
}

#[tokio::test]
async fn framed_buffers_with_blocks_arrive_as_the_reference_parses_them() {
    let m = common::manifest();
    for case in common::cases(&m, "parse") {
        let name = case["stream"].as_str().unwrap().to_string();
        let indices = common::indices(case);
        let handler: Handler = Arc::new(move |mut request| {
            let indices = indices.clone();
            reply(move |tx| {
                tokio::spawn(async move {
                    let mut parser = Parser::new(ParseOptions::accepting(indices));
                    let mut parts = Vec::new();
                    while let Ok(Some(buffer)) = request.message().await {
                        match parser.push(buffer) {
                            Ok(p) => parts.extend(p),
                            Err(e) => {
                                let _ = tx.send(Err(Status::invalid_argument(e.to_string()))).await;
                                return;
                            }
                        }
                    }
                    match parser.finish() {
                        Ok(p) => parts.extend(p),
                        Err(e) => {
                            let _ = tx.send(Err(Status::invalid_argument(e.to_string()))).await;
                            return;
                        }
                    }
                    send_part(&tx, summaries(&parts)).await;
                });
            })
        });
        let channel = serve(handler).await;
        let got = call_buffers(channel, PATH, input(common::stream(&name)), opts())
            .await
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        let got: serde_json::Value = serde_json::from_slice(&got[0].data).unwrap();
        let got: Vec<_> = got
            .as_array()
            .unwrap()
            .iter()
            .map(common::summary_of)
            .collect();
        let want: Vec<_> = case["python_parse"]["parts"]
            .as_array()
            .unwrap()
            .iter()
            .map(common::summary_of)
            .collect();
        assert_eq!(got, want, "{name}");
    }
}

#[tokio::test]
async fn the_request_direction_waits_while_the_peer_closed_the_signal() {
    let total = 40;
    let received = Arc::new(AtomicUsize::new(0));
    let during_pause = Arc::new(AtomicUsize::new(usize::MAX));
    let (r, d) = (received.clone(), during_pause.clone());
    let handler: Handler = Arc::new(move |mut request| {
        let (received, during_pause) = (r.clone(), d.clone());
        reply(move |tx| {
            tokio::spawn(async move {
                // Close the signal before reading anything.
                let _ = tx
                    .send(Ok(Buffer {
                        signal: Some(true),
                        ..Default::default()
                    }))
                    .await;
                let counter = received.clone();
                let reader = tokio::spawn(async move {
                    while let Ok(Some(_)) = request.message().await {
                        counter.fetch_add(1, Ordering::SeqCst);
                    }
                });
                tokio::time::sleep(Duration::from_millis(500)).await;
                during_pause.store(received.load(Ordering::SeqCst), Ordering::SeqCst);
                // Open it again, and wait for the rest.
                let _ = tx
                    .send(Ok(Buffer {
                        signal: Some(true),
                        ..Default::default()
                    }))
                    .await;
                let _ = reader.await;
                send_part(
                    &tx,
                    received.load(Ordering::SeqCst).to_string().into_bytes(),
                )
                .await;
            });
        })
    });
    let channel = serve(handler).await;
    let buffers: Vec<Buffer> = (0..total)
        .flat_map(|i| serialize_part(&Part::new(1, vec![i as u8; 8])).collect::<Vec<_>>())
        .collect();
    assert_eq!(buffers.len(), total);
    // A slow input, as a large file is: one Buffer every 25 ms, 1 s in all. Without the
    // signal, about 20 would leave during the pause of 500 ms.
    let (tx, rx) = mpsc::channel(1);
    tokio::spawn(async move {
        for buffer in buffers {
            tokio::time::sleep(Duration::from_millis(25)).await;
            if tx.send(Ok(buffer)).await.is_err() {
                return;
            }
        }
    });
    let got = call_buffers(channel, PATH, Box::pin(ReceiverStream::new(rx)), opts())
        .await
        .unwrap();
    assert_eq!(got[0].data, total.to_string().into_bytes());
    // A Buffer or two can leave before the client reads the signal; not the rest.
    let paused = during_pause.load(Ordering::SeqCst);
    assert!(
        paused <= 2,
        "{paused} Buffers arrived while the signal was closed"
    );
}

#[tokio::test]
async fn an_error_of_the_input_ends_with_markers_that_the_receiver_refuses() {
    let outcome = Arc::new(Mutex::new(None::<String>));
    let o = outcome.clone();
    let handler: Handler = Arc::new(move |mut request| {
        let outcome = o.clone();
        reply(move |_tx| {
            tokio::spawn(async move {
                // A receiver that accepts the indices of its method, as nodo does.
                let mut parser = Parser::new(ParseOptions::accepting([1]));
                let mut parts = 0;
                let end = loop {
                    match request.message().await {
                        Ok(Some(buffer)) => match parser.push(buffer) {
                            Ok(p) => parts += p.len(),
                            Err(e) => break format!("refused after {parts} parts: {e}"),
                        },
                        Ok(None) => match parser.finish() {
                            Ok(p) => break format!("complete with {} parts", parts + p.len()),
                            Err(e) => break format!("refused at the end: {e}"),
                        },
                        Err(e) => break format!("transport error: {e}"),
                    }
                };
                *outcome.lock().unwrap() = Some(end);
            });
        })
    });
    let channel = serve(handler).await;
    // Three parts, and the start of a fourth that the input cannot finish: without the
    // marker, the reference would take the cut fourth part as a message.
    let mut items: Vec<Result<Buffer, Error>> = (0..3)
        .flat_map(|i| serialize_part(&Part::new(1, vec![i; 8])).collect::<Vec<_>>())
        .map(Ok)
        .collect();
    let big = Part::new(1, vec![7u8; bee_rpc::CHUNK_SIZE * 2]);
    items.extend(serialize_part(&big).take(2).map(Ok));
    items.push(Err(Error::Decode("the file is cut".into())));
    let error = call_buffers(channel, PATH, Box::pin(tokio_stream::iter(items)), opts())
        .await
        .unwrap_err();
    assert!(
        matches!(error, Error::Decode(ref t) if t == "the file is cut"),
        "{error}"
    );
    let mut seen = None;
    for _ in 0..200 {
        seen = outcome.lock().unwrap().clone();
        if seen.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let seen = seen.expect("the receiver saw the end of the request");
    assert_eq!(
        seen,
        format!("refused after 3 parts: {}", Error::IntersectingBlocks)
    );
}

#[tokio::test]
async fn the_error_of_the_input_comes_first_when_the_receiver_answers_with_an_error() {
    // nodo answers a refused request with an error status at once.
    let handler: Handler = Arc::new(move |mut request| {
        reply(move |tx| {
            tokio::spawn(async move {
                let mut parser = Parser::new(ParseOptions::accepting([1]));
                while let Ok(Some(buffer)) = request.message().await {
                    if let Err(e) = parser.push(buffer) {
                        let _ = tx.send(Err(Status::unknown(e.to_string()))).await;
                        return;
                    }
                }
            });
        })
    });
    for _ in 0..20 {
        let channel = serve(handler.clone()).await;
        let mut items: Vec<Result<Buffer, Error>> = serialize_part(&Part::new(1, vec![1u8; 8]))
            .map(Ok)
            .collect();
        items.push(Err(Error::Decode("the file is cut".into())));
        let error = call_buffers(channel, PATH, Box::pin(tokio_stream::iter(items)), opts())
            .await
            .unwrap_err();
        assert!(
            matches!(error, Error::Decode(ref t) if t == "the file is cut"),
            "{error}"
        );
    }
}

/// The parts that a receiver gets from `before`, then the cancel markers, then a
/// clean end: it must refuse, never give a cut message.
fn after_markers(before: Vec<Buffer>) -> Result<Vec<Part>, Error> {
    let mut parser = Parser::new(ParseOptions::accepting([1]));
    let mut parts = Vec::new();
    for buffer in before.into_iter().chain(cancel_markers()) {
        parts.extend(parser.push(buffer)?);
    }
    parts.extend(parser.finish()?);
    Ok(parts)
}

#[test]
fn the_cancel_markers_are_refused_at_any_point_of_a_stream() {
    // No message open.
    let whole = serialize_part(&Part::new(1, b"abc".to_vec())).collect::<Vec<_>>();
    assert!(matches!(
        after_markers(whole),
        Err(Error::IntersectingBlocks)
    ));
    // A message open.
    let big = Part::new(1, vec![1u8; bee_rpc::CHUNK_SIZE * 2]);
    let open = serialize_part(&big).take(2).collect::<Vec<_>>();
    assert!(matches!(
        after_markers(open),
        Err(Error::IntersectingBlocks)
    ));
    // Inside a block of the stream, cut before its end.
    let mut inside = common::stream("parse_one_block.bee");
    let end = inside.iter().rposition(|b| b.block.is_some()).unwrap();
    inside.truncate(end);
    assert!(matches!(
        after_markers(inside),
        Err(Error::IntersectingBlocks)
    ));
    // Nothing before.
    assert!(matches!(
        after_markers(Vec::new()),
        Err(Error::IntersectingBlocks)
    ));
}

#[tokio::test]
async fn a_raw_response_gives_each_buffer_unchanged_with_its_blocks() {
    for name in [
        "parse_big_block.bee",
        "parse_nested_depth2.bee",
        "parse_one_block.bee",
    ] {
        let stream = common::stream(name);
        let sent = stream.clone();
        let handler: Handler = Arc::new(move |_request| {
            let sent = sent.clone();
            reply(move |tx| {
                tokio::spawn(async move {
                    for buffer in sent {
                        let _ = tx.send(Ok(buffer)).await;
                    }
                });
            })
        });
        let channel = serve(handler).await;
        let mut response = call_raw(channel, PATH, vec![Part::new(1, b"go".to_vec())], opts())
            .await
            .unwrap();
        let mut got = Vec::new();
        while let Some(buffer) = response.next().await.unwrap() {
            got.push(buffer);
        }
        assert_eq!(common::encode(&got), common::encode(&stream), "{name}");
    }
}

struct Peer {
    _child: Child,
    channel: tonic::transport::Channel,
}

/// The Python peer of `tools/interop/py_peer.py`, as in `interop.rs`.
async fn python_peer() -> Option<Peer> {
    let python = match std::env::var("BEE_RPC_PYTHON") {
        Ok(p) => p,
        Err(_) if std::env::var_os("CI").is_some() => panic!("BEE_RPC_PYTHON is not set in CI"),
        Err(_) => {
            eprintln!("relay: BEE_RPC_PYTHON is not set; test not run");
            return None;
        }
    };
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut child = Command::new(python)
        .arg("-I")
        .arg(root.join("tools/interop/py_peer.py"))
        .arg("--vectors")
        .arg(common::vectors_dir())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("start the Python peer");
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    let line = tokio::time::timeout(Duration::from_secs(30), lines.next_line())
        .await
        .expect("the Python peer did not start")
        .unwrap()
        .expect("the Python peer closed stdout");
    let port: u16 = line
        .strip_prefix("PORT ")
        .expect("PORT line")
        .parse()
        .unwrap();
    let channel = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}"))
        .unwrap()
        .connect()
        .await
        .expect("connect to the Python peer");
    Some(Peer {
        _child: child,
        channel,
    })
}

#[tokio::test]
async fn a_raw_response_from_python_is_the_bee_file_that_it_reads() {
    let Some(p) = python_peer().await else { return };
    let m = common::manifest();
    for case in common::cases(&m, "parse") {
        let name = case["stream"].as_str().unwrap();
        let mut response = call_raw(
            p.channel.clone(),
            "/bee.WireHarness/Stream",
            vec![Part::new(1, name.as_bytes().to_vec())],
            opts(),
        )
        .await
        .unwrap_or_else(|e| panic!("{name}: {e}"));
        let mut got = Vec::new();
        while let Some(buffer) = response.next().await.unwrap() {
            got.push(buffer);
        }
        assert_eq!(
            common::encode(&got),
            common::encode(&common::stream(name)),
            "{name}"
        );
    }
}
