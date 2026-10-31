//! Rust port of [bee-rpc-over-grpc-py](https://github.com/bee-rpc-protocol/bee-rpc-over-grpc-py).
//!
//! Bee-RPC sends a sequence of messages over one gRPC bidi stream of
//! `buffer.Buffer`. Each message has an index (which type it is) and is sent as
//! one Buffer or as a head, chunks and a separator. A message can contain blocks:
//! content-addressed parts, framed by a start marker and an end marker with the
//! same hash, that a receiver can skip when it already holds them.
//!
//! The reference is the Python library at v0.0.2. The test vectors hold the
//! expectations, generated from it by `tools/vectors/generate.py`.
//! `DEVIATIONS.md` lists the places where this crate is stricter on purpose.
//!
//! The public API in this file and its modules is the contract of the port. The
//! tests in `tests/` use it. Do not change a signature without approval
//! (see `AGENT_GUIDE.md`).

// Input from a peer must never make the crate panic. CI runs clippy with
// -D warnings, so these lints are errors outside the tests.
#![forbid(unsafe_code)]
#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)
)]
#![deny(missing_docs)]

pub mod bee_file;
pub mod error;
pub mod frame;
pub mod hash;
pub mod pointer;
pub mod signal;
pub mod skip;
pub mod store;

#[cfg(feature = "grpc")]
pub mod grpc;

#[allow(clippy::all, missing_docs)]
pub mod buffer {
    include!("gen/buffer.rs");
}

pub use error::Error;
pub use prost::Message;

/// The largest chunk the sender puts in one Buffer: 1 MiB (`utils.CHUNK_SIZE`).
pub const CHUNK_SIZE: usize = 1024 * 1024;

/// The deepest nesting of blocks a receiver accepts (`client.MAX_BLOCK_NESTING`).
pub const MAX_BLOCK_NESTING: usize = 32;
