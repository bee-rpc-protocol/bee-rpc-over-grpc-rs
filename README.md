# bee-rpc-over-grpc-rs

Rust port of [bee-rpc-over-grpc-py](https://github.com/bee-rpc-protocol/bee-rpc-over-grpc-py).
Bee-RPC moves buffers of any size over gRPC. It splits a message into blocks, and
each block has a hash, so a receiver can skip a block that it already holds.

Status: **the port is complete** (steps 1 to 7 of `PORTING.md`). The crate passes the
vectors from the Python reference and the interop tests against a Python peer. The gRPC
call is behind the feature `grpc`.

Version 0.2.0 adds two calls for a relay, a client that takes the Buffers of one call or
of a .bee file and gives them to another call, without the blocks in memory:
`grpc::call_buffers` sends Buffers that are already framed, as they are, with the flow
control signal and the skip requests of the peer; `grpc::call_raw` gives the Buffers of a
response as they are. If the input of `call_buffers` fails, the call sends cancel
markers (blocks that intersect) before it ends the request direction, so that a peer
refuses the input instead of taking a cut message for a complete one. See the docs of
`src/grpc.rs` and `tests/relay.rs`.

Version 0.3.0 lets `call_raw` skip blocks: with `block_skip`, the request direction stays
open, and `RawResponse::request_skip` asks the peer to drop the rest of a block. A client
that needs only the small parts of a message around a large block (the specification of a
Celaut service around its filesystem) reads them without the block.

| File | Content |
|---|---|
| `PORTING.md` | The steps of the port, in order |
| `AGENT_GUIDE.md` | Rules and safeguards for the agent that implements it |
| `PROMPT.md` | The prompt to give to that agent |
| `DEVIATIONS.md` | Where this crate is stricter than the reference, on purpose |
| `crates/bee-rpc/tests/vectors/` | Expectations generated from the Python reference |
| `tools/vectors/generate.py` | The generator of the vectors |
| `tools/interop/py_peer.py` | A Python peer for the interop tests |
| `SPEC.sha256` | Hashes of the specification files (CI checks them) |

## Why a Rust port

- The Celaut light client for Android ([celaut-project/chatui](https://github.com/celaut-project/chatui))
  needs Bee-RPC and has no Python.
- One crate for all Rust users, tested against the Python library with the same vectors.

## Compatibility rule

The Python library at v0.0.2 is the reference. A Rust peer and a Python peer must
talk to each other with no change on either side. `DEVIATIONS.md` lists where the
Rust parser refuses input that the reference accepts.

## Build and test

```sh
cargo test --all-features
cargo run -p gen-protos          # after a change of crates/bee-rpc/protos/
```

`src/gen/` is committed, so a normal build does not need `protoc`. CI runs only
the test files listed in `.github/required-tests.txt`; a step of the port adds its
tests there.

To regenerate the vectors (maintainers only), with a Python 3.11 environment from
`tools/vectors/requirements.txt`:

```sh
python -I tools/vectors/generate.py /tmp/new-vectors
diff -r /tmp/new-vectors crates/bee-rpc/tests/vectors
```

Then copy them, run `tools/spec-hash.sh --write`, and commit both.

## License

GPL-3.0, as the Python library.
