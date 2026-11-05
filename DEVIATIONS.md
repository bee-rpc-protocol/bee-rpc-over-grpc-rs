# Deviations from the reference

The Python library (bee-rpc-over-grpc-py v0.0.3) is the reference. This crate does
the same on the wire, with the differences below. Each one is on purpose and has a
test. Do not add a deviation without approval of a maintainer.

None of them changes the bytes that a sender writes. A Python peer and a Rust peer
interoperate with no change on either side.

| Id | Topic | Reference | This crate | Test |
|---|---|---|---|---|
| D1 | A message with blocks, parsed in memory | Message mode ends the message at the first block end marker. The rest comes out as a second message. Only disk mode rebuilds the message | The parser returns the whole message (the expansion), the same bytes as the reference disk mode | `parse.rs`: `rebuilds_messages_with_blocks` |
| D2 | A block marker with no hash type that this node can read | The parser does not see a block. The message ends there, and the parts after it come out as other messages | Error `UnresolvableBlock` | `parse_errors.rs`: case `block_without_hash_type` |
| D3 | Block hash check in memory | Message mode does not check that a block content hashes to its id. Disk mode checks it | The parser always checks it. Error `BlockIdMismatch` | `parse.rs`: `corrupt_block_content_is_refused`; `parse_errors.rs`: case `block_id_mismatch` |
| D4 | Empty messages | The parser drops an empty message, unless the expected type is `Empty` | The parser returns it as a `Part` with no data. The caller decides | `parse.rs`: `parses_the_serialize_vectors_back_to_their_inputs`; `parse_errors.rs`: `empty_message_is_emitted` |
| D5 | Skip requests without a `StreamControl` | Without a control, a skip-only Buffer is read as an empty chunk, and between messages it can start an empty message | The parser always removes the `skip` field and drops a skip-only Buffer | `parse_errors.rs`: `skip_buffers_are_never_message_bytes` |

## Expectations in the vectors

`tools/vectors/generate.py` takes the expectation of each case from the reference.
For streams with blocks it uses the reference disk mode (D1). For a case where this
crate is stricter, the manifest has `"rust_expects": "error"` and the note starts
with `DEVIATION`.
