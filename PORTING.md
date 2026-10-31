# Port plan

Reference: `bee-rpc-over-grpc-py` v0.0.2 (`ab3c3624`), `src/bee_rpc/`. Do the steps
in this order. Each step is one PR. A step is done when its test files pass and are
in `.github/required-tests.txt`.

| Step | Module (`crates/bee-rpc/src/`) | Python source | Test files (`crates/bee-rpc/tests/`) |
|---|---|---|---|
| 1 | `buffer` | `buffer.proto` | Done |
| 2 | `hash`, `pointer` | `utils.py`: hash algorithms, hash types, `block_pointer`, `resolve_hash_types`, `inherit_hash_types`, `block_id_from_pointer` | `hash`, `pointer` |
| 3 | `bee_file`, `signal` | `client.write_to_file`, `reader.read_bee_file`, `utils.Signal` | `bee_file`, `signal` |
| 4 | `frame`: `serialize_part` | `client.serialize_to_buffer`, `send_message` | `serialize` |
| 5 | `skip` | `control.py`, `client.skip_requested_blocks` | `skip` |
| 6 | `frame`: `Parser` | `client.parse_from_buffer`, `parser_iterator`, `save_chunks_to_block`, `save_block_content` (hash check) | `parse`, `parse_errors` |
| 7 | `grpc` (feature `grpc`) | `client.client_grpc` | `interop` |

Each module has its rules in its doc comment. The tests check those rules.

## Not in this plan

Do not start these. They need new vectors and the approval of a maintainer.

- Writing a block store on disk: building multiblock directories, `_.json` and the wbp
  file (`block_driver.py`, `block_builder.py`, `block_splitter.py`). Reading one is done:
  `store` (version 0.4.0, `reader.py`).
- The send side of blocks: build a multiblock object from a message, and frame
  blocks to `block_depth` (`read_from_registry`, `build_multiblock`). (Version 0.2.0
  can send Buffers that are already framed, with their blocks, for a relay:
  `grpc::call_buffers`. It does not build blocks.)
- Partitions (`Head.partitions`, `get_submessage`, `put_submessage`).
- The TLS identity check of a Celaut node. It is Celaut-specific and belongs in
  `celaut-wire` in celaut-project/chatui.
