#!/usr/bin/env python3
"""Generate the test vectors of the Rust port from the Python reference.

The Python library (bee-rpc-over-grpc-py, tag v0.0.2) is the reference. Every
expectation in crates/bee-rpc/tests/vectors/ comes from this script, not from a hand-written
guess. Do not edit the vectors by hand. Change this script, run it again, and
commit both.

Usage (from the repository root):

    python -I tools/vectors/generate.py crates/bee-rpc/tests/vectors

Requirements: Python 3.11, grpcio==1.56.0, protobuf==7.36.2 and
bee-rpc-over-grpc-py at tag v0.0.2 (see tools/vectors/requirements.txt).

Output:

    crates/bee-rpc/tests/vectors/manifest.json   all cases and their expectations
    crates/bee-rpc/tests/vectors/*.bee           Buffer streams, in the .bee format
                                  (4-byte big-endian length + Buffer bytes)

The script is deterministic: two runs give the same bytes. CI runs it and
fails on a diff.
"""
import hashlib
import json
import os
import random
import shutil
import sys
import tempfile

from bee_rpc import block_builder, buffer_pb2, client
from bee_rpc.client import parse_from_buffer, serialize_to_buffer, skip_requested_blocks
from bee_rpc.reader import read_multiblock_directory
from bee_rpc.control import StreamControl, skip_buffer, skipped_block_id
from bee_rpc.utils import (
    CHUNK_SIZE, Dir, Enviroment, block_id_from_pointer, block_pointer,
    block_pointer_length, hash_type_of, inherit_hash_types, modify_env,
    resolve_hash_types, HashTypeError,
)

REFERENCE = 'bee-rpc-over-grpc-py v0.0.2'
OUT = os.path.abspath(sys.argv[1] if len(sys.argv) > 1 else 'crates/bee-rpc/tests/vectors')
HEX_LIMIT = 4096  # bytes; longer data is given as length + sha256 only

os.makedirs(OUT, exist_ok=True)
if os.listdir(OUT):
    sys.exit('generate.py: %s is not empty. Give an empty or new directory.' % OUT)

manifest = {
    'reference': REFERENCE,
    'chunk_size': CHUNK_SIZE,
    'max_block_nesting': client.MAX_BLOCK_NESTING,
    'hash_types': {},
    'pointers': [],
    'resolve': [],
    'serialize': [],
    'parse': [],
    'parse_errors': [],
    'skip': [],
    'strip_skips': [],
    'bee_file': [],
}


# -- helpers ------------------------------------------------------------------

def data_bytes(seed: int, n: int) -> bytes:
    return random.Random(seed).randbytes(n)


def describe(data: bytes) -> dict:
    d = {'len': len(data), 'sha256': hashlib.sha256(data).hexdigest()}
    if len(data) <= HEX_LIMIT:
        d['hex'] = data.hex()
    return d


def write_stream(name: str, buffers) -> str:
    """Write Buffers in the .bee format. Returns the file name."""
    path = os.path.join(OUT, name + '.bee')
    with open(path, 'wb') as f:
        for b in buffers:
            raw = b.SerializeToString()
            f.write(len(raw).to_bytes(4, 'big'))
            f.write(raw)
    return name + '.bee'


def fresh_env(root: str, block_depth: int = 1) -> str:
    """Point the library at empty cache and block directories under `root`."""
    blocks = tempfile.mkdtemp(prefix='blocks-', dir=root)
    cache = tempfile.mkdtemp(prefix='cache-', dir=root)
    modify_env(cache_dir=cache + os.sep, block_dir=blocks + os.sep, block_depth=block_depth)
    Enviroment.cache_dir = cache + os.sep  # modify_env appends 'grpcbigbuffer/'
    os.makedirs(Enviroment.cache_dir, exist_ok=True)
    return blocks


def expand(result) -> bytes:
    """The full bytes of one parser result, in either mode."""
    if isinstance(result, Dir):
        if os.path.isdir(result.dir):
            return b''.join(c for c in read_multiblock_directory(result.dir, ignore_blocks=True)
                            if isinstance(c, bytes))
        with open(result.dir, 'rb') as f:
            return f.read()
    if isinstance(result, buffer_pb2.Empty):
        return b''
    if isinstance(result, bytes):
        return result
    return result.SerializeToString()


def python_parse(buffers, indices: dict, message_mode: bool = True, skip_wbp: bool = False):
    """What the reference parser yields, as a list of byte strings, or an error.

    `message_mode=False` is the reference's disk mode. Use it for every stream
    with blocks: the message mode of v0.0.2 ends a message at the first block end
    marker (see DEVIATIONS.md, D1), so its output is not the expectation there.

    `skip_wbp=True` is for synthetic streams that are not a protobuf object: the
    wbp file is a storage artefact, not a wire one, and it cannot be built for them.
    """
    heads = []

    def tap(it):
        for b in it:
            if b.HasField('head'):
                heads.append(b.head.index)
            yield b

    out = []
    Enviroment.skip_wbp_generation = skip_wbp
    try:
        results = list(parse_from_buffer(
            request_iterator=tap(iter(buffers)),
            indices=dict(indices),
            partitions_message_mode=message_mode,
        ))
    except Exception as e:  # noqa: BLE001 - the error is the expectation
        return {'error': type(e).__name__, 'message': str(e)[:200]}
    finally:
        Enviroment.skip_wbp_generation = False
    out = [expand(r) for r in results]
    return {'mode': 'message' if message_mode else 'disk', 'parts': [describe(o) for o in out]}


# -- 1. hash types ------------------------------------------------------------

for name, factory in [
    ('sha3_256', hashlib.sha3_256),
    ('sha3_512', hashlib.sha3_512),
    ('sha256', hashlib.sha256),
    ('sha512', hashlib.sha512),
    ('blake2b_256', lambda: hashlib.blake2b(digest_size=32)),
    ('blake2b_512', hashlib.blake2b),
]:
    h = factory()
    h.update(b'abc')
    manifest['hash_types'][name] = {
        'hash_type': hash_type_of(factory).hex(),
        'digest_of_abc': h.hexdigest(),
    }
manifest['default_hash_type'] = Enviroment.hash_type.hex()

# -- 2. block pointers --------------------------------------------------------

SHA3 = Enviroment.hash_type
SHA2 = hash_type_of(hashlib.sha256)
for block_id in [hashlib.sha3_256(b'x').digest(), hashlib.sha3_256(b'y' * 100).digest()]:
    for omit in (False, True):
        manifest['pointers'].append({
            'block_id': block_id.hex(),
            'omit_types': omit,
            'pointer': block_pointer(block_id, omit_types=omit).SerializeToString().hex(),
            'length': block_pointer_length(block_id, omit_types=omit),
        })


def resolve_case(name, hashes, inherited):
    block = buffer_pb2.Buffer.Block(hashes=[
        buffer_pb2.Buffer.Block.Hash(type=t, value=v) for t, v in hashes])
    case = {
        'name': name,
        'pointer': block.SerializeToString().hex(),
        'inherited': [t.hex() for t in inherited] if inherited is not None else None,
    }
    try:
        resolved = resolve_hash_types(block, inherited)
        case['resolved'] = [t.hex() for t in resolved]
        case['inherit_next'] = [t.hex() for t in inherit_hash_types(resolved, inherited)]
    except HashTypeError:
        case['resolved'] = None
    bid = block_id_from_pointer(block, inherited, hexadecimal=False)
    case['block_id'] = bid.hex() if bid is not None else None
    manifest['resolve'].append(case)


V1, V2 = b'\x01' * 32, b'\x02' * 32
resolve_case('all_typed', [(SHA3, V1)], None)
resolve_case('second_type_is_registry_key', [(SHA2, V1), (SHA3, V2)], None)
resolve_case('untyped_at_top_is_error', [(b'', V1)], None)
resolve_case('untyped_inherits', [(b'', V1)], [SHA3])
resolve_case('inherit_per_index', [(SHA2, V1), (b'', V2)], [SHA3, SHA3])
resolve_case('inherit_too_short_is_error', [(SHA3, V1), (b'', V2)], [SHA3])
resolve_case('no_registry_type', [(SHA2, V1)], None)
resolve_case('longer_ancestor_kept', [(SHA2, V1)], [SHA3, SHA2, SHA3])

# -- 3. serialize (and parse back) ---------------------------------------------

Head = buffer_pb2.Buffer.Head
Partition = Head.Partition


def big_head(n_partitions: int) -> Head:
    h = Head(index=7)
    for i in range(n_partitions):
        p = h.partitions.add()
        p.index[i % 1000].index[1].CopyFrom(Partition())
    return h


def serialize_case(name, messages, indices, parse_indices, note=''):
    """`messages`: Python objects given to serialize_to_buffer.
    `indices`: the serializer indices. `parse_indices`: the parser indices."""
    buffers = list(serialize_to_buffer(message_iterator=iter(messages), indices=dict(indices)))
    inputs = []
    index_of = {v: k for k, v in {**indices, 0: bytes}.items()}
    if len(dict(indices)) == 0 and messages and not isinstance(messages[0], bytes):
        index_of[type(messages[0])] = 1
    for m in messages:
        raw = m if isinstance(m, bytes) else m.SerializeToString()
        inputs.append({'index': index_of[type(m)], **describe(raw)})
    manifest['serialize'].append({
        'name': name,
        'note': note,
        'inputs': inputs,
        'stream': write_stream('serialize_' + name, buffers),
        'buffers': len(buffers),
        'parse_indices': sorted(parse_indices),
        'python_parse': python_parse(buffers, {k: bytes for k in parse_indices}),
    })


root = tempfile.mkdtemp(prefix='bee-vectors-')
try:
    fresh_env(root)

    serialize_case('empty_message', [buffer_pb2.Empty()], {1: buffer_pb2.Empty}, [1],
                   'An Empty message: one Buffer with head index 1, an empty chunk, separator.')
    serialize_case('bytes_small', [b'hello'], {}, [1],
                   'Raw bytes always use index 0. Head{index: 0} is present but has no fields.')
    serialize_case('head_small', [Head(index=3)], {1: Head}, [1],
                   'A small message under index 1.')
    serialize_case('three_messages', [Head(index=1), buffer_pb2.Empty(), b'raw'],
                   {1: Head, 2: buffer_pb2.Empty}, [1, 2],
                   'Three messages with indices 1, 2 and 0, in one stream.')
    serialize_case('bytes_chunk_minus_one', [data_bytes(1, CHUNK_SIZE - 1)], {}, [1],
                   'The largest payload that still goes in one Buffer.')
    serialize_case('bytes_chunk_exact', [data_bytes(2, CHUNK_SIZE)], {}, [1],
                   'CHUNK_SIZE bytes: head, one chunk, separator (three Buffers).')
    serialize_case('bytes_two_chunks_plus', [data_bytes(3, 2 * CHUNK_SIZE + 5)], {}, [1],
                   'Head, three chunks (CHUNK_SIZE, CHUNK_SIZE, 5), separator.')
    serialize_case('head_large', [big_head(150000)], {1: Head}, [1],
                   'A message larger than CHUNK_SIZE under index 1.')

    # -- 4. streams with blocks (receiver side) ---------------------------------

    def file_block(data: bytes):
        path = os.path.join(root, 'f-' + hashlib.sha256(data).hexdigest())
        with open(path, 'wb') as f:
            f.write(data)
        block_hash, block = block_builder.create_block(file_path=path, copy=True)
        return block_hash, block.SerializeToString()

    def install_dir_block(block_id: bytes, directory: str):
        shutil.move(directory.rstrip(os.sep), os.path.join(Enviroment.block_dir, block_id.hex()))

    def blocks_case(name, block_depth, build, note):
        fresh_env(root, block_depth=block_depth)
        obj_dir, contents, expected = build()
        buffers = list(serialize_to_buffer(
            message_iterator=iter([Dir(dir=obj_dir, _type=buffer_pb2.Buffer)]),
            indices={1: buffer_pb2.Buffer}))
        # The receiver holds no block: parse with an empty block directory.
        fresh_env(root)
        parsed = python_parse(buffers, {1: bytes}, message_mode=False)
        assert parsed.get("parts") and parsed["parts"][0]["sha256"] == \
            hashlib.sha256(expected).hexdigest(), name
        markers = []
        depth = 0
        open_ids = []
        for b in buffers:
            if b.HasField('block'):
                bid = client.get_hash_from_block(b.block)
                if open_ids and open_ids[-1] == bid:
                    open_ids.pop()
                else:
                    open_ids.append(bid)
                    markers.append({'id': bid, 'depth': len(open_ids),
                                    'previous_lengths_position':
                                        list(b.block.previous_lengths_position)})
        manifest['parse'].append({
            'name': name,
            'note': note,
            'stream': write_stream('parse_' + name, buffers),
            'parse_indices': [1],
            'python_parse': parsed,
            'blocks': {k.hex(): v.hex() for k, v in contents.items()},
            'block_starts': markers,
        })
        return buffers

    def one_block():
        leaf = data_bytes(10, 9000)
        leaf_id, leaf_ptr = file_block(leaf)
        obj = buffer_pb2.Buffer(chunk=leaf_ptr, separator=True)
        _, obj_dir = block_builder.build_multiblock(obj, blocks=[leaf_id])
        expected = buffer_pb2.Buffer(chunk=leaf, separator=True).SerializeToString()
        return obj_dir, {leaf_id: leaf}, expected

    blocks_case('one_block', 1, one_block,
                'An object with one block in its chunk field. Markers: start, content, end.')

    def big_block():
        leaf = data_bytes(11, 2 * CHUNK_SIZE + 100)
        leaf_id, leaf_ptr = file_block(leaf)
        obj = buffer_pb2.Buffer(chunk=leaf_ptr)
        _, obj_dir = block_builder.build_multiblock(obj, blocks=[leaf_id])
        expected = buffer_pb2.Buffer(chunk=leaf).SerializeToString()
        return obj_dir, {leaf_id: leaf}, expected

    big_buffers = blocks_case('big_block', 1, big_block,
                              'One block of three chunks. Used by the skip vectors.')

    def nested(depth):
        def build():
            leaf = data_bytes(12, 9000)
            leaf_id, leaf_ptr = file_block(leaf)
            inner = buffer_pb2.Buffer(chunk=leaf_ptr)
            inner_id, inner_dir = block_builder.build_multiblock(inner, blocks=[leaf_id])
            install_dir_block(inner_id, inner_dir)
            tail = data_bytes(13, 4000)
            tail_id, tail_ptr = file_block(tail)
            outer = buffer_pb2.Buffer()
            outer.block.hashes.add().value = block_pointer(inner_id).SerializeToString()
            outer.block.hashes.add().value = tail_ptr
            _, outer_dir = block_builder.build_multiblock(outer, blocks=[inner_id, tail_id])
            inner_full = buffer_pb2.Buffer(chunk=leaf).SerializeToString()
            full = buffer_pb2.Buffer()
            full.block.hashes.add().value = inner_full
            full.block.hashes.add().value = tail
            contents = {leaf_id: leaf, tail_id: tail, inner_id: inner_full}
            return outer_dir, contents, full.SerializeToString()
        return build

    blocks_case('nested_depth1', 1, nested(1),
                'An object that points to a multiblock block and to a file block. The '
                'sender frames one level: the inner block content goes flat.')
    nested_buffers = blocks_case('nested_depth2', 2, nested(2),
                                 'The same object, framed two levels deep. The leaf block '
                                 'has its own markers inside the inner block.')

    # -- 5. parse errors and edge cases ------------------------------------------

    def ptr(block_id: bytes) -> buffer_pb2.Buffer.Block:
        return block_pointer(block_id)

    def error_case(name, buffers, parse_indices, rust_expects, note, message_mode=True):
        manifest['parse_errors'].append({
            'name': name,
            'note': note,
            'stream': write_stream('error_' + name, buffers),
            'parse_indices': parse_indices,
            'python_parse': python_parse(buffers, {k: bytes for k in parse_indices},
                                         message_mode=message_mode,
                                         skip_wbp=not message_mode),
            'rust_expects': rust_expects,
        })

    fresh_env(root)
    error_case('unknown_head_index',
               [buffer_pb2.Buffer(head=Head(index=9), chunk=b'x', separator=True)],
               [1], 'error',
               'Head index 9 is not accepted: the parser must refuse it.')
    error_case('headless_message',
               [buffer_pb2.Buffer(chunk=b'abc', separator=True)],
               [1], 'ok',
               'A Buffer with no head goes to index 1 when index 1 is accepted.')
    error_case('message_without_separator',
               [buffer_pb2.Buffer(head=Head(index=1)), buffer_pb2.Buffer(chunk=b'abc')],
               [1], 'ok',
               'The stream ends with no separator. The reference yields what it has.')

    a, b = hashlib.sha3_256(b'A').digest(), hashlib.sha3_256(b'B').digest()
    error_case('intersecting_blocks',
               [buffer_pb2.Buffer(head=Head(index=1)),
                buffer_pb2.Buffer(block=ptr(a)), buffer_pb2.Buffer(block=ptr(b)),
                buffer_pb2.Buffer(chunk=b'z'), buffer_pb2.Buffer(block=ptr(a)),
                buffer_pb2.Buffer(block=ptr(b)), buffer_pb2.Buffer(separator=True)],
               [1], 'error',
               'Block A opens, block B opens, then A closes before B: not allowed.', message_mode=False)

    def chain(levels: int):
        """`levels` nested blocks with real ids: level k adds one byte of its own."""
        content = b'core'
        ids = []
        for k in range(levels, 0, -1):
            content = bytes([k]) + content
            ids.append(hashlib.sha3_256(content).digest())
        ids.reverse()  # ids[0] is the outermost
        out = [buffer_pb2.Buffer(head=Head(index=1))]
        for k, bid in enumerate(ids, start=1):
            out.append(buffer_pb2.Buffer(block=ptr(bid)))
            out.append(buffer_pb2.Buffer(chunk=bytes([k])))
        out.append(buffer_pb2.Buffer(chunk=b'core'))
        for bid in reversed(ids):
            out.append(buffer_pb2.Buffer(block=ptr(bid)))
        out.append(buffer_pb2.Buffer(separator=True))
        return out

    error_case('nesting_at_limit', chain(client.MAX_BLOCK_NESTING), [1], 'ok',
               '32 nested blocks: the limit, accepted. Each id is the hash of its expansion.', message_mode=False)
    error_case('nesting_over_limit', chain(client.MAX_BLOCK_NESTING + 1), [1], 'error',
               '33 nested blocks: refused.', message_mode=False)

    bad = hashlib.sha3_256(b'not the content').digest()
    error_case('block_id_mismatch',
               [buffer_pb2.Buffer(head=Head(index=1)), buffer_pb2.Buffer(block=ptr(bad)),
                buffer_pb2.Buffer(chunk=b'content'), buffer_pb2.Buffer(block=ptr(bad)),
                buffer_pb2.Buffer(separator=True)],
               [1], 'error',
               'The content does not hash to the block id: refused (reference disk mode).',
               message_mode=False)
    untyped = buffer_pb2.Buffer.Block(hashes=[buffer_pb2.Buffer.Block.Hash(value=a)])
    error_case('block_without_hash_type',
               [buffer_pb2.Buffer(head=Head(index=1)), buffer_pb2.Buffer(block=untyped),
                buffer_pb2.Buffer(chunk=b'A'), buffer_pb2.Buffer(block=untyped),
                buffer_pb2.Buffer(separator=True)],
               [1], 'error',
               'DEVIATION D2: a block marker on the wire with no hash type. The reference '
               'does not see a block and ends the message there. The Rust parser refuses it.',
               message_mode=False)

    # -- 6. skip (reverse-direction control) ---------------------------------------

    def skip_case(name, source_name, buffers, skip_ids, after, note):
        """`after`: how many Buffers of `buffers` go out before the skip request
        arrives. 0 means the request is there before the stream starts."""
        control = StreamControl()
        out = []

        def feed():
            for i, b in enumerate(buffers):
                if i == after:
                    for sid in skip_ids:
                        control.observe(skip_buffer(sid))
                yield b

        for b in skip_requested_blocks(feed(), control=control):
            out.append(b)
        manifest['skip'].append({
            'name': name,
            'note': note,
            'input': source_name,
            'skip_ids': skip_ids,
            'request_after': after,
            'output': write_stream('skip_' + name, out),
            'output_buffers': len(out),
        })

    big_stream = 'parse_big_block.bee'
    nested_stream = 'parse_nested_depth2.bee'
    big_id = manifest['parse'][1]['block_starts'][0]['id']
    nested_starts = manifest['parse'][3]['block_starts']
    inner_id = nested_starts[0]['id']
    leaf_id = nested_starts[1]['id']

    skip_case('none', big_stream, big_buffers, [], 0, 'No skip request: the output is the input.')
    skip_case('before_start', big_stream, big_buffers, [big_id], 0,
              'The request is there before the block starts: start and end markers only.')
    skip_case('mid_block', big_stream, big_buffers, [big_id], 3,
              'The request arrives after the first chunk of the block: the rest is dropped, '
              'the end marker is kept.')
    skip_case('nested_child', nested_stream, nested_buffers, [leaf_id], 0,
              'Skip the inner leaf block only.')
    skip_case('nested_parent', nested_stream, nested_buffers, [inner_id], 0,
              'Skip the parent: its children markers are dropped too, its end marker is kept.')

    sid = hashlib.sha3_256(b'held').hexdigest()
    sb = skip_buffer(sid)
    manifest['skip_buffer'] = {
        'block_id': sid,
        'buffer': sb.SerializeToString().hex(),
        'skipped_block_id': skipped_block_id(sb),
    }

    def strip_case(name, buffers, note):
        control = StreamControl()
        # control.reader clears the skip field in place: write the input first.
        input_file = write_stream('strip_in_' + name, buffers)
        out = list(control.reader(iter(buffers)))
        manifest['strip_skips'].append({
            'name': name, 'note': note,
            'input': input_file,
            'output': write_stream('strip_out_' + name, out),
            'observed': sorted(control._skip),
        })

    with_payload = skip_buffer(sid)
    with_payload.chunk = b'payload'
    strip_case('inert_removed',
               [buffer_pb2.Buffer(head=Head(index=1)), skip_buffer(sid),
                buffer_pb2.Buffer(chunk=b'x', separator=True)],
               'A Buffer with only a skip request (and its empty chunk) is removed.')
    strip_case('payload_kept',
               [buffer_pb2.Buffer(head=Head(index=1)), with_payload,
                buffer_pb2.Buffer(separator=True)],
               'A skip request on a Buffer with payload: the skip field is cleared, '
               'the Buffer is kept.')

    # -- 7. .bee file written by the reference itself --------------------------------

    fresh_env(root)
    path = client.write_to_file(path=OUT, file_name='bee_file_reference',
                                input=iter([Head(index=5), b'tail']),
                                indices={1: Head})
    manifest['bee_file'].append({
        'name': 'reference_writer',
        'note': 'Written by bee_rpc.client.write_to_file, not by this script.',
        'file': os.path.basename(path),
        'parse_indices': [1],
        'python_parse': python_parse(list(client.read_bee_file(path)), {1: bytes}),
    })
finally:
    shutil.rmtree(root, ignore_errors=True)

with open(os.path.join(OUT, 'manifest.json'), 'w') as f:
    json.dump(manifest, f, indent=1, sort_keys=True)
    f.write('\n')
print('wrote', OUT)
