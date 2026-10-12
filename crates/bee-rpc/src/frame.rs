//! Messages to Buffers and Buffers to messages (`client.serialize_to_buffer`,
//! `client.parse_from_buffer`).
//!
//! # Sender rules (must be byte-identical to the reference)
//!
//! For a part with index `i` and bytes `data`:
//!
//! - The head is `Head { index: i, partitions: [] }`. It is present also when `i`
//!   is 0 (then it encodes as an empty message, but the field is set).
//! - If `data.len() < CHUNK_SIZE`: one Buffer `{ chunk: data, head, separator: true }`.
//!   `chunk` is set also when `data` is empty.
//! - Else: `{ head }`, then one `{ chunk }` per `CHUNK_SIZE` piece (the last piece
//!   can be shorter), then `{ separator: true }`.
//!
//! # Receiver rules
//!
//! - A Buffer with a head starts a message with that index. A Buffer with no head
//!   and no open message starts a message with `ParseOptions::headless_index`.
//! - The index must be accepted ([`ParseOptions::accepted`]; index 0 is always
//!   accepted), else [`Error::UnknownIndex`].
//! - `chunk` bytes are added to the open message. `separator: true` ends it.
//! - At the end of the stream, an open message is emitted as it is (the
//!   reference does the same).
//! - `signal: true` toggles the [`Signal`].
//! - The `skip` field is never part of a message. A Buffer with only `skip` and an
//!   empty chunk is dropped. With a [`StreamControl`], the skip request is recorded.
//! - Blocks: a `block` marker opens a block, a marker with the same id closes it.
//!   The id is [`crate::pointer::block_id_from_pointer`] with no inherited types
//!   and `ParseOptions::algorithm`. No id: [`Error::UnresolvableBlock`]. A marker
//!   whose id is open but not innermost: [`Error::IntersectingBlocks`]. More than
//!   [`crate::MAX_BLOCK_NESTING`] open: [`Error::NestingTooDeep`]. When a block
//!   closes, its content (the bytes added while it was open, nested blocks
//!   included) must hash to its id, else [`Error::BlockIdMismatch`]. A stream
//!   that ends with an open block: [`Error::UnclosedBlock`].
//! - A chunk on a start marker Buffer goes inside the new block. A chunk on an end
//!   marker Buffer goes inside the block that closes.
//! - If the [`BlockStore`] holds an opened block, its bytes come from the store,
//!   the bytes on the wire until its end marker are dropped, and the
//!   [`StreamControl`] (if any) gets `request_skip(id)`.
//! - Block markers are not part of the message bytes: the message is the
//!   expansion (the original object with the block content in place).
//! - Empty messages are emitted (see `DEVIATIONS.md`, D4).

use std::collections::BTreeSet;
use std::sync::Arc;

use crate::buffer::buffer::{Block, Head};
use crate::buffer::Buffer;
use crate::hash::{HashAlgorithm, Hasher};
use crate::pointer::block_id_from_pointer;
use crate::signal::Signal;
use crate::skip::StreamControl;
use crate::{Error, CHUNK_SIZE, MAX_BLOCK_NESTING};

/// One message: its index and its bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Part {
    /// The head index. 0 means raw bytes.
    pub index: i32,
    /// The serialized message, or the raw bytes.
    pub data: Vec<u8>,
}

impl Part {
    /// A new part.
    pub fn new(index: i32, data: impl Into<Vec<u8>>) -> Self {
        Part {
            index,
            data: data.into(),
        }
    }
}

/// The Buffers of one part, in order. See the sender rules in the module docs.
pub fn serialize_part(part: &Part) -> PartBuffers<'_> {
    let stage = if part.data.len() < CHUNK_SIZE {
        Stage::Short
    } else {
        Stage::Head
    };
    PartBuffers {
        index: part.index,
        data: &part.data,
        chunks: part.data.chunks(CHUNK_SIZE),
        stage,
    }
}

/// The Buffers of all parts, one after the other.
pub fn serialize_parts<'a>(parts: impl IntoIterator<Item = &'a Part>) -> Vec<Buffer> {
    parts.into_iter().flat_map(serialize_part).collect()
}

/// Iterator over the Buffers of one part. Do not copy the whole part at once
/// for a large part: make each chunk when `next` is called.
pub struct PartBuffers<'a> {
    index: i32,
    data: &'a [u8],
    chunks: std::slice::Chunks<'a, u8>,
    stage: Stage,
}

/// Where a [`PartBuffers`] is in the sender rules.
enum Stage {
    /// The one-Buffer form: chunk, head and separator.
    Short,
    /// The long form, before its head.
    Head,
    /// The long form, after its head: the chunks, then the separator.
    Chunks,
    /// All Buffers are out.
    Done,
}

impl PartBuffers<'_> {
    fn head(&self) -> Option<Head> {
        Some(Head {
            index: self.index,
            partitions: Vec::new(),
        })
    }
}

impl Iterator for PartBuffers<'_> {
    type Item = Buffer;

    fn next(&mut self) -> Option<Buffer> {
        match self.stage {
            Stage::Short => {
                self.stage = Stage::Done;
                Some(Buffer {
                    chunk: Some(self.data.to_vec()),
                    head: self.head(),
                    separator: Some(true),
                    ..Default::default()
                })
            }
            Stage::Head => {
                self.stage = Stage::Chunks;
                Some(Buffer {
                    head: self.head(),
                    ..Default::default()
                })
            }
            Stage::Chunks => match self.chunks.next() {
                Some(piece) => Some(Buffer {
                    chunk: Some(piece.to_vec()),
                    ..Default::default()
                }),
                None => {
                    self.stage = Stage::Done;
                    Some(Buffer {
                        separator: Some(true),
                        ..Default::default()
                    })
                }
            },
            Stage::Done => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = match self.stage {
            Stage::Short => 1,
            Stage::Head => self.chunks.len() + 2,
            Stage::Chunks => self.chunks.len() + 1,
            Stage::Done => 0,
        };
        (n, Some(n))
    }
}

impl ExactSizeIterator for PartBuffers<'_> {}

impl std::iter::FusedIterator for PartBuffers<'_> {}

/// Options of the [`Parser`].
#[derive(Clone, Debug)]
pub struct ParseOptions {
    /// The accepted head indices. `None` accepts all. Index 0 is always accepted.
    pub accepted: Option<BTreeSet<i32>>,
    /// The index of a message that starts with no head. The reference uses 1.
    pub headless_index: i32,
    /// The algorithm that block ids are read in.
    pub algorithm: HashAlgorithm,
}

impl Default for ParseOptions {
    fn default() -> Self {
        ParseOptions {
            accepted: None,
            headless_index: 1,
            algorithm: HashAlgorithm::Sha3_256,
        }
    }
}

impl ParseOptions {
    /// Options that accept only `indices` (and 0).
    pub fn accepting(indices: impl IntoIterator<Item = i32>) -> Self {
        ParseOptions {
            accepted: Some(indices.into_iter().collect()),
            ..Default::default()
        }
    }
}

/// Blocks that the receiver already holds, by id.
pub trait BlockStore: Send + Sync {
    /// The full content (the expansion) of block `id`, if the store holds it.
    fn get(&self, id: &[u8]) -> Option<Vec<u8>>;
}

/// A block start marker, as the parser saw it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockStart {
    /// The block id.
    pub id: Vec<u8>,
    /// 1 for a block directly in the message, 2 for a block in that block, ...
    pub depth: usize,
    /// `previous_lengths_position` of the marker, as received.
    pub previous_lengths_position: Vec<u64>,
}

/// A block that is open: its id, and a hasher over its expansion.
struct OpenBlock {
    id: Vec<u8>,
    hasher: Hasher,
    /// The content comes from the store; the wire bytes up to its end marker
    /// are dropped, and the content is not checked again.
    held: bool,
}

/// The message that is being read.
struct OpenMessage {
    index: i32,
    data: Vec<u8>,
}

/// A push parser: give it Buffers, get complete messages.
///
/// It does no I/O, so the same parser serves a gRPC stream, a .bee file or a test.
pub struct Parser {
    options: ParseOptions,
    store: Option<Arc<dyn BlockStore>>,
    signal: Option<Signal>,
    control: Option<StreamControl>,
    message: Option<OpenMessage>,
    blocks: Vec<OpenBlock>,
    starts: Vec<BlockStart>,
}

fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// True if `buffer` has nothing that a message or a block needs.
fn is_inert(buffer: &Buffer) -> bool {
    buffer.head.is_none()
        && buffer.block.is_none()
        && buffer.separator != Some(true)
        && buffer.signal != Some(true)
        && buffer.chunk.as_ref().is_none_or(|c| c.is_empty())
}

impl Parser {
    /// A new parser.
    pub fn new(options: ParseOptions) -> Self {
        Parser {
            options,
            store: None,
            signal: None,
            control: None,
            message: None,
            blocks: Vec::new(),
            starts: Vec::new(),
        }
    }

    /// Use `store` for blocks that the receiver already holds.
    pub fn with_store(mut self, store: Arc<dyn BlockStore>) -> Self {
        self.store = Some(store);
        self
    }

    /// Toggle `signal` when a Buffer has `signal: true`.
    pub fn with_signal(mut self, signal: Signal) -> Self {
        self.signal = Some(signal);
        self
    }

    /// Record skip requests from the peer in `control`, and ask the peer to skip
    /// blocks that the store holds.
    pub fn with_control(mut self, control: StreamControl) -> Self {
        self.control = Some(control);
        self
    }

    /// Add one Buffer. Returns the messages that it completes (zero or more).
    pub fn push(&mut self, mut buffer: Buffer) -> Result<Vec<Part>, Error> {
        let mut out = Vec::new();

        // The skip field is never message data (D5).
        if buffer.skip.is_some() {
            if let Some(control) = &self.control {
                control.observe(&buffer);
            }
            buffer.skip = None;
            if is_inert(&buffer) {
                return Ok(out);
            }
        }
        if buffer.signal == Some(true) {
            if let Some(signal) = &self.signal {
                signal.change();
            }
        }

        if self.blocks.last().is_some_and(|b| b.held) {
            // Inside a block that the store holds: only its end marker matters.
            let closes = match (&buffer.block, self.blocks.last()) {
                (Some(block), Some(top)) => {
                    block_id_from_pointer(block, None, self.options.algorithm).as_deref()
                        == Some(top.id.as_slice())
                }
                _ => false,
            };
            if !closes {
                return Ok(out);
            }
            self.blocks.pop();
            buffer.chunk = None;
        } else {
            if self.message.is_none() {
                if let Some(head) = &buffer.head {
                    self.start_message(head.index)?;
                } else if buffer.chunk.is_some()
                    || buffer.block.is_some()
                    || buffer.separator == Some(true)
                {
                    self.start_message(self.options.headless_index)?;
                }
            }
            if self.message.is_none() {
                return Ok(out);
            }
            if let Some(block) = buffer.block.take() {
                self.marker(&block, buffer.chunk.take())?;
            }
            if let Some(chunk) = buffer.chunk.take() {
                self.add_content(&chunk);
            }
        }

        if buffer.separator == Some(true) {
            if let Some(last) = self.blocks.last() {
                return Err(Error::UnclosedBlock(to_hex(&last.id)));
            }
            out.extend(self.message.take().map(|m| Part {
                index: m.index,
                data: m.data,
            }));
        }
        Ok(out)
    }

    /// End of the stream. Returns the message that is still open, if any.
    pub fn finish(&mut self) -> Result<Vec<Part>, Error> {
        if let Some(last) = self.blocks.last() {
            return Err(Error::UnclosedBlock(to_hex(&last.id)));
        }
        Ok(self
            .message
            .take()
            .map(|m| Part {
                index: m.index,
                data: m.data,
            })
            .into_iter()
            .collect())
    }

    /// Every block start marker seen so far, in stream order.
    pub fn block_starts(&self) -> &[BlockStart] {
        &self.starts
    }

    fn start_message(&mut self, index: i32) -> Result<(), Error> {
        let accepted = index == 0
            || self
                .options
                .accepted
                .as_ref()
                .is_none_or(|set| set.contains(&index));
        if !accepted {
            return Err(Error::UnknownIndex(index));
        }
        self.message = Some(OpenMessage {
            index,
            data: Vec::new(),
        });
        Ok(())
    }

    /// Bytes of the expansion: to the message and to every open block.
    fn add_content(&mut self, data: &[u8]) {
        if let Some(message) = &mut self.message {
            message.data.extend_from_slice(data);
        }
        for block in &mut self.blocks {
            block.hasher.update(data);
        }
    }

    /// A block marker: it closes the innermost open block, or opens a block.
    /// `chunk` is the chunk of the same Buffer.
    fn marker(&mut self, block: &Block, chunk: Option<Vec<u8>>) -> Result<(), Error> {
        let algorithm = self.options.algorithm;
        let id = block_id_from_pointer(block, None, algorithm).ok_or(Error::UnresolvableBlock)?;
        if self.blocks.iter().any(|b| b.id == id) {
            if self.blocks.last().map(|b| &b.id) != Some(&id) {
                return Err(Error::IntersectingBlocks);
            }
            if let Some(chunk) = chunk {
                self.add_content(&chunk);
            }
            if let Some(closed) = self.blocks.pop() {
                let actual = closed.hasher.finalize();
                if actual != id {
                    return Err(Error::BlockIdMismatch {
                        id: to_hex(&id),
                        actual: to_hex(&actual),
                    });
                }
            }
            return Ok(());
        }
        if self.blocks.len() >= MAX_BLOCK_NESTING {
            return Err(Error::NestingTooDeep(MAX_BLOCK_NESTING));
        }
        self.starts.push(BlockStart {
            id: id.clone(),
            depth: self.blocks.len() + 1,
            previous_lengths_position: block.previous_lengths_position.clone(),
        });
        let held_content = self.store.as_ref().and_then(|s| s.get(&id));
        let held = held_content.is_some();
        self.blocks.push(OpenBlock {
            id: id.clone(),
            hasher: algorithm.hasher(),
            held,
        });
        match held_content {
            Some(content) => {
                self.add_content(&content);
                if let Some(control) = &self.control {
                    control.request_skip(&id);
                }
            }
            None => {
                if let Some(chunk) = chunk {
                    self.add_content(&chunk);
                }
            }
        }
        Ok(())
    }
}

/// Parse a whole stream (all of `buffers`, then [`Parser::finish`]).
pub fn parse_all(
    buffers: impl IntoIterator<Item = Buffer>,
    options: ParseOptions,
) -> Result<Vec<Part>, Error> {
    let mut parser = Parser::new(options);
    let mut out = Vec::new();
    for b in buffers {
        out.extend(parser.push(b)?);
    }
    out.extend(parser.finish()?);
    Ok(out)
}
