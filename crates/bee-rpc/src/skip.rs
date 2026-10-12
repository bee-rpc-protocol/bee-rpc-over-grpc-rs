//! Reverse-direction block skip (`control.py`, `client.skip_requested_blocks`).
//!
//! The receiver of a block that it already holds sends a skip request in the
//! other direction: a Buffer with `skip` (field 6) set to a one-hash pointer and
//! an empty `chunk`. The sender then drops the rest of that block.
//!
//! # Sender filter rules ([`SkipFilter`], must match the vectors)
//!
//! - The skip set is checked again for each Buffer, not once per block.
//! - The end marker of a block is always emitted, exactly once, also when the
//!   block is skipped.
//! - A skipped block drops its children: their start and end markers too.
//! - When a parent is skipped while a child is open, the shallowest skipped open
//!   block wins.
//! - Start markers that come before the skip request are emitted.

use std::collections::{BTreeSet, HashSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};

use crate::buffer::buffer::block::Hash;
use crate::buffer::buffer::Block;
use crate::buffer::Buffer;
use crate::hash::HashAlgorithm;

/// The wire form of a skip request (`control.skip_buffer`).
pub fn skip_buffer(block_id: &[u8], algorithm: HashAlgorithm) -> Buffer {
    Buffer {
        skip: Some(Block {
            hashes: vec![Hash {
                r#type: algorithm.hash_type(),
                value: block_id.to_vec(),
            }],
            ..Default::default()
        }),
        // A peer that does not know field 6 sees an empty chunk, which is inert.
        chunk: Some(Vec::new()),
        ..Default::default()
    }
}

/// The block id of a skip request in this algorithm, if `buffer` has one
/// (`control.skipped_block_id`).
pub fn skipped_block_id(buffer: &Buffer, algorithm: HashAlgorithm) -> Option<Vec<u8>> {
    block_id(buffer.skip.as_ref()?, &algorithm.hash_type())
}

/// The value of the first hash of `block` with type `hash_type`
/// (`client.get_hash_from_block`).
fn block_id(block: &Block, hash_type: &[u8]) -> Option<Vec<u8>> {
    block
        .hashes
        .iter()
        .find(|h| h.r#type == hash_type)
        .map(|h| h.value.clone())
}

/// True if `buffer` carries nothing but a skip request and its empty chunk
/// (`control._is_inert`).
fn is_inert(buffer: &Buffer) -> bool {
    !(buffer.head.is_some()
        || buffer.block.is_some()
        || buffer.separator == Some(true)
        || buffer.signal == Some(true)
        || buffer.chunk.as_ref().is_some_and(|c| !c.is_empty()))
}

#[derive(Debug, Default)]
struct State {
    /// The ids that the peer asked us to skip.
    peer_skips: HashSet<Vec<u8>>,
    /// The ids that we asked the peer to skip.
    asked: HashSet<Vec<u8>>,
    /// The skip Buffers to send to the peer, oldest first.
    outbound: VecDeque<Buffer>,
}

/// The skip state of one call, shared by its parse side and its send side
/// (`control.StreamControl`). Clone it to share it. It must be `Send + Sync`.
#[derive(Clone, Debug)]
pub struct StreamControl {
    algorithm: HashAlgorithm,
    hash_type: Arc<[u8]>,
    state: Arc<Mutex<State>>,
}

impl StreamControl {
    /// A new control, enabled, for block ids in `algorithm`.
    pub fn new(algorithm: HashAlgorithm) -> Self {
        StreamControl {
            algorithm,
            hash_type: algorithm.hash_type().into(),
            state: Arc::new(Mutex::new(State::default())),
        }
    }

    /// The state. A panic in another thread while it held the lock cannot leave
    /// the sets half-written, so a poisoned lock is still usable.
    fn state(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Receiver side: ask the peer to skip `block_id`. Queues one skip Buffer.
    /// A second request for the same id queues nothing.
    pub fn request_skip(&self, block_id: &[u8]) {
        if block_id.is_empty() {
            return;
        }
        let mut state = self.state();
        if state.asked.insert(block_id.to_vec()) {
            state
                .outbound
                .push_back(skip_buffer(block_id, self.algorithm));
        }
    }

    /// Take all queued skip Buffers, oldest first.
    pub fn take_outbound(&self) -> Vec<Buffer> {
        self.state().outbound.drain(..).collect()
    }

    /// Record a skip request from the peer, if `buffer` has one. True if it had.
    pub fn observe(&self, buffer: &Buffer) -> bool {
        let Some(id) = buffer
            .skip
            .as_ref()
            .and_then(|skip| block_id(skip, &self.hash_type))
        else {
            return false;
        };
        self.state().peer_skips.insert(id);
        true
    }

    /// Sender side: true if the peer asked to skip `block_id`.
    pub fn should_skip(&self, block_id: &[u8]) -> bool {
        !block_id.is_empty() && self.state().peer_skips.contains(block_id)
    }

    /// The ids that the peer asked to skip, sorted.
    pub fn peer_skips(&self) -> Vec<Vec<u8>> {
        let sorted: BTreeSet<Vec<u8>> = self.state().peer_skips.iter().cloned().collect();
        sorted.into_iter().collect()
    }
}

/// Receiver side of an incoming stream (`StreamControl.reader`): record the skip
/// request of `buffer` in `control`, then drop the Buffer if it carries nothing
/// else, or return it with the `skip` field cleared.
pub fn strip_skip(control: &StreamControl, mut buffer: Buffer) -> Option<Buffer> {
    if control.observe(&buffer) {
        if is_inert(&buffer) {
            return None;
        }
        buffer.skip = None;
    }
    Some(buffer)
}

/// Sender side: drops the body of blocks that the peer asked to skip
/// (`client.skip_requested_blocks`). See the rules in the module docs.
pub struct SkipFilter {
    control: StreamControl,
    /// The ids of the open blocks, outermost first. `None` for a marker with no
    /// hash in the algorithm of the control.
    stack: Vec<Option<Vec<u8>>>,
    /// The 1-based depth in `stack` of the block that is being skipped.
    skipping_at: Option<usize>,
}

impl SkipFilter {
    /// A new filter that reads the skip set of `control`.
    pub fn new(control: StreamControl) -> Self {
        SkipFilter {
            control,
            stack: Vec::new(),
            skipping_at: None,
        }
    }

    /// The depth of the shallowest open block that the peer asked to skip. The
    /// shallowest, so that a request for a parent takes its open children too.
    fn outermost_skipped(&self) -> Option<usize> {
        self.stack
            .iter()
            .position(|id| id.as_deref().is_some_and(|id| self.control.should_skip(id)))
            .map(|i| i + 1)
    }

    /// Give the next outgoing Buffer. Returns it if it must be sent.
    pub fn push(&mut self, buffer: Buffer) -> Option<Buffer> {
        if let Some(block) = &buffer.block {
            let id = block_id(block, &self.control.hash_type);
            if self.stack.last() == Some(&id) {
                // The end marker of the innermost open block.
                let depth = self.stack.len();
                self.stack.pop();
                return match self.skipping_at {
                    None => Some(buffer),
                    Some(at) if at == depth => {
                        self.skipping_at = None;
                        Some(buffer)
                    }
                    // Inside a shallower skipped block: dropped with it.
                    Some(_) => None,
                };
            }
            // A start marker.
            self.stack.push(id);
            if self.skipping_at.is_some() {
                return None;
            }
            self.skipping_at = self.outermost_skipped();
            return Some(buffer);
        }
        if self.skipping_at.is_none() {
            self.skipping_at = self.outermost_skipped();
        }
        match self.skipping_at {
            None => Some(buffer),
            Some(_) => None,
        }
    }
}
