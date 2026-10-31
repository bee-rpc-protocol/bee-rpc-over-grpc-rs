//! Errors of the crate.

/// Every error that this crate returns.
///
/// The tests check the variant, not the message. Keep the variants. You can add
/// fields or new variants.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A head index that the parser does not accept.
    #[error("bee-rpc: head index {0} is not accepted")]
    UnknownIndex(i32),

    /// A block end marker that does not close the innermost open block.
    #[error("bee-rpc: blocks intersect")]
    IntersectingBlocks,

    /// More than [`crate::MAX_BLOCK_NESTING`] nested blocks.
    #[error("bee-rpc: blocks nested more than {0} deep")]
    NestingTooDeep(usize),

    /// The content of a block does not hash to its id.
    #[error("bee-rpc: block {id} hashes to {actual}")]
    BlockIdMismatch {
        /// The id on the marker, hexadecimal.
        id: String,
        /// The hash of the received content, hexadecimal.
        actual: String,
    },

    /// A hash type that is not known, or a pointer whose type cannot be deduced.
    #[error("bee-rpc: hash type error: {0}")]
    HashType(String),

    /// A block marker that names no block in this hash type.
    #[error("bee-rpc: a block marker without a resolvable hash type")]
    UnresolvableBlock,

    /// A stream that ends inside a block.
    #[error("bee-rpc: the stream ended inside block {0}")]
    UnclosedBlock(String),

    /// Bytes that are not a valid Buffer, or a .bee file that is cut.
    #[error("bee-rpc: decode error: {0}")]
    Decode(String),

    /// A block store that cannot be read: a missing block, or a `_.json` that is not in the
    /// format of the reference.
    #[error("bee-rpc: store: {0}")]
    Store(String),

    /// An I/O error.
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// A gRPC transport or status error.
    #[error("bee-rpc: transport: {0}")]
    Transport(String),
}
