//! The block store on disk, and the send side of a stored object
//! (`reader.py`: `read_from_registry`, `read_multiblock_directory`, `read_block`;
//! `client.serialize_to_buffer` for a `Dir`).
//!
//! # The format
//!
//! A *multiblock directory* holds a file `_.json` and the files that it names. `_.json`
//! is a list. An integer `n` is the file `n` of the directory: bytes of the object. A pair
//! `["<id>", [l1, l2, ...]]` is a block: `<id>` is its id (the hash of its expansion, as
//! lowercase hexadecimal), and the integers are its `previous_lengths_position`. The block
//! is the file or the multiblock directory `<id>` in the *block directory* of the
//! [`Store`]. Other files of the directory (`wbp.bin`) are not read.
//!
//! # What the reader sends
//!
//! [`Store::read_registry`] gives the Buffers of a stored object: its bytes in chunks of
//! [`CHUNK_SIZE`], and for each block a start marker, the content of the block, and an end
//! marker (the same pointer, with the same `previous_lengths_position`). A block that is a
//! multiblock directory has its own markers inside only to the depth that the store has
//! ([`Store::with_block_depth`], `Enviroment.block_depth`): below it the content of the
//! block is sent flat. The id of a block that is a single file is checked against its
//! bytes before the first byte of it leaves. [`Store::read_message`] adds the head and the
//! separator, as the reference sends a `Dir`.
//!
//! The reader is a blocking iterator, one record at a time: run it where blocking is
//! allowed (`spawn_blocking`). It keeps no block in memory.
//!
//! # Where this crate is stricter
//!
//! The reference has no limit for directories inside directories. This reader refuses
//! more than [`MAX_BLOCK_NESTING`] of them, so a store that points to itself ends with an
//! error. It also reads `_.json` with a small parser of the only shape that the reference
//! writes, and refuses another shape. A block id that is not lowercase hexadecimal is an
//! error, and so cannot name a path outside of the block directory.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::buffer::buffer::{Block, Head};
use crate::buffer::Buffer;
use crate::hash::{HashAlgorithm, Hasher};
use crate::pointer::block_pointer;
use crate::{Error, CHUNK_SIZE, MAX_BLOCK_NESTING};

/// The name of the file that lists the parts of a multiblock directory.
pub const METADATA_FILE_NAME: &str = "_.json";

fn invalid(reason: impl Into<String>) -> Error {
    Error::Store(reason.into())
}

/// One entry of `_.json`.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Entry {
    /// The file with this name, in the same directory.
    File(u64),
    /// A block: its id (lowercase hexadecimal) and its `previous_lengths_position`.
    Block { id: String, lengths: Vec<u64> },
}

/// A cursor over the text of `_.json`.
struct Json<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Json<'a> {
    fn skip_space(&mut self) {
        while let Some(b) = self.bytes.get(self.at) {
            if matches!(b, b' ' | b'\n' | b'\r' | b'\t') {
                self.at += 1;
            } else {
                break;
            }
        }
    }

    fn peek(&mut self) -> Option<u8> {
        self.skip_space();
        self.bytes.get(self.at).copied()
    }

    fn expect(&mut self, wanted: u8) -> Result<(), Error> {
        if self.peek() == Some(wanted) {
            self.at += 1;
            Ok(())
        } else {
            Err(invalid(format!(
                "{METADATA_FILE_NAME}: expected '{}' at byte {}",
                char::from(wanted),
                self.at
            )))
        }
    }

    fn number(&mut self) -> Result<u64, Error> {
        self.skip_space();
        let start = self.at;
        while self.bytes.get(self.at).is_some_and(u8::is_ascii_digit) {
            self.at += 1;
        }
        let text = self
            .bytes
            .get(start..self.at)
            .and_then(|b| std::str::from_utf8(b).ok())
            .unwrap_or("");
        text.parse().map_err(|_| {
            invalid(format!(
                "{METADATA_FILE_NAME}: expected a non-negative integer at byte {start}"
            ))
        })
    }

    /// A string with no escapes: block ids are hexadecimal.
    fn string(&mut self) -> Result<String, Error> {
        self.expect(b'"')?;
        let start = self.at;
        while let Some(b) = self.bytes.get(self.at) {
            match b {
                b'"' => break,
                b'\\' => {
                    return Err(invalid(format!(
                        "{METADATA_FILE_NAME}: an escape in a string at byte {}",
                        self.at
                    )))
                }
                _ => self.at += 1,
            }
        }
        let text = self
            .bytes
            .get(start..self.at)
            .and_then(|b| std::str::from_utf8(b).ok())
            .ok_or_else(|| invalid(format!("{METADATA_FILE_NAME}: a string that is not UTF-8")))?
            .to_string();
        self.expect(b'"')?;
        Ok(text)
    }

    fn lengths(&mut self) -> Result<Vec<u64>, Error> {
        self.expect(b'[')?;
        let mut out = Vec::new();
        if self.peek() == Some(b']') {
            self.at += 1;
            return Ok(out);
        }
        loop {
            out.push(self.number()?);
            match self.peek() {
                Some(b',') => self.at += 1,
                _ => break,
            }
        }
        self.expect(b']')?;
        Ok(out)
    }

    fn entry(&mut self) -> Result<Entry, Error> {
        if self.peek() == Some(b'[') {
            self.at += 1;
            let id = self.string()?;
            self.expect(b',')?;
            let lengths = self.lengths()?;
            self.expect(b']')?;
            Ok(Entry::Block { id, lengths })
        } else {
            Ok(Entry::File(self.number()?))
        }
    }
}

fn parse_entries(text: &[u8]) -> Result<Vec<Entry>, Error> {
    let mut json = Json { bytes: text, at: 0 };
    json.expect(b'[')?;
    let mut entries = Vec::new();
    if json.peek() == Some(b']') {
        json.at += 1;
    } else {
        loop {
            entries.push(json.entry()?);
            match json.peek() {
                Some(b',') => json.at += 1,
                _ => break,
            }
        }
        json.expect(b']')?;
    }
    if json.peek().is_some() {
        return Err(invalid(format!(
            "{METADATA_FILE_NAME}: text after the list"
        )));
    }
    Ok(entries)
}

/// A block id, as the bytes of the hash. Only lowercase hexadecimal, so that the id is
/// also a safe file name.
fn decode_id(id: &str) -> Result<Vec<u8>, Error> {
    let digits = id.as_bytes();
    if digits.is_empty()
        || !digits.len().is_multiple_of(2)
        || !digits
            .iter()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(b))
    {
        return Err(invalid(format!(
            "the block id {id:?} is not lowercase hexadecimal"
        )));
    }
    let nibble = |b: u8| {
        if b.is_ascii_digit() {
            b - b'0'
        } else {
            b - b'a' + 10
        }
    };
    Ok(digits
        .chunks(2)
        .map(|pair| match pair {
            [hi, lo] => (nibble(*hi) << 4) | nibble(*lo),
            _ => 0,
        })
        .collect())
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// A block store on disk and the way to read an object that points to it.
#[derive(Debug, Clone)]
pub struct Store {
    block_dir: PathBuf,
    block_depth: usize,
    algorithm: HashAlgorithm,
}

impl Store {
    /// A store whose blocks are in `block_dir` (`Enviroment.block_dir`), with the hash of
    /// nodo (SHA3-256) and one level of framed blocks, as the reference has by default.
    pub fn new(block_dir: impl Into<PathBuf>) -> Self {
        Store {
            block_dir: block_dir.into(),
            block_depth: 1,
            algorithm: HashAlgorithm::Sha3_256,
        }
    }

    /// How many levels of blocks have their own markers (`Enviroment.block_depth`). The
    /// content of a deeper block goes flat. A receiver reads up to [`MAX_BLOCK_NESTING`].
    pub fn with_block_depth(mut self, block_depth: usize) -> Self {
        self.block_depth = block_depth;
        self
    }

    /// The Buffers of the object at `path`: a multiblock directory, or a plain file (which
    /// is read as bytes, with no check). `read_from_registry`.
    pub fn read_registry(&self, path: impl AsRef<Path>) -> Result<RegistryReader, Error> {
        let path = path.as_ref();
        let mut reader = RegistryReader {
            store: self.clone(),
            stack: Vec::new(),
            done: false,
        };
        if std::fs::metadata(path)?.is_dir() {
            reader.push_dir(path, false, 1, 1)?;
        } else {
            reader.stack.push(Task::File(File::open(path)?));
        }
        Ok(reader)
    }

    /// The Buffers that send the object at `path` as the message `index`: the head, the
    /// Buffers of [`Store::read_registry`], and the separator (`send_file`).
    pub fn read_message(
        &self,
        index: i32,
        path: impl AsRef<Path>,
    ) -> Result<impl Iterator<Item = Result<Buffer, Error>>, Error> {
        let head = Buffer {
            head: Some(Head {
                index,
                partitions: Vec::new(),
            }),
            ..Buffer::default()
        };
        let end = Buffer {
            separator: Some(true),
            ..Buffer::default()
        };
        Ok(std::iter::once(Ok(head))
            .chain(self.read_registry(path)?)
            .chain(std::iter::once(Ok(end))))
    }
}

/// What the reader has still to send, innermost last.
enum Task {
    /// The entries of a multiblock directory that were not read yet.
    Dir {
        dir: PathBuf,
        entries: std::vec::IntoIter<Entry>,
        /// True: send the blocks of this directory flat, with no markers.
        ignore_blocks: bool,
        /// The level of this directory among the framed blocks, from 1.
        depth: usize,
        /// How many directories hold this one, for the limit.
        level: usize,
    },
    /// A file that is being read in chunks.
    File(File),
    /// A block that is a single file: read in chunks, and hashed again as it goes.
    Block(Box<BlockFile>),
    /// A Buffer that is sent as it is: the end marker of a block.
    Marker(Block),
}

/// A block file that was checked, and the hash of what is sent from it.
struct BlockFile {
    file: File,
    hasher: Hasher,
    expected: Vec<u8>,
    id: String,
}

/// The Buffers of one stored object, one at a time. See [`Store::read_registry`]. After an
/// error the iterator ends.
pub struct RegistryReader {
    store: Store,
    stack: Vec<Task>,
    done: bool,
}

impl std::fmt::Debug for RegistryReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegistryReader").finish_non_exhaustive()
    }
}

impl RegistryReader {
    /// Read `_.json` of `dir` and put the directory on the stack.
    fn push_dir(
        &mut self,
        dir: &Path,
        ignore_blocks: bool,
        depth: usize,
        level: usize,
    ) -> Result<(), Error> {
        if level > MAX_BLOCK_NESTING {
            return Err(Error::NestingTooDeep(MAX_BLOCK_NESTING));
        }
        let text = std::fs::read(dir.join(METADATA_FILE_NAME))?;
        let entries = parse_entries(&text)?;
        self.stack.push(Task::Dir {
            dir: dir.to_path_buf(),
            entries: entries.into_iter(),
            ignore_blocks,
            depth,
            level,
        });
        Ok(())
    }

    /// Put the content of the block `id` on the stack (`read_block`): a file, after its
    /// hash was checked, or a multiblock directory.
    fn push_block(
        &mut self,
        id: &str,
        ignore_blocks: bool,
        depth: usize,
        level: usize,
    ) -> Result<(), Error> {
        let expected = decode_id(id)?;
        let path = self.store.block_dir.join(id);
        let metadata = std::fs::metadata(&path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                invalid(format!("the store has no block {id}"))
            } else {
                Error::Io(e)
            }
        })?;
        if metadata.is_dir() {
            return self.push_dir(&path, ignore_blocks, depth, level);
        }
        // Before the first byte leaves: a block that is cut or changed on disk must stop
        // the stream, not go out with its marker and little else. The check and the send
        // use the same handle, so a file that is replaced (renamed over) after the check
        // is not the one that is sent. A file that is written in place after the check is
        // caught by the hash that the send computes: the end of the file is an error, and
        // the end marker of the block does not leave.
        let mut hasher = self.store.algorithm.hasher();
        let mut file = File::open(&path)?;
        let mut piece = vec![0u8; CHUNK_SIZE];
        loop {
            let n = file.read(&mut piece)?;
            if n == 0 {
                break;
            }
            hasher.update(piece.get(..n).unwrap_or_default());
        }
        let actual = hasher.finalize();
        if actual != expected {
            return Err(Error::BlockIdMismatch {
                id: id.to_string(),
                actual: hex(&actual),
            });
        }
        file.seek(SeekFrom::Start(0))?;
        self.stack.push(Task::Block(Box::new(BlockFile {
            file,
            hasher: self.store.algorithm.hasher(),
            expected,
            id: id.to_string(),
        })));
        Ok(())
    }

    fn step(&mut self) -> Result<Option<Buffer>, Error> {
        loop {
            let Some(task) = self.stack.pop() else {
                return Ok(None);
            };
            match task {
                Task::Marker(block) => {
                    return Ok(Some(Buffer {
                        block: Some(block),
                        ..Buffer::default()
                    }))
                }
                Task::Block(mut block) => {
                    let mut chunk = vec![0u8; CHUNK_SIZE];
                    let mut filled = 0;
                    while filled < CHUNK_SIZE {
                        let n = block
                            .file
                            .read(chunk.get_mut(filled..).unwrap_or_default())?;
                        if n == 0 {
                            break;
                        }
                        filled += n;
                    }
                    chunk.truncate(filled);
                    block.hasher.update(&chunk);
                    if filled < CHUNK_SIZE {
                        // The end of the file: what left is what the id names, or the
                        // stream stops before the end marker.
                        let actual = block.hasher.finalize();
                        if actual != block.expected {
                            return Err(Error::BlockIdMismatch {
                                id: block.id,
                                actual: hex(&actual),
                            });
                        }
                        if filled == 0 {
                            continue;
                        }
                        return Ok(Some(Buffer {
                            chunk: Some(chunk),
                            ..Buffer::default()
                        }));
                    }
                    self.stack.push(Task::Block(block));
                    return Ok(Some(Buffer {
                        chunk: Some(chunk),
                        ..Buffer::default()
                    }));
                }
                Task::File(mut file) => {
                    let mut chunk = vec![0u8; CHUNK_SIZE];
                    let mut filled = 0;
                    while filled < CHUNK_SIZE {
                        let n = file.read(chunk.get_mut(filled..).unwrap_or_default())?;
                        if n == 0 {
                            break;
                        }
                        filled += n;
                    }
                    if filled == 0 {
                        continue;
                    }
                    chunk.truncate(filled);
                    // More may follow: the file stays on the stack.
                    self.stack.push(Task::File(file));
                    return Ok(Some(Buffer {
                        chunk: Some(chunk),
                        ..Buffer::default()
                    }));
                }
                Task::Dir {
                    dir,
                    mut entries,
                    ignore_blocks,
                    depth,
                    level,
                } => {
                    let Some(entry) = entries.next() else {
                        continue;
                    };
                    self.stack.push(Task::Dir {
                        dir: dir.clone(),
                        entries,
                        ignore_blocks,
                        depth,
                        level,
                    });
                    match entry {
                        Entry::File(name) => {
                            let file = File::open(dir.join(name.to_string()))?;
                            self.stack.push(Task::File(file));
                        }
                        Entry::Block { id, lengths } => {
                            if ignore_blocks {
                                self.push_block(&id, true, depth, level + 1)?;
                                continue;
                            }
                            let mut marker =
                                block_pointer(&decode_id(&id)?, self.store.algorithm, false);
                            marker.previous_lengths_position = lengths;
                            // The end marker waits under the content of the block.
                            self.stack.push(Task::Marker(marker.clone()));
                            let framed_below = depth < self.store.block_depth;
                            self.push_block(&id, !framed_below, depth + 1, level + 1)?;
                            return Ok(Some(Buffer {
                                block: Some(marker),
                                ..Buffer::default()
                            }));
                        }
                    }
                }
            }
        }
    }
}

impl Iterator for RegistryReader {
    type Item = Result<Buffer, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        match self.step() {
            Ok(Some(buffer)) => Some(Ok(buffer)),
            Ok(None) => {
                self.done = true;
                None
            }
            Err(e) => {
                self.done = true;
                Some(Err(e))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_list_of_the_reference_is_read() {
        let entries = parse_entries(br#"[1, ["ab12", [1, 5]], 2]"#).unwrap();
        assert_eq!(
            entries,
            vec![
                Entry::File(1),
                Entry::Block {
                    id: "ab12".into(),
                    lengths: vec![1, 5]
                },
                Entry::File(2)
            ]
        );
    }

    #[test]
    fn spaces_and_an_empty_list_are_accepted() {
        assert_eq!(parse_entries(b" [ ] \n").unwrap(), vec![]);
        assert!(parse_entries(b"[ [\"ab\" ,\n[ ] ] ]").is_ok());
    }

    #[test]
    fn another_shape_is_refused() {
        for text in [
            &br#"[["ab"]]"#[..],
            br#"[["ab", [1], 7]]"#,
            br#"[-1]"#,
            br#"[1.5]"#,
            br#"["ab"]"#,
            br#"[1,]"#,
            br#"[1] x"#,
            br#"[1"#,
            br#"[["a\"b", [1]]]"#,
            b"",
        ] {
            assert!(
                parse_entries(text).is_err(),
                "{}",
                String::from_utf8_lossy(text)
            );
        }
    }

    #[test]
    fn an_id_is_lowercase_hexadecimal_and_never_a_path() {
        assert_eq!(decode_id("00ff").unwrap(), vec![0, 255]);
        for id in ["", "0", "0G", "AB", "../x", "a/b", "ab "] {
            assert!(decode_id(id).is_err(), "{id:?}");
        }
    }
}
