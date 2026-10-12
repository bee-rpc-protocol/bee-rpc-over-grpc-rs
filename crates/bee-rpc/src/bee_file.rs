//! The `.bee` file format (`client.write_to_file`, `reader.read_bee_file`).
//!
//! A sequence of records. Each record is a 4-byte big-endian length, then that
//! many bytes of one serialized Buffer. The file ends after the last record.

use std::io::{ErrorKind, Read, Write};

use prost::Message;

use crate::buffer::Buffer;
use crate::Error;

/// The size of the length prefix of a record.
const LEN_SIZE: usize = 4;

/// The most bytes that the reader reserves before it reads a record. A larger
/// record grows the buffer while it arrives, so a false length does not make the
/// reader allocate memory that the file does not hold.
const MAX_RESERVE: usize = 1024 * 1024;

/// Write `buffers` as .bee records, then flush `writer`.
///
/// Each record is two `write_all` calls (the length, then the Buffer), so pass a
/// buffered writer (`BufWriter`) if `writer` is a file or a socket.
///
/// A Buffer of more than `u32::MAX` bytes does not fit the length prefix. It is
/// [`Error::Io`] with [`ErrorKind::InvalidInput`], and nothing of that Buffer is
/// written.
pub fn write_bee<W: Write>(
    writer: &mut W,
    buffers: impl IntoIterator<Item = Buffer>,
) -> Result<(), Error> {
    for buffer in buffers {
        let record = buffer.encode_to_vec();
        let len = u32::try_from(record.len()).map_err(|_| {
            std::io::Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "a Buffer of {} bytes does not fit a .bee record",
                    record.len()
                ),
            )
        })?;
        writer.write_all(&len.to_be_bytes())?;
        writer.write_all(&record)?;
    }
    writer.flush()?;
    Ok(())
}

/// Read .bee records. A cut length or a cut record is [`Error::Decode`]. A record
/// that is not a Buffer is [`Error::Decode`].
pub fn read_bee<R: Read>(reader: R) -> BeeReader<R> {
    BeeReader {
        reader,
        done: false,
    }
}

/// Iterator over the Buffers of a .bee file.
///
/// After an error, the iterator ends.
pub struct BeeReader<R> {
    reader: R,
    done: bool,
}

impl<R: Read> BeeReader<R> {
    /// Read the next record. `Ok(None)` at the end of the file.
    fn read_record(&mut self) -> Result<Option<Buffer>, Error> {
        let mut len_bytes = [0u8; LEN_SIZE];
        let got = read_full(&mut self.reader, &mut len_bytes)?;
        if got == 0 {
            return Ok(None);
        }
        if got != LEN_SIZE {
            return Err(Error::Decode(format!(
                "the length of a record is cut: {got} of {LEN_SIZE} bytes"
            )));
        }
        let len = u32::from_be_bytes(len_bytes);
        let mut record =
            Vec::with_capacity(usize::try_from(len).map_or(MAX_RESERVE, |l| l.min(MAX_RESERVE)));
        let read = (&mut self.reader)
            .take(u64::from(len))
            .read_to_end(&mut record)?;
        if read as u64 != u64::from(len) {
            return Err(Error::Decode(format!(
                "a record is cut: {read} of {len} bytes"
            )));
        }
        Buffer::decode(record.as_slice())
            .map(Some)
            .map_err(|e| Error::Decode(e.to_string()))
    }
}

/// Fill `buf` from `reader` until it is full or the reader ends. Returns the
/// number of bytes read.
fn read_full<R: Read>(reader: &mut R, buf: &mut [u8]) -> Result<usize, Error> {
    let mut filled = 0;
    while let Some(rest) = buf.get_mut(filled..) {
        if rest.is_empty() {
            break;
        }
        match reader.read(rest) {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(e) if e.kind() == ErrorKind::Interrupted => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(filled)
}

impl<R: Read> Iterator for BeeReader<R> {
    type Item = Result<Buffer, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        match self.read_record() {
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
