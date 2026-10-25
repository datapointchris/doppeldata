//! The bytes of one object on their way to the store. Every format writes into a [`ByteSink`], so a
//! layer that rewrites bytes, such as text compression or a deliberate malformation, wraps one.

use std::io;
use std::io::Write;
use std::sync::Arc;

use bytes::Bytes;
use object_store::ObjectStore;
use object_store::buffered::BufWriter;
use object_store::path::Path;
use sha2::Digest;
use sha2::Sha256;
use tokio::io::AsyncWriteExt;
use tokio::runtime::Handle;

use crate::WriteError;

/// What a finished object holds, as the manifest records it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WrittenObject {
    pub bytes: u64,
    /// `sha256:` and the hex SHA-256 of the object's bytes.
    pub sha256: String,
}

/// A writer that completes one object. `finish` makes the object whole in the store. Dropping a
/// sink unfinished leaves nothing of its object, a started multipart upload included.
pub trait ByteSink: Write + Send {
    fn finish(self: Box<Self>) -> Result<WrittenObject, WriteError>;
}

/// Bytes handed to the store at a time. object_store's `BufWriter` gathers them into one put, or into
/// multipart parts once an object outgrows its capacity.
const CHUNK_BYTES: usize = 1024 * 1024;

/// Part uploads one object keeps in flight. Every worker thread has an object open, so the store
/// sees this many times the thread count.
const PARTS_IN_FLIGHT: usize = 2;

/// The bottom of every sink: it hashes and counts the bytes, then uploads them through object_store.
/// It blocks on `runtime` from the calling thread, so it must never run on a runtime's own thread.
pub(crate) struct ObjectSink {
    runtime: Handle,
    writer: BufWriter,
    pending: Vec<u8>,
    hasher: Sha256,
    bytes: u64,
    /// Set once `finish` starts the upload's completion, after which object_store refuses an abort.
    completing: bool,
}

impl ObjectSink {
    pub(crate) fn new(store: Arc<dyn ObjectStore>, path: Path, runtime: Handle) -> Self {
        Self {
            runtime,
            writer: BufWriter::new(store, path).with_max_concurrency(PARTS_IN_FLIGHT),
            pending: Vec::with_capacity(CHUNK_BYTES),
            hasher: Sha256::new(),
            bytes: 0,
            completing: false,
        }
    }

    fn send_pending(&mut self) -> io::Result<()> {
        let chunk = Bytes::from(std::mem::replace(&mut self.pending, Vec::with_capacity(CHUNK_BYTES)));
        self.runtime.block_on(self.writer.put(chunk)).map_err(io::Error::other)
    }
}

impl Write for ObjectSink {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.hasher.update(data);
        self.bytes += data.len() as u64;
        self.pending.extend_from_slice(data);
        if self.pending.len() >= CHUNK_BYTES {
            self.send_pending()?;
        }
        Ok(data.len())
    }

    /// The pending chunk is sent once it fills or the object finishes, never on a flush, so a format
    /// that flushes often still uploads in large parts.
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl ByteSink for ObjectSink {
    fn finish(mut self: Box<Self>) -> Result<WrittenObject, WriteError> {
        if !self.pending.is_empty() {
            self.send_pending()?;
        }
        self.completing = true;
        let runtime = self.runtime.clone();
        runtime.block_on(self.writer.shutdown())?;
        Ok(WrittenObject {
            bytes: self.bytes,
            sha256: sha256_text(self.hasher.clone().finalize().as_slice()),
        })
    }
}

impl Drop for ObjectSink {
    fn drop(&mut self) {
        if !self.completing {
            let runtime = self.runtime.clone();
            let _ = runtime.block_on(self.writer.abort());
        }
    }
}

pub(crate) fn sha256_text(hash: &[u8]) -> String {
    let mut text = String::with_capacity(7 + 2 * hash.len());
    text.push_str("sha256:");
    for byte in hash {
        text.push_str(&format!("{byte:02x}"));
    }
    text
}

/// Gzip over a sink. The header carries no file name and a zero modification time, so the bytes are
/// a function of the input alone.
pub(crate) struct GzipSink(flate2::write::GzEncoder<Box<dyn ByteSink>>);

impl GzipSink {
    pub(crate) fn new(inner: Box<dyn ByteSink>, level: u32) -> Self {
        Self(flate2::GzBuilder::new().mtime(0).write(inner, flate2::Compression::new(level)))
    }
}

impl Write for GzipSink {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.0.write(data)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl ByteSink for GzipSink {
    fn finish(self: Box<Self>) -> Result<WrittenObject, WriteError> {
        self.0.finish()?.finish()
    }
}

/// Zstandard over a sink, as one frame.
pub(crate) struct ZstdSink(zstd::stream::write::Encoder<'static, Box<dyn ByteSink>>);

impl ZstdSink {
    pub(crate) fn new(inner: Box<dyn ByteSink>, level: i32) -> io::Result<Self> {
        Ok(Self(zstd::stream::write::Encoder::new(inner, level)?))
    }
}

impl Write for ZstdSink {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.0.write(data)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl ByteSink for ZstdSink {
    fn finish(self: Box<Self>) -> Result<WrittenObject, WriteError> {
        self.0.finish()?.finish()
    }
}

/// UTF-8 text re-encoded as windows-1252. A character it cannot hold becomes `?`, as Python's
/// `errors="replace"` writes it.
pub(crate) struct Latin1Sink {
    inner: Box<dyn ByteSink>,
    encoder: encoding_rs::Encoder,
    /// The start of a character split across two writes, held until the rest arrives.
    carry: Vec<u8>,
    out: Vec<u8>,
}

impl Latin1Sink {
    pub(crate) fn new(inner: Box<dyn ByteSink>) -> Self {
        Self {
            inner,
            encoder: encoding_rs::WINDOWS_1252.new_encoder(),
            carry: Vec::new(),
            out: Vec::new(),
        }
    }

    fn encode(&mut self, text: &str, last: bool) -> io::Result<()> {
        self.out.clear();
        self.out.reserve(text.len() + 16);
        let mut rest = text;
        loop {
            let (result, read) = self.encoder.encode_from_utf8_to_vec_without_replacement(rest, &mut self.out, last);
            rest = &rest[read..];
            match result {
                encoding_rs::EncoderResult::InputEmpty => break,
                encoding_rs::EncoderResult::OutputFull => self.out.reserve(rest.len() + 16),
                encoding_rs::EncoderResult::Unmappable(_) => self.out.push(b'?'),
            }
        }
        self.inner.write_all(&self.out)
    }
}

impl Write for Latin1Sink {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        let joined;
        let bytes = if self.carry.is_empty() {
            data
        } else {
            self.carry.extend_from_slice(data);
            joined = std::mem::take(&mut self.carry);
            &joined
        };
        let valid = match std::str::from_utf8(bytes) {
            Ok(text) => text.len(),
            Err(error) if error.error_len().is_none() => error.valid_up_to(),
            Err(error) => return Err(io::Error::new(io::ErrorKind::InvalidData, error)),
        };
        let (text, tail) = bytes.split_at(valid);
        let tail = tail.to_vec();
        self.encode(std::str::from_utf8(text).expect("checked above"), false)?;
        self.carry = tail;
        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl ByteSink for Latin1Sink {
    fn finish(mut self: Box<Self>) -> Result<WrittenObject, WriteError> {
        if !self.carry.is_empty() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "text ended inside a UTF-8 character").into());
        }
        self.encode("", true)?;
        self.inner.finish()
    }
}
