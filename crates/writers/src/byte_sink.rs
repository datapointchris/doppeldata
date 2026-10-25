//! The bytes of one object on their way to the store. Every format writes into a [`ByteSink`], so a
//! layer that rewrites bytes, such as text compression, wraps one.

use std::collections::VecDeque;
use std::io;
use std::io::Write;
use std::sync::Arc;

use bytes::Bytes;
use object_store::MultipartUpload;
use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use object_store::path::Path;
use sha2::Digest;
use sha2::Sha256;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;

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

/// Bytes one multipart part carries. An object smaller than this is one put. S3 takes parts of
/// 5 MiB and up.
const PART_BYTES: usize = 10 * 1024 * 1024;

/// Part uploads one object keeps in flight. Every worker thread has an object open, so the store
/// sees this many times the thread count.
const PARTS_IN_FLIGHT: usize = 2;

/// The bottom of every sink: it hashes and counts the bytes, then uploads them through object_store,
/// as one put or as a multipart upload once the object outgrows a part. It blocks on `runtime` from
/// the calling thread, so it must never run on a runtime's own thread.
pub(crate) struct ObjectSink {
    store: Arc<dyn ObjectStore>,
    path: Path,
    runtime: Handle,
    pending: Vec<u8>,
    upload: Option<Box<dyn MultipartUpload>>,
    in_flight: VecDeque<JoinHandle<object_store::Result<()>>>,
    hasher: Sha256,
    bytes: u64,
}

impl ObjectSink {
    pub(crate) fn new(store: Arc<dyn ObjectStore>, path: Path, runtime: Handle) -> Self {
        Self {
            store,
            path,
            runtime,
            pending: Vec::new(),
            upload: None,
            in_flight: VecDeque::new(),
            hasher: Sha256::new(),
            bytes: 0,
        }
    }

    fn send_part(&mut self) -> Result<(), WriteError> {
        let part = Bytes::from(std::mem::take(&mut self.pending));
        if self.upload.is_none() {
            self.upload = Some(self.runtime.block_on(self.store.put_multipart(&self.path))?);
        }
        while self.in_flight.len() >= PARTS_IN_FLIGHT {
            self.wait_oldest()?;
        }
        let upload = self.upload.as_mut().expect("the upload started above");
        self.in_flight.push_back(self.runtime.spawn(upload.put_part(part.into())));
        Ok(())
    }

    fn wait_oldest(&mut self) -> Result<(), WriteError> {
        let part = self.in_flight.pop_front().expect("a part is in flight");
        self.runtime.block_on(part).map_err(io::Error::other)??;
        Ok(())
    }

    fn complete(&mut self) -> Result<(), WriteError> {
        if self.upload.is_none() {
            let object = Bytes::from(std::mem::take(&mut self.pending));
            self.runtime.block_on(self.store.put(&self.path, object.into()))?;
            return Ok(());
        }
        if !self.pending.is_empty() {
            self.send_part()?;
        }
        while !self.in_flight.is_empty() {
            self.wait_oldest()?;
        }
        let mut upload = self.upload.take().expect("checked above");
        let completed = self.runtime.block_on(upload.complete());
        if completed.is_err() {
            let _ = self.runtime.block_on(upload.abort());
        }
        completed?;
        Ok(())
    }

    /// Ends a started multipart upload, so the store keeps none of its parts. Parts still uploading
    /// finish first, because S3 may keep a part that lands after the abort.
    fn abort(&mut self) {
        while let Some(part) = self.in_flight.pop_front() {
            let _ = self.runtime.block_on(part);
        }
        if let Some(mut upload) = self.upload.take() {
            let _ = self.runtime.block_on(upload.abort());
        }
    }
}

impl Write for ObjectSink {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.hasher.update(data);
        self.bytes += data.len() as u64;
        self.pending.extend_from_slice(data);
        if self.pending.len() >= PART_BYTES {
            self.send_part().map_err(io::Error::other)?;
        }
        Ok(data.len())
    }

    /// The pending part is sent once it fills or the object finishes, never on a flush, so a format
    /// that flushes often still uploads in whole parts.
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl ByteSink for ObjectSink {
    fn finish(mut self: Box<Self>) -> Result<WrittenObject, WriteError> {
        self.complete()?;
        Ok(WrittenObject {
            bytes: self.bytes,
            sha256: sha256_text(self.hasher.clone().finalize().as_slice()),
        })
    }
}

impl Drop for ObjectSink {
    fn drop(&mut self) {
        self.abort();
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
