//! Arrow IPC, as the random-access file format or the streaming format.

use arrow_array::RecordBatch;
use arrow_ipc::CompressionType;
use arrow_ipc::writer::FileWriter;
use arrow_ipc::writer::IpcWriteOptions;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::SchemaRef;
use doppeldata_core::TextCell;
use doppeldata_core::format_settings::ArrowSettings;
use doppeldata_core::format_settings::IpcCompression;
use doppeldata_core::format_settings::IpcFormat;

use super::FileEncoder;
use crate::WriteError;
use crate::byte_sink::ByteSink;
use crate::byte_sink::WrittenObject;

pub(super) enum IpcEncoder {
    File(FileWriter<Box<dyn ByteSink>>),
    Stream(StreamWriter<Box<dyn ByteSink>>),
}

impl IpcEncoder {
    pub(super) fn new(settings: &ArrowSettings, schema: &SchemaRef, sink: Box<dyn ByteSink>) -> Result<Self, WriteError> {
        let compression = match settings.compression {
            IpcCompression::None => None,
            IpcCompression::Lz4 => Some(CompressionType::LZ4_FRAME),
            IpcCompression::Zstd => Some(CompressionType::ZSTD),
        };
        let options = IpcWriteOptions::default().try_with_compression(compression)?;
        Ok(match settings.ipc {
            IpcFormat::File => Self::File(FileWriter::try_new_with_options(sink, schema, options)?),
            IpcFormat::Stream => Self::Stream(StreamWriter::try_new_with_options(sink, schema, options)?),
        })
    }
}

impl FileEncoder for IpcEncoder {
    fn write(&mut self, batch: &RecordBatch, _cells: &[Vec<TextCell>]) -> Result<(), WriteError> {
        match self {
            Self::File(writer) => writer.write(batch)?,
            Self::Stream(writer) => writer.write(batch)?,
        }
        Ok(())
    }

    fn finish(self: Box<Self>) -> Result<WrittenObject, WriteError> {
        let sink = match *self {
            Self::File(writer) => writer.into_inner()?,
            Self::Stream(writer) => writer.into_inner()?,
        };
        sink.finish()
    }
}
