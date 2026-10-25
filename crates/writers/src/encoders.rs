//! One encoder per format, each turning record batches into one object's bytes through a
//! [`ByteSink`].

mod delimited;
mod ipc;
mod json;
mod parquet;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use doppeldata_core::TextCell;
use doppeldata_core::manifest::Output;
use doppeldata_core::manifest::TextCompression;

use crate::WriteError;
use crate::byte_sink::ByteSink;
use crate::byte_sink::GzipSink;
use crate::byte_sink::WrittenObject;
use crate::byte_sink::ZstdSink;

/// Writes one object. `cells` holds, per column of the batch, the text cells to print in place of
/// the null the batch holds, each placed by its row within the batch. Dropping an encoder
/// unfinished leaves nothing of its object.
pub(crate) trait FileEncoder: Send {
    fn write(&mut self, batch: &RecordBatch, cells: &[Vec<TextCell>]) -> Result<(), WriteError>;
    fn finish(self: Box<Self>) -> Result<WrittenObject, WriteError>;
}

/// Whether `output` prints text cells. Arrow and Parquet keep the null the batch holds.
pub(crate) fn prints_text_cells(output: &Output) -> bool {
    !matches!(output, Output::Parquet(_) | Output::Arrow(_))
}

/// Refuses settings no object could be written with, before any is opened.
pub(crate) fn check(output: &Output, schema: &SchemaRef) -> Result<(), WriteError> {
    match output {
        Output::Parquet(settings) => parquet::check(settings, schema),
        Output::Arrow(_) => Ok(()),
        Output::Csv(settings) => delimited::check(settings, b',', schema),
        Output::Tsv(settings) => delimited::check(settings, b'\t', schema),
        Output::Json(settings) | Output::Ndjson(settings) => {
            text_compression_level(settings.compression, settings.compression_level).map(|_| ())
        }
    }
}

/// An encoder writing `schema`'s batches as `output` into `sink`.
pub(crate) fn open(output: &Output, schema: &SchemaRef, sink: Box<dyn ByteSink>) -> Result<Box<dyn FileEncoder>, WriteError> {
    Ok(match output {
        Output::Parquet(settings) => Box::new(parquet::ParquetEncoder::new(settings, schema, sink)?),
        Output::Arrow(settings) => Box::new(ipc::IpcEncoder::new(settings, schema, sink)?),
        Output::Csv(settings) => {
            let sink = compressed(sink, settings.compression, settings.compression_level)?;
            Box::new(delimited::DelimitedEncoder::new(settings, b',', schema, sink)?)
        }
        Output::Tsv(settings) => {
            let sink = compressed(sink, settings.compression, settings.compression_level)?;
            Box::new(delimited::DelimitedEncoder::new(settings, b'\t', schema, sink)?)
        }
        Output::Json(settings) => {
            let sink = compressed(sink, settings.compression, settings.compression_level)?;
            Box::new(json::JsonEncoder::new(settings, schema, sink, json::Layout::Array)?)
        }
        Output::Ndjson(settings) => {
            let sink = compressed(sink, settings.compression, settings.compression_level)?;
            Box::new(json::JsonEncoder::new(settings, schema, sink, json::Layout::Lines)?)
        }
    })
}

/// The level a text codec runs at: 0 to 9 for gzip, defaulting to 6, and 1 to 22 for zstd,
/// defaulting to 3, as each library does.
fn text_compression_level(compression: TextCompression, level: Option<i32>) -> Result<i32, WriteError> {
    let (range, default) = match compression {
        TextCompression::None => return Ok(0),
        TextCompression::Gzip => (0..=9, 6),
        TextCompression::Zstd => (1..=22, 3),
    };
    let level = level.unwrap_or(default);
    if range.contains(&level) {
        Ok(level)
    } else {
        Err(WriteError::Settings(format!(
            "compression_level {level} is outside {}..={} for {compression:?}",
            range.start(),
            range.end()
        )))
    }
}

fn compressed(sink: Box<dyn ByteSink>, compression: TextCompression, level: Option<i32>) -> Result<Box<dyn ByteSink>, WriteError> {
    let level = text_compression_level(compression, level)?;
    Ok(match compression {
        TextCompression::None => sink,
        TextCompression::Gzip => Box::new(GzipSink::new(sink, level as u32)),
        TextCompression::Zstd => Box::new(ZstdSink::new(sink, level)?),
    })
}
