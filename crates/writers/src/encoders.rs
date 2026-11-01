//! One encoder per format, each turning record batches into one object's bytes through a
//! [`ByteSink`].

mod delimited;
mod ipc;
mod json;
mod parquet;

use arrow_array::RecordBatch;
use arrow_schema::DataType;
use arrow_schema::SchemaRef;
use doppeldata_core::FileFault;
use doppeldata_core::RowFaults;
use doppeldata_core::ScenarioFault;
use doppeldata_core::TextCell;
use doppeldata_core::format_settings::Output;
use doppeldata_core::format_settings::TextCompression;
use doppeldata_core::format_settings::TextEncoding;
use doppeldata_core::manifest::FaultRecord;
use doppeldata_core::scenario::FaultKind;
use serde_json::Map;
use serde_json::Value;

use crate::WriteError;
use crate::byte_sink::ByteSink;
use crate::byte_sink::GzipSink;
use crate::byte_sink::WrittenObject;
use crate::byte_sink::ZstdSink;

/// Writes one object. `cells` holds, per column of the batch, the text cells to print in place of
/// the null the batch holds, each placed by its row within the batch. `faults` places the rows
/// `malformed_rows` breaks. Dropping an encoder unfinished leaves nothing of its object.
pub(crate) trait FileEncoder: Send {
    fn write(&mut self, batch: &RecordBatch, cells: &[Vec<TextCell>], faults: &RowFaults) -> Result<(), WriteError>;
    fn finish(self: Box<Self>) -> Result<Finished, WriteError>;
}

/// A finished object, and every fault its bytes hold: the file's own first, then each row's in
/// the order written. Each record's `file` is left for the caller to fill.
pub(crate) struct Finished {
    pub(crate) object: WrittenObject,
    pub(crate) faults: Vec<FaultRecord>,
}

impl From<WrittenObject> for Finished {
    fn from(object: WrittenObject) -> Self {
        Self {
            object,
            faults: Vec::new(),
        }
    }
}

/// The record of a fault at `line` of dataset row `row`, its file left empty.
pub(crate) fn fault_record(
    kind: FaultKind,
    line: Option<u64>,
    row: Option<u64>,
    column: Option<&str>,
    detail: Map<String, Value>,
) -> FaultRecord {
    FaultRecord {
        file: String::new(),
        line,
        row,
        kind: kind.into(),
        column: column.map(String::from),
        detail: detail.into(),
    }
}

/// The file's columns a row fault of each class may land in.
pub(crate) struct FaultColumns {
    /// String columns.
    pub(crate) text: Vec<usize>,
    /// Columns a reader parses as a type other than a string.
    pub(crate) typed: Vec<usize>,
    /// Number and boolean columns.
    pub(crate) scalar: Vec<usize>,
}

impl FaultColumns {
    pub(crate) fn of(schema: &SchemaRef) -> Self {
        let mut columns = Self {
            text: Vec::new(),
            typed: Vec::new(),
            scalar: Vec::new(),
        };
        for (index, field) in schema.fields().iter().enumerate() {
            let data_type = field.data_type();
            if is_text(data_type) {
                columns.text.push(index);
            } else if !data_type.is_nested() {
                columns.typed.push(index);
            }
            if data_type.is_numeric() || *data_type == DataType::Boolean {
                columns.scalar.push(index);
            }
        }
        columns
    }

    /// The column of `among` that `pick` chooses, or None from an empty list.
    pub(crate) fn chosen(among: &[usize], pick: u64) -> Option<usize> {
        (!among.is_empty()).then(|| among[(pick % among.len() as u64) as usize])
    }
}

pub(crate) fn is_text(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)
}

/// Why a write under `malformed_rows` cannot place `kind` in a file of `output` holding `schema`,
/// or None where it can.
pub(crate) fn fault_refusal(output: &Output, schema: &SchemaRef, kind: FaultKind) -> Option<(ScenarioFault, String)> {
    use FaultKind as K;
    let format = match output {
        Output::Csv(_) => "csv",
        Output::Tsv(_) => "tsv",
        Output::Json(_) => "json",
        Output::Ndjson(_) => "ndjson",
        Output::Parquet(_) => "parquet",
        Output::Arrow(_) => "arrow",
    };
    if !kind.formats().hold(output) {
        return Some((
            ScenarioFault::OpNotForFormat,
            format!("{} is not a fault a {format} file can hold", kind.name()),
        ));
    }
    if let Output::Csv(settings) | Output::Tsv(settings) = output {
        let latin1 = settings.encoding == TextEncoding::Latin1;
        let clash = match kind {
            K::InvalidUtf8 | K::Latin1File if latin1 => Some("the files are written latin1, where every byte is a character"),
            K::Bom if settings.bom => Some("the writer already starts every file with a byte order mark"),
            K::Bom if latin1 => Some("a byte order mark is UTF-8's, and the files are written latin1"),
            K::HeaderDrift if !settings.header => Some("the files are written with no header"),
            _ => None,
        };
        if let Some(clash) = clash {
            return Some((ScenarioFault::OpNotForFormat, format!("{} cannot be written: {clash}", kind.name())));
        }
    }
    let columns = FaultColumns::of(schema);
    let fields = schema.fields();
    let has = |test: &dyn Fn(&DataType) -> bool| fields.iter().any(|field| test(field.data_type()));
    let wanting = match kind {
        K::RaggedShort if fields.len() < 2 => Some("a file of at least two columns"),
        K::EmbeddedNewline if columns.text.is_empty() => Some("a string column"),
        K::TypeInvalid if columns.typed.is_empty() => Some("a column of a type other than string"),
        K::JsonTypeFlip if columns.scalar.is_empty() => Some("a number or boolean column"),
        K::ParquetTimestampUnitDrift | K::ParquetInt96 if !has(&|t| matches!(t, DataType::Timestamp(..))) => Some("a timestamp column"),
        K::ParquetDecimalScaleDrift
            if !has(&|t| matches!(t, DataType::Decimal128(p, _) if *p < 38) || matches!(t, DataType::Decimal256(p, _) if *p < 76)) =>
        {
            Some("a decimal column below the widest precision")
        }
        _ => None,
    };
    wanting.map(|wanting| {
        let message = format!("{} needs {wanting} in the files, and they hold none", kind.name());
        (ScenarioFault::OpNotForColumn, message)
    })
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

/// What a write under `malformed_rows` breaks in one object: the fault of the whole file, if it
/// takes one. Without the op an encoder is opened with none of this, and counts no lines.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Faulting {
    pub(crate) file: Option<FileFault>,
}

/// An encoder writing `schema`'s batches as `output` into `sink`.
pub(crate) fn open(
    output: &Output,
    schema: &SchemaRef,
    sink: Box<dyn ByteSink>,
    faulting: Option<Faulting>,
) -> Result<Box<dyn FileEncoder>, WriteError> {
    let file_fault = faulting.and_then(|faulting| faulting.file);
    let layout_mixed = file_fault.is_some_and(|fault| fault.kind == FaultKind::JsonLayoutMix);
    Ok(match output {
        Output::Parquet(settings) => Box::new(parquet::ParquetEncoder::new(settings, schema, sink, file_fault)?),
        Output::Arrow(settings) => Box::new(ipc::IpcEncoder::new(settings, schema, sink)?),
        Output::Csv(settings) => {
            let sink = compressed(sink, settings.compression, settings.compression_level)?;
            Box::new(delimited::DelimitedEncoder::new(settings, b',', schema, sink, faulting)?)
        }
        Output::Tsv(settings) => {
            let sink = compressed(sink, settings.compression, settings.compression_level)?;
            Box::new(delimited::DelimitedEncoder::new(settings, b'\t', schema, sink, faulting)?)
        }
        Output::Json(settings) | Output::Ndjson(settings) => {
            let sink = compressed(sink, settings.compression, settings.compression_level)?;
            let layout = match (matches!(output, Output::Json(_)), layout_mixed) {
                (true, false) | (false, true) => json::Layout::Array,
                (false, false) | (true, true) => json::Layout::Lines,
            };
            Box::new(json::JsonEncoder::new(settings, schema, sink, layout, faulting)?)
        }
    })
}

/// The level a text codec runs at: `level`, or the codec's default when none is given. A level
/// outside the codec's range is refused.
fn text_compression_level(compression: TextCompression, level: Option<i32>) -> Result<i32, WriteError> {
    let Some((range, default)) = compression.levels() else {
        return Ok(0);
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
