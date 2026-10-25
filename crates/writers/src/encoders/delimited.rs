//! CSV and TSV through the `csv` crate, which sets the line terminator and the quoting style per
//! file where `arrow-csv` cannot. A thin adapter turns each batch into records.

use std::fmt::Write as _;
use std::io::Write as _;

use arrow_array::Array;
use arrow_array::RecordBatch;
use arrow_cast::display::ArrayFormatter;
use arrow_cast::display::FormatOptions;
use arrow_schema::SchemaRef;
use doppeldata_core::TextCell;
use doppeldata_core::manifest::DelimitedSettings;
use doppeldata_core::manifest::LineTerminator;
use doppeldata_core::manifest::Quoting;
use doppeldata_core::manifest::TextEncoding;

use super::FileEncoder;
use super::text_compression_level;
use crate::WriteError;
use crate::byte_sink::ByteSink;
use crate::byte_sink::Latin1Sink;
use crate::byte_sink::WrittenObject;

const UTF8_BOM: &[u8] = b"\xEF\xBB\xBF";

/// A dialect character, which must be one ASCII byte other than a line break.
fn dialect_byte(name: &str, character: char) -> Result<u8, WriteError> {
    match u8::try_from(character) {
        Ok(byte) if byte.is_ascii() && byte != b'\n' && byte != b'\r' => Ok(byte),
        _ => Err(WriteError::Settings(format!(
            "{name} {character:?} is not one ASCII character other than a line break"
        ))),
    }
}

pub(super) fn check(settings: &DelimitedSettings, default_delimiter: u8, schema: &SchemaRef) -> Result<(), WriteError> {
    let delimiter = settings
        .delimiter
        .map_or(Ok(default_delimiter), |character| dialect_byte("delimiter", character))?;
    let quote = dialect_byte("quote", settings.quote)?;
    if let Some(escape) = settings.escape {
        dialect_byte("escape", escape)?;
    }
    if delimiter == quote {
        return Err(WriteError::Settings("the delimiter and the quote are the same character".into()));
    }
    if settings.bom && settings.encoding != TextEncoding::Utf8 {
        return Err(WriteError::Settings("a byte order mark is written only for UTF-8".into()));
    }
    text_compression_level(settings.compression, settings.compression_level)?;
    for field in schema.fields() {
        if field.data_type().is_nested() {
            return Err(WriteError::UnsupportedColumn {
                column: field.name().clone(),
                data_type: field.data_type().clone(),
                format: "csv",
            });
        }
    }
    Ok(())
}

pub(super) struct DelimitedEncoder {
    writer: csv::Writer<Box<dyn ByteSink>>,
    null: String,
    date_format: Option<String>,
    timestamp_format: Option<String>,
    timestamp_tz_format: Option<String>,
    time_format: Option<String>,
    field: String,
}

impl DelimitedEncoder {
    pub(super) fn new(
        settings: &DelimitedSettings,
        default_delimiter: u8,
        schema: &SchemaRef,
        sink: Box<dyn ByteSink>,
    ) -> Result<Self, WriteError> {
        let mut sink: Box<dyn ByteSink> = match settings.encoding {
            TextEncoding::Utf8 => sink,
            TextEncoding::Latin1 => Box::new(Latin1Sink::new(sink)),
        };
        if settings.bom {
            sink.write_all(UTF8_BOM)?;
        }
        let mut builder = csv::WriterBuilder::new();
        builder
            .has_headers(false)
            .delimiter(settings.delimiter.map_or(Ok(default_delimiter), |c| dialect_byte("delimiter", c))?)
            .quote(dialect_byte("quote", settings.quote)?)
            .quote_style(match settings.quoting {
                Quoting::Necessary => csv::QuoteStyle::Necessary,
                Quoting::Always => csv::QuoteStyle::Always,
                Quoting::NonNumeric => csv::QuoteStyle::NonNumeric,
                Quoting::Never => csv::QuoteStyle::Never,
            })
            .terminator(match settings.line_terminator {
                LineTerminator::Lf => csv::Terminator::Any(b'\n'),
                LineTerminator::Crlf => csv::Terminator::CRLF,
            })
            .buffer_capacity(256 * 1024);
        match settings.escape {
            Some(escape) => builder.double_quote(false).escape(dialect_byte("escape", escape)?),
            None => builder.double_quote(true),
        };
        let mut writer = builder.from_writer(sink);
        if settings.header {
            writer.write_record(schema.fields().iter().map(|field| field.name().as_bytes()))?;
        }
        Ok(Self {
            writer,
            null: settings.null.clone(),
            date_format: settings.date_format.clone(),
            timestamp_format: settings.timestamp_format.clone(),
            timestamp_tz_format: settings.timestamp_tz_format.clone(),
            time_format: settings.time_format.clone(),
            field: String::new(),
        })
    }
}

impl FileEncoder for DelimitedEncoder {
    fn write(&mut self, batch: &RecordBatch, cells: &[Vec<TextCell>]) -> Result<(), WriteError> {
        let options = FormatOptions::default()
            .with_date_format(self.date_format.as_deref())
            .with_datetime_format(self.timestamp_format.as_deref())
            .with_timestamp_format(self.timestamp_format.as_deref())
            .with_timestamp_tz_format(self.timestamp_tz_format.as_deref())
            .with_time_format(self.time_format.as_deref());
        let formatters = batch
            .columns()
            .iter()
            .map(|column| ArrayFormatter::try_new(column.as_ref(), &options))
            .collect::<Result<Vec<_>, _>>()?;
        let mut next_cell = vec![0usize; batch.num_columns()];
        for row in 0..batch.num_rows() {
            for (column, formatter) in formatters.iter().enumerate() {
                let column_cells = &cells[column];
                let cursor = &mut next_cell[column];
                if column_cells.get(*cursor).is_some_and(|cell| cell.row as usize == row) {
                    self.writer.write_field(&column_cells[*cursor].text)?;
                    *cursor += 1;
                } else if batch.column(column).is_null(row) {
                    self.writer.write_field(&self.null)?;
                } else {
                    self.field.clear();
                    write!(self.field, "{}", formatter.value(row)).map_err(|_| WriteError::Format {
                        column: batch.schema().field(column).name().clone(),
                    })?;
                    self.writer.write_field(&self.field)?;
                }
            }
            self.writer.write_record(None::<&[u8]>)?;
        }
        Ok(())
    }

    fn finish(self: Box<Self>) -> Result<WrittenObject, WriteError> {
        let sink = self.writer.into_inner().map_err(|error| WriteError::Io(error.into_error()))?;
        sink.finish()
    }
}
