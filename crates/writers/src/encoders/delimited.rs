//! CSV and TSV through csv-core, the csv crate's own writer, which sets the line terminator and the
//! quoting style per file where `arrow-csv` cannot. Driving it field by field lets an empty string
//! print quoted where a null prints nothing.

use std::fmt::Write as _;
use std::io;
use std::io::Write as _;

use arrow_array::Array;
use arrow_array::RecordBatch;
use arrow_cast::display::ArrayFormatter;
use arrow_cast::display::FormatOptions;
use arrow_schema::SchemaRef;
use csv_core::WriteResult;
use doppeldata_core::TextCell;
use doppeldata_core::format_settings::DelimitedSettings;
use doppeldata_core::format_settings::LineTerminator;
use doppeldata_core::format_settings::Quoting;
use doppeldata_core::format_settings::TextEncoding;

use super::FileEncoder;
use super::text_compression_level;
use crate::WriteError;
use crate::byte_sink::ByteSink;
use crate::byte_sink::Latin1Sink;
use crate::byte_sink::WrittenObject;

const UTF8_BOM: &[u8] = b"\xEF\xBB\xBF";

/// Bytes gathered before they go to the sink.
const BUFFER_BYTES: usize = 256 * 1024;

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

/// csv-core's writer and the buffer it writes into, flushed to the sink as it fills.
struct RecordWriter {
    writer: csv_core::Writer,
    sink: Box<dyn ByteSink>,
    buffer: Box<[u8]>,
    used: usize,
}

impl RecordWriter {
    fn field(&mut self, mut input: &[u8]) -> io::Result<()> {
        loop {
            let (result, read, wrote) = self.writer.field(input, &mut self.buffer[self.used..]);
            self.used += wrote;
            input = &input[read..];
            match result {
                WriteResult::InputEmpty => return Ok(()),
                WriteResult::OutputFull => self.flush()?,
            }
        }
    }

    fn delimiter(&mut self) -> io::Result<()> {
        loop {
            let (result, wrote) = self.writer.delimiter(&mut self.buffer[self.used..]);
            self.used += wrote;
            match result {
                WriteResult::InputEmpty => return Ok(()),
                WriteResult::OutputFull => self.flush()?,
            }
        }
    }

    /// Ends the record. A record of one field that wrote nothing prints as two quotes, so the line
    /// is not read as blank.
    fn terminator(&mut self) -> io::Result<()> {
        loop {
            let (result, wrote) = self.writer.terminator(&mut self.buffer[self.used..]);
            self.used += wrote;
            match result {
                WriteResult::InputEmpty => return Ok(()),
                WriteResult::OutputFull => self.flush()?,
            }
        }
    }

    /// Bytes csv-core does not see. Only a whole field is written this way, between a delimiter and
    /// the next.
    fn raw(&mut self, bytes: &[u8]) -> io::Result<()> {
        if self.buffer.len() - self.used < bytes.len() {
            self.flush()?;
        }
        self.buffer[self.used..self.used + bytes.len()].copy_from_slice(bytes);
        self.used += bytes.len();
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.sink.write_all(&self.buffer[..self.used])?;
        self.used = 0;
        Ok(())
    }

    fn finish(mut self) -> Result<WrittenObject, WriteError> {
        loop {
            let (result, wrote) = self.writer.finish(&mut self.buffer[self.used..]);
            self.used += wrote;
            match result {
                WriteResult::InputEmpty => break,
                WriteResult::OutputFull => self.flush()?,
            }
        }
        self.flush()?;
        self.sink.finish()
    }
}

pub(super) struct DelimitedEncoder {
    out: RecordWriter,
    null: String,
    /// The two quotes an empty string prints as where a null prints empty, so the two read back
    /// apart. None where nothing is quoted.
    empty_string: Option<[u8; 2]>,
    date_format: Option<String>,
    timestamp_format: Option<String>,
    timestamp_tz_format: Option<String>,
    time_format: Option<String>,
    text: String,
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
        let quote = dialect_byte("quote", settings.quote)?;
        let mut builder = csv_core::WriterBuilder::new();
        builder
            .delimiter(settings.delimiter.map_or(Ok(default_delimiter), |c| dialect_byte("delimiter", c))?)
            .quote(quote)
            .quote_style(match settings.quoting {
                Quoting::Necessary => csv_core::QuoteStyle::Necessary,
                Quoting::Always => csv_core::QuoteStyle::Always,
                Quoting::NonNumeric => csv_core::QuoteStyle::NonNumeric,
                Quoting::Never => csv_core::QuoteStyle::Never,
            })
            .terminator(match settings.line_terminator {
                LineTerminator::Lf => csv_core::Terminator::Any(b'\n'),
                LineTerminator::Crlf => csv_core::Terminator::CRLF,
            });
        match settings.escape {
            Some(escape) => builder.double_quote(false).escape(dialect_byte("escape", escape)?),
            None => builder.double_quote(true),
        };
        let mut encoder = Self {
            out: RecordWriter {
                writer: builder.build(),
                sink,
                buffer: vec![0; BUFFER_BYTES].into_boxed_slice(),
                used: 0,
            },
            null: settings.null.clone(),
            empty_string: (settings.null.is_empty() && settings.quoting != Quoting::Never).then_some([quote, quote]),
            date_format: settings.date_format.clone(),
            timestamp_format: settings.timestamp_format.clone(),
            timestamp_tz_format: settings.timestamp_tz_format.clone(),
            time_format: settings.time_format.clone(),
            text: String::new(),
        };
        if settings.header {
            for (index, field) in schema.fields().iter().enumerate() {
                if index > 0 {
                    encoder.out.delimiter()?;
                }
                encoder.out.field(field.name().as_bytes())?;
            }
            encoder.out.terminator()?;
        }
        Ok(encoder)
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
        let one_column = formatters.len() == 1;
        let mut next_cell = vec![0usize; batch.num_columns()];
        for row in 0..batch.num_rows() {
            for (column, formatter) in formatters.iter().enumerate() {
                if column > 0 {
                    self.out.delimiter()?;
                }
                let column_cells = &cells[column];
                let cursor = &mut next_cell[column];
                if column_cells.get(*cursor).is_some_and(|cell| cell.row as usize == row) {
                    self.out.field(column_cells[*cursor].text.as_bytes())?;
                    *cursor += 1;
                } else if batch.column(column).is_null(row) {
                    if !self.null.is_empty() {
                        self.out.field(self.null.as_bytes())?;
                    }
                } else {
                    self.text.clear();
                    write!(self.text, "{}", formatter.value(row)).map_err(|_| WriteError::Format {
                        column: batch.schema().field(column).name().clone(),
                    })?;
                    match self.empty_string {
                        Some(quotes) if self.text.is_empty() && !one_column => self.out.raw(&quotes)?,
                        _ => self.out.field(self.text.as_bytes())?,
                    }
                }
            }
            self.out.terminator()?;
        }
        Ok(())
    }

    fn finish(self: Box<Self>) -> Result<WrittenObject, WriteError> {
        self.out.finish()
    }
}
