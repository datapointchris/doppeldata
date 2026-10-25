//! CSV and TSV through csv-core, the csv crate's own writer, which sets the line terminator and the
//! quoting style per file where `arrow-csv` cannot. Driving it field by field lets an empty string
//! print quoted where a null prints nothing, and lets `malformed_rows` break a record on purpose.

use std::fmt::Write as _;
use std::io;
use std::io::Write as _;

use arrow_array::Array;
use arrow_array::RecordBatch;
use arrow_cast::display::ArrayFormatter;
use arrow_cast::display::FormatOptions;
use arrow_schema::SchemaRef;
use csv_core::WriteResult;
use doppeldata_core::LineEnd;
use doppeldata_core::RowFault;
use doppeldata_core::RowFaults;
use doppeldata_core::TextCell;
use doppeldata_core::format_settings::DelimitedSettings;
use doppeldata_core::format_settings::LineTerminator;
use doppeldata_core::format_settings::Quoting;
use doppeldata_core::format_settings::TextEncoding;
use doppeldata_core::manifest::FaultRecord;
use doppeldata_core::scenario::FaultKind;
use serde_json::Map;
use serde_json::Value;
use serde_json::json;

use super::FaultColumns;
use super::Faulting;
use super::FileEncoder;
use super::Finished;
use super::fault_record;
use super::text_compression_level;
use crate::WriteError;
use crate::byte_sink::ByteSink;
use crate::byte_sink::Latin1Sink;

const UTF8_BOM: &[u8] = b"\xEF\xBB\xBF";

/// Bytes gathered before they go to the sink.
const BUFFER_BYTES: usize = 256 * 1024;

/// The room csv-core is always left to write into, past which a held record grows the buffer.
const MIN_ROOM: usize = 64;

/// DuckDB's default `max_line_size`, which a `line_too_long` record passes.
const DUCKDB_MAX_LINE_SIZE: usize = 2_097_152;

/// The head of a file doppeldata's profiler guesses the encoding from. One invalid UTF-8 byte
/// there reads the file as Latin-1, where no row is rejected, so no `invalid_utf8` starts in it.
const ENCODING_GUESSED_FROM_BYTES: u64 = 64 * 1024;

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

/// The line breaks in `bytes`: each LF, and each CR not followed by one.
fn breaks(bytes: &[u8]) -> u64 {
    let mut count = 0;
    for (at, &byte) in bytes.iter().enumerate() {
        if byte == b'\n' || (byte == b'\r' && bytes.get(at + 1) != Some(&b'\n')) {
            count += 1;
        }
    }
    count
}

/// Where the record being written starts and where each of its fields does, kept while the file
/// is to end in a cut record.
#[derive(Default)]
struct HeldRecord {
    start: u64,
    fields: Vec<u64>,
    /// Where its terminator starts.
    content_end: u64,
}

/// csv-core's writers and the buffer they write into, flushed to the sink as it fills.
struct RecordWriter {
    /// One writer per line break a record may end in, the file's own first. Only a file mixing
    /// line breaks holds more than one.
    writers: Vec<(LineEnd, csv_core::Writer)>,
    current: usize,
    sink: Box<dyn ByteSink>,
    buffer: Vec<u8>,
    used: usize,
    /// The bytes flushed before `buffer[0]`.
    flushed: u64,
    /// Whether line breaks are counted, which only a write under `malformed_rows` reads.
    counting: bool,
    lines: u64,
    /// The records ending in LF, CRLF and CR.
    ends: [u64; 3],
    /// Whether each record stays in the buffer until the next starts, so the file's last can be
    /// cut.
    hold: bool,
    held: HeldRecord,
}

impl RecordWriter {
    fn position(&self) -> u64 {
        self.flushed + self.used as u64
    }

    /// Starts a record that ends in `end`.
    fn begin_record(&mut self, end: LineEnd) {
        self.current = self.writers.iter().position(|(own, _)| *own == end).unwrap_or(0);
        if self.hold {
            let start = self.position();
            self.held = HeldRecord {
                start,
                fields: vec![start],
                content_end: start,
            };
        }
    }

    fn field(&mut self, mut input: &[u8]) -> io::Result<()> {
        if self.counting {
            self.lines += breaks(input);
        }
        loop {
            let used = self.used;
            let (result, read, wrote) = self.writers[self.current].1.field(input, &mut self.buffer[used..]);
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
            let used = self.used;
            let (result, wrote) = self.writers[self.current].1.delimiter(&mut self.buffer[used..]);
            self.used += wrote;
            match result {
                WriteResult::InputEmpty => break,
                WriteResult::OutputFull => self.flush()?,
            }
        }
        if self.hold {
            let at = self.position();
            self.held.fields.push(at);
        }
        Ok(())
    }

    /// Ends the record. A record of one field that wrote nothing prints as two quotes, so the line
    /// is not read as blank.
    fn terminator(&mut self) -> io::Result<()> {
        if self.hold {
            self.held.content_end = self.position();
        }
        loop {
            let used = self.used;
            let (result, wrote) = self.writers[self.current].1.terminator(&mut self.buffer[used..]);
            self.used += wrote;
            match result {
                WriteResult::InputEmpty => break,
                WriteResult::OutputFull => self.flush()?,
            }
        }
        if self.counting {
            self.lines += 1;
            self.ends[self.writers[self.current].0 as usize] += 1;
        }
        Ok(())
    }

    /// Bytes csv-core does not see. Only a whole field is written this way, between a delimiter and
    /// the next.
    fn raw(&mut self, bytes: &[u8]) -> io::Result<()> {
        if self.counting {
            self.lines += breaks(bytes);
        }
        if self.buffer.len() - self.used < bytes.len() {
            self.flush()?;
        }
        if self.buffer.len() - self.used < bytes.len() {
            self.buffer.resize(self.used + bytes.len(), 0);
        }
        self.buffer[self.used..self.used + bytes.len()].copy_from_slice(bytes);
        self.used += bytes.len();
        Ok(())
    }

    /// Sends the buffer to the sink, all but a held record, and grows it where that record leaves
    /// csv-core too little room.
    fn flush(&mut self) -> io::Result<()> {
        let keep_from = match self.hold {
            true => (self.held.start - self.flushed) as usize,
            false => self.used,
        };
        self.sink.write_all(&self.buffer[..keep_from])?;
        self.buffer.copy_within(keep_from..self.used, 0);
        self.flushed += keep_from as u64;
        self.used -= keep_from;
        if self.buffer.len() - self.used < MIN_ROOM {
            let grown = self.buffer.len() * 2;
            self.buffer.resize(grown, 0);
        }
        Ok(())
    }

    /// Ends the object. `cut` ends it at that position instead, inside the last record.
    fn finish(mut self, cut: Option<u64>) -> Result<crate::byte_sink::WrittenObject, WriteError> {
        match cut {
            Some(cut) => self.used = (cut - self.flushed) as usize,
            None => loop {
                let used = self.used;
                let (result, wrote) = self.writers[self.current].1.finish(&mut self.buffer[used..]);
                self.used += wrote;
                match result {
                    WriteResult::InputEmpty => break,
                    WriteResult::OutputFull => self.flush()?,
                }
            },
        }
        self.hold = false;
        self.flush()?;
        self.sink.finish()
    }
}

/// A field of a record a fault rewrites, read before any of it is written.
enum Field {
    Null,
    /// A text cell, printed as it is.
    Cell(String),
    /// A value as its formatter printed it.
    Value(String),
}

impl Field {
    fn text(&self) -> &str {
        match self {
            Self::Null => "",
            Self::Cell(text) | Self::Value(text) => text,
        }
    }
}

/// What a fault writes in place of one field.
enum Replacement {
    /// Printed by csv-core, which quotes it where it needs.
    Text(String),
    /// Written as it is.
    Raw(Vec<u8>),
}

/// What a write under `malformed_rows` tracks of one file.
struct Faults {
    file: Option<FaultKind>,
    records: Vec<FaultRecord>,
    columns: FaultColumns,
    names: Vec<String>,
    quote: u8,
    escape: Option<u8>,
    delimiter: u8,
    /// The record a fault of the last row written holds, which a cut supersedes.
    last_row_fault: Option<usize>,
    last_line: u64,
    rows: u64,
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
    /// The columns in the order written: the schema's, or with two swapped by `header_drift`.
    order: Vec<usize>,
    base_end: LineEnd,
    faults: Option<Faults>,
}

impl DelimitedEncoder {
    pub(super) fn new(
        settings: &DelimitedSettings,
        default_delimiter: u8,
        schema: &SchemaRef,
        sink: Box<dyn ByteSink>,
        faulting: Option<Faulting>,
    ) -> Result<Self, WriteError> {
        let file_fault = faulting.and_then(|faulting| faulting.file);
        let file = file_fault.map(|fault| fault.kind);
        let mut records = Vec::new();
        let file_record = |detail: Value| fault_record(file.expect("a file fault"), None, None, None, as_map(detail));
        let mut sink: Box<dyn ByteSink> = match (settings.encoding, file) {
            (TextEncoding::Latin1, _) => Box::new(Latin1Sink::new(sink)),
            (TextEncoding::Utf8, Some(FaultKind::Latin1File)) => {
                records.push(file_record(json!({"encoding": "windows-1252"})));
                Box::new(Latin1Sink::new(sink))
            }
            (TextEncoding::Utf8, _) => sink,
        };
        if settings.bom || file == Some(FaultKind::Bom) {
            sink.write_all(UTF8_BOM)?;
        }
        if file == Some(FaultKind::Bom) {
            records.push(file_record(json!({})));
        }
        let quote = dialect_byte("quote", settings.quote)?;
        let escape = settings.escape.map(|escape| dialect_byte("escape", escape)).transpose()?;
        let own_delimiter = settings.delimiter.map_or(Ok(default_delimiter), |c| dialect_byte("delimiter", c))?;
        let delimiter = match file {
            Some(FaultKind::WrongDelimiter) => {
                let other = b";,|"
                    .iter()
                    .copied()
                    .find(|&byte| byte != own_delimiter && byte != quote)
                    .expect("three candidates and two exclusions");
                records.push(file_record(json!({"delimiter": char::from(other).to_string()})));
                other
            }
            _ => own_delimiter,
        };
        let base_end = match settings.line_terminator {
            LineTerminator::Lf => LineEnd::Lf,
            LineTerminator::Crlf => LineEnd::Crlf,
        };
        let ends = match file {
            Some(FaultKind::MixedLineEndings) => {
                let mut ends = vec![base_end];
                ends.extend([LineEnd::Lf, LineEnd::Crlf, LineEnd::Cr].into_iter().filter(|&end| end != base_end));
                ends
            }
            _ => vec![base_end],
        };
        let build = |end: LineEnd| {
            let mut builder = csv_core::WriterBuilder::new();
            builder
                .delimiter(delimiter)
                .quote(quote)
                .quote_style(match settings.quoting {
                    Quoting::Necessary => csv_core::QuoteStyle::Necessary,
                    Quoting::Always => csv_core::QuoteStyle::Always,
                    Quoting::NonNumeric => csv_core::QuoteStyle::NonNumeric,
                    Quoting::Never => csv_core::QuoteStyle::Never,
                })
                .terminator(match end {
                    LineEnd::Lf => csv_core::Terminator::Any(b'\n'),
                    LineEnd::Crlf => csv_core::Terminator::CRLF,
                    LineEnd::Cr => csv_core::Terminator::Any(b'\r'),
                });
            match escape {
                Some(escape) => builder.double_quote(false).escape(escape),
                None => builder.double_quote(true),
            };
            (end, builder.build())
        };
        let names: Vec<String> = schema.fields().iter().map(|field| field.name().clone()).collect();
        let mut order: Vec<usize> = (0..names.len()).collect();
        let mut header = names.clone();
        if let Some(fault) = file_fault.filter(|fault| fault.kind == FaultKind::HeaderDrift) {
            let width = names.len() as u64;
            let first = (fault.pick % width) as usize;
            if width >= 2 && (fault.pick >> 32) & 1 == 0 {
                let second = ((first as u64 + 1 + (fault.pick >> 33) % (width - 1)) % width) as usize;
                order.swap(first, second);
                header = order.iter().map(|&column| names[column].clone()).collect();
            } else {
                let upper = names[first].to_uppercase();
                header[first] = if upper == names[first] { format!("{upper}_1") } else { upper };
            }
            records.push(file_record(json!({"header": header})));
        }
        let mut encoder = Self {
            out: RecordWriter {
                writers: ends.into_iter().map(build).collect(),
                current: 0,
                sink,
                buffer: vec![0; BUFFER_BYTES],
                used: 0,
                flushed: 0,
                counting: faulting.is_some(),
                lines: 0,
                ends: [0; 3],
                hold: file == Some(FaultKind::TruncatedLine),
                held: HeldRecord::default(),
            },
            null: settings.null.clone(),
            empty_string: (settings.null.is_empty() && settings.quoting != Quoting::Never).then_some([quote, quote]),
            date_format: settings.date_format.clone(),
            timestamp_format: settings.timestamp_format.clone(),
            timestamp_tz_format: settings.timestamp_tz_format.clone(),
            time_format: settings.time_format.clone(),
            text: String::new(),
            order,
            base_end,
            faults: faulting.map(|_| Faults {
                file,
                records,
                columns: FaultColumns::of(schema),
                names,
                quote,
                escape,
                delimiter,
                last_row_fault: None,
                last_line: 0,
                rows: 0,
            }),
        };
        if settings.header {
            encoder.out.begin_record(base_end);
            for (index, name) in header.iter().enumerate() {
                if index > 0 {
                    encoder.out.delimiter()?;
                }
                encoder.out.field(name.as_bytes())?;
            }
            encoder.out.terminator()?;
        }
        Ok(encoder)
    }

    /// Writes `field`, as the record's every unbroken field is written.
    fn put(&mut self, field: &Field, one_column: bool) -> io::Result<()> {
        match field {
            Field::Null if self.null.is_empty() => Ok(()),
            Field::Null => self.out.field(self.null.as_bytes()),
            Field::Cell(text) => self.out.field(text.as_bytes()),
            Field::Value(text) => match self.empty_string {
                Some(quotes) if text.is_empty() && !one_column => self.out.raw(&quotes),
                _ => self.out.field(text.as_bytes()),
            },
        }
    }

    /// Writes the first `count` of `fields` in the written order, `replaced` in place of its column.
    fn put_fields(&mut self, fields: &[Field], count: usize, replaced: Option<(usize, Replacement)>, one_column: bool) -> io::Result<()> {
        for position in 0..count {
            if position > 0 {
                self.out.delimiter()?;
            }
            let column = self.order[position];
            match &replaced {
                Some((at, Replacement::Text(text))) if *at == column => self.out.field(text.as_bytes())?,
                Some((at, Replacement::Raw(bytes))) if *at == column => self.out.raw(bytes)?,
                _ => self.put(&fields[column], one_column)?,
            }
        }
        Ok(())
    }

    /// Writes the row `fault` breaks, and the record of it, unless the file cannot hold it, where
    /// the row is written whole and nothing is recorded.
    fn put_fault(&mut self, fault: &RowFault, fields: &[Field], one_column: bool) -> Result<(), WriteError> {
        let faults = self.faults.as_ref().expect("a fault is placed only under malformed_rows");
        let width = fields.len();
        let text_columns = faults.columns.text.clone();
        let typed_columns = faults.columns.typed.clone();
        let named = fault
            .column
            .as_ref()
            .and_then(|name| faults.names.iter().position(|own| own == name));
        let file = faults.file;
        let (quote, escape, delimiter) = (faults.quote, faults.escape, faults.delimiter);
        let any: Vec<usize> = (0..width).collect();
        let text_or_any = if text_columns.is_empty() { &any } else { &text_columns };
        let token_pick = fault.pick >> 32;
        let quoted = |inner: &[u8]| {
            let mut bytes = vec![quote];
            for &byte in inner {
                if byte == quote {
                    bytes.push(escape.unwrap_or(quote));
                }
                bytes.push(byte);
            }
            bytes.push(quote);
            bytes
        };
        let start_line = self.out.lines + 1;
        let start = self.out.position();
        let (column, detail) = match fault.kind {
            FaultKind::RaggedShort => {
                let kept = 1 + (fault.pick % (width as u64 - 1)) as usize;
                self.put_fields(fields, kept, None, one_column)?;
                (None, json!({"tokens": kept, "expected": width}))
            }
            FaultKind::RaggedLong => {
                let extra = 1 + (fault.pick & 1) as usize;
                self.put_fields(fields, width, None, one_column)?;
                for _ in 0..extra {
                    self.out.delimiter()?;
                    self.out.field(b"extra")?;
                }
                (None, json!({"tokens": width + extra, "expected": width}))
            }
            FaultKind::UnescapedQuote => {
                let column = FaultColumns::chosen(text_or_any, fault.pick).expect("a file holds a column");
                let kept: String = fields[column]
                    .text()
                    .chars()
                    .filter(|&c| !c.is_ascii() || ![quote, delimiter, b'\r', b'\n', escape.unwrap_or(quote)].contains(&(c as u8)))
                    .collect();
                let kept = if kept.chars().count() < 2 { "ab".to_string() } else { kept };
                let (left, right) = split_half(&kept);
                let mut bytes = vec![quote];
                bytes.extend_from_slice(left.as_bytes());
                bytes.push(quote);
                bytes.extend_from_slice(right.as_bytes());
                bytes.push(quote);
                self.put_fields(fields, width, Some((column, Replacement::Raw(bytes))), one_column)?;
                (Some(column), json!({}))
            }
            FaultKind::EmbeddedNewline => {
                let column = FaultColumns::chosen(&text_columns, fault.pick).expect("the layout checked for a string column");
                let (left, right) = split_half(fields[column].text());
                let bytes = quoted(format!("{left}\n{right}").as_bytes());
                self.put_fields(fields, width, Some((column, Replacement::Raw(bytes))), one_column)?;
                (Some(column), json!({}))
            }
            FaultKind::TypeInvalid => {
                let column = named
                    .or_else(|| FaultColumns::chosen(&typed_columns, fault.pick))
                    .expect("the layout checked for a typed column");
                let text = fault
                    .text
                    .clone()
                    .unwrap_or_else(|| invalid_token(fields[column].text(), token_pick, &self.null));
                self.put_fields(fields, width, Some((column, Replacement::Text(text.clone()))), one_column)?;
                (Some(column), json!({"text": text}))
            }
            FaultKind::InvalidUtf8 if file == Some(FaultKind::Latin1File) || start < ENCODING_GUESSED_FROM_BYTES => {
                self.put_fields(fields, width, None, one_column)?;
                return Ok(());
            }
            FaultKind::InvalidUtf8 => {
                let column = FaultColumns::chosen(text_or_any, fault.pick).expect("a file holds a column");
                let (bad, name): (&[u8], &str) = match token_pick & 1 {
                    0 => (b"\xC3\x28", "c3 28"),
                    _ => (b"\x80", "80"),
                };
                let (left, right) = split_half(fields[column].text());
                let mut inner = left.as_bytes().to_vec();
                inner.extend_from_slice(bad);
                inner.extend_from_slice(right.as_bytes());
                self.put_fields(fields, width, Some((column, Replacement::Raw(quoted(&inner)))), one_column)?;
                (Some(column), json!({"bytes": name}))
            }
            FaultKind::LineTooLong => {
                let column = FaultColumns::chosen(text_or_any, fault.pick).expect("a file holds a column");
                let mut text = fields[column].text().to_string();
                text.extend(std::iter::repeat_n('x', DUCKDB_MAX_LINE_SIZE + 1));
                self.put_fields(fields, width, Some((column, Replacement::Text(text))), one_column)?;
                (Some(column), Value::Null)
            }
            kind => unreachable!("{} breaks a whole file, never a row", kind.name()),
        };
        self.out.terminator()?;
        let detail = match fault.kind {
            FaultKind::LineTooLong => json!({"bytes": self.out.position() - start, "limit": DUCKDB_MAX_LINE_SIZE}),
            _ => detail,
        };
        let faults = self.faults.as_mut().expect("checked above");
        let column = column.map(|column| faults.names[column].as_str());
        faults.last_row_fault = Some(faults.records.len());
        let record = fault_record(fault.kind, Some(start_line), fault.dataset_row, column, as_map(detail));
        faults.records.push(record);
        Ok(())
    }
}

/// `text` split at its middle character.
fn split_half(text: &str) -> (&str, &str) {
    let middle = text.char_indices().nth(text.chars().count() / 2).map_or(text.len(), |(at, _)| at);
    text.split_at(middle)
}

/// Text no reader parses as a typed value: `N/A`, `-`, `?`, or `value` with a unit after it. A
/// token the dialect prints for a null is passed over, so the fault never reads back as one.
fn invalid_token(value: &str, pick: u64, null: &str) -> String {
    let with_unit = format!("{value} kg");
    let tokens = ["N/A", "-", "?", with_unit.as_str()];
    let usable: Vec<&str> = tokens
        .into_iter()
        .filter(|token| *token != null && !(token.ends_with(" kg") && value.is_empty()))
        .collect();
    usable[(pick % usable.len() as u64) as usize].to_string()
}

fn as_map(value: Value) -> Map<String, Value> {
    match value {
        Value::Object(map) => map,
        _ => Map::new(),
    }
}

impl FileEncoder for DelimitedEncoder {
    fn write(&mut self, batch: &RecordBatch, cells: &[Vec<TextCell>], faults: &RowFaults) -> Result<(), WriteError> {
        let (date, timestamp, timestamp_tz, time) = (
            self.date_format.clone(),
            self.timestamp_format.clone(),
            self.timestamp_tz_format.clone(),
            self.time_format.clone(),
        );
        let options = FormatOptions::default()
            .with_date_format(date.as_deref())
            .with_datetime_format(timestamp.as_deref())
            .with_timestamp_format(timestamp.as_deref())
            .with_timestamp_tz_format(timestamp_tz.as_deref())
            .with_time_format(time.as_deref());
        let formatters = batch
            .columns()
            .iter()
            .map(|column| ArrayFormatter::try_new(column.as_ref(), &options))
            .collect::<Result<Vec<_>, _>>()?;
        let one_column = formatters.len() == 1;
        let mixed_ends = self.out.writers.len() > 1;
        let mut next_cell = vec![0usize; batch.num_columns()];
        let mut next_fault = 0;
        let format_error = |column: usize| WriteError::Format {
            column: batch.schema().field(column).name().clone(),
        };
        for row in 0..batch.num_rows() {
            let end = match (&faults.line_ends, mixed_ends) {
                (Some(ends), true) => ends[row],
                _ => self.base_end,
            };
            self.out.begin_record(end);
            if let Some(tracked) = &mut self.faults {
                tracked.last_row_fault = None;
                tracked.last_line = self.out.lines + 1;
                tracked.rows += 1;
            }
            let fault = faults.faults.get(next_fault).filter(|fault| fault.row as usize == row);
            if let Some(fault) = fault {
                next_fault += 1;
                let mut fields = Vec::with_capacity(formatters.len());
                for (column, formatter) in formatters.iter().enumerate() {
                    let column_cells = &cells[column];
                    let cursor = &mut next_cell[column];
                    let field = if column_cells.get(*cursor).is_some_and(|cell| cell.row as usize == row) {
                        *cursor += 1;
                        Field::Cell(column_cells[*cursor - 1].text.clone())
                    } else if batch.column(column).is_null(row) {
                        Field::Null
                    } else {
                        let mut text = String::new();
                        write!(text, "{}", formatter.value(row)).map_err(|_| format_error(column))?;
                        Field::Value(text)
                    };
                    fields.push(field);
                }
                self.put_fault(fault, &fields, one_column)?;
                continue;
            }
            for position in 0..self.order.len() {
                let column = self.order[position];
                if position > 0 {
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
                    write!(self.text, "{}", formatters[column].value(row)).map_err(|_| format_error(column))?;
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

    fn finish(mut self: Box<Self>) -> Result<Finished, WriteError> {
        let Some(mut faults) = self.faults.take() else {
            return Ok(self.out.finish(None)?.into());
        };
        let mut first = Vec::new();
        if faults.file == Some(FaultKind::MixedLineEndings) {
            let [lf, crlf, cr] = self.out.ends;
            first.push(fault_record(
                FaultKind::MixedLineEndings,
                None,
                None,
                None,
                as_map(json!({"lf": lf, "crlf": crlf, "cr": cr})),
            ));
        }
        let mut cut = None;
        if faults.file == Some(FaultKind::TruncatedLine) && faults.rows > 0 {
            let held = &self.out.held;
            let end = self.out.position();
            let field = (held.fields.len() - 1) / 2;
            let field_start = held.fields[field];
            let field_end = held.fields.get(field + 1).map_or(held.content_end, |next| next - 1);
            let mut at = field_start + (field_end - field_start) / 2;
            let buffered = |at: u64| self.out.buffer[(at - self.out.flushed) as usize];
            while at > held.start && buffered(at) & 0xC0 == 0x80 {
                at -= 1;
            }
            if let Some(index) = faults.last_row_fault {
                faults.records.remove(index);
            }
            let detail = json!({"kept_bytes": at - held.start, "bytes": end - held.start});
            first.push(fault_record(
                FaultKind::TruncatedLine,
                Some(faults.last_line),
                None,
                None,
                as_map(detail),
            ));
            cut = Some(at);
        }
        let object = self.out.finish(cut)?;
        let mut records = faults.records;
        let at = records.iter().take_while(|record| record.line.is_none()).count();
        records.splice(at..at, first);
        Ok(Finished { object, faults: records })
    }
}
