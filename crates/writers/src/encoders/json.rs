//! JSON and NDJSON. Each column is encoded by arrow-json's own encoder, and a text cell replaces the
//! null at its row with its own JSON.

use std::io::Write as _;

use arrow_array::RecordBatch;
use arrow_json::writer::EncoderOptions;
use arrow_json::writer::make_encoder;
use arrow_schema::DataType;
use arrow_schema::SchemaRef;
use doppeldata_core::RowFaults;
use doppeldata_core::TextCell;
use doppeldata_core::format_settings::JsonSettings;
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
use crate::WriteError;
use crate::byte_sink::ByteSink;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Layout {
    /// One array of objects: `[{...},{...}]`.
    Array,
    /// One object per line.
    Lines,
}

/// What a write under `malformed_rows` tracks of one file.
struct Faults {
    file: Option<FaultKind>,
    records: Vec<FaultRecord>,
    columns: FaultColumns,
    names: Vec<String>,
    /// The record of a fault of the last row written, which a cut supersedes.
    last_row_fault: Option<usize>,
    /// The last record's bytes, held from the sink so the file can end inside it.
    held: Vec<u8>,
}

pub(super) struct JsonEncoder {
    sink: Box<dyn ByteSink>,
    layout: Layout,
    options: EncoderOptions,
    explicit_nulls: bool,
    /// Each column's name as a JSON string and a colon, encoded once.
    keys: Vec<Vec<u8>>,
    rows_written: u64,
    buffer: Vec<u8>,
    faults: Option<Faults>,
}

impl JsonEncoder {
    pub(super) fn new(
        settings: &JsonSettings,
        schema: &SchemaRef,
        mut sink: Box<dyn ByteSink>,
        layout: Layout,
        faulting: Option<Faulting>,
    ) -> Result<Self, WriteError> {
        if layout == Layout::Array {
            sink.write_all(b"[")?;
        }
        let keys = schema
            .fields()
            .iter()
            .map(|field| {
                let mut key = serde_json::to_vec(field.name()).expect("a string always serializes");
                key.push(b':');
                key
            })
            .collect();
        let faults = faulting.map(|faulting| {
            let file = faulting.file.map(|fault| fault.kind);
            let mut records = Vec::new();
            if file == Some(FaultKind::JsonLayoutMix) {
                let written = match layout {
                    Layout::Array => "array",
                    Layout::Lines => "ndjson",
                };
                records.push(fault_record(
                    FaultKind::JsonLayoutMix,
                    None,
                    None,
                    None,
                    as_map(json!({"layout": written})),
                ));
            }
            Faults {
                file,
                records,
                columns: FaultColumns::of(schema),
                names: schema.fields().iter().map(|field| field.name().clone()).collect(),
                last_row_fault: None,
                held: Vec::new(),
            }
        });
        Ok(Self {
            sink,
            layout,
            options: EncoderOptions::default().with_explicit_nulls(settings.explicit_nulls),
            explicit_nulls: settings.explicit_nulls,
            keys,
            rows_written: 0,
            buffer: Vec::new(),
            faults,
        })
    }

    fn truncating(&self) -> bool {
        self.faults
            .as_ref()
            .is_some_and(|faults| faults.file == Some(FaultKind::TruncatedLine))
    }
}

/// A text cell as JSON: a number, `true`, `false` or `null` as itself, and anything else as a string.
fn push_text_cell(text: &str, out: &mut Vec<u8>) {
    match serde_json::from_str::<Value>(text) {
        Ok(Value::Number(_) | Value::Bool(_) | Value::Null) => out.extend_from_slice(text.trim().as_bytes()),
        _ => serde_json::to_writer(&mut *out, text).expect("a string always serializes"),
    }
}

fn as_map(value: Value) -> Map<String, Value> {
    match value {
        Value::Object(map) => map,
        _ => Map::new(),
    }
}

/// How a row's fault rewrites it: one column's value, or the record's closing brace.
enum Rewrite {
    Value { column: usize, json: Vec<u8> },
    Unbalanced,
}

impl FileEncoder for JsonEncoder {
    fn write(&mut self, batch: &RecordBatch, cells: &[Vec<TextCell>], faults: &RowFaults) -> Result<(), WriteError> {
        let schema = batch.schema();
        let mut encoders = schema
            .fields()
            .iter()
            .zip(batch.columns())
            .map(|(field, column)| make_encoder(field, column.as_ref(), &self.options))
            .collect::<Result<Vec<_>, _>>()?;
        let mut next_cell = vec![0usize; batch.num_columns()];
        let mut next_fault = 0;
        let truncating = self.truncating();
        self.buffer.clear();
        if let Some(tracked) = &mut self.faults {
            self.buffer.append(&mut tracked.held);
        }
        let mut last_start = 0;
        for row in 0..batch.num_rows() {
            last_start = self.buffer.len();
            let fault = faults.faults.get(next_fault).filter(|fault| fault.row as usize == row);
            next_fault += usize::from(fault.is_some());
            let mut rewrite = None;
            if let (Some(fault), Some(tracked)) = (fault, &mut self.faults) {
                tracked.last_row_fault = None;
                let line = (self.layout == Layout::Lines).then_some(self.rows_written + 1);
                let (column, detail, chosen) = match fault.kind {
                    FaultKind::TypeInvalid => {
                        let named = fault
                            .column
                            .as_ref()
                            .and_then(|name| tracked.names.iter().position(|own| own == name));
                        let column = named
                            .or_else(|| FaultColumns::chosen(&tracked.columns.typed, fault.pick))
                            .expect("the layout checked for a typed column");
                        let tokens = ["N/A", "-", "?"];
                        let text = fault
                            .text
                            .clone()
                            .unwrap_or_else(|| tokens[((fault.pick >> 32) % 3) as usize].to_string());
                        let json = serde_json::to_vec(&text).expect("a string always serializes");
                        (Some(column), json!({"text": text}), Some(Rewrite::Value { column, json }))
                    }
                    FaultKind::JsonUnbalanced => (None, json!({}), Some(Rewrite::Unbalanced)),
                    FaultKind::JsonTypeFlip => {
                        let valid: Vec<usize> = tracked
                            .columns
                            .scalar
                            .iter()
                            .copied()
                            .filter(|&column| !encoders[column].is_null(row))
                            .collect();
                        match FaultColumns::chosen(&valid, fault.pick) {
                            Some(column) => {
                                let mut encoded = Vec::new();
                                encoders[column].encode(row, &mut encoded);
                                let text = String::from_utf8(encoded).expect("arrow-json writes UTF-8");
                                let json = serde_json::to_vec(&text).expect("a string always serializes");
                                let from = match schema.field(column).data_type() {
                                    DataType::Boolean => "boolean",
                                    _ => "number",
                                };
                                (
                                    Some(column),
                                    json!({"from": from, "to": "string"}),
                                    Some(Rewrite::Value { column, json }),
                                )
                            }
                            None => (None, Value::Null, None),
                        }
                    }
                    kind => unreachable!("{} is not a fault a JSON record holds", kind.name()),
                };
                if chosen.is_some() {
                    tracked.last_row_fault = Some(tracked.records.len());
                    let name = column.map(|column| tracked.names[column].as_str());
                    tracked
                        .records
                        .push(fault_record(fault.kind, line, fault.dataset_row, name, as_map(detail)));
                }
                rewrite = chosen;
            } else if let Some(tracked) = &mut self.faults {
                tracked.last_row_fault = None;
            }
            if self.layout == Layout::Array && self.rows_written > 0 {
                self.buffer.push(b',');
            }
            self.buffer.push(b'{');
            let mut first = true;
            for (column, encoder) in encoders.iter_mut().enumerate() {
                let column_cells = &cells[column];
                let cursor = &mut next_cell[column];
                let cell = column_cells.get(*cursor).filter(|cell| cell.row as usize == row);
                let replaced = match &rewrite {
                    Some(Rewrite::Value { column: at, json }) if *at == column => Some(json),
                    _ => None,
                };
                let is_null = encoder.is_null(row);
                if cell.is_some() {
                    *cursor += 1;
                }
                if replaced.is_none() && cell.is_none() && is_null && !self.explicit_nulls {
                    continue;
                }
                if !first {
                    self.buffer.push(b',');
                }
                first = false;
                self.buffer.extend_from_slice(&self.keys[column]);
                if let Some(json) = replaced {
                    self.buffer.extend_from_slice(json);
                } else if let Some(cell) = cell {
                    push_text_cell(&cell.text, &mut self.buffer);
                } else if is_null {
                    self.buffer.extend_from_slice(b"null");
                } else {
                    encoder.encode(row, &mut self.buffer);
                }
            }
            if !matches!(rewrite, Some(Rewrite::Unbalanced)) {
                self.buffer.push(b'}');
            }
            if self.layout == Layout::Lines {
                self.buffer.push(b'\n');
            }
            self.rows_written += 1;
        }
        match (truncating, &mut self.faults) {
            (true, Some(tracked)) => {
                tracked.held = self.buffer.split_off(last_start);
                self.sink.write_all(&self.buffer)?;
            }
            _ => self.sink.write_all(&self.buffer)?,
        }
        Ok(())
    }

    fn finish(mut self: Box<Self>) -> Result<Finished, WriteError> {
        let truncating = self.truncating();
        let Some(mut faults) = self.faults.take() else {
            if self.layout == Layout::Array {
                self.sink.write_all(b"]\n")?;
            }
            return Ok(self.sink.finish()?.into());
        };
        let mut first = Vec::new();
        if truncating && self.rows_written > 0 {
            let held = std::mem::take(&mut faults.held);
            let mut at = held.len() / 2;
            while at > 0 && held[at] & 0xC0 == 0x80 {
                at -= 1;
            }
            self.sink.write_all(&held[..at])?;
            if let Some(index) = faults.last_row_fault {
                faults.records.remove(index);
            }
            let line = (self.layout == Layout::Lines).then_some(self.rows_written);
            let detail = json!({"kept_bytes": at, "bytes": held.len()});
            first.push(fault_record(FaultKind::TruncatedLine, line, None, None, as_map(detail)));
        } else if self.layout == Layout::Array {
            self.sink.write_all(b"]\n")?;
        }
        let object = self.sink.finish()?;
        let mut records = faults.records;
        let at = records
            .iter()
            .take_while(|record| record.line.is_none() && record.kind.is_file_kind())
            .count();
        records.splice(at..at, first);
        Ok(Finished { object, faults: records })
    }
}
