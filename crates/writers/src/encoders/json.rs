//! JSON and NDJSON. Each column is encoded by arrow-json's own encoder, and a text cell replaces the
//! null at its row with its own JSON.

use std::io::Write as _;

use arrow_array::RecordBatch;
use arrow_json::writer::EncoderOptions;
use arrow_json::writer::make_encoder;
use arrow_schema::SchemaRef;
use doppeldata_core::TextCell;
use doppeldata_core::format_settings::JsonSettings;
use serde_json::Value;

use super::FileEncoder;
use crate::WriteError;
use crate::byte_sink::ByteSink;
use crate::byte_sink::WrittenObject;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Layout {
    /// One array of objects: `[{...},{...}]`.
    Array,
    /// One object per line.
    Lines,
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
}

impl JsonEncoder {
    pub(super) fn new(
        settings: &JsonSettings,
        schema: &SchemaRef,
        mut sink: Box<dyn ByteSink>,
        layout: Layout,
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
        Ok(Self {
            sink,
            layout,
            options: EncoderOptions::default().with_explicit_nulls(settings.explicit_nulls),
            explicit_nulls: settings.explicit_nulls,
            keys,
            rows_written: 0,
            buffer: Vec::new(),
        })
    }
}

/// A text cell as JSON: a number, `true`, `false` or `null` as itself, and anything else as a string.
fn push_text_cell(text: &str, out: &mut Vec<u8>) {
    match serde_json::from_str::<Value>(text) {
        Ok(Value::Number(_) | Value::Bool(_) | Value::Null) => out.extend_from_slice(text.trim().as_bytes()),
        _ => serde_json::to_writer(&mut *out, text).expect("a string always serializes"),
    }
}

impl FileEncoder for JsonEncoder {
    fn write(&mut self, batch: &RecordBatch, cells: &[Vec<TextCell>]) -> Result<(), WriteError> {
        let schema = batch.schema();
        let mut encoders = schema
            .fields()
            .iter()
            .zip(batch.columns())
            .map(|(field, column)| make_encoder(field, column.as_ref(), &self.options))
            .collect::<Result<Vec<_>, _>>()?;
        let mut next_cell = vec![0usize; batch.num_columns()];
        self.buffer.clear();
        for row in 0..batch.num_rows() {
            if self.layout == Layout::Array && self.rows_written > 0 {
                self.buffer.push(b',');
            }
            self.buffer.push(b'{');
            let mut first = true;
            for (column, encoder) in encoders.iter_mut().enumerate() {
                let column_cells = &cells[column];
                let cursor = &mut next_cell[column];
                let cell = column_cells.get(*cursor).filter(|cell| cell.row as usize == row);
                let is_null = encoder.is_null(row);
                if cell.is_none() && is_null && !self.explicit_nulls {
                    continue;
                }
                if !first {
                    self.buffer.push(b',');
                }
                first = false;
                self.buffer.extend_from_slice(&self.keys[column]);
                if let Some(cell) = cell {
                    push_text_cell(&cell.text, &mut self.buffer);
                    *cursor += 1;
                } else if is_null {
                    self.buffer.extend_from_slice(b"null");
                } else {
                    encoder.encode(row, &mut self.buffer);
                }
            }
            self.buffer.push(b'}');
            if self.layout == Layout::Lines {
                self.buffer.push(b'\n');
            }
            self.rows_written += 1;
        }
        self.sink.write_all(&self.buffer)?;
        Ok(())
    }

    fn finish(mut self: Box<Self>) -> Result<WrittenObject, WriteError> {
        if self.layout == Layout::Array {
            self.sink.write_all(b"]\n")?;
        }
        self.sink.finish()
    }
}
