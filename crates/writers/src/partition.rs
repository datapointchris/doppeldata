//! Splits a batch by its partition columns' values, and names each value's directory. A partition
//! column leaves the files, because its value is in the path.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::Array;
use arrow_array::RecordBatch;
use arrow_array::UInt32Array;
use arrow_cast::display::ArrayFormatter;
use arrow_cast::display::FormatOptions;
use arrow_schema::Schema;
use arrow_schema::SchemaRef;
use doppeldata_core::TextCell;
use doppeldata_core::profile::PartitionStyle;

use crate::WriteError;

/// The directory a null or empty partition value is written under, as Hive and Spark name it.
pub const DEFAULT_PARTITION: &str = "__HIVE_DEFAULT_PARTITION__";

/// The value of each partition key, in key order. None is a null or an empty value.
pub(crate) type PartitionValues = Vec<Option<String>>;

/// The rows of one batch that share partition values, with the partition columns left out.
pub(crate) struct Piece {
    pub values: PartitionValues,
    pub batch: RecordBatch,
    /// Text cells per kept column, each placed by its row within `batch`.
    pub cells: Vec<Vec<TextCell>>,
}

pub(crate) struct Partitioner {
    keys: Vec<(String, usize)>,
    kept: Vec<usize>,
    file_schema: SchemaRef,
    style: PartitionStyle,
}

impl Partitioner {
    /// A partitioner over `schema` by `keys`, which the layout has already checked name its columns.
    pub(crate) fn new(schema: &SchemaRef, keys: &[String], style: PartitionStyle) -> Self {
        let keys: Vec<(String, usize)> = keys
            .iter()
            .map(|key| (key.clone(), schema.index_of(key).expect("the layout checked every key")))
            .collect();
        let kept: Vec<usize> = (0..schema.fields().len())
            .filter(|index| !keys.iter().any(|(_, key)| key == index))
            .collect();
        let file_schema = match keys.is_empty() {
            true => schema.clone(),
            false => Arc::new(Schema::new_with_metadata(
                kept.iter().map(|&index| schema.field(index).clone()).collect::<Vec<_>>(),
                schema.metadata().clone(),
            )),
        };
        Self {
            keys,
            kept,
            file_schema,
            style,
        }
    }

    /// The schema of every file: the plan's, less the partition columns.
    pub(crate) fn file_schema(&self) -> &SchemaRef {
        &self.file_schema
    }

    /// The directories an object with `values` sits under, ending in `/`, or nothing unpartitioned.
    pub(crate) fn directory(&self, values: &PartitionValues) -> String {
        let mut directory = String::new();
        for ((key, _), value) in self.keys.iter().zip(values) {
            let value = value.as_deref().map_or_else(|| DEFAULT_PARTITION.to_string(), escape_path_name);
            match self.style {
                PartitionStyle::Hive => directory.push_str(&format!("{}={value}/", escape_path_name(key))),
                PartitionStyle::Directory => directory.push_str(&format!("{value}/")),
            }
        }
        directory
    }

    /// The manifest's record of `values`, by key, or None unpartitioned.
    pub(crate) fn record(&self, values: PartitionValues) -> Option<BTreeMap<String, Option<String>>> {
        (!self.keys.is_empty()).then(|| self.keys.iter().map(|(key, _)| key.clone()).zip(values).collect())
    }

    /// `batch` split by partition values, in value order. `cells` holds every column's text cells,
    /// in the plan's column order.
    pub(crate) fn split(&self, batch: RecordBatch, cells: Vec<Vec<TextCell>>) -> Result<Vec<Piece>, WriteError> {
        if self.keys.is_empty() {
            return Ok(vec![Piece {
                values: Vec::new(),
                batch,
                cells,
            }]);
        }
        let options = FormatOptions::default()
            .with_datetime_format(Some("%Y-%m-%d %H:%M:%S%.f"))
            .with_timestamp_format(Some("%Y-%m-%d %H:%M:%S%.f"));
        let formatters = self
            .keys
            .iter()
            .map(|&(_, index)| ArrayFormatter::try_new(batch.column(index).as_ref(), &options))
            .collect::<Result<Vec<_>, _>>()?;
        let mut groups: BTreeMap<PartitionValues, Vec<u32>> = BTreeMap::new();
        for row in 0..batch.num_rows() {
            let values = self
                .keys
                .iter()
                .zip(&formatters)
                .map(|(&(_, index), formatter)| match batch.column(index).is_null(row) {
                    true => None,
                    false => Some(formatter.value(row).to_string()).filter(|text| !text.is_empty()),
                })
                .collect();
            groups.entry(values).or_default().push(row as u32);
        }
        let mut placement = vec![(0usize, 0u32); batch.num_rows()];
        let mut pieces = Vec::with_capacity(groups.len());
        for (group, (values, rows)) in groups.into_iter().enumerate() {
            for (position, &row) in rows.iter().enumerate() {
                placement[row as usize] = (group, position as u32);
            }
            let indices = UInt32Array::from(rows);
            let columns = self
                .kept
                .iter()
                .map(|&index| arrow_select::take::take(batch.column(index).as_ref(), &indices, None))
                .collect::<Result<Vec<_>, _>>()?;
            pieces.push(Piece {
                values,
                batch: RecordBatch::try_new(self.file_schema.clone(), columns)?,
                cells: vec![Vec::new(); self.kept.len()],
            });
        }
        for (kept_index, &column) in self.kept.iter().enumerate() {
            for cell in &cells[column] {
                let (group, position) = placement[cell.row as usize];
                pieces[group].cells[kept_index].push(TextCell {
                    row: position,
                    text: cell.text.clone(),
                });
            }
        }
        Ok(pieces)
    }
}

/// A partition key or value escaped as Hive and Spark escape a path name: each character a path or
/// a URL would read specially, and each control character, becomes `%` and two upper-case hex
/// digits.
fn escape_path_name(text: &str) -> String {
    let mut escaped = String::with_capacity(text.len());
    for character in text.chars() {
        let special = matches!(
            character,
            '"' | '#' | '%' | '\'' | '*' | '/' | ':' | '=' | '?' | '\\' | '\u{7F}' | '{' | '[' | ']' | '^'
        ) || ('\u{01}'..='\u{1F}').contains(&character);
        if special {
            escaped.push_str(&format!("%{:02X}", character as u32));
        } else {
            escaped.push(character);
        }
    }
    escaped
}
