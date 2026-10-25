//! Which rows each file holds, and what each object is named. A layout comes from the plan and the
//! settings alone, so it is known before a block is generated.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;

use arrow_schema::DataType;
use arrow_schema::SchemaRef;
use doppeldata_core::Plan;
use doppeldata_core::file_rows_draws;
use doppeldata_core::format_settings::Output;
use doppeldata_core::manifest::FileRows;
use doppeldata_core::manifest::LayoutSettings;
use doppeldata_core::manifest::Manifest;
use doppeldata_core::profile::PartitionStyle;
use doppeldata_core::profile::Quantiles;

/// The prefix of every data object's file name.
pub const PART_PREFIX: &str = "part-";

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LayoutError {
    #[error("file_rows must be at least 1")]
    ZeroFileRows,
    #[error("partition_values must be at least 1")]
    ZeroPartitionValues,
    #[error("{0}")]
    RowsPerFile(String),
    #[error("partition key {key:?} names no column in the profile")]
    PartitionKeyMissing { key: String },
    #[error("partition key {key:?} appears twice")]
    PartitionKeyRepeated { key: String },
    #[error("partition key {key:?} is a {data_type} column, and a directory name holds only a scalar")]
    PartitionKeyType { key: String, data_type: DataType },
    #[error("every column is a partition key, which leaves the files with no columns")]
    EveryColumnPartitioned,
    #[error("the manifest's ranges do not rebuild the dataset: {0}")]
    ManifestRanges(String),
    /// A scenario's perturbation the files cannot hold. `field` is its RFC 6901 JSON Pointer into
    /// the scenario.
    #[error("{field}: {message}")]
    Scenario { field: String, message: String },
}

/// The contiguous rows one file holds. A partitioned range writes one object per value it holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileRange {
    pub index: u32,
    pub first_row: u64,
    pub rows: u64,
}

/// Every file's rows and name, before anything is generated.
#[derive(Debug, Clone, PartialEq)]
pub struct FileLayout {
    pub ranges: Vec<FileRange>,
    pub partition_by: Vec<String>,
    pub partition_style: PartitionStyle,
    /// The first eight hex digits of the dataset identity, in every object name, so a rerun with a
    /// changed input never mixes its files with an earlier run's.
    pub identity8: String,
    pub extension: String,
}

impl FileLayout {
    /// The object name of `range`, without its partition directories: `part-00000-1f2e3d4c.parquet`.
    pub fn file_name(&self, range: &FileRange) -> String {
        file_name(range.index, &self.identity8, &self.extension)
    }
}

pub(crate) fn file_name(range: u32, identity8: &str, extension: &str) -> String {
    format!("{PART_PREFIX}{range:05}-{identity8}.{extension}")
}

/// The layout `settings` give `plan`'s rows, with names for `output`'s format. A partitioned range
/// holds `file_rows` times `partition_values`, so each of its objects lands near `file_rows`.
pub fn layout(plan: &Plan, settings: &LayoutSettings, output: &Output) -> Result<FileLayout, LayoutError> {
    check_partition_keys(&plan.schema(), &settings.partition_by)?;
    let scale = match (settings.partition_by.is_empty(), settings.partition_values) {
        (true, _) => 1,
        (false, 0) => return Err(LayoutError::ZeroPartitionValues),
        (false, values) => values,
    };
    let rows = plan.identity().rows();
    let sizes = match &settings.file_rows {
        FileRows::Fixed(0) => return Err(LayoutError::ZeroFileRows),
        FileRows::Fixed(file_rows) => fixed_sizes(rows, file_rows.saturating_mul(scale)),
        FileRows::Drawn(quantiles) => drawn_sizes(rows, plan.identity().seed(), quantiles, scale)?,
    };
    let ranges = ranges_from_sizes(sizes);
    check_scenario(plan, &settings.partition_by, &ranges)?;
    Ok(FileLayout {
        ranges,
        partition_by: settings.partition_by.clone(),
        partition_style: settings.partition_style,
        identity8: identity8(&plan.identity().digest()),
        extension: output.extension(),
    })
}

/// The layout a manifest's files were written in, rebuilt from each file's range, so a regeneration
/// names and splits every file the same way. Refuses ranges that skip one or leave a row uncovered.
pub fn layout_from_manifest(plan: &Plan, manifest: &Manifest) -> Result<FileLayout, LayoutError> {
    check_partition_keys(&plan.schema(), &manifest.layout.partition_by)?;
    let mut ranges: BTreeMap<u32, FileRange> = BTreeMap::new();
    for file in &manifest.files {
        let range = FileRange {
            index: file.range,
            first_row: file.first_row,
            rows: file.range_rows,
        };
        match ranges.entry(file.range) {
            Entry::Vacant(entry) => {
                entry.insert(range);
            }
            Entry::Occupied(entry) if *entry.get() == range => {}
            Entry::Occupied(_) => {
                return Err(LayoutError::ManifestRanges(format!(
                    "two files give range {} different rows",
                    file.range
                )));
            }
        }
    }
    let rows = plan.identity().rows();
    let mut next_row = 0;
    for (expected, (&index, range)) in ranges.iter().enumerate() {
        if index != expected as u32 {
            return Err(LayoutError::ManifestRanges(format!("no file holds range {expected}")));
        }
        if range.first_row != next_row || range.rows == 0 {
            return Err(LayoutError::ManifestRanges(format!(
                "range {index} starts at row {} where row {next_row} is next",
                range.first_row
            )));
        }
        next_row += range.rows;
    }
    if next_row != rows {
        return Err(LayoutError::ManifestRanges(format!(
            "the ranges cover {next_row} of the dataset's {rows} rows"
        )));
    }
    let ranges: Vec<FileRange> = ranges.into_values().collect();
    check_scenario(plan, &manifest.layout.partition_by, &ranges)?;
    Ok(FileLayout {
        ranges,
        partition_by: manifest.layout.partition_by.clone(),
        partition_style: manifest.layout.partition_style,
        identity8: identity8(&manifest.dataset_digest),
        extension: manifest.output.extension(),
    })
}

fn check_partition_keys(schema: &SchemaRef, keys: &[String]) -> Result<(), LayoutError> {
    for (index, key) in keys.iter().enumerate() {
        if keys[..index].contains(key) {
            return Err(LayoutError::PartitionKeyRepeated { key: key.clone() });
        }
        let field = schema
            .field_with_name(key)
            .map_err(|_| LayoutError::PartitionKeyMissing { key: key.clone() })?;
        if field.data_type().is_nested() || matches!(field.data_type(), DataType::Binary | DataType::LargeBinary) {
            return Err(LayoutError::PartitionKeyType {
                key: key.clone(),
                data_type: field.data_type().clone(),
            });
        }
    }
    if !keys.is_empty() && keys.len() == schema.fields().len() {
        return Err(LayoutError::EveryColumnPartitioned);
    }
    Ok(())
}

/// Refuses a layout the plan's scenario cannot hold: a partition key its schema drift drops or
/// renames, or an `across_files` copy whose source could share its file.
fn check_scenario(plan: &Plan, partition_by: &[String], ranges: &[FileRange]) -> Result<(), LayoutError> {
    if let Some(drift) = plan.drift()
        && let Some((key, field)) = partition_by.iter().find_map(|key| drift.moves(key).map(|field| (key, field)))
    {
        return Err(LayoutError::Scenario {
            field,
            message: format!("the write is partitioned by {key:?}, which schema_drift drops or renames in the files past its cut"),
        });
    }
    if let Some(drift) = plan.drift()
        && !ranges.is_empty()
        && ranges.iter().all(|range| range.first_row < drift.first_row())
    {
        return Err(LayoutError::Scenario {
            field: drift.cut_field(),
            message: format!(
                "no file starts at or past row {}, where schema_drift's files begin, so none would drift; write smaller files, or lower after_fraction",
                drift.first_row()
            ),
        });
    }
    if let Some((field, reach)) = plan.across_files_reach()
        && let Some(range) = ranges.iter().find(|range| range.rows > reach)
    {
        let message = match reach {
            0 => "the dataset is one block, so an across_files copy has no other block to come from; copy within_file".to_string(),
            _ => format!(
                "file {} holds {} rows, and an across_files copy lies {reach} rows from its source, so every file must hold at most that many; write smaller files, or copy within_file",
                range.index, range.rows
            ),
        };
        return Err(LayoutError::Scenario {
            field: field.to_string(),
            message,
        });
    }
    Ok(())
}

pub(crate) fn identity8(digest: &str) -> String {
    digest.trim_start_matches("sha256:").chars().take(8).collect()
}

fn ranges_from_sizes(sizes: Vec<u64>) -> Vec<FileRange> {
    let mut first_row = 0;
    sizes
        .into_iter()
        .enumerate()
        .map(|(index, rows)| {
            let range = FileRange {
                index: index as u32,
                first_row,
                rows,
            };
            first_row += rows;
            range
        })
        .collect()
}

fn fixed_sizes(rows: u64, range_rows: u64) -> Vec<u64> {
    (0..rows.div_ceil(range_rows))
        .map(|range| range_rows.min(rows - range * range_rows))
        .collect()
}

/// Range sizes from the engine's draws over `quantiles`, each scaled by the partition values, until
/// they cover `rows`. The last range takes the remainder. A remainder under half the range drawn
/// before it folds into that range, so no partition gets a sliver file the source never held.
fn drawn_sizes(rows: u64, seed: u64, quantiles: &Quantiles, scale: u64) -> Result<Vec<u64>, LayoutError> {
    let mut draws = file_rows_draws(seed, quantiles).map_err(LayoutError::RowsPerFile)?;
    let mut sizes = Vec::new();
    let mut covered = 0;
    while covered < rows {
        let drawn = draws.next().expect("the draws never end").round() as u64;
        let mut size = drawn.max(1).saturating_mul(scale).min(rows - covered);
        if rows - covered - size < size / 2 {
            size = rows - covered;
        }
        sizes.push(size);
        covered += size;
    }
    Ok(sizes)
}
