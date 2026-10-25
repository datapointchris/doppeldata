//! Which rows each file holds, and what each object is named. A layout comes from the plan and the
//! settings alone, so it is known before a block is generated.

use arrow_schema::DataType;
use arrow_schema::SchemaRef;
use doppeldata_core::Plan;
use doppeldata_core::manifest::FileRows;
use doppeldata_core::manifest::IpcFormat;
use doppeldata_core::manifest::LayoutSettings;
use doppeldata_core::manifest::Manifest;
use doppeldata_core::manifest::Output;
use doppeldata_core::manifest::TextCompression;
use doppeldata_core::profile::PartitionStyle;
use doppeldata_core::profile::Quantiles;
use rand::Rng;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use sha2::Digest;
use sha2::Sha256;

/// The prefix of every data object's file name. `replace` deletes only objects carrying it.
pub const PART_PREFIX: &str = "part-";

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LayoutError {
    #[error("file_rows must be at least 1")]
    ZeroFileRows,
    #[error("rows_per_file quantile {name:?} is not p and its level's digits, such as p05")]
    QuantileName { name: String },
    #[error("rows_per_file holds no quantile at or above 1 row")]
    NoUsableQuantile,
    #[error("partition key {key:?} names no column in the profile")]
    PartitionKeyMissing { key: String },
    #[error("partition key {key:?} appears twice")]
    PartitionKeyRepeated { key: String },
    #[error("partition key {key:?} is a {data_type} column, and a directory name holds only a scalar")]
    PartitionKeyType { key: String, data_type: DataType },
    #[error("every column is a partition key, which leaves the files with no columns")]
    EveryColumnPartitioned,
    #[error("the manifest's files leave rows {first_row} to {rows} without a range")]
    ManifestGap { first_row: u64, rows: u64 },
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
        format!("{PART_PREFIX}{:05}-{}.{}", range.index, self.identity8, self.extension)
    }
}

/// The layout `settings` give `plan`'s rows, with names for `output`'s format.
pub fn layout(plan: &Plan, settings: &LayoutSettings, output: &Output) -> Result<FileLayout, LayoutError> {
    check_partition_keys(&plan.schema(), &settings.partition_by)?;
    let rows = plan.identity().rows();
    let sizes = match &settings.file_rows {
        FileRows::Fixed(0) => return Err(LayoutError::ZeroFileRows),
        FileRows::Fixed(file_rows) => fixed_sizes(rows, *file_rows),
        FileRows::Drawn(quantiles) => drawn_sizes(rows, plan.identity().seed(), quantiles)?,
    };
    Ok(FileLayout {
        ranges: ranges_from_sizes(sizes),
        partition_by: settings.partition_by.clone(),
        partition_style: settings.partition_style,
        identity8: identity8(&plan.identity().digest()),
        extension: extension(output),
    })
}

/// The layout a manifest's files were written in, so a regeneration names and splits every file the
/// same way. Each distinct `first_row` starts a range.
pub fn layout_from_manifest(plan: &Plan, manifest: &Manifest) -> Result<FileLayout, LayoutError> {
    check_partition_keys(&plan.schema(), &manifest.layout.partition_by)?;
    let mut starts: Vec<u64> = manifest.files.iter().map(|file| file.first_row).collect();
    starts.sort_unstable();
    starts.dedup();
    let rows = plan.identity().rows();
    let mut sizes = Vec::with_capacity(starts.len());
    for (index, &start) in starts.iter().enumerate() {
        let end = starts.get(index + 1).copied().unwrap_or(rows);
        if end <= start {
            return Err(LayoutError::ManifestGap { first_row: start, rows });
        }
        sizes.push(end - start);
    }
    if starts.first().is_some_and(|&first| first != 0) || (starts.is_empty() && rows > 0) {
        return Err(LayoutError::ManifestGap { first_row: 0, rows });
    }
    Ok(FileLayout {
        ranges: ranges_from_sizes(sizes),
        partition_by: manifest.layout.partition_by.clone(),
        partition_style: manifest.layout.partition_style,
        identity8: identity8(&manifest.identity),
        extension: extension(&manifest.output),
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

fn identity8(digest: &str) -> String {
    digest.trim_start_matches("sha256:").chars().take(8).collect()
}

fn extension(output: &Output) -> String {
    let text = |base: &str, compression: TextCompression| match compression {
        TextCompression::None => base.to_string(),
        TextCompression::Gzip => format!("{base}.gz"),
        TextCompression::Zstd => format!("{base}.zst"),
    };
    match output {
        Output::Parquet(_) => "parquet".into(),
        Output::Arrow(settings) => match settings.ipc {
            IpcFormat::File => "arrow".into(),
            IpcFormat::Stream => "arrows".into(),
        },
        Output::Csv(settings) => text("csv", settings.compression),
        Output::Tsv(settings) => text("tsv", settings.compression),
        Output::Json(settings) => text("json", settings.compression),
        Output::Ndjson(settings) => text("ndjson", settings.compression),
    }
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

fn fixed_sizes(rows: u64, file_rows: u64) -> Vec<u64> {
    (0..rows.div_ceil(file_rows))
        .map(|file| file_rows.min(rows - file * file_rows))
        .collect()
}

/// Sizes drawn through the piecewise-linear inverse CDF of `quantiles`, until they cover `rows`.
/// The last file takes the remainder.
fn drawn_sizes(rows: u64, seed: u64, quantiles: &Quantiles) -> Result<Vec<u64>, LayoutError> {
    let mut knots = Vec::with_capacity(quantiles.0.len());
    for (name, &value) in &quantiles.0 {
        knots.push((quantile_level(name)?, value));
    }
    knots.sort_by(|a, b| a.0.total_cmp(&b.0));
    if !knots.iter().any(|&(_, value)| value >= 1.0) {
        return Err(LayoutError::NoUsableQuantile);
    }
    let mut rng = layout_stream(seed);
    let mut sizes = Vec::new();
    let mut covered = 0;
    while covered < rows {
        const SCALE: f64 = 1.0 / (1u64 << 53) as f64;
        let level = (rng.next_u64() >> 11) as f64 * SCALE;
        let size = (inverse_cdf(&knots, level).round() as u64).max(1).min(rows - covered);
        sizes.push(size);
        covered += size;
    }
    Ok(sizes)
}

/// `p05` is 0.05 and `p999` is 0.999: the digits after `p` are the level's decimal digits.
fn quantile_level(name: &str) -> Result<f64, LayoutError> {
    let digits = name
        .strip_prefix('p')
        .filter(|digits| digits.len() >= 2 && digits.bytes().all(|b| b.is_ascii_digit()))
        .ok_or_else(|| LayoutError::QuantileName { name: name.into() })?;
    Ok(format!("0.{digits}").parse().expect("0. and digits parse"))
}

fn inverse_cdf(knots: &[(f64, f64)], level: f64) -> f64 {
    let first = knots[0];
    let last = knots[knots.len() - 1];
    if level <= first.0 {
        return first.1;
    }
    if level >= last.0 {
        return last.1;
    }
    let upper = knots.partition_point(|&(knot, _)| knot <= level);
    let (low, high) = (knots[upper - 1], knots[upper]);
    low.1 + (high.1 - low.1) * (level - low.0) / (high.0 - low.0)
}

/// The stream file sizes are drawn from, keyed by the seed alone, so the split never moves a value.
fn layout_stream(seed: u64) -> ChaCha8Rng {
    let mut hasher = Sha256::new();
    hasher.update(b"doppeldata.layout\0");
    hasher.update(seed.to_le_bytes());
    ChaCha8Rng::from_seed(hasher.finalize().into())
}
