//! The record written beside a generated dataset, and the writer settings it records. The writers
//! fill it in. The engine owns its model, so its JSON Schema is published with the profile's.

use std::borrow::Cow;
use std::collections::BTreeMap;

use schemars::JsonSchema;
use schemars::Schema;
use schemars::SchemaGenerator;
use schemars::json_schema;
use serde::Deserialize;
use serde::Serialize;

use crate::identity::DatasetIdentity;
use crate::profile::PartitionStyle;
use crate::profile::Quantiles;

/// The manifest format version this engine writes and reads.
pub const MANIFEST_VERSION: u64 = 1;

/// The file name a manifest is written under, beside the data. Spark, Hive and Glue skip a file
/// whose name starts with `_`, so it never reads as data.
pub const MANIFEST_FILE_NAME: &str = "_manifest.json";

/// The rows a file holds when neither the call nor the profile sets a size.
pub const DEFAULT_FILE_ROWS: u64 = 1_000_000;

/// The record of a generated dataset, which with its profile regenerates every file byte for byte.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Manifest {
    pub manifest_version: ManifestVersion,
    /// The tool that wrote the files, as `doppeldata <version>`.
    pub tool: String,
    /// The engine's stream version, from the dataset identity.
    pub stream_version: u32,
    /// The dataset identity's digest, `sha256:` and 64 lowercase hex digits.
    pub identity: String,
    pub profile: ManifestProfile,
    /// The scenario applied, or null for none.
    pub scenario: Option<ManifestScenario>,
    /// The seed, in decimal.
    #[serde(with = "decimal")]
    #[schemars(with = "Decimal")]
    pub seed: u64,
    /// The dataset's row count, in decimal.
    #[serde(with = "decimal")]
    #[schemars(with = "Decimal")]
    pub rows: u64,
    #[serde(flatten)]
    pub output: Output,
    pub layout: LayoutSettings,
    /// Where the files were written, as the caller named it, such as `s3://bucket/orders/`.
    pub destination: String,
    /// Every object written, in path order.
    pub files: Vec<ManifestFile>,
    /// True when the run stopped early, so `files` lists only the objects that completed.
    pub partial: bool,
    /// The threads that generated and encoded. The count never changes a byte.
    pub threads: u32,
    pub elapsed_ms: u64,
    /// When the write finished, in RFC 3339.
    pub written_at: String,
}

impl Manifest {
    /// A manifest naming `identity`'s dataset, with no files yet. The identity's digest, stream
    /// version, seed and row count are copied, never recomputed.
    pub fn new(identity: &DatasetIdentity, profile: ManifestProfile, output: Output, layout: LayoutSettings, destination: String) -> Self {
        Self {
            manifest_version: ManifestVersion,
            tool: format!("doppeldata {}", crate::VERSION),
            stream_version: identity.stream_version(),
            identity: identity.digest(),
            profile,
            scenario: None,
            seed: identity.seed(),
            rows: identity.rows(),
            output,
            layout,
            destination,
            files: Vec::new(),
            partial: false,
            threads: 0,
            elapsed_ms: 0,
            written_at: String::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ManifestProfile {
    /// The profile's `name`.
    pub name: String,
    /// The profile format version it was read as.
    pub version: u64,
    /// The profile's digest, `sha256:` and the hex SHA-256 of its RFC 8785 canonical JSON.
    pub digest: String,
    /// The profile's path as the caller gave it, or null when it came from memory.
    pub path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ManifestScenario {
    pub name: String,
    pub digest: String,
}

/// One object the run wrote.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ManifestFile {
    /// The object's path under the destination, `/`-separated.
    pub path: String,
    /// The dataset row the object's range starts at. Every object a partitioned range splits into
    /// carries the range's first row.
    pub first_row: u64,
    /// The rows in this object.
    pub rows: u64,
    pub bytes: u64,
    /// `sha256:` and the hex SHA-256 of the object's bytes.
    pub sha256: String,
    /// Each partition key's value in the object's path, null for a null value. Absent when the
    /// dataset is not partitioned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition: Option<BTreeMap<String, Option<String>>>,
}

/// The format, under `format`, and that format's settings, under `writer`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "format", content = "writer", rename_all = "snake_case")]
pub enum Output {
    Parquet(ParquetSettings),
    Arrow(ArrowSettings),
    Csv(DelimitedSettings),
    Tsv(DelimitedSettings),
    /// One JSON array of objects per file.
    Json(JsonSettings),
    /// One JSON object per line.
    Ndjson(JsonSettings),
}

impl Default for Output {
    fn default() -> Self {
        Self::Parquet(ParquetSettings::default())
    }
}

impl Output {
    /// The format's name, as `format` holds it.
    pub fn format_name(&self) -> &'static str {
        match self {
            Self::Parquet(_) => "parquet",
            Self::Arrow(_) => "arrow",
            Self::Csv(_) => "csv",
            Self::Tsv(_) => "tsv",
            Self::Json(_) => "json",
            Self::Ndjson(_) => "ndjson",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct ParquetSettings {
    pub compression: ParquetCompression,
    /// The codec's level: 1 to 22 for zstd, 0 to 10 for gzip. Null takes the codec's default.
    pub compression_level: Option<i32>,
    /// The most rows a row group holds.
    pub row_group_rows: u64,
    pub statistics: ParquetStatistics,
    /// The columns written with a dictionary. Null gives every column one.
    pub dictionary_columns: Option<Vec<String>>,
    /// The unit every timestamp column is cast to. Null keeps each column's own. parquet 59
    /// cannot write INT96, so no unit reproduces it.
    pub timestamp_unit: Option<TimestampUnit>,
}

impl Default for ParquetSettings {
    fn default() -> Self {
        Self {
            compression: ParquetCompression::Zstd,
            compression_level: None,
            row_group_rows: 1024 * 1024,
            statistics: ParquetStatistics::Page,
            dictionary_columns: None,
            timestamp_unit: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ParquetCompression {
    None,
    Snappy,
    Gzip,
    Zstd,
    /// LZ4_RAW, the LZ4 codec current readers expect.
    Lz4,
}

/// How much of the min, max and null count each column chunk records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ParquetStatistics {
    None,
    /// Per column chunk.
    Chunk,
    /// Per page too, with the page index.
    Page,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TimestampUnit {
    S,
    Ms,
    Us,
    Ns,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct ArrowSettings {
    pub ipc: IpcFormat,
    pub compression: IpcCompression,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum IpcFormat {
    /// The random-access file format, `.arrow`.
    #[default]
    File,
    /// The streaming format, `.arrows`.
    Stream,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum IpcCompression {
    #[default]
    None,
    Lz4,
    Zstd,
}

/// A CSV or TSV dialect. Every character must be ASCII.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct DelimitedSettings {
    /// Null takes the format's own: `,` for CSV, a tab for TSV.
    pub delimiter: Option<char>,
    pub quote: char,
    /// The character that escapes a quote inside a quoted field. Null doubles the quote instead.
    pub escape: Option<char>,
    pub quoting: Quoting,
    /// Whether the first line names the columns.
    pub header: bool,
    /// What a null prints where the column's profile gives no missing token for it.
    pub null: String,
    pub line_terminator: LineTerminator,
    /// A chrono format for date columns. Null writes ISO 8601.
    pub date_format: Option<String>,
    /// A chrono format for timestamp columns without a time zone. Null writes ISO 8601.
    pub timestamp_format: Option<String>,
    /// A chrono format for timestamp columns with a time zone. Null writes ISO 8601 with the offset.
    pub timestamp_tz_format: Option<String>,
    /// A chrono format for time columns. Null writes ISO 8601.
    pub time_format: Option<String>,
    /// Whether the file starts with a UTF-8 byte order mark.
    pub bom: bool,
    pub encoding: TextEncoding,
    pub compression: TextCompression,
    /// The codec's level: 1 to 22 for zstd, 0 to 9 for gzip. Null takes the codec's default.
    pub compression_level: Option<i32>,
}

impl Default for DelimitedSettings {
    fn default() -> Self {
        Self {
            delimiter: None,
            quote: '"',
            escape: None,
            quoting: Quoting::Necessary,
            header: true,
            null: String::new(),
            line_terminator: LineTerminator::Lf,
            date_format: None,
            timestamp_format: None,
            timestamp_tz_format: None,
            time_format: None,
            bom: false,
            encoding: TextEncoding::Utf8,
            compression: TextCompression::None,
            compression_level: None,
        }
    }
}

/// Which fields are quoted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Quoting {
    /// Only a field holding the delimiter, the quote or a line break.
    #[default]
    Necessary,
    Always,
    /// Every field that does not parse as a number.
    NonNumeric,
    /// None, even where a field needs it.
    Never,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum LineTerminator {
    #[default]
    Lf,
    Crlf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TextEncoding {
    #[default]
    Utf8,
    /// windows-1252, the superset of ISO-8859-1 every decoder applies to a `latin1` label.
    Latin1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TextCompression {
    #[default]
    None,
    Gzip,
    Zstd,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct JsonSettings {
    /// Whether a null field is written as `null` rather than left out.
    pub explicit_nulls: bool,
    pub compression: TextCompression,
    /// The codec's level: 1 to 22 for zstd, 0 to 9 for gzip. Null takes the codec's default.
    pub compression_level: Option<i32>,
}

/// How rows are split into files and directories.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct LayoutSettings {
    pub file_rows: FileRows,
    /// The columns whose values name each object's directories, outermost first. A partition
    /// column is left out of the files, because its value is in the path.
    pub partition_by: Vec<String>,
    pub partition_style: PartitionStyle,
}

impl Default for LayoutSettings {
    fn default() -> Self {
        Self {
            file_rows: FileRows::Fixed(DEFAULT_FILE_ROWS),
            partition_by: Vec::new(),
            partition_style: PartitionStyle::Hive,
        }
    }
}

/// The rows each file's range holds.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum FileRows {
    /// The same count in every range but the last.
    Fixed(u64),
    /// Each range's count drawn from these quantiles, from a stream keyed by the seed, so the
    /// split is the same on every run.
    Drawn(Quantiles),
}

/// `manifest_version`, which must equal [`MANIFEST_VERSION`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u64", into = "u64")]
pub struct ManifestVersion;

impl TryFrom<u64> for ManifestVersion {
    type Error = String;

    fn try_from(version: u64) -> Result<Self, Self::Error> {
        if version == MANIFEST_VERSION {
            Ok(Self)
        } else {
            Err(format!(
                "manifest_version {version} is not {MANIFEST_VERSION}, the version this engine reads"
            ))
        }
    }
}

impl From<ManifestVersion> for u64 {
    fn from(_: ManifestVersion) -> u64 {
        MANIFEST_VERSION
    }
}

impl JsonSchema for ManifestVersion {
    fn schema_name() -> Cow<'static, str> {
        "ManifestVersion".into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "description": "The manifest format version, which changes only on a breaking format change.",
            "const": MANIFEST_VERSION,
        })
    }
}

/// A 64-bit unsigned integer written in decimal, because a JSON number is exact only to 2^53.
struct Decimal;

impl JsonSchema for Decimal {
    fn schema_name() -> Cow<'static, str> {
        "Decimal".into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "description": "A 64-bit unsigned integer in decimal, because a JSON number is exact only to 2^53.",
            "type": "string",
            "pattern": "^(0|[1-9][0-9]{0,19})$",
        })
    }
}

mod decimal {
    use serde::Deserialize;
    use serde::Deserializer;
    use serde::Serializer;

    pub fn serialize<S: Serializer>(value: &u64, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(value)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
        let text = String::deserialize(deserializer)?;
        text.parse()
            .map_err(|_| serde::de::Error::custom(format!("{text:?} is not a 64-bit unsigned integer in decimal")))
    }
}

/// The manifest's JSON Schema (Draft 2020-12), as the library ships it.
pub fn json_schema() -> String {
    let mut schema = schemars::schema_for!(Manifest);
    schema.insert("title".into(), format!("doppeldata manifest v{MANIFEST_VERSION}").into());
    let mut text = serde_json::to_string_pretty(&schema).expect("a derived schema always serializes");
    text.push('\n');
    text
}
