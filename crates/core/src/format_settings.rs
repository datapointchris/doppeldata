//! Each output format and the settings it is written with. A profile's `source` records the settings
//! its files were written with, and a manifest records the settings a generated dataset was written
//! with, so both take these types.

use std::ops::RangeInclusive;
use std::sync::Arc;

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde::de::Error;
use serde_json::Value;

use crate::profile::provenance;

/// Reads a profile's copy of a writer's settings past any key the settings do not take, as every
/// profile section is read, so a profile a later release wrote still loads. A caller's settings,
/// read as the type itself, still refuse an unknown key. The keys taken are those the type's
/// default serializes.
pub fn recorded_settings<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: DeserializeOwned + Serialize + Default,
{
    let mut recorded = serde_json::Map::<String, Value>::deserialize(deserializer)?;
    let taken = serde_json::to_value(T::default()).map_err(D::Error::custom)?;
    recorded.retain(|key, _| taken.get(key).is_some());
    serde_json::from_value(Value::Object(recorded)).map_err(D::Error::custom)
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

    /// The file name's extension, with the text codec's suffix: `parquet`, `arrows`, `csv.gz`.
    pub fn extension(&self) -> String {
        match self {
            Self::Parquet(_) => "parquet".into(),
            Self::Arrow(settings) => match settings.ipc {
                IpcFormat::File => "arrow".into(),
                IpcFormat::Stream => "arrows".into(),
            },
            Self::Csv(settings) | Self::Tsv(settings) => format!("{}{}", self.format_name(), settings.compression.suffix()),
            Self::Json(settings) | Self::Ndjson(settings) => format!("{}{}", self.format_name(), settings.compression.suffix()),
        }
    }

    /// Whether the format writes every value as text, so a value the profile records only as text
    /// can print in place of the null the data holds.
    pub fn is_text(&self) -> bool {
        match self {
            Self::Parquet(_) | Self::Arrow(_) => false,
            Self::Csv(_) | Self::Tsv(_) | Self::Json(_) | Self::Ndjson(_) => true,
        }
    }

    /// The documents each file holds, where a Parquet or Arrow IPC write embeds the profile.
    pub fn embedded(&self) -> Option<&EmbeddedDocuments> {
        match self {
            Self::Parquet(settings) => settings.embedded.as_ref(),
            Self::Arrow(settings) => settings.embedded.as_ref(),
            Self::Csv(_) | Self::Tsv(_) | Self::Json(_) | Self::Ndjson(_) => None,
        }
    }
}

/// The JSON a write embeds in each file's metadata: the whole profile, and the scenario's document
/// where one applies. Only a write's caller sets it, so neither a profile nor a manifest holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddedDocuments {
    pub profile: Arc<str>,
    pub scenario: Option<Arc<str>>,
}

impl EmbeddedDocuments {
    /// Each document beside the metadata key it is written under.
    pub fn entries(&self) -> impl Iterator<Item = (&str, &str)> {
        let scenario = self.scenario.as_deref().map(|scenario| (provenance::SCENARIO, scenario));
        std::iter::once((provenance::PROFILE, self.profile.as_ref())).chain(scenario)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ParquetSettings {
    /// What each file's key-value metadata holds beside the provenance keys.
    #[serde(skip)]
    pub embedded: Option<EmbeddedDocuments>,
    pub compression: ParquetCompression,
    /// The codec's level, which must fall in the codec's range. Null takes the codec's default.
    pub compression_level: Option<i32>,
    /// The most rows a row group holds.
    pub row_group_rows: u64,
    pub statistics: ParquetStatistics,
    /// The columns written with a dictionary. Null gives every column one.
    pub dictionary_columns: Option<Vec<String>>,
    /// The unit every timestamp column is cast to. Null keeps each column's own. Parquet holds no
    /// seconds unit, so a column at `s` is stored at `ms`, as pyarrow stores it. parquet's Arrow
    /// writer cannot write INT96, so no unit reproduces it.
    pub timestamp_unit: Option<TimestampUnit>,
}

impl Default for ParquetSettings {
    fn default() -> Self {
        Self {
            embedded: None,
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
#[serde(default, deny_unknown_fields)]
pub struct ArrowSettings {
    /// What each file's schema metadata holds beside the provenance keys.
    #[serde(skip)]
    pub embedded: Option<EmbeddedDocuments>,
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
#[serde(default, deny_unknown_fields)]
pub struct DelimitedSettings {
    /// Null takes the format's own: `,` for CSV, a tab for TSV.
    pub delimiter: Option<char>,
    pub quote: char,
    /// The character that escapes a quote inside a quoted field. Null doubles the quote instead.
    pub escape: Option<char>,
    pub quoting: Quoting,
    /// Whether the first line names the columns.
    pub header: bool,
    /// What a null prints where the column's profile gives no missing token for it. When it is
    /// empty, an empty string is quoted so the two read back apart.
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
    /// The codec's level, which must fall in the codec's range. Null takes the codec's default.
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

/// Which fields are quoted. A null is never quoted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Quoting {
    /// Only a field holding the delimiter, the quote or a line break, and an empty string where a
    /// null prints empty.
    #[default]
    Necessary,
    Always,
    /// Every field that does not parse as a number.
    NonNumeric,
    /// None, even where a field needs it.
    Never,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Serialize, Deserialize, JsonSchema)]
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

impl TextCompression {
    /// What a file name takes after the format's extension: `.gz`, `.zst` or nothing.
    pub fn suffix(self) -> &'static str {
        match self {
            Self::None => "",
            Self::Gzip => ".gz",
            Self::Zstd => ".zst",
        }
    }

    /// The levels the codec takes, and the one it runs at when none is given, as its library has
    /// them. None for no codec.
    pub fn levels(self) -> Option<(RangeInclusive<i32>, i32)> {
        match self {
            Self::None => None,
            Self::Gzip => Some((0..=9, 6)),
            Self::Zstd => Some((1..=22, 3)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct JsonSettings {
    /// Whether a null field is written as `null` rather than left out.
    pub explicit_nulls: bool,
    pub compression: TextCompression,
    /// The codec's level, which must fall in the codec's range. Null takes the codec's default.
    pub compression_level: Option<i32>,
}
