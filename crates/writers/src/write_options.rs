//! What to write, started from what a profile's `source` section records and laid over with a
//! caller's choices. The CLI, the SDK and the example all build their options here.

use std::sync::Arc;

use doppeldata_core::format_settings::ArrowSettings;
use doppeldata_core::format_settings::EmbeddedDocuments;
use doppeldata_core::format_settings::IpcCompression;
use doppeldata_core::format_settings::JsonSettings;
use doppeldata_core::format_settings::Output;
use doppeldata_core::format_settings::ParquetCompression;
use doppeldata_core::format_settings::ParquetSettings;
use doppeldata_core::format_settings::TextCompression;
use doppeldata_core::manifest::FileRows;
use doppeldata_core::manifest::LayoutSettings;
use doppeldata_core::manifest::ManifestScenario;
use doppeldata_core::profile::Compression;
use doppeldata_core::profile::PartitionStyle;
use doppeldata_core::profile::Profile;
use doppeldata_core::profile::Quantiles;
use doppeldata_core::profile::SourceFormat;
use doppeldata_core::profile::TableProfile;

use crate::WriteError;

/// What to write: the format and its settings, how rows split into files, and the profile's path
/// and the scenario's record for the manifest to name.
#[derive(Debug, Clone, Default)]
pub struct WriteOptions {
    pub output: Output,
    pub layout: LayoutSettings,
    pub profile_path: Option<String>,
    pub scenario: Option<ManifestScenario>,
}

/// What a caller chooses over a profile's recorded settings. A field left `None` keeps what the
/// profile records. It reads from JSON, so a binding passes every choice as one object.
#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WriteChoices {
    pub format: Option<SourceFormat>,
    /// Keys of the format's settings, as the manifest's `writer` holds them.
    pub writer: Option<serde_json::Map<String, serde_json::Value>>,
    pub file_rows: Option<u64>,
    /// The partition keys. An empty list writes no partitions, whatever the profile records.
    pub partition_by: Option<Vec<String>>,
    pub partition_style: Option<PartitionStyle>,
    pub profile_path: Option<String>,
    /// The scenario applied, as the manifest records it.
    pub scenario: Option<ManifestScenario>,
    /// The whole profile's canonical JSON, for each file's metadata to hold. Only a Parquet or Arrow
    /// IPC write takes it.
    pub embedded_profile: Option<String>,
    /// The scenario's canonical document, held beside an embedded profile.
    pub embedded_scenario: Option<String>,
}

impl WriteOptions {
    /// The options `profile` records, with `choices` laid over them in the order the manifest's
    /// fields depend on: the format, its settings, the file size, then the partitions. A
    /// scenario's `scenario_file_rows` sizes the files over the profile's, and under a size the
    /// choices give.
    pub fn chosen(profile: &Profile, scenario_file_rows: Option<&Quantiles>, choices: WriteChoices) -> Result<Self, WriteError> {
        let mut options = Self::from_profile(profile, choices.format)?;
        options.profile_path = choices.profile_path;
        options.scenario = choices.scenario;
        if let Some(writer) = choices.writer {
            options.override_writer(writer)?;
        }
        if let Some(quantiles) = scenario_file_rows {
            options.layout.file_rows = FileRows::Drawn(quantiles.clone());
        }
        if let Some(file_rows) = choices.file_rows {
            options.layout.file_rows = FileRows::Fixed(file_rows);
        }
        if let Some(keys) = choices.partition_by {
            options.partition_by(profile, keys);
        }
        if let Some(style) = choices.partition_style {
            options.layout.partition_style = style;
        }
        if let Some(profile) = choices.embedded_profile {
            options.embed(EmbeddedDocuments {
                profile: profile.into(),
                scenario: choices.embedded_scenario.map(Arc::from),
            })?;
        }
        Ok(options)
    }

    /// Embeds `documents` in each file's metadata, which only Parquet and Arrow IPC files hold.
    fn embed(&mut self, documents: EmbeddedDocuments) -> Result<(), WriteError> {
        match &mut self.output {
            Output::Parquet(settings) => settings.embedded = Some(documents),
            Output::Arrow(settings) => settings.embedded = Some(documents),
            other => {
                return Err(WriteError::Settings(format!(
                    "a {} file holds no metadata to embed the profile in; embed it in a parquet or arrow write",
                    other.format_name()
                )));
            }
        }
        Ok(())
    }

    /// The options `profile`'s `source` records. `format` overrides the recorded format, and with
    /// neither the output is Parquet.
    ///
    /// `source.compression` is the recorded format's codec, so it applies only when the output keeps
    /// that format and the format's own section leaves its codec at the default. A recorded partition
    /// key that names no column is dropped, because the source held its value only in its paths.
    pub fn from_profile(profile: &Profile, format: Option<SourceFormat>) -> Result<Self, WriteError> {
        let table = match profile {
            Profile::Table(table) => Some(table.as_ref()),
            Profile::Bundle(_) => None,
        };
        let source = table.and_then(|table| table.source.as_ref());
        let recorded = source.and_then(|source| source.format);
        let format = format.or(recorded).unwrap_or(SourceFormat::Parquet);
        let compression = source
            .and_then(|source| source.compression)
            .filter(|_| recorded.is_none_or(|recorded| recorded == format));
        let output = match format {
            SourceFormat::Parquet => {
                let mut settings = source
                    .and_then(|source| source.parquet.as_ref())
                    .map(|parquet| parquet.writer.clone())
                    .unwrap_or_default();
                if let Some(compression) = compression
                    && settings.compression == ParquetSettings::default().compression
                {
                    settings.compression = parquet_compression(compression)?;
                }
                Output::Parquet(settings)
            }
            SourceFormat::Arrow => {
                let mut settings = ArrowSettings::default();
                if let Some(compression) = compression {
                    settings.compression = ipc_compression(compression)?;
                }
                Output::Arrow(settings)
            }
            SourceFormat::Csv | SourceFormat::Tsv => {
                let mut settings = source
                    .and_then(|source| source.csv.as_ref())
                    .map(|csv| csv.writer.clone())
                    .unwrap_or_default();
                if let Some(compression) = compression
                    && settings.compression == TextCompression::None
                {
                    settings.compression = text_compression(compression)?;
                }
                match format {
                    SourceFormat::Csv => Output::Csv(settings),
                    _ => Output::Tsv(settings),
                }
            }
            SourceFormat::Json | SourceFormat::Ndjson => {
                let mut settings: JsonSettings = source
                    .and_then(|source| source.json.as_ref())
                    .map(|json| json.writer.clone())
                    .unwrap_or_default();
                if let Some(compression) = compression
                    && settings.compression == TextCompression::None
                {
                    settings.compression = text_compression(compression)?;
                }
                match format {
                    SourceFormat::Json => Output::Json(settings),
                    _ => Output::Ndjson(settings),
                }
            }
            SourceFormat::Orc | SourceFormat::Avro => {
                return Err(WriteError::Settings(format!("doppeldata writes no {format:?} files")));
            }
        };
        let mut options = Self { output, ..Self::default() };
        if let Some(layout) = source.and_then(|source| source.layout.as_ref()) {
            if let Some(quantiles) = &layout.rows_per_file {
                options.layout.file_rows = FileRows::Drawn(quantiles.clone());
            }
            if let Some(partitioning) = &layout.partitioning {
                let keys = partitioning
                    .keys
                    .iter()
                    .flatten()
                    .map(|key| key.name.clone())
                    .filter(|name| table.is_some_and(|table| column(table, name).is_some()))
                    .collect();
                options.partition_by(profile, keys);
                options.layout.partition_style = partitioning.style.unwrap_or_default();
            }
        }
        Ok(options)
    }

    /// Lays `overrides`, keyed as the manifest's `writer` is, over the format's settings. A key the
    /// format does not take is refused.
    pub fn override_writer(&mut self, overrides: serde_json::Map<String, serde_json::Value>) -> Result<(), WriteError> {
        let mut document = serde_json::to_value(&self.output).expect("an output always serializes");
        document["writer"]
            .as_object_mut()
            .expect("every format's settings serialize as an object")
            .extend(overrides);
        self.output = serde_json::from_value(document).map_err(|error| WriteError::Settings(format!("writer: {error}")))?;
        Ok(())
    }

    /// Partitions by `keys`, counting the values they take together from `profile`: a key's recorded
    /// partition values, else its column's distinct count, else one.
    pub fn partition_by(&mut self, profile: &Profile, keys: Vec<String>) {
        let table = match profile {
            Profile::Table(table) => Some(table.as_ref()),
            Profile::Bundle(_) => None,
        };
        self.layout.partition_values = keys
            .iter()
            .map(|key| table.map_or(1, |table| key_values(table, key)))
            .fold(1, u64::saturating_mul);
        self.layout.partition_by = keys;
    }
}

fn column<'a>(table: &'a TableProfile, name: &str) -> Option<&'a doppeldata_core::profile::Column> {
    table.columns.iter().flatten().find(|column| column.name == name)
}

fn key_values(table: &TableProfile, key: &str) -> u64 {
    let recorded = table
        .source
        .as_ref()
        .and_then(|source| source.layout.as_ref())
        .and_then(|layout| layout.partitioning.as_ref())
        .and_then(|partitioning| partitioning.keys.iter().flatten().find(|recorded| recorded.name == key))
        .and_then(|recorded| recorded.values.as_ref())
        .and_then(|values| values.count);
    let distinct = column(table, key)
        .and_then(|column| column.distinct.as_ref())
        .and_then(|distinct| distinct.approx);
    recorded.or(distinct).unwrap_or(1).max(1)
}

fn unsupported(compression: Compression, format: &str) -> WriteError {
    WriteError::Settings(format!(
        "source.compression {compression:?} is not one {format} can be written with"
    ))
}

fn parquet_compression(compression: Compression) -> Result<ParquetCompression, WriteError> {
    Ok(match compression {
        Compression::None => ParquetCompression::None,
        Compression::Gzip => ParquetCompression::Gzip,
        Compression::Zstd => ParquetCompression::Zstd,
        Compression::Snappy => ParquetCompression::Snappy,
        Compression::Lz4 => ParquetCompression::Lz4,
        Compression::Bzip2 | Compression::Lzo => return Err(unsupported(compression, "parquet")),
    })
}

fn ipc_compression(compression: Compression) -> Result<IpcCompression, WriteError> {
    Ok(match compression {
        Compression::None => IpcCompression::None,
        Compression::Lz4 => IpcCompression::Lz4,
        Compression::Zstd => IpcCompression::Zstd,
        _ => return Err(unsupported(compression, "arrow")),
    })
}

fn text_compression(compression: Compression) -> Result<TextCompression, WriteError> {
    Ok(match compression {
        Compression::None => TextCompression::None,
        Compression::Gzip => TextCompression::Gzip,
        Compression::Zstd => TextCompression::Zstd,
        _ => return Err(unsupported(compression, "text")),
    })
}
