//! The format, writer settings and layout a profile's `source` section records, for a call to start
//! from and then override. `source.parquet`, `source.csv` and `source.json` take the keys the
//! manifest's `writer` holds for that format, and other keys there are left alone.

use doppeldata_core::manifest::FileRows;
use doppeldata_core::manifest::IpcCompression;
use doppeldata_core::manifest::LayoutSettings;
use doppeldata_core::manifest::Output;
use doppeldata_core::manifest::ParquetCompression;
use doppeldata_core::manifest::TextCompression;
use doppeldata_core::profile::Compression;
use doppeldata_core::profile::OpenObject;
use doppeldata_core::profile::Source;
use doppeldata_core::profile::SourceFormat;
use serde::de::DeserializeOwned;

use crate::WriteError;

/// The output and layout `source` records. `format` overrides the recorded format, and with neither
/// the output is Parquet. `source.compression` is the recorded format's codec, so it applies only
/// when the output keeps that format.
pub fn settings_from_source(source: Option<&Source>, format: Option<SourceFormat>) -> Result<(Output, LayoutSettings), WriteError> {
    let recorded = source.and_then(|source| source.format);
    let format = format.or(recorded).unwrap_or(SourceFormat::Parquet);
    let object = |pick: fn(&Source) -> Option<&OpenObject>| source.and_then(pick);
    let compression = source
        .and_then(|source| source.compression)
        .filter(|_| recorded.is_none_or(|recorded| recorded == format));
    let output = match format {
        SourceFormat::Parquet => {
            let (mut settings, named) =
                from_object::<doppeldata_core::manifest::ParquetSettings>(object(|s| s.parquet.as_ref()), "parquet")?;
            if let (Some(compression), false) = (compression, named) {
                settings.compression = parquet_compression(compression)?;
            }
            Output::Parquet(settings)
        }
        SourceFormat::Arrow => {
            let mut settings = doppeldata_core::manifest::ArrowSettings::default();
            if let Some(compression) = compression {
                settings.compression = ipc_compression(compression)?;
            }
            Output::Arrow(settings)
        }
        SourceFormat::Csv | SourceFormat::Tsv => {
            let (mut settings, named) = from_object::<doppeldata_core::manifest::DelimitedSettings>(object(|s| s.csv.as_ref()), "csv")?;
            if let (Some(compression), false) = (compression, named) {
                settings.compression = text_compression(compression)?;
            }
            match format {
                SourceFormat::Csv => Output::Csv(settings),
                _ => Output::Tsv(settings),
            }
        }
        SourceFormat::Json | SourceFormat::Ndjson => {
            let (mut settings, named) = from_object::<doppeldata_core::manifest::JsonSettings>(object(|s| s.json.as_ref()), "json")?;
            if let (Some(compression), false) = (compression, named) {
                settings.compression = text_compression(compression)?;
            }
            match format {
                SourceFormat::Json => Output::Json(settings),
                _ => Output::Ndjson(settings),
            }
        }
        SourceFormat::Orc | SourceFormat::Avro => {
            return Err(WriteError::Settings(format!(
                "doppeldata writes no {format:?}; choose parquet, arrow, csv, tsv, json or ndjson"
            )));
        }
    };
    let mut layout = LayoutSettings::default();
    if let Some(recorded) = source.and_then(|source| source.layout.as_ref()) {
        if let Some(quantiles) = &recorded.rows_per_file {
            layout.file_rows = FileRows::Drawn(quantiles.clone());
        }
        if let Some(partitioning) = &recorded.partitioning {
            layout.partition_by = partitioning.keys.iter().flatten().map(|key| key.name.clone()).collect();
            layout.partition_style = partitioning.style.unwrap_or_default();
        }
    }
    Ok((output, layout))
}

/// The settings `object` holds, and whether it names its own compression.
fn from_object<T: DeserializeOwned + Default>(object: Option<&OpenObject>, section: &str) -> Result<(T, bool), WriteError> {
    let Some(object) = object else {
        return Ok((T::default(), false));
    };
    let settings = serde_json::from_value(serde_json::Value::Object(object.clone()))
        .map_err(|error| WriteError::Settings(format!("source.{section}: {error}")))?;
    Ok((settings, object.contains_key("compression")))
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
