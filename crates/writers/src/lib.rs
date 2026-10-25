//! Writes the blocks a doppeldata plan generates as Parquet, Arrow IPC, CSV, TSV, JSON or NDJSON, to
//! any object store, with a manifest that regenerates every file.
//!
//! [`layout`] says which rows each file holds before anything is generated. [`write`] generates and
//! writes them on worker threads, and [`regenerate`] writes a manifest's files again.

mod byte_sink;
mod destination;
mod encoders;
mod layout;
mod partition;
mod source_settings;
mod write;

use arrow_schema::ArrowError;
use arrow_schema::DataType;
use doppeldata_core::BlockOutOfRange;

pub use byte_sink::ByteSink;
pub use byte_sink::WrittenObject;
pub use destination::Destination;
pub use layout::FileLayout;
pub use layout::FileRange;
pub use layout::LayoutError;
pub use layout::PART_PREFIX;
pub use layout::layout;
pub use layout::layout_from_manifest;
pub use partition::DEFAULT_PARTITION;
pub use source_settings::settings_from_source;
pub use write::RunOptions;
pub use write::WriteFailure;
pub use write::WriteOptions;
pub use write::regenerate;
pub use write::write;

#[derive(Debug, thiserror::Error)]
pub enum WriteError {
    #[error(transparent)]
    Layout(#[from] LayoutError),
    #[error("{0}")]
    Settings(String),
    #[error("column {column:?} is {data_type}, which {format} cannot hold")]
    UnsupportedColumn {
        column: String,
        data_type: DataType,
        format: &'static str,
    },
    #[error("{url} already holds {objects} objects, such as {example}; replace deletes the ones this tool wrote")]
    DestinationNotEmpty { url: String, objects: usize, example: String },
    #[error("{url} holds {object}, which this tool did not write, so replace refuses to mix its files with it")]
    ForeignObject { url: String, object: String },
    #[error("object name {name:?}: {reason}")]
    ObjectName { name: String, reason: String },
    #[error("the plan's identity {plan} is not the manifest's {manifest}")]
    IdentityMismatch { plan: String, manifest: String },
    #[error("the write was cancelled")]
    Cancelled,
    #[error("a value in column {column:?} would not format as text")]
    Format { column: String },
    #[error(transparent)]
    Block(#[from] BlockOutOfRange),
    #[error("object store: {0}")]
    Store(#[from] object_store::Error),
    #[error("arrow: {0}")]
    Arrow(#[from] ArrowError),
    #[error("parquet: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("csv: {0}")]
    Csv(#[from] csv::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}
