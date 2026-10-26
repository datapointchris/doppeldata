//! Writes the blocks a doppeldata plan generates as Parquet, Arrow IPC, CSV, TSV, JSON or NDJSON, to
//! any object store, with a manifest that regenerates every file.
//!
//! [`layout`] says which rows each file holds before anything is generated. [`write`] generates and
//! writes them on worker threads, and [`regenerate`] writes a manifest's files again. [`claim`],
//! [`write_ranges`] and [`commit`] split one write across processes, such as Spark executors.

mod byte_sink;
mod cdc;
mod destination;
mod encoders;
mod layout;
mod partition;
mod planned_ranges;
mod size_estimate;
mod write;
mod write_options;

use arrow_schema::ArrowError;
use arrow_schema::DataType;
use doppeldata_core::BlockOutOfRange;

pub use byte_sink::ByteSink;
pub use byte_sink::WrittenObject;
pub use destination::Destination;
pub use destination::DestinationUrl;
pub use layout::FileLayout;
pub use layout::FileRange;
pub use layout::LayoutError;
pub use layout::PART_PREFIX;
pub use layout::layout;
pub use layout::layout_from_manifest;
pub use partition::DEFAULT_PARTITION;
pub use planned_ranges::PlannedRange;
pub use planned_ranges::PlannedStream;
pub use planned_ranges::file_columns;
pub use planned_ranges::planned_output;
pub use planned_ranges::planned_ranges;
pub use planned_ranges::planned_stream;
pub use size_estimate::estimate_bytes;
pub use write::RangesWritten;
pub use write::RunOptions;
pub use write::SplitRun;
pub use write::WriteFailure;
pub use write::claim;
pub use write::commit;
pub use write::regenerate;
pub use write::write;
pub use write::write_ranges;
pub use write_options::WriteChoices;
pub use write_options::WriteOptions;

/// The head of a text file doppeldata's profiler reads its dialect, JSON layout and encoding from.
/// One invalid UTF-8 byte there reads the file as Latin-1, where no row is rejected, so no
/// `invalid_utf8` fault starts in it.
pub const FIRST_BLOCK_BYTES: u64 = 64 * 1024;

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
    #[error(
        "{url} already holds {objects} objects, such as {example}; a write with replace set deletes the objects an earlier doppeldata write recorded"
    )]
    DestinationNotEmpty { url: String, objects: usize, example: String },
    #[error("{url} holds {object}, which no doppeldata manifest there accounts for, so replace leaves the destination as it is")]
    ForeignObject { url: String, object: String },
    #[error("another write claimed {url} first, and two writes never share a destination")]
    DestinationClaimed { url: String },
    #[error(
        "{url}'s store cannot create an object only where none exists, so the write cannot claim it; enable conditional put on the store"
    )]
    Unclaimable { url: String },
    #[error(
        "the supplied runtime is current-thread, which nothing drives while the write blocks its caller; pass a multi-thread runtime's handle, or none"
    )]
    CurrentThreadRuntime,
    #[error("object name {name:?}: {reason}")]
    ObjectName { name: String, reason: String },
    #[error("the plan's identity {plan} is not the manifest's {manifest}")]
    IdentityMismatch { plan: String, manifest: String },
    #[error("the manifest is partial, so it lists only what a stopped run completed; write the dataset again instead")]
    PartialManifest,
    #[error("{} regenerated objects differ from the manifest's record: {}", files.len(), files.join(", "))]
    RegeneratedFilesDiffer { files: Vec<String> },
    #[error("the write was cancelled")]
    Cancelled,
    #[error("range {range} is past the layout's {ranges} ranges")]
    RangeOutOfLayout { range: u32, ranges: usize },
    #[error("{missing} of the layout's {ranges} ranges have no object recorded, the first being range {first}")]
    RangesUnrecorded { missing: usize, ranges: usize, first: u32 },
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
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// The write finished and stored `_manifest.json`, then could not delete its claim.
    #[error("{url}/_manifest.json records every file, but the claim _manifest.partial.json could not be removed: {error}")]
    ClaimNotRemoved { url: String, error: Box<WriteError> },
}

/// What a failed write tells its caller to do about it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureKind {
    /// The arguments cannot be written as given, so the call must change. Nothing was written.
    Refused,
    /// The write was attempted and failed.
    Failed,
}

impl WriteError {
    /// The kebab-case code a caller branches on, and what kind of failure it is.
    pub fn code(&self) -> (&'static str, FailureKind) {
        use FailureKind::Failed;
        use FailureKind::Refused;
        match self {
            Self::Layout(LayoutError::Scenario { fault, .. }) => (fault.code(), Refused),
            Self::Layout(_) => ("invalid-layout", Refused),
            Self::Settings(_) => ("invalid-settings", Refused),
            Self::UnsupportedColumn { .. } => ("unsupported-column", Refused),
            Self::DestinationNotEmpty { .. } => ("destination-not-empty", Refused),
            Self::ForeignObject { .. } => ("foreign-object", Refused),
            Self::Unclaimable { .. } => ("unclaimable", Refused),
            Self::ObjectName { .. } => ("invalid-object-name", Refused),
            Self::DestinationClaimed { .. } => ("destination-claimed", Failed),
            Self::CurrentThreadRuntime => ("current-thread-runtime", Failed),
            Self::IdentityMismatch { .. } => ("identity-mismatch", Failed),
            Self::PartialManifest => ("partial-manifest", Failed),
            Self::RegeneratedFilesDiffer { .. } => ("regenerated-files-differ", Failed),
            Self::Cancelled => ("cancelled", Failed),
            Self::RangeOutOfLayout { .. } => ("range-out-of-layout", Refused),
            Self::RangesUnrecorded { .. } => ("ranges-unrecorded", Failed),
            Self::Format { .. } => ("unformattable-value", Failed),
            Self::Block(_) => ("block-out-of-range", Failed),
            Self::Store(object_store::Error::Unauthenticated { .. }) => ("no-credentials", Failed),
            Self::Store(object_store::Error::PermissionDenied { .. }) => ("store-refused", Failed),
            Self::Store(_) => ("store", Failed),
            Self::Arrow(_) => ("arrow", Failed),
            Self::Parquet(_) => ("parquet", Failed),
            Self::Io(_) => ("io", Failed),
            Self::ClaimNotRemoved { .. } => ("claim-not-removed", Failed),
        }
    }
}
