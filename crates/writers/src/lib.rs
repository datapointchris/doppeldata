//! Writes the blocks a doppeldata plan generates as Parquet, Arrow IPC, CSV, TSV, JSON or NDJSON, to
//! any object store, with a manifest that regenerates every file.
//!
//! [`layout`] says which rows each file holds before anything is generated. [`write`] generates and
//! writes them on worker threads, and [`regenerate`] writes a manifest's files again. [`claim`],
//! [`write_ranges`] and [`commit`] split one write across processes, such as Spark executors.
//! [`claim_bundle`] and [`commit_bundle`] bracket a bundle's tables, each written as one dataset.

mod bundle;
mod byte_sink;
mod cdc;
mod destination;
mod encoders;
mod file_metadata;
pub mod issue_codes;
mod layout;
mod partition;
mod planned_ranges;
mod regenerated_files;
mod size_estimate;
mod write;
mod write_options;

use arrow_schema::ArrowError;
use arrow_schema::DataType;
use doppeldata_core::BlockOutOfRange;
use doppeldata_core::issue_codes::IssueCode;

pub use bundle::bundle_manifest;
pub use bundle::claim_bundle;
pub use bundle::commit_bundle;
pub use byte_sink::ByteSink;
pub use byte_sink::WrittenObject;
pub use destination::Destination;
pub use destination::DestinationUrl;
pub use file_metadata::FileMetadataError;
pub use file_metadata::file_metadata;
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
pub use regenerated_files::DifferenceKind;
pub use regenerated_files::FieldDifference;
pub use regenerated_files::FileDifference;
pub use size_estimate::estimate_bytes;
pub use write::RangesWritten;
pub use write::RunOptions;
pub use write::SplitRun;
pub use write::WriteFailure;
pub use write::check_writable;
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
    /// `differing` names each identity key whose value the two hold differently, where it is known.
    #[error("the plan's identity {plan} is not the manifest's {manifest}{}", because_differing(differing))]
    IdentityMismatch {
        plan: String,
        manifest: String,
        differing: Vec<String>,
    },
    #[error("the manifest is partial, so it lists only what a stopped run completed; write the dataset again instead")]
    PartialManifest,
    #[error("the manifest's files embed the profile's JSON, which a plan does not hold")]
    EmbeddedProfileManifest,
    #[error("the manifest records the one file {file}, and a regeneration writes a directory of parts")]
    OneFileManifest { file: String },
    #[error("{} regenerated objects differ from the manifest's record: {}", files.len(), listed(files))]
    RegeneratedFilesDiffer { files: Vec<FileDifference> },
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

impl WriteError {
    /// The code a caller branches on, whose exit status docs/issue-codes.md lists.
    pub fn code(&self) -> &'static IssueCode {
        match self {
            Self::Layout(LayoutError::Scenario { fault, .. }) => fault.code(),
            Self::Layout(_) => &issue_codes::INVALID_LAYOUT,
            Self::Settings(_) => &issue_codes::INVALID_SETTINGS,
            Self::UnsupportedColumn { .. } => &issue_codes::UNSUPPORTED_COLUMN,
            Self::DestinationNotEmpty { .. } => &issue_codes::DESTINATION_NOT_EMPTY,
            Self::ForeignObject { .. } => &issue_codes::FOREIGN_OBJECT,
            Self::Unclaimable { .. } => &issue_codes::UNCLAIMABLE,
            Self::ObjectName { .. } => &issue_codes::INVALID_OBJECT_NAME,
            Self::DestinationClaimed { .. } => &issue_codes::DESTINATION_CLAIMED,
            Self::CurrentThreadRuntime => &issue_codes::CURRENT_THREAD_RUNTIME,
            Self::IdentityMismatch { .. } => &issue_codes::IDENTITY_MISMATCH,
            Self::PartialManifest => &issue_codes::PARTIAL_MANIFEST,
            Self::EmbeddedProfileManifest => &issue_codes::EMBEDDED_PROFILE_MANIFEST,
            Self::OneFileManifest { .. } => &issue_codes::ONE_FILE_MANIFEST,
            Self::RegeneratedFilesDiffer { .. } => &issue_codes::REGENERATED_FILES_DIFFER,
            Self::Cancelled => &issue_codes::CANCELLED,
            Self::RangeOutOfLayout { .. } => &issue_codes::RANGE_OUT_OF_LAYOUT,
            Self::RangesUnrecorded { .. } => &issue_codes::RANGES_UNRECORDED,
            Self::Format { .. } => &issue_codes::UNFORMATTABLE_VALUE,
            Self::Block(_) => &issue_codes::BLOCK_OUT_OF_RANGE,
            Self::Store(object_store::Error::Unauthenticated { .. }) => &issue_codes::NO_CREDENTIALS,
            Self::Store(object_store::Error::PermissionDenied { .. }) => &issue_codes::STORE_REFUSED,
            Self::Store(_) => &issue_codes::STORE,
            Self::Arrow(_) => &issue_codes::ARROW,
            Self::Parquet(_) => &issue_codes::PARQUET,
            Self::Io(_) => &issue_codes::LOCAL_IO,
            Self::ClaimNotRemoved { .. } => &issue_codes::CLAIM_NOT_REMOVED,
        }
    }
}

/// `, because the two differ in <keys>`, or nothing where no key is named.
fn because_differing(differing: &[String]) -> String {
    match differing {
        [] => String::new(),
        keys => format!(", because the two differ in {}", keys.join(" and ")),
    }
}

/// Each differing object as `path: what differs`, one after another.
fn listed(files: &[FileDifference]) -> String {
    files.iter().map(ToString::to_string).collect::<Vec<_>>().join("; ")
}
