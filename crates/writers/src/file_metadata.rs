//! The key-value metadata of a local Parquet or Arrow IPC file, where a generated dataset's
//! provenance keys and an embedded profile are.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::BufReader;
use std::io::Read;
use std::path::Path;

use arrow_ipc::reader::FileReader;
use arrow_ipc::reader::StreamReader;
use parquet::file::reader::FileReader as _;
use parquet::file::reader::SerializedFileReader;

/// Why a file's metadata could not be read.
#[derive(Debug, thiserror::Error)]
pub enum FileMetadataError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("parquet: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("arrow: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),
}

const PARQUET_MAGIC: &[u8] = b"PAR1";
const ARROW_FILE_MAGIC: &[u8] = b"ARROW1";
/// The continuation marker an Arrow IPC stream's first message opens with.
const ARROW_STREAM_MARKER: &[u8] = &[0xFF; 4];

/// The key-value metadata of the Parquet or Arrow IPC file at `path`, or None for a file of
/// neither format, told apart by the bytes it opens with.
pub fn file_metadata(path: &Path) -> Result<Option<BTreeMap<String, String>>, FileMetadataError> {
    let mut head = Vec::with_capacity(ARROW_FILE_MAGIC.len());
    File::open(path)?.take(ARROW_FILE_MAGIC.len() as u64).read_to_end(&mut head)?;
    if head.starts_with(PARQUET_MAGIC) {
        let reader = SerializedFileReader::new(File::open(path)?)?;
        let pairs = reader.metadata().file_metadata().key_value_metadata().cloned().unwrap_or_default();
        return Ok(Some(
            pairs
                .into_iter()
                .filter_map(|pair| pair.value.map(|value| (pair.key, value)))
                .collect(),
        ));
    }
    let schema = if head.starts_with(ARROW_FILE_MAGIC) {
        FileReader::try_new(BufReader::new(File::open(path)?), None)?.schema()
    } else if head.starts_with(ARROW_STREAM_MARKER) {
        StreamReader::try_new(BufReader::new(File::open(path)?), None)?.schema()
    } else {
        return Ok(None);
    };
    Ok(Some(schema.metadata().clone().into_iter().collect()))
}
