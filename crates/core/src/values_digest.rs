//! The SHA-256 the determinism contract pins over generated values. It covers every buffer of
//! every block's arrays and every text cell, so a moved value or a moved cell changes it.

use arrow_array::Array;
use arrow_array::RecordBatch;
use arrow_data::ArrayData;
use sha2::Digest;
use sha2::Sha256;

use crate::samplers::TextCell;

/// The hex SHA-256 over `blocks` in order: each block's batch with the text cells
/// [`crate::Plan::text_cells`] returned for it.
pub fn values_digest(blocks: &[(RecordBatch, Vec<Vec<TextCell>>)]) -> String {
    let mut hash = Sha256::new();
    for (batch, cells) in blocks {
        for (column, cells) in batch.columns().iter().zip(cells) {
            hash_data(&mut hash, &column.to_data());
            hash_cells(&mut hash, cells);
        }
    }
    hash.finalize().iter().map(|byte| format!("{byte:02x}")).collect()
}

fn hash_data(hash: &mut Sha256, data: &ArrayData) {
    hash.update(format!("{}:{}:{}", data.data_type(), data.len(), data.offset()));
    if let Some(nulls) = data.nulls() {
        for valid in nulls.iter() {
            hash.update([u8::from(valid)]);
        }
    }
    for buffer in data.buffers() {
        hash.update(buffer.as_slice());
    }
    for child in data.child_data() {
        hash_data(hash, child);
    }
}

fn hash_cells(hash: &mut Sha256, cells: &[TextCell]) {
    hash.update((cells.len() as u64).to_le_bytes());
    for cell in cells {
        hash.update(cell.row.to_le_bytes());
        hash.update((cell.text.len() as u64).to_le_bytes());
        hash.update(cell.text.as_bytes());
    }
}
