//! The SHA-256 the determinism contract pins over generated values. It covers every buffer of
//! every block's arrays and every text cell, so a moved value or a moved cell changes it.

use arrow_array::Array;
use arrow_array::RecordBatch;
use arrow_data::ArrayData;
use arrow_select::concat::concat_batches;
use sha2::Digest;
use sha2::Sha256;

use crate::Plan;
use crate::PlanError;
use crate::perturbations::CdcStream;
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

/// The hash a determinism pin holds over `plan`'s `blocks`: [`values_digest`], and under a `cdc`
/// its stream too, built on `threads` threads.
pub fn plan_values_digest(plan: &Plan, blocks: &[(RecordBatch, Vec<Vec<TextCell>>)], threads: usize) -> Result<String, PlanError> {
    Ok(match plan.cdc_stream(threads)? {
        Some(stream) => stream_values_digest(plan, blocks, &stream),
        None => values_digest(blocks),
    })
}

/// As [`values_digest`] over `blocks`, the full load, then every change file of `stream` in path
/// order and the table a correct merge of it produces. `plan` is the plan `stream` was built from.
fn stream_values_digest(plan: &Plan, blocks: &[(RecordBatch, Vec<Vec<TextCell>>)], stream: &CdcStream) -> String {
    let files = (0..stream.file_count()).map(|file| stream.file(file));
    let whole =
        |parts: Vec<RecordBatch>| concat_batches(&stream.expected_schema(), &parts).expect("every part carries the expected schema");
    let mut expected = vec![whole(stream.expected_rows(plan, 0..u64::MAX).collect())];
    if stream.inserts_rows() {
        expected.push(whole(stream.expected_inserts(plan).collect()));
    }
    let mut all = blocks.to_vec();
    all.extend(files.chain(expected).map(|batch| {
        let width = batch.num_columns();
        (batch, vec![Vec::new(); width])
    }));
    values_digest(&all)
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
