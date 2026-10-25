//! The bytes a write would store, measured by encoding its first blocks with its own settings,
//! so nothing reaches a store.

use std::io;
use std::io::Write;

use doppeldata_core::Plan;

use crate::WriteError;
use crate::byte_sink::ByteSink;
use crate::byte_sink::WrittenObject;
use crate::encoders;
use crate::layout::layout;
use crate::partition::Partitioner;
use crate::partition::Piece;
use crate::planned_ranges::planned_ranges;
use crate::write_options::WriteOptions;

/// Counts what an encoder writes and keeps none of it.
#[derive(Default)]
struct CountingSink {
    bytes: u64,
}

impl Write for CountingSink {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.bytes += buf.len() as u64;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl ByteSink for CountingSink {
    fn finish(self: Box<Self>) -> Result<WrittenObject, WriteError> {
        Ok(WrittenObject {
            bytes: self.bytes,
            sha256: String::new(),
        })
    }
}

/// Blocks the larger of the two sample objects holds.
const SAMPLE_BLOCKS: u64 = 4;

/// The bytes writing `plan` with `options` would store, its manifest left out.
///
/// An object's bytes are taken as a fixed part plus a part per row. Encoding the first block as
/// one object and the first `SAMPLE_BLOCKS` as another gives both: the difference is what the
/// later blocks' rows add, and the rest of the first object is fixed, such as a header, a footer
/// and a dictionary. Each range counts as many objects as `planned_ranges` bounds it to.
pub fn estimate_bytes(plan: &Plan, options: &WriteOptions) -> Result<u64, WriteError> {
    let layout = layout(plan, &options.layout, &options.output)?;
    let partitioner = Partitioner::new(&plan.schema(), &layout.partition_by, layout.partition_style);
    encoders::check(&options.output, partitioner.file_schema())?;
    let mut pieces = Vec::new();
    for block in 0..plan.block_count().min(SAMPLE_BLOCKS) {
        let batch = plan.generate_block(block)?;
        let cells = match options.output.is_text() {
            true => plan.text_cells(block)?,
            false => vec![Vec::new(); batch.num_columns()],
        };
        pieces.push(partitioner.split(batch, cells)?);
    }
    let Some(first) = pieces.first() else {
        return Ok(0);
    };
    let encoded = |blocks: &[Vec<Piece>]| -> Result<(u64, u64), WriteError> {
        let mut encoder = encoders::open(&options.output, partitioner.file_schema(), Box::new(CountingSink::default()))?;
        let mut rows = 0;
        for piece in blocks.iter().flatten() {
            encoder.write(&piece.batch, &piece.cells)?;
            rows += piece.batch.num_rows() as u64;
        }
        Ok((rows, encoder.finish()?.bytes))
    };
    let (small_rows, small) = encoded(std::slice::from_ref(first))?;
    let (large_rows, large) = encoded(&pieces)?;
    let per_row = match large_rows > small_rows {
        true => large.saturating_sub(small) as f64 / (large_rows - small_rows) as f64,
        false => small as f64 / small_rows.max(1) as f64,
    };
    let fixed = (small as f64 - per_row * small_rows as f64).max(0.0);
    let objects: u64 = planned_ranges(plan, options)?.iter().map(|range| range.objects_at_most).sum();
    Ok((fixed * objects as f64 + per_row * plan.identity().rows() as f64).round() as u64)
}
