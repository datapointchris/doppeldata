//! The bytes a write would store, measured by encoding its first blocks with its own settings,
//! so nothing reaches a store.

use std::io;
use std::io::Write;

use doppeldata_core::BLOCK_ROWS;
use doppeldata_core::Plan;
use doppeldata_core::ScenarioFault;
use doppeldata_core::format_settings::DelimitedSettings;
use doppeldata_core::format_settings::Output;
use doppeldata_core::format_settings::TextCompression;
use doppeldata_core::scenario::FaultKind;

use crate::FIRST_BLOCK_BYTES;
use crate::WriteError;
use crate::byte_sink::ByteSink;
use crate::byte_sink::WrittenObject;
use crate::encoders;
use crate::encoders::Faulting;
use crate::layout::FileLayout;
use crate::layout::LayoutError;
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

/// An object's bytes, taken as a fixed part plus a part per row it writes.
struct ObjectBytes {
    /// Such as a header, a footer and a dictionary.
    fixed: f64,
    per_row: f64,
    /// The rows written for each row generated, which a scenario's added rows raise.
    written_per_generated: f64,
}

impl ObjectBytes {
    /// An object's bytes when its range generates `rows`.
    fn of(&self, rows: u64) -> f64 {
        self.fixed + self.per_row * self.written_per_generated * rows as f64
    }
}

/// Encoding the first block as one object and the first `SAMPLE_BLOCKS` as another gives both
/// parts: the difference is what the later blocks' rows add, and the rest of the first object is
/// fixed. None for a plan of no rows.
fn object_bytes(plan: &Plan, partitioner: &Partitioner, output: &Output) -> Result<Option<ObjectBytes>, WriteError> {
    let mut pieces = Vec::new();
    let mut generated_rows = 0;
    for block in 0..plan.block_count().min(SAMPLE_BLOCKS) {
        let placed = plan.placed_block(block, output.is_text())?;
        generated_rows += (plan.identity().rows() - block * BLOCK_ROWS).min(BLOCK_ROWS);
        pieces.push(partitioner.split(placed)?);
    }
    let Some(first) = pieces.first() else {
        return Ok(None);
    };
    let faulting = plan.malformed().map(|_| Faulting::default());
    let encoded = |blocks: &[Vec<Piece>]| -> Result<(u64, u64), WriteError> {
        let mut encoder = encoders::open(output, partitioner.file_schema(), Box::new(CountingSink::default()), faulting)?;
        let mut rows = 0;
        for piece in blocks.iter().flatten() {
            encoder.write(&piece.batch, &piece.cells, &piece.faults)?;
            rows += piece.batch.num_rows() as u64;
        }
        Ok((rows, encoder.finish()?.object.bytes))
    };
    let (small_rows, small) = encoded(std::slice::from_ref(first))?;
    let (large_rows, large) = encoded(&pieces)?;
    let per_row = match large_rows > small_rows {
        true => large.saturating_sub(small) as f64 / (large_rows - small_rows) as f64,
        false => small as f64 / small_rows.max(1) as f64,
    };
    Ok(Some(ObjectBytes {
        fixed: (small as f64 - per_row * small_rows as f64).max(0.0),
        per_row,
        written_per_generated: large_rows as f64 / generated_rows.max(1) as f64,
    }))
}

/// The bytes writing `plan` with `options` would store, its manifest left out. Each range counts
/// as many objects as `planned_ranges` bounds it to.
pub fn estimate_bytes(plan: &Plan, options: &WriteOptions) -> Result<u64, WriteError> {
    let layout = layout(plan, &options.layout, &options.output)?;
    let partitioner = Partitioner::new(&plan.schema(), &layout.partition_by, layout.partition_style);
    encoders::check(&options.output, partitioner.file_schema())?;
    let Some(bytes) = object_bytes(plan, &partitioner, &options.output)? else {
        return Ok(0);
    };
    let objects: u64 = planned_ranges(plan, options)?.iter().map(|range| range.objects_at_most).sum();
    let rows_part = bytes.per_row * bytes.written_per_generated * plan.identity().rows() as f64;
    Ok((bytes.fixed * objects as f64 + rows_part).round() as u64)
}

/// Refuses `invalid_utf8` where no file `layout` makes reaches past its first block, the only part
/// of a file the encoder writes one in. A range's whole row count bounds each of its objects, so a
/// write that could place one is never refused.
pub(crate) fn check_invalid_utf8_reach(plan: &Plan, layout: &FileLayout, output: &Output) -> Result<(), WriteError> {
    let Some((_, field)) = plan
        .malformed()
        .and_then(|malformed| malformed.kinds().iter().find(|(kind, _)| *kind == FaultKind::InvalidUtf8))
    else {
        return Ok(());
    };
    // The first block is counted before compression.
    let uncompressed = |settings: &DelimitedSettings| DelimitedSettings {
        compression: TextCompression::None,
        compression_level: None,
        ..settings.clone()
    };
    let output = match output {
        Output::Csv(settings) => Output::Csv(uncompressed(settings)),
        Output::Tsv(settings) => Output::Tsv(uncompressed(settings)),
        _ => return Ok(()),
    };
    let partitioner = Partitioner::new(&plan.schema(), &layout.partition_by, layout.partition_style);
    let Some(bytes) = object_bytes(plan, &partitioner, &output)? else {
        return Ok(());
    };
    let largest = layout.ranges.iter().map(|range| range.rows).max().unwrap_or(0);
    if bytes.of(largest) > FIRST_BLOCK_BYTES as f64 {
        return Ok(());
    }
    let rows = ((FIRST_BLOCK_BYTES as f64 - bytes.fixed) / (bytes.per_row * bytes.written_per_generated)).ceil();
    let message = format!(
        "invalid_utf8 is written only past a file's first {FIRST_BLOCK_BYTES} bytes, which doppeldata's profiler reads the encoding from, and the largest file holds about {:.0} bytes; write files of more than {rows:.0} rows",
        bytes.of(largest)
    );
    Err(LayoutError::Scenario {
        fault: ScenarioFault::InvalidParameter,
        field: field.clone(),
        message,
    }
    .into())
}
