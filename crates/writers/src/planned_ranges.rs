//! The row ranges a write would make, each with the path its objects take and how many objects it
//! can hold, and the change stream a `cdc` write makes beside them, known before anything is
//! generated.

use doppeldata_core::Plan;
use doppeldata_core::format_settings::Output;
use doppeldata_core::manifest::ChangeCounts;

use crate::WriteError;
use crate::cdc;
use crate::layout::layout;
use crate::partition::Partitioner;
use crate::write::worker_threads;
use crate::write_options::WriteOptions;

/// One row range of a planned write, spelled as the manifest spells the objects it becomes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedRange {
    /// The object path under the destination. A partitioned range writes one object per value, so
    /// each partition directory is `*`, as in `status=*/part-00000-1f2e3d4c.parquet`. Under `cdc`
    /// it is the range's full-load file.
    pub path: String,
    pub range: u32,
    pub first_row: u64,
    pub range_rows: u64,
    /// One unpartitioned. Partitioned, one per partition value the range can hold.
    pub objects_at_most: u64,
}

/// Every range writing `plan` with `options` would make, in row order.
pub fn planned_ranges(plan: &Plan, options: &WriteOptions) -> Result<Vec<PlannedRange>, WriteError> {
    let layout = layout(plan, &options.layout, &options.output)?;
    let directories = Partitioner::new(&plan.schema(), &layout.partition_by, layout.partition_style).directory_pattern();
    Ok(layout
        .ranges
        .iter()
        .map(|range| PlannedRange {
            path: match plan.cdc() {
                Some(cdc) => format!("{}.{}", cdc.load_stem(range.index as usize), layout.extension),
                None => format!("{directories}{}", layout.file_name(range)),
            },
            range: range.index,
            first_row: range.first_row,
            range_rows: range.rows,
            objects_at_most: match layout.partition_by.is_empty() {
                true => 1,
                false => range.rows.min(options.layout.partition_values),
            },
        })
        .collect())
}

/// The change stream a `cdc` write makes beside its full load.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedStream {
    /// The layout the stream takes, as the op's `style` names it.
    pub style: &'static str,
    /// Where the full load and the change files lie under the destination.
    pub prefix: String,
    /// Where the expected table lies under the destination.
    pub expected_prefix: String,
    /// The changes the stream holds, counted by operation, a fault's second delivery left out.
    pub changes: ChangeCounts,
    /// A file whose every change a fault moves later is left unwritten, so the write may make fewer.
    pub change_files_at_most: u64,
    /// One for each range, and one for the inserted rows where the stream inserts any.
    pub expected_parts: u64,
}

/// The stream writing `plan` with `options` would make, or None without a `cdc`. Each change is
/// drawn to count it, and refused where the write would refuse it.
pub fn planned_stream(plan: &Plan, options: &WriteOptions) -> Result<Option<PlannedStream>, WriteError> {
    let Some(cdc) = plan.cdc() else {
        return Ok(None);
    };
    let layout = layout(plan, &options.layout, &options.output)?;
    let outline = plan
        .cdc_outline(worker_threads(0))
        .map_err(cdc::refusal)?
        .expect("a cdc plan outlines a stream");
    Ok(Some(PlannedStream {
        style: cdc.style(),
        prefix: cdc.prefix().into(),
        expected_prefix: cdc.expected_prefix(),
        changes: outline.changes,
        change_files_at_most: outline.change_files_at_most,
        expected_parts: layout.ranges.len() as u64 + u64::from(outline.changes.insert > 0),
    }))
}

/// The format and settings the write's manifest records: the options', with the stream's
/// timestamp unit under a `cdc` writing Parquet.
pub fn planned_output(plan: &Plan, options: &WriteOptions) -> Output {
    match plan.cdc() {
        Some(cdc) => cdc::stream_output(&options.output, cdc),
        None => options.output.clone(),
    }
}
