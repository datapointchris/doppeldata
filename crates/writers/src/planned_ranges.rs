//! The row ranges a write would make, each with the path its objects take and how many objects it
//! can hold, known before anything is generated.

use doppeldata_core::Plan;

use crate::WriteError;
use crate::layout::layout;
use crate::partition::Partitioner;
use crate::write_options::WriteOptions;

/// One row range of a planned write, spelled as the manifest spells the objects it becomes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedRange {
    /// The object path under the destination. A partitioned range writes one object per value, so
    /// each partition directory is `*`, as in `status=*/part-00000-1f2e3d4c.parquet`.
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
            path: format!("{directories}{}", layout.file_name(range)),
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
