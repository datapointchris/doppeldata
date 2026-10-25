//! Each value named takes its share of rows, written as the column's type. Every other row keeps
//! its own value, so a write partitioned by the column puts those shares in its directories.

use arrow_array::ArrayRef;

use super::Block;
use super::BlockPerturbation;
use super::Planned;
use super::Planning;
use super::ScenarioFault;
use super::Stage;
use super::array_edits;
use super::pointer_token;
use crate::plan::PlanError;
use crate::rng::unit;
use crate::scenario::PartitionSkew;

/// The slack allowed when the shares sum to 1, so a sum off in its last bits is not refused.
const SHARE_SLACK: f64 = 1e-9;

#[derive(Debug)]
struct SkewRows {
    column: usize,
    values: ArrayRef,
    /// The running total of the shares, one per value.
    reach: Vec<f64>,
    rows_key: u64,
}

pub(super) fn plan(op: &PartitionSkew, planning: &Planning<'_>) -> Result<Planned, PlanError> {
    let (column, field) = planning.column(&op.column, "/column")?;
    let texts: Vec<Option<String>> = op.shares.keys().cloned().map(Some).collect();
    let values = array_edits::cast_texts(&texts, field.data_type()).map_err(|(index, message)| {
        let key = pointer_token(texts[index].as_deref().unwrap_or_default());
        planning.refuse(ScenarioFault::InvalidParameter, &format!("/shares/{key}"), message)
    })?;
    let reach: Vec<f64> = op
        .shares
        .values()
        .scan(0.0, |total, share| {
            *total += share.0;
            Some(*total)
        })
        .collect();
    if reach.last().is_some_and(|&total| total > 1.0 + SHARE_SLACK) {
        return Err(planning.refuse(
            ScenarioFault::InvalidParameter,
            "/shares",
            format!("the shares sum to {}, above 1", reach.last().copied().unwrap_or_default()),
        ));
    }
    Ok(planning.rows(SkewRows {
        column,
        values,
        reach,
        rows_key: planning.key("rows"),
    }))
}

impl BlockPerturbation for SkewRows {
    fn apply(&self, stage: &Stage<'_>, mut block: Block) -> Block {
        let mut draws = stage.stream(self.rows_key);
        let mut rows: Vec<u32> = Vec::new();
        let mut picks: Vec<u32> = Vec::new();
        for row in 0..block.batch.num_rows() as u32 {
            let draw = unit(&mut draws);
            if let Some(value) = self.reach.iter().position(|&reach| draw < reach) {
                rows.push(row);
                picks.push(value as u32);
            }
        }
        let written = array_edits::replace_rows(
            block.batch.column(self.column),
            &rows,
            &array_edits::take_rows(&self.values, &picks),
        );
        array_edits::drop_cells(&mut block.cells, self.column, &rows);
        block.with_column(stage.schema, self.column, written)
    }
}
