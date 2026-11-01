//! `rate` of a numeric column's rows set between `multiplier` and `multiplier + 1` spans past its
//! trimmed extremes: above the high one, or below the low one in a column that goes below zero.

use super::Block;
use super::BlockPerturbation;
use super::NumericEdit;
use super::Planned;
use super::Planning;
use super::Stage;
use crate::plan::PlanError;
use crate::rng::unit;
use crate::scenario::Outliers;

#[derive(Debug)]
struct PlaceOutliers {
    edit: NumericEdit,
    low: f64,
    high: f64,
    span: f64,
    multiplier: f64,
}

pub(super) fn plan(op: &Outliers, planning: &Planning<'_>) -> Result<Planned, PlanError> {
    let edit = NumericEdit::plan(planning, &op.column, op.fraction.0, "outliers")?;
    let (low, high) = planning.numeric_extremes(&op.column);
    let span = match high > low {
        true => high - low,
        false => high.abs().max(1.0),
    };
    Ok(planning.rows(PlaceOutliers {
        edit,
        low,
        high,
        span,
        multiplier: op.multiplier,
    }))
}

impl BlockPerturbation for PlaceOutliers {
    fn apply(&self, stage: &Stage<'_>, block: Block) -> Block {
        self.edit.apply(stage, block, |_, values| {
            let below = self.low < 0.0 && unit(values) < 0.5;
            let distance = self.span * (self.multiplier + unit(values));
            match below {
                true => self.low - distance,
                false => self.high + distance,
            }
        })
    }
}
