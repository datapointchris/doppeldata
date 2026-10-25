//! Gaussian noise of standard deviation `sigma` added to `rate` of a numeric column's rows.

use super::Block;
use super::BlockPerturbation;
use super::NumericEdit;
use super::Planned;
use super::Planning;
use super::Stage;
use super::open_unit;
use crate::plan::PlanError;
use crate::samplers::normal_quantile;
use crate::scenario::Noise;

#[derive(Debug)]
struct AddNoise {
    edit: NumericEdit,
    sigma: f64,
}

pub(super) fn plan(op: &Noise, planning: &Planning<'_>) -> Result<Planned, PlanError> {
    let edit = NumericEdit::plan(planning, &op.column, op.rate.0, "noise")?;
    Ok(planning.rows(AddNoise { edit, sigma: op.sigma }))
}

impl BlockPerturbation for AddNoise {
    fn apply(&self, stage: &Stage<'_>, block: Block) -> Block {
        self.edit.apply(stage, block, |value, values| {
            value + self.sigma * normal_quantile(open_unit(values))
        })
    }
}
