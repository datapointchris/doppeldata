//! `rate` of a numeric column's rows multiplied by `factor`, as a unit error writes them.

use super::Block;
use super::BlockPerturbation;
use super::NumericEdit;
use super::Planned;
use super::Planning;
use super::Stage;
use crate::plan::PlanError;
use crate::scenario::Scaling;

#[derive(Debug)]
struct Scale {
    edit: NumericEdit,
    factor: f64,
}

pub(super) fn plan(op: &Scaling, planning: &Planning<'_>) -> Result<Planned, PlanError> {
    let edit = NumericEdit::plan(planning, &op.column, op.rate.0, "scaling")?;
    Ok(planning.rows(Scale { edit, factor: op.factor }))
}

impl BlockPerturbation for Scale {
    fn apply(&self, stage: &Stage<'_>, block: Block) -> Block {
        self.edit.apply(stage, block, |value, _| value * self.factor)
    }
}
