//! Nulls written into a column. MCAR draws each row's null at `rate`. MAR and MNAR raise a row's
//! chance with its value's rank in the block, of `depends_on` or of the column itself, so the
//! chances still average `rate`.

use std::collections::HashMap;

use arrow_array::Array;
use arrow_ord::rank::rank;
use arrow_schema::Field;
use arrow_schema::SchemaRef;
use arrow_schema::SortOptions;

use super::Block;
use super::BlockPerturbation;
use super::Planned;
use super::Planning;
use super::ScenarioFault;
use super::Stage;
use super::array_edits;
use crate::plan::PlanError;
use crate::rng::unit;
use crate::scenario::Mechanism;
use crate::scenario::Missingness;

#[derive(Debug)]
struct WriteNulls {
    column: usize,
    /// The column whose rank raises a row's chance, for MAR and MNAR.
    ranked: Option<usize>,
    rate: f64,
    rows_key: u64,
}

pub(super) fn plan(op: &Missingness, planning: &Planning<'_>) -> Result<Planned, PlanError> {
    let (column, field) = planning.column(&op.column, "/column")?;
    let ranked = match (op.mechanism, &op.depends_on) {
        (Mechanism::Mar, Some(depends_on)) => Some(planning.column(depends_on, "/depends_on")?.0),
        (Mechanism::Mar, None) => {
            return Err(planning.refuse(
                ScenarioFault::InvalidParameter,
                "/mechanism",
                "MAR needs depends_on, the column its chance of a null rises with",
            ));
        }
        (_, Some(_)) => {
            return Err(planning.refuse(
                ScenarioFault::InvalidParameter,
                "/depends_on",
                "only MAR takes depends_on: MCAR depends on nothing, and MNAR on the column's own value",
            ));
        }
        (Mechanism::Mnar, None) => Some(column),
        (Mechanism::Mcar, None) => None,
    };
    if let Some(ranked) = ranked
        && rank(&arrow_array::new_empty_array(planning.schema.field(ranked).data_type()), None).is_err()
    {
        let name = planning.schema.field(ranked).name();
        return Err(planning.refuse(
            ScenarioFault::OpNotForColumn,
            if op.depends_on.is_some() { "/depends_on" } else { "/column" },
            format!("{name:?} has no order to rank its values by"),
        ));
    }
    let nullable: Field = field.as_ref().clone().with_nullable(true);
    let schema: SchemaRef = planning.schema_with(&[nullable], &[]);
    let op = WriteNulls {
        column,
        ranked,
        rate: op.fraction.0,
        rows_key: planning.key("rows"),
    };
    Ok(Planned::Rows { op: Box::new(op), schema })
}

/// Each row's share of the way up the block's order of `array`: the average rank of its value
/// among the non-null values, as a fraction strictly inside (0, 1). A null row is 0.5.
fn rank_shares(array: &dyn Array) -> Vec<f64> {
    let options = SortOptions {
        descending: false,
        nulls_first: false,
    };
    let highest = rank(array, Some(options)).expect("planning refused a column with no order");
    let mut tied: HashMap<u32, u32> = HashMap::new();
    for (row, &rank) in highest.iter().enumerate() {
        if array.is_valid(row) {
            *tied.entry(rank).or_default() += 1;
        }
    }
    let valid = (array.len() - array.null_count()) as f64;
    highest
        .iter()
        .enumerate()
        .map(|(row, &rank)| match array.is_valid(row) {
            true => {
                let average = f64::from(rank) - f64::from(tied[&rank] - 1) / 2.0;
                (average - 0.5) / valid
            }
            false => 0.5,
        })
        .collect()
}

impl BlockPerturbation for WriteNulls {
    fn apply(&self, stage: &Stage<'_>, mut block: Block) -> Block {
        let mut draws = stage.stream(self.rows_key);
        let rows: Vec<u32> = match self.ranked {
            None => array_edits::drawn_rows(&mut draws, self.rate, block.batch.num_rows()),
            Some(ranked) => {
                let reach = self.rate.min(1.0 - self.rate);
                rank_shares(block.batch.column(ranked))
                    .into_iter()
                    .enumerate()
                    .filter(|&(_, share)| unit(&mut draws) < self.rate + (2.0 * share - 1.0) * reach)
                    .map(|(row, _)| row as u32)
                    .collect()
            }
        };
        let written = array_edits::null_rows(block.batch.column(self.column), &rows);
        array_edits::drop_cells(&mut block.cells, self.column, &rows);
        block.with_column(stage.schema, self.column, written)
    }
}
