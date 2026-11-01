//! Two columns' values exchanged in `rate` of rows, text cells and all.

use super::Block;
use super::BlockPerturbation;
use super::Planned;
use super::Planning;
use super::ScenarioFault;
use super::Stage;
use super::array_edits;
use crate::physical_type::physical_type_name;
use crate::plan::PlanError;
use crate::samplers::TextCell;
use crate::scenario::SwappedValues;

#[derive(Debug)]
struct SwapRows {
    columns: [usize; 2],
    rate: f64,
    rows_key: u64,
}

pub(super) fn plan(op: &SwappedValues, planning: &Planning<'_>) -> Result<Planned, PlanError> {
    let (first, first_field) = planning.column(&op.columns[0], "/columns/0")?;
    let (second, second_field) = planning.column(&op.columns[1], "/columns/1")?;
    if first == second {
        return Err(planning.refuse(
            ScenarioFault::InvalidParameter,
            "/columns/1",
            "swapped_values needs two different columns",
        ));
    }
    if first_field.data_type() != second_field.data_type() {
        return Err(planning.refuse(
            ScenarioFault::InvalidParameter,
            "/columns",
            format!(
                "{:?} is {} and {:?} is {}, and swapped_values needs one type",
                op.columns[0],
                physical_type_name(first_field.data_type()),
                op.columns[1],
                physical_type_name(second_field.data_type())
            ),
        ));
    }
    let nullable = first_field.is_nullable() || second_field.is_nullable();
    let schema = planning.schema_with(
        &[
            first_field.as_ref().clone().with_nullable(nullable),
            second_field.as_ref().clone().with_nullable(nullable),
        ],
        &[],
    );
    let op = SwapRows {
        columns: [first, second],
        rate: op.fraction.0,
        rows_key: planning.key("rows"),
    };
    Ok(Planned::Rows { op: Box::new(op), schema })
}

/// `own`'s cells outside `rows`, and `other`'s inside them, in row order.
fn swapped_cells(own: &[TextCell], other: &[TextCell], rows: &[u32]) -> Vec<TextCell> {
    let swapped = |cell: &&TextCell| rows.binary_search(&cell.row).is_ok();
    let mut cells: Vec<TextCell> = own.iter().filter(|cell| !swapped(cell)).cloned().collect();
    cells.extend(other.iter().filter(swapped).cloned());
    cells.sort_by_key(|cell| cell.row);
    cells
}

impl BlockPerturbation for SwapRows {
    fn apply(&self, stage: &Stage<'_>, mut block: Block) -> Block {
        let rows = array_edits::drawn_rows(&mut stage.stream(self.rows_key), self.rate, block.batch.num_rows());
        let [first, second] = self.columns.map(|column| block.batch.column(column).clone());
        let first_written = array_edits::replace_rows(&first, &rows, &array_edits::take_rows(&second, &rows));
        let second_written = array_edits::replace_rows(&second, &rows, &array_edits::take_rows(&first, &rows));
        if let Some(cells) = block.cells.as_mut() {
            let [a, b] = self.columns;
            let (first_cells, second_cells) = (cells[a].clone(), cells[b].clone());
            cells[a] = swapped_cells(&first_cells, &second_cells, &rows);
            cells[b] = swapped_cells(&second_cells, &first_cells, &rows);
        }
        let mut columns = block.batch.columns().to_vec();
        columns[self.columns[0]] = first_written;
        columns[self.columns[1]] = second_written;
        block.with_columns(stage.schema, columns)
    }
}
