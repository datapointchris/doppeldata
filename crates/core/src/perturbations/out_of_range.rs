//! `rate` of rows take a sentinel from `values`, or a number past the column's extreme on the side
//! `beyond` names.

use arrow_array::Array;
use arrow_array::ArrayRef;

use super::Block;
use super::BlockPerturbation;
use super::Planned;
use super::Planning;
use super::ScenarioFault;
use super::Stage;
use super::array_edits;
use crate::plan::PlanError;
use crate::rng::below;
use crate::rng::unit;
use crate::samplers::NumericOutput;
use crate::scenario::OutOfRange;
use crate::scenario::Side;

#[derive(Debug)]
enum Written {
    Sentinels(ArrayRef),
    /// Between one and two spans past `edge`, on the side `toward` is the sign of.
    Beyond {
        output: NumericOutput,
        edge: f64,
        span: f64,
        toward: f64,
    },
}

#[derive(Debug)]
struct OutOfRangeRows {
    column: usize,
    written: Written,
    rate: f64,
    rows_key: u64,
    values_key: u64,
}

/// A JSON scalar as the text a cast to the column's type reads.
fn as_text(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::Null => None,
        serde_json::Value::String(text) => Some(text.clone()),
        other => Some(other.to_string()),
    }
}

pub(super) fn plan(op: &OutOfRange, planning: &Planning<'_>) -> Result<Planned, PlanError> {
    let (column, field) = planning.column(&op.column, "/column")?;
    let written = match (&op.values, op.beyond) {
        (Some(values), None) if !values.is_empty() => {
            let texts: Vec<Option<String>> = values.iter().map(as_text).collect();
            let sentinels = array_edits::cast_texts(&texts, field.data_type())
                .map_err(|(index, message)| planning.refuse(ScenarioFault::InvalidParameter, &format!("/values/{index}"), message))?;
            Written::Sentinels(sentinels)
        }
        (None, Some(side)) => {
            let (_, output) = planning.numeric(&op.column, "/column", "out_of_range with beyond")?;
            let (low, high) = planning.numeric_extremes(&op.column);
            let span = match high > low {
                true => high - low,
                false => high.abs().max(1.0),
            };
            match side {
                Side::Low => Written::Beyond {
                    output,
                    edge: low,
                    span,
                    toward: -1.0,
                },
                Side::High => Written::Beyond {
                    output,
                    edge: high,
                    span,
                    toward: 1.0,
                },
            }
        }
        _ => {
            return Err(planning.refuse(
                ScenarioFault::InvalidParameter,
                "",
                "out_of_range takes either a non-empty values list or beyond, and not both",
            ));
        }
    };
    let schema = match &written {
        Written::Sentinels(sentinels) if sentinels.null_count() > 0 => {
            planning.schema_with(&[field.as_ref().clone().with_nullable(true)], &[])
        }
        _ => planning.schema.clone(),
    };
    let op = OutOfRangeRows {
        column,
        written,
        rate: op.rate.0,
        rows_key: planning.key("rows"),
        values_key: planning.key("values"),
    };
    Ok(Planned::Rows { op: Box::new(op), schema })
}

impl BlockPerturbation for OutOfRangeRows {
    fn apply(&self, stage: &Stage<'_>, mut block: Block) -> Block {
        let rows = array_edits::drawn_rows(&mut stage.stream(self.rows_key), self.rate, block.batch.num_rows());
        let mut draws = stage.stream(self.values_key);
        let values = match &self.written {
            Written::Sentinels(sentinels) => {
                let picks: Vec<u32> = rows.iter().map(|_| below(&mut draws, sentinels.len() as u32)).collect();
                array_edits::take_rows(sentinels, &picks)
            }
            &Written::Beyond {
                output,
                edge,
                span,
                toward,
            } => {
                let numbers: Vec<f64> = rows.iter().map(|_| edge + toward * span * (1.0 + unit(&mut draws))).collect();
                array_edits::from_f64(&numbers, output)
            }
        };
        let written = array_edits::replace_rows(block.batch.column(self.column), &rows, &values);
        array_edits::drop_cells(&mut block.cells, self.column, &rows);
        block.with_column(stage.schema, self.column, written)
    }
}
