//! `rate` of rows have their event time moved earlier by a lognormal lag, at least one tick of the
//! column's type. `ingest`, where named, holds every row's unshifted time, so a late row's event
//! time lies before it.

use arrow_array::ArrayRef;
use arrow_array::Int64Array;
use arrow_cast::can_cast_types;
use arrow_cast::cast;
use arrow_schema::DataType;
use arrow_schema::Field;
use arrow_schema::TimeUnit;

use super::Block;
use super::BlockPerturbation;
use super::Planned;
use super::Planning;
use super::ScenarioFault;
use super::Stage;
use super::Z_99;
use super::array_edits;
use super::open_unit;
use crate::physical_type::physical_type_name;
use crate::plan::PlanError;
use crate::samplers::normal_quantile;
use crate::scenario::LateArrival;

const MICROSECONDS_PER_DAY: f64 = 86_400_000_000.0;
const MILLISECONDS_PER_DAY: i64 = 86_400_000;

/// Where `ingest` is written: over a column the schema holds, or as one appended.
#[derive(Debug)]
enum Ingest {
    None,
    Over(usize),
    Appended,
}

#[derive(Debug)]
struct ShiftEarlier {
    column: usize,
    data_type: DataType,
    ingest: Ingest,
    ingest_type: DataType,
    rate: f64,
    /// The lag's log-median and log-sigma, in microseconds.
    mu: f64,
    sigma: f64,
    rows_key: u64,
    lags_key: u64,
}

pub(super) fn plan(op: &LateArrival, planning: &Planning<'_>) -> Result<Planned, PlanError> {
    let (column, field) = planning.column(&op.column, "/column")?;
    if !is_event_time(field.data_type()) {
        return Err(planning.refuse(
            ScenarioFault::OpNotForColumn,
            "/column",
            format!(
                "{:?} is {}, and late_arrival moves a timestamp or date",
                op.column,
                physical_type_name(field.data_type())
            ),
        ));
    }
    let median = op.lag.median.microseconds as f64;
    let sigma = match op.lag.p99 {
        Some(p99) if p99.microseconds as f64 <= median => {
            return Err(planning.refuse(
                ScenarioFault::InvalidParameter,
                "/lag/p99",
                "the lag's p99 must lie above its median",
            ));
        }
        Some(p99) => libm::log(p99.microseconds as f64 / median) / Z_99,
        None => 1.0,
    };
    let (ingest, ingest_type, schema) = match op.ingest.as_deref() {
        None => (Ingest::None, field.data_type().clone(), planning.schema.clone()),
        Some(name) if name == op.column => {
            return Err(planning.refuse(
                ScenarioFault::InvalidParameter,
                "/ingest",
                "ingest holds the unshifted time, so it must be a column other than the one moved",
            ));
        }
        Some(name) => match planning.schema.index_of(name) {
            Ok(index) => {
                let ingest_field = planning.schema.field(index);
                if !is_event_time(ingest_field.data_type()) || !can_cast_types(field.data_type(), ingest_field.data_type()) {
                    return Err(planning.refuse(
                        ScenarioFault::OpNotForColumn,
                        "/ingest",
                        format!(
                            "{name:?} is {}, and ingest holds a timestamp or date",
                            physical_type_name(ingest_field.data_type())
                        ),
                    ));
                }
                let widened = ingest_field
                    .clone()
                    .with_nullable(ingest_field.is_nullable() || field.is_nullable());
                (
                    Ingest::Over(index),
                    ingest_field.data_type().clone(),
                    planning.schema_with(&[widened], &[]),
                )
            }
            Err(_) => {
                let added = Field::new(name, field.data_type().clone(), field.is_nullable());
                (Ingest::Appended, field.data_type().clone(), planning.schema_with(&[], &[added]))
            }
        },
    };
    let op = ShiftEarlier {
        column,
        data_type: field.data_type().clone(),
        ingest,
        ingest_type,
        rate: op.rate.0,
        mu: libm::log(median),
        sigma,
        rows_key: planning.key("rows"),
        lags_key: planning.key("lags"),
    };
    Ok(Planned::Rows { op: Box::new(op), schema })
}

fn is_event_time(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64)
}

impl ShiftEarlier {
    /// A lag of `microseconds` in the column's ticks, at least one tick. Date64 moves by whole days.
    fn ticks(&self, microseconds: f64) -> i64 {
        let ticks = match &self.data_type {
            DataType::Timestamp(TimeUnit::Second, _) => microseconds / 1e6,
            DataType::Timestamp(TimeUnit::Millisecond, _) => microseconds / 1e3,
            DataType::Timestamp(TimeUnit::Microsecond, _) => microseconds,
            DataType::Timestamp(TimeUnit::Nanosecond, _) => microseconds * 1e3,
            _ => microseconds / MICROSECONDS_PER_DAY,
        };
        let ticks = (ticks.round() as i64).max(1);
        match self.data_type {
            DataType::Date64 => ticks * MILLISECONDS_PER_DAY,
            _ => ticks,
        }
    }
}

impl BlockPerturbation for ShiftEarlier {
    fn apply(&self, stage: &Stage<'_>, block: Block) -> Block {
        let original = block.batch.column(self.column).clone();
        let rows = array_edits::drawn_valid_rows(&mut stage.stream(self.rows_key), self.rate, &original);
        let mut lags = stage.stream(self.lags_key);
        let taken = cast(&array_edits::take_rows(&original, &rows), &DataType::Int64).expect("an event time casts to int64");
        let taken: &Int64Array = arrow_array::cast::AsArray::as_primitive(&taken);
        let shifted: Int64Array = taken
            .values()
            .iter()
            .map(|&tick| {
                let lag = libm::exp(self.mu + self.sigma * normal_quantile(open_unit(&mut lags)));
                Some(tick.saturating_sub(self.ticks(lag)))
            })
            .collect();
        let shifted = cast(&shifted, &self.data_type).expect("int64 casts back to the event time's type");
        let written = array_edits::replace_rows(&original, &rows, &shifted);
        let mut columns = block.batch.columns().to_vec();
        columns[self.column] = written;
        let ingest = || -> ArrayRef { cast(&original, &self.ingest_type).expect("planning checked the cast") };
        match self.ingest {
            Ingest::None => {}
            Ingest::Over(index) => columns[index] = ingest(),
            Ingest::Appended => columns.push(ingest()),
        }
        let cells = block.cells.map(|mut cells| {
            match self.ingest {
                Ingest::Over(index) => cells[index].clear(),
                Ingest::Appended => cells.push(Vec::new()),
                Ingest::None => {}
            }
            cells
        });
        Block { cells, ..block }.with_columns(stage.schema, columns)
    }
}
