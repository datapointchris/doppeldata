//! The perturbation catalog. Each op a scenario names is planned once against the profile, then
//! applied in list order to every block of generated rows. An op draws only from streams keyed by
//! its own id, so inserting another never moves its rows.

mod array_edits;
mod broken_characters;
mod categorical_shift;
mod cdc;
mod duplicates;
mod hot_keys;
mod late_arrival;
mod malformed_rows;
mod missingness;
mod noise;
mod out_of_range;
mod outliers;
mod partition_skew;
mod scaling;
mod schema_drift;
mod small_files;
mod swapped_values;

use std::fmt::Debug;
use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_array::RecordBatch;
use arrow_array::RecordBatchOptions;
use arrow_schema::Field;
use arrow_schema::FieldRef;
use arrow_schema::Schema;
use arrow_schema::SchemaRef;
use rand_chacha::ChaCha8Rng;

pub use self::cdc::CdcPlan;
pub use self::cdc::CdcStream;
pub use self::cdc::StreamFaultRecord;
pub use self::cdc::StreamOutline;
pub use self::cdc::names_stream_file;
pub(crate) use self::cdc::outline as cdc_outline;
pub(crate) use self::cdc::plan as plan_cdc;
pub(crate) use self::cdc::stream as cdc_stream;
pub use self::malformed_rows::FileFault;
pub use self::malformed_rows::LineEnd;
pub use self::malformed_rows::MalformedPlan;
pub use self::malformed_rows::RowFault;
pub use self::malformed_rows::RowFaults;
pub use self::schema_drift::SchemaDriftPlan;
use crate::physical_type::physical_type_name;
use crate::plan::Plan;
use crate::plan::PlanError;
use crate::profile::Column;
use crate::profile::Quantiles;
use crate::profile::TableProfile;
use crate::rng::rng_for;
use crate::rng::stream_key;
use crate::rng::unit;
use crate::samplers::DEFAULT_NUMERIC_RANGE as DEFAULT_RANGE;
use crate::samplers::NumericOutput;
use crate::samplers::PlanContext;
use crate::samplers::TextCell;
use crate::samplers::quantile_knots;
use crate::scenario::Perturbation;

/// Why a scenario cannot be applied to a profile, as a code a caller can branch on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScenarioFault {
    /// A column the op reads that the profile does not hold.
    UnknownColumn,
    /// An op that cannot act on a column of this type, such as noise on a string.
    OpNotForColumn,
    /// A parameter value the op cannot use with this profile.
    InvalidParameter,
    /// Two perturbations with one id, which would share a stream.
    DuplicatePerturbationId,
    /// An op the output format cannot hold, such as a schema drift in CSV files, which read as one
    /// table only under one header.
    OpNotForFormat,
    /// A change stream with no one column to key it by: no `key`, and a profile stating no
    /// primary key or one of several columns.
    NoPrimaryKey,
    /// An op that orders the whole table, such as `cdc`, asked of a write split across processes.
    OpNeedsOneProcess,
}

impl ScenarioFault {
    pub fn code(self) -> &'static str {
        match self {
            Self::UnknownColumn => "unknown-column",
            Self::OpNotForColumn => "op-not-for-column",
            Self::InvalidParameter => "invalid-parameter",
            Self::DuplicatePerturbationId => "duplicate-perturbation-id",
            Self::OpNotForFormat => "op-not-for-format",
            Self::NoPrimaryKey => "no-primary-key",
            Self::OpNeedsOneProcess => "op-needs-one-process",
        }
    }
}

/// A block's rows as a perturbation receives and returns them.
#[derive(Debug, Clone)]
pub(crate) struct Block {
    pub(crate) batch: RecordBatch,
    /// Each column's text cells, where the caller writes a text format.
    pub(crate) cells: Option<Vec<Vec<TextCell>>>,
    /// The generated row each row is filed with, by its place in the block. None while every row
    /// is a generated one in its own place. A copy carries its source's, so a file takes the rows
    /// generated in its range and every copy filed with one of them.
    pub(crate) homes: Option<Vec<u32>>,
}

impl Block {
    /// Each row's home, the identity where no op added rows.
    pub(crate) fn homes(&self) -> Vec<u32> {
        self.homes.clone().unwrap_or_else(|| (0..self.batch.num_rows() as u32).collect())
    }

    /// The block with `columns` in place of the batch's, under `schema`.
    pub(crate) fn with_columns(self, schema: &SchemaRef, columns: Vec<ArrayRef>) -> Self {
        let options = RecordBatchOptions::new().with_row_count(Some(self.batch.num_rows()));
        let batch = RecordBatch::try_new_with_options(schema.clone(), columns, &options).expect("a perturbation keeps its columns' fields");
        Self { batch, ..self }
    }

    /// The block with column `index` replaced by `array`, under `schema`.
    pub(crate) fn with_column(self, schema: &SchemaRef, index: usize, array: ArrayRef) -> Self {
        let mut columns = self.batch.columns().to_vec();
        columns[index] = array;
        self.with_columns(schema, columns)
    }
}

/// Where one perturbation is applied: the plan, its place in the list, the block, and the schema
/// its output carries.
pub(crate) struct Stage<'a> {
    pub(crate) plan: &'a Plan,
    pub(crate) index: usize,
    pub(crate) block: u64,
    pub(crate) schema: &'a SchemaRef,
}

impl Stage<'_> {
    pub(crate) fn stream(&self, key: u64) -> ChaCha8Rng {
        rng_for(self.plan.identity().seed(), key, self.block)
    }

    /// Another block as it reaches this perturbation, every earlier one applied.
    pub(crate) fn other_block(&self, block: u64, cells: bool) -> Block {
        self.plan.block_through(block, self.index, cells)
    }
}

/// An op applied to each block's rows.
pub(crate) trait BlockPerturbation: Debug + Send + Sync {
    /// The rows this op adds to `block` when the block reaches it holding `rows`. A count is
    /// planned before any row is drawn, so block boundaries never move.
    fn added_rows(&self, _seed: u64, _block: u64, _rows: usize) -> usize {
        0
    }

    /// Where the op copies rows from across the dataset: the parameter that sets it, as a pointer
    /// under the op's entry, and the fewest dataset rows between a copy's home and its source.
    fn source_reach(&self) -> Option<(&'static str, u64)> {
        None
    }

    fn apply(&self, stage: &Stage<'_>, block: Block) -> Block;
}

/// What a perturbation became once planned.
pub(crate) enum Planned {
    /// Applied to every block, with the schema its output carries.
    Rows { op: Box<dyn BlockPerturbation>, schema: SchemaRef },
    /// Applied by the writers to the files after a row.
    SchemaDrift(SchemaDriftPlan),
    /// File sizes in rows, drawn as the layout draws a profile's.
    SmallFiles(Quantiles),
    /// Rows and files the writers break.
    Malformed(MalformedPlan),
}

/// Everything one perturbation is planned against.
pub(crate) struct Planning<'a> {
    pub(crate) table: &'a TableProfile,
    pub(crate) seed: u64,
    pub(crate) rows: u64,
    pub(crate) block_count: u64,
    /// The schema the op receives.
    pub(crate) schema: &'a SchemaRef,
    pub(crate) index: usize,
    pub(crate) id: &'a str,
}

impl Planning<'_> {
    /// The stream for the op's draw named `draw`.
    pub(crate) fn key(&self, draw: &str) -> u64 {
        stream_key(&self.table.name, self.id, &format!("perturbation.{draw}"))
    }

    /// The table name a column sampler the op runs names its streams by, apart from every column's.
    pub(crate) fn sampler_table(&self) -> String {
        format!("{}\0perturbation {}", self.table.name, self.id)
    }

    /// What a column sampler the op runs is planned against: the table's own context, with its
    /// streams named for `sampler_table`.
    pub(crate) fn sampler_context<'b>(&self, sampler_table: &'b str) -> PlanContext<'b> {
        crate::plan::plan_context(self.table, sampler_table, self.seed, self.rows)
    }

    pub(crate) fn refuse(&self, fault: ScenarioFault, field: &str, message: impl Into<String>) -> PlanError {
        PlanError::Scenario {
            code: fault,
            field: format!("/perturbations/{}{field}", self.index),
            message: message.into(),
        }
    }

    /// The index and field of `name` in the schema the op receives, refused as `unknown-column` at
    /// `field` when it holds none.
    pub(crate) fn column(&self, name: &str, field: &str) -> Result<(usize, FieldRef), PlanError> {
        match self.schema.index_of(name) {
            Ok(index) => Ok((index, self.schema.fields()[index].clone())),
            Err(_) => Err(self.refuse(ScenarioFault::UnknownColumn, field, format!("the profile holds no column {name:?}"))),
        }
    }

    /// The profile's description of the top-level column `name`.
    pub(crate) fn profile_column(&self, name: &str) -> Option<&Column> {
        self.table.columns.iter().flatten().find(|column| column.name == name)
    }

    /// The schema the op receives, with `changed` in place of the field of the same name and
    /// `added` after the rest.
    pub(crate) fn schema_with(&self, changed: &[Field], added: &[Field]) -> SchemaRef {
        let fields: Vec<FieldRef> = self
            .schema
            .fields()
            .iter()
            .map(|field| match changed.iter().find(|changed| changed.name() == field.name()) {
                Some(changed) => Arc::new(changed.clone()),
                None => field.clone(),
            })
            .chain(added.iter().cloned().map(Arc::new))
            .collect();
        Arc::new(Schema::new_with_metadata(fields, self.schema.metadata().clone()))
    }

    /// An op that changes no field, applied with the schema it receives.
    pub(crate) fn rows(&self, op: impl BlockPerturbation + 'static) -> Planned {
        Planned::Rows {
            op: Box::new(op),
            schema: self.schema.clone(),
        }
    }

    /// The index and Arrow type of a numeric column `op` acts on, named at `field`.
    pub(crate) fn numeric(&self, name: &str, field: &str, op: &str) -> Result<(usize, NumericOutput), PlanError> {
        let (index, column) = self.column(name, field)?;
        match NumericOutput::from_type(column.data_type()) {
            Some(output) => Ok((index, output)),
            None => Err(self.refuse(
                ScenarioFault::OpNotForColumn,
                field,
                format!("{name:?} is {}, and {op} acts on a number", physical_type_name(column.data_type())),
            )),
        }
    }

    /// The trimmed extremes of numeric column `name`: the profile's, else its outermost quantiles,
    /// else the range a column with neither is drawn from.
    pub(crate) fn numeric_extremes(&self, name: &str) -> (f64, f64) {
        let numeric = self.profile_column(name).and_then(|column| column.numeric.as_ref());
        let extremes = numeric.and_then(|numeric| numeric.extremes.as_ref());
        let knots = quantile_knots(numeric.and_then(|numeric| numeric.quantiles.as_ref())).unwrap_or_default();
        let low = extremes.and_then(|e| e.low).or(knots.first().map(|&(_, value)| value));
        let high = extremes.and_then(|e| e.high).or(knots.last().map(|&(_, value)| value));
        (low.unwrap_or(DEFAULT_RANGE.0), high.unwrap_or(DEFAULT_RANGE.1))
    }
}

/// `rate` of a numeric column's non-null rows rewritten, each from its old value and the op's
/// value stream. Noise, scaling and outliers differ only in the rewrite.
#[derive(Debug)]
pub(crate) struct NumericEdit {
    column: usize,
    output: NumericOutput,
    rate: f64,
    rows_key: u64,
    values_key: u64,
}

impl NumericEdit {
    pub(crate) fn plan(planning: &Planning<'_>, column: &str, rate: f64, op: &str) -> Result<Self, PlanError> {
        let (column, output) = planning.numeric(column, "/column", op)?;
        Ok(Self {
            column,
            output,
            rate,
            rows_key: planning.key("rows"),
            values_key: planning.key("values"),
        })
    }

    pub(crate) fn apply(&self, stage: &Stage<'_>, block: Block, mut rewrite: impl FnMut(f64, &mut ChaCha8Rng) -> f64) -> Block {
        let column = block.batch.column(self.column).clone();
        let rows = array_edits::drawn_valid_rows(&mut stage.stream(self.rows_key), self.rate, &column);
        let old = array_edits::as_f64(&array_edits::take_rows(&column, &rows));
        let mut values = stage.stream(self.values_key);
        let new: Vec<f64> = old.values().iter().map(|&value| rewrite(value, &mut values)).collect();
        let written = array_edits::replace_rows(&column, &rows, &array_edits::from_f64(&new, self.output));
        block.with_column(stage.schema, self.column, written)
    }
}

/// Φ⁻¹(0.99), the standard deviations between a normal's median and its 99th percentile, which
/// turns a lognormal's median and p99 into its sigma.
pub(crate) const Z_99: f64 = 2.326_347_874_040_840_8;

/// `key` escaped as one RFC 6901 reference token.
pub(crate) fn pointer_token(key: &str) -> String {
    key.replace('~', "~0").replace('/', "~1")
}

/// A uniform draw from (0, 1), open at both ends, for an inverse CDF that diverges at either.
pub(crate) fn open_unit(rng: &mut ChaCha8Rng) -> f64 {
    const HALF_STEP: f64 = 0.5 / 9_007_199_254_740_992.0;
    unit(rng) + HALF_STEP
}

/// `perturbation` planned against the profile and the schema it receives.
pub(crate) fn plan_perturbation(perturbation: &Perturbation, planning: &Planning<'_>) -> Result<Planned, PlanError> {
    match perturbation {
        Perturbation::HotKeys(op) => hot_keys::plan(op, planning),
        Perturbation::Duplicates(op) => duplicates::plan(op, planning),
        Perturbation::LateArrival(op) => late_arrival::plan(op, planning),
        Perturbation::Missingness(op) => missingness::plan(op, planning),
        Perturbation::OutOfRange(op) => out_of_range::plan(op, planning),
        Perturbation::SwappedValues(op) => swapped_values::plan(op, planning),
        Perturbation::CategoricalShift(op) => categorical_shift::plan(op, planning),
        Perturbation::Noise(op) => noise::plan(op, planning),
        Perturbation::Scaling(op) => scaling::plan(op, planning),
        Perturbation::BrokenCharacters(op) => broken_characters::plan(op, planning),
        Perturbation::Outliers(op) => outliers::plan(op, planning),
        Perturbation::SchemaDrift(op) => schema_drift::plan(op, planning),
        Perturbation::SmallFiles(op) => small_files::plan(op, planning),
        Perturbation::PartitionSkew(op) => partition_skew::plan(op, planning),
        Perturbation::MalformedRows(op) => malformed_rows::plan(op, planning),
        Perturbation::Cdc(_) => unreachable!("cdc is planned after the loop, against every other op's rows"),
    }
}
