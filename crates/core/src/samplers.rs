//! One sampler per column kind, chosen from a profile column's type and statistics, and the null
//! mask and text-only values every column shares.

mod categorical;
mod dependent_columns;
mod distinct_values;
mod file_rows;
mod fitted_family;
mod inverse_cdf;
mod listed_values;
mod nested;
mod numeric;
mod numeric_top_values;
mod ranked_marginal;
mod temporal;
mod text;
mod text_cells;
mod text_lengths;
mod type_mix;

use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_array::BooleanArray;
use arrow_buffer::BooleanBuffer;
use arrow_buffer::NullBuffer;
use arrow_schema::DataType;
use arrow_schema::Field;
use arrow_schema::FieldRef;
use arrow_schema::TimeUnit;
use rand_distr::weighted::WeightedAliasIndex;

use self::categorical::CategoricalSampler;
pub(crate) use self::dependent_columns::DependentColumns;
pub(crate) use self::dependent_columns::DrawnColumns;
pub(crate) use self::dependent_columns::FollowingColumns;
use self::distinct_values::DistinctTarget;
use self::distinct_values::ValueIndex;
pub use self::file_rows::file_rows_draws;
pub use self::fitted_family::FITTED_FAMILIES;
use self::fitted_family::FittedFamily;
pub use self::fitted_family::FittedFamilyKind;
pub(crate) use self::fitted_family::normal_quantile;
use self::listed_values::ListedValues;
use self::nested::ListSampler;
use self::nested::StructSampler;
pub(crate) use self::numeric::DEFAULT_RANGE as DEFAULT_NUMERIC_RANGE;
pub(crate) use self::numeric::NumericOutput;
use self::numeric::NumericSampler;
use self::numeric::SequenceSampler;
pub(crate) use self::numeric::quantile_knots;
use self::numeric_top_values::SourceCounts;
use self::temporal::TemporalOutput;
use self::temporal::TemporalSampler;
use self::text::CheckDigitDraw;
use self::text::Shape;
use self::text::StringOutput;
use self::text::TextSampler;
use self::text::TextSource;
pub(crate) use self::text::push_uuid;
pub use self::text::shape_of;
pub use self::text_cells::TextCell;
use self::text_cells::TextOnlyValues;
pub use self::text_lengths::pattern_within_lengths;
pub use self::text_lengths::provider_lengths;
pub use self::type_mix::TYPE_MIX_FIELD_KEY;
use self::type_mix::TypeMix;
pub(crate) use self::type_mix::foreign_text;
pub use self::type_mix::raw_type_name;
use crate::physical_type;
use crate::physical_type::Container;
use crate::physical_type::PhysicalType;
use crate::plan::ColumnFault;
use crate::plan::PlanError;
use crate::profile::Column;
use crate::profile::Distinct;
use crate::profile::GeneratorHint;
use crate::profile::Keys;
use crate::profile::LogicalType;
use crate::profile::Provider;
use crate::profile::RawValueType;
use crate::profile::StringStats;
use crate::rng::Bernoulli;
use crate::rng::rng_for;
use crate::rng::stream_key;

/// The length a string with no stored length statistics is drawn between.
const DEFAULT_STRING_LENGTH: (f64, f64) = (8.0, 16.0);
/// The length a list with no stored length statistics is drawn between.
const DEFAULT_LIST_LENGTH: (f64, f64) = (0.0, 4.0);
/// The most extra repeats a pattern's `*`, `+` or `{n,}` generates.
const PATTERN_MAX_REPEAT: u32 = 16;

pub use text::check_digit_holds;

/// Why the engine cannot generate from `pattern`, as planning a profile that stores it refuses
/// it, or None when it can.
pub fn pattern_problem(pattern: &str) -> Option<String> {
    text::compile_pattern(pattern, PATTERN_MAX_REPEAT).err()
}

/// Where a block sits: the seed, the block's number, and the dataset row its first row is.
#[derive(Debug, Clone, Copy)]
pub(crate) struct BlockPosition {
    pub(crate) seed: u64,
    pub(crate) block: u64,
    pub(crate) first_row: u64,
}

/// What every column's plan is built in: the table its streams are named for, the seed, and the
/// rows a column's values fill over the whole dataset. A list element's rows are the elements its
/// lists are expected to hold.
pub(crate) struct PlanContext<'a> {
    pub(crate) table: &'a str,
    pub(crate) seed: u64,
    pub(crate) rows: f64,
    /// The source's rows a distinct ratio is counted over, `dataset.rows`.
    pub(crate) source_rows: Option<f64>,
    /// The source's rows a rate is a share of: those a sample read, or else every row.
    pub(crate) described_rows: Option<f64>,
    /// The profile's `privacy.k_min`, or [`crate::profile::DEFAULT_K_MIN`] where it states none. It bounds the rows
    /// the trimmed extremes leave beyond them.
    pub(crate) k_min: u64,
    /// The keys the profile states, whose columns' foreign text is each one row's own.
    pub(crate) keys: Option<&'a Keys>,
}

impl PlanContext<'_> {
    /// Whether `name` is a column of a key the profile states.
    fn is_key_column(&self, name: &str) -> bool {
        let Some(keys) = self.keys else { return false };
        keys.primary
            .iter()
            .chain(keys.candidates.iter().flatten())
            .any(|key| key.columns.iter().any(|column| column == name))
    }

    /// The distinct values a column is to hold over the dataset, drawn in its `filled_rows`. At or
    /// above the source's rows its ratio holds, so a longer run holds more values. Below them it is
    /// what a random sample of the source holds where each value fills an equal share of its rows,
    /// `count · (1 − (1 − rows/N)^(N/count))`, so a short run keeps the source's values.
    fn distinct_target(&self, distinct: Option<&Distinct>, filled_rows: f64, reads: &mut FieldReads) -> Option<DistinctTarget> {
        let distinct = distinct?;
        let ratio = reads.read("distinct_ratio", "/distinct/ratio", distinct.ratio)?.0;
        let held = ratio * self.rows;
        let source_rows = reads
            .read("approx_values", "/distinct/approx_values", distinct.approx_values)
            .map(|count| count as f64)
            .filter(|&count| count > 0.0 && ratio > 0.0)
            .map(|count| (count, self.source_rows.unwrap_or(count / ratio)));
        let distinct = match source_rows {
            Some((count, source_rows)) if source_rows > self.rows => {
                count * (1.0 - libm::pow(1.0 - self.rows / source_rows, source_rows / count))
            }
            _ => held,
        };
        Some(DistinctTarget { distinct, filled_rows })
    }
}

/// Why a sampler cannot be built, before the column's place in the profile is known.
#[derive(Debug)]
pub(crate) struct Refusal {
    fault: ColumnFault,
    message: String,
}

impl Refusal {
    pub(crate) fn new(fault: ColumnFault, message: impl Into<String>) -> Self {
        Self {
            fault,
            message: message.into(),
        }
    }
}

/// Where a column sits: the dotted path its streams are named by, and its JSON Pointer in the
/// profile. A nested field's path is its own `path`, or its parent's and its name.
pub(crate) struct ColumnSite {
    path: String,
    pointer: String,
    nested: bool,
}

impl ColumnSite {
    /// The `index`th of the table's columns.
    pub(crate) fn top_level(index: usize, column: &Column) -> Self {
        Self {
            path: column.path.clone().unwrap_or_else(|| column.name.clone()),
            pointer: format!("/columns/{index}"),
            nested: false,
        }
    }

    /// `child`, which sits at `below` under this column's pointer.
    fn child(&self, child: &Column, below: &str) -> Self {
        Self {
            path: child.path.clone().unwrap_or_else(|| format!("{}.{}", self.path, child.name)),
            pointer: format!("{}{below}", self.pointer),
            nested: true,
        }
    }

    /// The error for a refusal of the statistic at `field` under this column.
    fn refuse(&self, field: &str) -> impl FnOnce(Refusal) -> PlanError + use<> {
        let field = format!("{}{field}", self.pointer);
        move |refusal| PlanError::Column {
            code: refusal.fault,
            field,
            message: refusal.message,
        }
    }

    /// As [`Self::refuse`], for a reason from a function that fails in one way only.
    fn refuse_as(&self, fault: ColumnFault, field: &str) -> impl FnOnce(String) -> PlanError + use<> {
        let refuse = self.refuse(field);
        move |message| refuse(Refusal::new(fault, message))
    }
}

/// Every stream a column draws from, each named by the table, the column's path and what it draws,
/// and hashed once.
#[derive(Debug, Clone, Copy)]
pub(crate) struct StreamKeys {
    values: u64,
    nulls: u64,
    missing_tokens: u64,
    invalid: u64,
    lengths: u64,
    /// Drawn once at plan time, from block 0, to pick a date column's empty days.
    gap_days: u64,
    /// A row's index into a string column's values, where the column draws one.
    index: u64,
    /// Positioned at an index rather than a block: the value at that index.
    indexed_values: u64,
    /// A row's draw given another column: its cell of a conditional table, or whether it breaks a lookup or an
    /// expression.
    given: u64,
    /// One word a row: which of its type mix's shares the row holds.
    type_mix: u64,
    /// The text of each row holding a share of another type.
    type_mix_text: u64,
}

impl StreamKeys {
    fn new(table: &str, path: &str) -> Self {
        let key = |draw| stream_key(table, path, draw);
        Self {
            values: key("values"),
            nulls: key("nulls"),
            missing_tokens: key("missing_tokens"),
            invalid: key("invalid"),
            lengths: key("lengths"),
            gap_days: key("gap_days"),
            index: key("index"),
            indexed_values: key("indexed_values"),
            given: key("given"),
            type_mix: key("type_mix"),
            type_mix_text: key("type_mix_text"),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum ValueSampler {
    Sequence(SequenceSampler),
    Numeric(NumericSampler),
    Categorical(CategoricalSampler),
    Temporal(TemporalSampler),
    Boolean(Bernoulli),
    Text(TextSampler),
    List(Box<ListSampler>),
    Struct(StructSampler),
}

impl ValueSampler {
    /// The sampler's kind, as a plan's trace names it.
    fn kind(&self) -> &'static str {
        match self {
            Self::Sequence(_) => "sequence",
            Self::Numeric(_) => "numeric",
            Self::Categorical(_) => "categorical",
            Self::Temporal(_) => "temporal",
            Self::Boolean(_) => "boolean",
            Self::Text(_) => "text",
            Self::List(_) => "list",
            Self::Struct(_) => "struct",
        }
    }

    fn data_type(&self) -> DataType {
        match self {
            Self::Sequence(sampler) => sampler.output.data_type(),
            Self::Numeric(sampler) => sampler.output.data_type(),
            Self::Categorical(sampler) => sampler.output.data_type(),
            Self::Temporal(sampler) => sampler.data_type(),
            Self::Boolean(_) => DataType::Boolean,
            Self::Text(sampler) => sampler.output.data_type(),
            Self::List(sampler) => sampler.data_type(),
            Self::Struct(sampler) => sampler.data_type(),
        }
    }

    fn generate(&self, at: &BlockPosition, keys: &StreamKeys, rows: usize, nulls: Option<NullBuffer>) -> ArrayRef {
        let values = || rng_for(at.seed, keys.values, at.block);
        match self {
            Self::Sequence(sampler) => sampler.generate(at.first_row, rows, nulls),
            Self::Numeric(sampler) => sampler.generate(at, keys, rows, nulls),
            Self::Categorical(sampler) => sampler.generate(at, keys, rows, nulls),
            Self::Temporal(sampler) => sampler.generate(&mut values(), rows, nulls),
            Self::Boolean(true_fraction) => {
                let mut rng = values();
                let values = BooleanBuffer::collect_bool(rows, |_| true_fraction.draw(&mut rng));
                Arc::new(BooleanArray::new(values, nulls))
            }
            Self::Text(sampler) => sampler.generate(at, keys, rows, nulls),
            Self::List(sampler) => sampler.generate(at, keys, rows, nulls),
            Self::Struct(sampler) => sampler.generate(at, rows, nulls),
        }
    }
}

/// Each profile field a sampler's builder read, as a parameter's name and the field's pointer
/// below the column, recorded where the builder reads it. So it names the branch the builder took.
#[derive(Debug, Default)]
pub(crate) struct FieldReads(Vec<(&'static str, &'static str)>);

impl FieldReads {
    /// `value`, the field at `pointer`, recorded as read under `name`.
    pub(crate) fn read<T>(&mut self, name: &'static str, pointer: &'static str, value: T) -> T {
        if self.0.iter().all(|&(_, read)| read != pointer) {
            self.0.push((name, pointer));
        }
        value
    }
}

/// Everything one column, or one nested field, needs to produce a block.
#[derive(Debug, Clone)]
pub(crate) struct ColumnPlan {
    pub(crate) field: FieldRef,
    /// What draws the values, as a plan reports it: `sequence from 1 by 1`.
    pub(crate) sampler: String,
    /// The sampler's kind, such as `numeric`.
    pub(crate) kind: &'static str,
    /// Each profile field the sampler reads, as a parameter's name and the field's pointer.
    pub(crate) read_fields: Vec<(&'static str, String)>,
    /// The share of rows the null mask draws as null.
    pub(crate) null_share: f64,
    keys: StreamKeys,
    values: ValueSampler,
    nulls: Option<Bernoulli>,
    text_only: Option<TextOnlyValues>,
    /// Where the column generates as text, the type mix its `values` are rendered through. `values`
    /// stays the column's own typed draw, which every dependence between columns reads.
    type_mix: Option<TypeMix>,
}

impl ColumnPlan {
    /// Whether no two rows the column draws hold one value: a sequence with a step, or a uuid
    /// drawn for each row, with no nulls.
    pub(crate) fn draws_unique_values(&self) -> bool {
        let unique = match &self.values {
            ValueSampler::Sequence(sequence) => sequence.step != 0,
            ValueSampler::Text(text) => text.index.is_none() && matches!(text.source, TextSource::Provider(Provider::Uuid)),
            _ => false,
        };
        let renders_its_own = self.type_mix.as_ref().is_none_or(TypeMix::renders_only);
        unique && renders_its_own && self.nulls.is_none() && self.text_only.is_none()
    }

    pub(crate) fn generate(&self, at: &BlockPosition, rows: usize) -> ArrayRef {
        let nulls = self
            .drawn_nulls(at, rows)
            .map(|nulls| NullBuffer::new(!&nulls))
            .filter(|nulls| nulls.null_count() > 0);
        self.rendered(at, self.values.generate(at, &self.keys, rows, nulls))
    }

    /// `typed`, the column's own draws for the block at `at`, as the column holds them: written as
    /// text where it generates from a type mix, else unchanged.
    pub(crate) fn rendered(&self, at: &BlockPosition, typed: ArrayRef) -> ArrayRef {
        match &self.type_mix {
            Some(mix) => mix.render(at, &self.keys, &typed),
            None => typed,
        }
    }

    /// `array`, as the column holds it, read as its own type: unchanged where it generates typed,
    /// else each own value, and null where a row holds text of another type.
    pub(crate) fn read_typed(&self, array: &ArrayRef) -> ArrayRef {
        match &self.type_mix {
            Some(mix) => mix.read(array, &self.values.data_type()),
            None => array.clone(),
        }
    }

    /// Whether the column generates as text through a type mix, its own share included.
    pub(crate) fn draws_type_mix(&self) -> bool {
        self.type_mix.is_some()
    }

    /// The JSON Pointer of this column and each field nested in it that generates as text, with the
    /// type its own draws render as.
    pub(crate) fn type_mix_pointers(&self) -> Vec<(String, RawValueType)> {
        let mut found: Vec<(String, RawValueType)> = self.type_mix.iter().map(|mix| (mix.pointer.clone(), mix.own)).collect();
        match &self.values {
            ValueSampler::Struct(structure) => found.extend(structure.fields.iter().flat_map(ColumnPlan::type_mix_pointers)),
            ValueSampler::List(list) => found.extend(list.element.type_mix_pointers()),
            _ => {}
        }
        found
    }

    /// Draws the column as its own type, as a relationship whose values it takes needs.
    pub(crate) fn drop_type_mix(&mut self) {
        if self.type_mix.take().is_some() {
            self.field = Arc::new(Field::new(self.field.name(), self.values.data_type(), self.field.is_nullable()));
        }
    }

    /// What draws the values, as a plan reports it, the type mix's text included.
    pub(crate) fn description(&self) -> String {
        match &self.type_mix {
            Some(mix) => format!("{}{}", self.sampler, mix.describe()),
            None => self.sampler.clone(),
        }
    }

    /// Writes the column's own draws as text on every row, as its parent's text needs where a
    /// relationship copies it. `column` is its profile entry. False where text cannot hold its type.
    pub(crate) fn render_as_text(&mut self, column: &Column, pointer: &str) -> bool {
        if self.type_mix.is_some() {
            return true;
        }
        let Ok(Some(mix)) = TypeMix::rendered_only(column, &self.values.data_type(), pointer) else {
            return false;
        };
        self.field = text_field(self.field.name(), self.field.is_nullable(), &mix);
        self.type_mix = Some(mix);
        true
    }

    /// The rows the null mask draws as null, one draw per row.
    fn drawn_nulls(&self, at: &BlockPosition, rows: usize) -> Option<BooleanBuffer> {
        self.nulls.map(|null| {
            let mut rng = rng_for(at.seed, self.keys.nulls, at.block);
            BooleanBuffer::collect_bool(rows, |_| null.draw(&mut rng))
        })
    }

    pub(crate) fn text_cells(&self, at: &BlockPosition, rows: usize) -> Vec<TextCell> {
        self.text_cells_given(at, rows, self.drawn_nulls(at, rows).as_ref())
    }

    /// As [`Self::text_cells`], for the rows `nulls` drew as null.
    pub(crate) fn text_cells_given(&self, at: &BlockPosition, rows: usize, nulls: Option<&BooleanBuffer>) -> Vec<TextCell> {
        let Some(text_only) = &self.text_only else {
            return Vec::new();
        };
        let mut tokens = rng_for(at.seed, self.keys.missing_tokens, at.block);
        text_only.cells(nulls, &mut tokens, rows)
    }

    /// The column made a key a child reads at any row of `rows`, its nulls and missing-value tokens
    /// dropped: a sequence or a uuid as drawn, another integer as a sequence across its profiled
    /// extremes, and other text as each row's own value. None for any other column.
    pub(crate) fn as_row_key(&mut self, column: &Column, rows: u64, seed: u64) -> Option<RowKey> {
        let key = match &mut self.values {
            ValueSampler::Sequence(sequence) if sequence.step != 0 => RowKey::Sequence {
                start: sequence.start,
                step: sequence.step,
            },
            ValueSampler::Numeric(numeric) if numeric.output.is_integer() => {
                let extremes = column.numeric.as_ref().and_then(|n| n.extremes.as_ref());
                let (low, high) = (extremes.and_then(|e| e.low)?, extremes.and_then(|e| e.high)?);
                if !low.is_finite() || !high.is_finite() || high < low {
                    return None;
                }
                let start = low.round() as i64;
                let step = (((high - low) / rows.saturating_sub(1).max(1) as f64).floor() as i64).max(1);
                let output = numeric.output;
                self.values = ValueSampler::Sequence(SequenceSampler { start, step, output });
                self.sampler = format!("sequence from {start} by {step} across its profiled extremes");
                RowKey::Sequence { start, step }
            }
            ValueSampler::Text(text)
                if text.index.is_none() && text.check_digit.is_none() && matches!(text.source, TextSource::Provider(Provider::Uuid)) =>
            {
                RowKey::Uuid {
                    seed,
                    values: self.keys.values,
                }
            }
            ValueSampler::Text(text) => {
                text.by_row = true;
                self.sampler = format!("{}, each row its own value", self.sampler);
                RowKey::TextAtRow {
                    seed,
                    keys: self.keys,
                    sampler: Box::new(text.clone()),
                }
            }
            _ => return None,
        };
        let key = match &mut self.type_mix {
            Some(mix) => {
                mix.make_keyed();
                RowKey::TypeMix {
                    own: Box::new(key),
                    mix: Box::new(mix.clone()),
                    seed,
                    keys: self.keys,
                }
            }
            None => key,
        };
        self.nulls = None;
        self.null_share = 0.0;
        self.text_only = None;
        Some(key)
    }

    /// What a key's values are written as in this column: an integer where it stores a number,
    /// text where it stores text. None for any other column.
    pub(crate) fn key_column(&self) -> Option<KeyColumn> {
        if self.type_mix.is_some() {
            return Some(KeyColumn::Text(StringOutput::Utf8));
        }
        Some(match &self.values {
            ValueSampler::Sequence(sequence) => KeyColumn::Integer(sequence.output),
            ValueSampler::Numeric(numeric) => KeyColumn::Integer(numeric.output),
            ValueSampler::Text(text) => KeyColumn::Text(text.output),
            ValueSampler::Categorical(categorical) => KeyColumn::Text(categorical.output),
            _ => return None,
        })
    }

    /// The share of rows the column's own null mask draws null, before any key draw replaces it.
    pub(crate) fn profiled_null_share(&self) -> f64 {
        self.null_share
    }
}

/// A key column whose value at any row is a function of the row alone.
#[derive(Debug, Clone)]
pub(crate) enum RowKey {
    Sequence {
        start: i64,
        step: i64,
    },
    /// The uuid the column's values stream draws at the row: two words of the block's stream each.
    Uuid {
        seed: u64,
        values: u64,
    },
    TextAtRow {
        seed: u64,
        keys: StreamKeys,
        sampler: Box<TextSampler>,
    },
    /// A key generated from its type mix: `own` at a row holding its own type, else text only that
    /// row holds.
    TypeMix {
        own: Box<RowKey>,
        mix: Box<TypeMix>,
        seed: u64,
        keys: StreamKeys,
    },
}

/// One value of a key.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum KeyValue {
    Null,
    Integer(i64),
    Text(String),
}

impl RowKey {
    pub(crate) fn is_text(&self) -> bool {
        !matches!(self, Self::Sequence { .. })
    }

    pub(crate) fn value_at(&self, row: u64) -> KeyValue {
        match self {
            Self::Sequence { start, step } => {
                let value = i128::from(*start) + i128::from(*step) * i128::from(row);
                KeyValue::Integer(value.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64)
            }
            Self::Uuid { seed, values } => {
                let mut rng = rng_for(*seed, *values, row / crate::rng::BLOCK_ROWS);
                rng.set_word_pos(4 * u128::from(row % crate::rng::BLOCK_ROWS));
                let mut out = String::with_capacity(36);
                text::push_uuid(&mut rng, &mut out);
                KeyValue::Text(out)
            }
            Self::TextAtRow { seed, keys, sampler } => {
                let mut out = String::new();
                sampler.push_at_row(*seed, keys, row, &mut out);
                KeyValue::Text(out)
            }
            Self::TypeMix { own, mix, seed, keys } => match mix.key_text_at(*seed, keys, row) {
                Some(text) => KeyValue::Text(text),
                None => own.value_at(row),
            },
        }
    }

    /// Whether the key is written as text from a type mix, so a child's key holding it is text too.
    pub(crate) fn draws_type_mix(&self) -> bool {
        matches!(self, Self::TypeMix { .. })
    }

    /// The largest integer the key holds over `rows` rows, None for text. A type mix's own integers
    /// count, so its orphans lie past them.
    pub(crate) fn largest(&self, rows: u64) -> Option<i64> {
        if let Self::TypeMix { own, .. } = self {
            return own.largest(rows);
        }
        let Self::Sequence { start, step } = self else { return None };
        let last = i128::from(*start) + i128::from(*step) * i128::from(rows.saturating_sub(1));
        Some(i128::from(*start).max(last).clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64)
    }
}

/// How a column stores a key's values.
#[derive(Debug, Clone, Copy)]
pub(crate) enum KeyColumn {
    Integer(NumericOutput),
    Text(StringOutput),
}

impl KeyColumn {
    /// The array of `values`, one per row, an integer written as text in a text column.
    pub(crate) fn array(self, values: &[KeyValue]) -> ArrayRef {
        let rows = values.len();
        let nulls = values
            .contains(&KeyValue::Null)
            .then(|| NullBuffer::new(BooleanBuffer::collect_bool(rows, |row| values[row] != KeyValue::Null)));
        match self {
            Self::Integer(output) => output.exact_array(rows, nulls, |row| match values[row] {
                KeyValue::Integer(value) => value,
                _ => 0,
            }),
            Self::Text(output) => {
                let mut row = 0;
                output.build(rows, nulls.as_ref(), |out| {
                    match &values[row] {
                        KeyValue::Integer(value) => out.push_str(&value.to_string()),
                        KeyValue::Text(text) => out.push_str(text),
                        KeyValue::Null => {}
                    }
                    row += 1;
                })
            }
        }
    }
}

/// The plan for `column`, which sits at `site`.
pub(crate) fn column_plan(context: &PlanContext, column: &Column, site: &ColumnSite) -> Result<ColumnPlan, PlanError> {
    column_plan_present(context, column, site, 1.0)
}

/// As [`column_plan`], for a field present in `presence` of its parent's rows.
fn column_plan_present(context: &PlanContext, column: &Column, site: &ColumnSite, presence: f64) -> Result<ColumnPlan, PlanError> {
    let keys = StreamKeys::new(context.table, &site.path);
    let mut reads = FieldReads::default();
    let may_be_null = column.nullable != Some(false);
    let null_fraction = reads
        .read("null_fraction", "/null_fraction", column.null_fraction)
        .map_or(0.0, |r| r.0);
    let null_share = if may_be_null {
        null_fraction + (1.0 - presence) * (1.0 - null_fraction)
    } else {
        0.0
    };
    let type_mix = match physical(column) {
        Ok(PhysicalType::Scalar(data_type)) => {
            let keyed = !site.nested && context.is_key_column(&column.name);
            let mix = reads.read("type_mix_fractions", "/type_mix_fractions", column.type_mix_fractions.as_ref());
            TypeMix::planned(column, mix, &data_type, keyed, &site.pointer).map_err(site.refuse("/temporal/format"))?
        }
        _ => None,
    };
    // A text source's distinct values are counted over the values parsing as the column's type, so
    // the own draw holds every one of them in its share of the rows.
    let own_share = type_mix.as_ref().map_or(1.0, TypeMix::own_share);
    let values = value_sampler(
        context,
        column,
        site,
        &keys,
        context.rows * (1.0 - null_share) * own_share,
        &mut reads,
    )?;
    let tokens = reads.read(
        "missing_token_fractions",
        "/missing_token_fractions",
        column.missing_token_fractions.as_ref(),
    );
    let text_only = TextOnlyValues::new(tokens, null_share).map_err(site.refuse("/missing_token_fractions"))?;
    if site.nested && text_only.is_some() {
        let message = "a text cell names no nested field, so a nested field's missing-value tokens are not generated yet".to_string();
        return Err(site.refuse_as(ColumnFault::NotGeneratedYet, "/missing_token_fractions")(message));
    }
    let nulls = Some(Bernoulli::new(null_share)).filter(|null| !null.never());
    let mut sampler = describe(column, &values);
    if column.missing_token_fractions.as_ref().is_some_and(|tokens| !tokens.is_empty()) {
        sampler.push_str(", with missing-value tokens as text");
    }
    let read_fields = reads
        .0
        .into_iter()
        .map(|(name, below)| (name, format!("{}{below}", site.pointer)))
        .collect();
    let kind = values.kind();
    let field = match &type_mix {
        Some(mix) => text_field(&column.name, may_be_null, mix),
        None => Arc::new(Field::new(&column.name, values.data_type(), may_be_null)),
    };
    Ok(ColumnPlan {
        field,
        sampler,
        kind,
        read_fields,
        null_share: nulls.map_or(0.0, |_| null_share),
        keys,
        values,
        nulls,
        text_only,
        type_mix,
    })
}

/// A string field named `name`, its metadata naming the type `mix`'s own share renders.
fn text_field(name: &str, nullable: bool, mix: &TypeMix) -> FieldRef {
    let metadata = std::collections::HashMap::from([(TYPE_MIX_FIELD_KEY.to_string(), raw_type_name(mix.own).to_string())]);
    Arc::new(Field::new(name, DataType::Utf8, nullable).with_metadata(metadata))
}

/// What draws `column`'s values through `values`, in a few words a plan prints.
fn describe(column: &Column, values: &ValueSampler) -> String {
    match values {
        ValueSampler::Sequence(sequence) => format!("sequence from {} by {}", sequence.start, sequence.step),
        ValueSampler::Numeric(numeric) => {
            let mut description = match &numeric.listed {
                Some(listed) => {
                    let mut listed_part = format!("{} top values", listed.len() - numeric.beyond_extremes);
                    if numeric.beyond_extremes > 0 {
                        listed_part.push_str(&format!(" and {} beyond the extremes", numeric.beyond_extremes));
                    }
                    if listed.other_share() > 0.0 {
                        listed_part.push_str(&format!(", the rest from {}", numeric.curve.describe()));
                    }
                    listed_part
                }
                None => numeric.curve.describe(),
            };
            if numeric.index.is_some() {
                description.push_str(", at the profiled distinct ratio");
            }
            description
        }
        ValueSampler::Categorical(categorical) => {
            let string = column.string.as_ref();
            let (top, other_fraction) = match (listed_values(string), listed_prefixes(string)) {
                (None, Some(prefixes)) => (
                    format!("{} top prefixes, each followed by {}", categorical.listed.len(), prefixes.suffix),
                    prefixes.other_fraction,
                ),
                _ => (
                    format!("{} top values", categorical.listed.len()),
                    string.and_then(|s| s.other_fraction),
                ),
            };
            match other_fraction.is_some_and(|r| r.0 > 0.0) {
                true => format!("{top}, the rest from {}", describe_source(&categorical.other)),
                false => top,
            }
        }
        ValueSampler::Temporal(_) => {
            let temporal = column.temporal.as_ref();
            let weights: Vec<&str> = [
                ("month", temporal.and_then(|t| t.month_fractions.as_ref()).is_some()),
                ("weekday", temporal.and_then(|t| t.weekday_fractions.as_ref()).is_some()),
                ("hour", temporal.and_then(|t| t.hour_fractions.as_ref()).is_some()),
            ]
            .into_iter()
            .filter_map(|(name, weighted)| weighted.then_some(name))
            .collect();
            match weights.is_empty() {
                true => "uniform between the extremes".to_string(),
                false => format!("between the extremes, weighted by {}", weights.join(", ")),
            }
        }
        ValueSampler::Boolean(_) => {
            let true_fraction = column.boolean.as_ref().and_then(|b| b.true_fraction).map_or(0.5, |r| r.0);
            format!("true in {true_fraction} of rows")
        }
        ValueSampler::Text(text) => {
            let mut description = describe_source(&text.source);
            if text.index.is_some() {
                description.push_str(", at the profiled distinct ratio");
            }
            if let Some(check_digit) = &text.check_digit {
                description.push_str(&format!(", with a {:?} check digit", check_digit.kind).to_lowercase());
            }
            description
        }
        ValueSampler::List(list) => format!("list of {}", list.element.description()),
        ValueSampler::Struct(structure) => format!("struct of {} fields", structure.fields.len()),
    }
}

fn describe_source(source: &TextSource) -> String {
    match source {
        TextSource::Provider(provider) => format!("the {} provider", snake_case(&format!("{provider:?}"))),
        TextSource::Icd10cm { dotted: true } => "the icd10cm provider, dotted after the category".to_string(),
        TextSource::Icd10cm { dotted: false } => "the icd10cm provider".to_string(),
        TextSource::Pattern(_) => "a pattern".to_string(),
        TextSource::Shapes { shapes, .. } => format!("{} shapes", shapes.len()),
        TextSource::Characters { .. } => "characters of the profiled classes".to_string(),
    }
}

/// `FreeEmail` as the profile names it, `free_email`.
fn snake_case(name: &str) -> String {
    let mut snake = String::with_capacity(name.len() + 4);
    for (index, character) in name.chars().enumerate() {
        if character.is_ascii_uppercase() && index > 0 {
            snake.push('_');
        }
        snake.push(character.to_ascii_lowercase());
    }
    snake
}

/// The column's Arrow type: its `physical_type` where given, otherwise one its statistics and
/// logical type imply.
fn physical(column: &Column) -> Result<PhysicalType, String> {
    if let Some(text) = &column.physical_type {
        return physical_type::parse(text).ok_or_else(|| format!("physical_type {text:?} is not an Arrow type this engine writes"));
    }
    let integral = column.numeric.as_ref().and_then(|n| n.integral) == Some(true);
    Ok(match column.logical_type {
        _ if column.list.is_some() => PhysicalType::Container(Container::List),
        _ if column.struct_fields.is_some() => PhysicalType::Container(Container::Struct),
        Some(LogicalType::Array) => PhysicalType::Container(Container::List),
        Some(LogicalType::Object) => PhysicalType::Container(Container::Struct),
        Some(LogicalType::Boolean) => PhysicalType::Scalar(DataType::Boolean),
        Some(LogicalType::Date) => PhysicalType::Scalar(DataType::Date32),
        Some(LogicalType::Time) => PhysicalType::Scalar(DataType::Time64(TimeUnit::Microsecond)),
        Some(LogicalType::Timestamp) => PhysicalType::Scalar(DataType::Timestamp(TimeUnit::Microsecond, None)),
        Some(LogicalType::Integer) => PhysicalType::Scalar(DataType::Int64),
        Some(LogicalType::Number) if integral => PhysicalType::Scalar(DataType::Int64),
        Some(LogicalType::Number) => PhysicalType::Scalar(DataType::Float64),
        Some(LogicalType::String) => PhysicalType::Scalar(DataType::Utf8),
        None if column.boolean.is_some() => PhysicalType::Scalar(DataType::Boolean),
        None if column.temporal.is_some() => PhysicalType::Scalar(DataType::Timestamp(TimeUnit::Microsecond, None)),
        None if integral => PhysicalType::Scalar(DataType::Int64),
        None if column.numeric.is_some() => PhysicalType::Scalar(DataType::Float64),
        None => PhysicalType::Scalar(DataType::Utf8),
    })
}

/// The sampler for `column`, whose value is not null in `filled_rows` of the dataset's rows.
fn value_sampler(
    context: &PlanContext,
    column: &Column,
    site: &ColumnSite,
    keys: &StreamKeys,
    filled_rows: f64,
    reads: &mut FieldReads,
) -> Result<ValueSampler, PlanError> {
    let scalar = match physical(column).map_err(site.refuse_as(ColumnFault::UnsupportedType, "/physical_type"))? {
        PhysicalType::Container(Container::Struct) => return struct_sampler(context, column, site, reads),
        PhysicalType::Container(container) => return list_sampler(context, column, site, container == Container::LargeList, reads),
        PhysicalType::Scalar(data_type) => data_type,
    };
    let numeric = column.numeric.as_ref();
    if let Some(output) = NumericOutput::from_type(&scalar) {
        if let Some(sequence) = reads.read("sequence", "/numeric/sequence", numeric.and_then(|n| n.sequence.as_ref())) {
            return Ok(ValueSampler::Sequence(SequenceSampler {
                start: sequence.start,
                step: sequence.step,
                output,
            }));
        }
        let fit = match reads.read("fit", "/numeric/fit", numeric.and_then(|n| n.fit.as_ref())) {
            Some(fit) => FittedFamily::from_fit(fit).map_err(site.refuse_as(ColumnFault::InvalidFit, "/numeric/fit"))?,
            None => None,
        };
        let stats = numeric.cloned().unwrap_or_else(empty_numeric_stats);
        let target = context.distinct_target(column.distinct.as_ref(), filled_rows, reads);
        let source = column.distinct.as_ref().and_then(|d| {
            let distinct = reads.read("approx_values", "/distinct/approx_values", d.approx_values)?;
            let ratio = reads.read("distinct_ratio", "/distinct/ratio", d.ratio)?.0;
            let described = context.described_rows.or((ratio > 0.0).then(|| distinct as f64 / ratio))?;
            let null_fraction = column.null_fraction.map_or(0.0, |r| r.0);
            Some(SourceCounts {
                distinct,
                values: described * (1.0 - null_fraction),
                k_min: context.k_min,
            })
        });
        let curve = numeric::numeric_cdf(&stats, fit.as_ref(), reads).map_err(site.refuse_as(ColumnFault::InvalidQuantiles, "/numeric"))?;
        let sampler = NumericSampler::new(curve, &stats, output, target, source, reads)
            .map_err(site.refuse_as(ColumnFault::UnusableRates, "/numeric/top_values"))?;
        return Ok(ValueSampler::Numeric(sampler));
    }
    if let Some(output) = TemporalOutput::from_type(&scalar) {
        let gap_days = || rng_for(context.seed, keys.gap_days, 0);
        let sampler = TemporalSampler::new(column.temporal.as_ref(), output, gap_days, reads).map_err(site.refuse("/temporal"))?;
        return Ok(ValueSampler::Temporal(sampler));
    }
    if scalar == DataType::Boolean {
        let true_fraction = reads.read(
            "true_fraction",
            "/boolean/true_fraction",
            column.boolean.as_ref().and_then(|b| b.true_fraction),
        );
        return Ok(ValueSampler::Boolean(Bernoulli::new(true_fraction.map_or(0.5, |r| r.0))));
    }
    let Some(output) = StringOutput::from_type(&scalar) else {
        let message = format!("{scalar} is not a type this engine generates");
        return Err(site.refuse_as(ColumnFault::UnsupportedType, "/physical_type")(message));
    };
    if column.temporal.is_some() {
        let message = "a string column with temporal statistics is not generated yet".to_string();
        return Err(site.refuse_as(ColumnFault::NotGeneratedYet, "/temporal")(message));
    }
    let string = column.string.as_ref();
    let hint = column.semantic.as_ref().and_then(|s| s.generator_hint.as_ref());
    let source = text_source(string, hint, site, reads)?;
    let target = context.distinct_target(column.distinct.as_ref(), filled_rows, reads);
    let categories = match reads.read("listed_values", "/string/top_values", listed_values(string)) {
        Some(top_values) => Some((
            top_values,
            reads.read("rest_fraction", "/string/other_fraction", string.and_then(|s| s.other_fraction)),
            None,
            "/string/top_values",
            source.variety(),
        )),
        None => match reads.read("listed_prefixes", "/string/prefixes", listed_prefixes(string)) {
            Some(prefixes) => {
                let suffix = text::compile_pattern(&prefixes.suffix, PATTERN_MAX_REPEAT)
                    .map_err(site.refuse_as(ColumnFault::InvalidPattern, "/string/prefixes/suffix"))?;
                // A column written as prefixes counts its distinct prefixes, so the rest are drawn to hold as many.
                let prefix_chars = prefixes
                    .values
                    .iter()
                    .map(|top| top.value.chars().count())
                    .max()
                    .unwrap_or_default();
                let variety = source.leading_variety(prefix_chars);
                Some((
                    &prefixes.values,
                    prefixes.other_fraction,
                    Some(suffix),
                    "/string/prefixes/values",
                    variety,
                ))
            }
            None => None,
        },
    };
    if let Some((listed, other_fraction, suffix, field, other_variety)) = categories {
        let values: Vec<_> = listed.iter().map(|top| (top.value.clone(), top.fraction.0)).collect();
        let other_fraction = other_fraction.map_or(0.0, |r| r.0);
        let listed = ListedValues::new(values, other_fraction).map_err(site.refuse_as(ColumnFault::UnusableRates, field))?;
        let other_index = target.and_then(|target| ValueIndex::for_distinct(listed.outside(target), &other_variety));
        return Ok(ValueSampler::Categorical(CategoricalSampler {
            listed,
            suffix: suffix.map(Box::new),
            other: source,
            other_index,
            output,
        }));
    }
    let check_digit = reads.read(
        "check_digit",
        "/semantic/generator_hint/check_digit",
        hint.and_then(|hint| hint.check_digit),
    );
    let check_digit = check_digit.map(|kind| {
        let invalid_fraction = reads.read(
            "invalid_fraction",
            "/quality/invalid_fraction",
            column.quality.as_ref().and_then(|q| q.invalid_fraction),
        );
        let invalid_fraction = invalid_fraction.map_or(0.0, |r| r.0);
        CheckDigitDraw {
            kind,
            invalid: Bernoulli::new(invalid_fraction),
        }
    });
    Ok(ValueSampler::Text(TextSampler {
        index: target.and_then(|target| ValueIndex::for_distinct(target, &source.variety())),
        by_row: false,
        source,
        check_digit,
        output,
    }))
}

/// The whole values a string column lists, which the categorical sampler draws before `prefixes`.
fn listed_values(string: Option<&StringStats>) -> Option<&Vec<crate::profile::TopValue>> {
    string.and_then(|s| s.top_values.as_ref()).filter(|values| !values.is_empty())
}

fn listed_prefixes(string: Option<&StringStats>) -> Option<&crate::profile::Prefixes> {
    string
        .and_then(|s| s.prefixes.as_ref())
        .filter(|prefixes| !prefixes.values.is_empty())
}

fn empty_numeric_stats() -> crate::profile::NumericStats {
    serde_json::from_value(serde_json::json!({})).expect("every numeric statistic is optional")
}

/// Where a string column's generated values come from, most specific first: a provider, a
/// pattern, the shapes, then characters of the profiled classes.
fn text_source(
    string: Option<&StringStats>,
    hint: Option<&GeneratorHint>,
    site: &ColumnSite,
    reads: &mut FieldReads,
) -> Result<TextSource, PlanError> {
    if let Some(provider) = reads.read("provider", "/semantic/generator_hint/provider", hint.and_then(|hint| hint.provider)) {
        if provider == Provider::Icd10cm {
            return Ok(TextSource::Icd10cm {
                dotted: dominant_shape_is_dotted(reads.read("shapes", "/string/shapes", string.and_then(|s| s.shapes.as_deref()))),
            });
        }
        return Ok(TextSource::Provider(provider));
    }
    const HINTED: &str = "/semantic/generator_hint/pattern";
    const STORED: &str = "/string/pattern";
    let pattern = match reads.read("pattern", HINTED, hint.and_then(|hint| hint.pattern.as_deref())) {
        Some(hinted) => Some((hinted, HINTED)),
        None => reads
            .read("pattern", STORED, string.and_then(|s| s.pattern.as_deref()))
            .map(|stored| (stored, STORED)),
    };
    if let Some((pattern, field)) = pattern {
        let compiled = text::compile_pattern(pattern, PATTERN_MAX_REPEAT).map_err(site.refuse_as(ColumnFault::InvalidPattern, field))?;
        return Ok(TextSource::Pattern(Box::new(compiled)));
    }
    let shapes = reads
        .read("shapes", "/string/shapes", string.and_then(|s| s.shapes.as_ref()))
        .filter(|shapes| !shapes.is_empty());
    if let Some(shapes) = shapes {
        let choice = WeightedAliasIndex::new(shapes.iter().map(|shape| shape.fraction.0).collect())
            .map_err(|error| format!("shape rates are unusable: {error}"))
            .map_err(site.refuse_as(ColumnFault::UnusableRates, "/string/shapes"))?;
        let total: f64 = shapes.iter().map(|shape| shape.fraction.0).sum();
        return Ok(TextSource::Shapes {
            choice,
            shapes: shapes
                .iter()
                .map(|shape| Shape::new(&shape.shape, shape.fraction.0 / total))
                .collect(),
        });
    }
    let length = reads.read("length", "/string/length", string.and_then(|s| s.length.as_ref()));
    let length = numeric::bounded_cdf(
        length.and_then(|l| l.quantiles.as_ref()),
        length.and_then(|l| l.min).map(|min| min as f64),
        length.and_then(|l| l.max).map(|max| max as f64),
        DEFAULT_STRING_LENGTH,
    )
    .map_err(site.refuse_as(ColumnFault::InvalidQuantiles, "/string/length"))?;
    let charset = reads.read("charset", "/string/charset", string.and_then(|s| s.charset.as_ref()));
    Ok(TextSource::Characters {
        length,
        alphabet: text::alphabet(charset.and_then(|c| c.classes.as_deref())),
    })
}

/// Whether the column's most common shape holds a dot, as an ICD-10-CM code written `E11.9` does.
fn dominant_shape_is_dotted(shapes: Option<&[crate::profile::Shape]>) -> bool {
    shapes
        .unwrap_or_default()
        .iter()
        .max_by(|a, b| a.fraction.0.total_cmp(&b.fraction.0))
        .is_some_and(|shape| shape.shape.contains('.'))
}

fn list_sampler(
    context: &PlanContext,
    column: &Column,
    site: &ColumnSite,
    large: bool,
    reads: &mut FieldReads,
) -> Result<ValueSampler, PlanError> {
    let list = column.list.as_ref();
    let element = match reads.read("element", "/list/element", list.and_then(|l| l.element.as_deref())) {
        Some(element) => element.clone(),
        None => serde_json::from_value(serde_json::json!({"name": "element", "logical_type": "string"}))
            .expect("a name and a logical type make a column"),
    };
    let length = reads.read("length", "/list/length", list.and_then(|l| l.length.as_ref()));
    let length = numeric::bounded_cdf(length, None, None, DEFAULT_LIST_LENGTH)
        .map_err(site.refuse_as(ColumnFault::InvalidQuantiles, "/list/length"))?;
    let empty_fraction = reads
        .read("empty_fraction", "/list/empty_fraction", list.and_then(|l| l.empty_fraction))
        .map(|r| r.0);
    let per_row = (1.0 - empty_fraction.unwrap_or(0.0)) * nested::mean_length(&length);
    let elements = PlanContext {
        rows: context.rows * per_row,
        source_rows: context.source_rows.map(|rows| rows * per_row),
        described_rows: context.described_rows.map(|rows| rows * per_row),
        ..*context
    };
    Ok(ValueSampler::List(Box::new(ListSampler {
        element: column_plan(&elements, &element, &site.child(&element, "/list/element"))?,
        length,
        empty: empty_fraction.map(Bernoulli::new),
        large,
    })))
}

fn struct_sampler(context: &PlanContext, column: &Column, site: &ColumnSite, reads: &mut FieldReads) -> Result<ValueSampler, PlanError> {
    let stats = column.struct_fields.as_ref();
    let presence = reads.read(
        "presence_fractions",
        "/struct/presence_fractions",
        stats.and_then(|s| s.presence_fractions.as_ref()),
    );
    let mut fields = Vec::new();
    for (index, field) in reads
        .read("fields", "/struct/fields", stats.and_then(|s| s.fields.as_ref()))
        .into_iter()
        .flatten()
        .enumerate()
    {
        let present = presence.and_then(|p| p.get(&field.name)).map_or(1.0, |r| r.0);
        let field_site = site.child(field, &format!("/struct/fields/{index}"));
        fields.push(column_plan_present(context, field, &field_site, present)?);
    }
    if fields.is_empty() {
        let message = "a struct column needs at least one field".to_string();
        return Err(site.refuse_as(ColumnFault::StructWithoutFields, "/struct/fields")(message));
    }
    Ok(ValueSampler::Struct(StructSampler { fields }))
}
