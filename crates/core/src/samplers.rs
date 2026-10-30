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
pub use self::text::shape_of;
pub use self::text_cells::TextCell;
use self::text_cells::TextOnlyValues;
use crate::physical_type;
use crate::physical_type::Container;
use crate::physical_type::PhysicalType;
use crate::plan::ColumnFault;
use crate::plan::PlanError;
use crate::profile::Column;
use crate::profile::Distinct;
use crate::profile::GeneratorHint;
use crate::profile::LogicalType;
use crate::profile::Provider;
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
    /// The source's rows a distinct ratio is counted over, `dataset.row_count`.
    pub(crate) source_rows: Option<f64>,
    /// The source's rows a rate is a share of: those a sample read, or else every row.
    pub(crate) described_rows: Option<f64>,
    /// The profile's `privacy.k_min`, or [`crate::profile::DEFAULT_K_MIN`] where it states none. It bounds the rows
    /// the trimmed extremes leave beyond them.
    pub(crate) k_min: u64,
}

impl PlanContext<'_> {
    /// The distinct values a column is to hold over the dataset, drawn in its `filled_rows`. At or
    /// above the source's rows its ratio holds, so a longer run holds more values. Below them it is
    /// what a random sample of the source holds where each value fills an equal share of its rows,
    /// `count · (1 − (1 − rows/N)^(N/count))`, so a short run keeps the source's values.
    fn distinct_target(&self, distinct: Option<&Distinct>, filled_rows: f64) -> Option<DistinctTarget> {
        let distinct = distinct?;
        let ratio = distinct.ratio?.0;
        let held = ratio * self.rows;
        let source_rows = distinct
            .approx
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
            Self::Boolean(true_rate) => {
                let mut rng = values();
                let values = BooleanBuffer::collect_bool(rows, |_| true_rate.draw(&mut rng));
                Arc::new(BooleanArray::new(values, nulls))
            }
            Self::Text(sampler) => sampler.generate(at, keys, rows, nulls),
            Self::List(sampler) => sampler.generate(at, keys, rows, nulls),
            Self::Struct(sampler) => sampler.generate(at, rows, nulls),
        }
    }
}

/// Everything one column, or one nested field, needs to produce a block.
#[derive(Debug, Clone)]
pub(crate) struct ColumnPlan {
    pub(crate) field: FieldRef,
    /// What draws the values, as a plan reports it: `sequence from 1 by 1`.
    pub(crate) sampler: String,
    /// The share of rows the null mask draws as null.
    pub(crate) null_share: f64,
    keys: StreamKeys,
    values: ValueSampler,
    nulls: Option<Bernoulli>,
    text_only: Option<TextOnlyValues>,
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
        unique && self.nulls.is_none() && self.text_only.is_none()
    }

    pub(crate) fn generate(&self, at: &BlockPosition, rows: usize) -> ArrayRef {
        let nulls = self
            .drawn_nulls(at, rows)
            .map(|nulls| NullBuffer::new(!&nulls))
            .filter(|nulls| nulls.null_count() > 0);
        self.values.generate(at, &self.keys, rows, nulls)
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
}

/// The plan for `column`, which sits at `site`.
pub(crate) fn column_plan(context: &PlanContext, column: &Column, site: &ColumnSite) -> Result<ColumnPlan, PlanError> {
    column_plan_present(context, column, site, 1.0)
}

/// As [`column_plan`], for a field present in `presence` of its parent's rows.
fn column_plan_present(context: &PlanContext, column: &Column, site: &ColumnSite, presence: f64) -> Result<ColumnPlan, PlanError> {
    let keys = StreamKeys::new(context.table, &site.path);
    let may_be_null = column.nullable != Some(false);
    let null_rate = column.null_rate.map_or(0.0, |r| r.0);
    let null_share = if may_be_null {
        null_rate + (1.0 - presence) * (1.0 - null_rate)
    } else {
        0.0
    };
    let values = value_sampler(context, column, site, &keys, context.rows * (1.0 - null_share))?;
    let text_only = TextOnlyValues::new(column.missing_tokens.as_ref(), null_share).map_err(site.refuse("/missing_tokens"))?;
    if site.nested && text_only.is_some() {
        let message = "a text cell names no nested field, so a nested field's missing-value tokens are not generated yet".to_string();
        return Err(site.refuse_as(ColumnFault::NotGeneratedYet, "/missing_tokens")(message));
    }
    let nulls = Some(Bernoulli::new(null_share)).filter(|null| !null.never());
    let mut sampler = describe(column, &values);
    if column.missing_tokens.as_ref().is_some_and(|tokens| !tokens.is_empty()) {
        sampler.push_str(", with missing-value tokens as text");
    }
    Ok(ColumnPlan {
        field: Arc::new(Field::new(&column.name, values.data_type(), may_be_null)),
        sampler,
        null_share: nulls.map_or(0.0, |_| null_share),
        keys,
        values,
        nulls,
        text_only,
    })
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
            let (top, other_rate) = match (listed_values(string), listed_prefixes(string)) {
                (None, Some(prefixes)) => (
                    format!("{} top prefixes, each followed by {}", categorical.listed.len(), prefixes.suffix),
                    prefixes.other_rate,
                ),
                _ => (
                    format!("{} top values", categorical.listed.len()),
                    string.and_then(|s| s.other_rate),
                ),
            };
            match other_rate.is_some_and(|r| r.0 > 0.0) {
                true => format!("{top}, the rest from {}", describe_source(&categorical.other)),
                false => top,
            }
        }
        ValueSampler::Temporal(_) => {
            let temporal = column.temporal.as_ref();
            let weights: Vec<&str> = [
                ("month", temporal.and_then(|t| t.by_month.as_ref()).is_some()),
                ("weekday", temporal.and_then(|t| t.by_dow.as_ref()).is_some()),
                ("hour", temporal.and_then(|t| t.by_hour.as_ref()).is_some()),
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
            let true_rate = column.boolean.as_ref().and_then(|b| b.true_rate).map_or(0.5, |r| r.0);
            format!("true in {true_rate} of rows")
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
        ValueSampler::List(list) => format!("list of {}", list.element.sampler),
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
) -> Result<ValueSampler, PlanError> {
    let scalar = match physical(column).map_err(site.refuse_as(ColumnFault::UnsupportedType, "/physical_type"))? {
        PhysicalType::Container(Container::Struct) => return struct_sampler(context, column, site),
        PhysicalType::Container(container) => return list_sampler(context, column, site, container == Container::LargeList),
        PhysicalType::Scalar(data_type) => data_type,
    };
    let numeric = column.numeric.as_ref();
    if let Some(output) = NumericOutput::from_type(&scalar) {
        if let Some(sequence) = numeric.and_then(|n| n.sequence.as_ref()) {
            return Ok(ValueSampler::Sequence(SequenceSampler {
                start: sequence.start,
                step: sequence.step,
                output,
            }));
        }
        let fit = match numeric.and_then(|n| n.fit.as_ref()) {
            Some(fit) => FittedFamily::from_fit(fit).map_err(site.refuse_as(ColumnFault::InvalidFit, "/numeric/fit"))?,
            None => None,
        };
        let stats = numeric.cloned().unwrap_or_else(empty_numeric_stats);
        let target = context.distinct_target(column.distinct.as_ref(), filled_rows);
        let source = column.distinct.as_ref().and_then(|d| {
            let (distinct, ratio) = (d.approx?, d.ratio?.0);
            let described = context.described_rows.or((ratio > 0.0).then(|| distinct as f64 / ratio))?;
            let null_rate = column.null_rate.map_or(0.0, |r| r.0);
            Some(SourceCounts {
                distinct,
                values: described * (1.0 - null_rate),
                k_min: context.k_min,
            })
        });
        let curve = numeric::numeric_cdf(&stats, fit.as_ref()).map_err(site.refuse_as(ColumnFault::InvalidQuantiles, "/numeric"))?;
        let sampler = NumericSampler::new(curve, &stats, output, target, source)
            .map_err(site.refuse_as(ColumnFault::UnusableRates, "/numeric/top_values"))?;
        return Ok(ValueSampler::Numeric(sampler));
    }
    if let Some(output) = TemporalOutput::from_type(&scalar) {
        let gap_days = || rng_for(context.seed, keys.gap_days, 0);
        let sampler = TemporalSampler::new(column.temporal.as_ref(), output, gap_days).map_err(site.refuse("/temporal"))?;
        return Ok(ValueSampler::Temporal(sampler));
    }
    if scalar == DataType::Boolean {
        let true_rate = column.boolean.as_ref().and_then(|b| b.true_rate).map_or(0.5, |r| r.0);
        return Ok(ValueSampler::Boolean(Bernoulli::new(true_rate)));
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
    let source = text_source(string, hint, site)?;
    let target = context.distinct_target(column.distinct.as_ref(), filled_rows);
    let categories = match (listed_values(string), listed_prefixes(string)) {
        (Some(top_values), _) => Some((
            top_values,
            string.and_then(|s| s.other_rate),
            None,
            "/string/top_values",
            source.variety(),
        )),
        (None, Some(prefixes)) => {
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
                prefixes.other_rate,
                Some(suffix),
                "/string/prefixes/values",
                variety,
            ))
        }
        (None, None) => None,
    };
    if let Some((listed, other_rate, suffix, field, other_variety)) = categories {
        let values: Vec<_> = listed.iter().map(|top| (top.value.clone(), top.rate.0)).collect();
        let other_rate = other_rate.map_or(0.0, |r| r.0);
        let listed = ListedValues::new(values, other_rate).map_err(site.refuse_as(ColumnFault::UnusableRates, field))?;
        let other_index = target.and_then(|target| ValueIndex::for_distinct(listed.outside(target), &other_variety));
        return Ok(ValueSampler::Categorical(CategoricalSampler {
            listed,
            suffix: suffix.map(Box::new),
            other: source,
            other_index,
            output,
        }));
    }
    let check_digit = hint.and_then(|hint| hint.check_digit).map(|kind| {
        let invalid_rate = column.quality.as_ref().and_then(|q| q.invalid_rate).map_or(0.0, |r| r.0);
        CheckDigitDraw {
            kind,
            invalid: Bernoulli::new(invalid_rate),
        }
    });
    Ok(ValueSampler::Text(TextSampler {
        index: target.and_then(|target| ValueIndex::for_distinct(target, &source.variety())),
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
fn text_source(string: Option<&StringStats>, hint: Option<&GeneratorHint>, site: &ColumnSite) -> Result<TextSource, PlanError> {
    if let Some(provider) = hint.and_then(|hint| hint.provider) {
        if provider == Provider::Icd10cm {
            return Ok(TextSource::Icd10cm {
                dotted: dominant_shape_is_dotted(string),
            });
        }
        return Ok(TextSource::Provider(provider));
    }
    let hinted_pattern = hint
        .and_then(|hint| hint.pattern.as_deref())
        .map(|p| (p, "/semantic/generator_hint/pattern"));
    let stored_pattern = string.and_then(|s| s.pattern.as_deref()).map(|p| (p, "/string/pattern"));
    if let Some((pattern, field)) = hinted_pattern.or(stored_pattern) {
        let compiled = text::compile_pattern(pattern, PATTERN_MAX_REPEAT).map_err(site.refuse_as(ColumnFault::InvalidPattern, field))?;
        return Ok(TextSource::Pattern(Box::new(compiled)));
    }
    let shapes = string.and_then(|s| s.shapes.as_ref()).filter(|shapes| !shapes.is_empty());
    if let Some(shapes) = shapes {
        let choice = WeightedAliasIndex::new(shapes.iter().map(|shape| shape.rate.0).collect())
            .map_err(|error| format!("shape rates are unusable: {error}"))
            .map_err(site.refuse_as(ColumnFault::UnusableRates, "/string/shapes"))?;
        let total: f64 = shapes.iter().map(|shape| shape.rate.0).sum();
        return Ok(TextSource::Shapes {
            choice,
            shapes: shapes.iter().map(|shape| Shape::new(&shape.shape, shape.rate.0 / total)).collect(),
        });
    }
    let length = string.and_then(|s| s.length.as_ref());
    let length = numeric::bounded_cdf(
        length.and_then(|l| l.quantiles.as_ref()),
        length.and_then(|l| l.min).map(|min| min as f64),
        length.and_then(|l| l.max).map(|max| max as f64),
        DEFAULT_STRING_LENGTH,
    )
    .map_err(site.refuse_as(ColumnFault::InvalidQuantiles, "/string/length"))?;
    Ok(TextSource::Characters {
        length,
        alphabet: text::alphabet(string.and_then(|s| s.charset.as_ref()).and_then(|c| c.classes.as_deref())),
    })
}

/// Whether the column's most common shape holds a dot, as an ICD-10-CM code written `E11.9` does.
fn dominant_shape_is_dotted(string: Option<&StringStats>) -> bool {
    let shapes = string.and_then(|s| s.shapes.as_deref()).unwrap_or_default();
    shapes
        .iter()
        .max_by(|a, b| a.rate.0.total_cmp(&b.rate.0))
        .is_some_and(|shape| shape.shape.contains('.'))
}

fn list_sampler(context: &PlanContext, column: &Column, site: &ColumnSite, large: bool) -> Result<ValueSampler, PlanError> {
    let list = column.list.as_ref();
    let element = match list.and_then(|l| l.element.as_deref()) {
        Some(element) => element.clone(),
        None => serde_json::from_value(serde_json::json!({"name": "element", "logical_type": "string"}))
            .expect("a name and a logical type make a column"),
    };
    let length = numeric::bounded_cdf(list.and_then(|l| l.length.as_ref()), None, None, DEFAULT_LIST_LENGTH)
        .map_err(site.refuse_as(ColumnFault::InvalidQuantiles, "/list/length"))?;
    let empty_rate = list.and_then(|l| l.empty_rate).map(|r| r.0);
    let per_row = (1.0 - empty_rate.unwrap_or(0.0)) * nested::mean_length(&length);
    let elements = PlanContext {
        rows: context.rows * per_row,
        source_rows: context.source_rows.map(|rows| rows * per_row),
        described_rows: context.described_rows.map(|rows| rows * per_row),
        ..*context
    };
    Ok(ValueSampler::List(Box::new(ListSampler {
        element: column_plan(&elements, &element, &site.child(&element, "/list/element"))?,
        length,
        empty: empty_rate.map(Bernoulli::new),
        large,
    })))
}

fn struct_sampler(context: &PlanContext, column: &Column, site: &ColumnSite) -> Result<ValueSampler, PlanError> {
    let stats = column.struct_fields.as_ref();
    let presence = stats.and_then(|s| s.presence.as_ref());
    let mut fields = Vec::new();
    for (index, field) in stats.and_then(|s| s.fields.as_ref()).into_iter().flatten().enumerate() {
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
