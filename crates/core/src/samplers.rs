//! One sampler per column kind, chosen from a profile column's type and statistics, and the null
//! mask and text-only values every column shares.

mod categorical;
mod distinct_values;
mod file_rows;
mod fitted_family;
mod inverse_cdf;
mod nested;
mod numeric;
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
use self::distinct_values::ValueIndex;
pub use self::file_rows::file_rows_draws;
pub use self::fitted_family::FITTED_FAMILIES;
use self::fitted_family::FittedFamily;
pub use self::fitted_family::FittedFamilyKind;
use self::nested::ListSampler;
use self::nested::StructSampler;
use self::numeric::NumericOutput;
use self::numeric::NumericSampler;
use self::numeric::Rounding;
use self::numeric::SequenceSampler;
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
use crate::code_lists::provider_list;
use crate::physical_type;
use crate::physical_type::Container;
use crate::physical_type::PhysicalType;
use crate::plan::ColumnFault;
use crate::plan::PlanError;
use crate::profile::Column;
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
/// An ICD-10-CM code longer than its category is written with a dot after the category's three
/// characters.
const ICD10CM_DOT_AFTER: usize = 3;

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
    type_mix: u64,
    type_mix_text: u64,
    missing_tokens: u64,
    invalid: u64,
    lengths: u64,
    /// Drawn once at plan time, from block 0, to pick a date column's empty days.
    gap_days: u64,
    /// A row's index into a string column's values, where the column draws one.
    index: u64,
    /// Positioned at an index rather than a block: the value at that index.
    indexed_values: u64,
}

impl StreamKeys {
    fn new(table: &str, path: &str) -> Self {
        let key = |draw| stream_key(table, path, draw);
        Self {
            values: key("values"),
            nulls: key("nulls"),
            type_mix: key("type_mix"),
            type_mix_text: key("type_mix_text"),
            missing_tokens: key("missing_tokens"),
            invalid: key("invalid"),
            lengths: key("lengths"),
            gap_days: key("gap_days"),
            index: key("index"),
            indexed_values: key("indexed_values"),
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
            Self::Numeric(sampler) => sampler.generate(&mut values(), rows, nulls),
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
    pub(crate) fn generate(&self, at: &BlockPosition, rows: usize) -> ArrayRef {
        let drawn_nulls = self.drawn_nulls(at, rows);
        let own_type = self
            .text_only
            .as_ref()
            .and_then(|text_only| text_only.own_type_rows(&mut rng_for(at.seed, self.keys.type_mix, at.block), rows));
        let valid = match (drawn_nulls.map(|nulls| !&nulls), own_type) {
            (Some(not_null), Some(own_type)) => Some(&not_null & &own_type),
            (valid, None) | (None, valid) => valid,
        };
        let nulls = valid.map(NullBuffer::new).filter(|nulls| nulls.null_count() > 0);
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
        let Some(text_only) = &self.text_only else {
            return Vec::new();
        };
        let stream = |key| rng_for(at.seed, key, at.block);
        text_only.cells(
            self.drawn_nulls(at, rows).as_ref(),
            &mut stream(self.keys.type_mix),
            &mut stream(self.keys.type_mix_text),
            &mut stream(self.keys.missing_tokens),
            rows,
        )
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
    let string_column = matches!(values.data_type(), DataType::Utf8 | DataType::LargeUtf8);
    let text_only = TextOnlyValues::new(column.type_mix.as_ref(), column.missing_tokens.as_ref(), null_share, string_column)
        .map_err(site.refuse("/missing_tokens"))?;
    if site.nested
        && let Some(text_only) = &text_only
    {
        let field = if text_only.has_foreign_values() {
            "/type_mix"
        } else {
            "/missing_tokens"
        };
        let message = "a text cell names no nested field, so a nested field's text-only values are not generated yet".to_string();
        return Err(site.refuse_as(ColumnFault::NotGeneratedYet, field)(message));
    }
    let nulls = Some(Bernoulli::new(null_share)).filter(|null| !null.never());
    let nullable = may_be_null || text_only.as_ref().is_some_and(TextOnlyValues::has_foreign_values);
    let mut sampler = describe(column, &values);
    if column.type_mix.as_ref().is_some_and(|mix| !mix.is_empty()) {
        sampler.push_str(", with values of other types as text");
    }
    if column.missing_tokens.as_ref().is_some_and(|tokens| !tokens.is_empty()) {
        sampler.push_str(", with missing-value tokens as text");
    }
    Ok(ColumnPlan {
        field: Arc::new(Field::new(&column.name, values.data_type(), nullable)),
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
        ValueSampler::Numeric(_) => {
            let numeric = column.numeric.as_ref();
            let fit = numeric
                .and_then(|n| n.fit.as_ref())
                .filter(|fit| FittedFamily::from_fit(fit).is_ok_and(|family| family.is_some()));
            let body = if numeric.and_then(|n| n.quantiles.as_ref()).is_some_and(|q| !q.0.is_empty()) {
                "quantiles".to_string()
            } else if numeric.and_then(|n| n.histogram.as_ref()).is_some() {
                "histogram".to_string()
            } else if let Some(fit) = fit {
                return format!("fitted {}", fit.family);
            } else if numeric.and_then(|n| n.extremes.as_ref()).is_some() {
                return "uniform between the extremes".to_string();
            } else {
                return format!("uniform from {} to {}", numeric::DEFAULT_RANGE.0, numeric::DEFAULT_RANGE.1);
            };
            match fit {
                Some(fit) => format!("{body} with fitted {} tails", fit.family),
                None => format!("{body} with linear tails"),
            }
        }
        ValueSampler::Categorical(categorical) => {
            let string = column.string.as_ref();
            let (top, other_rate) = match (listed_values(string), listed_prefixes(string)) {
                (None, Some(prefixes)) => (
                    format!("{} top prefixes, each followed by {}", categorical.values.len(), prefixes.suffix),
                    prefixes.other_rate,
                ),
                _ => (
                    format!("{} top values", categorical.values.len()),
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
        TextSource::CodeList { list, dot_after } => match dot_after {
            Some(at) => format!("the {} list, dotted after {at} characters", list.name),
            None => format!("the {} list", list.name),
        },
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
        return Ok(ValueSampler::Numeric(NumericSampler {
            cdf: numeric::numeric_cdf(&stats, fit.as_ref()).map_err(site.refuse_as(ColumnFault::InvalidQuantiles, "/numeric"))?,
            rounding: Rounding::for_column(numeric, &output),
            output,
        }));
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
    // The ratio is of the dataset's rows, so a run longer than the profiled one holds more values.
    let distinct = column.distinct.as_ref().and_then(|d| d.ratio).map(|ratio| ratio.0 * context.rows);
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
        let other_share = other_rate / (values.iter().map(|(_, rate)| rate).sum::<f64>() + other_rate);
        let listed_distinct = values.len() as f64;
        let other_index =
            distinct.and_then(|distinct| ValueIndex::for_distinct(distinct - listed_distinct, filled_rows * other_share, &other_variety));
        let sampler = CategoricalSampler::new(values, other_rate, suffix, source, other_index, output)
            .map_err(site.refuse_as(ColumnFault::UnusableRates, field))?;
        return Ok(ValueSampler::Categorical(sampler));
    }
    let check_digit = hint.and_then(|hint| hint.check_digit).map(|kind| {
        let invalid_rate = column.quality.as_ref().and_then(|q| q.invalid_rate).map_or(0.0, |r| r.0);
        CheckDigitDraw {
            kind,
            invalid: Bernoulli::new(invalid_rate),
        }
    });
    Ok(ValueSampler::Text(TextSampler {
        index: distinct.and_then(|distinct| ValueIndex::for_distinct(distinct, filled_rows, &source.variety())),
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
        if let Some(list) = provider_list(provider) {
            return Ok(TextSource::CodeList {
                list,
                dot_after: (provider == Provider::Icd10cm && dominant_shape_is_dotted(string)).then_some(ICD10CM_DOT_AFTER),
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
    let elements = PlanContext {
        rows: context.rows * (1.0 - empty_rate.unwrap_or(0.0)) * nested::mean_length(&length),
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
