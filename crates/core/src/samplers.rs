//! One sampler per column kind, chosen from a profile column's type and statistics, and the null
//! mask and text-only values every column shares.

mod categorical;
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
use self::fitted_family::FittedFamily;
use self::nested::ListSampler;
use self::nested::StructSampler;
use self::numeric::NumericOutput;
use self::numeric::NumericSampler;
use self::numeric::Rounding;
use self::numeric::SequenceSampler;
use self::temporal::TemporalOutput;
use self::temporal::TemporalSampler;
use self::text::CheckDigit;
use self::text::Provider;
use self::text::StringOutput;
use self::text::TextSampler;
use self::text::TextSource;
pub use self::text_cells::TextCell;
use self::text_cells::TextOnlyValues;
use crate::physical_type;
use crate::physical_type::Container;
use crate::physical_type::PhysicalType;
use crate::profile::Column;
use crate::profile::LogicalType;
use crate::profile::StringStats;
use crate::rng::Bernoulli;
use crate::rng::draw;
use crate::rng::rng_for;
use crate::rng::stream_key;

/// The length a string with no stored length statistics is drawn between.
const DEFAULT_STRING_LENGTH: (f64, f64) = (8.0, 16.0);
/// The length a list with no stored length statistics is drawn between.
const DEFAULT_LIST_LENGTH: (f64, f64) = (0.0, 4.0);
/// The most extra repeats a pattern's `*`, `+` or `{n,}` generates.
const PATTERN_MAX_REPEAT: u32 = 16;

/// Where a block sits: the seed, the block's number, and the dataset row its first row is.
#[derive(Debug, Clone, Copy)]
pub(crate) struct BlockPosition {
    pub(crate) seed: u64,
    pub(crate) block: u64,
    pub(crate) first_row: u64,
}

/// What every column's plan is built in: the table its streams are named for, and the seed.
pub(crate) struct PlanContext<'a> {
    pub(crate) table: &'a str,
    pub(crate) seed: u64,
}

/// A column's stream names, hashed once.
#[derive(Debug, Clone, Copy)]
pub(crate) struct StreamKeys {
    values: u64,
    nulls: u64,
    type_mix: u64,
    type_mix_text: u64,
    missing_tokens: u64,
    invalid: u64,
    lengths: u64,
}

impl StreamKeys {
    fn new(table: &str, path: &str) -> Self {
        let key = |name| stream_key(table, path, name);
        Self {
            values: key(draw::VALUES),
            nulls: key(draw::NULLS),
            type_mix: key(draw::TYPE_MIX),
            type_mix_text: key(draw::TYPE_MIX_TEXT),
            missing_tokens: key(draw::MISSING_TOKENS),
            invalid: key(draw::INVALID),
            lengths: key(draw::LENGTHS),
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
            Self::Categorical(sampler) => sampler.generate(&mut values(), rows, nulls),
            Self::Temporal(sampler) => sampler.generate(&mut values(), rows, nulls),
            Self::Boolean(true_rate) => {
                let mut rng = values();
                let values = BooleanBuffer::collect_bool(rows, |_| true_rate.draw(&mut rng));
                Arc::new(BooleanArray::new(values, nulls))
            }
            Self::Text(sampler) => {
                let mut invalid = rng_for(at.seed, keys.invalid, at.block);
                sampler.generate(&mut values(), &mut invalid, rows, nulls)
            }
            Self::List(sampler) => sampler.generate(at, keys, rows, nulls),
            Self::Struct(sampler) => sampler.generate(at, rows, nulls),
        }
    }
}

/// Everything one column, or one nested field, needs to produce a block.
#[derive(Debug, Clone)]
pub(crate) struct ColumnPlan {
    pub(crate) field: FieldRef,
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

/// The plan for `column` at `path`, the dotted path its streams are named by.
pub(crate) fn column_plan(context: &PlanContext, column: &Column, path: &str) -> Result<ColumnPlan, String> {
    column_plan_present(context, column, path, 1.0)
}

/// As [`column_plan`], for a field present in `presence` of its parent's rows.
fn column_plan_present(context: &PlanContext, column: &Column, path: &str, presence: f64) -> Result<ColumnPlan, String> {
    let keys = StreamKeys::new(context.table, path);
    let values = value_sampler(context, column, path)?;
    let text_only = TextOnlyValues::new(column.type_mix.as_ref(), column.missing_tokens.as_ref())?;
    let may_be_null = column.nullable != Some(false);
    let null_rate = column.null_rate.map_or(0.0, |r| r.0);
    let null_share = if may_be_null { 1.0 - presence * (1.0 - null_rate) } else { 0.0 };
    let nulls = Some(Bernoulli::new(null_share)).filter(|null| !null.never());
    let nullable = may_be_null || text_only.as_ref().is_some_and(TextOnlyValues::has_foreign_values);
    Ok(ColumnPlan {
        field: Arc::new(Field::new(&column.name, values.data_type(), nullable)),
        keys,
        values,
        nulls,
        text_only,
    })
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

fn value_sampler(context: &PlanContext, column: &Column, path: &str) -> Result<ValueSampler, String> {
    let scalar = match physical(column)? {
        PhysicalType::Container(Container::Struct) => return struct_sampler(context, column, path),
        PhysicalType::Container(container) => return list_sampler(context, column, path, container == Container::LargeList),
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
            Some(fit) => FittedFamily::from_fit(fit)?,
            None => None,
        };
        let stats = numeric.cloned().unwrap_or_else(empty_numeric_stats);
        return Ok(ValueSampler::Numeric(NumericSampler {
            cdf: numeric::numeric_cdf(&stats, fit.as_ref())?,
            rounding: Rounding::for_column(numeric, &output),
            output,
        }));
    }
    if let Some(output) = TemporalOutput::from_type(&scalar) {
        let gap_days = || rng_for(context.seed, stream_key(context.table, path, draw::GAP_DAYS), 0);
        return Ok(ValueSampler::Temporal(TemporalSampler::new(
            column.temporal.as_ref(),
            output,
            gap_days,
        )?));
    }
    if scalar == DataType::Boolean {
        let true_rate = column.boolean.as_ref().and_then(|b| b.true_rate).map_or(0.5, |r| r.0);
        return Ok(ValueSampler::Boolean(Bernoulli::new(true_rate)));
    }
    let Some(output) = StringOutput::from_type(&scalar) else {
        return Err(format!("{scalar} is not a type this engine generates"));
    };
    if column.temporal.is_some() {
        return Err("a string column with temporal statistics is not generated yet".into());
    }
    let string = column.string.as_ref();
    let hint = column.semantic.as_ref().and_then(|s| s.generator_hint.as_ref());
    let source = text_source(string, hint)?;
    let top_values = string.and_then(|s| s.top_values.as_ref()).filter(|values| !values.is_empty());
    if let Some(top_values) = top_values {
        let values = top_values.iter().map(|top| (top.value.clone(), top.rate.0)).collect();
        let other_rate = string.and_then(|s| s.other_rate).map_or(0.0, |r| r.0);
        return Ok(ValueSampler::Categorical(CategoricalSampler::new(
            values, other_rate, source, output,
        )?));
    }
    let check_digit = match hint.and_then(|hint| hint.get("check_digit")) {
        Some(name) => {
            CheckDigit::from_name(name.as_str().ok_or("generator_hint.check_digit is not a string")?)?;
            let invalid_rate = column.quality.as_ref().and_then(|q| q.invalid_rate).map_or(0.0, |r| r.0);
            Some(CheckDigit {
                invalid: Bernoulli::new(invalid_rate),
            })
        }
        None => None,
    };
    Ok(ValueSampler::Text(TextSampler {
        source,
        check_digit,
        output,
    }))
}

fn empty_numeric_stats() -> crate::profile::NumericStats {
    serde_json::from_value(serde_json::json!({})).expect("every numeric statistic is optional")
}

/// Where a string column's generated values come from, most specific first: a provider, a
/// pattern, the shapes, then characters of the profiled classes.
fn text_source(string: Option<&StringStats>, hint: Option<&crate::profile::OpenObject>) -> Result<TextSource, String> {
    let hinted = |key: &str| match hint.and_then(|hint| hint.get(key)) {
        Some(value) => value
            .as_str()
            .map(Some)
            .ok_or_else(|| format!("generator_hint.{key} is not a string")),
        None => Ok(None),
    };
    if let Some(provider) = hinted("provider")? {
        return Ok(TextSource::Provider(Provider::from_name(provider)?));
    }
    if let Some(pattern) = hinted("pattern")?.or(string.and_then(|s| s.pattern.as_deref())) {
        return Ok(TextSource::Pattern(Box::new(text::compile_pattern(pattern, PATTERN_MAX_REPEAT)?)));
    }
    let shapes = string.and_then(|s| s.shapes.as_ref()).filter(|shapes| !shapes.is_empty());
    if let Some(shapes) = shapes {
        let choice = WeightedAliasIndex::new(shapes.iter().map(|shape| shape.rate.0).collect())
            .map_err(|error| format!("shape rates are unusable: {error}"))?;
        return Ok(TextSource::Shapes {
            choice,
            shapes: shapes.iter().map(|shape| text::shape_chars(&shape.shape)).collect(),
        });
    }
    let length = string.and_then(|s| s.length.as_ref());
    Ok(TextSource::Characters {
        length: numeric::bounded_cdf(
            length.and_then(|l| l.quantiles.as_ref()),
            length.and_then(|l| l.min).map(|min| min as f64),
            length.and_then(|l| l.max).map(|max| max as f64),
            DEFAULT_STRING_LENGTH,
        )?,
        alphabet: text::alphabet(string.and_then(|s| s.charset.as_ref()).and_then(|c| c.classes.as_deref())),
    })
}

fn child_path(path: &str, child: &Column) -> String {
    child.path.clone().unwrap_or_else(|| format!("{path}.{}", child.name))
}

fn list_sampler(context: &PlanContext, column: &Column, path: &str, large: bool) -> Result<ValueSampler, String> {
    let list = column.list.as_ref();
    let element = match list.and_then(|l| l.element.as_deref()) {
        Some(element) => element.clone(),
        None => serde_json::from_value(serde_json::json!({"name": "element", "logical_type": "string"}))
            .expect("a name and a logical type make a column"),
    };
    let element_path = child_path(path, &element);
    Ok(ValueSampler::List(Box::new(ListSampler {
        element: column_plan(context, &element, &element_path).map_err(|e| format!("{element_path}: {e}"))?,
        length: numeric::bounded_cdf(list.and_then(|l| l.length.as_ref()), None, None, DEFAULT_LIST_LENGTH)?,
        empty: list.and_then(|l| l.empty_rate).map(|r| Bernoulli::new(r.0)),
        large,
    })))
}

fn struct_sampler(context: &PlanContext, column: &Column, path: &str) -> Result<ValueSampler, String> {
    let stats = column.struct_fields.as_ref();
    let presence = stats.and_then(|s| s.presence.as_ref());
    let mut fields = Vec::new();
    for field in stats.and_then(|s| s.fields.as_ref()).into_iter().flatten() {
        let field_path = child_path(path, field);
        let present = presence.and_then(|p| p.get(&field.name)).map_or(1.0, |r| r.0);
        let plan = column_plan_present(context, field, &field_path, present).map_err(|e| format!("{field_path}: {e}"))?;
        fields.push(plan);
    }
    if fields.is_empty() {
        return Err("a struct column needs at least one field in struct.fields".into());
    }
    Ok(ValueSampler::Struct(StructSampler { fields }))
}
