//! Numeric columns: a counting sequence, or one uniform draw per row through a piecewise-linear
//! inverse CDF whose knots come from the profile.
//!
//! The knots are the stored quantiles, with the extremes at levels 0 and 1. A histogram's edges at
//! their cumulative shares of every row, its overflow included, fill the body between the
//! outermost quantiles, and `zero_rate` adds a flat run at zero. Beyond the outermost quantile, a
//! fitted family's conditional tail, truncated at the extremes, is tabulated into dense knots;
//! without a usable fit the tail is linear.

use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_array::ArrowPrimitiveType;
use arrow_array::PrimitiveArray;
use arrow_array::types::Decimal128Type;
use arrow_array::types::Float32Type;
use arrow_array::types::Float64Type;
use arrow_array::types::Int8Type;
use arrow_array::types::Int16Type;
use arrow_array::types::Int32Type;
use arrow_array::types::Int64Type;
use arrow_array::types::UInt8Type;
use arrow_array::types::UInt16Type;
use arrow_array::types::UInt32Type;
use arrow_array::types::UInt64Type;
use arrow_buffer::NullBuffer;
use arrow_buffer::ScalarBuffer;
use arrow_schema::DataType;
use rand_chacha::ChaCha8Rng;

use super::fitted_family::FittedFamily;
use super::inverse_cdf::InverseCdf;
use super::inverse_cdf::Knots;
use crate::profile::Histogram;
use crate::profile::NumericStats;
use crate::profile::Quantiles;
use crate::rng::unit;

/// Knots tabulated across each fitted tail.
const TAIL_KNOTS: i32 = 80;
/// Tail knot `k` sits where `2^(-k · TAIL_GRID_STEP)` of the tail's share remains, so the knots
/// close in on the extreme geometrically and the last leaves about a millionth of it.
const TAIL_GRID_STEP: f64 = 0.25;
/// The range a numeric column with no quantiles, histogram, fit or extremes is drawn from.
pub(crate) const DEFAULT_RANGE: (f64, f64) = (0.0, 1000.0);

/// The level a quantile's name stands for: `p05` is 0.05, `p999` is 0.999.
pub(crate) fn quantile_level(name: &str) -> Option<f64> {
    let digits = name.strip_prefix('p')?;
    let numerator: u64 = digits.parse().ok()?;
    let denominator = (0..digits.len()).fold(1.0, |power, _| power * 10.0);
    Some(numerator as f64 / denominator)
}

/// A profile's quantiles as knots in level order.
pub(crate) fn quantile_knots(quantiles: Option<&Quantiles>) -> Result<Vec<(f64, f64)>, String> {
    let mut knots = Vec::new();
    for (name, &value) in quantiles.map(|q| &q.0).into_iter().flatten() {
        let level = quantile_level(name).ok_or_else(|| format!("quantile `{name}` names no level"))?;
        knots.push((level, value));
    }
    knots.sort_by(|a, b| a.0.total_cmp(&b.0));
    Ok(knots)
}

fn histogram_knots(histogram: Option<&Histogram>) -> Vec<(f64, f64)> {
    let Some(histogram) = histogram else {
        return Vec::new();
    };
    let (outside_below, outside_above) = histogram
        .overflow
        .as_ref()
        .map_or((0, 0), |overflow| (overflow.below, overflow.above));
    let total = outside_below + histogram.counts.iter().sum::<u64>() + outside_above;
    if total == 0 || histogram.edges.len() != histogram.counts.len() + 1 {
        return Vec::new();
    }
    let mut below = outside_below;
    let mut knots = vec![(below as f64 / total as f64, histogram.edges[0])];
    for (&count, &edge) in histogram.counts.iter().zip(&histogram.edges[1..]) {
        below += count;
        knots.push((below as f64 / total as f64, edge));
    }
    knots
}

/// The inverse CDF a numeric column draws through.
pub(crate) fn numeric_cdf(stats: &NumericStats, fit: Option<&FittedFamily>) -> Result<InverseCdf, String> {
    let extremes = stats.extremes.as_ref();
    let low = extremes.and_then(|e| e.low);
    let high = extremes.and_then(|e| e.high);
    let quantiles = quantile_knots(stats.quantiles.as_ref())?;
    let histogram = histogram_knots(stats.histogram.as_ref());
    let (frame, interior) = if quantiles.is_empty() {
        (histogram, Vec::new())
    } else {
        (quantiles, histogram)
    };
    let mut knots = Knots::from_sorted(frame).ok_or("the quantiles or histogram edges fall")?;
    if knots.is_empty() {
        knots = match fit {
            Some(fit) => {
                let median = fit.inverse(0.5);
                let median = median.max(low.unwrap_or(median)).min(high.unwrap_or(median));
                Knots::from_sorted(vec![(0.5, median)]).expect("one knot is in order")
            }
            None => {
                let low = low.unwrap_or(DEFAULT_RANGE.0);
                let high = high.unwrap_or(DEFAULT_RANGE.1).max(low);
                Knots::from_sorted(vec![(0.0, low), (1.0, high)]).expect("low is at most high")
            }
        };
    }
    let (body_low, body_high) = (knots.first(), knots.last());
    if let Some((level, value)) = body_low.filter(|&(level, _)| level > 0.0) {
        let low = low.filter(|&low| low <= value);
        match fit.and_then(|fit| lower_tail(fit, low, level, value)) {
            Some(tail) => knots.extend(tail),
            None => _ = knots.insert(0.0, low.unwrap_or(value)),
        }
    }
    if let Some((level, value)) = body_high.filter(|&(level, _)| level < 1.0) {
        let high = high.filter(|&high| high >= value);
        match fit.and_then(|fit| upper_tail(fit, level, value, high)) {
            Some(tail) => knots.extend(tail),
            None => _ = knots.insert(1.0, high.unwrap_or(value)),
        }
    }
    if let (Some((first, _)), Some((last, _))) = (body_low, body_high) {
        for (level, value) in interior.into_iter().filter(|&(level, _)| first < level && level < last) {
            knots.insert(level, value);
        }
    }
    if let Some(zero_rate) = stats.zero_rate.map(|r| r.0).filter(|&r| r > 0.0) {
        let below_zero = stats.negative_rate.map(|r| r.0).or_else(|| {
            let lowest = knots.first().map_or(0.0, |(_, value)| value);
            (lowest >= 0.0).then_some(0.0)
        });
        if let Some(below_zero) = below_zero {
            knots.insert(below_zero, 0.0);
            knots.insert((below_zero + zero_rate).min(1.0), 0.0);
        }
    }
    Ok(InverseCdf::new(knots))
}

/// The (level, value) knots of the inverse CDF the generator draws a numeric column through, its
/// fitted tail included, before any rounding. A check reads a profile through these, so it and the
/// generator read one distribution.
pub fn numeric_cdf_knots(stats: &NumericStats) -> Result<Vec<(f64, f64)>, String> {
    let fit = match stats.fit.as_ref() {
        Some(fit) => FittedFamily::from_fit(fit)?,
        None => None,
    };
    Ok(numeric_cdf(stats, fit.as_ref())?.knots())
}

/// An inverse CDF over stored quantiles between a minimum and a maximum, linear between them, or
/// uniform over `default` when nothing is stored. String and list lengths draw through it.
pub(crate) fn bounded_cdf(
    quantiles: Option<&Quantiles>,
    low: Option<f64>,
    high: Option<f64>,
    default: (f64, f64),
) -> Result<InverseCdf, String> {
    let mut knots = Knots::from_sorted(quantile_knots(quantiles)?).ok_or("the quantiles fall")?;
    if knots.is_empty() {
        let low = low.unwrap_or(default.0);
        let high = high.unwrap_or(default.1).max(low);
        knots.extend([(0.0, low), (1.0, high)]);
    }
    let (first, last) = (knots.first(), knots.last());
    if let Some((_, value)) = first {
        knots.insert(0.0, low.filter(|&low| low <= value).unwrap_or(value));
    }
    if let Some((_, value)) = last {
        knots.insert(1.0, high.filter(|&high| high >= value).unwrap_or(value));
    }
    Ok(InverseCdf::new(knots))
}

/// Knots from level 0 up to `level`, drawn from the fit conditioned on [`low`, `value`].
fn lower_tail(fit: &FittedFamily, low: Option<f64>, level: f64, value: f64) -> Option<Vec<(f64, f64)>> {
    let floor = low.map_or(0.0, |low| fit.cdf_below(low));
    let top = fit.cdf(value);
    if top.partial_cmp(&floor) != Some(std::cmp::Ordering::Greater) {
        return None;
    }
    let mut knots: Vec<(f64, f64)> = (1..=TAIL_KNOTS)
        .rev()
        .map(|k| {
            let share = libm::exp2(-f64::from(k) * TAIL_GRID_STEP);
            let drawn = fit.inverse(floor + share * (top - floor));
            (share * level, drawn.max(low.unwrap_or(drawn)).min(value))
        })
        .collect();
    let deepest = knots[0].1;
    knots.insert(0, (0.0, low.unwrap_or(deepest)));
    Some(knots)
}

/// Knots from `level` up to level 1, drawn from the fit conditioned on [`value`, `high`].
fn upper_tail(fit: &FittedFamily, level: f64, value: f64, high: Option<f64>) -> Option<Vec<(f64, f64)>> {
    let floor = fit.cdf_below(value);
    let top = high.map_or(1.0, |high| fit.cdf(high));
    if top.partial_cmp(&floor) != Some(std::cmp::Ordering::Greater) {
        return None;
    }
    let mut knots: Vec<(f64, f64)> = (1..=TAIL_KNOTS)
        .map(|k| {
            let share = 1.0 - libm::exp2(-f64::from(k) * TAIL_GRID_STEP);
            let drawn = fit.inverse(floor + share * (top - floor));
            (level + share * (1.0 - level), drawn.min(high.unwrap_or(drawn)).max(value))
        })
        .collect();
    let farthest = knots[knots.len() - 1].1;
    knots.push((1.0, high.unwrap_or(farthest)));
    Some(knots)
}

/// How a drawn value is rounded before it is stored.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Rounding {
    Exact,
    Integer,
    /// To this many decimal places, as 10 to their power.
    Decimals(f64),
}

impl Rounding {
    pub(crate) fn for_column(stats: Option<&NumericStats>, output: &NumericOutput) -> Self {
        let integral = stats.and_then(|s| s.integral).unwrap_or(false);
        if integral || output.is_integer() {
            return Self::Integer;
        }
        match (output, stats.and_then(|s| s.scale)) {
            (NumericOutput::Float32 | NumericOutput::Float64, Some(scale)) => Self::Decimals(power_of_ten(scale)),
            _ => Self::Exact,
        }
    }

    fn apply(self, value: f64) -> f64 {
        match self {
            Self::Exact => value,
            Self::Integer => value.round(),
            Self::Decimals(factor) => (value * factor).round() / factor,
        }
    }
}

fn power_of_ten(exponent: i32) -> f64 {
    let power = (0..exponent.unsigned_abs()).fold(1.0, |power, _| power * 10.0);
    if exponent < 0 { 1.0 / power } else { power }
}

#[derive(Debug, Clone)]
pub(crate) struct NumericSampler {
    pub(crate) cdf: InverseCdf,
    pub(crate) rounding: Rounding,
    pub(crate) output: NumericOutput,
}

impl NumericSampler {
    pub(crate) fn generate(&self, rng: &mut ChaCha8Rng, rows: usize, nulls: Option<NullBuffer>) -> ArrayRef {
        self.output
            .drawn_array(rows, nulls, || self.rounding.apply(self.cdf.value_at(unit(rng))))
    }
}

/// An integer column counting from `start` by `step`, which draws nothing.
#[derive(Debug, Clone)]
pub(crate) struct SequenceSampler {
    pub(crate) start: i64,
    pub(crate) step: i64,
    pub(crate) output: NumericOutput,
}

impl SequenceSampler {
    pub(crate) fn generate(&self, first_row: u64, rows: usize, nulls: Option<NullBuffer>) -> ArrayRef {
        let at = |row: usize| {
            let index = i128::from(first_row) + row as i128;
            let value = i128::from(self.start) + i128::from(self.step) * index;
            value.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
        };
        self.output.exact_array(rows, nulls, at)
    }
}

/// The Arrow type a numeric column is stored as.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum NumericOutput {
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Decimal128 { precision: u8, scale: i8 },
}

pub(crate) fn primitive<T: ArrowPrimitiveType>(
    rows: usize,
    nulls: Option<NullBuffer>,
    value: impl FnMut(usize) -> T::Native,
) -> PrimitiveArray<T> {
    let values: ScalarBuffer<T::Native> = (0..rows).map(value).collect();
    PrimitiveArray::new(values, nulls)
}

impl NumericOutput {
    pub(crate) fn from_type(data_type: &DataType) -> Option<Self> {
        Some(match data_type {
            DataType::Int8 => Self::Int8,
            DataType::Int16 => Self::Int16,
            DataType::Int32 => Self::Int32,
            DataType::Int64 => Self::Int64,
            DataType::UInt8 => Self::UInt8,
            DataType::UInt16 => Self::UInt16,
            DataType::UInt32 => Self::UInt32,
            DataType::UInt64 => Self::UInt64,
            DataType::Float32 => Self::Float32,
            DataType::Float64 => Self::Float64,
            &DataType::Decimal128(precision, scale) if (1..=38).contains(&precision) => Self::Decimal128 { precision, scale },
            _ => return None,
        })
    }

    pub(crate) fn data_type(&self) -> DataType {
        match *self {
            Self::Int8 => DataType::Int8,
            Self::Int16 => DataType::Int16,
            Self::Int32 => DataType::Int32,
            Self::Int64 => DataType::Int64,
            Self::UInt8 => DataType::UInt8,
            Self::UInt16 => DataType::UInt16,
            Self::UInt32 => DataType::UInt32,
            Self::UInt64 => DataType::UInt64,
            Self::Float32 => DataType::Float32,
            Self::Float64 => DataType::Float64,
            Self::Decimal128 { precision, scale } => DataType::Decimal128(precision, scale),
        }
    }

    pub(crate) fn is_integer(&self) -> bool {
        !matches!(self, Self::Float32 | Self::Float64 | Self::Decimal128 { .. })
    }

    /// An array of drawn values. Each integer type saturates at its bounds, and a decimal at its
    /// precision.
    pub(crate) fn drawn_array(&self, rows: usize, nulls: Option<NullBuffer>, mut value: impl FnMut() -> f64) -> ArrayRef {
        match *self {
            Self::Int8 => Arc::new(primitive::<Int8Type>(rows, nulls, |_| value() as i8)),
            Self::Int16 => Arc::new(primitive::<Int16Type>(rows, nulls, |_| value() as i16)),
            Self::Int32 => Arc::new(primitive::<Int32Type>(rows, nulls, |_| value() as i32)),
            Self::Int64 => Arc::new(primitive::<Int64Type>(rows, nulls, |_| value() as i64)),
            Self::UInt8 => Arc::new(primitive::<UInt8Type>(rows, nulls, |_| value() as u8)),
            Self::UInt16 => Arc::new(primitive::<UInt16Type>(rows, nulls, |_| value() as u16)),
            Self::UInt32 => Arc::new(primitive::<UInt32Type>(rows, nulls, |_| value() as u32)),
            Self::UInt64 => Arc::new(primitive::<UInt64Type>(rows, nulls, |_| value() as u64)),
            Self::Float32 => Arc::new(primitive::<Float32Type>(rows, nulls, |_| value() as f32)),
            Self::Float64 => Arc::new(primitive::<Float64Type>(rows, nulls, |_| value())),
            Self::Decimal128 { precision, scale } => {
                let factor = power_of_ten(i32::from(scale));
                let largest = 10i128.pow(u32::from(precision)) - 1;
                let array = primitive::<Decimal128Type>(rows, nulls, |_| ((value() * factor).round() as i128).clamp(-largest, largest));
                Arc::new(array.with_precision_and_scale(precision, scale).expect("precision is 1 to 38"))
            }
        }
    }

    /// An array of exact integers, such as a sequence's, which saturate at the type's bounds.
    pub(crate) fn exact_array(&self, rows: usize, nulls: Option<NullBuffer>, value: impl Fn(usize) -> i64) -> ArrayRef {
        let narrow = |row: usize, low: i64, high: i64| value(row).clamp(low, high);
        match *self {
            Self::Int8 => Arc::new(primitive::<Int8Type>(rows, nulls, |r| narrow(r, -128, 127) as i8)),
            Self::Int16 => Arc::new(primitive::<Int16Type>(rows, nulls, |r| {
                narrow(r, i16::MIN.into(), i16::MAX.into()) as i16
            })),
            Self::Int32 => Arc::new(primitive::<Int32Type>(rows, nulls, |r| {
                narrow(r, i32::MIN.into(), i32::MAX.into()) as i32
            })),
            Self::Int64 => Arc::new(primitive::<Int64Type>(rows, nulls, &value)),
            Self::UInt8 => Arc::new(primitive::<UInt8Type>(rows, nulls, |r| narrow(r, 0, 255) as u8)),
            Self::UInt16 => Arc::new(primitive::<UInt16Type>(rows, nulls, |r| narrow(r, 0, u16::MAX.into()) as u16)),
            Self::UInt32 => Arc::new(primitive::<UInt32Type>(rows, nulls, |r| narrow(r, 0, u32::MAX.into()) as u32)),
            Self::UInt64 => Arc::new(primitive::<UInt64Type>(rows, nulls, |r| narrow(r, 0, i64::MAX) as u64)),
            Self::Float32 | Self::Float64 | Self::Decimal128 { .. } => {
                let mut row = 0;
                self.drawn_array(rows, nulls, || {
                    row += 1;
                    value(row - 1) as f64
                })
            }
        }
    }
}
