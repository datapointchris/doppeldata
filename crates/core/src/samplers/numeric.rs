//! Numeric columns: a counting sequence, or one uniform draw per row through a piecewise-linear
//! inverse CDF whose knots come from the profile.
//!
//! The knots are the stored quantiles, with the extremes at levels 0 and 1. A histogram's edges at
//! their cumulative shares of every row, its overflow included, fill the body between the
//! outermost quantiles, and `zero_rate` adds a flat run at zero, clamped between the knots beside
//! it. Beyond the outermost quantile, a fitted family's conditional tail, truncated at the
//! extremes, is tabulated into dense knots; without a usable fit the tail is linear.

use std::collections::BTreeMap;
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

use super::BlockPosition;
use super::StreamKeys;
use super::distinct_values::DistinctTarget;
use super::distinct_values::Stratum;
use super::distinct_values::ValueIndex;
use super::distinct_values::Variety;
use super::fitted_family::FittedFamily;
use super::inverse_cdf::InverseCdf;
use super::inverse_cdf::Knots;
use super::listed_values::ListedValues;
use super::numeric_top_values::ListedNumbers;
use super::numeric_top_values::SourceCounts;
use crate::profile::Histogram;
use crate::profile::NumericStats;
use crate::profile::Quantiles;
use crate::rng::rng_for;
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

/// What a numeric column's inverse CDF was built from, as a plan describes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CurveShape {
    pub(crate) body: CurveBody,
    /// The family each tail was drawn from, below the body and above it, or `None` for a straight one.
    pub(crate) lower: Option<&'static str>,
    pub(crate) upper: Option<&'static str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CurveBody {
    Quantiles,
    Histogram,
    /// The fit's median alone, between its tails.
    FitMedian,
    Extremes,
    DefaultRange,
}

impl CurveShape {
    pub(crate) fn describe(self) -> String {
        let tails = match (self.lower, self.upper) {
            (Some(family), Some(_)) => format!("fitted {family} tails"),
            (None, None) => "linear tails".to_string(),
            (lower, upper) => format!("a {} lower tail and a {} upper tail", tail_kind(lower), tail_kind(upper)),
        };
        match self.body {
            CurveBody::Quantiles => format!("quantiles with {tails}"),
            CurveBody::Histogram => format!("histogram with {tails}"),
            CurveBody::FitMedian => match (self.lower, self.upper) {
                (Some(family), Some(_)) => format!("fitted {family}"),
                _ => format!("the fit's median with {tails}"),
            },
            CurveBody::Extremes => "uniform between the extremes".to_string(),
            CurveBody::DefaultRange => format!("uniform from {} to {}", DEFAULT_RANGE.0, DEFAULT_RANGE.1),
        }
    }
}

fn tail_kind(family: Option<&str>) -> String {
    family.map_or_else(|| "linear".to_string(), |family| format!("fitted {family}"))
}

/// The inverse CDF a numeric column draws through, and what it was built from.
pub(crate) fn numeric_cdf(stats: &NumericStats, fit: Option<&FittedFamily>) -> Result<(InverseCdf, CurveShape), String> {
    let extremes = stats.extremes.as_ref();
    let low = extremes.and_then(|e| e.low);
    let high = extremes.and_then(|e| e.high);
    let quantiles = quantile_knots(stats.quantiles.as_ref())?;
    let histogram = histogram_knots(stats.histogram.as_ref());
    let mut shape = CurveShape {
        body: if quantiles.is_empty() {
            CurveBody::Histogram
        } else {
            CurveBody::Quantiles
        },
        lower: None,
        upper: None,
    };
    let (frame, interior) = if quantiles.is_empty() {
        (histogram, Vec::new())
    } else {
        (quantiles, histogram)
    };
    let mut knots = Knots::from_sorted(frame).ok_or("the quantiles or histogram edges fall")?;
    if knots.is_empty() {
        knots = match fit {
            Some(fit) => {
                shape.body = CurveBody::FitMedian;
                let median = fit.inverse(0.5);
                let median = median.max(low.unwrap_or(median)).min(high.unwrap_or(median));
                Knots::from_sorted(vec![(0.5, median)]).expect("one knot is in order")
            }
            None => {
                shape.body = match low.is_some() || high.is_some() {
                    true => CurveBody::Extremes,
                    false => CurveBody::DefaultRange,
                };
                let low = low.unwrap_or(DEFAULT_RANGE.0);
                let high = high.unwrap_or(DEFAULT_RANGE.1).max(low);
                Knots::from_sorted(vec![(0.0, low), (1.0, high)]).expect("low is at most high")
            }
        };
    }
    let (body_low, body_high) = (knots.first(), knots.last());
    // The extreme sits beyond the body's outermost knot, so it is always in order. A tail knot
    // the fit cannot place, such as a NaN, is left out, and the curve runs straight across it.
    if let Some((level, value)) = body_low.filter(|&(level, _)| level > 0.0) {
        let low = low.filter(|&low| low <= value);
        // Where the body starts at or above zero, the stated negative rate is the share a fitted tail
        // below zero may hold, and the levels from it up to the body draw between zero and the body.
        let below_zero = stats
            .negative_rate
            .map(|r| r.0)
            .filter(|&share| 0.0 < share && share < level && value >= 0.0 && low.is_some_and(|low| low < 0.0));
        let (top_level, top_value) = below_zero.map_or((level, value), |share| (share, 0.0));
        // A family whose support stops short of the extreme cannot draw the values between them.
        let reaches = fit.filter(|fit| low.is_none_or(|low| fit.support().0 <= low));
        _ = knots.insert(top_level, top_value);
        match reaches.and_then(|fit| Some((fit.name, lower_tail(fit, low, top_level, top_value)?))) {
            Some((family, tail)) => {
                shape.lower = Some(family);
                _ = knots.extend(tail);
            }
            None => _ = knots.insert(0.0, low.unwrap_or(top_value)),
        }
    }
    if let Some((level, value)) = body_high.filter(|&(level, _)| level < 1.0) {
        let high = high.filter(|&high| high >= value);
        let reaches = fit.filter(|fit| high.is_none_or(|high| fit.support().1 >= high));
        match reaches.and_then(|fit| Some((fit.name, upper_tail(fit, level, value, high)?))) {
            Some((family, tail)) => {
                shape.upper = Some(family);
                _ = knots.extend(tail);
            }
            None => _ = knots.insert(1.0, high.unwrap_or(value)),
        }
    }
    if let (Some((first, _)), Some((last, _))) = (body_low, body_high) {
        // The quantiles frame the body, so a histogram edge they contradict is left out.
        for (level, value) in interior.into_iter().filter(|&(level, _)| first < level && level < last) {
            _ = knots.insert(level, value);
        }
    }
    if let Some(zero_rate) = stats.zero_rate.map(|r| r.0).filter(|&r| r > 0.0) {
        let below_zero = stats.negative_rate.map(|r| r.0).or_else(|| {
            let lowest = knots.first().map_or(0.0, |(_, value)| value);
            (lowest >= 0.0).then_some(0.0)
        });
        if let Some(below_zero) = below_zero {
            knots.insert_run(below_zero, (below_zero + zero_rate).min(1.0), 0.0);
        }
    }
    Ok((InverseCdf::new(knots), shape))
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
        knots = Knots::from_sorted(vec![(0.0, low), (1.0, high)]).expect("low is at most high");
    }
    // Each extreme is kept only where it lies beyond the outermost quantile, so both are in order.
    let (first, last) = (knots.first(), knots.last());
    if let Some((_, value)) = first {
        _ = knots.insert(0.0, low.filter(|&low| low <= value).unwrap_or(value));
    }
    if let Some((_, value)) = last {
        _ = knots.insert(1.0, high.filter(|&high| high >= value).unwrap_or(value));
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
            // The decimal type quantizes to its scale anyway, so rounding there first changes no value.
            (&NumericOutput::Decimal128 { scale, .. }, _) => Self::Decimals(power_of_ten(i32::from(scale))),
            (NumericOutput::Float32 | NumericOutput::Float64, Some(scale)) => Self::Decimals(power_of_ten(scale)),
            _ => Self::Exact,
        }
    }

    pub(crate) fn apply(self, value: f64) -> f64 {
        match self {
            Self::Exact => value,
            Self::Integer => value.round(),
            Self::Decimals(factor) => (value * factor).round() / factor,
        }
    }

    /// The values a rounded value can take, as how many of them fit in one unit, or `None` for any.
    pub(crate) fn lattice(self) -> Option<f64> {
        match self {
            Self::Exact => None,
            Self::Integer => Some(1.0),
            Self::Decimals(factor) => Some(factor),
        }
    }
}

pub(crate) fn power_of_ten(exponent: i32) -> f64 {
    let power = (0..exponent.unsigned_abs()).fold(1.0, |power, _| power * 10.0);
    if exponent < 0 { 1.0 / power } else { power }
}

/// How many distinct values draws through `cdf` take once rounded. A linear piece's interior
/// values are equally likely, so each piece is one stratum, and each end value and flat run is a
/// stratum of one value, its shares merged wherever pieces meet.
pub(crate) fn variety(cdf: &InverseCdf, rounding: Rounding) -> Variety {
    let mut points: BTreeMap<u64, f64> = BTreeMap::new();
    let mut point = |value: f64, share: f64| *points.entry((rounding.apply(value) + 0.0).to_bits()).or_default() += share;
    let mut strata = Vec::new();
    for ((start, low), (end, high)) in cdf.segments() {
        let width = end - start;
        let Some(factor) = rounding.lattice().filter(|_| high > low) else {
            match high > low {
                true => strata.push(Stratum {
                    values: f64::INFINITY,
                    share: width,
                }),
                false => point(low, width),
            }
            continue;
        };
        let (from, to) = (low * factor, high * factor);
        let (first, last) = (from.round(), to.round());
        if first == last {
            point(low, width);
            continue;
        }
        // Levels per lattice step. The end values hold only the part of their step the piece reaches.
        let density = width / (to - from);
        point(low, (first + 0.5 - from) * density);
        point(high, (to - (last - 0.5)) * density);
        let interior = last - first - 1.0;
        if interior > 0.0 {
            strata.push(Stratum {
                values: interior,
                share: interior * density,
            });
        }
    }
    strata.extend(points.into_values().map(|share| Stratum { values: 1.0, share }));
    Variety::Strata(strata)
}

#[derive(Debug, Clone)]
pub(crate) struct NumericSampler {
    /// What a value the column does not list is drawn through: the column's inverse CDF, or where it
    /// lists values, the inverse CDF of the share outside them.
    pub(crate) cdf: InverseCdf,
    /// What the column's inverse CDF was built from, which the outside share's is cut from.
    pub(crate) curve: CurveShape,
    pub(crate) rounding: Rounding,
    pub(crate) output: NumericOutput,
    /// The profile's listed values, then those drawn beyond the trimmed extremes.
    pub(crate) listed: Option<ListedValues<f64>>,
    pub(crate) beyond_extremes: usize,
    /// Where set, a value drawn through `cdf` is the one at an index the row draws.
    pub(crate) index: Option<ValueIndex>,
}

impl NumericSampler {
    /// The sampler drawing through `cdf`, the column's inverse CDF, built as `curve` says. It fails
    /// only on unusable rates.
    pub(crate) fn new(
        (cdf, curve): (InverseCdf, CurveShape),
        stats: &NumericStats,
        output: NumericOutput,
        target: Option<DistinctTarget>,
        source: Option<SourceCounts>,
    ) -> Result<Self, String> {
        let rounding = Rounding::for_column(Some(stats), &output);
        let Some(numbers) = ListedNumbers::of(&cdf, stats, rounding, source)? else {
            let index = target.and_then(|target| ValueIndex::for_distinct_in_bands(target, &variety(&cdf, rounding)));
            return Ok(Self {
                cdf,
                curve,
                rounding,
                output,
                listed: None,
                beyond_extremes: 0,
                index,
            });
        };
        let cdf = numbers.outside.unwrap_or(cdf);
        let listed = numbers.listed;
        let index = target.and_then(|target| ValueIndex::for_distinct_in_bands(listed.outside(target), &variety(&cdf, rounding)));
        Ok(Self {
            cdf,
            curve,
            rounding,
            output,
            listed: Some(listed),
            beyond_extremes: numbers.beyond_extremes,
            index,
        })
    }

    pub(crate) fn generate(&self, at: &BlockPosition, keys: &StreamKeys, rows: usize, nulls: Option<NullBuffer>) -> ArrayRef {
        let mut values = rng_for(at.seed, keys.values, at.block);
        let mut indices = rng_for(at.seed, keys.index, at.block);
        let mut unlisted = |values: &mut ChaCha8Rng| {
            let level = match self.index {
                Some(index) => index.stratified_level(at.seed, keys, &mut indices),
                None => unit(values),
            };
            self.rounding.apply(self.cdf.value_at(level))
        };
        match &self.listed {
            None => self.output.drawn_array(rows, nulls, || unlisted(&mut values)),
            Some(listed) => self.output.drawn_array(rows, nulls, || match listed.sample(&mut values) {
                Some(&value) => value,
                None => unlisted(&mut values),
            }),
        }
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
