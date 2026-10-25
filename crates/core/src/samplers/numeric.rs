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
use rand_distr::Distribution;
use rand_distr::weighted::WeightedAliasIndex;

use super::BlockPosition;
use super::StreamKeys;
use super::distinct_values::Stratum;
use super::distinct_values::ValueIndex;
use super::distinct_values::Variety;
use super::fitted_family::FittedFamily;
use super::inverse_cdf::InverseCdf;
use super::inverse_cdf::Knots;
use crate::profile::ExtremesMethod;
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
        match reaches.and_then(|fit| lower_tail(fit, low, top_level, top_value)) {
            Some(tail) => _ = knots.extend(tail),
            None => _ = knots.insert(0.0, low.unwrap_or(top_value)),
        }
    }
    if let Some((level, value)) = body_high.filter(|&(level, _)| level < 1.0) {
        let high = high.filter(|&high| high >= value);
        let reaches = fit.filter(|fit| high.is_none_or(|high| fit.support().1 >= high));
        match reaches.and_then(|fit| upper_tail(fit, level, value, high)) {
            Some(tail) => _ = knots.extend(tail),
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
    Ok(InverseCdf::new(knots))
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

    fn apply(self, value: f64) -> f64 {
        match self {
            Self::Exact => value,
            Self::Integer => value.round(),
            Self::Decimals(factor) => (value * factor).round() / factor,
        }
    }

    /// The values a rounded value can take, as how many of them fit in one unit, or `None` for any.
    fn lattice(self) -> Option<f64> {
        match self {
            Self::Exact => None,
            Self::Integer => Some(1.0),
            Self::Decimals(factor) => Some(factor),
        }
    }
}

fn power_of_ten(exponent: i32) -> f64 {
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
                    strings: f64::INFINITY,
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
                strings: interior,
                share: interior * density,
            });
        }
    }
    strata.extend(points.into_values().map(|share| Stratum { strings: 1.0, share }));
    Variety::Strata(strata)
}

/// The inverse CDF of the values `cdf` draws outside the `listed` (value, rate) pairs. Every level
/// whose value rounds to a listed one is cut out. Where a histogram counts the column, each bin's
/// remaining levels are scaled to the rows its count leaves outside the listed values, so a ramp
/// the quantiles draw between two listed values keeps no levels the counts do not give it. `None`
/// where no level is left.
fn outside_cdf(cdf: &InverseCdf, listed: &[(f64, f64)], rounding: Rounding, histogram: Option<&Histogram>) -> Option<InverseCdf> {
    let is_listed = |value: f64| {
        let rounded = rounding.apply(value);
        listed.iter().any(|&(listed, _)| listed == rounded)
    };
    let histogram = histogram.filter(|h| h.edges.len() == h.counts.len() + 1 && h.edges.len() > 1);
    let mut cuts: Vec<f64> = rounding
        .lattice()
        .map(|factor| {
            let half = 0.5 / factor;
            listed.iter().flat_map(|&(value, _)| [value - half, value + half]).collect()
        })
        .unwrap_or_default();
    cuts.extend(histogram.into_iter().flat_map(|h| h.edges.iter().copied()));
    cuts.sort_by(f64::total_cmp);
    cuts.dedup();
    // Each kept piece as (level width, value at its start, value at its end).
    let mut pieces: Vec<(f64, f64, f64)> = Vec::new();
    for ((start, low), (end, high)) in cdf.segments() {
        if high == low {
            if !is_listed(low) {
                pieces.push((end - start, low, high));
            }
            continue;
        }
        let mut at = (start, low);
        for cut in cuts
            .iter()
            .copied()
            .filter(|&cut| low < cut && cut < high)
            .chain(std::iter::once(high))
        {
            let level = if cut == high {
                end
            } else {
                start + (cut - low) / (high - low) * (end - start)
            };
            if !is_listed((at.1 + cut) / 2.0) {
                pieces.push((level - at.0, at.1, cut));
            }
            at = (level, cut);
        }
    }
    let weights = histogram.and_then(|h| bin_weights(&pieces, listed, h));
    let widths: Vec<f64> = match &weights {
        Some(weights) => pieces.iter().zip(weights).map(|(&(width, ..), weight)| width * weight).collect(),
        None => pieces.iter().map(|&(width, ..)| width).collect(),
    };
    let total: f64 = widths.iter().sum();
    if total.partial_cmp(&0.0) != Some(std::cmp::Ordering::Greater) {
        return None;
    }
    let mut knots = Vec::with_capacity(2 * pieces.len());
    let mut level = 0.0;
    for (&(_, low, high), width) in pieces.iter().zip(widths) {
        knots.push((level, low));
        level = (level + width / total).min(1.0);
        knots.push((level, high));
    }
    if let Some(last) = knots.last_mut() {
        last.0 = 1.0;
    }
    Knots::from_sorted(knots).map(InverseCdf::new)
}

/// Each piece's scale: the rows its bin's count leaves outside the listed values, over the levels
/// the bin's pieces hold. The rows below the first edge and above the last are bins of their own.
/// `None` where no bin holding a piece leaves any row, as when rounded rates swamp a tiny remainder.
fn bin_weights(pieces: &[(f64, f64, f64)], listed: &[(f64, f64)], histogram: &Histogram) -> Option<Vec<f64>> {
    let edges = &histogram.edges;
    let (below, above) = histogram.overflow.as_ref().map_or((0, 0), |o| (o.below, o.above));
    let counted: Vec<u64> = std::iter::once(below)
        .chain(histogram.counts.iter().copied())
        .chain([above])
        .collect();
    let total = counted.iter().sum::<u64>() as f64;
    // Bin 0 is below the first edge and the last is above the last edge. The last edge closes its bin.
    let bin = |value: f64| match edges.partition_point(|&edge| edge <= value) {
        at if at == edges.len() && value == edges[edges.len() - 1] => at - 1,
        at => at,
    };
    let mut leftover: Vec<f64> = counted.iter().map(|&count| count as f64).collect();
    for &(value, rate) in listed {
        leftover[bin(value)] -= rate * total;
    }
    let mut levels = vec![0.0; counted.len()];
    let homes: Vec<usize> = pieces.iter().map(|&(_, low, high)| bin((low + high) / 2.0)).collect();
    for (&(width, ..), &home) in pieces.iter().zip(&homes) {
        levels[home] += width;
    }
    let scale: Vec<f64> = leftover
        .iter()
        .zip(&levels)
        .map(|(&rows, &level)| if level > 0.0 { rows.max(0.0) / level } else { 0.0 })
        .collect();
    homes
        .iter()
        .any(|&home| scale[home] > 0.0)
        .then(|| homes.iter().map(|&home| scale[home]).collect())
}

/// What a profile states about the source a lattice column's hidden values are deduced from.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SourceCounts {
    /// Its distinct values, `distinct.approx`.
    pub(crate) distinct: u64,
    /// Its values not null.
    pub(crate) values: f64,
    pub(crate) k_min: u64,
}

/// The values a lattice column's trimmed extremes must hide, as (value, rate) pairs. Where the
/// source holds more distinct values than its listed values and the lattice between its extremes
/// can, the rest lie beyond the extremes. Each side's values share `(k_min - 1) / 2` rows, half
/// of what the floor allows beyond an extreme. They go above where the histogram's overflow counts
/// rows only above, below where it counts them only below, and otherwise above and below in turn.
fn hidden_beyond(stats: &NumericStats, rounding: Rounding, listed: &[(f64, f64)], source: SourceCounts) -> Vec<(f64, f64)> {
    let extremes = stats.extremes.as_ref().filter(|e| e.method == Some(ExtremesMethod::TrimmedK));
    let (Some(factor), Some(low), Some(high)) = (rounding.lattice(), extremes.and_then(|e| e.low), extremes.and_then(|e| e.high)) else {
        return Vec::new();
    };
    let (first, last) = ((low * factor).round(), (high * factor).round());
    let listed_inside = listed.iter().filter(|&&(value, _)| low <= value && value <= high).count() as f64;
    let room = (last - first + 1.0 - listed_inside).max(0.0);
    let beyond_room = source.distinct as f64 - listed.len() as f64 - room;
    let floor_allows = 2 * source.k_min.saturating_sub(1);
    let hidden = (beyond_room.max(0.0) as u64).min(floor_allows);
    if hidden == 0 || source.values.partial_cmp(&0.0) != Some(std::cmp::Ordering::Greater) {
        return Vec::new();
    }
    let (rows_below, rows_above) = stats
        .histogram
        .as_ref()
        .and_then(|h| h.overflow.as_ref())
        .map_or((0, 0), |o| (o.below, o.above));
    let above = match (rows_below > 0, rows_above > 0) {
        (false, true) => hidden,
        (true, false) => 0,
        _ => hidden.div_ceil(2),
    };
    let side_rows = (source.k_min.saturating_sub(1)) as f64 / 2.0;
    let rate = |count: u64| side_rows / count as f64 / source.values;
    let steps = |count: u64| 1..=count;
    let mut values: Vec<(f64, f64)> = steps(above).map(|step| ((last + step as f64) / factor, rate(above))).collect();
    values.extend(steps(hidden - above).map(|step| ((first - step as f64) / factor, rate(hidden - above))));
    values
}

/// Values a column lists by rate, and the share drawn outside them.
#[derive(Debug, Clone)]
pub(crate) struct ListedValues {
    pub(crate) values: Vec<f64>,
    /// One entry per value, then one for the outside share where its rate is above zero.
    choice: WeightedAliasIndex<f64>,
}

#[derive(Debug, Clone)]
pub(crate) struct NumericSampler {
    /// What a value the column does not list is drawn through: the column's inverse CDF, or where it
    /// lists values, the inverse CDF of the share outside them.
    pub(crate) cdf: InverseCdf,
    pub(crate) rounding: Rounding,
    pub(crate) output: NumericOutput,
    pub(crate) listed: Option<ListedValues>,
    /// Where set, a value drawn through `cdf` is the one at an index the row draws.
    pub(crate) index: Option<ValueIndex>,
}

/// The distinct values a column is to hold over the dataset, and the rows not null it draws them in.
#[derive(Debug, Clone, Copy)]
pub(crate) struct DistinctTarget {
    pub(crate) distinct: f64,
    pub(crate) filled_rows: f64,
}

impl NumericSampler {
    /// The sampler drawing through `cdf`, the column's inverse CDF. It fails only on unusable rates.
    pub(crate) fn new(
        cdf: InverseCdf,
        stats: &NumericStats,
        output: NumericOutput,
        target: Option<DistinctTarget>,
        source: Option<SourceCounts>,
    ) -> Result<Self, String> {
        let rounding = Rounding::for_column(Some(stats), &output);
        let top_values = stats.top_values.as_deref().filter(|values| !values.is_empty());
        let Some(top_values) = top_values else {
            let index = target.and_then(|t| ValueIndex::for_distinct_in_bands(t.distinct, t.filled_rows, &variety(&cdf, rounding)));
            return Ok(Self {
                cdf,
                rounding,
                output,
                listed: None,
                index,
            });
        };
        let mut listed: Vec<(f64, f64)> = top_values.iter().map(|top| (top.value, top.rate.0)).collect();
        let other_rate = stats.other_rate.map_or(0.0, |r| r.0);
        // Rows hidden beyond the extremes are part of the outside share, never added to it.
        let hidden: Vec<(f64, f64)> = source
            .map(|source| hidden_beyond(stats, rounding, &listed, source))
            .unwrap_or_default()
            .into_iter()
            .scan(other_rate, |left, (value, rate)| {
                let rate = rate.min(*left);
                *left -= rate;
                Some((value, rate))
            })
            .filter(|&(_, rate)| rate > 0.0)
            .collect();
        let outside_rate = (other_rate - hidden.iter().map(|&(_, rate)| rate).sum::<f64>()).max(0.0);
        let outside = outside_cdf(&cdf, &listed, rounding, stats.histogram.as_ref()).unwrap_or(cdf);
        let listed_count = (listed.len() + hidden.len()) as f64;
        listed.extend(hidden);
        let mut weights: Vec<f64> = listed.iter().map(|&(_, rate)| rate).collect();
        if outside_rate > 0.0 {
            weights.push(outside_rate);
        }
        let outside_share = outside_rate / weights.iter().sum::<f64>();
        let choice = WeightedAliasIndex::new(weights).map_err(|error| format!("top_values rates are unusable: {error}"))?;
        let index = target.and_then(|t| {
            ValueIndex::for_distinct_in_bands(
                t.distinct - listed_count,
                t.filled_rows * outside_share,
                &variety(&outside, rounding),
            )
        });
        Ok(Self {
            cdf: outside,
            rounding,
            output,
            listed: Some(ListedValues {
                values: listed.into_iter().map(|(value, _)| value).collect(),
                choice,
            }),
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
            Some(listed) => self
                .output
                .drawn_array(rows, nulls, || match listed.values.get(listed.choice.sample(&mut values)) {
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
