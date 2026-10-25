//! A numeric column's `top_values`: drawn by rate, beside an `other` share drawn through the inverse
//! CDF with every listed value cut out, and the values a lattice column's distinct count puts beyond
//! its trimmed extremes.

use std::cmp::Ordering;

use super::inverse_cdf::InverseCdf;
use super::inverse_cdf::Knots;
use super::listed_values::ListedValues;
use super::numeric::Rounding;
use crate::profile::Histogram;
use crate::profile::NumericStats;

/// What a profile states about the source a lattice column's hidden values are deduced from.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SourceCounts {
    /// Its distinct values, `distinct.approx`.
    pub(crate) distinct: u64,
    /// Its values not null.
    pub(crate) values: f64,
    pub(crate) k_min: u64,
}

/// What a numeric column's `top_values` become.
#[derive(Debug, Clone)]
pub(crate) struct ListedNumbers {
    /// The profile's values, then those drawn beyond the trimmed extremes.
    pub(crate) listed: ListedValues<f64>,
    pub(crate) beyond_extremes: usize,
    /// The inverse CDF the `other` share draws through, where any level lies outside the list.
    pub(crate) outside: Option<InverseCdf>,
}

impl ListedNumbers {
    /// `None` where the column lists no values. It fails only on unusable rates.
    pub(crate) fn of(
        cdf: &InverseCdf,
        stats: &NumericStats,
        rounding: Rounding,
        source: Option<SourceCounts>,
    ) -> Result<Option<Self>, String> {
        let Some(top_values) = stats.top_values.as_deref().filter(|values| !values.is_empty()) else {
            return Ok(None);
        };
        let mut listed: Vec<(f64, f64)> = top_values.iter().map(|top| (top.value, top.rate.0)).collect();
        let other_rate = stats.other_rate.map_or(0.0, |r| r.0);
        // Rows hidden beyond the extremes are part of the `other` share, never added to it.
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
        let outside = outside_cdf(cdf, &listed, rounding, stats.histogram.as_ref());
        // With no level left outside the list, its share is drawn as the listed values are.
        let outside_rate = match outside {
            Some(_) => (other_rate - hidden.iter().map(|&(_, rate)| rate).sum::<f64>()).max(0.0),
            None => 0.0,
        };
        let beyond_extremes = hidden.len();
        listed.extend(hidden);
        Ok(Some(Self {
            listed: ListedValues::new(listed, outside_rate)?,
            beyond_extremes,
            outside,
        }))
    }
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
    if total.partial_cmp(&0.0) != Some(Ordering::Greater) {
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

/// The values a lattice column's extremes must hide, as (value, rate) pairs. Where the source holds
/// more distinct values than its listed values and the lattice between its extremes can, the rest
/// lie beyond the extremes, at most one for each row the extremes' method leaves beyond them. Each
/// side's values share half those rows. They go above where the histogram's overflow counts rows
/// only above, below where it counts them only below, and otherwise above and below in turn.
fn hidden_beyond(stats: &NumericStats, rounding: Rounding, listed: &[(f64, f64)], source: SourceCounts) -> Vec<(f64, f64)> {
    let extremes = stats.extremes.as_ref();
    let rows_beyond_each = extremes.and_then(|e| e.method).map(|method| method.rows_beyond_each(source.k_min));
    let (Some(rows_beyond_each), Some(factor), Some(low), Some(high)) = (
        rows_beyond_each,
        rounding.lattice(),
        extremes.and_then(|e| e.low),
        extremes.and_then(|e| e.high),
    ) else {
        return Vec::new();
    };
    let (first, last) = ((low * factor).round(), (high * factor).round());
    let listed_inside = listed.iter().filter(|&&(value, _)| low <= value && value <= high).count() as f64;
    let room = (last - first + 1.0 - listed_inside).max(0.0);
    let beyond_room = source.distinct as f64 - listed.len() as f64 - room;
    let hidden = (beyond_room.max(0.0) as u64).min(2 * rows_beyond_each);
    if hidden == 0 || source.values.partial_cmp(&0.0) != Some(Ordering::Greater) {
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
    let side_rows = rows_beyond_each as f64 / 2.0;
    let rate = |count: u64| side_rows / count as f64 / source.values;
    let steps = |count: u64| 1..=count;
    let mut values: Vec<(f64, f64)> = steps(above).map(|step| ((last + step as f64) / factor, rate(above))).collect();
    values.extend(steps(hidden - above).map(|step| ((first - step as f64) / factor, rate(hidden - above))));
    values
}
