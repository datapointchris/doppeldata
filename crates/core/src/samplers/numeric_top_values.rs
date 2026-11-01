//! A numeric column's `top_values`: drawn by rate, beside an `other` share drawn through the inverse
//! CDF with every listed value cut out, and the values a lattice column's distinct count puts beyond
//! its trimmed extremes.

use std::cmp::Ordering;

use super::FieldReads;
use super::inverse_cdf::InverseCdf;
use super::inverse_cdf::Knots;
use super::listed_values::ListedValues;
use super::numeric::Rounding;
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
    /// The values beyond the trimmed extremes by their rates, beside the share `outside` draws, so a value the
    /// profile lists none of is drawn from both. `None` where no value lies beyond them.
    pub(crate) unlisted: Option<ListedValues<f64>>,
}

impl ListedNumbers {
    /// `None` where the column lists no values. It fails only on unusable rates.
    pub(crate) fn of(
        cdf: &InverseCdf,
        stats: &NumericStats,
        rounding: Rounding,
        source: Option<SourceCounts>,
        reads: &mut FieldReads,
    ) -> Result<Option<Self>, String> {
        let top_values = reads.read("listed_values", "/numeric/top_values", stats.top_values.as_deref());
        let Some(top_values) = top_values.filter(|values| !values.is_empty()) else {
            return Ok(None);
        };
        let mut listed: Vec<(f64, f64)> = top_values.iter().map(|top| (top.value, top.fraction.0)).collect();
        let other_fraction = reads
            .read("rest_fraction", "/numeric/other_fraction", stats.other_fraction)
            .map_or(0.0, |r| r.0);
        // Rows hidden beyond the extremes are part of the `other` share, never added to it.
        let hidden: Vec<(f64, f64)> = source
            .map(|source| hidden_beyond(stats, rounding, &listed, source))
            .unwrap_or_default()
            .into_iter()
            .scan(other_fraction, |left, (value, rate)| {
                let rate = rate.min(*left);
                *left -= rate;
                Some((value, rate))
            })
            .filter(|&(_, rate)| rate > 0.0)
            .collect();
        let outside = outside_cdf(cdf, &listed, rounding);
        // With no level left outside the list, its share is drawn as the listed values are.
        let outside_rate = match outside {
            Some(_) => (other_fraction - hidden.iter().map(|&(_, rate)| rate).sum::<f64>()).max(0.0),
            None => 0.0,
        };
        let beyond_extremes = hidden.len();
        let unlisted = if hidden.is_empty() {
            None
        } else {
            Some(ListedValues::new(hidden.clone(), outside_rate)?)
        };
        listed.extend(hidden);
        Ok(Some(Self {
            listed: ListedValues::new(listed, outside_rate)?,
            beyond_extremes,
            outside,
            unlisted,
        }))
    }
}

/// The inverse CDF of the values `cdf` draws outside the `listed` (value, rate) pairs. Every level
/// whose value rounds to a listed one is cut out, and the levels left keep their widths. `None`
/// where no level is left.
fn outside_cdf(cdf: &InverseCdf, listed: &[(f64, f64)], rounding: Rounding) -> Option<InverseCdf> {
    let is_listed = |value: f64| {
        let rounded = rounding.apply(value);
        listed.iter().any(|&(listed, _)| listed == rounded)
    };
    let mut cuts: Vec<f64> = rounding
        .lattice()
        .map(|factor| {
            let half = 0.5 / factor;
            listed.iter().flat_map(|&(value, _)| [value - half, value + half]).collect()
        })
        .unwrap_or_default();
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
    let total: f64 = pieces.iter().map(|&(width, ..)| width).sum();
    if total.partial_cmp(&0.0) != Some(Ordering::Greater) {
        return None;
    }
    let mut knots = Vec::with_capacity(2 * pieces.len());
    let mut level = 0.0;
    for &(width, low, high) in &pieces {
        knots.push((level, low));
        level = (level + width / total).min(1.0);
        knots.push((level, high));
    }
    if let Some(last) = knots.last_mut() {
        last.0 = 1.0;
    }
    Knots::from_sorted(knots).map(InverseCdf::new)
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
        .map_or((0, 0), |o| (o.rows_below, o.rows_above));
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
