//! A numeric column's values as one rising function of one uniform level, so a level a copula or a
//! parent column chooses reaches the value at that rank. Each listed value is a flat run of levels
//! as wide as its share, and the levels between the runs draw through the curve of the values
//! outside the list.

use super::StreamKeys;
use super::distinct_values::ValueIndex;
use super::inverse_cdf::InverseCdf;
use super::numeric::NumericSampler;
use super::numeric::Rounding;

#[derive(Debug, Clone, Copy)]
struct Run {
    value: f64,
    start: f64,
    share: f64,
    /// The shares of this run and every run below it.
    through: f64,
}

#[derive(Debug, Clone)]
pub(crate) struct RankedMarginal {
    /// The listed values by rising value.
    runs: Vec<Run>,
    /// The curve of the values outside the list, and their share of the levels.
    outside: Option<InverseCdf>,
    outside_share: f64,
    index: Option<ValueIndex>,
    rounding: Rounding,
    /// The levels whose values round to zero.
    zero_band: (f64, f64),
}

impl RankedMarginal {
    /// The marginal `sampler` draws, as the same values at the same shares.
    pub(crate) fn of(sampler: &NumericSampler) -> Self {
        let Some(listed) = &sampler.listed else {
            return Self::of_curve(sampler.cdf.clone(), sampler.rounding, sampler.index);
        };
        let outside_share = listed.other_share();
        let outside = (outside_share > 0.0).then(|| sampler.cdf.clone());
        let mut values: Vec<(f64, f64)> = listed.shared().map(|(&value, share)| (value, share)).collect();
        values.sort_by(|a, b| a.0.total_cmp(&b.0));
        let mut through = 0.0;
        let runs = values
            .into_iter()
            .map(|(value, share)| {
                let below_outside = outside.as_ref().map_or(0.0, |cdf| outside_share * cdf.level_of(value, false));
                let run = Run {
                    value,
                    start: through + below_outside,
                    share,
                    through: through + share,
                };
                through += share;
                run
            })
            .collect();
        Self::built(runs, outside, outside_share, sampler.index, sampler.rounding)
    }

    /// The marginal drawn through `cdf` alone, at `index` where the column holds a distinct ratio.
    pub(crate) fn of_curve(cdf: InverseCdf, rounding: Rounding, index: Option<ValueIndex>) -> Self {
        Self::built(Vec::new(), Some(cdf), 1.0, index, rounding)
    }

    fn built(runs: Vec<Run>, outside: Option<InverseCdf>, outside_share: f64, index: Option<ValueIndex>, rounding: Rounding) -> Self {
        let mut marginal = Self {
            runs,
            outside,
            outside_share,
            index,
            rounding,
            zero_band: (0.0, 0.0),
        };
        marginal.zero_band = marginal.band_of(0.0);
        marginal
    }

    /// The levels whose values round to `value`, from the first to the last.
    fn band_of(&self, value: f64) -> (f64, f64) {
        match self.rounding.lattice() {
            Some(factor) => (self.rank_of(value - 0.5 / factor, true), self.rank_of(value + 0.5 / factor, false)),
            None => (self.rank_of(value, false), self.rank_of(value, true)),
        }
    }

    /// The warp drawing a group's values through this marginal, from the group's `quantiles` as rising
    /// `(level, value)` pairs and the share `zero` of its values that are zero, where stated.
    ///
    /// Each stated quantile maps into the levels drawing its value, so the group holds it at its level. A
    /// run of quantiles at one value spreads across that value's levels. Between them the group takes the
    /// marginal's values at the marginal's spacing, its listed values and its curve's shape included. A
    /// stated zero share replaces the quantiles at zero, and starts after the last quantile below zero.
    pub(crate) fn warp(&self, quantiles: &[(f64, f64)], zero: Option<f64>) -> LevelWarp {
        let (zero_low, zero_high) = self.zero_band;
        let zero = zero.filter(|_| zero_high > zero_low);
        let mut stated = Vec::with_capacity(quantiles.len());
        let mut start = 0;
        while start < quantiles.len() {
            let value = quantiles[start].1;
            let end = (start..quantiles.len())
                .find(|&at| quantiles[at].1 != value)
                .unwrap_or(quantiles.len());
            let (low, high) = self.band_of(value);
            if zero.is_none() || (low, high) != self.zero_band {
                let tied = (end - start) as f64;
                for (offset, &(level, _)) in quantiles[start..end].iter().enumerate() {
                    stated.push((level, low + (high - low) * (offset as f64 + 0.5) / tied));
                }
            }
            start = end;
        }
        let mut knots = vec![(0.0, 0.0)];
        let zero_band = match zero {
            Some(share) => {
                let below = stated.partition_point(|&(_, to)| to <= zero_low);
                let first_zero = if zero_low > 0.0 && below > 0 { stated[below - 1].0 } else { 0.0 };
                let first_above = stated.get(below).map_or(1.0, |&(level, _)| level);
                let share = share.min(first_above - first_zero).max(0.0);
                knots.extend_from_slice(&stated[..below]);
                knots.extend([(first_zero, zero_low), (first_zero + share, zero_high)]);
                knots.extend_from_slice(&stated[below..]);
                (first_zero, first_zero + share)
            }
            None => {
                knots.extend(stated);
                (0.0, 0.0)
            }
        };
        knots.push((1.0, 1.0));
        let mut rising: Vec<(f64, f64)> = Vec::with_capacity(knots.len());
        for (level, to) in knots {
            let (last_level, last_to) = rising.last().copied().unwrap_or((0.0, 0.0));
            let knot = (level.max(last_level), to.max(last_to));
            if rising.last() != Some(&knot) {
                rising.push(knot);
            }
        }
        let mut warp = LevelWarp { knots: rising, zero_band };
        if zero.is_none() {
            warp.zero_band = (warp.inverse(zero_low), warp.inverse(zero_high));
        }
        warp
    }

    /// The least level whose value reaches `value`, or passes it where `above`.
    fn rank_of(&self, value: f64, above: bool) -> f64 {
        let listed: f64 = self
            .runs
            .iter()
            .filter(|run| run.value < value || (above && run.value == value))
            .map(|run| run.share)
            .sum();
        let outside = self
            .outside
            .as_ref()
            .map_or(0.0, |cdf| self.outside_share * cdf.level_of(value, above));
        (listed + outside).min(1.0)
    }

    /// The levels whose values round to zero, from the first to the last.
    pub(crate) fn zero_band(&self) -> (f64, f64) {
        self.zero_band
    }

    /// The value at `level`, in [0, 1). A level outside every run is placed in the outside curve,
    /// within the band of the column's distinct index where it has one.
    pub(crate) fn value_at(&self, level: f64, seed: u64, keys: &StreamKeys) -> f64 {
        self.value_banded(level, |outside_level| match self.index {
            Some(index) => index.band_level(outside_level, seed, keys),
            None => outside_level,
        })
    }

    /// Each of the rising `levels` scored by the middle of the levels drawing its value, as an average rank
    /// scores a tie, and whether some value spans more than `tie_share` of the levels and two of `levels`. A
    /// value's levels are bounded halfway to the next level drawing another. A distinct index is taken as
    /// drawing every value once.
    pub(crate) fn tie_scores(&self, levels: &[f64], tie_share: f64) -> (Vec<f64>, bool) {
        let values: Vec<f64> = levels.iter().map(|&level| self.value_banded(level, |outside| outside)).collect();
        let mut scores = vec![0.0; levels.len()];
        let mut tied = false;
        let mut start = 0;
        while start < levels.len() {
            let end = (start..levels.len())
                .find(|&at| values[at] != values[start])
                .unwrap_or(levels.len());
            let low = if start == 0 {
                0.0
            } else {
                (levels[start - 1] + levels[start]) / 2.0
            };
            let high = if end == levels.len() {
                1.0
            } else {
                (levels[end - 1] + levels[end]) / 2.0
            };
            tied |= end - start > 1 && high - low > tie_share;
            scores[start..end].fill((low + high) / 2.0);
            start = end;
        }
        (scores, tied)
    }

    /// The middle of the levels drawing the value at `level`, as an average rank scores a tie. A distinct index is
    /// taken as drawing every value once.
    pub(crate) fn score_at(&self, level: f64) -> f64 {
        let (low, high) = self.band_of(self.value_banded(level, |outside| outside));
        (low + high) / 2.0
    }

    fn value_banded(&self, level: f64, band: impl Fn(f64) -> f64) -> f64 {
        let at = self.runs.partition_point(|run| run.start <= level);
        let below = at.checked_sub(1).map(|run| self.runs[run]);
        if let Some(run) = below
            && (level < run.start + run.share || self.outside.is_none())
        {
            return run.value;
        }
        let Some(outside) = &self.outside else {
            return self.runs.first().map_or(0.0, |run| run.value);
        };
        let listed_below = below.map_or(0.0, |run| run.through);
        let outside_level = ((level - listed_below) / self.outside_share).clamp(0.0, 1.0 - f64::EPSILON);
        self.rounding.apply(outside.value_at(band(outside_level)))
    }
}

/// A rising map from a group's levels to the levels of its column's marginal, which `RankedMarginal::warp` builds.
#[derive(Debug, Clone)]
pub(crate) struct LevelWarp {
    /// `(group level, marginal level)`, each rising, from `(0, 0)` to `(1, 1)`. Two knots at one group level
    /// jump between their marginal levels.
    knots: Vec<(f64, f64)>,
    /// The group's levels that draw zero.
    zero_band: (f64, f64),
}

impl LevelWarp {
    /// The group's levels that draw zero, from the first to the last.
    pub(crate) fn zero_band(&self) -> (f64, f64) {
        self.zero_band
    }

    /// The marginal's level for the group's `level`, in [0, 1).
    pub(crate) fn apply(&self, level: f64) -> f64 {
        let after = self.knots.partition_point(|&(from, _)| from <= level).min(self.knots.len() - 1);
        let ((from, to), (next_from, next_to)) = (self.knots[after - 1], self.knots[after]);
        let along = if next_from > from {
            (level - from) / (next_from - from)
        } else {
            0.0
        };
        (to + along.clamp(0.0, 1.0) * (next_to - to)).min(1.0 - f64::EPSILON)
    }

    /// The least group level whose marginal level reaches `marginal`.
    fn inverse(&self, marginal: f64) -> f64 {
        let after = self.knots.partition_point(|&(_, to)| to < marginal);
        if after == 0 {
            return 0.0;
        }
        let Some(&(next_from, next_to)) = self.knots.get(after) else {
            return 1.0;
        };
        let (from, to) = self.knots[after - 1];
        from + (marginal - to) / (next_to - to) * (next_from - from)
    }
}

/// A level of a curve whose own zero band is `band`, moved so that `zero` of the levels draw zero, or `None`
/// where `level` is one of those. The levels below and above the band keep their order and their shares of the
/// values that are not zero.
pub(crate) fn with_zero_share(level: f64, (low, high): (f64, f64), zero: f64) -> Option<f64> {
    let nonzero = 1.0 - (high - low);
    if zero >= 1.0 || nonzero <= 0.0 {
        return None;
    }
    let scale = nonzero / (1.0 - zero);
    let zero_from = low / scale;
    if level < zero_from {
        return Some(level * scale);
    }
    if level < zero_from + zero {
        return None;
    }
    Some((high + (level - zero_from - zero) * scale).min(1.0 - f64::EPSILON))
}
