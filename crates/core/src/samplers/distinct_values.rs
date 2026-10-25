//! A generated value drawn as a pure function of an index, so a column holds its profiled share of
//! distinct values with no pool of them held. Each row draws an index from `0..range`. A string at
//! an index comes from that index's own stream, and a number from a level inside that index's band
//! of levels. Either is the same in every block.

use rand_chacha::ChaCha8Rng;

use super::StreamKeys;
use crate::rng::below_u64;
use crate::rng::rng_for;
use crate::rng::unit;

/// How many distinct strings a source draws.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Variety {
    /// Not counted, so each distinct index is taken to give a distinct string.
    Uncounted,
    /// Groups of strings, each string drawn as often as the others in its group.
    Strata(Vec<Stratum>),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct Stratum {
    pub(crate) strings: f64,
    /// The share of draws landing in the group.
    pub(crate) share: f64,
}

impl Variety {
    /// `strings` strings, each drawn as often as any other.
    pub(crate) fn uniform(strings: f64) -> Self {
        Self::Strata(vec![Stratum { strings, share: 1.0 }])
    }

    /// The distinct strings that `indices` distinct indices are expected to give.
    fn distinct_strings(&self, indices: f64) -> f64 {
        match self {
            Self::Uncounted => indices,
            Self::Strata(strata) => strata
                .iter()
                .map(|stratum| {
                    let draws = stratum.share * indices;
                    if stratum.strings.is_infinite() {
                        draws
                    } else {
                        -stratum.strings * libm::expm1(-draws / stratum.strings)
                    }
                })
                .sum(),
        }
    }

    /// The distinct strings that `draws` draws give when each lands in one of `range` equal bands
    /// of levels, and a band gives one string wherever it is drawn. Each stratum lays its share of
    /// the bands evenly across its strings.
    fn distinct_strings_in_bands(&self, range: f64, draws: f64) -> f64 {
        let drawn = -libm::expm1(-draws / range);
        match self {
            Self::Uncounted => range * drawn,
            Self::Strata(strata) => strata
                .iter()
                .map(|stratum| {
                    let bands = stratum.share * range;
                    if stratum.strings.is_infinite() {
                        bands * drawn
                    } else {
                        stratum.strings * cell_reached(bands / stratum.strings, drawn)
                    }
                })
                .sum(),
        }
    }
}

/// The chance that some drawn band gives a string, where the levels rounding to the string span
/// `width` bands and each band is drawn with chance `drawn`. Nothing aligns the string's levels
/// with the bands, so the chance is averaged over where in a band they start.
fn cell_reached(width: f64, drawn: f64) -> f64 {
    let (q, p) = (drawn, 1.0 - drawn);
    if width <= 1.0 {
        // The levels lie inside one band, or a band edge splits them between two.
        return q * width - q * q * width * width * width / 6.0;
    }
    let whole = width.floor();
    let part = width - whole;
    // Started at u in its first band, the string takes 1 - u of it, whole bands after it, and
    // `last` - q·u of the band it ends in, scaled to the chance that band misses it.
    let missed = |from: f64, to: f64, last: f64| {
        let cube = |u: f64| u * u * u;
        p * last * (to - from) + q * (last - p) * (to * to - from * from) / 2.0 - q * q * (cube(to) - cube(from)) / 3.0
    };
    let missed = libm::pow(p, whole - 1.0) * missed(0.0, 1.0 - part, 1.0 - q * part)
        + libm::pow(p, whole) * missed(1.0 - part, 1.0, 1.0 + q - q * part);
    1.0 - missed
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ValueIndex {
    range: u64,
}

impl ValueIndex {
    /// The range whose `draws` uniform draws are expected to give `distinct` distinct strings from a
    /// source of `variety`. `None` where fresh draws come no nearer than that, as at a ratio of 1.
    pub(crate) fn for_distinct(distinct: f64, draws: f64, variety: &Variety) -> Option<Self> {
        let distinct = distinct.max(1.0);
        if draws.is_nan() || draws < 1.0 || variety.distinct_strings(draws) <= distinct {
            return None;
        }
        let indices = solve_rising(|indices| variety.distinct_strings(indices), distinct, distinct, draws);
        // K - K·e^(-n/K) is at least n - n²/2K, so this range holds `indices` distinct indices or more.
        let widest = draws * draws / (2.0 * (draws - indices));
        let range = solve_rising(|range| distinct_indices(range, draws), indices, indices, widest);
        Some(Self {
            range: (range.round() as u64).max(1),
        })
    }

    /// The range whose `draws` draws at [`Self::stratified_level`] levels are expected to give
    /// `distinct` distinct strings from a source of `variety`. `None` where fresh draws come no
    /// nearer than that.
    pub(crate) fn for_distinct_in_bands(distinct: f64, draws: f64, variety: &Variety) -> Option<Self> {
        let distinct = distinct.max(1.0);
        if draws.is_nan() || draws < 1.0 || variety.distinct_strings(draws) <= distinct {
            return None;
        }
        let reached = |range: f64| variety.distinct_strings_in_bands(range, draws);
        // The count rises toward the fresh draws' as the bands narrow, so some range reaches it.
        let mut widest = distinct;
        while reached(widest) < distinct && widest < f64::MAX / 4.0 {
            widest *= 2.0;
        }
        let range = solve_rising(reached, distinct, distinct, widest);
        Some(Self {
            range: (range.round() as u64).max(1),
        })
    }

    /// The stream of the value at the index the next row draws from `indices`.
    pub(crate) fn value_stream(self, seed: u64, keys: &StreamKeys, indices: &mut ChaCha8Rng) -> ChaCha8Rng {
        rng_for(seed, keys.indexed_values, below_u64(indices, self.range))
    }

    /// The level in [0, 1) of the value at the index the next row draws: a uniform draw inside the
    /// index's own share of the range. A uniform index and a uniform offset make a uniform level, so
    /// the values follow the inverse CDF they are drawn through. And a few indices reach each piece
    /// of it in proportion, where independent levels could leave a short piece with none.
    pub(crate) fn stratified_level(self, seed: u64, keys: &StreamKeys, indices: &mut ChaCha8Rng) -> f64 {
        let index = below_u64(indices, self.range);
        let offset = unit(&mut rng_for(seed, keys.indexed_values, index));
        ((index as f64 + offset) / self.range as f64).min(1.0 - f64::EPSILON)
    }
}

/// The distinct indices that `draws` uniform draws from `0..range` are expected to hold.
fn distinct_indices(range: f64, draws: f64) -> f64 {
    -range * libm::expm1(-draws / range)
}

/// The `x` in `[low, high]` where the rising `f` reaches `target`, halving the bounds until they
/// meet.
fn solve_rising(f: impl Fn(f64) -> f64, target: f64, mut low: f64, mut high: f64) -> f64 {
    for _ in 0..2048 {
        let mid = low + (high - low) / 2.0;
        if mid <= low || mid >= high {
            break;
        }
        if f(mid) < target {
            low = mid;
        } else {
            high = mid;
        }
    }
    high
}
