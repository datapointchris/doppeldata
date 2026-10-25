//! A generated string drawn as a pure function of an index, so a string column holds its profiled
//! share of distinct values with no pool of them held. Each row draws an index from `0..range`,
//! and the value at an index comes from that index's own stream, so it is the same in every block.

use rand_chacha::ChaCha8Rng;

use super::StreamKeys;
use crate::rng::below_u64;
use crate::rng::rng_for;

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

    /// The stream of the value at the index the next row draws from `indices`.
    pub(crate) fn value_stream(self, seed: u64, keys: &StreamKeys, indices: &mut ChaCha8Rng) -> ChaCha8Rng {
        rng_for(seed, keys.indexed_values, below_u64(indices, self.range))
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
