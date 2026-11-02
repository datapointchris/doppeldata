//! A string column with stored `top_values`: an alias table over their rates, where a draw of the
//! `other_fraction` share generates a value the list does not hold instead. With a suffix, each top
//! value is a prefix, and a draw of the suffix pattern follows it.

use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_buffer::NullBuffer;
use rand_chacha::ChaCha8Rng;
use rand_distr::Distribution;
use rand_distr::weighted::WeightedAliasIndex;

use super::BlockPosition;
use super::StreamKeys;
use super::distinct_values::Stratum;
use super::distinct_values::ValueIndex;
use super::distinct_values::Variety;
use super::listed_values::ListedValues;
use super::text::Pattern;
use super::text::StringOutput;
use super::text::TextSource;
use super::unique_pattern_key::KeyEnd;
use super::unique_pattern_key::SeededPatternKey;
use super::unique_pattern_key::UniquePatternKey;
use crate::rng::below_u64;
use crate::rng::rng_for;
use crate::rng::splitmix;

/// The draws of `other` a rest row takes before it keeps one the list holds, where the rest is not
/// drawn through [`UnlistedStrings`]. Where a share q of `other`'s draws land on a listed value,
/// q^REST_DRAWS of the rest rows keep one.
const REST_DRAWS: usize = 64;

#[derive(Debug, Clone)]
pub(crate) struct CategoricalSampler {
    pub(crate) listed: ListedValues<String>,
    pub(crate) suffix: Option<Box<Pattern>>,
    pub(crate) other: TextSource,
    /// Where set, an `other` row's value is the one at an index the row draws.
    pub(crate) other_index: Option<ValueIndex>,
    /// Where set, an `other` row's value is drawn through it, and `other` is not drawn.
    pub(crate) unlisted: Option<UnlistedStrings>,
    /// The listed values, which a draw of `other` is drawn again on holding in its first `held_chars`
    /// characters, every character for a list of whole values.
    pub(crate) held: HashSet<String>,
    pub(crate) held_chars: usize,
    pub(crate) output: StringOutput,
}

impl CategoricalSampler {
    pub(crate) fn generate(&self, at: &BlockPosition, keys: &StreamKeys, rows: usize, nulls: Option<NullBuffer>) -> ArrayRef {
        let mut values = rng_for(at.seed, keys.values, at.block);
        let mut indices = rng_for(at.seed, keys.index, at.block);
        let seeded = self.seeded(at, keys);
        self.output
            .build(rows, nulls.as_ref(), |out| match self.listed.sample(&mut values) {
                Some(value) => {
                    out.push_str(value);
                    if let Some(suffix) = &self.suffix {
                        suffix.push(&mut values, out);
                    }
                }
                None => self.push_other(at, keys, &seeded, &mut values, &mut indices, out),
            })
    }

    /// Each row drawn as a listed draw's miss draws it, from `other`.
    pub(crate) fn generate_rest(&self, at: &BlockPosition, keys: &StreamKeys, rows: usize, nulls: Option<NullBuffer>) -> ArrayRef {
        let mut values = rng_for(at.seed, keys.values, at.block);
        let mut indices = rng_for(at.seed, keys.index, at.block);
        let seeded = self.seeded(at, keys);
        self.output.build(rows, nulls.as_ref(), |out| {
            self.push_other(at, keys, &seeded, &mut values, &mut indices, out)
        })
    }

    fn seeded(&self, at: &BlockPosition, keys: &StreamKeys) -> Vec<SeededOrder<'_>> {
        self.unlisted
            .as_ref()
            .map_or_else(Vec::new, |unlisted| unlisted.seeded(at.seed, keys))
    }

    fn push_other(
        &self,
        at: &BlockPosition,
        keys: &StreamKeys,
        seeded: &[SeededOrder<'_>],
        values: &mut ChaCha8Rng,
        indices: &mut ChaCha8Rng,
        out: &mut String,
    ) {
        match (&self.unlisted, self.other_index) {
            (Some(unlisted), Some(index)) => {
                let (slot, mut rng) = index.drawn(at.seed, keys, indices);
                unlisted.push(seeded, Some(slot), &mut rng, out);
            }
            (Some(unlisted), None) => unlisted.push(seeded, None, values, out),
            (None, Some(index)) => self.push_redrawn(&mut index.value_stream(at.seed, keys, indices), out),
            (None, None) => self.push_redrawn(values, out),
        }
    }

    /// A draw of `other` the list does not hold, drawn again from `rng` up to `REST_DRAWS` times, the last draw
    /// standing.
    fn push_redrawn(&self, rng: &mut ChaCha8Rng, out: &mut String) {
        let start = out.len();
        for draw in 1..=REST_DRAWS {
            self.other.push(rng, out);
            let drawn = &out[start..];
            let leading = drawn.char_indices().nth(self.held_chars).map_or(drawn, |(end, _)| &drawn[..end]);
            if draw == REST_DRAWS || !self.held.contains(leading) {
                return;
            }
            out.truncate(start);
        }
    }
}

/// A rest whose source is a pattern or shapes, drawn without replacement: each language's strings in a seeded order,
/// the listed ones left out. So two slots of the rest's value index hold two strings until a language runs out.
#[derive(Debug, Clone)]
pub(crate) struct UnlistedStrings {
    /// Which language a slot draws from, by each one's share of the rest, where there is more than one.
    choice: Option<WeightedAliasIndex<f64>>,
    orders: Vec<UnlistedOrder>,
}

#[derive(Debug, Clone)]
struct UnlistedOrder {
    key: Arc<UniquePatternKey>,
    /// The ranks of the listed strings the language holds.
    listed: Vec<u128>,
}

/// One block's order of a language's strings, the rows of its listed strings sorted, and the rows left.
struct SeededOrder<'a> {
    key: SeededPatternKey<'a>,
    listed: Vec<u64>,
    unlisted: u64,
}

impl UnlistedStrings {
    /// The rest `source` draws outside `held`, beside the distinct strings its slots give. None where the source is
    /// not a regular language, an automaton passes the bound, or the list holds every string.
    pub(crate) fn new(source: &TextSource, held: &HashSet<String>) -> Option<(Self, Variety)> {
        let mut orders = Vec::new();
        let mut strata = Vec::new();
        for (language, share) in source.languages()? {
            let key = UniquePatternKey::new(&language, KeyEnd::Whole).ok()?;
            let listed: Vec<u128> = held.iter().filter_map(|value| key.rank_of(value.as_bytes())).collect();
            let unlisted = key.strings().min(u128::from(u64::MAX)) - listed.len() as u128;
            if unlisted > 0 && share > 0.0 {
                strata.push(Stratum {
                    values: unlisted as f64,
                    share,
                });
                orders.push(UnlistedOrder {
                    key: Arc::new(key),
                    listed,
                });
            }
        }
        let total: f64 = strata.iter().map(|stratum| stratum.share).sum();
        if orders.is_empty() || total <= 0.0 {
            return None;
        }
        for stratum in &mut strata {
            stratum.share /= total;
        }
        let choice = match orders.len() {
            1 => None,
            _ => Some(WeightedAliasIndex::new(strata.iter().map(|stratum| stratum.share).collect()).ok()?),
        };
        Some((Self { choice, orders }, Variety::Unreplaced(strata)))
    }

    fn seeded(&self, seed: u64, keys: &StreamKeys) -> Vec<SeededOrder<'_>> {
        self.orders
            .iter()
            .enumerate()
            .map(|(at, order)| {
                let key = order.key.seeded(seed, splitmix(keys.rest_order ^ at as u64));
                let mut listed: Vec<u64> = order.listed.iter().filter_map(|&rank| key.row_of(rank)).collect();
                listed.sort_unstable();
                let unlisted = key.rows() - listed.len() as u64;
                SeededOrder { key, listed, unlisted }
            })
            .collect()
    }

    /// The string `slot` takes, its language picked from `rng`: the slot's place among the language's strings the
    /// list does not hold, counted again from the first past the last. With no slot, a place drawn from `rng`.
    fn push(&self, seeded: &[SeededOrder<'_>], slot: Option<u64>, rng: &mut ChaCha8Rng, out: &mut String) {
        let order = &seeded[self.choice.as_ref().map_or(0, |choice| choice.sample(rng))];
        let mut row = slot.map_or_else(|| below_u64(rng, order.unlisted), |slot| slot % order.unlisted);
        for &listed in &order.listed {
            if listed > row {
                break;
            }
            row += 1;
        }
        order.key.push(row, out);
    }
}
