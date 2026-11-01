//! Values a profile lists by rate, drawn by an alias table, beside an `other` share another source
//! draws.

use rand::Rng;
use rand_distr::Distribution;
use rand_distr::weighted::WeightedAliasIndex;

use super::distinct_values::DistinctTarget;

#[derive(Debug, Clone)]
pub(crate) struct ListedValues<T> {
    values: Vec<T>,
    /// One entry per value, then one for `other` where its rate is above zero.
    choice: WeightedAliasIndex<f64>,
    /// Each value's share of the draws.
    shares: Vec<f64>,
    other_share: f64,
}

impl<T> ListedValues<T> {
    /// The values by their rates, and `other_fraction` beside them. It fails only on unusable rates.
    pub(crate) fn new(listed: Vec<(T, f64)>, other_fraction: f64) -> Result<Self, String> {
        let mut weights: Vec<f64> = listed.iter().map(|&(_, rate)| rate).collect();
        let other_fraction = other_fraction.max(0.0);
        if other_fraction > 0.0 {
            weights.push(other_fraction);
        }
        let total: f64 = weights.iter().sum();
        let shares = listed.iter().map(|&(_, rate)| rate / total).collect();
        let choice = WeightedAliasIndex::new(weights).map_err(|error| format!("top_values rates are unusable: {error}"))?;
        Ok(Self {
            values: listed.into_iter().map(|(value, _)| value).collect(),
            choice,
            shares,
            other_share: other_fraction / total,
        })
    }

    /// Each value beside its share of the draws.
    pub(crate) fn shared(&self) -> impl Iterator<Item = (&T, f64)> {
        self.values.iter().zip(self.shares.iter().copied())
    }

    /// The next row's listed value, or `None` where the row falls in the `other` share.
    pub(crate) fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> Option<&T> {
        self.values.get(self.choice.sample(rng))
    }

    pub(crate) fn len(&self) -> usize {
        self.values.len()
    }

    /// The share of rows drawn outside the list.
    pub(crate) fn other_share(&self) -> f64 {
        self.other_share
    }

    /// What the `other` share is to hold of a column's `target`: every value the list does not,
    /// in the rows it draws.
    pub(crate) fn outside(&self, target: DistinctTarget) -> DistinctTarget {
        DistinctTarget {
            distinct: target.distinct - self.values.len() as f64,
            filled_rows: target.filled_rows * self.other_share,
        }
    }
}
