//! Values a profile lists by rate, drawn by an alias table, beside an `other` share another source
//! draws.

use rand::Rng;
use rand_distr::Distribution;
use rand_distr::weighted::WeightedAliasIndex;

#[derive(Debug, Clone)]
pub(crate) struct ListedValues<T> {
    values: Vec<T>,
    /// One entry per value, then one for `other` where its rate is above zero.
    choice: WeightedAliasIndex<f64>,
    other_share: f64,
}

impl<T> ListedValues<T> {
    /// The values by their rates, and `other_rate` beside them. It fails only on unusable rates.
    pub(crate) fn new(listed: Vec<(T, f64)>, other_rate: f64) -> Result<Self, String> {
        let mut weights: Vec<f64> = listed.iter().map(|&(_, rate)| rate).collect();
        let other_rate = other_rate.max(0.0);
        if other_rate > 0.0 {
            weights.push(other_rate);
        }
        let total: f64 = weights.iter().sum();
        let choice = WeightedAliasIndex::new(weights).map_err(|error| format!("top_values rates are unusable: {error}"))?;
        Ok(Self {
            values: listed.into_iter().map(|(value, _)| value).collect(),
            choice,
            other_share: other_rate / total,
        })
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
}
