//! A string column with stored `top_values`: an alias table over their rates, where a draw of the
//! `other_rate` share generates a value instead.

use arrow_array::ArrayRef;
use arrow_buffer::NullBuffer;
use rand_chacha::ChaCha8Rng;
use rand_distr::Distribution;
use rand_distr::weighted::WeightedAliasIndex;

use super::text::StringOutput;
use super::text::TextSource;

#[derive(Debug, Clone)]
pub(crate) struct CategoricalSampler {
    pub(crate) values: Vec<String>,
    /// One entry per value, then one for `other` when its rate is above zero.
    pub(crate) choice: WeightedAliasIndex<f64>,
    pub(crate) other: TextSource,
    pub(crate) output: StringOutput,
}

impl CategoricalSampler {
    pub(crate) fn new(values: Vec<(String, f64)>, other_rate: f64, other: TextSource, output: StringOutput) -> Result<Self, String> {
        let mut weights: Vec<f64> = values.iter().map(|(_, rate)| *rate).collect();
        if other_rate > 0.0 {
            weights.push(other_rate);
        }
        let choice = WeightedAliasIndex::new(weights).map_err(|error| format!("top_values rates are unusable: {error}"))?;
        Ok(Self {
            values: values.into_iter().map(|(value, _)| value).collect(),
            choice,
            other,
            output,
        })
    }

    pub(crate) fn generate(&self, rng: &mut ChaCha8Rng, rows: usize, nulls: Option<NullBuffer>) -> ArrayRef {
        self.output
            .build(rows, nulls.as_ref(), |out| match self.values.get(self.choice.sample(rng)) {
                Some(value) => out.push_str(value),
                None => self.other.push(rng, out),
            })
    }
}
