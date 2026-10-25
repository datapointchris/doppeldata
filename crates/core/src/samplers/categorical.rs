//! A string column with stored `top_values`: an alias table over their rates, where a draw of the
//! `other_rate` share generates a value instead. With a suffix, each top value is a prefix, and a
//! draw of the suffix pattern follows it.

use arrow_array::ArrayRef;
use arrow_buffer::NullBuffer;

use super::BlockPosition;
use super::StreamKeys;
use super::distinct_values::ValueIndex;
use super::listed_values::ListedValues;
use super::text::Pattern;
use super::text::StringOutput;
use super::text::TextSource;
use crate::rng::rng_for;

#[derive(Debug, Clone)]
pub(crate) struct CategoricalSampler {
    pub(crate) listed: ListedValues<String>,
    pub(crate) suffix: Option<Box<Pattern>>,
    pub(crate) other: TextSource,
    /// Where set, an `other` row's value is the one at an index the row draws.
    pub(crate) other_index: Option<ValueIndex>,
    pub(crate) output: StringOutput,
}

impl CategoricalSampler {
    pub(crate) fn generate(&self, at: &BlockPosition, keys: &StreamKeys, rows: usize, nulls: Option<NullBuffer>) -> ArrayRef {
        let mut values = rng_for(at.seed, keys.values, at.block);
        let mut indices = rng_for(at.seed, keys.index, at.block);
        self.output
            .build(rows, nulls.as_ref(), |out| match self.listed.sample(&mut values) {
                Some(value) => {
                    out.push_str(value);
                    if let Some(suffix) = &self.suffix {
                        suffix.push(&mut values, out);
                    }
                }
                None => match self.other_index {
                    Some(index) => self.other.push(&mut index.value_stream(at.seed, keys, &mut indices), out),
                    None => self.other.push(&mut values, out),
                },
            })
    }
}
