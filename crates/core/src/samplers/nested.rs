//! List and struct columns. A list draws each row's length, then that many elements from its
//! element column. A struct draws each field as its own column. A child's streams are named by its
//! own path, so it draws the same whatever its siblings are.

use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_array::GenericListArray;
use arrow_array::OffsetSizeTrait;
use arrow_array::StructArray;
use arrow_buffer::NullBuffer;
use arrow_buffer::OffsetBuffer;
use arrow_schema::DataType;
use arrow_schema::Fields;

use super::BlockPosition;
use super::ColumnPlan;
use super::StreamKeys;
use super::inverse_cdf::InverseCdf;
use crate::rng::Bernoulli;
use crate::rng::rng_for;
use crate::rng::unit;

/// A list's length at level `u` of its length distribution.
fn length_at(length: &InverseCdf, u: f64) -> usize {
    length.value_at(u).round().max(0.0) as usize
}

/// The mean length a non-empty list draws, over evenly spaced levels.
pub(crate) fn mean_length(length: &InverseCdf) -> f64 {
    const LEVELS: u32 = 4096;
    let total: usize = (0..LEVELS)
        .map(|level| length_at(length, (f64::from(level) + 0.5) / f64::from(LEVELS)))
        .sum();
    total as f64 / f64::from(LEVELS)
}

#[derive(Debug, Clone)]
pub(crate) struct ListSampler {
    pub(crate) element: ColumnPlan,
    pub(crate) length: InverseCdf,
    pub(crate) empty: Option<Bernoulli>,
    /// Whether offsets are 64-bit, as a `large_list` stores them.
    pub(crate) large: bool,
}

impl ListSampler {
    pub(crate) fn data_type(&self) -> DataType {
        if self.large {
            DataType::LargeList(self.element.field.clone())
        } else {
            DataType::List(self.element.field.clone())
        }
    }

    pub(crate) fn generate(&self, at: &BlockPosition, keys: &StreamKeys, rows: usize, nulls: Option<NullBuffer>) -> ArrayRef {
        let mut rng = rng_for(at.seed, keys.lengths, at.block);
        let lengths: Vec<usize> = (0..rows)
            .map(|_| {
                if self.empty.is_some_and(|empty| empty.draw(&mut rng)) {
                    0
                } else {
                    length_at(&self.length, unit(&mut rng))
                }
            })
            .collect();
        let elements = BlockPosition { first_row: 0, ..*at };
        let values = self.element.generate(&elements, lengths.iter().sum());
        if self.large {
            self.array::<i64>(lengths, values, nulls)
        } else {
            self.array::<i32>(lengths, values, nulls)
        }
    }

    fn array<O: OffsetSizeTrait>(&self, lengths: Vec<usize>, values: ArrayRef, nulls: Option<NullBuffer>) -> ArrayRef {
        let offsets = OffsetBuffer::<O>::from_lengths(lengths);
        Arc::new(GenericListArray::<O>::new(self.element.field.clone(), offsets, values, nulls))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct StructSampler {
    pub(crate) fields: Vec<ColumnPlan>,
}

impl StructSampler {
    fn arrow_fields(&self) -> Fields {
        self.fields.iter().map(|field| field.field.clone()).collect()
    }

    pub(crate) fn data_type(&self) -> DataType {
        DataType::Struct(self.arrow_fields())
    }

    pub(crate) fn generate(&self, at: &BlockPosition, rows: usize, nulls: Option<NullBuffer>) -> ArrayRef {
        let children = self.fields.iter().map(|field| field.generate(at, rows)).collect();
        Arc::new(StructArray::new(self.arrow_fields(), children, nulls))
    }
}
