//! Values only a text format can carry. A `type_mix` share such as 0.1% `string` in an integer
//! column becomes cells of that type, which the Arrow batch records as nulls. A null cell in a
//! column with `missing_tokens` prints one of them.

use std::collections::BTreeMap;

use arrow_buffer::BooleanBuffer;
use chrono::DateTime;
use rand::RngExt;
use rand_chacha::ChaCha8Rng;
use rand_distr::Distribution;
use rand_distr::weighted::WeightedAliasIndex;

use crate::profile::Rate;
use crate::rng::unit;

/// One cell a CSV or JSON writer prints in place of the Arrow value, which is null.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextCell {
    /// The row within its block.
    pub row: u32,
    pub text: String,
}

/// A type a raw value can parse as, in the profiler's `type_mix` vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ForeignType {
    Integer,
    Decimal,
    Boolean,
    Date,
    Timestamp,
    Text,
}

impl ForeignType {
    fn from_name(name: &str) -> Self {
        match name {
            "integer" => Self::Integer,
            "decimal" | "number" | "float" => Self::Decimal,
            "boolean" => Self::Boolean,
            "date" => Self::Date,
            "timestamp" => Self::Timestamp,
            _ => Self::Text,
        }
    }

    fn text(self, rng: &mut ChaCha8Rng) -> String {
        const DAYS_FROM_EPOCH_TO_2000: i64 = 10_957;
        let instant = |rng: &mut ChaCha8Rng| {
            let seconds = (DAYS_FROM_EPOCH_TO_2000 + rng.random_range(0..10_957i64)) * 86_400;
            DateTime::from_timestamp(seconds + rng.random_range(0..86_400i64), 0).unwrap_or_default()
        };
        match self {
            Self::Integer => rng.random_range(0..100_000u32).to_string(),
            Self::Decimal => format!("{}.{:02}", rng.random_range(0..10_000u32), rng.random_range(0..100u32)),
            Self::Boolean => if rng.random_bool(0.5) { "true" } else { "false" }.to_string(),
            Self::Date => instant(rng).format("%Y-%m-%d").to_string(),
            Self::Timestamp => instant(rng).format("%Y-%m-%dT%H:%M:%S").to_string(),
            Self::Text => {
                let length = rng.random_range(3..=8);
                (0..length).map(|_| char::from(b'a' + rng.random_range(0..26u8))).collect()
            }
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TextOnlyValues {
    /// Each foreign type with the running total of shares up to and including it.
    foreign: Vec<(f64, ForeignType)>,
    tokens: Option<(Vec<String>, WeightedAliasIndex<f64>)>,
}

impl TextOnlyValues {
    /// The column's text-only values, or `None` when it has none. Every share in `type_mix` but the
    /// largest is foreign to the column's type.
    pub(crate) fn new(
        type_mix: Option<&BTreeMap<String, Rate>>,
        missing_tokens: Option<&BTreeMap<String, Rate>>,
    ) -> Result<Option<Self>, String> {
        let mut foreign = Vec::new();
        if let Some(type_mix) = type_mix {
            let dominant = type_mix.iter().max_by(|a, b| a.1.0.total_cmp(&b.1.0)).map(|(name, _)| name);
            let mut total = 0.0;
            for (name, share) in type_mix.iter().filter(|(name, share)| Some(*name) != dominant && share.0 > 0.0) {
                total += share.0;
                foreign.push((total, ForeignType::from_name(name)));
            }
        }
        let tokens = match missing_tokens.filter(|tokens| tokens.values().any(|r| r.0 > 0.0)) {
            Some(tokens) => {
                let choice = WeightedAliasIndex::new(tokens.values().map(|r| r.0).collect())
                    .map_err(|error| format!("missing_tokens rates are unusable: {error}"))?;
                Some((tokens.keys().cloned().collect(), choice))
            }
            None => None,
        };
        Ok((!foreign.is_empty() || tokens.is_some()).then_some(Self { foreign, tokens }))
    }

    pub(crate) fn has_foreign_values(&self) -> bool {
        !self.foreign.is_empty()
    }

    fn foreign_total(&self) -> f64 {
        self.foreign.last().map_or(0.0, |(total, _)| *total)
    }

    /// Which rows hold a value of the column's own type, from one draw per row.
    pub(crate) fn own_type_rows(&self, mix_rng: &mut ChaCha8Rng, rows: usize) -> Option<BooleanBuffer> {
        let total = self.foreign_total();
        (total > 0.0).then(|| BooleanBuffer::collect_bool(rows, |_| unit(mix_rng) >= total))
    }

    /// The block's text cells. `nulls` marks the rows the null mask drew.
    pub(crate) fn cells(
        &self,
        nulls: Option<&BooleanBuffer>,
        mix_rng: &mut ChaCha8Rng,
        text_rng: &mut ChaCha8Rng,
        token_rng: &mut ChaCha8Rng,
        rows: usize,
    ) -> Vec<TextCell> {
        let total = self.foreign_total();
        let mut cells = Vec::new();
        for row in 0..rows {
            let draw = if total > 0.0 { unit(mix_rng) } else { 1.0 };
            let text = if nulls.is_some_and(|nulls| nulls.value(row)) {
                self.tokens
                    .as_ref()
                    .map(|(tokens, choice)| tokens[choice.sample(token_rng)].clone())
            } else {
                self.foreign
                    .iter()
                    .find(|(upper, _)| draw < *upper)
                    .map(|(_, foreign)| foreign.text(text_rng))
            };
            if let Some(text) = text {
                cells.push(TextCell { row: row as u32, text });
            }
        }
        cells
    }
}
