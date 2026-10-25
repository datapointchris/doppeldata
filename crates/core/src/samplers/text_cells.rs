//! Values only a text format can carry. A `type_mix` share such as 0.1% `string` in an integer
//! column becomes cells of that type, which the Arrow batch records as nulls. A null cell in a
//! column with `missing_tokens` prints one of them, as often as their rates say.

use std::collections::BTreeMap;

use arrow_buffer::BooleanBuffer;
use chrono::DateTime;
use rand::RngExt;
use rand_chacha::ChaCha8Rng;
use rand_distr::Distribution;
use rand_distr::weighted::WeightedAliasIndex;

use super::Refusal;
use crate::plan::ColumnFault;
use crate::profile::Rate;
use crate::profile::RawValueType;
use crate::rng::unit;

/// One cell a CSV or JSON writer prints in place of the Arrow value, which is null.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextCell {
    /// The row within its block.
    pub row: u32,
    pub text: String,
}

/// Text that parses as a `type_mix` type, foreign to the column's own.
fn foreign_text(foreign: RawValueType, rng: &mut ChaCha8Rng) -> String {
    const DAYS_FROM_EPOCH_TO_2000: i64 = 10_957;
    let instant = |rng: &mut ChaCha8Rng| {
        let seconds = (DAYS_FROM_EPOCH_TO_2000 + rng.random_range(0..10_957i64)) * 86_400;
        DateTime::from_timestamp(seconds + rng.random_range(0..86_400i64), 0).unwrap_or_default()
    };
    match foreign {
        RawValueType::Integer => rng.random_range(0..100_000u32).to_string(),
        RawValueType::Decimal => format!("{}.{:02}", rng.random_range(0..10_000u32), rng.random_range(0..100u32)),
        RawValueType::Boolean => if rng.random_bool(0.5) { "true" } else { "false" }.to_string(),
        RawValueType::Date => instant(rng).format("%Y-%m-%d").to_string(),
        RawValueType::Timestamp => instant(rng).format("%Y-%m-%dT%H:%M:%S").to_string(),
        RawValueType::String => {
            let length = rng.random_range(3..=8);
            (0..length).map(|_| char::from(b'a' + rng.random_range(0..26u8))).collect()
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TextOnlyValues {
    /// Each foreign type with the running total of shares up to and including it.
    foreign: Vec<(f64, RawValueType)>,
    /// The tokens, and a draw over them for a null row. The draw has one more entry, for a null
    /// that prints no token, when the tokens cover only part of the nulls.
    tokens: Option<(Vec<String>, WeightedAliasIndex<f64>)>,
}

/// The slack allowed when missing tokens' rates sum to the null rate, so a sum off in its last
/// bits is not refused.
const TOKEN_RATE_SLACK: f64 = 1e-9;

impl TextOnlyValues {
    /// The column's text-only values, or `None` when it has none. Every share in `type_mix` but the
    /// largest is foreign to the column's type. A string column holds every raw value as itself,
    /// and its string statistics describe them all, so its `type_mix` adds nothing. Each missing
    /// token's rate is its share of rows, so the tokens' rates sum to at most `null_share`.
    pub(crate) fn new(
        type_mix: Option<&BTreeMap<RawValueType, Rate>>,
        missing_tokens: Option<&BTreeMap<String, Rate>>,
        null_share: f64,
        string_column: bool,
    ) -> Result<Option<Self>, Refusal> {
        let mut foreign = Vec::new();
        if let Some(type_mix) = type_mix.filter(|_| !string_column) {
            let dominant = type_mix.iter().max_by(|a, b| a.1.0.total_cmp(&b.1.0)).map(|(&raw, _)| raw);
            let mut total = 0.0;
            for (&raw, share) in type_mix.iter().filter(|&(&raw, share)| Some(raw) != dominant && share.0 > 0.0) {
                total += share.0;
                foreign.push((total, raw));
            }
        }
        let tokens = match missing_tokens.filter(|tokens| tokens.values().any(|r| r.0 > 0.0)) {
            Some(tokens) => {
                let covered: f64 = tokens.values().map(|r| r.0).sum();
                if covered > null_share + TOKEN_RATE_SLACK {
                    let message = format!("the tokens cover {covered} of rows, more than the {null_share} that are null");
                    return Err(Refusal::new(ColumnFault::TokensExceedNullRate, message));
                }
                let mut weights: Vec<f64> = tokens.values().map(|r| r.0).collect();
                if null_share - covered > TOKEN_RATE_SLACK {
                    weights.push(null_share - covered);
                }
                let choice = WeightedAliasIndex::new(weights)
                    .map_err(|error| Refusal::new(ColumnFault::UnusableRates, format!("missing_tokens rates are unusable: {error}")))?;
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
                    .and_then(|(tokens, choice)| tokens.get(choice.sample(token_rng)).cloned())
            } else {
                self.foreign
                    .iter()
                    .find(|(upper, _)| draw < *upper)
                    .map(|&(_, foreign)| foreign_text(foreign, text_rng))
            };
            if let Some(text) = text {
                cells.push(TextCell { row: row as u32, text });
            }
        }
        cells
    }
}
