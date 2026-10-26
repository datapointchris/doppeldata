//! Values only a text format can carry. A null cell in a column with `missing_tokens` prints one
//! of them, as often as their rates say.

use std::collections::BTreeMap;

use arrow_buffer::BooleanBuffer;
use rand_chacha::ChaCha8Rng;
use rand_distr::Distribution;
use rand_distr::weighted::WeightedAliasIndex;

use super::Refusal;
use crate::plan::ColumnFault;
use crate::profile::Rate;

/// One cell a CSV or JSON writer prints in place of the Arrow value, which is null.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextCell {
    /// The row within its block.
    pub row: u32,
    pub text: String,
}

#[derive(Debug, Clone)]
pub(crate) struct TextOnlyValues {
    /// The tokens, and a draw over them for a null row. The draw has one more entry, for a null
    /// that prints no token, when the tokens cover only part of the nulls.
    tokens: (Vec<String>, WeightedAliasIndex<f64>),
}

/// The slack allowed when missing tokens' rates sum to the null rate, so a sum off in its last
/// bits is not refused.
const TOKEN_RATE_SLACK: f64 = 1e-9;

impl TextOnlyValues {
    /// The column's text-only values, or `None` when it has none. Each missing token's rate is its
    /// share of rows, so the tokens' rates sum to at most `null_share`.
    pub(crate) fn new(missing_tokens: Option<&BTreeMap<String, Rate>>, null_share: f64) -> Result<Option<Self>, Refusal> {
        let Some(tokens) = missing_tokens.filter(|tokens| tokens.values().any(|r| r.0 > 0.0)) else {
            return Ok(None);
        };
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
        Ok(Some(Self {
            tokens: (tokens.keys().cloned().collect(), choice),
        }))
    }

    /// The block's text cells. `nulls` marks the rows the null mask drew.
    pub(crate) fn cells(&self, nulls: Option<&BooleanBuffer>, token_rng: &mut ChaCha8Rng, rows: usize) -> Vec<TextCell> {
        let (tokens, choice) = &self.tokens;
        let mut cells = Vec::new();
        for row in 0..rows {
            if nulls.is_some_and(|nulls| nulls.value(row))
                && let Some(text) = tokens.get(choice.sample(token_rng)).cloned()
            {
                cells.push(TextCell { row: row as u32, text });
            }
        }
        cells
    }
}
