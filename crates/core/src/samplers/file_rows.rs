//! The rows each file of a generated dataset holds, drawn from the source's recorded file sizes. The
//! draw never touches a value, so the split it makes never moves one.

use rand_chacha::ChaCha8Rng;

use super::inverse_cdf::InverseCdf;
use super::inverse_cdf::Knots;
use super::numeric::quantile_knots;
use crate::profile::Quantiles;
use crate::rng::rng_for;
use crate::rng::stream_key;
use crate::rng::unit;

/// File sizes in rows, drawn without end through the inverse CDF of `quantiles` from a stream keyed
/// by the seed alone. A caller takes draws until they cover its rows.
pub fn file_rows_draws(seed: u64, quantiles: &Quantiles) -> Result<impl Iterator<Item = f64>, String> {
    let knots = quantile_knots(Some(quantiles)).map_err(|error| format!("rows_per_file: {error}"))?;
    if knots.last().is_none_or(|&(_, rows)| rows < 1.0) {
        return Err("rows_per_file holds no quantile at or above 1 row".into());
    }
    let knots = Knots::from_sorted(knots).ok_or("rows_per_file falls from one quantile to the next")?;
    let cdf = InverseCdf::new(knots);
    let mut rng: ChaCha8Rng = rng_for(seed, stream_key("", "", "layout.rows_per_file"), 0);
    Ok(std::iter::repeat_with(move || cdf.value_at(unit(&mut rng))))
}
