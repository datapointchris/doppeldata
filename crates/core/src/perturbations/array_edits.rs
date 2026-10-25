//! Edits to one column of a block, shared by the ops: which rows an op draws, a value written into
//! each, and the text cells dropped at a row an op writes.

use std::sync::Arc;

use arrow_array::Array;
use arrow_array::ArrayRef;
use arrow_array::Float64Array;
use arrow_array::StringArray;
use arrow_array::cast::AsArray;
use arrow_array::make_array;
use arrow_buffer::BooleanBuffer;
use arrow_buffer::NullBuffer;
use arrow_cast::CastOptions;
use arrow_cast::cast;
use arrow_cast::cast_with_options;
use arrow_cast::display::ArrayFormatter;
use arrow_cast::display::FormatOptions;
use arrow_schema::DataType;
use arrow_select::interleave::interleave;
use arrow_select::take::take;
use rand_chacha::ChaCha8Rng;

use crate::rng::Bernoulli;
use crate::samplers::NumericOutput;
use crate::samplers::TextCell;

/// The rows `rate` selects, one draw per row so a row's draw never depends on another's.
pub(super) fn drawn_rows(rng: &mut ChaCha8Rng, rate: f64, rows: usize) -> Vec<u32> {
    let chance = Bernoulli::new(rate);
    (0..rows as u32).filter(|_| chance.draw(rng)).collect()
}

/// As [`drawn_rows`], keeping only rows whose value in `array` is not null.
pub(super) fn drawn_valid_rows(rng: &mut ChaCha8Rng, rate: f64, array: &dyn Array) -> Vec<u32> {
    let mut rows = drawn_rows(rng, rate, array.len());
    rows.retain(|&row| array.is_valid(row as usize));
    rows
}

/// `original` with row `rows[k]` taking `values[k]`. `rows` ascends.
pub(super) fn replace_rows(original: &dyn Array, rows: &[u32], values: &dyn Array) -> ArrayRef {
    debug_assert_eq!(rows.len(), values.len());
    let mut next = rows.iter().enumerate().peekable();
    let picks: Vec<(usize, usize)> = (0..original.len())
        .map(|row| match next.next_if(|&(_, &written)| written as usize == row) {
            Some((k, _)) => (1, k),
            None => (0, row),
        })
        .collect();
    interleave(&[original, values], &picks).expect("an op writes values of the column's own type")
}

/// `original` with `rows` null. `rows` ascends.
pub(super) fn null_rows(original: &ArrayRef, rows: &[u32]) -> ArrayRef {
    if rows.is_empty() {
        return original.clone();
    }
    let mut next = rows.iter().peekable();
    let written = BooleanBuffer::collect_bool(original.len(), |row| next.next_if(|&&r| r as usize == row).is_some());
    let valid = match original.nulls() {
        Some(nulls) => nulls.inner() & &!&written,
        None => !&written,
    };
    let data = original.to_data().into_builder().nulls(Some(NullBuffer::new(valid)));
    make_array(data.build().expect("only the validity changed"))
}

/// The rows of `array` at `rows`.
pub(super) fn take_rows(array: &dyn Array, rows: &[u32]) -> ArrayRef {
    take(array, &arrow_array::UInt32Array::from(rows.to_vec()), None).expect("rows lie inside the block")
}

/// The values of a numeric array as f64, null where it is null.
pub(super) fn as_f64(array: &dyn Array) -> Float64Array {
    cast(array, &DataType::Float64)
        .expect("a numeric type casts to float64")
        .as_primitive()
        .clone()
}

/// `values` written as `data_type`: an integer rounded to the nearest, and each type saturated at its
/// bounds.
pub(super) fn from_f64(values: &[f64], output: NumericOutput) -> ArrayRef {
    let integer = output.is_integer();
    let mut next = values.iter();
    output.drawn_array(values.len(), None, || {
        let value = *next.next().expect("one value per row");
        if integer { value.round() } else { value }
    })
}

/// Text values cast to `data_type`, or why one cannot be. `None` stands for null.
pub(super) fn cast_texts(texts: &[Option<String>], data_type: &DataType) -> Result<ArrayRef, (usize, String)> {
    let strict = CastOptions {
        safe: false,
        ..CastOptions::default()
    };
    let strings = |texts: &[Option<String>]| -> ArrayRef { Arc::new(StringArray::from(texts.to_vec())) };
    if let Ok(cast) = cast_with_options(&strings(texts), data_type, &strict) {
        return Ok(cast);
    }
    for (index, text) in texts.iter().enumerate() {
        if let Err(error) = cast_with_options(&strings(std::slice::from_ref(text)), data_type, &strict) {
            return Err((index, format!("{text:?} is not a {data_type}: {error}")));
        }
    }
    unreachable!("a list that fails to cast holds a value that fails alone")
}

/// A string for each value of `array`, as a set of distinct keys compares them. Null is None.
pub(super) fn display_values(array: &dyn Array) -> Vec<Option<String>> {
    let formatter = ArrayFormatter::try_new(array, &FormatOptions::default()).expect("a scalar type formats");
    (0..array.len())
        .map(|row| array.is_valid(row).then(|| formatter.value(row).to_string()))
        .collect()
}

/// Removes column `column`'s cells at `rows`, which ascend. A written row prints its new value.
pub(super) fn drop_cells(cells: &mut Option<Vec<Vec<TextCell>>>, column: usize, rows: &[u32]) {
    if let Some(column_cells) = cells.as_mut().and_then(|cells| cells.get_mut(column)) {
        column_cells.retain(|cell| rows.binary_search(&cell.row).is_err());
    }
}

/// The cells of `rows` in `cells`, renumbered as rows `first..` in the order `rows` lists them.
pub(super) fn cells_of_rows(cells: &[TextCell], rows: &[u32], first: u32) -> Vec<TextCell> {
    let mut placed: Vec<TextCell> = Vec::new();
    for (offset, &row) in rows.iter().enumerate() {
        if let Ok(found) = cells.binary_search_by_key(&row, |cell| cell.row) {
            placed.push(TextCell {
                row: first + offset as u32,
                text: cells[found].text.clone(),
            });
        }
    }
    placed
}
