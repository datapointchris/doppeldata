//! `rate` of a string column's values broken. A value holding a character past ASCII becomes
//! mojibake, its UTF-8 bytes read as windows-1252. Any other value has one character replaced by
//! U+FFFD.

use arrow_array::Array;
use arrow_array::cast::AsArray;
use arrow_schema::DataType;

use super::Block;
use super::BlockPerturbation;
use super::Planned;
use super::Planning;
use super::ScenarioFault;
use super::Stage;
use super::array_edits;
use crate::physical_type::physical_type_name;
use crate::plan::PlanError;
use crate::rng::below;
use crate::scenario::BrokenCharacters;

/// The characters windows-1252 reads bytes 0x80 to 0x9F as. A byte it leaves undefined reads as
/// the C1 control of the same number, as decoders that map every byte do.
const CP1252_HIGH_CONTROLS: [char; 32] = [
    '\u{20AC}', '\u{0081}', '\u{201A}', '\u{0192}', '\u{201E}', '\u{2026}', '\u{2020}', '\u{2021}', '\u{02C6}', '\u{2030}', '\u{0160}',
    '\u{2039}', '\u{0152}', '\u{008D}', '\u{017D}', '\u{008F}', '\u{0090}', '\u{2018}', '\u{2019}', '\u{201C}', '\u{201D}', '\u{2022}',
    '\u{2013}', '\u{2014}', '\u{02DC}', '\u{2122}', '\u{0161}', '\u{203A}', '\u{0153}', '\u{009D}', '\u{017E}', '\u{0178}',
];

#[derive(Debug)]
struct BreakCharacters {
    column: usize,
    rate: f64,
    rows_key: u64,
    values_key: u64,
}

pub(super) fn plan(op: &BrokenCharacters, planning: &Planning<'_>) -> Result<Planned, PlanError> {
    let (column, field) = planning.column(&op.column, "/column")?;
    if !matches!(field.data_type(), DataType::Utf8 | DataType::LargeUtf8) {
        return Err(planning.refuse(
            ScenarioFault::OpNotForColumn,
            "/column",
            format!(
                "{:?} is {}, and broken_characters acts on a string",
                op.column,
                physical_type_name(field.data_type())
            ),
        ));
    }
    Ok(planning.rows(BreakCharacters {
        column,
        rate: op.rate.0,
        rows_key: planning.key("rows"),
        values_key: planning.key("values"),
    }))
}

/// `text`'s UTF-8 bytes read as windows-1252.
fn mojibake(text: &str) -> String {
    text.bytes()
        .map(|byte| match byte {
            0x80..=0x9F => CP1252_HIGH_CONTROLS[usize::from(byte - 0x80)],
            _ => char::from(byte),
        })
        .collect()
}

/// `text` with the character at `position` replaced by U+FFFD, or U+FFFD alone where it is empty.
fn replaced(text: &str, position: usize) -> String {
    match text.is_empty() {
        true => '\u{FFFD}'.to_string(),
        false => text
            .chars()
            .enumerate()
            .map(|(index, c)| if index == position { '\u{FFFD}' } else { c })
            .collect(),
    }
}

impl BlockPerturbation for BreakCharacters {
    fn apply(&self, stage: &Stage<'_>, block: Block) -> Block {
        let column = block.batch.column(self.column).clone();
        let rows = array_edits::drawn_valid_rows(&mut stage.stream(self.rows_key), self.rate, &column);
        let mut positions = stage.stream(self.values_key);
        let originals = array_edits::take_rows(&column, &rows);
        let texts: Vec<Option<String>> = (0..originals.len())
            .map(|index| {
                let text = match originals.data_type() {
                    DataType::LargeUtf8 => originals.as_string::<i64>().value(index),
                    _ => originals.as_string::<i32>().value(index),
                };
                Some(match text.is_ascii() {
                    false => mojibake(text),
                    true => replaced(text, below(&mut positions, text.len().max(1) as u32) as usize),
                })
            })
            .collect();
        let broken = array_edits::cast_texts(&texts, column.data_type()).expect("a string casts to its own type");
        let written = array_edits::replace_rows(&column, &rows, &broken);
        block.with_column(stage.schema, self.column, written)
    }
}
