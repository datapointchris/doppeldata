//! A text source's column whose values parse as several types generates as text. Each row draws one
//! of the column's `type_mix` shares. Its own type's share renders the column's own draw in its
//! source layout, and every other share is text parsing as that type.

use std::fmt::Write as _;

use arrow_array::Array;
use arrow_array::ArrayRef;
use arrow_array::cast::AsArray;
use arrow_array::types::Date32Type;
use arrow_array::types::Date64Type;
use arrow_array::types::Decimal128Type;
use arrow_array::types::Float32Type;
use arrow_array::types::Float64Type;
use arrow_array::types::Int8Type;
use arrow_array::types::Int16Type;
use arrow_array::types::Int32Type;
use arrow_array::types::Int64Type;
use arrow_array::types::TimestampMicrosecondType;
use arrow_array::types::TimestampMillisecondType;
use arrow_array::types::TimestampNanosecondType;
use arrow_array::types::TimestampSecondType;
use arrow_array::types::UInt8Type;
use arrow_array::types::UInt16Type;
use arrow_array::types::UInt32Type;
use arrow_array::types::UInt64Type;
use arrow_schema::DataType;
use arrow_schema::TimeUnit;
use chrono::DateTime;
use chrono::NaiveDate;
use chrono::Utc;
use chrono::format::Item;
use chrono::format::StrftimeItems;
use rand::RngExt;
use rand_chacha::ChaCha8Rng;

use super::BlockPosition;
use super::Refusal;
use super::StreamKeys;
use super::text::StringOutput;
use crate::plan::ColumnFault;
use crate::profile::Column;
use crate::profile::RawValueType;
use crate::profile::SourceFormat;
use crate::rng::BLOCK_ROWS;
use crate::rng::rng_for;
use crate::rng::unit;

/// The Arrow field metadata key a column generated from its type mix carries, naming the type its
/// own share renders. A JSON writer prints such a field's value as the JSON its text parses as.
pub const TYPE_MIX_FIELD_KEY: &str = "doppeldata.type_mix";

/// The layout a date renders in where the profile states none.
const DEFAULT_DATE_LAYOUT: &str = "%Y-%m-%d";
/// The layout a timestamp renders in where the profile states none.
const DEFAULT_TIMESTAMP_LAYOUT: &str = "%Y-%m-%dT%H:%M:%S%.f";

/// Whether `format` is read as text, so each value's type is what its text parses as.
fn is_text_source(format: SourceFormat) -> bool {
    matches!(
        format,
        SourceFormat::Csv | SourceFormat::Tsv | SourceFormat::Json | SourceFormat::Ndjson
    )
}

/// The raw type a column of `data_type` reads its text as, or None for a type text never parses as.
pub(crate) fn own_raw_type(data_type: &DataType) -> Option<RawValueType> {
    Some(match data_type {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => RawValueType::Integer,
        DataType::Float32 | DataType::Float64 | DataType::Decimal128(..) => RawValueType::Decimal,
        DataType::Boolean => RawValueType::Boolean,
        DataType::Date32 | DataType::Date64 => RawValueType::Date,
        DataType::Timestamp(..) => RawValueType::Timestamp,
        _ => return None,
    })
}

/// The name a profile's `type_mix` gives `raw`.
pub fn raw_type_name(raw: RawValueType) -> &'static str {
    match raw {
        RawValueType::String => "string",
        RawValueType::Integer => "integer",
        RawValueType::Decimal => "decimal",
        RawValueType::Boolean => "boolean",
        RawValueType::Date => "date",
        RawValueType::Timestamp => "timestamp",
    }
}

/// What a row of the column holds: its own draw, or text of another type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Pick {
    Own,
    Foreign(RawValueType),
}

/// How an own value is written as text.
#[derive(Debug, Clone)]
enum Rendering {
    Digits,
    /// A float's shortest decimal, or `places` places where the profile states a scale.
    Float {
        places: Option<usize>,
    },
    Decimal,
    Boolean,
    Instant {
        layout: Vec<Item<'static>>,
    },
}

/// One column's type mix, as it generates.
#[derive(Debug, Clone)]
pub(crate) struct TypeMix {
    pub(crate) own: RawValueType,
    rendering: Rendering,
    /// Each pick with the running share up to and including it, in the profile's order of types.
    /// Empty where the column only renders its own type as text.
    picks: Vec<(f64, Pick)>,
    own_share: f64,
    /// Whether the column is a key, so each foreign value is one only its row holds.
    keyed: bool,
    /// The column's JSON Pointer in the profile.
    pub(crate) pointer: String,
}

impl TypeMix {
    /// The type mix `column`, planned as `data_type`, generates from: where its table's source is
    /// read as text, and the share of its values parsing as its own type is under 1. None for any
    /// other column. `keyed` makes each foreign value one no other row holds.
    pub(crate) fn planned(
        column: &Column,
        data_type: &DataType,
        source: Option<SourceFormat>,
        keyed: bool,
        pointer: &str,
    ) -> Result<Option<Self>, Refusal> {
        let (Some(own), Some(mix)) = (own_raw_type(data_type), column.type_mix.as_ref()) else {
            return Ok(None);
        };
        let total: f64 = mix.values().map(|share| share.0.max(0.0)).sum();
        let own_share = mix.get(&own).map_or(0.0, |share| share.0);
        if !source.is_some_and(is_text_source) || own_share >= 1.0 || total <= 0.0 {
            return Ok(None);
        }
        let mut running = 0.0;
        let mut picks = Vec::with_capacity(mix.len());
        for (&raw, share) in mix.iter().filter(|(_, share)| share.0 > 0.0) {
            running += share.0 / total;
            picks.push((running, if raw == own { Pick::Own } else { Pick::Foreign(raw) }));
        }
        Ok(Some(Self {
            own,
            rendering: rendering(column, data_type)?,
            picks,
            own_share: own_share / total,
            keyed,
            pointer: pointer.to_string(),
        }))
    }

    /// `column`, planned as `data_type`, written as text in its own type's layout on every row.
    /// None where text never parses as that type.
    pub(crate) fn rendered_only(column: &Column, data_type: &DataType, pointer: &str) -> Result<Option<Self>, Refusal> {
        let Some(own) = own_raw_type(data_type) else {
            return Ok(None);
        };
        Ok(Some(Self {
            own,
            rendering: rendering(column, data_type)?,
            picks: Vec::new(),
            own_share: 1.0,
            keyed: false,
            pointer: pointer.to_string(),
        }))
    }

    /// The share of rows holding a value that hold the column's own draw.
    pub(crate) fn own_share(&self) -> f64 {
        self.own_share
    }

    /// Whether every row holds the column's own draw, written as text.
    pub(crate) fn renders_only(&self) -> bool {
        self.picks.iter().all(|(_, pick)| *pick == Pick::Own)
    }

    /// Makes each foreign value one only its row holds, as a key's must be.
    pub(crate) fn make_keyed(&mut self) {
        self.keyed = true;
    }

    /// How the plan describes it, after the own draw's description.
    pub(crate) fn describe(&self) -> String {
        let own = raw_type_name(self.own);
        if self.renders_only() {
            return format!(", written as {own} text");
        }
        let distinct = if self.keyed { ", each one row's own" } else { "" };
        format!(
            ", written as {own} text on {:.4} of values and text of other types on the rest{distinct}",
            self.own_share
        )
    }

    fn pick(&self, draw: f64) -> Pick {
        self.picks
            .iter()
            .find(|(upper, _)| draw < *upper)
            .or(self.picks.last())
            .map_or(Pick::Own, |&(_, pick)| pick)
    }

    /// `typed`, the column's own draws for the block at `at`, as text: each row's pick drawn from
    /// the `type_mix` stream, one word a row, and each foreign value from `type_mix_text`.
    pub(crate) fn render(&self, at: &BlockPosition, keys: &StreamKeys, typed: &ArrayRef) -> ArrayRef {
        let rows = typed.len();
        let own = self.rendering.writer(typed.as_ref());
        let nulls = typed.logical_nulls();
        if self.renders_only() {
            let mut row = 0;
            return StringOutput::Utf8.build(rows, nulls.as_ref(), |out| {
                own(row, out);
                row += 1;
            });
        }
        let mut picks = rng_for(at.seed, keys.type_mix, at.block);
        let mut texts = rng_for(at.seed, keys.type_mix_text, at.block);
        let mut row = 0;
        StringOutput::Utf8.build(rows, nulls.as_ref(), |out| {
            match self.pick(unit(&mut picks)) {
                Pick::Own => own(row, out),
                Pick::Foreign(raw) if self.keyed => push_key_text(raw, at.first_row + row as u64, out),
                Pick::Foreign(raw) => out.push_str(&foreign_text(raw, &mut texts)),
            }
            row += 1;
        })
    }

    /// The pick at dataset row `row`, read from the word the block's draw takes there.
    fn pick_at(&self, seed: u64, keys: &StreamKeys, row: u64) -> Pick {
        let mut rng = rng_for(seed, keys.type_mix, row / BLOCK_ROWS);
        rng.set_word_pos(2 * u128::from(row % BLOCK_ROWS));
        self.pick(unit(&mut rng))
    }

    /// The foreign text at dataset row `row` of a keyed column, or None where the row holds its own
    /// draw.
    pub(crate) fn key_text_at(&self, seed: u64, keys: &StreamKeys, row: u64) -> Option<String> {
        match self.pick_at(seed, keys, row) {
            Pick::Own => None,
            Pick::Foreign(raw) => {
                let mut out = String::new();
                push_key_text(raw, row, &mut out);
                Some(out)
            }
        }
    }
}

/// How `column`, planned as `data_type`, writes its own values, refused where its temporal layout
/// cannot be written.
fn rendering(column: &Column, data_type: &DataType) -> Result<Rendering, Refusal> {
    Ok(match data_type {
        DataType::Float32 | DataType::Float64 => Rendering::Float {
            places: column
                .numeric
                .as_ref()
                .and_then(|numeric| numeric.scale)
                .and_then(|scale| usize::try_from(scale).ok()),
        },
        DataType::Decimal128(..) => Rendering::Decimal,
        DataType::Boolean => Rendering::Boolean,
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(..) => {
            let default = match data_type {
                DataType::Timestamp(..) => DEFAULT_TIMESTAMP_LAYOUT,
                _ => DEFAULT_DATE_LAYOUT,
            };
            let stated = column.temporal.as_ref().and_then(|temporal| temporal.format.as_deref());
            Rendering::Instant {
                layout: written_layout(stated.unwrap_or(default))?,
            }
        }
        _ => Rendering::Digits,
    })
}

/// `layout` as items chrono writes. `%#z` only parses, so it is written as `%:z`, which it reads.
fn written_layout(layout: &str) -> Result<Vec<Item<'static>>, Refusal> {
    let refuse = |reason: &str| Refusal::new(ColumnFault::InvalidPattern, format!("temporal.format {layout:?} {reason}"));
    let items = StrftimeItems::new(&layout.replace("%#z", "%:z"))
        .parse_to_owned()
        .map_err(|_| refuse("is not a strftime layout"))?;
    let mut probe = String::new();
    write!(probe, "{}", DateTime::<Utc>::UNIX_EPOCH.format_with_items(items.iter())).map_err(|_| refuse("cannot be written"))?;
    Ok(items)
}

/// Appends one row's own-type value to a string.
type RowWriter<'a> = Box<dyn Fn(usize, &mut String) + 'a>;

impl Rendering {
    /// Writes the value at a row of `array` as text.
    fn writer<'a>(&'a self, array: &'a dyn Array) -> RowWriter<'a> {
        macro_rules! digits {
            ($type:ty) => {{
                let values = array.as_primitive::<$type>();
                Box::new(move |row, out: &mut String| {
                    let _ = write!(out, "{}", values.value(row));
                })
            }};
        }
        match (self, array.data_type()) {
            (Self::Digits, DataType::Int8) => digits!(Int8Type),
            (Self::Digits, DataType::Int16) => digits!(Int16Type),
            (Self::Digits, DataType::Int32) => digits!(Int32Type),
            (Self::Digits, DataType::Int64) => digits!(Int64Type),
            (Self::Digits, DataType::UInt8) => digits!(UInt8Type),
            (Self::Digits, DataType::UInt16) => digits!(UInt16Type),
            (Self::Digits, DataType::UInt32) => digits!(UInt32Type),
            (Self::Digits, DataType::UInt64) => digits!(UInt64Type),
            (Self::Float { places }, DataType::Float32) => {
                let values = array.as_primitive::<Float32Type>();
                Box::new(move |row, out| push_float(f64::from(values.value(row)), values.value(row), *places, out))
            }
            (Self::Float { places }, DataType::Float64) => {
                let values = array.as_primitive::<Float64Type>();
                Box::new(move |row, out| push_float(values.value(row), values.value(row), *places, out))
            }
            (Self::Decimal, &DataType::Decimal128(_, scale)) => {
                let values = array.as_primitive::<Decimal128Type>();
                Box::new(move |row, out| push_decimal(values.value(row), scale, out))
            }
            (Self::Boolean, DataType::Boolean) => {
                let values = array.as_boolean();
                Box::new(move |row, out| out.push_str(if values.value(row) { "true" } else { "false" }))
            }
            (Self::Instant { layout }, DataType::Date32) => {
                let values = array.as_primitive::<Date32Type>();
                Box::new(move |row, out| push_instant(i64::from(values.value(row)) * 86_400_000_000, layout, out))
            }
            (Self::Instant { layout }, DataType::Date64) => {
                let values = array.as_primitive::<Date64Type>();
                Box::new(move |row, out| push_instant(values.value(row).saturating_mul(1_000), layout, out))
            }
            (Self::Instant { layout }, &DataType::Timestamp(unit, _)) => {
                macro_rules! instants {
                    ($type:ty) => {{
                        let values = array.as_primitive::<$type>();
                        Box::new(move |row, out: &mut String| push_instant(micros(values.value(row), unit), layout, out))
                    }};
                }
                match unit {
                    TimeUnit::Second => instants!(TimestampSecondType),
                    TimeUnit::Millisecond => instants!(TimestampMillisecondType),
                    TimeUnit::Microsecond => instants!(TimestampMicrosecondType),
                    TimeUnit::Nanosecond => instants!(TimestampNanosecondType),
                }
            }
            (rendering, data_type) => unreachable!("{rendering:?} renders no {data_type}"),
        }
    }
}

/// `ticks` of `unit` as microseconds, saturating.
fn micros(ticks: i64, unit: TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => ticks.saturating_mul(1_000_000),
        TimeUnit::Millisecond => ticks.saturating_mul(1_000),
        TimeUnit::Microsecond => ticks,
        TimeUnit::Nanosecond => ticks.div_euclid(1_000),
    }
}

fn push_float<T: std::fmt::Display>(value: f64, native: T, places: Option<usize>, out: &mut String) {
    let _ = match places {
        Some(places) => write!(out, "{value:.places$}"),
        None => write!(out, "{native}"),
    };
}

/// A decimal's unscaled `value` at `scale` places.
fn push_decimal(value: i128, scale: i8, out: &mut String) {
    if scale <= 0 {
        let _ = write!(out, "{value}");
        out.extend(std::iter::repeat_n('0', usize::from(scale.unsigned_abs())).filter(|_| value != 0));
        return;
    }
    let factor = 10u128.pow(u32::from(scale.unsigned_abs()));
    let magnitude = value.unsigned_abs();
    let sign = if value < 0 { "-" } else { "" };
    let width = usize::from(scale.unsigned_abs());
    let _ = write!(out, "{sign}{}.{:0width$}", magnitude / factor, magnitude % factor);
}

/// `micros` after the epoch, in `layout`, at UTC.
fn push_instant(micros: i64, layout: &[Item<'static>], out: &mut String) {
    let instant = DateTime::<Utc>::from_timestamp_micros(micros).unwrap_or(DateTime::<Utc>::UNIX_EPOCH);
    let _ = write!(out, "{}", instant.format_with_items(layout.iter()));
}

/// Text that parses as `raw`, foreign to its column's type.
pub(crate) fn foreign_text(raw: RawValueType, rng: &mut ChaCha8Rng) -> String {
    const DAYS_FROM_EPOCH_TO_2000: i64 = 10_957;
    let instant = |rng: &mut ChaCha8Rng| {
        let seconds = (DAYS_FROM_EPOCH_TO_2000 + rng.random_range(0..10_957i64)) * 86_400;
        DateTime::from_timestamp(seconds + rng.random_range(0..86_400i64), 0).unwrap_or_default()
    };
    match raw {
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

/// Text of type `raw` that only dataset row `row` holds. It never parses as an earlier raw type,
/// and never as a number where the column's own values are digits. A boolean or date share holds
/// too few values for every row, so it is written as string text.
fn push_key_text(raw: RawValueType, row: u64, out: &mut String) {
    match raw {
        RawValueType::Decimal => {
            let _ = write!(out, "{row}.5");
        }
        RawValueType::Integer => {
            let _ = write!(out, "-{}", u128::from(row) + 1);
        }
        RawValueType::Timestamp => {
            let start = NaiveDate::from_ymd_opt(2000, 1, 1)
                .expect("a valid date")
                .and_time(chrono::NaiveTime::MIN);
            let seconds = i64::try_from(row).unwrap_or(i64::MAX);
            match start.checked_add_signed(chrono::TimeDelta::seconds(seconds)) {
                Some(instant) => {
                    let _ = write!(out, "{}", instant.format("%Y-%m-%dT%H:%M:%S"));
                }
                None => push_row_letters(row, out),
            }
        }
        RawValueType::String | RawValueType::Boolean | RawValueType::Date => push_row_letters(row, out),
    }
}

/// `x` and `row` in base 26 written `a` to `z`. The leading `x` keeps it from reading as `true`.
fn push_row_letters(row: u64, out: &mut String) {
    out.push('x');
    let start = out.len();
    let mut rest = row;
    loop {
        out.push(char::from(b'a' + (rest % 26) as u8));
        rest /= 26;
        if rest == 0 {
            break;
        }
    }
    // The digits went in least significant first.
    let written: String = out[start..].chars().rev().collect();
    out.truncate(start);
    out.push_str(&written);
}
