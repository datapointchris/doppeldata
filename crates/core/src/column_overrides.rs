//! Columns a caller pins to values of its own, each laid in at its column's place in the draw
//! order, so every column drawn from it reads the pinned values and every other column draws as
//! it would unpinned.

use arrow_array::Array;
use arrow_array::ArrayRef;
use arrow_array::StringArray;
use arrow_array::UInt32Array;
use arrow_array::new_null_array;
use arrow_cast::cast::CastOptions;
use arrow_cast::cast::cast_with_options;
use arrow_schema::DataType;
use arrow_schema::Field;
use serde::Deserialize;
use serde::Serialize;

use crate::identity::sha256_digest;
use crate::physical_type::physical_type_name;
use crate::plan::PlanError;
use crate::rng::BLOCK_ROWS;

/// The kind of value a caller gave, as the caller's language names it. Each column type takes the
/// kinds [`takes`] lists, so a string never pins an integer column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ValueKind {
    Boolean,
    Integer,
    Float,
    Decimal,
    String,
    Date,
    Timestamp,
    Time,
}

impl ValueKind {
    const ALL: [Self; 8] = [
        Self::Boolean,
        Self::Integer,
        Self::Float,
        Self::Decimal,
        Self::String,
        Self::Date,
        Self::Timestamp,
        Self::Time,
    ];

    fn noun(self) -> &'static str {
        match self {
            Self::Boolean => "a boolean",
            Self::Integer => "an integer",
            Self::Float => "a float",
            Self::Decimal => "a decimal",
            Self::String => "a string",
            Self::Date => "a date",
            Self::Timestamp => "a datetime",
            Self::Time => "a time",
        }
    }
}

/// One value a caller pins: its kind, and its text, which an integer, float or decimal writes in
/// decimal and a date, datetime or time in ISO 8601.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct GivenValue {
    pub kind: ValueKind,
    pub text: String,
}

/// What a column is pinned to: one value on every row, or a value for each row, null where `None`.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OverrideValues {
    Every(Option<GivenValue>),
    EachRow(Vec<Option<GivenValue>>),
}

/// A top-level column and the values a caller pins it to.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct ColumnOverride {
    pub column: String,
    pub values: OverrideValues,
}

/// The digest an identity names `overrides` by: the SHA-256 of their JSON sorted by column, so the
/// order a caller lists them in moves nothing. Each struct serializes its fields in sorted order.
pub(crate) fn overrides_digest(overrides: &[ColumnOverride]) -> String {
    let mut sorted: Vec<&ColumnOverride> = overrides.iter().collect();
    sorted.sort_by(|a, b| a.column.cmp(&b.column));
    sha256_digest(
        serde_json::to_string(&sorted)
            .expect("strings and lists of them always serialize")
            .as_bytes(),
    )
}

/// A pinned column's values, cast to its type once at plan time.
#[derive(Debug)]
enum PinnedValues {
    /// One value repeated across a block's rows, sliced to each block.
    Every(ArrayRef),
    /// Every row's value, sliced at each block's first row.
    EachRow(ArrayRef),
}

/// A top-level column by its index, and the values it takes in place of its draw.
#[derive(Debug)]
pub(crate) struct PinnedColumn {
    pub(crate) index: usize,
    values: PinnedValues,
}

impl PinnedColumn {
    /// The pinned values of the `rows` rows from `first_row`.
    pub(crate) fn block(&self, first_row: u64, rows: usize) -> ArrayRef {
        match &self.values {
            PinnedValues::Every(array) => array.slice(0, rows),
            PinnedValues::EachRow(array) => array.slice(first_row as usize, rows),
        }
    }
}

/// The refusal of an override, naming its column's JSON pointer, or none for a column the table
/// lacks.
fn refuse(field: &str, message: String) -> PlanError {
    PlanError::Override {
        field: field.to_string(),
        message,
    }
}

/// Whether a column of `data_type` takes a value of `kind`. A float column takes an integer, and a
/// decimal column an integer or a float, because each holds them.
fn takes(data_type: &DataType, kind: ValueKind) -> bool {
    use ValueKind::*;
    match data_type {
        DataType::Boolean => kind == Boolean,
        DataType::Float16 | DataType::Float32 | DataType::Float64 => matches!(kind, Integer | Float),
        DataType::Decimal128(..) | DataType::Decimal256(..) => matches!(kind, Integer | Float | Decimal),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => kind == String,
        DataType::Date32 | DataType::Date64 => kind == Date,
        DataType::Timestamp(..) => kind == Timestamp,
        DataType::Time32(_) | DataType::Time64(_) => kind == Time,
        integer if integer.is_integer() => kind == Integer,
        _ => false,
    }
}

/// Whether any value pins a column of `data_type`: a scalar, never a list, struct or binary.
fn pinnable(data_type: &DataType) -> bool {
    ValueKind::ALL.into_iter().any(|kind| takes(data_type, kind))
}

/// Each override resolved against the table's top-level `fields` for `rows` rows. `type_mix`
/// holds whether each column generates as text from a type mix. It refuses a column the table
/// lacks, one no value pins, a value of a kind its column does not take or that it cannot hold, a
/// null on a column holding none, and a list not one value per row.
pub(crate) fn pinned_columns(
    fields: &[&Field],
    type_mix: &[bool],
    overrides: &[ColumnOverride],
    rows: u64,
) -> Result<Vec<PinnedColumn>, PlanError> {
    let mut pinned = Vec::with_capacity(overrides.len());
    for given in overrides {
        let column = given.column.as_str();
        let Some(index) = fields.iter().position(|field| field.name() == column) else {
            let held: Vec<&str> = fields.iter().map(|field| field.name().as_str()).collect();
            return Err(refuse(
                "",
                format!("the table holds no column {column:?}; it holds {}", held.join(", ")),
            ));
        };
        let pointer = format!("/columns/{index}");
        if pinned.iter().any(|earlier: &PinnedColumn| earlier.index == index) {
            return Err(refuse(&pointer, format!("column {column:?} is pinned twice")));
        }
        let field = fields[index];
        let type_name = physical_type_name(field.data_type());
        if type_mix[index] {
            return Err(refuse(
                &pointer,
                format!("column {column:?} generates as text from its type_mix, which no override pins"),
            ));
        }
        if !pinnable(field.data_type()) {
            return Err(refuse(
                &pointer,
                format!("column {column:?} holds {type_name} values, which no override pins"),
            ));
        }
        let values = match &given.values {
            OverrideValues::Every(value) => {
                let one = cast(field, &pointer, &type_name, std::slice::from_ref(value))?;
                let repeated = UInt32Array::from(vec![0; rows.min(BLOCK_ROWS) as usize]);
                PinnedValues::Every(arrow_select::take::take(&one, &repeated, None).expect("index 0 is inside a one-value array"))
            }
            OverrideValues::EachRow(values) => {
                if values.len() as u64 != rows {
                    return Err(refuse(
                        &pointer,
                        format!(
                            "the override of column {column:?} holds {} values, and the sample is {rows} rows",
                            values.len()
                        ),
                    ));
                }
                PinnedValues::EachRow(cast(field, &pointer, &type_name, values)?)
            }
        };
        pinned.push(PinnedColumn { index, values });
    }
    Ok(pinned)
}

/// `values` as an array of `field`'s type, refusing at `pointer` the first that its column does not
/// take or cannot hold.
fn cast(field: &Field, pointer: &str, type_name: &str, values: &[Option<GivenValue>]) -> Result<ArrayRef, PlanError> {
    let column = field.name();
    if values.iter().all(Option::is_none) {
        if !field.is_nullable() && !values.is_empty() {
            return Err(refuse(
                pointer,
                format!("column {column:?} holds no nulls, and the override gives it one"),
            ));
        }
        return Ok(new_null_array(field.data_type(), values.len()));
    }
    for value in values {
        match value {
            None if !field.is_nullable() => {
                return Err(refuse(
                    pointer,
                    format!("column {column:?} holds no nulls, and the override gives it one"),
                ));
            }
            Some(value) if !takes(field.data_type(), value.kind) => {
                return Err(refuse(
                    pointer,
                    format!(
                        "column {column:?} holds {type_name} values, and the override gives it {} {:?}",
                        value.kind.noun(),
                        value.text
                    ),
                ));
            }
            Some(value) => {
                if let DataType::Decimal128(_, scale) | DataType::Decimal256(_, scale) = field.data_type()
                    && decimal_places(&value.text) > (*scale).max(0) as usize
                {
                    return Err(refuse(
                        pointer,
                        format!(
                            "column {column:?} holds {type_name} values, and {} has more decimal places",
                            value.text
                        ),
                    ));
                }
            }
            None => {}
        }
    }
    let texts = StringArray::from_iter(values.iter().map(|value| value.as_ref().map(|value| value.text.as_str())));
    let options = CastOptions {
        safe: true,
        ..CastOptions::default()
    };
    let typed = cast_with_options(&texts, field.data_type(), &options).map_err(|error| {
        refuse(
            pointer,
            format!("column {column:?} holds {type_name} values, which its override cannot be read as: {error}"),
        )
    })?;
    if let Some(row) = (0..values.len()).find(|&row| texts.is_valid(row) && typed.is_null(row)) {
        return Err(refuse(
            pointer,
            format!("column {column:?} holds {type_name} values, and {:?} is not one", texts.value(row)),
        ));
    }
    Ok(typed)
}

/// The places a number's text needs after the decimal point, its trailing zeros left out, before
/// any exponent.
fn decimal_places(text: &str) -> usize {
    let mantissa = text.split(['e', 'E']).next().unwrap_or(text);
    mantissa
        .split_once('.')
        .map_or(0, |(_, fraction)| fraction.trim_end_matches('0').len())
}
