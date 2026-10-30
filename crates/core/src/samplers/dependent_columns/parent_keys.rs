//! Each row's place among the values a dependence lists for its parent column. A value is a key as
//! the profile writes a category: a string as itself, an integer in decimal, a boolean as `true` or
//! `false`, and any other type as Arrow writes it as text.

use std::collections::HashMap;

use arrow_array::Array;
use arrow_array::ArrayRef;
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_schema::DataType;

/// A row whose parent holds a value the dependence does not list.
pub(crate) const OTHER: u32 = u32::MAX - 1;
/// A row whose parent is null.
pub(crate) const NULL: u32 = u32::MAX;

#[derive(Debug, Clone)]
pub(crate) struct ParentKeys {
    by_text: HashMap<String, u32>,
    by_integer: HashMap<i64, u32>,
}

impl ParentKeys {
    /// The keys in their listed order, each at its place.
    pub(crate) fn new<'a>(keys: impl IntoIterator<Item = &'a String>) -> Self {
        let by_text: HashMap<String, u32> = keys.into_iter().enumerate().map(|(at, key)| (key.clone(), at as u32)).collect();
        let by_integer = by_text
            .iter()
            .filter_map(|(key, &at)| {
                key.parse::<i64>()
                    .ok()
                    .filter(|value| value.to_string() == *key)
                    .map(|value| (value, at))
            })
            .collect();
        Self { by_text, by_integer }
    }

    /// Each row's place among the keys, or [`OTHER`] or [`NULL`].
    pub(crate) fn places(&self, array: &ArrayRef) -> Vec<u32> {
        let text = |value: &str| self.by_text.get(value).copied().unwrap_or(OTHER);
        let rows = 0..array.len();
        match array.data_type() {
            DataType::Utf8 => {
                let strings = array.as_string::<i32>();
                rows.map(|row| if strings.is_null(row) { NULL } else { text(strings.value(row)) })
                    .collect()
            }
            DataType::LargeUtf8 => {
                let strings = array.as_string::<i64>();
                rows.map(|row| if strings.is_null(row) { NULL } else { text(strings.value(row)) })
                    .collect()
            }
            DataType::Boolean => {
                let booleans = array.as_boolean();
                let (yes, no) = (text("true"), text("false"));
                rows.map(|row| match booleans.is_null(row) {
                    true => NULL,
                    false if booleans.value(row) => yes,
                    false => no,
                })
                .collect()
            }
            data_type if data_type.is_integer() => {
                let integers = arrow_cast::cast(array, &DataType::Int64).expect("an integer array casts to Int64");
                let integers = integers.as_primitive::<Int64Type>();
                rows.map(|row| match integers.is_null(row) {
                    true => NULL,
                    false => self.by_integer.get(&integers.value(row)).copied().unwrap_or(OTHER),
                })
                .collect()
            }
            _ => match arrow_cast::cast(array, &DataType::Utf8) {
                Ok(strings) => {
                    let strings = strings.as_string::<i32>();
                    rows.map(|row| if strings.is_null(row) { NULL } else { text(strings.value(row)) })
                        .collect()
                }
                Err(_) => rows.map(|row| if array.is_null(row) { NULL } else { OTHER }).collect(),
            },
        }
    }
}
