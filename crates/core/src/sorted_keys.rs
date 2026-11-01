//! Serializes a JSON value with every object's keys sorted, for a field whose written bytes must
//! not depend on the order its keys were inserted in. schemars' `preserve_order` turns on
//! serde_json's across the workspace, so a `Value` or `Map` keeps insertion order unless sorted.

use serde::Serialize;
use serde::Serializer;
use serde_json::Value;

/// Serializes `value` with the keys of every object it holds sorted, at any depth. A field names it
/// in `#[serde(serialize_with = "crate::sorted_keys::serialize")]`.
pub fn serialize<T: Clone + Into<Value>, S: Serializer>(value: &T, serializer: S) -> Result<S::Ok, S::Error> {
    let mut value: Value = value.clone().into();
    value.sort_all_objects();
    value.serialize(serializer)
}
