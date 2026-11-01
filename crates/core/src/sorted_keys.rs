//! A JSON value written with every object's keys sorted at any depth. schemars' `preserve_order`
//! turns on serde_json's insertion order for the whole workspace, so a `Value` or `Map` keeps the
//! order its keys were inserted in. One that reaches a written byte, a SHA-256 or a document handed
//! to Python is held as `SortedKeys`, which writes the same bytes whatever that order was.

use std::borrow::Cow;
use std::ops::Deref;
use std::ops::DerefMut;

use schemars::JsonSchema;
use schemars::Schema;
use schemars::SchemaGenerator;
use serde::Deserialize;
use serde::Serialize;
use serde::Serializer;
use serde_json::Value;

/// `T`, serialized with the keys of every object it holds sorted, at any depth. It reads as `T`
/// reads, and its JSON Schema is `T`'s.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(transparent)]
pub struct SortedKeys<T>(pub T);

impl<T: Clone + Into<Value>> Serialize for SortedKeys<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut value: Value = self.0.clone().into();
        value.sort_all_objects();
        value.serialize(serializer)
    }
}

impl<T: JsonSchema> JsonSchema for SortedKeys<T> {
    fn inline_schema() -> bool {
        T::inline_schema()
    }

    fn schema_name() -> Cow<'static, str> {
        T::schema_name()
    }

    fn schema_id() -> Cow<'static, str> {
        T::schema_id()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        T::json_schema(generator)
    }
}

impl<T> From<T> for SortedKeys<T> {
    fn from(value: T) -> Self {
        Self(value)
    }
}

impl<T> Deref for SortedKeys<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.0
    }
}

impl<T> DerefMut for SortedKeys<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.0
    }
}
