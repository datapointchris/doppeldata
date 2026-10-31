use std::fs;
use std::path::Path;

use serde_json::Value;

#[test]
fn a_profile_section_added_without_deny_unknown_fields_fails_here() {
    let schema: Value = serde_json::from_str(&doppeldata_core::profile::json_schema()).unwrap();
    let mut open = Vec::new();
    open_objects(&schema, String::new(), &mut open);
    assert!(
        open.is_empty(),
        "these profile sections read a misspelled key as absent; give each struct #[serde(deny_unknown_fields)]: {open:?}"
    );
}

/// The JSON Pointer of each object schema below `value` that accepts a key it does not declare.
fn open_objects(value: &Value, at: String, open: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            if map.contains_key("properties") && map.get("additionalProperties") != Some(&Value::Bool(false)) {
                open.push(at.clone());
            }
            for (key, inner) in map {
                open_objects(inner, format!("{at}/{key}"), open);
            }
        }
        Value::Array(items) => {
            for (index, inner) in items.iter().enumerate() {
                open_objects(inner, format!("{at}/{index}"), open);
            }
        }
        _ => {}
    }
}

#[test]
fn every_committed_schema_is_the_one_the_model_derives() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../library/src/doppeldata/schemas");
    for schema in doppeldata_core::published_schemas::all() {
        let path = dir.join(schema.file_name);
        let committed = fs::read_to_string(&path).unwrap_or_default();
        assert!(
            committed == (schema.derive)(),
            "{} is not what the Rust model derives; run `task generate` and commit the result",
            path.display()
        );
    }
}
