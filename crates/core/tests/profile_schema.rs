use std::fs;
use std::path::Path;

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
