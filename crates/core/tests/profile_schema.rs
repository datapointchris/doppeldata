use std::fs;
use std::path::Path;

#[test]
fn the_committed_profile_schema_is_the_one_the_model_derives() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../library/src/doppeldata/schemas/profile.v1.json");
    let committed = fs::read_to_string(&path).unwrap_or_default();
    assert!(
        committed == doppeldata_core::profile::json_schema(),
        "{} is not what the Rust model derives; run `task generate` and commit the result",
        path.display()
    );
}
