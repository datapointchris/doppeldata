use std::sync::Arc;

use doppeldata_core::format_settings::Output;
use doppeldata_core::format_settings::ParquetSettings;
use doppeldata_core::manifest::FileRows;
use doppeldata_core::manifest::LayoutSettings;
use doppeldata_core::manifest::Manifest;
use doppeldata_core::profile::Profile;
use doppeldata_writers::Destination;
use doppeldata_writers::RunOptions;
use doppeldata_writers::SplitRun;
use doppeldata_writers::WriteError;
use doppeldata_writers::WriteOptions;
use object_store::ObjectStoreExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use serde_json::json;

/// What a Spark job that lost a task leaves: a commit missing one range's records stores the
/// partial manifest under the claim's name, with every record it was given, and fails.
#[test]
fn a_commit_missing_a_range_stores_the_partial_manifest_and_fails() {
    let profile: Profile = serde_json::from_value(json!({
        "profile_version": 1,
        "kind": "table",
        "name": "split_write_commit",
        "columns": [{"name": "id", "physical_type": "int64", "numeric": {"sequence": {"start": 1, "step": 1}}}],
    }))
    .unwrap();
    let plan = doppeldata_core::plan(&profile, &format!("sha256:{}", "0".repeat(64)), 7, 150_000).unwrap();
    let store = Arc::new(InMemory::new());
    let destination = Destination::new(store.clone(), Path::from("dataset"), "memory://dataset");
    let options = WriteOptions {
        output: Output::Parquet(ParquetSettings::default()),
        layout: LayoutSettings {
            file_rows: FileRows::Fixed(40_000),
            ..LayoutSettings::default()
        },
        ..WriteOptions::default()
    };
    let run = RunOptions::default();
    doppeldata_writers::claim(&plan, &options, &run, &destination).unwrap();
    let files = doppeldata_writers::write_ranges(&plan, &options, &[0, 2, 3], &run, &destination).unwrap();
    let split = SplitRun {
        tasks: 2,
        threads: 1,
        elapsed_ms: 0,
    };

    let failure = doppeldata_writers::commit(&plan, &options, files.clone(), split, &run, &destination).unwrap_err();

    assert!(matches!(
        failure.error,
        WriteError::RangesUnrecorded {
            missing: 1,
            ranges: 4,
            first: 1
        }
    ));
    assert!(failure.manifest_stored);
    let stored = |name: &str| futures::executor::block_on(async { store.get(&Path::from(format!("dataset/{name}"))).await?.bytes().await });
    let partial: Manifest = serde_json::from_slice(&stored("_manifest.partial.json").unwrap()).unwrap();
    assert!(partial.partial);
    assert_eq!(partial.files, files.files);
    assert_eq!(partial.tasks, Some(2));
    assert!(stored("_manifest.json").is_err());
}
