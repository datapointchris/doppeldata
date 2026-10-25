//! The determinism contract users regenerate datasets on: what names a dataset, and that its values
//! depend on nothing else.

use std::fs;
use std::path::Path;
use std::path::PathBuf;

use arrow_array::RecordBatch;
use doppeldata_core::BLOCK_ROWS;
use doppeldata_core::DatasetIdentity;
use doppeldata_core::STREAM_VERSION;
use doppeldata_core::TextCell;
use doppeldata_core::profile::Profile;
use doppeldata_core::values_digest;
use rayon::prelude::*;

// `crates/check-pinned-values-through-wheel.py` reads PROFILE_DIGEST, SEED, ROWS,
// PINNED_STREAM_VERSION and PINNED_VALUES from this file, and generates the fixture through the
// built wheel, so each stays a literal on one line.
const PROFILE_DIGEST: &str = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const SEED: u64 = 20_251_025;
const ROWS: u64 = 135_393;
const _: () = assert!(
    2 * BLOCK_ROWS < ROWS && ROWS < 3 * BLOCK_ROWS,
    "two whole blocks and part of a third"
);

/// The stream version the fixture's values were generated under, and a SHA-256 over them. A release
/// freezes the pair: once released, a change to any value raises STREAM_VERSION.
/// `crates/check-stream-version-raised.sh` reads both lines and refuses a new hash at a released version.
const PINNED_STREAM_VERSION: u32 = 3;
const PINNED_VALUES: &str = "6adf255b8952320cbd5ec8a04810142ed9fb441754f8f78498aa9c35bf34428e";

fn document(path: &Path) -> serde_json::Value {
    let text = fs::read_to_string(path).unwrap_or_else(|error| panic!("{}: {error}", path.display()));
    serde_saphyr::from_str(&text).unwrap_or_else(|error| panic!("{}: {error}", path.display()))
}

fn fixture() -> serde_json::Value {
    document(&Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/every-column-kind.profile.yaml"))
}

type Block = (RecordBatch, Vec<Vec<TextCell>>);

fn generate(document: &serde_json::Value, threads: usize) -> Vec<Block> {
    let profile: Profile = serde_json::from_value(document.clone()).unwrap();
    let plan = doppeldata_core::plan(&profile, PROFILE_DIGEST, SEED, ROWS).unwrap();
    let pool = rayon::ThreadPoolBuilder::new().num_threads(threads).build().unwrap();
    pool.install(|| {
        (0..plan.block_count())
            .into_par_iter()
            .map(|block| (plan.generate_block(block).unwrap(), plan.text_cells(block).unwrap()))
            .collect()
    })
}

#[test]
fn values_depend_on_no_thread_count_and_no_other_column() {
    let document = fixture();
    let one_thread = generate(&document, 1);
    assert!(one_thread == generate(&document, 16), "16 threads changed a value");

    let mut with_another = document.clone();
    let another = serde_json::json!({
        "name": "another", "logical_type": "number", "null_rate": 0.1,
        "numeric": {"quantiles": {"p25": 1.0, "p50": 2.0, "p75": 4.0}, "extremes": {"low": 0.0, "high": 9.0}}
    });
    with_another["columns"].as_array_mut().unwrap().insert(0, another);
    for ((alone, alone_cells), (beside, beside_cells)) in one_thread.iter().zip(generate(&with_another, 4)) {
        for (index, field) in alone.schema().fields().iter().enumerate() {
            let name = field.name();
            let (beside_index, _) = beside.schema().column_with_name(name).unwrap();
            assert_eq!(alone.column(index), beside.column(beside_index), "another column changed {name}");
            assert_eq!(
                alone_cells[index], beside_cells[beside_index],
                "another column changed {name}'s text cells"
            );
        }
    }

    assert_eq!(
        (STREAM_VERSION, values_digest(&one_thread).as_str()),
        (PINNED_STREAM_VERSION, PINNED_VALUES),
        "the values changed; a change to a released stream version's draws raises STREAM_VERSION"
    );
}

#[test]
fn every_bundled_example_plans() {
    let examples = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../library/src/doppeldata/examples");
    let paths: Vec<PathBuf> = fs::read_dir(&examples).unwrap().map(|entry| entry.unwrap().path()).collect();
    assert!(!paths.is_empty(), "{} holds no examples", examples.display());
    for path in paths {
        let profile: Profile = serde_json::from_value(document(&path)).unwrap();
        if let Err(error) = doppeldata_core::plan(&profile, PROFILE_DIGEST, SEED, ROWS) {
            panic!("{}: {error}", path.display());
        }
    }
}

#[test]
fn a_dataset_identity_digests_to_its_pinned_value() {
    let identity = DatasetIdentity::new(PROFILE_DIGEST, u64::MAX, 10_000_000);
    assert_eq!(
        identity.canonical_json(),
        concat!(
            r#"{"dataset_rows":"10000000","#,
            r#""profile_digest":"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","#,
            r#""scenario_digest":null,"seed":"18446744073709551615","stream_version":3}"#
        )
    );
    assert_eq!(
        identity.digest(),
        "sha256:811d04b1b4fac4c77d1d093ea098d23fcdf23103d5e955e107474766a5c0ca48"
    );
}
