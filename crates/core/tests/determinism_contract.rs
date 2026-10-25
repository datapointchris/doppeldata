//! The determinism contract users regenerate datasets on: what names a dataset, and that its values
//! depend on nothing else.

use std::fs;
use std::path::Path;
use std::path::PathBuf;

use arrow_array::RecordBatch;
use doppeldata_core::AppliedScenario;
use doppeldata_core::BLOCK_ROWS;
use doppeldata_core::DatasetIdentity;
use doppeldata_core::STREAM_VERSION;
use doppeldata_core::TextCell;
use doppeldata_core::profile::Profile;
use doppeldata_core::scenario::Scenario;
use doppeldata_core::values_digest;
use rayon::prelude::*;

// `crates/check-pinned-values-through-wheel.py` reads PROFILE_DIGEST, SCENARIO_DIGEST, SEED, ROWS,
// PINNED_STREAM_VERSION, PINNED_VALUES and PINNED_SCENARIO_VALUES from this file, and generates the
// fixture through the built wheel, so each stays a literal on one line.
const PROFILE_DIGEST: &str = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const SCENARIO_DIGEST: &str = "sha256:fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210";
const SEED: u64 = 20_251_025;
const ROWS: u64 = 135_393;
const _: () = assert!(
    2 * BLOCK_ROWS < ROWS && ROWS < 3 * BLOCK_ROWS,
    "two whole blocks and part of a third"
);

/// The stream version the fixture's values were generated under, and a SHA-256 over them, without a
/// scenario and with the scenario fixture. A release freezes all three: once released, a change to
/// any value raises STREAM_VERSION. `crates/check-stream-version-raised.sh` reads these lines and
/// refuses a new hash at a released version.
const PINNED_STREAM_VERSION: u32 = 7;
const PINNED_VALUES: &str = "12735e7dbf80220695779bfdad51845c4897e2a2fcf7ee8ab08f2f058a65b385";
const PINNED_SCENARIO_VALUES: &str = "8a40b3e492b186d3c55436d6c10b2045a171133ef566b6ad37a5b899709c18fa";

fn document(path: &Path) -> serde_json::Value {
    let text = fs::read_to_string(path).unwrap_or_else(|error| panic!("{}: {error}", path.display()));
    serde_saphyr::from_str(&text).unwrap_or_else(|error| panic!("{}: {error}", path.display()))
}

fn fixture() -> serde_json::Value {
    document(&Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/every-column-kind.profile.yaml"))
}

fn scenario_fixture() -> Scenario {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/every-column-kind.scenario.yaml");
    serde_json::from_value(document(&path)).unwrap()
}

type Block = (RecordBatch, Vec<Vec<TextCell>>);

fn generate(document: &serde_json::Value, threads: usize) -> Vec<Block> {
    generate_with(document, None, threads)
}

fn generate_with(document: &serde_json::Value, scenario: Option<&Scenario>, threads: usize) -> Vec<Block> {
    generate_rows(document, scenario, SEED, ROWS, threads)
}

fn generate_rows(document: &serde_json::Value, scenario: Option<&Scenario>, seed: u64, rows: u64, threads: usize) -> Vec<Block> {
    let patched = scenario.map_or_else(|| document.clone(), |scenario| scenario.patched_profile(document).unwrap());
    let profile: Profile = serde_json::from_value(patched).unwrap();
    let applied = scenario.map(|scenario| AppliedScenario {
        scenario,
        digest: SCENARIO_DIGEST,
    });
    let plan = doppeldata_core::plan_with_scenario(&profile, PROFILE_DIGEST, applied, seed, rows).unwrap();
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
fn a_scenario_values_depend_on_no_thread_count() {
    let document = fixture();
    let scenario = scenario_fixture();
    let one_thread = generate_with(&document, Some(&scenario), 1);
    assert!(
        one_thread == generate_with(&document, Some(&scenario), 16),
        "16 threads changed a value"
    );
    assert_eq!(
        (STREAM_VERSION, values_digest(&one_thread).as_str()),
        (PINNED_STREAM_VERSION, PINNED_SCENARIO_VALUES),
        "the scenario's values changed; a change to a released stream version's draws raises STREAM_VERSION"
    );
}

/// The fixtures of the last release, generated by this build. `crates/check-stream-version-raised.sh`
/// runs it when a fixture changed since that release at the same stream version, and names the
/// release's fixtures, seed, rows and pinned hash in the environment.
#[test]
#[ignore = "crates/check-stream-version-raised.sh runs it against a release's fixtures"]
fn a_released_fixture_generates_the_values_its_release_pinned() {
    let given = |name: &str| std::env::var(name).unwrap_or_else(|_| panic!("{name} is unset"));
    let profile = document(Path::new(&given("DOPPELDATA_RELEASED_PROFILE")));
    let scenario: Option<Scenario> = std::env::var("DOPPELDATA_RELEASED_SCENARIO")
        .ok()
        .filter(|path| !path.is_empty())
        .map(|path| serde_json::from_value(document(Path::new(&path))).unwrap());
    let seed = given("DOPPELDATA_RELEASED_SEED").parse().unwrap();
    let rows = given("DOPPELDATA_RELEASED_ROWS").parse().unwrap();
    assert_eq!(
        values_digest(&generate_rows(&profile, scenario.as_ref(), seed, rows, 4)),
        given("DOPPELDATA_RELEASED_VALUES"),
        "a released fixture's values changed at its stream version; a change to a released stream version's draws raises STREAM_VERSION"
    );
}

#[test]
fn every_bundled_example_plans() {
    let examples = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../library/src/doppeldata/examples");
    let paths: Vec<PathBuf> = fs::read_dir(&examples)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.to_string_lossy().ends_with(".profile.yaml"))
        .collect();
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
            r#""scenario_digest":null,"seed":"18446744073709551615","stream_version":7}"#
        )
    );
    assert_eq!(
        identity.digest(),
        "sha256:3fbdfbf82031470bc8e14a248d3d80ff37224337afb951a031a7233b7c4fd029"
    );
    let with_scenario = identity.with_scenario(SCENARIO_DIGEST);
    assert_eq!(
        with_scenario.canonical_json(),
        concat!(
            r#"{"dataset_rows":"10000000","#,
            r#""profile_digest":"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","#,
            r#""scenario_digest":"sha256:fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210","#,
            r#""seed":"18446744073709551615","stream_version":4}"#
        )
    );
    assert_eq!(
        with_scenario.digest(),
        "sha256:bd55826c0c6410bd412152c07fd50aa41b847bd9cd54283af45d5ff5e2474b92"
    );
}
