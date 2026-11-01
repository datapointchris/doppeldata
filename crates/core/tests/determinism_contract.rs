//! The determinism contract users regenerate datasets on: what names a dataset, and that its values
//! depend on nothing else.

use std::fs;
use std::path::Path;
use std::path::PathBuf;

use arrow_array::RecordBatch;
use doppeldata_core::AppliedScenario;
use doppeldata_core::BLOCK_ROWS;
use doppeldata_core::BundlePlan;
use doppeldata_core::DatasetIdentity;
use doppeldata_core::Plan;
use doppeldata_core::STREAM_VERSION;
use doppeldata_core::TextCell;
use doppeldata_core::bundle_values_sha256;
use doppeldata_core::plan_values_sha256;
use doppeldata_core::profile::Profile;
use doppeldata_core::scenario::Scenario;
use doppeldata_core::values_sha256;
use rayon::prelude::*;

// `crates/check-pinned-values-through-wheel.py` reads PROFILE_SHA256, SCENARIO_SHA256, SEED, ROWS,
// PINNED_STREAM_VERSION and every PINNED_*VALUES from this file, and generates each fixture through
// the built wheel, so each stays a literal on one line.
const PROFILE_SHA256: &str = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const SCENARIO_SHA256: &str = "sha256:fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210";
const SEED: u64 = 20_251_025;
const ROWS: u64 = 135_393;
const _: () = assert!(
    2 * BLOCK_ROWS < ROWS && ROWS < 3 * BLOCK_ROWS,
    "two whole blocks and part of a third"
);

/// The stream version the fixture's values were generated under, and a SHA-256 over them:
/// PINNED_VALUES without a scenario, PINNED_SCENARIO_VALUES with the scenario fixture, and
/// PINNED_CDC_VALUES with the cdc fixture, whose hash also covers every change file and the
/// expected table. PINNED_BUNDLE_VALUES covers every table of the bundle fixture at scale 1. A
/// release freezes the version and every hash beside it: once released, a change to any value
/// raises STREAM_VERSION. `crates/check-stream-version-raised.sh` reads these lines and refuses a
/// new hash at a released version.
const PINNED_STREAM_VERSION: u32 = 14;
const PINNED_VALUES: &str = "90f0c9e783f8e2993f49f50a155998e8e3f9eb42e426c6643dabc717d6756eec";
const PINNED_SCENARIO_VALUES: &str = "ed6d8de4b1c92406fbf48276896701e40c368dc0ac7713a514a1c4ea5bab0db6";
const PINNED_CDC_VALUES: &str = "74f943481dcbdcef07429f6ebcc46ce1f4c14fe022ac0f86ceac7c285e561f25";
const PINNED_BUNDLE_VALUES: &str = "a797deac6442bd7e9ccc1a8457bd053d18359acba3d458db62ff3c2b8fff40e3";

fn document(path: &Path) -> serde_json::Value {
    let text = fs::read_to_string(path).unwrap_or_else(|error| panic!("{}: {error}", path.display()));
    serde_saphyr::from_str(&text).unwrap_or_else(|error| panic!("{}: {error}", path.display()))
}

fn fixture() -> serde_json::Value {
    document(&Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/every-column-kind.profile.yaml"))
}

fn scenario_fixture() -> Scenario {
    scenario_at("tests/every-column-kind.scenario.yaml")
}

fn scenario_at(path: &str) -> Scenario {
    serde_json::from_value(document(&Path::new(env!("CARGO_MANIFEST_DIR")).join(path))).unwrap()
}

type Block = (RecordBatch, Vec<Vec<TextCell>>);

fn generate(document: &serde_json::Value, threads: usize) -> Vec<Block> {
    generate_with(document, None, threads)
}

fn generate_with(document: &serde_json::Value, scenario: Option<&Scenario>, threads: usize) -> Vec<Block> {
    generate_rows(document, scenario, SEED, ROWS, threads)
}

fn generate_rows(document: &serde_json::Value, scenario: Option<&Scenario>, seed: u64, rows: u64, threads: usize) -> Vec<Block> {
    blocks(&planned(document, scenario, seed, rows), threads)
}

fn planned(document: &serde_json::Value, scenario: Option<&Scenario>, seed: u64, rows: u64) -> Plan {
    let patched = scenario.map_or_else(|| document.clone(), |scenario| scenario.patched_profile(document).unwrap());
    let profile: Profile = serde_json::from_value(patched).unwrap();
    let applied = scenario.map(|scenario| AppliedScenario {
        scenario,
        sha256: SCENARIO_SHA256,
    });
    let inputs = doppeldata_core::PlanInputs {
        scenario: applied,
        ..Default::default()
    };
    doppeldata_core::plan_with(&profile, PROFILE_SHA256, seed, rows, inputs).unwrap()
}

fn blocks(plan: &Plan, threads: usize) -> Vec<Block> {
    let pool = rayon::ThreadPoolBuilder::new().num_threads(threads).build().unwrap();
    pool.install(|| {
        (0..plan.block_count())
            .into_par_iter()
            .map(|block| (plan.generate_block(block).unwrap(), plan.text_cells(block).unwrap()))
            .collect()
    })
}

fn pinned_hash(document: &serde_json::Value, scenario: Option<&Scenario>, seed: u64, rows: u64, threads: usize) -> String {
    let plan = planned(document, scenario, seed, rows);
    plan_values_sha256(&plan, &blocks(&plan, threads), threads).unwrap()
}

fn bundle_fixture() -> serde_json::Value {
    document(&Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/every-column-kind.bundle.yaml"))
}

fn bundle_planned(document: &serde_json::Value, seed: u64) -> BundlePlan {
    let Profile::Bundle(bundle) = serde_json::from_value(document.clone()).unwrap() else {
        panic!("the fixture is a bundle")
    };
    doppeldata_core::plan_bundle(&bundle, PROFILE_SHA256, None, seed, 1.0).unwrap()
}

/// Every table's blocks in block order, drawn last block first where `reversed` asks.
fn bundle_blocks(plan: &BundlePlan, threads: usize, reversed: bool) -> Vec<Vec<Block>> {
    let pool = rayon::ThreadPoolBuilder::new().num_threads(threads).build().unwrap();
    plan.tables()
        .map(|(_, table)| {
            let mut order: Vec<u64> = (0..table.block_count()).collect();
            if reversed {
                order.reverse();
            }
            let mut drawn: Vec<(u64, Block)> = pool.install(|| {
                order
                    .into_par_iter()
                    .map(|block| (block, (table.generate_block(block).unwrap(), table.text_cells(block).unwrap())))
                    .collect()
            });
            drawn.sort_by_key(|(block, _)| *block);
            drawn.into_iter().map(|(_, block)| block).collect()
        })
        .collect()
}

#[test]
fn a_bundle_values_depend_on_no_thread_count_and_no_block_order() {
    let plan = bundle_planned(&bundle_fixture(), SEED);
    let one_thread = bundle_blocks(&plan, 1, false);
    assert!(one_thread == bundle_blocks(&plan, 16, false), "16 threads changed a value");
    assert!(
        one_thread == bundle_blocks(&plan, 1, true),
        "drawing the last block first changed a value"
    );
    assert_eq!(
        (STREAM_VERSION, bundle_values_sha256(&one_thread).as_str()),
        (PINNED_STREAM_VERSION, PINNED_BUNDLE_VALUES),
        "the bundle's values changed; a change to a released stream version's draws raises STREAM_VERSION"
    );
}

#[test]
fn values_depend_on_no_thread_count_and_no_other_column() {
    let document = fixture();
    let one_thread = generate(&document, 1);
    assert!(one_thread == generate(&document, 16), "16 threads changed a value");

    let mut with_another = document.clone();
    let another = serde_json::json!({
        "name": "another", "logical_type": "number", "null_fraction": 0.1,
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
        (STREAM_VERSION, values_sha256(&one_thread).as_str()),
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
        (STREAM_VERSION, values_sha256(&one_thread).as_str()),
        (PINNED_STREAM_VERSION, PINNED_SCENARIO_VALUES),
        "the scenario's values changed; a change to a released stream version's draws raises STREAM_VERSION"
    );
}

#[test]
fn a_change_stream_depends_on_no_thread_count() {
    let document = fixture();
    let scenario = scenario_at("tests/every-column-kind.cdc.scenario.yaml");
    let one_thread = pinned_hash(&document, Some(&scenario), SEED, ROWS, 1);
    assert_eq!(
        one_thread,
        pinned_hash(&document, Some(&scenario), SEED, ROWS, 16),
        "16 threads changed a change file or the expected table"
    );
    assert_eq!(
        (STREAM_VERSION, one_thread.as_str()),
        (PINNED_STREAM_VERSION, PINNED_CDC_VALUES),
        "the change stream changed; a change to a released stream version's draws raises STREAM_VERSION"
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
    let values = match profile["kind"].as_str() {
        Some("bundle") => bundle_values_sha256(&bundle_blocks(&bundle_planned(&profile, seed), 4, false)),
        _ => pinned_hash(&profile, scenario.as_ref(), seed, rows, 4),
    };
    assert_eq!(
        values,
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
        .filter(|path| path.to_string_lossy().ends_with(".profile.yaml") || path.to_string_lossy().ends_with(".bundle.yaml"))
        .collect();
    assert!(!paths.is_empty(), "{} holds no examples", examples.display());
    for path in paths {
        let planned = match serde_json::from_value(document(&path)).unwrap() {
            Profile::Bundle(bundle) => doppeldata_core::plan_bundle(&bundle, PROFILE_SHA256, None, SEED, 1.0).map(|_| ()),
            profile => doppeldata_core::plan(&profile, PROFILE_SHA256, SEED, ROWS).map(|_| ()),
        };
        if let Err(error) = planned {
            panic!("{}: {error}", path.display());
        }
    }
}

#[test]
fn a_dataset_identity_hashes_to_its_pinned_sha256() {
    let identity = DatasetIdentity::new(PROFILE_SHA256, u64::MAX, 10_000_000);
    assert_eq!(
        identity.canonical_json(),
        concat!(
            r#"{"dataset_rows":"10000000","#,
            r#""profile_sha256":"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","#,
            r#""scale":null,"scenario_sha256":null,"seed":"18446744073709551615","stream_version":14,"table":null}"#
        )
    );
    assert_eq!(
        identity.sha256(),
        "sha256:3cd5a211a2b89749d70f68a742fd743221b8b3eb346a093a9c3dd52622142f72"
    );
    let with_scenario = identity.with_scenario(SCENARIO_SHA256);
    assert_eq!(
        with_scenario.canonical_json(),
        concat!(
            r#"{"dataset_rows":"10000000","#,
            r#""profile_sha256":"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","#,
            r#""scale":null,"#,
            r#""scenario_sha256":"sha256:fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210","#,
            r#""seed":"18446744073709551615","stream_version":14,"table":null}"#
        )
    );
    let in_bundle = DatasetIdentity::new(PROFILE_SHA256, u64::MAX, 10_000_000).in_bundle(0.1, Some("orders"));
    assert_eq!(
        in_bundle.canonical_json(),
        concat!(
            r#"{"dataset_rows":"10000000","#,
            r#""profile_sha256":"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","#,
            r#""scale":"0.1","scenario_sha256":null,"seed":"18446744073709551615","stream_version":14,"table":"orders"}"#
        )
    );
    assert_eq!(
        with_scenario.sha256(),
        "sha256:9775993aa4c16f2c98e3b34fc494c7e75cdf30851df736646e41626b5b1c0044"
    );
}
