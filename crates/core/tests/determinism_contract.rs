//! The determinism contract users regenerate datasets on: what names a dataset, and that its values
//! depend on nothing else.

use std::fs;
use std::path::Path;
use std::path::PathBuf;

use arrow_array::Array;
use arrow_array::RecordBatch;
use arrow_data::ArrayData;
use doppeldata_core::BLOCK_ROWS;
use doppeldata_core::DatasetIdentity;
use doppeldata_core::STREAM_VERSION;
use doppeldata_core::TextCell;
use doppeldata_core::profile::Profile;
use rayon::prelude::*;
use sha2::Digest;
use sha2::Sha256;

const PROFILE_DIGEST: &str = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const SEED: u64 = 20_251_025;
const ROWS: u64 = 2 * BLOCK_ROWS + 4_321;

/// The stream version the fixture's values were generated under, and a SHA-256 over them. A release
/// freezes the pair: once released, a change to any value raises STREAM_VERSION.
const PINNED_STREAM_VERSION: u32 = 1;
const PINNED_VALUES: &str = "988ba6426e0945d9343ce4c88abc4d00368065127fae4560cc5a85f5b51551af";

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

fn hash_data(hash: &mut Sha256, data: &ArrayData) {
    hash.update(format!("{}:{}:{}", data.data_type(), data.len(), data.offset()));
    if let Some(nulls) = data.nulls() {
        for valid in nulls.iter() {
            hash.update([u8::from(valid)]);
        }
    }
    for buffer in data.buffers() {
        hash.update(buffer.as_slice());
    }
    for child in data.child_data() {
        hash_data(hash, child);
    }
}

fn hash_cells(hash: &mut Sha256, cells: &[TextCell]) {
    hash.update((cells.len() as u64).to_le_bytes());
    for cell in cells {
        hash.update(cell.row.to_le_bytes());
        hash.update((cell.text.len() as u64).to_le_bytes());
        hash.update(cell.text.as_bytes());
    }
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

    let mut hash = Sha256::new();
    for (batch, cells) in &one_thread {
        for (column, cells) in batch.columns().iter().zip(cells) {
            hash_data(&mut hash, &column.to_data());
            hash_cells(&mut hash, cells);
        }
    }
    let values: String = hash.finalize().iter().map(|byte| format!("{byte:02x}")).collect();
    assert_eq!(
        (STREAM_VERSION, values.as_str()),
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
            r#"{"profile_digest":"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","#,
            r#""rows":"10000000","scenario_digest":null,"seed":"18446744073709551615","stream_version":1}"#
        )
    );
    assert_eq!(
        identity.digest(),
        "sha256:a1c2c24ce89be7b5eefa875b2156a2329bb1319f395d5ba02fc25852809a9465"
    );
}
