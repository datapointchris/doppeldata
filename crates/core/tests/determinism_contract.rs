//! The determinism contract users regenerate datasets on: what names a dataset, and that its values
//! depend on nothing else.

use std::fs;
use std::path::Path;

use arrow_array::Array;
use arrow_array::RecordBatch;
use arrow_data::ArrayData;
use doppeldata_core::BLOCK_ROWS;
use doppeldata_core::DatasetIdentity;
use doppeldata_core::STREAM_VERSION;
use doppeldata_core::profile::Profile;
use rayon::prelude::*;
use sha2::Digest;
use sha2::Sha256;

const PROFILE_DIGEST: &str = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const SEED: u64 = 20_251_025;
const ROWS: u64 = 2 * BLOCK_ROWS + 4_321;

fn example_document(name: &str) -> serde_json::Value {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../library/src/doppeldata/examples")
        .join(format!("{name}.profile.yaml"));
    serde_saphyr::from_str(&fs::read_to_string(path).unwrap()).unwrap()
}

fn generate(document: &serde_json::Value, threads: usize) -> Vec<RecordBatch> {
    let profile: Profile = serde_json::from_value(document.clone()).unwrap();
    let plan = doppeldata_core::plan(&profile, PROFILE_DIGEST, SEED, ROWS).unwrap();
    let pool = rayon::ThreadPoolBuilder::new().num_threads(threads).build().unwrap();
    pool.install(|| {
        (0..plan.block_count())
            .into_par_iter()
            .map(|block| plan.generate_block(block).unwrap())
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

#[test]
fn values_depend_on_no_thread_count_and_no_other_column() {
    let mut hash = Sha256::new();
    for example in ["eight-columns", "orders"] {
        let document = example_document(example);
        let one_thread = generate(&document, 1);
        assert_eq!(one_thread, generate(&document, 16), "{example}: 16 threads changed a value");

        let mut with_ninth = document.clone();
        let ninth = serde_json::json!({
            "name": "ninth", "logical_type": "number", "null_rate": 0.1,
            "numeric": {"quantiles": {"p25": 1.0, "p50": 2.0, "p75": 4.0}, "extremes": {"low": 0.0, "high": 9.0}}
        });
        with_ninth["columns"].as_array_mut().unwrap().insert(0, ninth);
        for (alone, beside) in one_thread.iter().zip(generate(&with_ninth, 4)) {
            for field in alone.schema().fields() {
                let name = field.name();
                let column = beside.column_by_name(name).unwrap();
                assert_eq!(
                    alone.column_by_name(name).unwrap(),
                    column,
                    "{example}: a ninth column changed {name}"
                );
            }
        }

        for batch in &one_thread {
            for column in batch.columns() {
                hash_data(&mut hash, &column.to_data());
            }
        }
    }
    let values: String = hash.finalize().iter().map(|byte| format!("{byte:02x}")).collect();
    assert_eq!(
        (STREAM_VERSION, values.as_str()),
        (1, "6e097fa737c470e885b62aa4e7b321ab60bd35c5939c503875ba5db7802b8a65"),
        "the values changed; a change to any draw raises STREAM_VERSION, and the pair moves together"
    );
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
