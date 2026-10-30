use std::sync::Arc;

use arrow_array::RecordBatch;
use doppeldata_core::BLOCK_ROWS;
use doppeldata_core::Plan;
use doppeldata_core::format_settings::DelimitedSettings;
use doppeldata_core::format_settings::Output;
use doppeldata_core::format_settings::ParquetSettings;
use doppeldata_core::manifest::FileRows;
use doppeldata_core::manifest::LayoutSettings;
use doppeldata_core::manifest::Manifest;
use doppeldata_core::profile::Profile;
use doppeldata_writers::Destination;
use doppeldata_writers::RunOptions;
use doppeldata_writers::WriteOptions;
use object_store::ObjectStoreExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::json;

/// Columns of each kind a split boundary could disturb, with both kinds of text cell: `type_mix`
/// errors and missing-value tokens.
fn plan() -> Plan {
    let profile: Profile = serde_json::from_value(json!({
        "kind": "table",
        "name": "split_invariance",
        "columns": [
            {"name": "id", "physical_type": "int64", "numeric": {"sequence": {"start": 1, "step": 1}}},
            {"name": "amount", "physical_type": "decimal128(12, 2)",
             "numeric": {"quantiles": {"p10": 5.0, "p50": 20.0, "p90": 80.0}, "extremes": {"low": 0.5, "high": 900.0}}},
            {"name": "at", "physical_type": "timestamp[us, tz=UTC]",
             "temporal": {"extremes": {"low": "2024-01-01T00:00:00", "high": "2024-12-31T23:59:59", "granularity": "second"}}},
            {"name": "flag", "boolean": {"true_rate": 0.3}},
            {"name": "status", "string": {"top_values": [{"value": "shipped", "rate": 0.6}, {"value": "pending", "rate": 0.3}],
             "other_rate": 0.1, "shapes": [{"shape": "aaaaaaa", "rate": 1.0}]}},
            {"name": "count", "physical_type": "int64", "type_mix": {"integer": 0.9, "string": 0.06, "decimal": 0.04},
             "numeric": {"integral": true, "quantiles": {"p50": 5.0}, "extremes": {"low": 0.0, "high": 10.0}}},
            {"name": "score", "physical_type": "double", "null_rate": 0.1, "missing_tokens": {"NA": 0.02, "-": 0.01},
             "numeric": {"quantiles": {"p50": 1.0}, "extremes": {"low": 0.0, "high": 2.0}}},
        ],
    }))
    .unwrap();
    doppeldata_core::plan(&profile, &format!("sha256:{}", "0".repeat(64)), 7, 150_000).unwrap()
}

/// Every object `output` writes under `file_rows`, in file order.
fn objects(plan: &Plan, output: Output, file_rows: u64) -> Vec<bytes::Bytes> {
    let store = Arc::new(InMemory::new());
    let destination = Destination::new(store.clone(), Path::from("dataset"), "memory://dataset");
    let options = WriteOptions {
        output,
        layout: LayoutSettings {
            file_rows: FileRows::Fixed(file_rows),
            ..LayoutSettings::default()
        },
        ..WriteOptions::default()
    };
    let run = RunOptions {
        threads: 4,
        ..RunOptions::default()
    };
    let manifest: Manifest = doppeldata_writers::write(plan, &options, &run, &destination).unwrap();
    manifest
        .files
        .iter()
        .map(|file| {
            futures::executor::block_on(async { store.get(&Path::from(format!("dataset/{}", file.path))).await?.bytes().await }).unwrap()
        })
        .collect()
}

fn parquet_rows(files: Vec<bytes::Bytes>) -> RecordBatch {
    let batches: Vec<RecordBatch> = files
        .into_iter()
        .flat_map(|file| ParquetRecordBatchReaderBuilder::try_new(file).unwrap().build().unwrap())
        .collect::<Result<_, _>>()
        .unwrap();
    arrow_select::concat::concat_batches(&batches[0].schema(), &batches).unwrap()
}

/// The reproducibility a committed manifest stands on: the split chooses the files, never the values.
#[test]
fn a_dataset_holds_the_same_values_and_csv_bytes_under_any_file_split() {
    assert!(
        [40_000, 100_000].iter().all(|file_rows| file_rows % BLOCK_ROWS != 0),
        "both file sizes cut blocks mid-block"
    );
    let plan = plan();
    let parquet = |file_rows| objects(&plan, Output::Parquet(ParquetSettings::default()), file_rows);
    let (narrow, wide) = (parquet(40_000), parquet(100_000));
    assert_ne!(narrow.len(), wide.len());
    assert_eq!(parquet_rows(narrow), parquet_rows(wide));

    let csv = |file_rows| {
        let settings = DelimitedSettings {
            header: false,
            ..DelimitedSettings::default()
        };
        objects(&plan, Output::Csv(settings), file_rows).concat()
    };
    let narrow = csv(40_000);
    assert!(
        narrow.windows(4).any(|window| window == b",NA\n"),
        "the CSV prints the missing-value tokens"
    );
    assert_eq!(narrow, csv(100_000));
}
