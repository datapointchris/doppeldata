//! The dependence a profile states between columns, held by the rows generated from it.

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use arrow_array::types::Float64Type;
use arrow_schema::DataType;
use doppeldata_core::profile::Profile;
use rayon::prelude::*;

const PROFILE_SHA256: &str = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const ROWS: u64 = 100_000;
const CORRELATION_TOLERANCE: f64 = 0.01;
/// A repaired group holds its pairs as near as a valid matrix allows, within the bound `data check` sets by default.
const REPAIRED_TOLERANCE: f64 = 0.05;
const SHARE_TOLERANCE: f64 = 0.01;

fn generated(document: &serde_json::Value) -> Vec<RecordBatch> {
    let profile: Profile = serde_json::from_value(document.clone()).unwrap();
    let plan = doppeldata_core::plan(&profile, PROFILE_SHA256, 7, ROWS).unwrap();
    (0..plan.block_count())
        .into_par_iter()
        .map(|block| plan.generate_block(block).unwrap())
        .collect()
}

fn numbers(batches: &[RecordBatch], name: &str) -> Vec<Option<f64>> {
    batches
        .iter()
        .flat_map(|batch| {
            let column = arrow_cast::cast(batch.column_by_name(name).unwrap(), &DataType::Float64).unwrap();
            column.as_primitive::<Float64Type>().iter().collect::<Vec<_>>()
        })
        .collect()
}

fn texts(batches: &[RecordBatch], name: &str) -> Vec<Option<String>> {
    batches
        .iter()
        .flat_map(|batch| {
            let column = arrow_cast::cast(batch.column_by_name(name).unwrap(), &DataType::Utf8).unwrap();
            column
                .as_string::<i32>()
                .iter()
                .map(|value| value.map(str::to_owned))
                .collect::<Vec<_>>()
        })
        .collect()
}

/// Each value's rank, ties taking the mean of the ranks they span.
fn ranks(values: &[f64]) -> Vec<f64> {
    let mut order: Vec<usize> = (0..values.len()).collect();
    order.sort_by(|&a, &b| values[a].total_cmp(&values[b]));
    let mut ranks = vec![0.0; values.len()];
    let mut start = 0;
    while start < order.len() {
        let end = start + order[start..].iter().take_while(|&&at| values[at] == values[order[start]]).count();
        let rank = (start + end - 1) as f64 / 2.0;
        for &at in &order[start..end] {
            ranks[at] = rank;
        }
        start = end;
    }
    ranks
}

fn spearman(pairs: &[(f64, f64)]) -> f64 {
    let (a, b): (Vec<f64>, Vec<f64>) = pairs.iter().copied().unzip();
    let (a, b) = (ranks(&a), ranks(&b));
    let mean = (a.len() as f64 - 1.0) / 2.0;
    let (mut ab, mut aa, mut bb) = (0.0, 0.0, 0.0);
    for (x, y) in a.iter().zip(&b) {
        ab += (x - mean) * (y - mean);
        aa += (x - mean) * (x - mean);
        bb += (y - mean) * (y - mean);
    }
    ab / (aa * bb).sqrt()
}

#[test]
fn generated_rows_hold_each_stated_correlation_and_each_conditional_share() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/every-column-kind.profile.yaml");
    let document: serde_json::Value = serde_saphyr::from_str(&fs::read_to_string(path).unwrap()).unwrap();
    let batches = generated(&document);
    let dataset = &document["dataset"];

    let correlations = dataset["correlations"].as_array().unwrap();
    assert!(correlations.len() >= 3, "the fixture states a copula group of three");
    for stated in correlations {
        let (a, b) = (stated["a"].as_str().unwrap(), stated["b"].as_str().unwrap());
        let pairs: Vec<(f64, f64)> = numbers(&batches, a)
            .into_iter()
            .zip(numbers(&batches, b))
            .filter_map(|pair| match pair {
                (Some(x), Some(y)) => Some((x, y)),
                _ => None,
            })
            .collect();
        let (measured, value) = (spearman(&pairs), stated["value"].as_f64().unwrap());
        let tolerance = if a.starts_with("repaired_") {
            REPAIRED_TOLERANCE
        } else {
            CORRELATION_TOLERANCE
        };
        assert!(
            (measured - value).abs() < tolerance,
            "{a} x {b}: {measured:.4} against the stated {value}"
        );
    }

    for conditional in dataset["conditionals"].as_array().unwrap() {
        let (given, column) = (conditional["given"].as_str().unwrap(), conditional["column"].as_str().unwrap());
        let mut counts: HashMap<(String, String), f64> = HashMap::new();
        let mut totals: HashMap<String, f64> = HashMap::new();
        for (parent, child) in texts(&batches, given).into_iter().zip(texts(&batches, column)) {
            if let (Some(parent), Some(child)) = (parent, child) {
                *totals.entry(parent.clone()).or_default() += 1.0;
                *counts.entry((parent, child)).or_default() += 1.0;
            }
        }
        for (parent, shares) in conditional["fractions"].as_object().unwrap() {
            for (child, share) in shares.as_object().unwrap() {
                let measured = counts.get(&(parent.clone(), child.clone())).copied().unwrap_or(0.0) / totals[parent];
                let share = share.as_f64().unwrap();
                assert!(
                    (measured - share).abs() < SHARE_TOLERANCE,
                    "{column}={child} given {given}={parent}: {measured:.4} against the stated {share}"
                );
            }
        }
        if conditional.get("other_fractions").is_none() {
            // A given value with no row draws what the rows leave, so the child keeps its own shares, its rest's too.
            let entry = document["columns"]
                .as_array()
                .unwrap()
                .iter()
                .find(|entry| entry["name"] == column)
                .unwrap();
            let listed: Vec<(&str, f64)> = entry["string"]["top_values"]
                .as_array()
                .unwrap()
                .iter()
                .map(|top| (top["value"].as_str().unwrap(), top["fraction"].as_f64().unwrap()))
                .collect();
            let rest = 1.0 - listed.iter().map(|(_, share)| share).sum::<f64>();
            let rows = totals.values().sum::<f64>();
            for (child, share) in listed.iter().map(|&(value, share)| (Some(value), share)).chain([(None, rest)]) {
                let held = counts
                    .iter()
                    .filter(|((_, value), _)| child.map_or(listed.iter().all(|(top, _)| value != top), |child| value == child))
                    .map(|(_, count)| count)
                    .sum::<f64>();
                assert!(
                    (held / rows - share).abs() < SHARE_TOLERANCE,
                    "{column}={} over every {given}: {:.4} against its own {share}",
                    child.unwrap_or("its rest"),
                    held / rows
                );
            }
        }
    }
}
