//! The reports `data check` and `profile diff` write. The library measures every metric and change,
//! and these types are what the two reports' JSON Schemas are derived from.

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

use crate::profile::CorrelationMethod;

/// Generated data measured against the profile it was generated from, by profiling the data with
/// that profile's histogram edges and value lists frozen.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct CheckReport {
    /// The profile the data was generated from.
    pub profile: ReportedProfile,
    /// The scenario applied to the profile, or null for none.
    pub scenario: Option<ReportedScenario>,
    pub data: CheckedData,
    /// The real data the data was scored against, or null when none was named.
    #[serde(default)]
    pub against: Option<RealData>,
    /// Whether every metric that decides and every column change passed. A metric the profile
    /// states and the data cannot supply fails, because a check vouches for the data.
    pub passed: bool,
    /// Whether every declared expectation held. True when no scenario declares any.
    pub verified: bool,
    pub metrics: Vec<MetricResult>,
    /// Columns the data adds or drops, or holds as another kind or semantic type. A change a writer
    /// alone causes is listed and passes: an Arrow type, which a writer may store at another width
    /// or unit, and a decimal read back from text as a float. So does a semantic type no detector
    /// pack both profiles ran declares.
    pub changes: Vec<FieldChange>,
    /// The scenario's declared expectations, empty when no scenario applies.
    pub expectations: Vec<ExpectationResult>,
}

/// Two profiles of one table, such as last month's and this month's, and how far the second
/// drifted from the first.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct DiffReport {
    /// The earlier profile, which each metric measures the other against.
    pub a: ReportedProfile,
    pub b: ReportedProfile,
    /// Whether any metric failed or any field changed. A metric B cannot supply is listed and
    /// passes, because a diff reports what changed between two profiles, and B binned afresh
    /// carries no overflow.
    pub drifted: bool,
    pub metrics: Vec<MetricResult>,
    pub changes: Vec<FieldChange>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ReportedProfile {
    /// The profile's `name`.
    pub name: String,
    /// `sha256:` and the hex SHA-256 of the profile's RFC 8785 canonical JSON.
    pub digest: String,
    /// The profile's path as the caller gave it, or null when it came from memory.
    pub path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ReportedScenario {
    pub name: String,
    /// The scenario's digest, taken as a manifest's `identity.scenario_digest` is: the SHA-256 of
    /// its RFC 8785 canonical JSON with each perturbation's `id` resolved, and with `description`
    /// and `expect` left out.
    pub digest: String,
}

/// The data that was profiled and measured.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct CheckedData {
    /// The data as the caller named it, such as `s3://bucket/orders/`.
    pub path: String,
    /// The rows profiled.
    pub rows: u64,
    /// The profile's `source_fingerprint` of the files read, or null for an in-memory table.
    pub fingerprint: Option<String>,
}

/// The real data a check scored the data against. The report holds its scores and none of its
/// rows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RealData {
    /// The real data as the caller named it.
    pub path: String,
    /// The rows the real data holds.
    pub rows: u64,
    /// The `source_fingerprint` of the real data's files, or null for an in-memory table.
    pub fingerprint: Option<String>,
    /// The rows of each side's seeded sample that the scores read.
    pub scored_rows: u64,
    /// The coefficient `CorrelationSimilarity` compares, spelled as a profile's correlations spell it.
    pub correlation: CorrelationMethod,
}

/// One metric's value for one column, or for a pair where `paired_with` names the second, or for
/// the table where `column` is null. `metric`, `column` and `paired_with` together name the row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MetricResult {
    /// The metric's name, such as `ks` or `KSComplement`, as `--thresholds` keys it.
    pub metric: String,
    /// The column, or a pair's first column in the profile's order.
    pub column: Option<String>,
    /// A pair's second column, for a score of two columns.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paired_with: Option<String>,
    /// The value of `paired_with` the metric's value is worst within, for a metric of one column
    /// given another, such as `conditional_kl`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worst_given_value: Option<String>,
    /// The metric's value. It is null where `unmeasured` says why the other profile cannot supply
    /// it, and where `skipped_by` names the perturbation that moves it.
    pub value: Option<f64>,
    pub threshold: f64,
    /// Whether `value` passes at or below `threshold`, or at or above it.
    pub bound: Bound,
    /// Whether the row counts against the report: false only on a row that decides and whose
    /// value misses `threshold` or is unmeasured.
    pub passed: bool,
    /// False on a row the report's verdict leaves out: a score against the real data whose
    /// threshold no thresholds file sets, and `malformed_rate_delta` on data written with no
    /// `malformed_rows`. Such a row passes. A score's `threshold` is its reference, which `value`
    /// and `bound` say whether it meets.
    pub decides: bool,
    /// The statistic the metric compares, as the source profile holds it, or the real data for a
    /// score against it: A's, in a diff.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected: Option<f64>,
    /// The same statistic in the data checked: B's, in a diff.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed: Option<f64>,
    /// Why the metric has no value: the source profile states what it compares, and the other
    /// profile holds nothing to compare it with, as a column under `k_min` values holds no
    /// quantiles. Or the data was written so as not to hold it, as clean files hold none of the
    /// source's malformed rows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unmeasured: Option<String>,
    /// The id of the scenario's perturbation that moves what the metric compares, so the metric
    /// is not measured and passes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skipped_by: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum Bound {
    /// The value passes at or below the threshold.
    #[serde(rename = "at_most")]
    Ceiling,
    /// The value passes at or above the threshold.
    #[serde(rename = "at_least")]
    Floor,
}

/// A field that differs between the two profiles, outside what the metrics measure.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct FieldChange {
    pub change: ChangeKind,
    /// The column the change is in, or null for a table-level field.
    pub column: Option<String>,
    /// An RFC 6901 pointer to the field: in the source profile, or A, for a dropped column, and in
    /// the other profile for every other change.
    pub field: String,
    /// The field's value in the source profile, or A, and null where it holds none.
    pub expected: Option<serde_json::Value>,
    /// The field's value in the profile of the data checked, or B, and null where it holds none.
    pub observed: Option<serde_json::Value>,
    /// False when the change fails the check or counts as drift.
    pub passed: bool,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ChangeKind {
    ColumnAdded,
    ColumnDropped,
    /// The column kind the profiler read the column as, from its logical type and an Arrow
    /// decimal type.
    KindChanged,
    /// The Arrow type, such as `int32` becoming `int64`.
    PhysicalTypeChanged,
    SemanticTypeChanged,
    /// More rows than the diff allows fall outside the histogram edges or value list frozen from A.
    Overflow,
    CsvDialectChanged,
    PartitionKeyAdded,
}

/// A scenario's declared expectation and the value the data holds.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ExpectationResult {
    /// The statistic expected, such as `hot_keys.share`.
    pub metric: String,
    /// The column measured, or null for a metric of the whole table, such as `malformed.rate`.
    pub column: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub at_least: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub at_most: Option<f64>,
    /// The value the data holds, or null where `unmeasured` says why.
    pub value: Option<f64>,
    pub passed: bool,
    /// Why the data holds no value, such as a column it lacks. The expectation then fails.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unmeasured: Option<String>,
}

/// The check report's JSON Schema (Draft 2020-12), as the library ships it.
pub fn check_json_schema() -> String {
    titled(schemars::schema_for!(CheckReport), "doppeldata check report")
}

/// The diff report's JSON Schema (Draft 2020-12), as the library ships it.
pub fn diff_json_schema() -> String {
    titled(schemars::schema_for!(DiffReport), "doppeldata diff report")
}

fn titled(mut schema: schemars::Schema, title: &str) -> String {
    schema.insert("title".into(), title.into());
    let mut text = serde_json::to_string_pretty(&schema).expect("a derived schema always serializes");
    text.push('\n');
    text
}
