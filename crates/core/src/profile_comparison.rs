//! The reports `data check` and `profile diff` write. The library measures every metric and change,
//! and these types are what the two reports' JSON Schemas are derived from.

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

/// The check report format version this engine publishes.
pub const CHECK_VERSION: u64 = 1;
/// The diff report format version this engine publishes.
pub const DIFF_VERSION: u64 = 1;

/// Generated data measured against the profile it was generated from, by profiling the data with
/// that profile's histogram edges and value lists frozen.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct CheckReport {
    #[schemars(extend("const" = CHECK_VERSION))]
    pub check_version: u64,
    /// The profile the data was generated from.
    pub profile: ReportedProfile,
    /// The scenario applied to the profile, or null for none.
    pub scenario: Option<ReportedScenario>,
    pub data: CheckedData,
    /// Whether every metric and every column change passed.
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
    #[schemars(extend("const" = DIFF_VERSION))]
    pub diff_version: u64,
    /// The earlier profile, which each metric measures the other against.
    pub a: ReportedProfile,
    pub b: ReportedProfile,
    /// Whether any metric failed or any field changed.
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
    /// `sha256:` and the hex SHA-256 of the scenario's RFC 8785 canonical JSON.
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

/// One metric's value for one column, or for the table where `column` is null.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MetricResult {
    /// The metric's name, such as `ks`, as `--thresholds` keys it.
    pub metric: String,
    pub column: Option<String>,
    pub value: f64,
    pub threshold: f64,
    /// Whether `value` passes at or below `threshold`, or at or above it.
    pub bound: Bound,
    pub passed: bool,
    /// The statistic the metric compares, as the source profile holds it: A's, in a diff.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<f64>,
    /// The same statistic in the profile of the data checked: B's, in a diff.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed: Option<f64>,
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
    pub source: Option<serde_json::Value>,
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
    pub column: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub at_least: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub at_most: Option<f64>,
    pub value: f64,
    pub passed: bool,
}

/// The check report's JSON Schema (Draft 2020-12), as the library ships it.
pub fn check_json_schema() -> String {
    titled(
        schemars::schema_for!(CheckReport),
        &format!("doppeldata check report v{CHECK_VERSION}"),
    )
}

/// The diff report's JSON Schema (Draft 2020-12), as the library ships it.
pub fn diff_json_schema() -> String {
    titled(
        schemars::schema_for!(DiffReport),
        &format!("doppeldata diff report v{DIFF_VERSION}"),
    )
}

fn titled(mut schema: schemars::Schema, title: &str) -> String {
    schema.insert("title".into(), title.into());
    let mut text = serde_json::to_string_pretty(&schema).expect("a derived schema always serializes");
    text.push('\n');
    text
}
