//! The record written beside a generated dataset. The writers fill it in. The engine owns its model,
//! so its JSON Schema is published with the profile's.

use std::borrow::Cow;
use std::collections::BTreeMap;

use schemars::JsonSchema;
use schemars::Schema;
use schemars::SchemaGenerator;
use schemars::json_schema;
use serde::Deserialize;
use serde::Serialize;

use crate::format_settings::Output;
use crate::identity::DatasetIdentity;
use crate::profile::PartitionStyle;
use crate::profile::Quantiles;
use crate::scenario::ListedFault;

/// The file name a complete dataset's manifest is written under, beside the data, once every object
/// is whole. Spark, Hive and Glue skip a file whose name starts with `_`, so it never reads as data.
pub const MANIFEST_FILE_NAME: &str = "_manifest.json";

/// The file name of a run's manifest until the run completes. A run claims its destination by
/// creating it before the first object, and a run that fails or is cancelled leaves its record
/// here, never under [`MANIFEST_FILE_NAME`].
pub const PARTIAL_MANIFEST_FILE_NAME: &str = "_manifest.partial.json";

/// The file a write under `malformed_rows` lists every fault it placed in, one JSON object a line,
/// beside the manifest. Its name starts with `_`, so Spark, Hive and Glue skip it. It ends in
/// `.jsonl`, which no format doppeldata writes ends in, so a DuckDB or Polars glob over `*.ndjson`
/// or `*.json` data leaves it out.
pub const FAULTS_FILE_NAME: &str = "_faults.jsonl";

/// The fault list a write to the one file `file_name` keeps beside it, such as
/// `_orders.csv.faults.jsonl`. It names the file, so several one-file writes share a directory.
pub fn one_file_faults_name(file_name: &str) -> String {
    format!("_{file_name}.faults.jsonl")
}

/// One fault `malformed_rows` placed, as a line of `_faults.jsonl`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct FaultRecord {
    /// The object's path under the destination, as the manifest's `files` names it, or its
    /// `cdc.files` for a fault in a change stream. Beside a one-file write, that file's name.
    pub file: String,
    /// The 1-based line the broken record starts on, a header counted. Null in a JSON array or a
    /// Parquet file, and for a fault of the whole file.
    pub line: Option<u64>,
    /// The dataset row the record was generated as, or in a change stream the row of its key, which
    /// lies past `dataset_rows` for a key the stream inserts. Null for a copy `duplicates` wrote,
    /// and for a fault of the whole file.
    pub row: Option<u64>,
    /// What broke: a kind `malformed_rows` names in `kinds`, or a fault `cdc` places in a change
    /// stream.
    pub kind: ListedFault,
    /// The column the fault is in, or null where it breaks the record or the file.
    pub column: Option<String>,
    /// What the fault wrote, under keys its `kind` sets, such as `text` for a `type_invalid`. Empty
    /// for a kind that writes nothing more than its name says, such as `bom`.
    pub detail: serde_json::Map<String, serde_json::Value>,
}

/// The JSON Schema (Draft 2020-12) of one line of a fault list, as the library ships it.
pub fn faults_json_schema() -> String {
    let mut schema = schemars::schema_for!(FaultRecord);
    schema.insert("title".into(), "doppeldata fault record".into());
    let mut text = serde_json::to_string_pretty(&schema).expect("a derived schema always serializes");
    text.push('\n');
    text
}

/// The rows a file holds when neither the call nor the profile sets a size.
pub const DEFAULT_FILE_ROWS: u64 = 1_000_000;

/// The `kind` of a [`Manifest`], which tells it from a [`BundleManifest`] under the same file name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum TableManifestKind {
    #[default]
    Table,
}

/// The `kind` of a [`BundleManifest`], which tells it from a [`Manifest`] under the same file name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum BundleManifestKind {
    #[default]
    Bundle,
}

/// The record of a generated dataset, which with its profile and scenario regenerates every file byte for byte.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Manifest {
    /// Always `table`: the manifest of one table's files.
    pub kind: TableManifestKind,
    /// The tool that wrote the files, as `doppeldata <version>`. The files' bytes are a function of
    /// the identity and this version.
    pub tool: String,
    /// `sha256:` and the hex SHA-256 of `identity`'s RFC 8785 canonical JSON.
    pub dataset_digest: String,
    pub identity: ManifestIdentity,
    pub profile: ManifestProfile,
    /// The scenario applied, or null for none.
    pub scenario: Option<ManifestScenario>,
    #[serde(flatten)]
    pub output: Output,
    pub layout: LayoutSettings,
    /// Where the files were written, as the caller named it, such as `s3://bucket/orders/`.
    pub destination: String,
    /// Every object of the rows written, in path order. Under `cdc` these are the full-load
    /// files, and `cdc` lists the change files and the expected table.
    pub files: Vec<ManifestFile>,
    /// The faults the write's fault list holds, counted by kind, or null for a write that lists
    /// none: one with no `malformed_rows`, and no `cdc` with a fault rate. A partial manifest counts
    /// the faults of the objects it lists.
    #[serde(default)]
    pub faults: Option<BTreeMap<ListedFault, u64>>,
    /// The change stream and the expected table a `cdc` write adds beside its full load, which
    /// `files` lists. Null for any other write.
    #[serde(default)]
    pub cdc: Option<ManifestCdc>,
    /// True when the run stopped early, so `files` lists only the objects that completed. Such a
    /// manifest is stored as `_manifest.partial.json`, and regenerates nothing.
    pub partial: bool,
    /// The threads that generated and encoded in each process that wrote. The count never changes
    /// a byte.
    pub threads: u32,
    /// The tasks a write split across processes divided its ranges among, such as a Spark job's,
    /// or null for a write in one process.
    #[serde(default)]
    pub tasks: Option<u32>,
    pub elapsed_ms: u64,
    /// When the write finished, in RFC 3339.
    pub written_at: String,
}

/// The values a dataset's rows are a pure function of, keyed and written as the identity's canonical
/// JSON, so a reader rebuilds `dataset_digest` from them with no mapping.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ManifestIdentity {
    /// The rows generated, in decimal, before any copies a scenario's `duplicates` adds. `files`
    /// gives the rows each file holds.
    #[serde(with = "decimal")]
    #[schemars(with = "Decimal")]
    pub dataset_rows: u64,
    /// `sha256:` and the hex SHA-256 of the profile's RFC 8785 canonical JSON, as given, before a
    /// scenario's patch. In a bundle, the bundle's, every table inline.
    pub profile_digest: String,
    /// The scale a bundle was generated at, in decimal, or null for a table generated alone.
    pub scale: Option<String>,
    /// `sha256:` and the hex SHA-256 of the scenario's RFC 8785 canonical JSON, or null for none.
    /// The JSON is taken with each perturbation's `id` resolved, and with `description` and
    /// `expect` left out. A composition's is its composed document's: the patches merged in order,
    /// the perturbations concatenated, and the names joined with `+`. In a bundle, the map of each
    /// table a scenario names to its composed document.
    pub scenario_digest: Option<String>,
    /// The seed, in decimal.
    #[serde(with = "decimal")]
    #[schemars(with = "Decimal")]
    pub seed: u64,
    /// The engine's stream version.
    pub stream_version: u32,
    /// The bundle table the rows are of, or null for a table generated alone and for a bundle's own
    /// manifest.
    pub table: Option<String>,
}

impl From<&DatasetIdentity> for ManifestIdentity {
    fn from(identity: &DatasetIdentity) -> Self {
        Self {
            dataset_rows: identity.rows(),
            profile_digest: identity.profile_digest().to_string(),
            scale: identity.scale().map(String::from),
            scenario_digest: identity.scenario_digest().map(String::from),
            seed: identity.seed(),
            stream_version: identity.stream_version(),
            table: identity.table().map(String::from),
        }
    }
}

/// The record written at a bundle's destination once every table is whole. Each table is written
/// under `<table>/` with a manifest of its own, which this names rather than holds.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct BundleManifest {
    /// Always `bundle`: the manifest naming each table's own.
    pub kind: BundleManifestKind,
    /// The tool that wrote the files, as `doppeldata <version>`.
    pub tool: String,
    /// `sha256:` and the hex SHA-256 of `identity`'s RFC 8785 canonical JSON.
    pub dataset_digest: String,
    /// The bundle's own identity: no table, and `dataset_rows` every table's rows summed.
    pub identity: ManifestIdentity,
    pub profile: ManifestProfile,
    /// The scenarios applied, or null for none.
    pub scenario: Option<ManifestScenario>,
    /// Where the bundle was written, as the caller named it.
    pub destination: String,
    /// Every table, each parent before its children.
    pub tables: Vec<BundleTable>,
    /// The pointer of each relationship and `inherited` entry the bundle states and does not draw,
    /// such as one whose columns another relationship with more of them draws.
    pub set_aside: Vec<String>,
    /// True until every table is whole. Such a manifest is stored as `_manifest.partial.json`.
    pub partial: bool,
    /// The threads that generated and encoded in each process that wrote.
    pub threads: u32,
    /// The tasks a write split across processes divided each table's ranges among, or null for a
    /// write in one process.
    #[serde(default)]
    pub tasks: Option<u32>,
    pub elapsed_ms: u64,
    /// When the write finished, in RFC 3339.
    pub written_at: String,
}

/// One table of a bundle's write.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct BundleTable {
    pub name: String,
    /// The table's manifest under the destination, `/`-separated, such as `orders/_manifest.json`.
    pub manifest: String,
    /// The table's manifest's `dataset_digest`.
    pub dataset_digest: String,
    /// The rows the table's identity names, in decimal.
    #[serde(with = "decimal")]
    #[schemars(with = "Decimal")]
    pub dataset_rows: u64,
    /// True until the table's own manifest is stored whole as `manifest`.
    pub partial: bool,
}

impl Manifest {
    /// A manifest naming `identity`'s dataset, with no files yet. The identity's values and digest
    /// are copied, never recomputed.
    pub fn new(identity: &DatasetIdentity, profile: ManifestProfile, output: Output, layout: LayoutSettings, destination: String) -> Self {
        Self {
            kind: TableManifestKind::Table,
            tool: format!("doppeldata {}", crate::VERSION),
            dataset_digest: identity.digest(),
            identity: ManifestIdentity::from(identity),
            profile,
            scenario: None,
            output,
            layout,
            destination,
            files: Vec::new(),
            faults: None,
            cdc: None,
            partial: false,
            threads: 0,
            tasks: None,
            elapsed_ms: 0,
            written_at: String::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ManifestProfile {
    /// The profile's `name`.
    pub name: String,
    /// The profile's path as the caller gave it, or null when it came from memory.
    pub path: Option<String>,
    /// Whether each file `files` and `cdc.expected` list holds the whole profile's canonical JSON,
    /// under the metadata key `doppeldata.profile`, and the scenario's document under
    /// `doppeldata.scenario` where one applies. A `cdc` change file never holds either. Left out
    /// when false.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub embedded: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ManifestScenario {
    /// The scenario's name. A composition's joins its members' names with `+`.
    pub name: String,
    /// The scenario's path as the caller gave it, or null when it came from memory or composes
    /// several.
    pub path: Option<String>,
    /// The canonical document `identity.scenario_digest` is taken over, so the dataset regenerates
    /// with no scenario file.
    pub document: serde_json::Map<String, serde_json::Value>,
    /// The scenarios composed, in order, present only for a composition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub composed_of: Option<Vec<ScenarioMember>>,
}

/// One scenario of a composition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ScenarioMember {
    pub name: String,
    /// The scenario's path as the caller gave it, or null when it came from memory.
    pub path: Option<String>,
    /// The scenario's own digest, taken as `identity.scenario_digest` is.
    pub digest: String,
}

/// One object the run wrote. A row or byte count is a JSON number, exact to 2^53 rows, which no
/// dataset reaches.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ManifestFile {
    /// The object's path under the destination, `/`-separated.
    pub path: String,
    /// The index of the row range the object holds, which its file name also carries.
    pub range: u32,
    /// The dataset row the range starts at.
    pub first_row: u64,
    /// The rows the range holds. A partitioned range splits them across one object per value.
    pub range_rows: u64,
    /// The rows in this object.
    pub rows: u64,
    pub bytes: u64,
    /// `sha256:` and the hex SHA-256 of the object's bytes.
    pub sha256: String,
    /// Each partition key's value in the object's path, null for a null value. Absent when the
    /// dataset is not partitioned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition: Option<BTreeMap<String, Option<String>>>,
}

/// What a `cdc` write adds beside its full load.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ManifestCdc {
    /// The layout the stream takes, as the op's `style` names it.
    pub style: String,
    /// Where the full load and the change files lie under the destination, `<schema>/<table>/`.
    pub prefix: String,
    /// Where the expected table lies under the destination, `_expected/<schema>/<table>/`.
    pub expected_prefix: String,
    /// The changes the stream holds, counted by operation, a fault's second delivery left out.
    pub changes: ChangeCounts,
    /// Every change file, in path order.
    pub files: Vec<StreamFile>,
    /// Every part of the table a correct merge of the stream produces, in path order.
    pub expected: Vec<StreamFile>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ChangeCounts {
    pub insert: u64,
    pub update: u64,
    pub delete: u64,
}

/// One object a `cdc` write stores beside its full load.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct StreamFile {
    /// The object's path under the destination, `/`-separated.
    pub path: String,
    pub rows: u64,
    pub bytes: u64,
    /// `sha256:` and the hex SHA-256 of the object's bytes.
    pub sha256: String,
}

/// How rows are split into files and directories.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct LayoutSettings {
    pub file_rows: FileRows,
    /// The columns whose values name each object's directories, outermost first. A partition
    /// column is left out of the files, because its value is in the path.
    pub partition_by: Vec<String>,
    pub partition_style: PartitionStyle,
    /// The values the partition keys take together, as the profile counts them. A range holds
    /// `file_rows` times this, so each of its objects lands near `file_rows` rows.
    pub partition_values: u64,
}

impl Default for LayoutSettings {
    fn default() -> Self {
        Self {
            file_rows: FileRows::Fixed(DEFAULT_FILE_ROWS),
            partition_by: Vec::new(),
            partition_style: PartitionStyle::Hive,
            partition_values: 1,
        }
    }
}

/// The rows each object holds, before a range is scaled by its partition values.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum FileRows {
    /// The same count in every range but the last.
    Fixed(u64),
    /// Each range's count drawn from these quantiles, from a stream keyed by the seed, so the
    /// split is the same on every run.
    Drawn(Quantiles),
}

/// A 64-bit unsigned integer written in decimal, because a JSON number is exact only to 2^53.
struct Decimal;

impl JsonSchema for Decimal {
    fn schema_name() -> Cow<'static, str> {
        "Decimal".into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "description": "A 64-bit unsigned integer in decimal, because a JSON number is exact only to 2^53.",
            "type": "string",
            "pattern": "^(0|[1-9][0-9]{0,19})$",
        })
    }
}

mod decimal {
    use serde::Deserialize;
    use serde::Deserializer;
    use serde::Serializer;

    pub fn serialize<S: Serializer>(value: &u64, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(value)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
        let text = String::deserialize(deserializer)?;
        text.parse()
            .map_err(|_| serde::de::Error::custom(format!("{text:?} is not a 64-bit unsigned integer in decimal")))
    }
}

/// The manifest's JSON Schema (Draft 2020-12), as the library ships it.
pub fn json_schema() -> String {
    let mut schema = schemars::schema_for!(Manifest);
    schema.insert("title".into(), "doppeldata manifest".into());
    let mut text = serde_json::to_string_pretty(&schema).expect("a derived schema always serializes");
    text.push('\n');
    text
}

/// A bundle manifest's JSON Schema (Draft 2020-12), as the library ships it.
pub fn bundle_json_schema() -> String {
    let mut schema = schemars::schema_for!(BundleManifest);
    schema.insert("title".into(), "doppeldata bundle manifest".into());
    let mut text = serde_json::to_string_pretty(&schema).expect("a derived schema always serializes");
    text.push('\n');
    text
}
