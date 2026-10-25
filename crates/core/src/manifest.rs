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
use crate::scenario::FaultKind;

/// The manifest format version this engine writes and reads.
pub const MANIFEST_VERSION: u64 = 1;

/// The file name a complete dataset's manifest is written under, beside the data, once every object
/// is whole. Spark, Hive and Glue skip a file whose name starts with `_`, so it never reads as data.
pub const MANIFEST_FILE_NAME: &str = "_manifest.json";

/// The file name of a run's manifest until the run completes. A run claims its destination by
/// creating it before the first object, and a run that fails or is cancelled leaves its record
/// here, never under [`MANIFEST_FILE_NAME`].
pub const PARTIAL_MANIFEST_FILE_NAME: &str = "_manifest.partial.json";

/// The file a write under `malformed_rows` lists every fault it placed in, one JSON object a line,
/// beside the manifest. Its name starts with `_`, so a reader of the data skips it.
pub const FAULTS_FILE_NAME: &str = "_faults.ndjson";

/// One fault `malformed_rows` placed, as a line of `_faults.ndjson`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FaultRecord {
    /// The object's path under the destination, as `files` names it.
    pub file: String,
    /// The 1-based line the broken record starts on, a header counted. Null in a JSON array, and
    /// for a fault of the whole file.
    pub line: Option<u64>,
    /// The dataset row the record was generated as. Null for a copy, and for a fault of the whole
    /// file.
    pub row: Option<u64>,
    pub kind: FaultKind,
    /// The column the fault is in, or null where it breaks the record or the file.
    pub column: Option<String>,
    pub detail: serde_json::Map<String, serde_json::Value>,
}

/// The rows a file holds when neither the call nor the profile sets a size.
pub const DEFAULT_FILE_ROWS: u64 = 1_000_000;

/// The record of a generated dataset, which with its profile and scenario regenerates every file byte for byte.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Manifest {
    pub manifest_version: ManifestVersion,
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
    /// Every object written, in path order.
    pub files: Vec<ManifestFile>,
    /// The faults `_faults.ndjson` lists, counted by kind, or null for a write with no
    /// `malformed_rows`.
    #[serde(default)]
    pub faults: Option<BTreeMap<FaultKind, u64>>,
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
    /// scenario's patch.
    pub profile_digest: String,
    /// `sha256:` and the hex SHA-256 of the scenario's RFC 8785 canonical JSON, or null for none.
    /// The JSON is taken with each perturbation's `id` resolved, and with `description` and
    /// `expect` left out. A composition's is its composed document's: the patches merged in order,
    /// the perturbations concatenated, and the names joined with `+`.
    pub scenario_digest: Option<String>,
    /// The seed, in decimal.
    #[serde(with = "decimal")]
    #[schemars(with = "Decimal")]
    pub seed: u64,
    /// The engine's stream version.
    pub stream_version: u32,
}

impl From<&DatasetIdentity> for ManifestIdentity {
    fn from(identity: &DatasetIdentity) -> Self {
        Self {
            dataset_rows: identity.rows(),
            profile_digest: identity.profile_digest().to_string(),
            scenario_digest: identity.scenario_digest().map(String::from),
            seed: identity.seed(),
            stream_version: identity.stream_version(),
        }
    }
}

impl Manifest {
    /// A manifest naming `identity`'s dataset, with no files yet. The identity's values and digest
    /// are copied, never recomputed.
    pub fn new(identity: &DatasetIdentity, profile: ManifestProfile, output: Output, layout: LayoutSettings, destination: String) -> Self {
        Self {
            manifest_version: ManifestVersion,
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
    /// The profile format version it was read as.
    pub version: u64,
    /// The profile's path as the caller gave it, or null when it came from memory.
    pub path: Option<String>,
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

/// `manifest_version`, which must equal [`MANIFEST_VERSION`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u64", into = "u64")]
pub struct ManifestVersion;

impl TryFrom<u64> for ManifestVersion {
    type Error = String;

    fn try_from(version: u64) -> Result<Self, Self::Error> {
        if version == MANIFEST_VERSION {
            Ok(Self)
        } else {
            Err(format!(
                "manifest_version {version} is not {MANIFEST_VERSION}, the version this engine reads"
            ))
        }
    }
}

impl From<ManifestVersion> for u64 {
    fn from(_: ManifestVersion) -> u64 {
        MANIFEST_VERSION
    }
}

impl JsonSchema for ManifestVersion {
    fn schema_name() -> Cow<'static, str> {
        "ManifestVersion".into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "description": "The manifest format version, which changes only on a breaking format change.",
            "const": MANIFEST_VERSION,
        })
    }
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
    schema.insert("title".into(), format!("doppeldata manifest v{MANIFEST_VERSION}").into());
    let mut text = serde_json::to_string_pretty(&schema).expect("a derived schema always serializes");
    text.push('\n');
    text
}
