//! The profile format: the file between a real dataset and every synthetic copy.
//!
//! The generator reads a profile as JSON the library hands it, never as YAML. The JSON Schema the
//! library validates against is derived from these types, so the two cannot disagree. Every section
//! refuses a key it does not declare. The library owns a profile's digest and passes it in.

mod bundle;
mod table;
mod values;

use std::borrow::Cow;

use schemars::JsonSchema;
use schemars::Schema;
use schemars::SchemaGenerator;
use schemars::json_schema;
use serde::Deserialize;
use serde::Deserializer;
use serde::de::Error;

pub use bundle::BundleKind;
pub use bundle::BundleProfile;
pub use bundle::BundleTable;
pub use bundle::Cardinality;
pub use bundle::ColumnRef;
pub use bundle::Fanout;
pub use bundle::Inherited;
pub use bundle::Relationship;
pub use bundle::TopShare;
pub use table::*;
pub use values::Duration;
pub use values::Quantiles;
pub use values::Rate;
pub use values::SignedDuration;

/// The schema-level metadata keys the generator writes into every Arrow schema it builds, so every
/// Parquet and Arrow file names its profile and its whole dataset identity. A scenario key is left
/// out when no scenario applies. Without its `doppeldata.` prefix, each identity key is the key the
/// dataset identity's canonical JSON holds it under, where an absent scenario is null.
pub mod provenance {
    /// The profile's `name`.
    pub const PROFILE_NAME: &str = "doppeldata.profile_name";
    /// The profile's digest, which the library computes. An identity key.
    pub const PROFILE_DIGEST: &str = "doppeldata.profile_digest";
    /// The scenario's name.
    pub const SCENARIO_NAME: &str = "doppeldata.scenario_name";
    /// The scenario's digest. An identity key.
    pub const SCENARIO_DIGEST: &str = "doppeldata.scenario_digest";
    /// The seed, in decimal. An identity key.
    pub const SEED: &str = "doppeldata.seed";
    /// The engine's stream version. An identity key.
    pub const STREAM_VERSION: &str = "doppeldata.stream_version";
    /// The rows generated in decimal, before any copies a scenario adds, which no one file of it
    /// holds. An identity key.
    pub const DATASET_ROWS: &str = "doppeldata.dataset_rows";
    /// The bundle table the file is of, null outside a bundle. An identity key.
    pub const TABLE: &str = "doppeldata.table";
    /// What a bundle's every table's row count was multiplied by, in decimal, null outside a bundle.
    /// An identity key.
    pub const SCALE: &str = "doppeldata.scale";
    /// The digest of the dataset's identity, a quick equality check beside the values it is taken over.
    pub const DATASET_DIGEST: &str = "doppeldata.dataset_digest";
    /// The digest of the columns a sample pins and their values, absent where none is pinned. An
    /// identity key, which an unpinned identity leaves out rather than writing as null.
    pub const OVERRIDES_DIGEST: &str = "doppeldata.overrides_digest";
    /// The columns a sample pins, as a sorted JSON list of names, beside `OVERRIDES_DIGEST`.
    pub const OVERRIDDEN_COLUMNS: &str = "doppeldata.overridden_columns";
    /// The whole profile as RFC 8785 canonical JSON. `PROFILE_DIGEST` is taken over it without the
    /// generated fields the digest leaves out. Only a write that embeds the profile writes it, and
    /// only into Parquet and Arrow IPC files.
    pub const PROFILE: &str = "doppeldata.profile";
    /// The scenario's canonical JSON, the bytes `SCENARIO_DIGEST` is taken over, as the manifest's
    /// `scenario.document` holds it. Only a write that embeds the profile under a scenario writes it.
    pub const SCENARIO: &str = "doppeldata.scenario";
}

/// A profile file: one table, or a bundle of tables and their relationships, as its `kind` names.
#[derive(Debug, Clone, PartialEq)]
pub enum Profile {
    Table(Box<TableProfile>),
    Bundle(Box<BundleProfile>),
}

/// The profile a file's `kind` names, read before the rest of the file.
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum ProfileKind {
    Table,
    Bundle,
}

/// Reads `kind`, then the whole object as the profile it names. `kind` is a field of each, so a
/// table reads the same alone and inside a bundle.
impl<'de> Deserialize<'de> for Profile {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let document = serde_json::Value::deserialize(deserializer)?;
        let kind = document.get("kind").cloned().ok_or_else(|| D::Error::missing_field("kind"))?;
        let read = match ProfileKind::deserialize(kind).map_err(D::Error::custom)? {
            ProfileKind::Table => TableProfile::deserialize(document).map(|table| Self::Table(Box::new(table))),
            ProfileKind::Bundle => BundleProfile::deserialize(document).map(|bundle| Self::Bundle(Box::new(bundle))),
        };
        read.map_err(D::Error::custom)
    }
}

impl JsonSchema for Profile {
    fn schema_name() -> Cow<'static, str> {
        "Profile".into()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "description": "A profile file: one table, or a bundle of tables and their relationships, as its `kind` names.",
            "oneOf": [generator.subschema_for::<TableProfile>(), generator.subschema_for::<BundleProfile>()],
        })
    }
}

/// The profile's JSON Schema (Draft 2020-12), as the library ships it.
pub fn json_schema() -> String {
    let mut schema = schemars::schema_for!(Profile);
    schema.insert("title".into(), "doppeldata profile".into());
    let mut text = serde_json::to_string_pretty(&schema).expect("a derived schema always serializes");
    text.push('\n');
    text
}
