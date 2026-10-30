//! Profile format v1: the file between a real dataset and every synthetic copy.
//!
//! The generator reads a profile as JSON the library hands it, never as YAML. The JSON Schema the
//! library validates against is derived from these types, so the two cannot disagree. The library
//! owns a profile's digest and passes it in.

mod bundle;
mod table;
mod values;

pub use bundle::BundleProfile;
pub use bundle::BundleTable;
pub use bundle::Cardinality;
pub use bundle::Fanout;
pub use bundle::InlineTable;
pub use bundle::Relationship;
pub use table::*;
pub use values::Duration;
pub use values::FORMAT_VERSION;
pub use values::ProfileVersion;
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
    /// The format version the profile was read as.
    pub const PROFILE_VERSION: &str = "doppeldata.profile_version";
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
    /// The digest of the dataset's identity, a quick equality check beside the five values.
    pub const DATASET_DIGEST: &str = "doppeldata.dataset_digest";
    /// The whole profile as RFC 8785 canonical JSON. `PROFILE_DIGEST` is taken over it without the
    /// generated fields the digest leaves out. Only a write that embeds the profile writes it, and
    /// only into Parquet and Arrow IPC files.
    pub const PROFILE: &str = "doppeldata.profile";
    /// The scenario's canonical JSON, the bytes `SCENARIO_DIGEST` is taken over, as the manifest's
    /// `scenario.document` holds it. Only a write that embeds the profile under a scenario writes it.
    pub const SCENARIO: &str = "doppeldata.scenario";
}

/// A profile file: one table, or a bundle of tables and their relationships.
#[derive(Debug, Clone, PartialEq, serde::Deserialize, schemars::JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Profile {
    Table(Box<TableProfile>),
    Bundle(Box<BundleProfile>),
}

/// The profile's JSON Schema (Draft 2020-12), as the library ships it.
pub fn json_schema() -> String {
    let mut schema = schemars::schema_for!(Profile);
    schema.insert("title".into(), format!("doppeldata profile v{FORMAT_VERSION}").into());
    let mut text = serde_json::to_string_pretty(&schema).expect("a derived schema always serializes");
    text.push('\n');
    text
}
