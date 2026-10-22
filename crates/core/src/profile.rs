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
pub use values::FORMAT_VERSION;
pub use values::ProfileVersion;
pub use values::Quantiles;
pub use values::Rate;

/// The schema-level metadata keys the generator is to write into every Parquet and Arrow file.
pub mod provenance {
    pub const PROFILE_NAME: &str = "doppeldata.profile_name";
    pub const PROFILE_VERSION: &str = "doppeldata.profile_version";
    pub const PROFILE_DIGEST: &str = "doppeldata.profile_digest";
    pub const SCENARIO_NAME: &str = "doppeldata.scenario_name";
    pub const SEED: &str = "doppeldata.seed";
}

/// A profile file: one table, or a bundle of tables and their relationships.
#[derive(Debug, Clone, PartialEq, serde::Deserialize, schemars::JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Profile {
    Table(Box<TableProfile>),
    Bundle(BundleProfile),
}

/// The profile's JSON Schema (Draft 2020-12), as the library ships it.
pub fn json_schema() -> String {
    let mut schema = schemars::schema_for!(Profile);
    schema.insert("title".into(), format!("doppeldata profile v{FORMAT_VERSION}").into());
    let mut text = serde_json::to_string_pretty(&schema).expect("a derived schema always serializes");
    text.push('\n');
    text
}
