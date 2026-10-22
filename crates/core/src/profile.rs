//! Profile format v1: the file between a real dataset and every synthetic copy.
//!
//! The engine reads a profile as the RFC 8785 canonical JSON Python sends it, never as YAML. The
//! JSON Schema Python validates against is derived from these types, so the two cannot disagree.

mod bundle;
mod table;
mod values;

use sha2::Digest;
use sha2::Sha256;

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

/// The schema-level metadata keys every Parquet and Arrow file the generator writes carries.
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
#[schemars(title = "doppeldata profile v1")]
pub enum Profile {
    Table(Box<TableProfile>),
    Bundle(BundleProfile),
}

/// Reads a profile from its canonical JSON and returns `sha256:<hex>` over those same bytes.
pub fn digest(canonical: &[u8]) -> Result<String, serde_json::Error> {
    serde_json::from_slice::<Profile>(canonical)?;
    let hash = Sha256::digest(canonical);
    let hex: String = hash.iter().map(|byte| format!("{byte:02x}")).collect();
    Ok(format!("sha256:{hex}"))
}

/// The JSON Schema (Draft 2020-12) committed as `library/src/doppeldata/schemas/profile.v1.json`.
pub fn json_schema() -> String {
    let schema = schemars::schema_for!(Profile);
    let mut text = serde_json::to_string_pretty(&schema).expect("a derived schema always serializes");
    text.push('\n');
    text
}
