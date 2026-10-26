use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::Deserialize;

use super::table::Generated;
use super::table::TableProfile;
use super::values::ProfileVersion;
use super::values::Quantiles;
use super::values::Rate;

/// Several tables' profiles and the relationships between them.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
pub struct BundleProfile {
    pub profile_version: ProfileVersion,
    pub name: String,
    pub generated: Option<Generated>,
    /// Each table by name: a profile file's path relative to the bundle, or the profile inline.
    pub tables: BTreeMap<String, BundleTable>,
    pub relationships: Option<Vec<Relationship>>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum BundleTable {
    File(String),
    Inline(Box<InlineTable>),
}

/// A table profile written inside a bundle, as its own file would hold it.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
pub struct InlineTable {
    pub kind: TableKind,
    #[serde(flatten)]
    pub table: TableProfile,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TableKind {
    Table,
}

/// A foreign key between two tables, as shares and quantiles, never key values.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
pub struct Relationship {
    /// `table.column` holding the reference.
    #[schemars(regex(pattern = r"^[^.]+\..+$"))]
    pub from: String,
    /// `table.column` referred to.
    #[schemars(regex(pattern = r"^[^.]+\..+$"))]
    pub to: String,
    /// The share of `from`'s distinct values that `to` holds.
    pub containment: Option<Rate>,
    /// The share of `from`'s rows holding a value that `to` does not hold, of those holding a value.
    pub orphan_rate: Option<Rate>,
    pub cardinality: Option<Cardinality>,
    pub fanout: Option<Fanout>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Cardinality {
    OneToOne,
    OneToMany,
    ManyToOne,
    ManyToMany,
}

/// Children per parent: the join skew a Spark comparison needs.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
pub struct Fanout {
    /// The child rows each of `to`'s values is held by, counting the values no child row holds.
    pub quantiles: Option<Quantiles>,
    /// The share of child rows the busiest parents hold: `top1`, `top10`, `top100`.
    pub top_share: Option<BTreeMap<String, Rate>>,
    /// The exponent of a Zipf law fitted by maximum likelihood to the parents' child counts, busiest first.
    pub zipf_s: Option<f64>,
}
