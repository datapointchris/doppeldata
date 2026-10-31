use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt;

use schemars::JsonSchema;
use schemars::Schema;
use schemars::SchemaGenerator;
use schemars::json_schema;
use serde::Deserialize;

use super::table::Generated;
use super::table::TableProfile;
use super::values::Quantiles;
use super::values::Rate;

/// Several tables' profiles and the relationships between them.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BundleProfile {
    pub kind: BundleKind,
    pub name: String,
    pub generated: Option<Generated>,
    /// Each table by name: a profile file's path relative to the bundle, or the profile inline.
    pub tables: BTreeMap<String, BundleTable>,
    pub relationships: Option<Vec<Relationship>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[schemars(inline)]
pub enum BundleKind {
    Bundle,
}

/// A table of a bundle: its file's path relative to the bundle, or the table profile inline, as
/// its own file would hold it.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum BundleTable {
    File(String),
    Inline(Box<TableProfile>),
}

/// A foreign key between two tables, as shares and quantiles, never key values.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Relationship {
    /// Each `table.column` holding the reference, all in one table. A composite key names each of its
    /// columns, in the order `to` names the columns each refers to.
    #[schemars(length(min = 1))]
    pub from: Vec<ColumnRef>,
    /// Each `table.column` referred to, all in one table, one for each of `from`.
    #[schemars(length(min = 1))]
    pub to: Vec<ColumnRef>,
    /// The share of `from`'s distinct values that `to` holds.
    pub containment: Option<Rate>,
    /// The share of `from`'s rows holding a value that `to` does not hold, of those holding a value.
    pub orphan_rate: Option<Rate>,
    pub cardinality: Option<Cardinality>,
    pub fanout: Option<Fanout>,
    /// The child columns holding their parent row's value on nearly every joined row, such as the
    /// customer's `region` an order carries.
    pub inherited: Option<Vec<Inherited>>,
    /// The column of `from`'s table numbering each parent's child rows from 1 to their count, such as
    /// an order's line number.
    pub ordinal: Option<String>,
}

impl Relationship {
    /// The relationship as a report names it: `orders.customer_id → customers.customer_id`, each side's
    /// columns joined by `, `.
    pub fn display_name(&self) -> String {
        let joined = |side: &[ColumnRef]| side.iter().map(ColumnRef::to_string).collect::<Vec<_>>().join(", ");
        format!("{} → {}", joined(&self.from), joined(&self.to))
    }
}

/// A column of one of a bundle's tables, written `table.column`. The table runs to the first `.`, so
/// a column's name may hold one and a table's may not.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(try_from = "String")]
pub struct ColumnRef {
    pub table: String,
    pub column: String,
}

impl TryFrom<String> for ColumnRef {
    type Error = String;

    fn try_from(text: String) -> Result<Self, String> {
        match text.split_once('.') {
            Some((table, column)) if !table.is_empty() && !column.is_empty() => Ok(Self {
                table: table.into(),
                column: column.into(),
            }),
            _ => Err(format!("{text:?} is not table.column")),
        }
    }
}

impl fmt::Display for ColumnRef {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}.{}", self.table, self.column)
    }
}

impl JsonSchema for ColumnRef {
    fn schema_name() -> Cow<'static, str> {
        "ColumnRef".into()
    }

    fn inline_schema() -> bool {
        true
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "description": "A column of one of the bundle's tables, as table.column. The table runs to the first dot.",
            "type": "string",
            "pattern": r"^[^.]+\..+$",
        })
    }
}

/// A child column that copies a column of its parent row, as a share of joined rows, never a value.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Inherited {
    /// The column of `from`'s table holding the copy.
    pub child: String,
    /// The column of `to`'s table it copies.
    pub parent: String,
    /// The share of joined child rows whose value equals their parent row's, a null equal to a null.
    pub agreement: Rate,
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
#[serde(deny_unknown_fields)]
pub struct Fanout {
    /// The child rows each of `to`'s values is held by, counting the values no child row holds.
    pub quantiles: Option<Quantiles>,
    pub top_share: Option<TopShare>,
    /// The exponent of a Zipf law fitted by maximum likelihood to the parents' child counts, busiest first.
    pub zipf_s: Option<f64>,
}

/// The share of matched child rows the busiest parents hold together, at the rank each field names.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TopShare {
    /// The busiest parent's.
    pub top1: Option<Rate>,
    /// The busiest 10 parents'.
    pub top10: Option<Rate>,
    /// The busiest 100 parents'.
    pub top100: Option<Rate>,
}

impl TopShare {
    /// Each field's name and the rank, at scale 1, that the share is held at.
    pub const PINS: [(&'static str, u64); 3] = [("top1", 1), ("top10", 10), ("top100", 100)];

    /// Each stated share, by its field's name and rank, busiest first.
    pub fn pins(&self) -> Vec<(&'static str, u64, Rate)> {
        let shares = [self.top1, self.top10, self.top100];
        Self::PINS
            .into_iter()
            .zip(shares)
            .filter_map(|((name, rank), share)| share.map(|share| (name, rank, share)))
            .collect()
    }
}
