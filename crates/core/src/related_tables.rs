//! A bundle's tables planned parent first. Each foreign key is drawn from its parent's keys by the
//! relationship's profiled fan-out, and each inherited column copies the row its key matched.
//!
//! A child row's key is O(1) in the row: a keyed permutation places the row among the child's
//! virtual positions, nulls first, then the matched slots laid out busiest parent first, then the
//! orphans. A matched slot's rank comes from the ranked counts, and the rank's parent row from a
//! second permutation. So a block needs nothing from any other block, and no pass over the data.

mod fanout_counts;
mod keyed_permutation;
mod parent_blocks;

use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::Array;
use arrow_array::ArrayRef;
use arrow_array::cast::AsArray;
use arrow_buffer::BooleanBuffer;
use arrow_cast::can_cast_types;
use arrow_cast::cast;
use arrow_schema::DataType;
use arrow_select::interleave::interleave;
use serde::Serialize;

use self::fanout_counts::RankedCounts;
use self::keyed_permutation::KeyedPermutation;
use crate::identity::DatasetIdentity;
use crate::identity::is_sha256_digest;
use crate::plan::Plan;
use crate::plan::PlanError;
use crate::plan::plan_table;
use crate::profile::BundleProfile;
use crate::profile::BundleTable;
use crate::profile::Column;
use crate::profile::ColumnRef;
use crate::profile::Relationship;
use crate::profile::TableProfile;
use crate::rng::BLOCK_ROWS;
use crate::rng::Bernoulli;
use crate::rng::rng_for;
use crate::rng::stream_key;
use crate::samplers::BlockPosition;
use crate::samplers::ColumnPlan;
use crate::samplers::KeyColumn;
use crate::samplers::KeyValue;
use crate::samplers::RowKey;
use crate::scenario::Scenario;

/// Why a bundle's relationships cannot be generated, as a code a caller can branch on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationshipFault {
    /// A table given by its file's path, which the library writes inline before the engine reads it.
    TableNotInline,
    /// A table recording no row count for the scale to multiply.
    NoRowCount,
    /// A relationship naming a table the bundle lacks, or a column its table lacks.
    UnknownColumn,
    /// `from` and `to` of different lengths, or one side naming columns of several tables.
    MismatchedColumns,
    /// A relationship from a table to itself, which is not generated yet.
    SelfReference,
    /// Tables referring to each other in a loop, so none can be drawn first.
    Cycle,
    /// A key a child refers to that no row can be asked for: not a sequence, a uuid, an integer or text.
    ParentKeyUnaddressable,
    /// A child column whose type cannot hold the values it takes from its parent.
    IncompatibleTypes,
    /// Fan-out quantiles that fall, or top shares no counts can hold.
    InvalidFanout,
    /// A scale that is not a positive finite number.
    InvalidScale,
}

impl RelationshipFault {
    pub fn code(self) -> &'static str {
        match self {
            Self::TableNotInline => "table-not-inline",
            Self::NoRowCount => "no-row-count",
            Self::UnknownColumn => "unknown-column",
            Self::MismatchedColumns => "mismatched-columns",
            Self::SelfReference => "self-reference",
            Self::Cycle => "relationship-cycle",
            Self::ParentKeyUnaddressable => "parent-key-unaddressable",
            Self::IncompatibleTypes => "incompatible-types",
            Self::InvalidFanout => "invalid-fanout",
            Self::InvalidScale => "invalid-scale",
        }
    }
}

impl std::fmt::Display for RelationshipFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.code())
    }
}

fn refuse(code: RelationshipFault, field: impl Into<String>, message: impl Into<String>) -> PlanError {
    PlanError::Relationship {
        code,
        field: field.into(),
        message: message.into(),
    }
}

/// The scenarios a bundle is generated under: each bundle table's, and the digest the library takes
/// over the map of table to scenario document.
#[derive(Debug, Clone, Copy)]
pub struct BundleScenarios<'a> {
    pub digest: &'a str,
    pub tables: &'a BTreeMap<String, Scenario>,
}

/// One relationship as the plan holds it.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PlannedRelationship {
    /// `orders.customer_id → customers.customer_id`.
    pub name: String,
    /// Its JSON Pointer in the bundle.
    pub pointer: String,
    /// The table holding the foreign key, and its columns, in `from`'s order.
    pub child: String,
    pub child_columns: Vec<String>,
    /// The table referred to, and the key's columns, in `to`'s order.
    pub parent: String,
    pub parent_columns: Vec<String>,
    /// What draws its columns instead, where another relationship does.
    pub set_aside: Option<String>,
    /// The pointer of each `inherited` entry the plan does not copy, and of an `ordinal` it does not
    /// number, and why.
    pub entries_set_aside: Vec<(String, String)>,
    /// Each child column the plan copies from the parent row, where it draws the relationship.
    pub copied: Vec<String>,
    /// The child column the plan numbers, where it draws the relationship.
    pub ordinal: Option<String>,
}

/// A bundle resolved for generation: a plan for each table, every parent before its children.
#[derive(Debug)]
pub struct BundlePlan {
    identity: DatasetIdentity,
    tables: Vec<(String, Arc<Plan>)>,
    relationships: Vec<PlannedRelationship>,
}

impl BundlePlan {
    /// The bundle's own identity: no table, and every table's rows summed.
    pub fn identity(&self) -> &DatasetIdentity {
        &self.identity
    }

    /// Each table's name and plan, every parent before its children.
    pub fn tables(&self) -> impl Iterator<Item = (&str, &Arc<Plan>)> {
        self.tables.iter().map(|(name, plan)| (name.as_str(), plan))
    }

    pub fn table(&self, name: &str) -> Option<&Arc<Plan>> {
        self.tables.iter().find(|(held, _)| held == name).map(|(_, plan)| plan)
    }

    pub fn relationships(&self) -> &[PlannedRelationship] {
        &self.relationships
    }

    /// The pointer of each relationship, `inherited` entry and `ordinal` the bundle states and the
    /// plan does not draw.
    pub fn set_aside(&self) -> Vec<String> {
        let mut pointers = Vec::new();
        for relationship in &self.relationships {
            if relationship.set_aside.is_some() {
                pointers.push(relationship.pointer.clone());
            }
            pointers.extend(relationship.entries_set_aside.iter().map(|(pointer, _)| pointer.clone()));
        }
        pointers
    }
}

/// A relationship's ends, each side's table and the indexes of its columns there.
struct Ends<'a> {
    child: &'a str,
    child_columns: Vec<usize>,
    parent: &'a str,
    parent_columns: Vec<usize>,
}

fn ends<'a>(tables: &BTreeMap<&'a str, &'a TableProfile>, index: usize, relationship: &Relationship) -> Result<Ends<'a>, PlanError> {
    let at = format!("/relationships/{index}");
    if relationship.from.len() != relationship.to.len() || relationship.from.is_empty() {
        return Err(refuse(
            RelationshipFault::MismatchedColumns,
            &at,
            "from and to name as many columns as each other, at least one",
        ));
    }
    let side = |names: &[ColumnRef], side: &str| -> Result<(&'a str, Vec<usize>), PlanError> {
        let mut table_name: Option<&'a str> = None;
        let mut columns = Vec::with_capacity(names.len());
        for (place, ColumnRef { table, column }) in names.iter().enumerate() {
            let field = format!("{at}/{side}/{place}");
            let Some((&name, profile)) = tables.get_key_value(table.as_str()) else {
                return Err(refuse(
                    RelationshipFault::UnknownColumn,
                    field,
                    format!("the bundle holds no table {table:?}"),
                ));
            };
            if table_name.is_some_and(|held| held != name) {
                return Err(refuse(
                    RelationshipFault::MismatchedColumns,
                    field,
                    format!("every column of {side} is of one table"),
                ));
            }
            table_name = Some(name);
            let found = profile.columns.iter().flatten().position(|held| held.name == *column);
            let Some(found) = found else {
                return Err(refuse(
                    RelationshipFault::UnknownColumn,
                    field,
                    format!("{table} holds no column {column:?}"),
                ));
            };
            columns.push(found);
        }
        Ok((table_name.expect("a side names a column"), columns))
    };
    let (child, child_columns) = side(&relationship.from, "from")?;
    let (parent, parent_columns) = side(&relationship.to, "to")?;
    if child == parent {
        return Err(refuse(
            RelationshipFault::SelfReference,
            at,
            format!("{child} refers to itself, which is not generated yet"),
        ));
    }
    Ok(Ends {
        child,
        child_columns,
        parent,
        parent_columns,
    })
}

/// What one table's plan draws from the bundle: the keys its children read, the relationships drawing
/// its foreign keys, and the columns it copies from its parents.
pub(crate) struct TableRelations<'a> {
    /// Each column a child refers to.
    pub(crate) addressed: BTreeSet<usize>,
    pub(crate) draws: Vec<DrawSpec<'a>>,
    pub(crate) inherited: Vec<InheritSpec<'a>>,
}

pub(crate) struct DrawSpec<'a> {
    pointer: String,
    relationship: &'a Relationship,
    columns: Vec<usize>,
    parent: Arc<Plan>,
    parent_name: &'a str,
    parent_columns: Vec<usize>,
    /// The child's files are sorted by this key first, so its rows are left in key order.
    sorted: bool,
    /// The bundle's scale, which places each `top_share` pin at the same fraction of the parents.
    scale: f64,
    /// The child column numbering each key's rows.
    ordinal: Option<usize>,
}

pub(crate) struct InheritSpec<'a> {
    /// The place of the relationship among the table's draws.
    draw: usize,
    child: usize,
    parent: usize,
    parent_name: &'a str,
    parent_column: &'a str,
    agreement: f64,
    /// The share of matched rows that copy their parent's value, so the rest, drawn as the child
    /// column's own profile says, agree by chance often enough to make `agreement`.
    copy: f64,
}

/// How often a child row drawn from its column's own profile equals its parent's, which `parent`'s
/// profile draws: across the string values both list, and a null meeting a null.
fn chance_agreement(child: &Column, parent: &Column) -> f64 {
    let nulls = child.null_rate.map_or(0.0, |rate| rate.0) * parent.null_rate.map_or(0.0, |rate| rate.0);
    let listed = |column: &Column| -> BTreeMap<String, f64> {
        let values = column.string.as_ref().and_then(|string| string.top_values.as_ref());
        values.into_iter().flatten().map(|top| (top.value.clone(), top.rate.0)).collect()
    };
    let parent_values = listed(parent);
    let values: f64 = listed(child)
        .iter()
        .filter_map(|(value, rate)| parent_values.get(value).map(|held| rate * held))
        .sum();
    (nulls + values).clamp(0.0, 1.0)
}

/// The copy share that, with `chance` agreement on the rows not copied, agrees on `agreement`.
fn copy_rate(agreement: f64, chance: f64) -> f64 {
    if chance >= 1.0 || agreement <= chance {
        return 0.0;
    }
    ((agreement - chance) / (1.0 - chance)).clamp(0.0, 1.0)
}

/// Resolves `bundle` into a plan for each table at `scale` times its row count, from `seed`. The
/// library computes the bundle's digest over every table as resolved, and the scenarios' digest.
pub fn plan_bundle(
    bundle: &BundleProfile,
    bundle_digest: &str,
    scenarios: Option<BundleScenarios<'_>>,
    seed: u64,
    scale: f64,
) -> Result<BundlePlan, PlanError> {
    if !is_sha256_digest(bundle_digest) {
        return Err(PlanError::ProfileDigest {
            digest: bundle_digest.into(),
        });
    }
    if !scale.is_finite() || scale <= 0.0 {
        return Err(refuse(
            RelationshipFault::InvalidScale,
            "",
            format!("scale {scale} is not above zero"),
        ));
    }
    let mut tables: BTreeMap<&str, &TableProfile> = BTreeMap::new();
    let mut rows: BTreeMap<&str, u64> = BTreeMap::new();
    for (name, table) in &bundle.tables {
        let BundleTable::Inline(table) = table else {
            return Err(refuse(
                RelationshipFault::TableNotInline,
                format!("/tables/{}", crate::perturbations::pointer_token(name)),
                "the engine reads every table inline",
            ));
        };
        let Some(row_count) = table.dataset.as_ref().and_then(|dataset| dataset.row_count) else {
            return Err(refuse(
                RelationshipFault::NoRowCount,
                format!("/tables/{}/dataset/row_count", crate::perturbations::pointer_token(name)),
                format!("{name} records no row count for the scale to multiply"),
            ));
        };
        tables.insert(name.as_str(), table.as_ref());
        rows.insert(name.as_str(), (row_count as f64 * scale).round() as u64);
    }
    if let Some(scenarios) = scenarios {
        if !is_sha256_digest(scenarios.digest) {
            return Err(PlanError::ScenarioDigest {
                digest: scenarios.digest.into(),
            });
        }
        if let Some(table) = scenarios.tables.keys().find(|table| !tables.contains_key(table.as_str())) {
            return Err(PlanError::Scenario {
                code: crate::ScenarioFault::UnknownTable,
                field: "/table".into(),
                message: format!("the bundle holds no table {table:?}"),
            });
        }
    }
    let stated = bundle.relationships.as_deref().unwrap_or_default();
    let resolved: Vec<Ends> = stated
        .iter()
        .enumerate()
        .map(|(index, relationship)| ends(&tables, index, relationship))
        .collect::<Result<_, _>>()?;
    let mut planned: Vec<PlannedRelationship> = stated
        .iter()
        .enumerate()
        .map(|(index, relationship)| {
            let columns = |side: &[ColumnRef]| side.iter().map(|reference| reference.column.clone()).collect();
            PlannedRelationship {
                name: relationship.display_name(),
                pointer: format!("/relationships/{index}"),
                child: resolved[index].child.to_string(),
                child_columns: columns(&relationship.from),
                parent: resolved[index].parent.to_string(),
                parent_columns: columns(&relationship.to),
                set_aside: None,
                entries_set_aside: Vec::new(),
                copied: Vec::new(),
                ordinal: None,
            }
        })
        .collect();
    let mut by_width: Vec<usize> = (0..resolved.len()).collect();
    by_width.sort_by_key(|&index| (Reverse(resolved[index].child_columns.len()), index));
    let mut drawn_by: HashMap<(&str, usize), usize> = HashMap::new();
    for &index in &by_width {
        let ends = &resolved[index];
        if let Some(&other) = ends.child_columns.iter().find_map(|&column| drawn_by.get(&(ends.child, column))) {
            planned[index].set_aside = Some(format!("{} draws its columns", planned[other].name));
            continue;
        }
        for &column in &ends.child_columns {
            drawn_by.insert((ends.child, column), index);
        }
    }
    let drawing: Vec<usize> = (0..resolved.len()).filter(|&index| planned[index].set_aside.is_none()).collect();
    let mut addressed: BTreeMap<&str, BTreeSet<usize>> = BTreeMap::new();
    for &index in &drawing {
        addressed
            .entry(resolved[index].parent)
            .or_default()
            .extend(&resolved[index].parent_columns);
    }
    let order = parent_first(&tables, &drawing, &resolved)?;
    let mut inherited_by: HashMap<(&str, usize), usize> = HashMap::new();
    let mut inherits: Vec<Vec<(usize, usize, f64, usize)>> = vec![Vec::new(); resolved.len()];
    for &index in &drawing {
        let (ends, relationship) = (&resolved[index], &stated[index]);
        for (place, entry) in relationship.inherited.iter().flatten().enumerate() {
            let pointer = format!("/relationships/{index}/inherited/{place}");
            let child_columns = tables[ends.child].columns.as_deref().unwrap_or_default();
            let parent_columns = tables[ends.parent].columns.as_deref().unwrap_or_default();
            let child = child_columns.iter().position(|column| column.name == entry.child);
            let parent = parent_columns.iter().position(|column| column.name == entry.parent);
            let why = match (child, parent) {
                (None, _) => Some(format!("{} holds no column {:?}", ends.child, entry.child)),
                (_, None) => Some(format!("{} holds no column {:?}", ends.parent, entry.parent)),
                (Some(child), Some(_)) if drawn_by.contains_key(&(ends.child, child)) => {
                    Some(format!("{} is a foreign key a relationship draws", entry.child))
                }
                (Some(child), Some(_)) if addressed.get(ends.child).is_some_and(|keys| keys.contains(&child)) => {
                    Some(format!("{} is a key a child refers to", entry.child))
                }
                (Some(child), Some(_)) => inherited_by
                    .get(&(ends.child, child))
                    .map(|&other| format!("{} copies it", planned[other].name)),
            };
            if let Some(why) = why {
                planned[index].entries_set_aside.push((pointer, why));
                continue;
            }
            let (child, parent) = (child.expect("found"), parent.expect("found"));
            inherited_by.insert((ends.child, child), index);
            inherits[index].push((child, parent, entry.agreement.0, place));
        }
    }
    let mut numbered_by: HashMap<(&str, usize), usize> = HashMap::new();
    let mut ordinals: Vec<Option<usize>> = vec![None; resolved.len()];
    for &index in &drawing {
        let Some(name) = stated[index].ordinal.as_deref() else {
            continue;
        };
        let ends = &resolved[index];
        let child_columns = tables[ends.child].columns.as_deref().unwrap_or_default();
        let found = child_columns.iter().position(|column| column.name == name);
        let why = match found {
            None => Some(format!("{} holds no column {name:?}", ends.child)),
            Some(column) if drawn_by.contains_key(&(ends.child, column)) => Some(format!("{name} is a foreign key a relationship draws")),
            Some(column) if addressed.get(ends.child).is_some_and(|keys| keys.contains(&column)) => {
                Some(format!("{name} is a key a child refers to"))
            }
            Some(column) => inherited_by
                .get(&(ends.child, column))
                .or_else(|| numbered_by.get(&(ends.child, column)))
                .map(|&other| format!("{} draws it", planned[other].name)),
        };
        if let Some(why) = why {
            planned[index]
                .entries_set_aside
                .push((format!("/relationships/{index}/ordinal"), why));
            continue;
        }
        let column = found.expect("found");
        numbered_by.insert((ends.child, column), index);
        ordinals[index] = Some(column);
    }
    let identity_of = |name: Option<&str>, rows: u64| {
        let identity = DatasetIdentity::new(bundle_digest, seed, rows).in_bundle(scale, name);
        match scenarios {
            Some(scenarios) => identity.with_scenario(scenarios.digest),
            None => identity,
        }
    };
    let mut plans: BTreeMap<&str, Arc<Plan>> = BTreeMap::new();
    for &name in &order {
        let table = tables[name];
        let mut draws = Vec::new();
        let mut inherited = Vec::new();
        for &index in drawing.iter().filter(|&&index| resolved[index].child == name) {
            let ends = &resolved[index];
            let sorted_by = table
                .source
                .as_ref()
                .and_then(|source| source.layout.as_ref())
                .and_then(|layout| layout.sort_order.as_ref())
                .and_then(|order| order.first());
            let first = table.columns.as_deref().unwrap_or_default()[ends.child_columns[0]].name.as_str();
            let parent_columns = tables[ends.parent].columns.as_deref().unwrap_or_default();
            let child_columns = table.columns.as_deref().unwrap_or_default();
            for &(child, parent, agreement, _) in &inherits[index] {
                inherited.push(InheritSpec {
                    draw: draws.len(),
                    child,
                    parent,
                    parent_name: ends.parent,
                    parent_column: parent_columns[parent].name.as_str(),
                    agreement,
                    copy: copy_rate(agreement, chance_agreement(&child_columns[child], &parent_columns[parent])),
                });
            }
            draws.push(DrawSpec {
                pointer: format!("/relationships/{index}"),
                relationship: &stated[index],
                columns: ends.child_columns.clone(),
                parent: plans[ends.parent].clone(),
                parent_name: ends.parent,
                parent_columns: ends.parent_columns.clone(),
                sorted: sorted_by.is_some_and(|column| column == first),
                scale,
                ordinal: ordinals[index],
            });
        }
        let relations = TableRelations {
            addressed: addressed.remove(name).unwrap_or_default(),
            draws,
            inherited,
        };
        let scenario = scenarios.and_then(|scenarios| scenarios.tables.get(name));
        let plan = plan_table(table, identity_of(Some(name), rows[name]), scenario, Some(relations), &[])
            .map_err(|error| error.in_bundle_table(name))?;
        plans.insert(name, Arc::new(plan));
    }
    for (planned, relationship) in planned.iter_mut().zip(stated) {
        if planned.set_aside.is_some() {
            continue;
        }
        let aside = |pointer: String| planned.entries_set_aside.iter().any(|(held, _)| *held == pointer);
        let copied: Vec<String> = relationship
            .inherited
            .iter()
            .flatten()
            .enumerate()
            .filter(|(place, _)| !aside(format!("{}/inherited/{place}", planned.pointer)))
            .map(|(_, entry)| entry.child.clone())
            .collect();
        let ordinal = relationship
            .ordinal
            .clone()
            .filter(|_| !aside(format!("{}/ordinal", planned.pointer)));
        (planned.copied, planned.ordinal) = (copied, ordinal);
    }
    let total = rows.values().sum();
    Ok(BundlePlan {
        identity: identity_of(None, total),
        tables: order.iter().map(|&name| (name.to_string(), plans[name].clone())).collect(),
        relationships: planned,
    })
}

/// The bundle's tables, every parent of a drawing relationship before its child, else by name.
fn parent_first<'a>(
    tables: &BTreeMap<&'a str, &TableProfile>,
    drawing: &[usize],
    resolved: &[Ends<'a>],
) -> Result<Vec<&'a str>, PlanError> {
    let mut waiting: BTreeMap<&str, BTreeSet<&str>> = tables.keys().map(|&name| (name, BTreeSet::new())).collect();
    for &index in drawing {
        waiting
            .get_mut(resolved[index].child)
            .expect("a table")
            .insert(resolved[index].parent);
    }
    let mut order = Vec::with_capacity(tables.len());
    while !waiting.is_empty() {
        let Some(&ready) = waiting.iter().find(|(_, parents)| parents.is_empty()).map(|(name, _)| name) else {
            let looped: Vec<&str> = waiting.keys().copied().collect();
            let index = drawing
                .iter()
                .copied()
                .find(|&index| waiting.contains_key(resolved[index].child))
                .expect("a waiting table waits on a relationship");
            return Err(refuse(
                RelationshipFault::Cycle,
                format!("/relationships/{index}"),
                format!("{} refer to each other in a loop", looped.join(", ")),
            ));
        };
        waiting.remove(ready);
        for parents in waiting.values_mut() {
            parents.remove(ready);
        }
        order.push(ready);
    }
    Ok(order)
}

/// Where a child's key draws a row of its parent from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Slot {
    Null,
    /// The `n`th of the relationship's orphan keys.
    Orphan(u64),
    /// The parent's row.
    Matched(u64),
}

/// How a key a child refers to is drawn at any row of its table.
#[derive(Debug, Clone)]
pub(crate) enum KeyAddress {
    Row(RowKey),
    /// A key that is itself a foreign key of its table: its member `member`.
    Foreign {
        draw: Arc<KeyDraw>,
        member: usize,
    },
}

impl KeyAddress {
    fn value_at(&self, row: u64) -> KeyValue {
        match self {
            Self::Row(key) => key.value_at(row),
            Self::Foreign { draw, member } => draw.value(*member, draw.slot(row)),
        }
    }

    fn is_text(&self) -> bool {
        match self {
            Self::Row(key) => key.is_text(),
            Self::Foreign { draw, member } => draw.parent_keys[*member].is_text(),
        }
    }

    /// Whether the key is written as text from a type mix, so the child's key holding it is text too.
    fn draws_type_mix(&self) -> bool {
        match self {
            Self::Row(key) => key.draws_type_mix(),
            Self::Foreign { draw, member } => draw.parent_keys[*member].draws_type_mix(),
        }
    }

    fn largest(&self, rows: u64) -> Option<i64> {
        match self {
            Self::Row(key) => key.largest(rows),
            Self::Foreign { draw, member } => draw.largest(*member),
        }
    }

    fn describe(&self) -> &'static str {
        match self {
            Self::Row(RowKey::Sequence { .. }) => "a sequence",
            Self::Row(RowKey::Uuid { .. }) => "a uuid",
            Self::Row(RowKey::TextAtRow { .. }) => "each row's own value",
            Self::Row(RowKey::TypeMix { .. }) => "a sequence written as text, else text only its row holds",
            Self::Foreign { .. } => "its own relationship",
        }
    }
}

/// One relationship as its child draws it.
#[derive(Debug)]
pub(crate) struct KeyDraw {
    name: String,
    seed: u64,
    rows: KeyedPermutation,
    parents: KeyedPermutation,
    parent_rows: u64,
    counts: RankedCounts,
    nulls: u64,
    matched: u64,
    orphans: u64,
    orphan_keys: u64,
    parent_keys: Vec<KeyAddress>,
    /// The stream each member's text orphans are drawn from.
    orphan_streams: Vec<u64>,
    child_rows: u64,
    pairing: Option<Pairing>,
    /// Each rank's parent row, from rank 1, where the ranks are placed by a copied column's value.
    placed: Option<Vec<u32>>,
    /// The parent column the ranks are placed by, and the child column it keeps the mix of.
    placed_by: Option<String>,
}

/// The most parents whose ranks are placed by a copied column's value, one `u32` row each. Placing
/// reads every parent row's value at plan time, so past this the parent permutation alone places them.
const PLACED_PARENTS: u64 = 1 << 22;

/// A value's matched rows still short of its stated share, per parent row it has left, the
/// furthest short popped first. A value with few parents and many rows to reach takes the busy ranks.
struct Short {
    per_parent: f64,
    value: usize,
}

impl PartialEq for Short {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl Eq for Short {}

impl PartialOrd for Short {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Short {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.per_parent
            .total_cmp(&other.per_parent)
            .then_with(|| other.value.cmp(&self.value))
    }
}

/// How one draw of a bridge takes its positions from the other's, so no two rows repeat their pair.
#[derive(Debug)]
struct Pairing {
    /// The other draw's position times this, modulo the child's rows, is this draw's.
    multiplier: u64,
    with: String,
}

/// 2^64 divided by the golden ratio. Multiples of its fraction of a range spread around it most evenly.
const GOLDEN_FRACTION: u64 = 0x9E37_79B9_7F4A_7C15;
/// The multipliers tried from each start before a bridge's draws are left unpaired.
const MULTIPLIER_TRIES: usize = 1024;

impl KeyDraw {
    fn new(
        spec: &DrawSpec<'_>,
        table: &str,
        path: &str,
        child_rows: u64,
        seed: u64,
        null_share: f64,
        parent_keys: Vec<KeyAddress>,
    ) -> Result<Self, PlanError> {
        let relationship = spec.relationship;
        let parent_rows = spec.parent.identity().rows();
        let nulls = ((child_rows as f64 * null_share).round() as u64).min(child_rows);
        let filled = child_rows - nulls;
        let orphans = match parent_rows {
            0 => filled,
            _ => ((filled as f64 * relationship.orphan_rate.map_or(0.0, |rate| rate.0)).round() as u64).min(filled),
        };
        let matched = filled - orphans;
        let counts = RankedCounts::new(relationship.fanout.as_ref(), parent_rows, matched, spec.scale)
            .map_err(|message| refuse(RelationshipFault::InvalidFanout, format!("{}/fanout", spec.pointer), message))?;
        let held = counts.held_parents();
        let containment = relationship.containment.map_or(1.0, |rate| rate.0);
        let orphan_keys = match orphans {
            0 => 0,
            _ if containment > 0.0 && containment < 1.0 => (held as f64 * (1.0 - containment) / containment).round() as u64,
            _ => (orphans as f64 * held as f64 / matched.max(1) as f64).round() as u64,
        }
        .clamp(orphans.min(1), orphans);
        let key = |draw: &str| stream_key(table, path, draw);
        let (rows, parents) = match spec.sorted {
            true => (KeyedPermutation::identity(child_rows), KeyedPermutation::identity(parent_rows)),
            false => (
                KeyedPermutation::new(child_rows, seed, key("foreign_key.rows")),
                KeyedPermutation::new(parent_rows, seed, key("foreign_key.parents")),
            ),
        };
        Ok(Self {
            name: relationship.display_name(),
            seed,
            rows,
            parents,
            parent_rows,
            counts,
            nulls,
            matched,
            orphans,
            orphan_keys,
            orphan_streams: (0..parent_keys.len())
                .map(|member| key(&format!("foreign_key.orphans.{member}")))
                .collect(),
            parent_keys,
            child_rows,
            pairing: None,
            placed: None,
            placed_by: None,
        })
    }

    /// Deals the ranks, busiest first, among the values `parent`'s column `column` holds, each to the
    /// value whose matched rows fall furthest short of the share `child`'s profile states. Each
    /// value's ranks then go to its parent rows in the parent permutation's order. So `child`, which
    /// copies `column`, keeps its own profiled mix where busy parents hold other values than idle
    /// ones. Left unplaced where `child` lists no values or the parents pass `PLACED_PARENTS`.
    fn place_by_value(&mut self, parent: &Plan, column: usize, child: &Column, by: String) {
        let parents = self.parent_rows;
        let listed = child.string.as_ref().and_then(|string| string.top_values.as_ref());
        let Some(listed) = listed.filter(|listed| !listed.is_empty()) else {
            return;
        };
        if parents == 0 || parents > PLACED_PARENTS || self.matched == 0 {
            return;
        }
        let mut values: HashMap<String, u32> = HashMap::new();
        let mut names: Vec<Option<String>> = vec![None];
        let mut held: Vec<u32> = Vec::with_capacity(parents as usize);
        for block in 0..parents.div_ceil(BLOCK_ROWS) {
            let batch = parent.block_through(block, 0, false).batch;
            let Ok(text) = cast(batch.column(column), &DataType::Utf8) else {
                return;
            };
            let text = text.as_string::<i32>();
            for row in 0..text.len() {
                if text.is_null(row) {
                    held.push(0);
                    continue;
                }
                let value = text.value(row);
                let at = match values.get(value) {
                    Some(&at) => at,
                    None => {
                        let at = names.len() as u32;
                        values.insert(value.to_string(), at);
                        names.push(Some(value.to_string()));
                        at
                    }
                };
                held.push(at);
            }
        }
        let mut room = vec![0u64; names.len()];
        for &value in &held {
            room[value as usize] += 1;
        }
        if room.iter().filter(|&&rows| rows > 0).count() < 2 {
            return;
        }
        let stated: HashMap<&str, f64> = listed.iter().map(|top| (top.value.as_str(), top.rate.0)).collect();
        let null_rate = child.null_rate.map_or(0.0, |rate| rate.0);
        // Summed in the profile's order, since a map's order varies by process and moves the sum by an ulp.
        let rest = (1.0 - listed.iter().map(|top| top.rate.0).sum::<f64>() - null_rate).max(0.0);
        let is_unlisted = |name: &Option<String>| name.as_deref().is_some_and(|value| !stated.contains_key(value));
        let unlisted: u64 = names
            .iter()
            .zip(&room)
            .filter(|(name, _)| is_unlisted(name))
            .map(|(_, &rows)| rows)
            .sum();
        let shares: Vec<f64> = names
            .iter()
            .zip(&room)
            .map(|(name, &rows)| match name.as_deref() {
                None => null_rate,
                Some(value) => stated
                    .get(value)
                    .copied()
                    .unwrap_or_else(|| rest * rows as f64 / unlisted.max(1) as f64),
            })
            .collect();
        let total: f64 = shares.iter().sum();
        if total.is_nan() || total <= 0.0 {
            return;
        }
        let mut wanted: Vec<f64> = shares.iter().map(|share| self.matched as f64 * share / total).collect();
        let mut short: BinaryHeap<Short> = (0..names.len())
            .filter(|&value| room[value] > 0)
            .map(|value| Short {
                per_parent: wanted[value] / room[value] as f64,
                value,
            })
            .collect();
        let mut rows_of: Vec<Vec<u32>> = room.iter().map(|&rows| Vec::with_capacity(rows as usize)).collect();
        for at in 0..parents {
            let row = self.parents.inverse(at);
            rows_of[held[row as usize] as usize].push(row as u32);
        }
        let mut next = vec![0usize; names.len()];
        let mut placed = Vec::with_capacity(parents as usize);
        for rank in 1..=parents {
            let value = short.pop().expect("the values hold a parent for every rank").value;
            placed.push(rows_of[value][next[value]]);
            next[value] += 1;
            room[value] -= 1;
            wanted[value] -= (self.counts.cumulative(rank) - self.counts.cumulative(rank - 1)) as f64;
            if room[value] > 0 {
                short.push(Short {
                    per_parent: wanted[value] / room[value] as f64,
                    value,
                });
            }
        }
        self.placed = Some(placed);
        self.placed_by = Some(by);
    }

    /// The most child rows one key holds: the busiest parent's, or one orphan key's. A rounded count
    /// may pass the busiest rank's by one.
    fn longest_run(&self) -> u64 {
        let orphan_run = match self.orphan_keys {
            0 => 0,
            keys => self.orphans.div_ceil(keys),
        };
        (self.counts.cumulative(1) + 1).max(orphan_run)
    }

    /// Takes this draw's positions from `driver`'s, times a multiplier modulo the rows, so the rows of
    /// one of `driver`'s keys sit at least this draw's longest run apart and hold different keys. Left
    /// unpaired where a key of either holds at most one row, so no pair repeats, or where the runs are
    /// too long for any multiplier to spread.
    fn pair_with(&mut self, driver: &KeyDraw) {
        let (runs, spread, rows) = (driver.longest_run(), self.longest_run(), self.child_rows);
        if runs <= 1 || spread <= 1 || u128::from(runs) * u128::from(spread) > u128::from(rows) {
            return;
        }
        if let Some(multiplier) = spreading_multiplier(rows, runs, spread) {
            self.rows = driver.rows.clone();
            self.pairing = Some(Pairing {
                multiplier,
                with: driver.name.clone(),
            });
        }
    }

    /// The child row's place among the draw's: nulls first, then each parent's run of matched rows
    /// busiest first, then each orphan key's run.
    fn position(&self, row: u64) -> u64 {
        let position = self.rows.forward(row);
        match &self.pairing {
            Some(pairing) => (u128::from(position) * u128::from(pairing.multiplier) % u128::from(self.child_rows)) as u64,
            None => position,
        }
    }

    pub(crate) fn slot(&self, row: u64) -> Slot {
        self.slot_at(self.position(row))
    }

    /// The number of the row at `position` among the rows holding its key, from 1, or null where its
    /// key is.
    fn ordinal_at(&self, position: u64) -> KeyValue {
        if position < self.nulls {
            return KeyValue::Null;
        }
        let matched = position - self.nulls;
        let first = match matched < self.matched {
            true => self.counts.cumulative(self.counts.rank_of(matched) - 1),
            false => {
                let orphan = matched - self.matched;
                let key = u128::from(orphan) * u128::from(self.orphan_keys) / u128::from(self.orphans);
                self.matched + (key * u128::from(self.orphans)).div_ceil(u128::from(self.orphan_keys)) as u64
            }
        };
        KeyValue::Integer((matched - first + 1) as i64)
    }

    fn slot_at(&self, position: u64) -> Slot {
        if position < self.nulls {
            return Slot::Null;
        }
        let matched = position - self.nulls;
        if matched < self.matched {
            return Slot::Matched(self.parent_of_rank(self.counts.rank_of(matched)));
        }
        let orphan = matched - self.matched;
        Slot::Orphan((u128::from(orphan) * u128::from(self.orphan_keys) / u128::from(self.orphans)) as u64)
    }

    /// The parent row of rank `rank`, from 1.
    fn parent_of_rank(&self, rank: u64) -> u64 {
        match &self.placed {
            Some(placed) => u64::from(placed[(rank - 1) as usize]),
            None => self.parents.inverse(rank - 1),
        }
    }

    fn value(&self, member: usize, slot: Slot) -> KeyValue {
        match slot {
            Slot::Null => KeyValue::Null,
            Slot::Matched(row) => self.parent_keys[member].value_at(row),
            // A key holding integers, a type mix's own share included, takes orphans past its largest.
            Slot::Orphan(orphan) => match self.parent_keys[member].largest(self.parent_rows) {
                None => {
                    let mut rng = rng_for(self.seed, self.orphan_streams[member], orphan);
                    let mut out = String::with_capacity(36);
                    crate::samplers::push_uuid(&mut rng, &mut out);
                    KeyValue::Text(out)
                }
                Some(largest) => KeyValue::Integer(largest.saturating_add(1).saturating_add(orphan as i64)),
            },
        }
    }

    /// The largest integer the member holds, its orphans past every parent's key.
    fn largest(&self, member: usize) -> Option<i64> {
        let largest = self.parent_keys[member].largest(self.parent_rows)?;
        Some(largest.saturating_add(self.orphan_keys as i64))
    }

    fn describe(&self) -> String {
        let paired = match &self.pairing {
            Some(pairing) => format!(", placed by {}'s so no two rows repeat the pair", pairing.with),
            None => String::new(),
        };
        let placed = match &self.placed_by {
            Some(by) => format!(", its busiest parents placed by {by}"),
            None => String::new(),
        };
        format!(
            "the relationship {}: {} matched rows over {} parents, {} orphan rows over {} keys, {} null{paired}{placed}",
            self.name,
            self.matched,
            self.counts.held_parents(),
            self.orphans,
            self.orphan_keys,
            self.nulls
        )
    }
}

/// A multiplier coprime with `rows` that places any `runs` consecutive positions, times it, at least
/// `spread` apart around `rows`. Tried from the golden fraction of `rows`, then from `spread` up.
fn spreading_multiplier(rows: u64, runs: u64, spread: u64) -> Option<u64> {
    let golden = ((u128::from(rows) * u128::from(GOLDEN_FRACTION)) >> 64) as u64;
    let spreads = |multiplier: u64| {
        greatest_common_divisor(multiplier, rows) == 1
            && (1..runs).all(|step| {
                let at = (u128::from(step) * u128::from(multiplier) % u128::from(rows)) as u64;
                at.min(rows - at) >= spread
            })
    };
    (golden..rows)
        .take(MULTIPLIER_TRIES)
        .chain((spread..rows).take(MULTIPLIER_TRIES))
        .find(|&multiplier| spreads(multiplier))
}

fn greatest_common_divisor(mut a: u64, mut b: u64) -> u64 {
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
}

/// The two draws whose columns are `table`'s primary key between them, the one placing the other
/// first: the draw its files are sorted by, else the earlier.
fn bridge(table: &TableProfile, draws: &[DrawSpec<'_>]) -> Option<(usize, usize)> {
    let primary: BTreeSet<&str> = table
        .dataset
        .as_ref()?
        .keys
        .as_ref()?
        .primary
        .as_ref()?
        .columns
        .iter()
        .map(String::as_str)
        .collect();
    let profile = table.columns.as_deref().unwrap_or_default();
    let names = |spec: &DrawSpec<'_>| -> BTreeSet<&str> { spec.columns.iter().map(|&column| profile[column].name.as_str()).collect() };
    for first in 0..draws.len() {
        for second in first + 1..draws.len() {
            let (a, b) = (names(&draws[first]), names(&draws[second]));
            if a.is_disjoint(&b) && a.union(&b).copied().collect::<BTreeSet<_>>() == primary {
                return Some(if draws[second].sorted { (second, first) } else { (first, second) });
            }
        }
    }
    None
}

/// The child columns that copy their parent row's, for one relationship.
#[derive(Debug)]
struct InheritedGroup {
    owner: u64,
    draw: usize,
    parent: Arc<Plan>,
    /// Each parent column copied, and the type the child holds it as.
    parent_columns: Vec<(usize, DataType)>,
    children: Vec<InheritedColumn>,
}

#[derive(Debug)]
struct InheritedColumn {
    column: usize,
    /// The place of its parent column in the group's.
    from: usize,
    copy: Bernoulli,
    stream: u64,
}

/// What a table draws from its relationships, and how its children read its keys.
#[derive(Debug)]
pub(crate) struct RelatedColumns {
    draws: Vec<Arc<KeyDraw>>,
    /// Each column a draw writes: the column, the draw, its member, and how the column stores it.
    keys: Vec<(usize, usize, usize, KeyColumn)>,
    /// Each column a draw numbers: the column, the draw, and how the column stores it.
    ordinals: Vec<(usize, usize, KeyColumn)>,
    inherited: Vec<InheritedGroup>,
    addresses: HashMap<usize, KeyAddress>,
}

/// One column a table's relationships draw for a block, beside the rows it holds null.
pub(crate) struct GivenColumn {
    pub(crate) array: ArrayRef,
    pub(crate) nulls: Option<BooleanBuffer>,
}

/// The parents of a foreign key's busiest ranks, as `hot_keys` writes them.
#[derive(Debug)]
pub(crate) struct HotParents {
    /// Each column of the key and its values, one per hot parent.
    pub(crate) members: Vec<(usize, ArrayRef)>,
    /// Each inherited column of the relationship, its parent's values, one per hot parent, and the
    /// trial and stream that decide whether a row copies.
    pub(crate) inherited: Vec<(usize, ArrayRef, Bernoulli, u64)>,
}

/// Plans `relations` against `columns`, the table's column plans: its keys made readable at any row,
/// its foreign keys drawn, and its inherited columns. Returns what the dependent columns leave alone,
/// each column with why.
pub(crate) fn relate(
    relations: TableRelations<'_>,
    table: &TableProfile,
    columns: &mut [ColumnPlan],
    seed: u64,
    rows: u64,
) -> Result<(RelatedColumns, BTreeMap<usize, String>), PlanError> {
    let profile = table.columns.as_deref().unwrap_or_default();
    let mut fixed = BTreeMap::new();
    let mut draws = Vec::with_capacity(relations.draws.len());
    let mut keys = Vec::new();
    let mut ordinals = Vec::new();
    for spec in &relations.draws {
        let parent_related = spec.parent.related().expect("a table a child refers to holds its keys' addresses");
        let parent_keys: Vec<KeyAddress> = spec
            .parent_columns
            .iter()
            .map(|column| parent_related.addresses[column].clone())
            .collect();
        let mut stored = Vec::with_capacity(spec.columns.len());
        for (member, &column) in spec.columns.iter().enumerate() {
            let field = format!("{}/from/{member}", spec.pointer);
            // A foreign key holds its parent's keys, so it is text exactly where they are drawn from a type mix.
            columns[column].drop_type_mix();
            if parent_keys[member].draws_type_mix() {
                columns[column].render_as_text(&profile[column], &format!("/columns/{column}"));
            }
            let stores = columns[column].key_column();
            let stored_as = match (stores, parent_keys[member].is_text()) {
                (Some(KeyColumn::Integer(_)), true) | (None, _) => {
                    return Err(refuse(
                        RelationshipFault::IncompatibleTypes,
                        field,
                        format!("{:?} cannot hold the values of the key it refers to", profile[column].name),
                    ));
                }
                (Some(stores), _) => stores,
            };
            stored.push(stored_as);
        }
        let names: Vec<&str> = spec.columns.iter().map(|&column| profile[column].name.as_str()).collect();
        let null_share = columns[spec.columns[0]].profiled_null_share();
        let draw = KeyDraw::new(spec, &table.name, &names.join(","), rows, seed, null_share, parent_keys)?;
        let index = draws.len();
        for (member, &column) in spec.columns.iter().enumerate() {
            keys.push((column, index, member, stored[member]));
            fixed.insert(column, format!("its relationship to {} draws it", spec.parent_name));
            columns[column].null_share = if rows == 0 { 0.0 } else { draw.nulls as f64 / rows as f64 };
        }
        if let Some(column) = spec.ordinal {
            columns[column].drop_type_mix();
            let Some(stored @ KeyColumn::Integer(_)) = columns[column].key_column() else {
                return Err(refuse(
                    RelationshipFault::IncompatibleTypes,
                    format!("{}/ordinal", spec.pointer),
                    format!("{:?} cannot hold a row's number", profile[column].name),
                ));
            };
            ordinals.push((column, index, stored));
            fixed.insert(column, format!("its relationship to {} numbers it", spec.parent_name));
            columns[column].null_share = if rows == 0 { 0.0 } else { draw.nulls as f64 / rows as f64 };
        }
        draws.push(draw);
    }
    if let Some((driver, paired)) = bridge(table, &relations.draws) {
        let (driver, paired) = match driver < paired {
            true => {
                let (low, high) = draws.split_at_mut(paired);
                (&low[driver], &mut high[0])
            }
            false => {
                let (low, high) = draws.split_at_mut(driver);
                (&high[0], &mut low[paired])
            }
        };
        paired.pair_with(driver);
    }
    for (index, (spec, draw)) in relations.draws.iter().zip(&mut draws).enumerate() {
        let listed = |child: usize| profile[child].string.as_ref().is_some_and(|string| string.top_values.is_some());
        let copied = relations.inherited.iter().find(|entry| entry.draw == index && listed(entry.child));
        if let Some(entry) = copied.filter(|_| !spec.sorted) {
            let by = format!(
                "{}.{}'s value, as {} holds it",
                entry.parent_name, entry.parent_column, profile[entry.child].name
            );
            draw.place_by_value(&spec.parent, entry.parent, &profile[entry.child], by);
        }
    }
    for (spec, draw) in relations.draws.iter().zip(&draws) {
        for &column in &spec.columns {
            columns[column].sampler = draw.describe();
        }
        if let Some(column) = spec.ordinal {
            columns[column].sampler = format!("each row's number among the rows holding its key, from 1, by {}", draw.name);
        }
    }
    let draws: Vec<Arc<KeyDraw>> = draws.into_iter().map(Arc::new).collect();
    let mut addresses = HashMap::new();
    for &column in &relations.addressed {
        if let Some(&(_, draw, member, _)) = keys.iter().find(|key| key.0 == column) {
            addresses.insert(
                column,
                KeyAddress::Foreign {
                    draw: draws[draw].clone(),
                    member,
                },
            );
            continue;
        }
        let Some(key) = columns[column].as_row_key(&profile[column], rows, seed) else {
            return Err(refuse(
                RelationshipFault::ParentKeyUnaddressable,
                format!("/tables/{}/columns/{column}", crate::perturbations::pointer_token(&table.name)),
                format!(
                    "{:?} is a key a child refers to, and is not a sequence, a uuid, an integer or text, which a row can be asked for",
                    profile[column].name
                ),
            ));
        };
        let address = KeyAddress::Row(key);
        columns[column].sampler = format!("{}, a key children read by row as {}", columns[column].sampler, address.describe());
        fixed.insert(column, "it is a key a child refers to".to_string());
        addresses.insert(column, address);
    }
    let mut inherited: Vec<InheritedGroup> = Vec::new();
    for spec in &relations.inherited {
        let parent = relations.draws[spec.draw].parent.clone();
        // A copied column holds its parent's values, so it is text exactly where its parent is drawn from a type mix.
        match parent.draws_type_mix(spec.parent) {
            true => {
                columns[spec.child].render_as_text(&profile[spec.child], &format!("/columns/{}", spec.child));
            }
            false => columns[spec.child].drop_type_mix(),
        }
        let parent_type = parent.generated_schema().field(spec.parent).data_type().clone();
        let child_type = columns[spec.child].field.data_type().clone();
        if !can_cast_types(&parent_type, &child_type) {
            return Err(refuse(
                RelationshipFault::IncompatibleTypes,
                format!("{}/inherited", relations.draws[spec.draw].pointer),
                format!(
                    "{:?} cannot hold {}.{}'s values",
                    profile[spec.child].name, spec.parent_name, spec.parent_column
                ),
            ));
        }
        let at = match inherited.iter().position(|group| group.draw == spec.draw) {
            Some(at) => at,
            None => {
                inherited.push(InheritedGroup {
                    owner: parent_blocks::owner(),
                    draw: spec.draw,
                    parent,
                    parent_columns: Vec::new(),
                    children: Vec::new(),
                });
                inherited.len() - 1
            }
        };
        let group = &mut inherited[at];
        let from = match group
            .parent_columns
            .iter()
            .position(|(column, held)| *column == spec.parent && *held == child_type)
        {
            Some(from) => from,
            None => {
                group.parent_columns.push((spec.parent, child_type));
                group.parent_columns.len() - 1
            }
        };
        let name = &profile[spec.child].name;
        group.children.push(InheritedColumn {
            column: spec.child,
            from,
            copy: Bernoulli::new(spec.copy),
            stream: stream_key(&table.name, profile[spec.child].path.as_deref().unwrap_or(name), "inherited"),
        });
        columns[spec.child].sampler = format!(
            "{}.{} copied on {:.4} of matched rows to agree on {}, else {}",
            spec.parent_name, spec.parent_column, spec.copy, spec.agreement, columns[spec.child].sampler
        );
        fixed.insert(
            spec.child,
            format!("its copy of {}.{} draws it", spec.parent_name, spec.parent_column),
        );
    }
    Ok((
        RelatedColumns {
            draws,
            keys,
            ordinals,
            inherited,
            addresses,
        },
        fixed,
    ))
}

impl RelatedColumns {
    /// Every column the relationships draw for the block at `at`, by its place among `columns`.
    pub(crate) fn generate(&self, columns: &[ColumnPlan], at: &BlockPosition, rows: usize) -> Vec<Option<GivenColumn>> {
        let mut given: Vec<Option<GivenColumn>> = (0..columns.len()).map(|_| None).collect();
        if self.keys.is_empty() && self.inherited.is_empty() {
            return given;
        }
        let positions: Vec<Vec<u64>> = self
            .draws
            .iter()
            .map(|draw| (0..rows as u64).map(|row| draw.position(at.first_row + row)).collect())
            .collect();
        let slots: Vec<Vec<Slot>> = self
            .draws
            .iter()
            .zip(&positions)
            .map(|(draw, positions)| positions.iter().map(|&position| draw.slot_at(position)).collect())
            .collect();
        for &(column, draw, member, stored) in &self.keys {
            let values: Vec<KeyValue> = slots[draw].iter().map(|&slot| self.draws[draw].value(member, slot)).collect();
            given[column] = Some(held(stored.array(&values)));
        }
        for &(column, draw, stored) in &self.ordinals {
            let values: Vec<KeyValue> = positions[draw]
                .iter()
                .map(|&position| self.draws[draw].ordinal_at(position))
                .collect();
            given[column] = Some(held(stored.array(&values)));
        }
        for group in &self.inherited {
            let slots = &slots[group.draw];
            let mut blocks: BTreeMap<u64, usize> = BTreeMap::new();
            for slot in slots {
                if let Slot::Matched(row) = slot {
                    let next = blocks.len();
                    blocks.entry(row / BLOCK_ROWS).or_insert(next);
                }
            }
            let mut fetched: Vec<Arc<[ArrayRef]>> = vec![Arc::from(Vec::new()); blocks.len()];
            for (&block, &place) in &blocks {
                fetched[place] = parent_blocks::parent_columns(group.owner, block, || group.parent_values(block));
            }
            for child in &group.children {
                let own = columns[child.column].generate(at, rows);
                let mut copies = rng_for(at.seed, child.stream, at.block);
                let picks: Vec<(usize, usize)> = slots
                    .iter()
                    .enumerate()
                    .map(|(row, slot)| {
                        let copied = child.copy.draw(&mut copies);
                        match slot {
                            Slot::Matched(parent) if copied => (1 + blocks[&(parent / BLOCK_ROWS)], (parent % BLOCK_ROWS) as usize),
                            _ => (0, row),
                        }
                    })
                    .collect();
                let mut sources: Vec<&dyn Array> = vec![own.as_ref()];
                sources.extend(fetched.iter().map(|columns| columns[child.from].as_ref()));
                let array = interleave(&sources, &picks).expect("every source holds the child's type");
                given[child.column] = Some(held(array));
            }
        }
        given
    }

    /// Whether a relationship draws `column`.
    pub(crate) fn draws_column(&self, column: usize) -> bool {
        self.keys.iter().any(|key| key.0 == column)
    }

    /// Every column the relationship drawing `column` writes: each column of its key, and each it
    /// copies from the parent row. None where no relationship draws `column`.
    pub(crate) fn drawn_with(&self, column: usize) -> Option<Vec<usize>> {
        let &(_, draw, _, _) = self.keys.iter().find(|key| key.0 == column)?;
        let keys = self.keys.iter().filter(|key| key.1 == draw).map(|key| key.0);
        let copied = self.inherited.iter().filter(|group| group.draw == draw);
        Some(
            keys.chain(copied.flat_map(|group| group.children.iter().map(|child| child.column)))
                .collect(),
        )
    }

    /// The parents of the `count` busiest ranks of the relationship drawing `column`, refused where
    /// the parent holds fewer rows.
    pub(crate) fn hot_parents(&self, column: usize, count: u64) -> Result<HotParents, String> {
        let &(_, draw_at, _, _) = self.keys.iter().find(|key| key.0 == column).expect("a drawn column");
        let draw = &self.draws[draw_at];
        if count > draw.parent_rows {
            return Err(format!("{} parents hold no {count} busiest", draw.parent_rows));
        }
        let parents: Vec<u64> = (1..=count).map(|rank| draw.parent_of_rank(rank)).collect();
        let members = self
            .keys
            .iter()
            .filter(|key| key.1 == draw_at)
            .map(|&(column, _, member, stored)| {
                let values: Vec<KeyValue> = parents.iter().map(|&row| draw.parent_keys[member].value_at(row)).collect();
                (column, stored.array(&values))
            })
            .collect();
        let mut inherited = Vec::new();
        for group in self.inherited.iter().filter(|group| group.draw == draw_at) {
            let mut blocks: BTreeMap<u64, Vec<ArrayRef>> = BTreeMap::new();
            for &row in &parents {
                blocks
                    .entry(row / BLOCK_ROWS)
                    .or_insert_with(|| group.parent_values(row / BLOCK_ROWS));
            }
            let sources: Vec<(u64, &Vec<ArrayRef>)> = blocks.iter().map(|(&block, columns)| (block, columns)).collect();
            for child in &group.children {
                let arrays: Vec<&dyn Array> = sources.iter().map(|(_, columns)| columns[child.from].as_ref()).collect();
                let picks: Vec<(usize, usize)> = parents
                    .iter()
                    .map(|&row| {
                        let place = sources.iter().position(|(block, _)| *block == row / BLOCK_ROWS).expect("drawn");
                        (place, (row % BLOCK_ROWS) as usize)
                    })
                    .collect();
                let values = interleave(&arrays, &picks).expect("every source holds the child's type");
                inherited.push((child.column, values, child.copy, child.stream));
            }
        }
        Ok(HotParents { members, inherited })
    }
}

impl InheritedGroup {
    /// The copied parent columns of parent block `block`, as the child holds them.
    fn parent_values(&self, block: u64) -> Vec<ArrayRef> {
        let batch = self.parent.block_through(block, 0, false).batch;
        self.parent_columns
            .iter()
            .map(|(column, held)| cast(batch.column(*column), held).expect("the plan checked the cast"))
            .collect()
    }
}

fn held(array: ArrayRef) -> GivenColumn {
    let nulls = array
        .logical_nulls()
        .filter(|nulls| nulls.null_count() > 0)
        .map(|nulls| !nulls.inner());
    GivenColumn { array, nulls }
}
