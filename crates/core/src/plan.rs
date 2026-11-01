//! A profile resolved into per-column samplers, and the pure function from a block number to that
//! block's rows.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::BooleanArray;
use arrow_array::RecordBatch;
use arrow_array::RecordBatchOptions;
use arrow_schema::Schema;
use arrow_schema::SchemaRef;
use arrow_select::filter::filter_record_batch;

use crate::column_overrides;
use crate::column_overrides::ColumnOverride;
use crate::column_overrides::PinnedColumn;
use crate::identity::DatasetIdentity;
use crate::identity::is_sha256;
use crate::issue_codes;
use crate::issue_codes::IssueCode;
use crate::perturbations;
use crate::perturbations::Block;
use crate::perturbations::BlockPerturbation;
use crate::perturbations::CdcPlan;
use crate::perturbations::CdcStream;
use crate::perturbations::MalformedPlan;
use crate::perturbations::Planned;
use crate::perturbations::Planning;
use crate::perturbations::RowFaults;
use crate::perturbations::ScenarioFault;
use crate::perturbations::SchemaDriftPlan;
use crate::perturbations::StreamOutline;
use crate::perturbations::plan_perturbation;
use crate::physical_type::physical_type_name;
use crate::profile::Column;
use crate::profile::DEFAULT_K_MIN;
use crate::profile::Profile;
use crate::profile::Quantiles;
use crate::profile::RawValueType;
use crate::profile::TableProfile;
use crate::profile::provenance;
use crate::related_tables;
use crate::related_tables::GivenColumn;
use crate::related_tables::RelatedColumns;
use crate::related_tables::RelationshipFault;
use crate::related_tables::TableRelations;
use crate::rng::BLOCK_ROWS;
use crate::samplers::BlockPosition;
use crate::samplers::ColumnPlan;
use crate::samplers::ColumnSite;
use crate::samplers::DependentColumns;
use crate::samplers::DrawnColumns;
use crate::samplers::PlanContext;
use crate::samplers::TextCell;
use crate::samplers::column_plan;
use crate::scenario::OPERATION_COLUMN;
use crate::scenario::Perturbation;
use crate::scenario::Scenario;
use serde::Serialize;
use serde_json::Value;

#[derive(Debug, thiserror::Error)]
pub enum PlanError {
    #[error("profile {name:?} is a bundle; plan one table at a time")]
    Bundle { name: String },
    #[error("profile {name:?} has no columns to generate")]
    NoColumns { name: String },
    #[error("column {column:?} appears twice, and both would draw the same stream")]
    DuplicateColumn { column: String },
    #[error("profile SHA-256 {sha256:?} is not `sha256:` and 64 lowercase hex digits")]
    ProfileSha256 { sha256: String },
    /// A column the engine cannot generate. `field` is an RFC 6901 JSON Pointer into the profile,
    /// as the library's own issues carry.
    #[error("{field}: {message} [{code}]")]
    Column { code: ColumnFault, field: String, message: String },
    /// A scenario the engine cannot apply to the profile. `field` is an RFC 6901 JSON Pointer into
    /// the scenario.
    #[error("{field}: {message} [{code}]")]
    Scenario {
        code: ScenarioFault,
        field: String,
        message: String,
    },
    #[error("scenario SHA-256 {sha256:?} is not `sha256:` and 64 lowercase hex digits")]
    ScenarioSha256 { sha256: String },
    /// A bundle's relationship the engine cannot draw. `field` is an RFC 6901 JSON Pointer into the
    /// bundle.
    #[error("{field}: {message} [{code}]")]
    Relationship {
        code: RelationshipFault,
        field: String,
        message: String,
    },
    /// An override naming a column the table lacks, or giving a value its column cannot hold.
    /// `field` is the column's JSON pointer, empty for a column the table lacks.
    #[error("{message} [{}]", issue_codes::INVALID_OVERRIDE)]
    Override { field: String, message: String },
}

/// Why a column cannot be generated, as a code a caller can branch on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnFault {
    /// `physical_type` names an Arrow type the engine does not write.
    UnsupportedType,
    /// The column holds a statistic the engine does not generate from yet.
    NotGeneratedYet,
    /// A fit names a family the engine inverts, with a parameter outside its domain.
    InvalidFit,
    /// A pattern that does not parse, or that no string matches.
    InvalidPattern,
    /// Quantiles or histogram edges that fall, or a quantile name with no level.
    InvalidQuantiles,
    /// Rates no alias table can draw from, such as all zero.
    UnusableRates,
    /// A temporal extreme that does not parse as the column's type.
    InvalidExtreme,
    StructWithoutFields,
    /// `missing_token_fractions` that, each a share of rows, sum above the column's `null_fraction`.
    TokensExceedNullRate,
    /// A dependence between columns names a column the table lacks.
    UnknownColumn,
    /// A dependence between columns the engine cannot draw: an expression that does not parse, a column of a
    /// kind the dependence cannot apply to, a value its column's type cannot hold, or one that would make a
    /// column depend on itself.
    InvalidDependence,
}

impl ColumnFault {
    /// The code a caller branches on.
    pub fn code(self) -> &'static IssueCode {
        match self {
            Self::UnsupportedType => &issue_codes::UNSUPPORTED_TYPE,
            Self::NotGeneratedYet => &issue_codes::NOT_GENERATED_YET,
            Self::InvalidFit => &issue_codes::INVALID_FIT,
            Self::InvalidPattern => &issue_codes::INVALID_PATTERN,
            Self::InvalidQuantiles => &issue_codes::INVALID_QUANTILES,
            Self::UnusableRates => &issue_codes::UNUSABLE_FRACTIONS,
            Self::InvalidExtreme => &issue_codes::INVALID_EXTREME,
            Self::StructWithoutFields => &issue_codes::STRUCT_WITHOUT_FIELDS,
            Self::TokensExceedNullRate => &issue_codes::TOKENS_EXCEED_NULL_FRACTION,
            Self::UnknownColumn => &issue_codes::UNKNOWN_COLUMN,
            Self::InvalidDependence => &issue_codes::INVALID_DEPENDENCE,
        }
    }
}

impl PlanError {
    /// The code a caller branches on.
    pub fn code(&self) -> &'static IssueCode {
        match self {
            Self::Bundle { .. } => &issue_codes::BUNDLE_GIVEN,
            Self::NoColumns { .. } => &issue_codes::NO_COLUMNS,
            Self::DuplicateColumn { .. } => &issue_codes::DUPLICATE_COLUMN,
            Self::ProfileSha256 { .. } => &issue_codes::INVALID_PROFILE_SHA256,
            Self::Column { code, .. } => code.code(),
            Self::Scenario { code, .. } => code.code(),
            Self::ScenarioSha256 { .. } => &issue_codes::INVALID_SCENARIO_SHA256,
            Self::Relationship { code, .. } => code.code(),
            Self::Override { .. } => &issue_codes::INVALID_OVERRIDE,
        }
    }

    /// The refusal of a bundle's table `table`, its pointer into the table made one into the bundle.
    pub(crate) fn in_bundle_table(self, table: &str) -> Self {
        match self {
            Self::Column { code, field, message } => Self::Column {
                code,
                field: format!("/tables/{}{field}", crate::perturbations::pointer_token(table)),
                message,
            },
            other => other,
        }
    }

    /// The RFC 6901 JSON Pointer that the refusal names: into the scenario where
    /// [`Self::in_scenario`] holds, else into the profile.
    pub fn field(&self) -> &str {
        match self {
            Self::Bundle { .. } => "/kind",
            Self::NoColumns { .. } | Self::DuplicateColumn { .. } => "/columns",
            Self::ProfileSha256 { .. } | Self::ScenarioSha256 { .. } => "",
            Self::Column { field, .. } | Self::Scenario { field, .. } | Self::Relationship { field, .. } | Self::Override { field, .. } => {
                field
            }
        }
    }

    /// Whether the refusal is of the scenario rather than the profile.
    pub fn in_scenario(&self) -> bool {
        matches!(self, Self::Scenario { .. } | Self::ScenarioSha256 { .. })
    }
}

impl std::fmt::Display for ScenarioFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.code().code)
    }
}

impl std::fmt::Display for ColumnFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.code().code)
    }
}

/// How one top-level column is drawn, and what in the profile and the scenario decides it, as a
/// plan reports it before generating anything.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ColumnTrace {
    pub name: String,
    /// The column's type, as a profile's `physical_type` spells it.
    pub physical_type: String,
    /// The sampler's kind: `sequence`, `numeric`, `categorical`, `temporal`, `boolean`, `text`,
    /// `list` or `struct`.
    pub sampler_kind: &'static str,
    /// What draws the values, such as `sequence from 1 by 1` or `50 top values`.
    pub summary: String,
    /// Each profile field the sampler reads that the profile holds. A field left out reads as its
    /// default.
    pub parameters: Vec<SamplerParameter>,
    pub null_fraction: f64,
    pub origin: ColumnOrigin,
    /// The columns whose values a derived column's follow.
    pub reads: Vec<String>,
    /// The id of each scenario op writing the column, in the scenario's order.
    pub ops: Vec<String>,
    /// The pointer of each profile field the scenario's patch sets for the column.
    pub patched_fields: Vec<String>,
}

/// One profile field a column's sampler reads: the parameter it sets, the field's JSON pointer,
/// and its value.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SamplerParameter {
    pub name: &'static str,
    pub field: String,
    pub value: crate::SortedKeys<Value>,
}

/// What decides a column's values. A column the scenario changes is `overlaid` though it also
/// follows others, and `reads` still names what it follows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ColumnOrigin {
    /// Drawn from the profile's statistics.
    Profiled,
    /// Computed from other columns: a lookup, a lag or an expression.
    Derived,
    /// Changed by the scenario: its patch sets the column's fields, or an op writes it.
    Overlaid,
    /// Taken from the values a caller pinned.
    Pinned,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("block {block} is past the plan's {block_count} blocks")]
pub struct BlockOutOfRange {
    pub block: u64,
    pub block_count: u64,
}

/// A profile resolved for generation. `generate_block` takes `&self` and the plan is `Send + Sync`,
/// so any thread can produce any block.
#[derive(Debug)]
pub struct Plan {
    identity: DatasetIdentity,
    /// The schema the samplers' rows carry, before any perturbation.
    generated_schema: SchemaRef,
    columns: Vec<ColumnPlan>,
    dependent: DependentColumns,
    block_count: u64,
    stages: Vec<PlannedStage>,
    drift: Option<SchemaDriftPlan>,
    file_rows: Option<Quantiles>,
    malformed: Option<MalformedPlan>,
    cdc: Option<CdcPlan>,
    /// Each perturbation copying rows from across the dataset: the pointer to the parameter that
    /// sets it, and the fewest dataset rows between a copy's home and its source.
    across_files: Vec<(String, u64)>,
    /// The first perturbation ordering the whole table: its pointer and its op.
    one_process: Option<(String, &'static str)>,
    /// Each perturbation's id and the columns it writes as planned, in the scenario's order.
    writes: Vec<(String, Vec<String>)>,
    /// What a bundle's relationships draw, and how its children read its keys.
    related: Option<RelatedColumns>,
    /// Each column a caller pinned, which takes its pinned values in place of its draw.
    pinned: Vec<PinnedColumn>,
    /// The pointer of each field the scenario's patch sets, by the column it sets it for.
    patched_fields: BTreeMap<String, Vec<String>>,
}

/// One perturbation applied to every block, and the schema its rows carry after it.
#[derive(Debug)]
struct PlannedStage {
    op: Box<dyn BlockPerturbation>,
    schema: SchemaRef,
}

/// A scenario as the engine applies it: the file read, and the SHA-256 the library computed over
/// its resolved document.
#[derive(Debug, Clone, Copy)]
pub struct AppliedScenario<'a> {
    pub scenario: &'a Scenario,
    pub sha256: &'a str,
}

/// A block's rows as a file takes them: the batch, the text cells only a text format prints, and
/// the generated row each row is filed with.
#[derive(Debug, Clone)]
pub struct PlacedBlock {
    pub batch: RecordBatch,
    /// One list per column, each in row order, empty where the caller asked for none.
    pub text_cells: Vec<Vec<TextCell>>,
    /// What `malformed_rows` breaks in these rows, empty without it.
    pub faults: RowFaults,
    homes: Option<Vec<u32>>,
}

impl PlacedBlock {
    /// The rows filed with generated rows `start..stop` of the block: a slice where no perturbation
    /// added a row, else every row whose home lies in the range, in block order.
    pub fn rows_homed_in(self, start: u32, stop: u32) -> PlacedBlock {
        let Some(homes) = &self.homes else {
            let batch = self.batch.slice(start as usize, (stop - start) as usize);
            let text_cells = self
                .text_cells
                .into_iter()
                .map(|column| {
                    column
                        .into_iter()
                        .filter(|cell| (start..stop).contains(&cell.row))
                        .map(|cell| TextCell {
                            row: cell.row - start,
                            text: cell.text,
                        })
                        .collect()
                })
                .collect();
            let faults = one_group(self.faults.placed(1, |row| (start..stop).contains(&row).then(|| (0, row - start))));
            return PlacedBlock {
                batch,
                text_cells,
                faults,
                homes: None,
            };
        };
        let kept: Vec<bool> = homes.iter().map(|home| (start..stop).contains(home)).collect();
        let mut placed_at: Vec<Option<u32>> = Vec::with_capacity(kept.len());
        let mut next = 0;
        for &keep in &kept {
            placed_at.push(keep.then_some(next));
            next += u32::from(keep);
        }
        let batch = filter_record_batch(&self.batch, &BooleanArray::from(kept)).expect("a mask of the batch's length filters it");
        let text_cells = self
            .text_cells
            .into_iter()
            .map(|column| {
                column
                    .into_iter()
                    .filter_map(|cell| placed_at[cell.row as usize].map(|row| TextCell { row, text: cell.text }))
                    .collect()
            })
            .collect();
        let faults = one_group(self.faults.placed(1, |row| placed_at[row as usize].map(|row| (0, row))));
        let homes = homes
            .iter()
            .copied()
            .filter(|home| (start..stop).contains(home))
            .map(|home| home - start)
            .collect();
        PlacedBlock {
            batch,
            text_cells,
            faults,
            homes: Some(homes),
        }
    }
}

fn one_group(mut groups: Vec<RowFaults>) -> RowFaults {
    groups.pop().expect("one group was asked for")
}

const _: () = {
    const fn shared_across_threads<T: Send + Sync>() {}
    shared_across_threads::<Plan>();
};

/// What a plan takes beside its profile, the profile's SHA-256, seed and rows, each left out by
/// default.
#[derive(Debug, Clone, Copy, Default)]
pub struct PlanInputs<'a> {
    /// The scenario whose perturbations apply to every block. The profile planned is the one
    /// [`Scenario::patched_profile`] made, and the profile's SHA-256 is the one before the patch,
    /// because the scenario's SHA-256 covers the patch.
    pub scenario: Option<AppliedScenario<'a>>,
    /// The columns pinned to given values. A pinned column takes its place in the draw order, so
    /// each column drawn from it reads the pinned values, and every column reading none draws as
    /// it does unpinned. A scenario's perturbations apply after. The identity names the overrides
    /// by their SHA-256.
    pub overrides: &'a [ColumnOverride],
}

/// Resolves a table profile into a plan for `rows` rows from `seed`. The library computes the
/// profile's `profile_sha256`, `sha256:` and the hex SHA-256 of its RFC 8785 canonical JSON.
pub fn plan(profile: &Profile, profile_sha256: &str, seed: u64, rows: u64) -> Result<Plan, PlanError> {
    plan_with(profile, profile_sha256, seed, rows, PlanInputs::default())
}

/// As [`plan`], with what `inputs` holds.
pub fn plan_with(profile: &Profile, profile_sha256: &str, seed: u64, rows: u64, inputs: PlanInputs<'_>) -> Result<Plan, PlanError> {
    let PlanInputs { scenario, overrides } = inputs;
    let table = match profile {
        Profile::Table(table) => table,
        Profile::Bundle(bundle) => {
            return Err(PlanError::Bundle { name: bundle.name.clone() });
        }
    };
    if !is_sha256(profile_sha256) {
        return Err(PlanError::ProfileSha256 {
            sha256: profile_sha256.into(),
        });
    }
    let mut identity = DatasetIdentity::new(profile_sha256, seed, rows);
    if let Some(applied) = scenario {
        if !is_sha256(applied.sha256) {
            return Err(PlanError::ScenarioSha256 {
                sha256: applied.sha256.into(),
            });
        }
        identity = identity.with_scenario(applied.sha256);
    }
    let inputs = TableInputs {
        scenario: scenario.map(|applied| applied.scenario),
        relations: None,
        overrides,
    };
    plan_table(table, identity, inputs)
}

/// What [`plan_table`] takes beside the table and its identity, each left out by default.
#[derive(Default)]
pub(crate) struct TableInputs<'a> {
    pub(crate) scenario: Option<&'a Scenario>,
    /// What a bundle's relationships draw, where the table is one of a bundle's.
    pub(crate) relations: Option<TableRelations<'a>>,
    pub(crate) overrides: &'a [ColumnOverride],
}

/// Plans `table` for the dataset `identity` names, under what `inputs` holds.
pub(crate) fn plan_table(table: &TableProfile, identity: DatasetIdentity, inputs: TableInputs<'_>) -> Result<Plan, PlanError> {
    let TableInputs {
        scenario,
        relations,
        overrides,
    } = inputs;
    let (seed, rows) = (identity.seed(), identity.rows());
    let columns = table.columns.as_deref().unwrap_or_default();
    if columns.is_empty() {
        return Err(PlanError::NoColumns { name: table.name.clone() });
    }
    let mut seen = BTreeSet::new();
    let context = plan_context(table, &table.name, seed, rows);
    let mut planned = Vec::with_capacity(columns.len());
    for (index, column) in columns.iter().enumerate() {
        if !seen.insert(column.name.as_str()) {
            return Err(PlanError::DuplicateColumn {
                column: column.name.clone(),
            });
        }
        planned.push(column_plan(&context, column, &ColumnSite::top_level(index, column))?);
    }
    let (related, fixed) = match relations {
        Some(relations) => {
            let (related, fixed) = related_tables::relate(relations, table, &mut planned, seed, rows)?;
            (Some(related), fixed)
        }
        None => (None, BTreeMap::new()),
    };
    let dependent = DependentColumns::plan(&context, table, &mut planned, &fixed)?;
    let pinned = {
        let fields: Vec<&arrow_schema::Field> = planned.iter().map(|column| column.field.as_ref()).collect();
        let type_mix: Vec<bool> = planned.iter().map(ColumnPlan::draws_type_mix).collect();
        column_overrides::pinned_columns(&fields, &type_mix, overrides, rows)?
    };
    let identity = if pinned.is_empty() {
        identity
    } else {
        identity.with_overrides(column_overrides::overrides_sha256(overrides))
    };
    let mut metadata = HashMap::from([
        (provenance::PROFILE_NAME.to_string(), table.name.clone()),
        (provenance::PROFILE_SHA256.to_string(), identity.profile_sha256().to_string()),
        (provenance::SEED.to_string(), seed.to_string()),
        (provenance::STREAM_VERSION.to_string(), identity.stream_version().to_string()),
        (provenance::DATASET_ROWS.to_string(), rows.to_string()),
        (provenance::DATASET_SHA256.to_string(), identity.sha256()),
    ]);
    if let Some(sha256) = identity.overrides_sha256() {
        let mut names: Vec<&str> = pinned.iter().map(|pin| planned[pin.index].field.name().as_str()).collect();
        names.sort_unstable();
        let names = serde_json::to_string(&names).expect("a list of strings serializes");
        metadata.insert(provenance::OVERRIDES_SHA256.to_string(), sha256.to_string());
        metadata.insert(provenance::OVERRIDDEN_COLUMNS.to_string(), names);
    }
    if let Some(sha256) = identity.scenario_sha256() {
        metadata.insert(provenance::SCENARIO_SHA256.to_string(), sha256.to_string());
    }
    if let Some(scenario) = scenario {
        metadata.insert(provenance::SCENARIO_NAME.to_string(), scenario.scenario.clone());
    }
    if let (Some(scale), Some(name)) = (identity.scale(), identity.table()) {
        metadata.insert(provenance::SCALE.to_string(), scale.to_string());
        metadata.insert(provenance::TABLE.to_string(), name.to_string());
    }
    let fields: Vec<_> = planned.iter().map(|column| column.field.clone()).collect();
    let mut plan = Plan {
        identity,
        generated_schema: Arc::new(Schema::new_with_metadata(fields, metadata)),
        columns: planned,
        dependent,
        block_count: rows.div_ceil(BLOCK_ROWS),
        stages: Vec::new(),
        drift: None,
        file_rows: None,
        malformed: None,
        cdc: None,
        across_files: Vec::new(),
        one_process: None,
        writes: Vec::new(),
        related,
        pinned,
        patched_fields: scenario.map(|scenario| patched_fields(scenario, columns)).unwrap_or_default(),
    };
    if let Some(scenario) = scenario {
        plan.apply_scenario(table, scenario)?;
    }
    Ok(plan)
}

/// The pointer of each field `scenario`'s patch sets, by the column of `columns` it sets it for.
fn patched_fields(scenario: &Scenario, columns: &[Column]) -> BTreeMap<String, Vec<String>> {
    let Some(Value::Object(patched)) = scenario.patch.as_ref().and_then(|patch| patch.0.get("columns")) else {
        return BTreeMap::new();
    };
    columns
        .iter()
        .enumerate()
        .filter_map(|(index, column)| {
            let mut pointers = Vec::new();
            leaf_pointers(patched.get(&column.name)?, &format!("/columns/{index}"), &mut pointers);
            Some((column.name.clone(), pointers))
        })
        .collect()
}

/// The pointer below `at` of each value `patch` sets, as RFC 6901 escapes a key.
fn leaf_pointers(patch: &Value, at: &str, pointers: &mut Vec<String>) {
    match patch {
        Value::Object(fields) if !fields.is_empty() => {
            for (key, inner) in fields {
                leaf_pointers(inner, &format!("{at}/{}", key.replace('~', "~0").replace('/', "~1")), pointers);
            }
        }
        _ => pointers.push(at.to_string()),
    }
}

/// What `table`'s column samplers are planned against for `rows` rows from `seed`, with streams
/// named for `name`.
pub(crate) fn plan_context<'a>(table: &'a TableProfile, name: &'a str, seed: u64, rows: u64) -> PlanContext<'a> {
    let source_rows = table.dataset.as_ref().and_then(|dataset| dataset.rows);
    let rows_read = table
        .generated
        .as_ref()
        .and_then(|g| g.sample.as_ref())
        .and_then(|sample| sample.rows_read);
    PlanContext {
        table: name,
        seed,
        rows: rows as f64,
        source_rows: source_rows.map(|rows| rows as f64),
        described_rows: rows_read.or(source_rows).map(|rows| rows as f64),
        k_min: table.privacy.as_ref().and_then(|privacy| privacy.k_min).unwrap_or(DEFAULT_K_MIN),
        keys: table.dataset.as_ref().and_then(|dataset| dataset.keys.as_ref()),
    }
}

/// Refuses an op beside `cdc` that would break its stream: one copying rows, which would hold a
/// key twice, one breaking records, and one writing a column every change holds or adds.
fn refuse_beside_cdc(scenario: &Scenario, cdc: &CdcPlan) -> Result<(), PlanError> {
    for (index, perturbation) in scenario.perturbations.iter().enumerate() {
        let refuse = |message: String| PlanError::Scenario {
            code: ScenarioFault::InvalidParameter,
            field: format!("/perturbations/{index}"),
            message,
        };
        match perturbation {
            Perturbation::Cdc(_) => {}
            Perturbation::Duplicates(_) => {
                return Err(refuse(
                    "duplicates copies rows, so a key would appear twice in cdc's full load".into(),
                ));
            }
            Perturbation::MalformedRows(_) => {
                return Err(refuse(
                    "malformed_rows breaks records, and cdc's faults are its duplicate_delivery_fraction and out_of_order_fraction".into(),
                ));
            }
            Perturbation::HotKeys(_)
            | Perturbation::LateArrival(_)
            | Perturbation::Missingness(_)
            | Perturbation::OutOfRange(_)
            | Perturbation::SwappedValues(_)
            | Perturbation::CategoricalShift(_)
            | Perturbation::Noise(_)
            | Perturbation::Scaling(_)
            | Perturbation::BrokenCharacters(_)
            | Perturbation::Outliers(_)
            | Perturbation::PartitionSkew(_)
            | Perturbation::SchemaDrift(_)
            | Perturbation::SmallFiles(_) => {
                let held = |column: &&str| cdc.holds_fixed(column) || *column == OPERATION_COLUMN || *column == cdc.timestamp_column();
                if let Some(column) = perturbation.written_columns().into_iter().find(held) {
                    return Err(refuse(format!(
                        "{} writes {column:?}, a column every change record of cdc holds as it is",
                        perturbation.op_name()
                    )));
                }
            }
        }
    }
    Ok(())
}

impl Plan {
    /// Plans each of `scenario`'s perturbations against the schema the one before it leaves. A
    /// `cdc` is planned against every other op's rows, which are its full load. A schema drift is
    /// planned last, because the writers apply it to a file's rows after every op has written them.
    fn apply_scenario(&mut self, table: &TableProfile, scenario: &Scenario) -> Result<(), PlanError> {
        let ids = scenario.resolved_ids().map_err(|duplicate| PlanError::Scenario {
            code: ScenarioFault::DuplicatePerturbationId,
            field: format!("/perturbations/{}/id", duplicate.index),
            message: format!(
                "id {:?} is an earlier perturbation's too, and two perturbations never share a stream",
                duplicate.id
            ),
        })?;
        let mut schema = self.generated_schema.clone();
        let mut drift_at: Option<usize> = None;
        let mut cdc_at: Option<usize> = None;
        for (index, perturbation) in scenario.perturbations.iter().enumerate() {
            let planning = Planning {
                table,
                seed: self.identity.seed(),
                rows: self.identity.rows(),
                block_count: self.block_count,
                schema: &schema,
                index,
                id: &ids[index],
                related: self.related.as_ref(),
            };
            if perturbation.needs_one_process() && self.one_process.is_none() {
                self.one_process = Some((format!("/perturbations/{index}"), perturbation.op_name()));
            }
            let mut written: Vec<String> = perturbation.written_columns().into_iter().map(String::from).collect();
            if let (Perturbation::HotKeys(op), Some(related)) = (perturbation, self.related.as_ref())
                && let Ok(column) = schema.index_of(&op.column)
                && let Some(drawn) = related.drawn_with(column)
            {
                let names = drawn.into_iter().map(|at| schema.field(at).name().clone());
                written.extend(names.filter(|name| *name != op.column));
            }
            self.writes.push((ids[index].clone(), written));
            let one_only = || {
                planning.refuse(
                    ScenarioFault::InvalidParameter,
                    "",
                    format!("a dataset takes one {}, and an earlier perturbation is one", perturbation.op_name()),
                )
            };
            if matches!(perturbation, Perturbation::Cdc(_)) && self.related.is_some() {
                return Err(planning.refuse(ScenarioFault::OpNotForFormat, "", "cdc of a bundle's table is not generated yet"));
            }
            let deferred = match perturbation {
                Perturbation::SchemaDrift(_) => Some(&mut drift_at),
                Perturbation::Cdc(_) => Some(&mut cdc_at),
                _ => None,
            };
            if let Some(at) = deferred {
                if at.replace(index).is_some() {
                    return Err(one_only());
                }
                continue;
            }
            match plan_perturbation(perturbation, &planning)? {
                Planned::Rows { op, schema: output } => {
                    if let Some((parameter, rows)) = op.source_reach() {
                        self.across_files.push((format!("/perturbations/{index}{parameter}"), rows));
                    }
                    self.stages.push(PlannedStage {
                        op,
                        schema: output.clone(),
                    });
                    schema = output;
                }
                Planned::SmallFiles(quantiles) => {
                    if self.file_rows.replace(quantiles).is_some() {
                        return Err(one_only());
                    }
                }
                Planned::Malformed(malformed) => {
                    if self.malformed.replace(malformed).is_some() {
                        return Err(one_only());
                    }
                }
                Planned::SchemaDrift(_) => unreachable!("a schema drift is planned after the loop"),
            }
        }
        if let Some(index) = cdc_at
            && let Perturbation::Cdc(op) = &scenario.perturbations[index]
        {
            let planning = Planning {
                table,
                seed: self.identity.seed(),
                rows: self.identity.rows(),
                block_count: self.block_count,
                schema: &schema,
                index,
                id: &ids[index],
                related: self.related.as_ref(),
            };
            let cdc = perturbations::plan_cdc(op, &planning, &self.columns, self.dependent.following())?;
            refuse_beside_cdc(scenario, &cdc)?;
            self.cdc = Some(cdc);
        }
        if let Some(index) = drift_at {
            let planning = Planning {
                table,
                seed: self.identity.seed(),
                rows: self.identity.rows(),
                block_count: self.block_count,
                schema: &schema,
                index,
                id: &ids[index],
                related: self.related.as_ref(),
            };
            if let Planned::SchemaDrift(drift) = plan_perturbation(&scenario.perturbations[index], &planning)? {
                self.drift = Some(drift);
            }
        }
        Ok(())
    }

    /// The Arrow schema every block's batch carries, with the provenance metadata.
    pub fn schema(&self) -> SchemaRef {
        self.stages
            .last()
            .map_or_else(|| self.generated_schema.clone(), |stage| stage.schema.clone())
    }

    /// The schema drift a scenario applies to the files after a row, where it applies one.
    pub fn drift(&self) -> Option<&SchemaDriftPlan> {
        self.drift.as_ref()
    }

    /// The file sizes a scenario's `small_files` draws, where it has one. A caller's own file size
    /// still overrides them.
    pub fn file_rows(&self) -> Option<&Quantiles> {
        self.file_rows.as_ref()
    }

    /// What a scenario's `malformed_rows` breaks, where it has one.
    pub fn malformed(&self) -> Option<&MalformedPlan> {
        self.malformed.as_ref()
    }

    /// The change stream a scenario's `cdc` writes beside the full load, where it has one.
    pub fn cdc(&self) -> Option<&CdcPlan> {
        self.cdc.as_ref()
    }

    /// Each perturbation's id and the columns it writes as planned, in the scenario's order. A
    /// `hot_keys` on a foreign key writes every column its relationship draws or copies.
    pub fn perturbation_writes(&self) -> &[(String, Vec<String>)] {
        &self.writes
    }

    /// The pointer to the first perturbation ordering the whole table, and its op, where one does.
    /// One process writes such a dataset.
    pub fn one_process_op(&self) -> Option<(&str, &str)> {
        self.one_process.as_ref().map(|(field, op)| (field.as_str(), *op))
    }

    /// The stream a scenario's `cdc` writes, counted on `threads` threads, or None without one. It
    /// refuses what [`Plan::cdc_stream`] refuses.
    pub fn cdc_outline(&self, threads: usize) -> Result<Option<StreamOutline>, PlanError> {
        self.cdc
            .as_ref()
            .map(|cdc| perturbations::cdc_outline(self, cdc, threads))
            .transpose()
    }

    /// The stream a scenario's `cdc` writes, built on `threads` threads, or None without one. It
    /// is refused where a schema drift's cut falls past every change.
    pub fn cdc_stream(&self, threads: usize) -> Result<Option<CdcStream>, PlanError> {
        self.cdc
            .as_ref()
            .map(|cdc| perturbations::cdc_stream(self, cdc, threads))
            .transpose()
    }

    /// Where a perturbation copies rows from across the dataset: the pointer to the parameter
    /// that sets it, and the fewest dataset rows that lie between a copy's home and its source. A
    /// file holding more rows than that could hold both. Of several, the nearest.
    pub fn across_files_reach(&self) -> Option<(&str, u64)> {
        let (field, rows) = self.across_files.iter().min_by_key(|(_, rows)| *rows)?;
        Some((field, *rows))
    }

    /// The rows the files hold: those generated, and every copy a perturbation plans for each
    /// block.
    pub fn rows_written(&self) -> u64 {
        let seed = self.identity.seed();
        (0..self.block_count)
            .map(|block| {
                let generated = (self.identity.rows() - block * BLOCK_ROWS).min(BLOCK_ROWS) as usize;
                self.stages
                    .iter()
                    .fold(generated, |rows, stage| rows + stage.op.added_rows(seed, block, rows)) as u64
            })
            .sum()
    }

    pub fn identity(&self) -> &DatasetIdentity {
        &self.identity
    }

    pub fn block_count(&self) -> u64 {
        self.block_count
    }

    /// How each top-level column is drawn, in schema order. Each parameter's value is read from
    /// `document`, the profile the plan was made from with any scenario's patch applied.
    pub fn column_traces(&self, document: &Value) -> Vec<ColumnTrace> {
        let following = self.dependent.following();
        let reads: BTreeMap<usize, Vec<usize>> = following.reads().collect();
        self.columns
            .iter()
            .enumerate()
            .map(|(index, column)| {
                let name = column.field.name().clone();
                let ops: Vec<String> = self
                    .writes
                    .iter()
                    .filter(|(_, written)| written.contains(&name))
                    .map(|(id, _)| id.clone())
                    .collect();
                let patched_fields = self.patched_fields.get(&name).cloned().unwrap_or_default();
                let origin = if self.pinned.iter().any(|pin| pin.index == index) {
                    ColumnOrigin::Pinned
                } else if !ops.is_empty() || !patched_fields.is_empty() {
                    ColumnOrigin::Overlaid
                } else if following.follows(index) {
                    ColumnOrigin::Derived
                } else {
                    ColumnOrigin::Profiled
                };
                let parameters = column
                    .read_fields
                    .iter()
                    .chain(self.dependent.sources(index))
                    .filter_map(|(parameter, field)| {
                        let value = document.pointer(field)?.clone();
                        Some(SamplerParameter {
                            name: parameter,
                            field: field.clone(),
                            value: value.into(),
                        })
                    })
                    .collect();
                let read_names = reads.get(&index).into_iter().flatten();
                ColumnTrace {
                    physical_type: physical_type_name(column.field.data_type()),
                    sampler_kind: column.kind,
                    summary: column.description(),
                    parameters,
                    null_fraction: column.null_share,
                    origin,
                    reads: read_names.map(|&read| self.columns[read].field.name().clone()).collect(),
                    ops,
                    patched_fields,
                    name,
                }
            })
            .collect()
    }

    /// The JSON Pointer into the profile of each column, nested fields included, that generates as
    /// text, with the raw type its own draws render as, in profile order. A column is text where its
    /// type mix holds other types, or where a bundle copies its values from a parent's text.
    pub fn type_mix_pointers(&self) -> Vec<(String, RawValueType)> {
        self.columns.iter().flat_map(ColumnPlan::type_mix_pointers).collect()
    }

    /// Whether top-level column `column` generates as text from a type mix.
    pub(crate) fn draws_type_mix(&self, column: usize) -> bool {
        self.columns[column].draws_type_mix()
    }

    /// The names of the columns each Gaussian copula draws together, each group sorted.
    pub fn copula_groups(&self) -> Vec<Vec<String>> {
        self.dependent.copula_groups()
    }

    /// The JSON pointer of each dependence entry the profile states and the plan does not draw, as a lookup or a
    /// conditional whose child another entry already draws, or one closing a cycle. Each child's sampler names why.
    pub fn set_aside(&self) -> Vec<String> {
        self.dependent.set_aside().to_vec()
    }

    fn position(&self, block: u64) -> Result<(BlockPosition, usize), BlockOutOfRange> {
        if block >= self.block_count {
            return Err(BlockOutOfRange {
                block,
                block_count: self.block_count,
            });
        }
        let first_row = block * BLOCK_ROWS;
        let rows = (self.identity.rows() - first_row).min(BLOCK_ROWS) as usize;
        let position = BlockPosition {
            seed: self.identity.seed(),
            block,
            first_row,
        };
        Ok((position, rows))
    }

    /// `block` as it reaches the perturbation at `through`, every earlier one applied. `cells`
    /// asks for the text cells too.
    pub(crate) fn block_through(&self, block: u64, through: usize, cells: bool) -> Block {
        let (position, rows) = self.position(block).expect("a perturbation reads a block inside the plan");
        self.staged(&position, rows, through, cells)
    }

    /// What a bundle's relationships draw for this table, where it is one of a bundle's.
    pub(crate) fn related(&self) -> Option<&RelatedColumns> {
        self.related.as_ref()
    }

    /// The schema the samplers' rows carry, before any perturbation.
    pub(crate) fn generated_schema(&self) -> &SchemaRef {
        &self.generated_schema
    }

    /// The rows `block` generates before any perturbation adds one.
    pub(crate) fn generated_rows(&self, block: u64) -> usize {
        self.position(block).map_or(0, |(_, rows)| rows)
    }

    /// `rows` rows drawn at `position`, every perturbation up to `through` applied.
    fn staged(&self, position: &BlockPosition, rows: usize, through: usize, cells: bool) -> Block {
        let options = RecordBatchOptions::new().with_row_count(Some(rows));
        let mut given = self.related.as_ref().map(|related| related.generate(&self.columns, position, rows));
        if !self.pinned.is_empty() {
            let laid = given.get_or_insert_with(|| (0..self.columns.len()).map(|_| None).collect());
            for pin in &self.pinned {
                laid[pin.index] = Some(GivenColumn {
                    array: pin.block(position.first_row, rows),
                    nulls: None,
                });
            }
        }
        let (arrays, cells) = match (self.dependent.is_empty(), given) {
            (true, None) => {
                let arrays = self.columns.iter().map(|column| column.generate(position, rows)).collect();
                let cells = cells.then(|| self.columns.iter().map(|column| column.text_cells(position, rows)).collect());
                (arrays, cells)
            }
            (_, given) => {
                let given = given.unwrap_or_default();
                let DrawnColumns { arrays, nulls } = self.dependent.generate(&self.columns, position, rows, &given);
                let cells = cells.then(|| {
                    self.columns
                        .iter()
                        .zip(&nulls)
                        .map(|(column, nulls)| column.text_cells_given(position, rows, nulls.as_ref()))
                        .collect()
                });
                (arrays, cells)
            }
        };
        let batch = RecordBatch::try_new_with_options(self.generated_schema.clone(), arrays, &options)
            .expect("every column's array matches its field");
        let mut placed = Block { batch, cells, homes: None };
        for (index, stage) in self.stages[..through].iter().enumerate() {
            let at = perturbations::Stage {
                plan: self,
                index,
                block: position.block,
                schema: &stage.schema,
            };
            placed = stage.op.apply(&at, placed);
        }
        placed
    }

    /// `rows` rows drawn at `position`, which may lie past the plan's blocks, every op applied.
    pub(crate) fn rows_at(&self, position: &BlockPosition, rows: usize) -> RecordBatch {
        self.staged(position, rows, self.stages.len(), false).batch
    }

    /// The rows of `block`, a pure function of the plan and the block number.
    pub fn generate_block(&self, block: u64) -> Result<RecordBatch, BlockOutOfRange> {
        self.position(block)?;
        Ok(self.block_through(block, self.stages.len(), false).batch)
    }

    /// The cells of `block` only a text format can carry, one list per column in schema order,
    /// each in row order. The batch [`Plan::generate_block`] returns holds a null at each.
    pub fn text_cells(&self, block: u64) -> Result<Vec<Vec<TextCell>>, BlockOutOfRange> {
        self.position(block)?;
        let rows = self.block_through(block, self.stages.len(), true);
        Ok(rows.cells.expect("cells were asked for"))
    }

    /// `block`'s rows as the writers file them, with its text cells where `text_cells` asks.
    pub fn placed_block(&self, block: u64, text_cells: bool) -> Result<PlacedBlock, BlockOutOfRange> {
        let (_, generated) = self.position(block)?;
        let rows = self.block_through(block, self.stages.len(), text_cells);
        let width = rows.batch.num_columns();
        let faults = self
            .malformed
            .as_ref()
            .map(|malformed| malformed.row_faults(block, generated, rows.batch.num_rows()))
            .unwrap_or_default();
        Ok(PlacedBlock {
            batch: rows.batch,
            text_cells: rows.cells.unwrap_or_else(|| vec![Vec::new(); width]),
            faults,
            homes: rows.homes,
        })
    }

    /// `placed`, the rows of `block`, in the shape the scenario's schema drift gives a file past
    /// its cut. Unchanged where no drift applies.
    pub fn drifted(&self, block: u64, placed: PlacedBlock) -> PlacedBlock {
        let Some(drift) = &self.drift else {
            return placed;
        };
        let rows = Block {
            batch: placed.batch,
            cells: Some(placed.text_cells),
            homes: placed.homes,
        };
        let drifted = drift.apply(self.identity.seed(), block, rows);
        PlacedBlock {
            batch: drifted.batch,
            text_cells: drifted.cells.expect("the cells went in"),
            faults: placed.faults,
            homes: drifted.homes,
        }
    }
}
