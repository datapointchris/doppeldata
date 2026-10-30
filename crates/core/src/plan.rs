//! A profile resolved into per-column samplers, and the pure function from a block number to that
//! block's rows.

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::BooleanArray;
use arrow_array::RecordBatch;
use arrow_array::RecordBatchOptions;
use arrow_schema::DataType;
use arrow_schema::Schema;
use arrow_schema::SchemaRef;
use arrow_select::filter::filter_record_batch;

use crate::identity::DatasetIdentity;
use crate::identity::is_sha256_digest;
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
use crate::profile::DEFAULT_K_MIN;
use crate::profile::FORMAT_VERSION;
use crate::profile::Profile;
use crate::profile::Quantiles;
use crate::profile::TableProfile;
use crate::profile::provenance;
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

#[derive(Debug, thiserror::Error)]
pub enum PlanError {
    #[error("profile {name:?} is a bundle; plan one table at a time")]
    Bundle { name: String },
    #[error("profile {name:?} has no columns to generate")]
    NoColumns { name: String },
    #[error("column {column:?} appears twice, and both would draw the same stream")]
    DuplicateColumn { column: String },
    #[error("profile digest {digest:?} is not `sha256:` and 64 lowercase hex digits")]
    ProfileDigest { digest: String },
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
    #[error("scenario digest {digest:?} is not `sha256:` and 64 lowercase hex digits")]
    ScenarioDigest { digest: String },
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
    /// `missing_tokens` whose rates, each a share of rows, sum above the column's null rate.
    TokensExceedNullRate,
    /// A dependence between columns names a column the table lacks.
    UnknownColumn,
    /// A dependence between columns the engine cannot draw: an expression that does not parse, a column of a
    /// kind the dependence cannot apply to, a value its column's type cannot hold, or one that would make a
    /// column depend on itself.
    InvalidDependence,
}

impl ColumnFault {
    /// The kebab-case code, in the form of the library's issue codes.
    pub fn code(self) -> &'static str {
        match self {
            Self::UnsupportedType => "unsupported-type",
            Self::NotGeneratedYet => "not-generated-yet",
            Self::InvalidFit => "invalid-fit",
            Self::InvalidPattern => "invalid-pattern",
            Self::InvalidQuantiles => "invalid-quantiles",
            Self::UnusableRates => "unusable-rates",
            Self::InvalidExtreme => "invalid-extreme",
            Self::StructWithoutFields => "struct-without-fields",
            Self::TokensExceedNullRate => "tokens-exceed-null-rate",
            Self::UnknownColumn => "unknown-column",
            Self::InvalidDependence => "invalid-dependence",
        }
    }
}

impl PlanError {
    /// The kebab-case code a caller branches on, in the form of the library's issue codes.
    pub fn code(&self) -> &'static str {
        match self {
            Self::Bundle { .. } => "bundle",
            Self::NoColumns { .. } => "no-columns",
            Self::DuplicateColumn { .. } => "duplicate-column",
            Self::ProfileDigest { .. } => "invalid-profile-digest",
            Self::Column { code, .. } => code.code(),
            Self::Scenario { code, .. } => code.code(),
            Self::ScenarioDigest { .. } => "invalid-scenario-digest",
        }
    }

    /// The RFC 6901 JSON Pointer that the refusal names: into the scenario where
    /// [`Self::in_scenario`] holds, else into the profile.
    pub fn field(&self) -> &str {
        match self {
            Self::Bundle { .. } => "/kind",
            Self::NoColumns { .. } | Self::DuplicateColumn { .. } => "/columns",
            Self::ProfileDigest { .. } | Self::ScenarioDigest { .. } => "",
            Self::Column { field, .. } | Self::Scenario { field, .. } => field,
        }
    }

    /// Whether the refusal is of the scenario rather than the profile.
    pub fn in_scenario(&self) -> bool {
        matches!(self, Self::Scenario { .. } | Self::ScenarioDigest { .. })
    }
}

impl std::fmt::Display for ScenarioFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.code())
    }
}

impl std::fmt::Display for ColumnFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.code())
    }
}

/// How one top-level column is drawn, as a plan reports it before generating anything.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnSampler {
    pub name: String,
    pub data_type: DataType,
    /// What draws the values, such as `sequence from 1 by 1` or `50 top values`.
    pub sampler: String,
    /// The share of rows drawn null.
    pub null_share: f64,
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
}

/// One perturbation applied to every block, and the schema its rows carry after it.
#[derive(Debug)]
struct PlannedStage {
    op: Box<dyn BlockPerturbation>,
    schema: SchemaRef,
}

/// A scenario as the engine applies it: the file read, and the digest the library computed over
/// its resolved document.
#[derive(Debug, Clone, Copy)]
pub struct AppliedScenario<'a> {
    pub scenario: &'a Scenario,
    pub digest: &'a str,
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

/// Resolves a table profile into a plan for `rows` rows from `seed`. The library computes the
/// profile's digest, `sha256:` and the hex SHA-256 of its RFC 8785 canonical JSON.
pub fn plan(profile: &Profile, profile_digest: &str, seed: u64, rows: u64) -> Result<Plan, PlanError> {
    plan_with_scenario(profile, profile_digest, None, seed, rows)
}

/// As [`plan`], with `scenario`'s perturbations applied to every block. `profile` is the one
/// [`Scenario::patched_profile`] made. `profile_digest` is the digest of the profile before the
/// patch, because the scenario's digest covers the patch.
pub fn plan_with_scenario(
    profile: &Profile,
    profile_digest: &str,
    scenario: Option<AppliedScenario<'_>>,
    seed: u64,
    rows: u64,
) -> Result<Plan, PlanError> {
    let table = match profile {
        Profile::Table(table) => table,
        Profile::Bundle(bundle) => {
            return Err(PlanError::Bundle { name: bundle.name.clone() });
        }
    };
    if !is_sha256_digest(profile_digest) {
        return Err(PlanError::ProfileDigest {
            digest: profile_digest.into(),
        });
    }
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
    let dependent = DependentColumns::plan(&context, table, &mut planned)?;
    let mut identity = DatasetIdentity::new(profile_digest, seed, rows);
    if let Some(applied) = scenario {
        if !is_sha256_digest(applied.digest) {
            return Err(PlanError::ScenarioDigest {
                digest: applied.digest.into(),
            });
        }
        identity = identity.with_scenario(applied.digest);
    }
    let mut metadata = HashMap::from([
        (provenance::PROFILE_NAME.to_string(), table.name.clone()),
        (provenance::PROFILE_VERSION.to_string(), FORMAT_VERSION.to_string()),
        (provenance::PROFILE_DIGEST.to_string(), profile_digest.to_string()),
        (provenance::SEED.to_string(), seed.to_string()),
        (provenance::STREAM_VERSION.to_string(), identity.stream_version().to_string()),
        (provenance::DATASET_ROWS.to_string(), rows.to_string()),
        (provenance::DATASET_DIGEST.to_string(), identity.digest()),
    ]);
    if let Some(applied) = scenario {
        metadata.insert(provenance::SCENARIO_NAME.to_string(), applied.scenario.scenario.clone());
        metadata.insert(provenance::SCENARIO_DIGEST.to_string(), applied.digest.to_string());
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
    };
    if let Some(applied) = scenario {
        plan.apply_scenario(table, applied.scenario)?;
    }
    Ok(plan)
}

/// What `table`'s column samplers are planned against for `rows` rows from `seed`, with streams
/// named for `name`.
pub(crate) fn plan_context<'a>(table: &TableProfile, name: &'a str, seed: u64, rows: u64) -> PlanContext<'a> {
    let source_rows = table.dataset.as_ref().and_then(|dataset| dataset.row_count);
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
                    "malformed_rows breaks records, and cdc's faults are its duplicate_delivery_rate and out_of_order_rate".into(),
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
            };
            if perturbation.needs_one_process() && self.one_process.is_none() {
                self.one_process = Some((format!("/perturbations/{index}"), perturbation.op_name()));
            }
            let one_only = || {
                planning.refuse(
                    ScenarioFault::InvalidParameter,
                    "",
                    format!("a dataset takes one {}, and an earlier perturbation is one", perturbation.op_name()),
                )
            };
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

    /// How each top-level column is drawn, in schema order.
    pub fn column_samplers(&self) -> Vec<ColumnSampler> {
        self.columns
            .iter()
            .map(|column| ColumnSampler {
                name: column.field.name().clone(),
                data_type: column.field.data_type().clone(),
                sampler: column.sampler.clone(),
                null_share: column.null_share,
            })
            .collect()
    }

    /// The names of the columns each Gaussian copula draws together, each group sorted.
    pub fn copula_groups(&self) -> Vec<Vec<String>> {
        self.dependent.copula_groups()
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

    /// `rows` rows drawn at `position`, every perturbation up to `through` applied.
    fn staged(&self, position: &BlockPosition, rows: usize, through: usize, cells: bool) -> Block {
        let options = RecordBatchOptions::new().with_row_count(Some(rows));
        let (arrays, cells) = match self.dependent.is_empty() {
            true => {
                let arrays = self.columns.iter().map(|column| column.generate(position, rows)).collect();
                let cells = cells.then(|| self.columns.iter().map(|column| column.text_cells(position, rows)).collect());
                (arrays, cells)
            }
            false => {
                let DrawnColumns { arrays, nulls } = self.dependent.generate(&self.columns, position, rows);
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
