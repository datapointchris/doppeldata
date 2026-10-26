//! `cdc`: a change stream in the layout AWS DMS writes to Amazon S3, beside the table a correct
//! merge of it produces. The full load is the plan's rows. Each of them may be updated, then
//! deleted, and new rows are inserted past them, every change committed inside the period.
//!
//! The stream is built a unit at a time. A unit is a block of the full load, or a block of
//! inserted rows numbered past the plan's blocks, so each unit's draws are its own streams'.

use std::collections::BTreeSet;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering as AtomicOrdering;

use arrow_array::Array;
use arrow_array::ArrayRef;
use arrow_array::RecordBatch;
use arrow_array::RecordBatchOptions;
use arrow_array::StringArray;
use arrow_array::new_null_array;
use arrow_schema::DataType;
use arrow_schema::Field;
use arrow_schema::FieldRef;
use arrow_schema::Schema;
use arrow_schema::SchemaRef;
use arrow_select::interleave::interleave;
use chrono::DateTime;
use chrono::Datelike;
use chrono::Timelike;
use chrono::Utc;
use rand_chacha::ChaCha8Rng;

use super::Block;
use super::Planning;
use super::ScenarioFault;
use crate::manifest::ChangeCounts;
use crate::plan::Plan;
use crate::plan::PlanError;
use crate::profile::Rate;
use crate::rng::BLOCK_ROWS;
use crate::rng::below;
use crate::rng::rng_for;
use crate::rng::unit;
use crate::samplers::BlockPosition;
use crate::samplers::ColumnPlan;
use crate::samplers::ColumnSite;
use crate::samplers::column_plan;
use crate::scenario::Cdc;
use crate::scenario::CdcStyle;
use crate::scenario::CdcTimestampUnit;
use crate::scenario::DateDelimiter;
use crate::scenario::DatePartition;
use crate::scenario::DateSequence;
use crate::scenario::OPERATION_COLUMN;
use crate::scenario::StreamFault;

/// DMS's `CdcMaxBatchInterval` default.
const DEFAULT_FILE_INTERVAL: i64 = 60_000_000;

/// The resolution of a change file's name: its last four digits count 100 µs.
const STAMP_MICROSECONDS: i64 = 100;

/// A `cdc` planned against the rows every other op writes.
#[derive(Debug)]
pub struct CdcPlan {
    index: usize,
    style: CdcStyle,
    prefix: String,
    data_schema: SchemaRef,
    /// Every column an update holds: the op's key, and each column of the profile's primary key.
    fixed: Vec<String>,
    /// Each column an update may change, by its place in the rows, with the sampler its new
    /// values are drawn from.
    updatable: Vec<(usize, ColumnPlan)>,
    full_load_rows: u64,
    block_count: u64,
    insert_rows: u64,
    updates: f64,
    deletes: f64,
    mean_updates: f64,
    start: i64,
    duration: i64,
    interval: i64,
    include_op: bool,
    timestamp_column: String,
    partition: Option<DatePartition>,
    timestamp_unit: Option<CdcTimestampUnit>,
    duplicate_rate: f64,
    out_of_order_rate: f64,
    changes_key: u64,
    times_key: u64,
    columns_key: u64,
    faults_key: u64,
}

pub(crate) fn plan(op: &Cdc, planning: &Planning<'_>, columns: &[ColumnPlan]) -> Result<CdcPlan, PlanError> {
    let invalid = |field: &str, message: String| planning.refuse(ScenarioFault::InvalidParameter, field, message);
    if op.schema.is_empty() || op.schema.contains('/') || op.schema == "." || op.schema == ".." {
        return Err(invalid(
            "/schema",
            format!("{:?} is not a name one path segment can hold", op.schema),
        ));
    }
    let primary: Vec<String> = (planning.table.dataset.as_ref())
        .and_then(|dataset| dataset.keys.as_ref())
        .and_then(|keys| keys.primary.as_ref())
        .map(|key| key.columns.clone())
        .unwrap_or_default();
    let key = match (&op.key, primary.as_slice()) {
        (Some(key), _) => key.clone(),
        (None, [only]) => only.clone(),
        (None, []) => {
            return Err(planning.refuse(
                ScenarioFault::NoPrimaryKey,
                "/key",
                "the profile states no dataset.keys.primary, so name the column that identifies a row with key",
            ));
        }
        (None, several) => {
            return Err(planning.refuse(
                ScenarioFault::NoPrimaryKey,
                "/key",
                format!("the profile's primary key is {several:?}; name one column that alone identifies a row with key"),
            ));
        }
    };
    let (key_index, _) = planning.column(&key, "/key")?;
    let unique = columns.get(key_index).is_some_and(ColumnPlan::draws_unique_values);
    if !unique {
        let drawn = columns.get(key_index).map_or("an op's values", |column| column.sampler.as_str());
        return Err(planning.refuse(
            ScenarioFault::OpNotForColumn,
            "/key",
            format!("{key:?} draws {drawn}, and a key holds a value no other row holds: a sequence or a uuid, with no nulls"),
        ));
    }
    let timestamp_column = op.timestamp_column().to_string();
    if timestamp_column == OPERATION_COLUMN || planning.schema.index_of(&timestamp_column).is_ok() {
        return Err(invalid(
            "/timestamp_column",
            format!("{timestamp_column:?} is a column every record holds already, so the commit time needs another name"),
        ));
    }
    if planning.schema.index_of(OPERATION_COLUMN).is_ok() {
        return Err(invalid(
            "",
            format!("the rows hold a column {OPERATION_COLUMN:?}, the name every change record's operation takes"),
        ));
    }
    let mut fixed = vec![key.clone()];
    fixed.extend(primary.iter().filter(|name| **name != key).cloned());
    let profile_columns = planning.table.columns.as_deref().unwrap_or_default();
    let sampler_table = planning.sampler_table();
    let context = planning.sampler_context(&sampler_table);
    let mut updatable = Vec::new();
    for (index, field) in planning.schema.fields().iter().enumerate() {
        let Some(column) = profile_columns.get(index).filter(|column| !fixed.contains(&column.name)) else {
            continue;
        };
        let sampler = column_plan(&context, column, &ColumnSite::top_level(index, column))?;
        if sampler.field.data_type() == field.data_type() {
            updatable.push((index, sampler));
        }
    }
    let rate = |rate: Option<Rate>| rate.map_or(0.0, |rate| rate.0);
    let (inserts, updates, deletes) = (rate(op.inserts), rate(op.updates), rate(op.deletes));
    if inserts == 0.0 && updates == 0.0 && deletes == 0.0 {
        return Err(invalid(
            "",
            "cdc names no change; set inserts, updates or deletes above zero".into(),
        ));
    }
    if updates > 0.0 && updatable.is_empty() {
        return Err(invalid(
            "/updates",
            "every column is a key column, so an update has nothing to change".into(),
        ));
    }
    let mean_updates = op.updates_per_row.as_ref().map_or(1.0, |spread| spread.mean);
    if !(mean_updates.is_finite() && mean_updates >= 1.0) {
        return Err(invalid(
            "/updates_per_row/mean",
            format!("{mean_updates} is not a count's mean, 1 or more"),
        ));
    }
    let (duplicate_rate, out_of_order_rate) = (rate(op.duplicate_delivery_rate), rate(op.out_of_order_rate));
    if duplicate_rate + out_of_order_rate > 1.0 {
        return Err(invalid(
            "/out_of_order_rate",
            "a change takes one fault at most, so duplicate_delivery_rate and out_of_order_rate sum to 1 at most".into(),
        ));
    }
    Ok(CdcPlan {
        index: planning.index,
        style: op.style,
        prefix: format!("{}/{}/", op.schema, planning.table.name),
        data_schema: planning.schema.clone(),
        fixed,
        updatable,
        full_load_rows: planning.rows,
        block_count: planning.block_count,
        insert_rows: libm::round(inserts * planning.rows as f64) as u64,
        updates,
        deletes,
        mean_updates,
        start: op.period.start.microseconds,
        duration: op.period.duration.microseconds,
        interval: op
            .file_interval
            .as_ref()
            .map_or(DEFAULT_FILE_INTERVAL, |interval| interval.microseconds),
        include_op: op.include_op_for_full_load,
        timestamp_column,
        partition: op.date_partition.clone().filter(|partition| partition.enabled),
        timestamp_unit: op.timestamp_unit,
        duplicate_rate,
        out_of_order_rate,
        changes_key: planning.key("changes"),
        times_key: planning.key("times"),
        columns_key: planning.key("columns"),
        faults_key: planning.key("faults"),
    })
}

impl CdcPlan {
    /// The pointer to the op's entry in the scenario.
    pub fn field(&self) -> String {
        format!("/perturbations/{}", self.index)
    }

    /// The layout's name, as the op's `style` gives it.
    pub fn style(&self) -> &'static str {
        match self.style {
            CdcStyle::Dms => "dms",
        }
    }

    /// Where the full load and the change files lie, `<schema>/<table>/`.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Where the expected table lies, `_expected/<schema>/<table>/`.
    pub fn expected_prefix(&self) -> String {
        format!("_expected/{}", self.prefix)
    }

    /// The path of full-load file `index`, from 0, with no extension: DMS counts them in hex.
    pub fn load_stem(&self, index: usize) -> String {
        format!("{}LOAD{:08X}", self.prefix, index + 1)
    }

    /// The unit the op names for a Parquet write's timestamps, where it names one.
    pub fn timestamp_unit(&self) -> Option<CdcTimestampUnit> {
        self.timestamp_unit
    }

    /// Whether the op places faults, so the write lists them in `_faults.jsonl`.
    pub fn places_faults(&self) -> bool {
        self.duplicate_rate > 0.0 || self.out_of_order_rate > 0.0
    }

    /// Whether `column` is one every update holds.
    pub fn holds_fixed(&self, column: &str) -> bool {
        self.fixed.iter().any(|fixed| fixed == column)
    }

    /// The column each record's commit time is written in.
    pub fn timestamp_column(&self) -> &str {
        &self.timestamp_column
    }

    /// The schema of a full-load file whose rows carry `data`.
    pub fn full_load_schema(&self, data: &SchemaRef) -> SchemaRef {
        record_schema(data, self.include_op, &self.timestamp_column)
    }

    /// `batch`, rows of the full load, as a full-load file holds them: after `Op` where the op
    /// includes it, and the period's start as every row's commit time.
    pub fn full_load(&self, batch: &RecordBatch) -> RecordBatch {
        let rows = batch.num_rows();
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns() + 2);
        if self.include_op {
            columns.push(Arc::new(StringArray::from_iter_values(std::iter::repeat_n("I", rows))));
        }
        let start = commit_text(self.start);
        columns.push(Arc::new(StringArray::from_iter_values(std::iter::repeat_n(start.as_str(), rows))));
        columns.extend(batch.columns().iter().cloned());
        let schema = self.full_load_schema(&batch.schema());
        let options = RecordBatchOptions::new().with_row_count(Some(rows));
        RecordBatch::try_new_with_options(schema, columns, &options).expect("a full-load record holds its columns")
    }

    /// The first instant past the drift's cut, at a change file name's resolution.
    fn cut(&self, plan: &Plan) -> Option<i64> {
        let drift = plan.drift()?;
        let cut = self.start + libm::ceil(drift.after_fraction() * self.duration as f64) as i64;
        Some((cut + STAMP_MICROSECONDS - 1).div_euclid(STAMP_MICROSECONDS) * STAMP_MICROSECONDS)
    }

    /// The start of the change file `commit` is written in, counted in 100 µs: the interval it
    /// falls in, cut at its date folder and at the drift's cut.
    fn piece(&self, commit: i64, cut: Option<i64>) -> i64 {
        let mut start = self.start + (commit - self.start).div_euclid(self.interval) * self.interval;
        if let Some(partition) = &self.partition {
            start = start.max(partition_floor(partition.sequence, commit));
        }
        if let Some(cut) = cut.filter(|&cut| commit >= cut) {
            start = start.max(cut);
        }
        start.div_euclid(STAMP_MICROSECONDS)
    }

    /// The path, with no extension, of the change file starting at `piece`.
    fn file_stem(&self, piece: i64) -> String {
        let instant = utc(piece * STAMP_MICROSECONDS);
        let folder = self
            .partition
            .as_ref()
            .map(|partition| folder(partition, &instant))
            .unwrap_or_default();
        let tenths = (piece * STAMP_MICROSECONDS).rem_euclid(1_000_000) / STAMP_MICROSECONDS;
        format!("{}{folder}{}{tenths:04}", self.prefix, instant.format("%Y%m%d-%H%M%S"))
    }

    fn commit_time(&self, rng: &mut ChaCha8Rng) -> i64 {
        self.start + 1 + libm::floor(unit(rng) * self.duration as f64) as i64
    }

    /// 1, 2, 3 and on, each `1 − 1/mean` as likely as the one before, by inverting its CDF.
    fn update_count(&self, draw: f64) -> u64 {
        let stay = 1.0 - 1.0 / self.mean_updates;
        if stay <= 0.0 {
            return 1;
        }
        1 + libm::floor(libm::log(1.0 - draw) / libm::log(stay)) as u64
    }

    /// The position of the unit holding full-load block `block`, and the rows it holds.
    fn full_load_position(&self, seed: u64, block: u64) -> (BlockPosition, usize) {
        let first_row = block * BLOCK_ROWS;
        let position = BlockPosition { seed, block, first_row };
        (position, (self.full_load_rows - first_row).min(BLOCK_ROWS) as usize)
    }

    /// The changes to the `rows` rows at `position`: each row inserted where `inserted`, then
    /// updated, then deleted, as its draws fall, at commit times strictly rising for its key.
    fn row_changes(&self, position: &BlockPosition, rows: usize, inserted: bool) -> Vec<Change> {
        let mut draws = rng_for(position.seed, self.changes_key, position.block);
        let mut times = rng_for(position.seed, self.times_key, position.block);
        let mut changes = Vec::new();
        for row in position.first_row..position.first_row + rows as u64 {
            let (updated, count, deleted) = (unit(&mut draws), unit(&mut draws), unit(&mut draws));
            let updates = if updated < self.updates { self.update_count(count) } else { 0 };
            let deleted = deleted < self.deletes;
            let total = u64::from(inserted) + updates + u64::from(deleted);
            if total == 0 {
                continue;
            }
            let mut commits: Vec<i64> = (0..total).map(|_| self.commit_time(&mut times)).collect();
            commits.sort_unstable();
            for at in 1..commits.len() {
                commits[at] = commits[at].max(commits[at - 1] + 1);
            }
            let last = changes.len() + total as usize - 1;
            for (seq, commit) in commits.into_iter().enumerate() {
                let operation = if inserted && seq == 0 {
                    Operation::Insert
                } else if deleted && seq + 1 == total as usize {
                    Operation::Delete
                } else {
                    Operation::Update
                };
                changes.push(Change {
                    operation,
                    commit,
                    row,
                    seq: seq as u32,
                    last,
                    fault: None,
                });
            }
        }
        changes
    }

    /// Inserted block `insert`'s position, and the rows it holds.
    fn insert_position(&self, seed: u64, insert: u64) -> (BlockPosition, usize) {
        let position = BlockPosition {
            seed,
            block: self.block_count + insert,
            first_row: self.full_load_rows + insert * BLOCK_ROWS,
        };
        (position, (self.insert_rows - insert * BLOCK_ROWS).min(BLOCK_ROWS) as usize)
    }

    /// The position of unit `at`, full-load blocks first, then inserted blocks, and the rows it
    /// holds.
    fn unit_position(&self, seed: u64, at: u64) -> (BlockPosition, usize) {
        match at.checked_sub(self.block_count) {
            None => self.full_load_position(seed, at),
            Some(insert) => self.insert_position(seed, insert),
        }
    }

    /// Each change's image of its row, the rows' columns drawn from `base`, the unit's rows at
    /// `position`, and an update's new values from the op's own samplers.
    fn images(&self, position: &BlockPosition, base: &RecordBatch, changes: &[Change]) -> RecordBatch {
        let (seed, block, first_row) = (position.seed, position.block, position.first_row);
        let updates = changes.iter().filter(|change| change.operation == Operation::Update).count();
        let drawn: Vec<ArrayRef> = self
            .updatable
            .iter()
            .map(|(_, sampler)| sampler.generate(position, updates))
            .collect();
        let width = base.num_columns();
        let mut updatable_at: Vec<Option<usize>> = vec![None; width];
        for (at, (index, _)) in self.updatable.iter().enumerate() {
            updatable_at[*index] = Some(at);
        }
        let mut choices = rng_for(seed, self.columns_key, block);
        let mut current: Vec<Option<usize>> = vec![None; self.updatable.len()];
        let mut sources: Vec<Vec<(usize, usize)>> = vec![Vec::with_capacity(changes.len()); width];
        let mut next_drawn = 0;
        let share = 1.0 / self.updatable.len().max(1) as f64;
        for change in changes {
            let base_row = (change.row - first_row) as usize;
            if change.seq == 0 {
                current.fill(None);
            }
            if change.operation == Operation::Update {
                let chosen = below(&mut choices, self.updatable.len() as u32) as usize;
                for (at, source) in current.iter_mut().enumerate() {
                    if at == chosen || unit(&mut choices) < share {
                        *source = Some(next_drawn);
                    }
                }
                next_drawn += 1;
            }
            for (column, sources) in sources.iter_mut().enumerate() {
                sources.push(match updatable_at[column].and_then(|at| current[at]) {
                    Some(drawn) => (1, drawn),
                    None => (0, base_row),
                });
            }
        }
        let arrays: Vec<ArrayRef> = (0..width)
            .map(|column| {
                let mut from: Vec<&dyn Array> = vec![base.column(column).as_ref()];
                if let Some(at) = updatable_at[column] {
                    from.push(drawn[at].as_ref());
                }
                interleave(&from, &sources[column]).expect("every source is a row of its array")
            })
            .collect();
        batch(base.schema(), arrays, changes.len())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Operation {
    Insert,
    Update,
    Delete,
}

impl Operation {
    fn letter(self) -> &'static str {
        match self {
            Self::Insert => "I",
            Self::Update => "U",
            Self::Delete => "D",
        }
    }
}

/// One change to one key.
#[derive(Debug, Clone, Copy)]
struct Change {
    operation: Operation,
    commit: i64,
    /// The dataset row of the change's key.
    row: u64,
    /// The change's place among its key's, from 0.
    seq: u32,
    /// The unit's record of its key's last change.
    last: usize,
    fault: Option<StreamFault>,
}

/// One unit's changes and each record's image. The rows it leaves at the period's end are
/// rebuilt from its block when written, so the stream never holds a second copy of the table.
#[derive(Debug)]
struct Unit {
    position: BlockPosition,
    rows: usize,
    changes: Vec<Change>,
    images: RecordBatch,
    /// The images in the drifted shape, where a drift applies.
    drifted: Option<RecordBatch>,
}

/// Where one record is written. The fields' order is the files' order and each file's record
/// order: by file, then commit time, then key.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Placement {
    piece: usize,
    commit: i64,
    row: u64,
    seq: u32,
    /// Whether this is a duplicate delivery's second.
    copy: bool,
    unit: usize,
    record: usize,
}

/// One change file: its path with no extension, and each record by its unit and its place there.
#[derive(Debug)]
struct ChangeFile {
    stem: String,
    drifted: bool,
    records: Vec<(usize, usize)>,
    faults: Vec<StreamFaultRecord>,
}

/// A fault `cdc` placed in a change file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamFaultRecord {
    pub kind: StreamFault,
    /// The record's place in its file, from 0.
    pub record: u64,
    /// The dataset row of the record's key.
    pub row: u64,
    /// The record's commit time, as its timestamp column prints it.
    pub commit: String,
    /// The path, with no extension, of the file the change's commit time falls in. For a
    /// duplicate delivery, that file holds its first.
    pub due_stem: String,
}

/// The change stream a `cdc` writes, and the table a correct merge of it produces.
#[derive(Debug)]
pub struct CdcStream {
    record_schema: SchemaRef,
    drifted_schema: Option<SchemaRef>,
    expected_schema: SchemaRef,
    units: Vec<Unit>,
    full_load_units: usize,
    files: Vec<ChangeFile>,
    changes: ChangeCounts,
    /// The drift's commit-time cut, where a drift applies.
    cut: Option<i64>,
}

impl CdcStream {
    /// The changes the stream holds, counted by operation, a second delivery left out.
    pub fn changes(&self) -> ChangeCounts {
        self.changes
    }

    pub fn file_count(&self) -> usize {
        self.files.len()
    }

    /// Every schema a change file carries: the rows', and the drifted one where a drift applies.
    pub fn file_schemas(&self) -> Vec<SchemaRef> {
        [Some(self.record_schema.clone()), self.drifted_schema.clone()]
            .into_iter()
            .flatten()
            .collect()
    }

    /// Change file `index`'s path under the destination, with no extension.
    pub fn file_stem(&self, index: usize) -> &str {
        &self.files[index].stem
    }

    /// The faults placed in change file `index`, in record order.
    pub fn file_faults(&self, index: usize) -> &[StreamFaultRecord] {
        &self.files[index].faults
    }

    /// Change file `index`'s records: `Op`, the commit time, then the rows' columns.
    pub fn file(&self, index: usize) -> RecordBatch {
        let file = &self.files[index];
        let schema = match (&self.drifted_schema, file.drifted) {
            (Some(drifted), true) => drifted.clone(),
            _ => self.record_schema.clone(),
        };
        let image = |unit: &Unit| match (&unit.drifted, file.drifted) {
            (Some(drifted), true) => drifted.clone(),
            _ => unit.images.clone(),
        };
        let images: Vec<RecordBatch> = self.units.iter().map(image).collect();
        let change = |&(unit, record): &(usize, usize)| self.units[unit].changes[record];
        let operations = StringArray::from_iter_values(file.records.iter().map(|at| change(at).operation.letter()));
        let commits = StringArray::from_iter_values(file.records.iter().map(|at| commit_text(change(at).commit)));
        let mut columns: Vec<ArrayRef> = vec![Arc::new(operations), Arc::new(commits)];
        for column in 0..schema.fields().len() - 2 {
            let from: Vec<&dyn Array> = images.iter().map(|images| images.column(column).as_ref()).collect();
            columns.push(interleave(&from, &file.records).expect("every record is a row of its unit's images"));
        }
        batch(schema, columns, file.records.len())
    }

    /// The schema of the table a correct merge produces.
    pub fn expected_schema(&self) -> SchemaRef {
        self.expected_schema.clone()
    }

    /// The rows a correct merge leaves of full-load rows `rows` of `plan`, the plan the stream was
    /// built from, in row order, one batch per block. Each block is merged as it is taken.
    pub fn expected_rows<'a>(&'a self, plan: &'a Plan, rows: Range<u64>) -> impl Iterator<Item = RecordBatch> + 'a {
        let first = (rows.start / BLOCK_ROWS) as usize;
        let last = (rows.end.div_ceil(BLOCK_ROWS) as usize).min(self.full_load_units);
        (first..last.max(first)).map(move |unit| {
            let (expected, expected_rows) = self.merged(plan, unit);
            let from = expected_rows.partition_point(|&row| row < rows.start);
            let to = expected_rows.partition_point(|&row| row < rows.end);
            expected.slice(from, to - from)
        })
    }

    pub fn inserts_rows(&self) -> bool {
        self.units.len() > self.full_load_units
    }

    /// The rows the stream inserts that a correct merge leaves, in row order, one batch per block
    /// of inserts. Empty where the stream inserts none.
    pub fn expected_inserts<'a>(&'a self, plan: &'a Plan) -> impl Iterator<Item = RecordBatch> + 'a {
        (self.full_load_units..self.units.len()).map(move |unit| self.merged(plan, unit).0)
    }

    /// The rows unit `at` leaves at the period's end, in row order, with each one's dataset row,
    /// its rows of `plan` regenerated.
    fn merged(&self, plan: &Plan, at: usize) -> (RecordBatch, Vec<u64>) {
        let unit = &self.units[at];
        let base = plan.rows_at(&unit.position, unit.rows);
        let (expected, expected_rows, finals) = survivors(&base, unit.position.first_row, &unit.images, &unit.changes);
        let (Some(drift), Some(cut), Some(drifted)) = (plan.drift(), self.cut, &unit.drifted) else {
            return (expected, expected_rows);
        };
        let (seed, block) = (unit.position.seed, unit.position.block);
        let mut columns = drift.apply(seed, block, block_of(expected)).batch.columns().to_vec();
        for position in drift.added_positions() {
            let nulls = new_null_array(drifted.column(position).data_type(), 1);
            let sources: Vec<(usize, usize)> = finals
                .iter()
                .map(|last| match last {
                    Some(last) if unit.changes[*last].commit >= cut => (0, *last),
                    _ => (1, 0),
                })
                .collect();
            columns[position] =
                interleave(&[drifted.column(position).as_ref(), nulls.as_ref()], &sources).expect("every source is a row of its array");
        }
        let expected = batch(nullable_added(drift.schema(), &drift.added_positions()), columns, finals.len());
        (expected, expected_rows)
    }
}

/// What a `cdc` writes, counted before its stream is built.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamOutline {
    /// The changes the stream holds, counted by operation, a second delivery left out.
    pub changes: ChangeCounts,
    /// The change files the changes fall in. A file whose every change a fault moves later is
    /// left unwritten, so the stream may write fewer.
    pub change_files_at_most: u64,
}

fn counted(counts: &mut ChangeCounts, operation: Operation) {
    match operation {
        Operation::Insert => counts.insert += 1,
        Operation::Update => counts.update += 1,
        Operation::Delete => counts.delete += 1,
    }
}

/// The units a stream draws: each full-load block, then each block of inserts.
fn unit_count(cdc: &CdcPlan) -> usize {
    (cdc.block_count + cdc.insert_rows.div_ceil(BLOCK_ROWS)) as usize
}

/// Refuses a drift whose cut falls past the last change file, `last`, which no file would
/// carry the drifted shape of.
fn refuse_cut_past_changes(plan: &Plan, cut: Option<i64>, last: Option<i64>) -> Result<(), PlanError> {
    match (plan.drift(), cut) {
        (Some(drift), Some(cut)) if last.is_none_or(|last| last * STAMP_MICROSECONDS < cut) => Err(PlanError::Scenario {
            code: ScenarioFault::InvalidParameter,
            field: drift.cut_field(),
            message: format!(
                "no change commits at or past {}, the drift's share of the period, so no change file would carry its shape",
                commit_text(cut)
            ),
        }),
        _ => Ok(()),
    }
}

/// Counts `cdc`'s stream over `plan`'s rows on `threads` threads, each unit's changes drawn and
/// dropped once counted. It refuses what [`stream`] refuses.
pub(crate) fn outline(plan: &Plan, cdc: &CdcPlan, threads: usize) -> Result<StreamOutline, PlanError> {
    let seed = plan.identity().seed();
    let cut = cdc.cut(plan);
    let per_unit = on_threads(unit_count(cdc), threads, |unit| {
        let (position, rows) = cdc.unit_position(seed, unit as u64);
        let mut counts = ChangeCounts::default();
        let mut pieces = BTreeSet::new();
        for change in cdc.row_changes(&position, rows, unit as u64 >= cdc.block_count) {
            counted(&mut counts, change.operation);
            pieces.insert(cdc.piece(change.commit, cut));
        }
        (counts, pieces)
    });
    let mut changes = ChangeCounts::default();
    let mut pieces = BTreeSet::new();
    for (counts, unit_pieces) in per_unit {
        changes.insert += counts.insert;
        changes.update += counts.update;
        changes.delete += counts.delete;
        pieces.extend(unit_pieces);
    }
    refuse_cut_past_changes(plan, cut, pieces.last().copied())?;
    Ok(StreamOutline {
        changes,
        change_files_at_most: pieces.len() as u64,
    })
}

/// Builds `cdc`'s stream over `plan`'s rows on `threads` threads. The bytes are the same on any
/// number, because each unit draws from its own streams.
pub(crate) fn stream(plan: &Plan, cdc: &CdcPlan, threads: usize) -> Result<CdcStream, PlanError> {
    let seed = plan.identity().seed();
    let full_load_units = cdc.block_count as usize;
    let count = unit_count(cdc);
    let changes = on_threads(count, threads, |unit| {
        let (position, rows) = cdc.unit_position(seed, unit as u64);
        cdc.row_changes(&position, rows, unit >= full_load_units)
    });
    let cut = cdc.cut(plan);
    let pieces: Vec<i64> = changes
        .iter()
        .flatten()
        .map(|change| cdc.piece(change.commit, cut))
        .collect::<BTreeSet<i64>>()
        .into_iter()
        .collect();
    refuse_cut_past_changes(plan, cut, pieces.last().copied())?;
    let piece_at = |commit: i64| {
        pieces
            .binary_search(&cdc.piece(commit, cut))
            .expect("every change's file is a piece")
    };
    let units: Vec<Unit> = on_threads(count, threads, |unit| {
        let mut changes = changes[unit].clone();
        let (position, rows) = cdc.unit_position(seed, unit as u64);
        let images = cdc.images(&position, &plan.rows_at(&position, rows), &changes);
        place_faults(cdc, seed, position.block, &images, &mut changes, |commit| {
            piece_at(commit) + 1 < pieces.len()
        });
        let drifted = match (plan.drift(), cut) {
            (Some(drift), Some(_)) => Some(drift.apply(seed, position.block, block_of(images.clone())).batch),
            _ => None,
        };
        Unit {
            position,
            rows,
            changes,
            images,
            drifted,
        }
    });
    let mut placed: Vec<Placement> = Vec::new();
    let mut counts = ChangeCounts::default();
    for (unit_at, unit) in units.iter().enumerate() {
        for (record, change) in unit.changes.iter().enumerate() {
            counted(&mut counts, change.operation);
            let own = piece_at(change.commit);
            let later = piece_at(unit.changes[change.last].commit) + 1;
            let at = |piece: usize, copy: bool| Placement {
                piece,
                commit: change.commit,
                row: change.row,
                seq: change.seq,
                copy,
                unit: unit_at,
                record,
            };
            match change.fault {
                Some(StreamFault::OutOfOrder) => placed.push(at(later, false)),
                Some(StreamFault::DuplicateDelivery) => {
                    placed.push(at(own, false));
                    placed.push(at(later, true));
                }
                None => placed.push(at(own, false)),
            }
        }
    }
    placed.sort_unstable();
    let drifted_from = cut.map(|cut| cut.div_euclid(STAMP_MICROSECONDS));
    let mut files: Vec<ChangeFile> = Vec::new();
    let mut file_piece = None;
    for placement in placed {
        if file_piece != Some(placement.piece) {
            file_piece = Some(placement.piece);
            files.push(ChangeFile {
                stem: cdc.file_stem(pieces[placement.piece]),
                drifted: drifted_from.is_some_and(|from| pieces[placement.piece] >= from),
                records: Vec::new(),
                faults: Vec::new(),
            });
        }
        let file = files.last_mut().expect("a file was just pushed");
        let faulted = match units[placement.unit].changes[placement.record].fault {
            Some(StreamFault::DuplicateDelivery) if !placement.copy => None,
            fault => fault,
        };
        if let Some(kind) = faulted {
            file.faults.push(StreamFaultRecord {
                kind,
                record: file.records.len() as u64,
                row: placement.row,
                commit: commit_text(placement.commit),
                due_stem: cdc.file_stem(pieces[piece_at(placement.commit)]),
            });
        }
        file.records.push((placement.unit, placement.record));
    }
    let drifted_schema = plan
        .drift()
        .map(|drift| record_schema(&drift.schema(), true, &cdc.timestamp_column));
    let expected_schema = match plan.drift() {
        Some(drift) => nullable_added(drift.schema(), &drift.added_positions()),
        None => cdc.data_schema.clone(),
    };
    Ok(CdcStream {
        record_schema: record_schema(&cdc.data_schema, true, &cdc.timestamp_column),
        drifted_schema,
        expected_schema,
        units,
        full_load_units,
        files,
        changes: counts,
        cut,
    })
}

/// Draws whether each change a later change of its key follows takes a fault. One draw per such
/// change, so a rate moves no other change's. It takes one only where a merge in file order would
/// end on a different image, and where a file after its key's last change exists to hold it.
fn place_faults(cdc: &CdcPlan, seed: u64, block: u64, images: &RecordBatch, changes: &mut [Change], has_later_file: impl Fn(i64) -> bool) {
    if cdc.duplicate_rate == 0.0 && cdc.out_of_order_rate == 0.0 {
        return;
    }
    let mut faults = rng_for(seed, cdc.faults_key, block);
    for at in 0..changes.len() {
        let last = changes[at].last;
        if last == at {
            continue;
        }
        let draw = unit(&mut faults);
        let kind = match draw {
            draw if draw < cdc.out_of_order_rate => StreamFault::OutOfOrder,
            draw if draw < cdc.out_of_order_rate + cdc.duplicate_rate => StreamFault::DuplicateDelivery,
            _ => continue,
        };
        let differs = changes[last].operation == Operation::Delete || !same_image(images, at, last);
        if differs && has_later_file(changes[last].commit) {
            changes[at].fault = Some(kind);
        }
    }
}

/// The rows of a unit left at the period's end, in row order: each key's last image where it
/// changed and was not deleted, else its full-load row. With each, the record of its last change.
fn survivors(base: &RecordBatch, first_row: u64, images: &RecordBatch, changes: &[Change]) -> (RecordBatch, Vec<u64>, Vec<Option<usize>>) {
    let mut sources = Vec::with_capacity(base.num_rows());
    let mut rows = Vec::with_capacity(base.num_rows());
    let mut finals = Vec::with_capacity(base.num_rows());
    let mut next = 0;
    for at in 0..base.num_rows() {
        let row = first_row + at as u64;
        let last = (next < changes.len() && changes[next].row == row).then(|| changes[next].last);
        if let Some(last) = last {
            next = last + 1;
            if changes[last].operation == Operation::Delete {
                continue;
            }
        }
        sources.push(last.map_or((0, at), |last| (1, last)));
        rows.push(row);
        finals.push(last);
    }
    let columns: Vec<ArrayRef> = (0..base.num_columns())
        .map(|column| {
            interleave(&[base.column(column).as_ref(), images.column(column).as_ref()], &sources)
                .expect("every source is a row of its array")
        })
        .collect();
    (batch(base.schema(), columns, rows.len()), rows, finals)
}

fn same_image(images: &RecordBatch, a: usize, b: usize) -> bool {
    images
        .columns()
        .iter()
        .all(|column| column.slice(a, 1).to_data() == column.slice(b, 1).to_data())
}

fn block_of(batch: RecordBatch) -> Block {
    Block {
        batch,
        cells: None,
        homes: None,
    }
}

fn batch(schema: SchemaRef, columns: Vec<ArrayRef>, rows: usize) -> RecordBatch {
    let options = RecordBatchOptions::new().with_row_count(Some(rows));
    RecordBatch::try_new_with_options(schema, columns, &options).expect("every column matches its field")
}

/// `data` after `Op`, where `with_op`, and the commit-time column.
fn record_schema(data: &SchemaRef, with_op: bool, timestamp_column: &str) -> SchemaRef {
    let mut fields: Vec<FieldRef> = Vec::with_capacity(data.fields().len() + 2);
    if with_op {
        fields.push(Arc::new(Field::new(OPERATION_COLUMN, DataType::Utf8, false)));
    }
    fields.push(Arc::new(Field::new(timestamp_column, DataType::Utf8, false)));
    fields.extend(data.fields().iter().cloned());
    Arc::new(Schema::new_with_metadata(fields, data.metadata().clone()))
}

/// `schema` with each added column nullable, because a key whose last change preceded the drift
/// holds none.
fn nullable_added(schema: SchemaRef, added: &[usize]) -> SchemaRef {
    let fields: Vec<FieldRef> = (schema.fields().iter().enumerate())
        .map(|(at, field)| match added.contains(&at) {
            true => Arc::new(field.as_ref().clone().with_nullable(true)),
            false => field.clone(),
        })
        .collect();
    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

fn utc(microseconds: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_micros(microseconds).expect("a period's instants are inside chrono's range")
}

/// An instant as DMS's `TimestampColumnName` writes it, `yyyy-MM-dd HH:mm:ss.SSSSSS`.
fn commit_text(microseconds: i64) -> String {
    utc(microseconds).format("%Y-%m-%d %H:%M:%S%.6f").to_string()
}

/// Whether `name`, a `/`-separated path under a stream's prefix, is a full-load or change file a
/// stream writes with `extension`.
pub fn names_stream_file(name: &str, extension: &str) -> bool {
    let Some(stem) = name.strip_suffix(extension).and_then(|stem| stem.strip_suffix('.')) else {
        return false;
    };
    let (folders, file) = stem.rsplit_once('/').unwrap_or(("", stem));
    let digits = |text: &str| text.bytes().all(|byte| byte.is_ascii_digit());
    if let Some(count) = file.strip_prefix("LOAD") {
        let hex = |byte: u8| byte.is_ascii_digit() || (b'A'..=b'F').contains(&byte);
        return folders.is_empty() && count.len() == 8 && count.bytes().all(hex);
    }
    let stamped = file
        .split_once('-')
        .is_some_and(|(day, time)| day.len() == 8 && time.len() == 10 && digits(day) && digits(time));
    let dated = |folder: &str| !folder.is_empty() && folder.bytes().all(|byte| byte.is_ascii_digit() || byte == b'-' || byte == b'_');
    stamped && (folders.is_empty() || folders.split('/').all(dated))
}

/// The first instant of the date folder `commit` falls in.
fn partition_floor(sequence: DateSequence, commit: i64) -> i64 {
    let instant = utc(commit).naive_utc();
    let floor = match sequence {
        DateSequence::YearMonthDayHour => instant.date().and_hms_opt(instant.hour(), 0, 0),
        DateSequence::YearMonth => instant.date().with_day(1).and_then(|day| day.and_hms_opt(0, 0, 0)),
        DateSequence::YearMonthDay | DateSequence::MonthYearDay | DateSequence::DayMonthYear => instant.date().and_hms_opt(0, 0, 0),
    };
    floor
        .expect("every date has a first hour and a first day")
        .and_utc()
        .timestamp_micros()
}

/// The date folder of `instant`, as DMS's `DatePartitionSequence` and `DatePartitionDelimiter`
/// name it, ending in `/`.
fn folder(partition: &DatePartition, instant: &DateTime<Utc>) -> String {
    let parts: &[&str] = match partition.sequence {
        DateSequence::YearMonthDay => &["%Y", "%m", "%d"],
        DateSequence::YearMonthDayHour => &["%Y", "%m", "%d", "%H"],
        DateSequence::YearMonth => &["%Y", "%m"],
        DateSequence::MonthYearDay => &["%m", "%Y", "%d"],
        DateSequence::DayMonthYear => &["%d", "%m", "%Y"],
    };
    let delimiter = match partition.delimiter {
        DateDelimiter::Slash => "/",
        DateDelimiter::Dash => "-",
        DateDelimiter::Underscore => "_",
        DateDelimiter::None => "",
    };
    format!("{}/", instant.format(&parts.join(delimiter)))
}

/// `work` for each of `count` units on up to `threads` threads, in unit order.
fn on_threads<T: Send>(count: usize, threads: usize, work: impl Fn(usize) -> T + Sync) -> Vec<T> {
    let next = AtomicUsize::new(0);
    let mut slots: Vec<Option<T>> = (0..count).map(|_| None).collect();
    std::thread::scope(|scope| {
        let workers: Vec<_> = (0..threads.clamp(1, count.max(1)))
            .map(|_| {
                scope.spawn(|| {
                    let mut done = Vec::new();
                    loop {
                        let at = next.fetch_add(1, AtomicOrdering::Relaxed);
                        if at >= count {
                            return done;
                        }
                        done.push((at, work(at)));
                    }
                })
            })
            .collect();
        for worker in workers {
            let done = worker.join().unwrap_or_else(|panic| std::panic::resume_unwind(panic));
            for (at, value) in done {
                slots[at] = Some(value);
            }
        }
    });
    slots.into_iter().map(|slot| slot.expect("every unit ran")).collect()
}
