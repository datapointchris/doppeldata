//! A `cdc` write: the full load as DMS's `LOAD` files, the change files beside them, and
//! `_expected`, the table a correct merge of them produces. Each object is a job the workers take
//! in turn, and every job's bytes come from the stream alone, so any number of threads writes the
//! same objects.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Instant;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use doppeldata_core::BLOCK_ROWS;
use doppeldata_core::CdcPlan;
use doppeldata_core::CdcStream;
use doppeldata_core::Plan;
use doppeldata_core::PlanError;
use doppeldata_core::RowFaults;
use doppeldata_core::ScenarioFault;
use doppeldata_core::TextCell;
use doppeldata_core::format_settings::Output;
use doppeldata_core::format_settings::ParquetSettings;
use doppeldata_core::format_settings::TimestampUnit;
use doppeldata_core::manifest::ChangeCounts;
use doppeldata_core::manifest::FaultRecord;
use doppeldata_core::manifest::Manifest;
use doppeldata_core::manifest::ManifestCdc;
use doppeldata_core::manifest::ManifestFile;
use doppeldata_core::manifest::StreamFile;
use doppeldata_core::scenario::CdcTimestampUnit;
use doppeldata_core::scenario::ListedFault;
use doppeldata_core::scenario::StreamFault;
use serde_json::Map;
use serde_json::Value;
use tokio::runtime::Handle;

use crate::WriteError;
use crate::byte_sink::ObjectSink;
use crate::destination::Destination;
use crate::encoders;
use crate::layout::FileLayout;
use crate::layout::FileRange;
use crate::layout::LayoutError;
use crate::layout::file_name;
use crate::write::RunOptions;
use crate::write::WriteFailure;
use crate::write::finish;
use crate::write::now_rfc3339;
use crate::write::on_upload_runtime;
use crate::write::worker_threads;

/// The unit a `cdc` write stores Parquet timestamps at: the op's, else DMS's default.
fn timestamp_unit(cdc: &CdcPlan) -> TimestampUnit {
    match cdc.timestamp_unit().unwrap_or(CdcTimestampUnit::Us) {
        CdcTimestampUnit::Us => TimestampUnit::Us,
        CdcTimestampUnit::Ms => TimestampUnit::Ms,
    }
}

/// The output a `cdc` write's files take: the write's, with a Parquet write's timestamps at the
/// op's unit. The manifest records it, so a regeneration reads it back unchanged.
pub(crate) fn stream_output(output: &Output, cdc: &CdcPlan) -> Output {
    match output {
        Output::Parquet(settings) => Output::Parquet(ParquetSettings {
            timestamp_unit: Some(timestamp_unit(cdc)),
            ..settings.clone()
        }),
        other => other.clone(),
    }
}

/// The output `_expected` takes: a Parquet write's own settings, else Parquet's defaults.
fn expected_output(output: &Output) -> Output {
    match output {
        Output::Parquet(_) => output.clone(),
        _ => Output::Parquet(ParquetSettings::default()),
    }
}

/// The manifest's `cdc` section before any file is written, which names the prefixes a later
/// write with `replace` may clear.
pub(crate) fn manifest_section(cdc: &CdcPlan) -> ManifestCdc {
    ManifestCdc {
        style: cdc.style().into(),
        prefix: cdc.prefix().into(),
        expected_prefix: cdc.expected_prefix(),
        changes: ChangeCounts::default(),
        files: Vec::new(),
        expected: Vec::new(),
    }
}

/// Refuses a write `cdc` cannot make: a format DMS does not write, a timestamp unit CSV has no
/// use for or the writer contradicts, and a partitioned write, since the stream's folders are
/// DMS's date partitions.
pub(crate) fn check_layout(cdc: &CdcPlan, partition_by: &[String], output: &Output) -> Result<(), LayoutError> {
    let refuse = |fault: ScenarioFault, field: String, message: String| Err(LayoutError::Scenario { fault, field, message });
    let unit_field = format!("{}/timestamp_unit", cdc.field());
    match output {
        Output::Csv(_) if cdc.timestamp_unit().is_some() => {
            return refuse(
                ScenarioFault::OpNotForFormat,
                unit_field,
                "timestamp_unit sets the unit Parquet stores a timestamp at, and the write makes CSV files".into(),
            );
        }
        Output::Parquet(settings) if settings.timestamp_unit.is_some_and(|unit| unit != timestamp_unit(cdc)) => {
            return refuse(
                ScenarioFault::InvalidParameter,
                unit_field,
                format!(
                    "the writer stores timestamps at {:?} and cdc at {:?}, and DMS stores every one at one unit; drop the writer's timestamp_unit",
                    settings.timestamp_unit.expect("the guard read it"),
                    timestamp_unit(cdc)
                ),
            );
        }
        Output::Csv(_) | Output::Parquet(_) => {}
        other => {
            return refuse(
                ScenarioFault::OpNotForFormat,
                cdc.field(),
                format!(
                    "cdc writes the CSV or Parquet files DMS writes, and the write makes {} files",
                    other.format_name()
                ),
            );
        }
    }
    if !partition_by.is_empty() {
        return refuse(
            ScenarioFault::InvalidParameter,
            cdc.field(),
            "cdc files its changes in DMS's date folders, so the write takes no partition_by".into(),
        );
    }
    Ok(())
}

/// A refusal the stream's build returns, as the write's.
fn refusal(error: PlanError) -> WriteError {
    match error {
        PlanError::Scenario { code, field, message } => LayoutError::Scenario {
            fault: code,
            field,
            message,
        }
        .into(),
        other => WriteError::Settings(other.to_string()),
    }
}

/// Writes `plan`'s full load, change stream and expected table to `destination`, then the fault
/// list and the manifest. The stream is built before the destination is claimed, so a refusal of
/// it leaves the destination untouched.
pub(crate) fn write(
    plan: &Plan,
    cdc: &CdcPlan,
    layout: &FileLayout,
    mut manifest: Manifest,
    run: &RunOptions,
    destination: &Destination,
) -> Result<Manifest, WriteFailure> {
    let started = Instant::now();
    let threads = worker_threads(run.threads);
    let stream = plan.cdc_stream(threads).map_err(refusal)?.expect("a cdc plan builds a stream");
    let output = stream_output(&manifest.output, cdc);
    encoders::check(&output, &cdc.full_load_schema(&plan.schema()))?;
    for schema in stream.file_schemas() {
        encoders::check(&output, &schema)?;
    }
    encoders::check(&expected_output(&output), &stream.expected_schema())?;
    manifest.output = output;
    manifest.cdc = Some(manifest_section(cdc));
    manifest.partial = true;
    manifest.written_at = now_rfc3339();
    manifest.threads = threads as u32;
    on_upload_runtime(run, |runtime| {
        runtime.block_on(destination.prepare(run.replace, &manifest))?;
        let jobs = Jobs {
            plan,
            cdc,
            stream: &stream,
            layout,
            output: &manifest.output,
            expected_output: expected_output(&manifest.output),
            destination,
            runtime,
            cancel: run.cancel.as_deref(),
            list: job_list(layout, &stream),
            next: AtomicUsize::new(0),
            stop: AtomicBool::new(false),
            load: Mutex::new(Vec::new()),
            changes: Mutex::new(Vec::new()),
            expected: Mutex::new(Vec::new()),
            faults: Mutex::new(Vec::new()),
            error: Mutex::new(None),
        };
        let workers = threads.min(jobs.list.len()).max(1);
        thread::scope(|scope| {
            for _ in 0..workers {
                scope.spawn(|| jobs.work());
            }
        });
        let written = jobs.written();
        manifest.files = written.load;
        manifest.cdc = Some(ManifestCdc {
            changes: stream.changes(),
            files: written.changes,
            expected: written.expected,
            ..manifest_section(cdc)
        });
        manifest.elapsed_ms = started.elapsed().as_millis() as u64;
        let faults = cdc.places_faults().then_some(written.faults);
        finish(runtime, destination, layout, manifest, faults, written.error)
    })
}

/// The paths whose record differs between two manifests' `cdc` sections, or that only one lists.
pub(crate) fn differing_stream_files(recorded: Option<&ManifestCdc>, written: Option<&ManifestCdc>) -> Vec<String> {
    let listed = |section: Option<&ManifestCdc>| -> BTreeMap<String, StreamFile> {
        let files = section
            .into_iter()
            .flat_map(|section| section.files.iter().chain(&section.expected));
        files.map(|file| (file.path.clone(), file.clone())).collect()
    };
    let (recorded, written) = (listed(recorded), listed(written));
    let mut paths: Vec<&String> = recorded.keys().chain(written.keys()).collect();
    paths.sort();
    paths.dedup();
    paths
        .into_iter()
        .filter(|path| recorded.get(*path) != written.get(*path))
        .cloned()
        .collect()
}

/// One object the write makes.
#[derive(Debug, Clone, Copy)]
enum Job {
    Load(FileRange),
    Change(usize),
    Expected(FileRange),
    Inserts,
}

/// What every worker shares, and what each object written left.
struct Jobs<'a> {
    plan: &'a Plan,
    cdc: &'a CdcPlan,
    stream: &'a CdcStream,
    layout: &'a FileLayout,
    output: &'a Output,
    expected_output: Output,
    destination: &'a Destination,
    runtime: &'a Handle,
    cancel: Option<&'a AtomicBool>,
    list: Vec<Job>,
    next: AtomicUsize,
    stop: AtomicBool,
    load: Mutex<Vec<ManifestFile>>,
    changes: Mutex<Vec<StreamFile>>,
    expected: Mutex<Vec<StreamFile>>,
    faults: Mutex<Vec<FaultRecord>>,
    error: Mutex<Option<WriteError>>,
}

/// Every object written, each list in path order, and why the rest stopped.
struct Written {
    load: Vec<ManifestFile>,
    changes: Vec<StreamFile>,
    expected: Vec<StreamFile>,
    faults: Vec<FaultRecord>,
    error: Option<WriteError>,
}

/// Every object of the write, in the order the workers take them.
fn job_list(layout: &FileLayout, stream: &CdcStream) -> Vec<Job> {
    let ranges = &layout.ranges;
    (ranges.iter().map(|range| Job::Load(*range)))
        .chain((0..stream.file_count()).map(Job::Change))
        .chain(ranges.iter().map(|range| Job::Expected(*range)))
        .chain(stream.expected_inserts().map(|_| Job::Inserts))
        .collect()
}

impl Jobs<'_> {
    fn stopped(&self) -> bool {
        self.stop.load(Ordering::Relaxed) || self.cancel.is_some_and(|cancel| cancel.load(Ordering::Relaxed))
    }

    fn work(&self) {
        while !self.stopped() {
            let Some(job) = self.list.get(self.next.fetch_add(1, Ordering::Relaxed)) else {
                return;
            };
            if let Err(error) = self.run(*job) {
                self.error.lock().expect("no worker panics holding it").get_or_insert(error);
                self.stop.store(true, Ordering::Relaxed);
                return;
            }
        }
    }

    fn run(&self, job: Job) -> Result<(), WriteError> {
        let extension = &self.layout.extension;
        match job {
            Job::Load(range) => {
                let path = format!("{}.{extension}", self.cdc.load_stem(range.index as usize));
                let schema = self.cdc.full_load_schema(&self.plan.schema());
                let end = range.first_row + range.rows;
                let mut batches = Vec::new();
                for block in range.first_row / BLOCK_ROWS..=(end - 1) / BLOCK_ROWS {
                    let block_first = block * BLOCK_ROWS;
                    let start = range.first_row.max(block_first) - block_first;
                    let stop = end.min(block_first + BLOCK_ROWS) - block_first;
                    let placed = self.plan.placed_block(block, false)?.rows_homed_in(start as u32, stop as u32);
                    batches.push(self.cdc.full_load(&placed.batch));
                }
                let (rows, object) = self.store(&path, self.output, &schema, &batches)?;
                self.load.lock().expect("no worker panics holding it").push(ManifestFile {
                    path,
                    range: range.index,
                    first_row: range.first_row,
                    range_rows: range.rows,
                    rows,
                    bytes: object.bytes,
                    sha256: object.sha256,
                    partition: None,
                });
            }
            Job::Change(file) => {
                let path = format!("{}.{extension}", self.stream.file_stem(file));
                let batch = self.stream.file(file);
                let written = self.store(&path, self.output, &batch.schema(), std::slice::from_ref(&batch))?;
                let faults = self.stream.file_faults(file).iter().map(|fault| self.fault_record(&path, fault));
                self.faults.lock().expect("no worker panics holding it").extend(faults);
                self.changes
                    .lock()
                    .expect("no worker panics holding it")
                    .push(stream_file(path, written));
            }
            Job::Expected(range) => {
                let batch = self.stream.expected_rows(range.first_row..range.first_row + range.rows);
                self.store_expected(range.index, &batch)?;
            }
            Job::Inserts => {
                let batch = self
                    .stream
                    .expected_inserts()
                    .expect("an inserts job is listed only where rows are inserted");
                self.store_expected(self.layout.ranges.len() as u32, &batch)?;
            }
        }
        Ok(())
    }

    fn store_expected(&self, part: u32, batch: &RecordBatch) -> Result<(), WriteError> {
        let path = format!(
            "{}{}",
            self.cdc.expected_prefix(),
            file_name(part, &self.layout.identity8, "parquet")
        );
        let schema = self.stream.expected_schema();
        let written = self.store(&path, &self.expected_output, &schema, std::slice::from_ref(batch))?;
        self.expected
            .lock()
            .expect("no worker panics holding it")
            .push(stream_file(path, written));
        Ok(())
    }

    /// Encodes `batches` as one object at `path`, and returns its rows and record.
    fn store(
        &self,
        path: &str,
        output: &Output,
        schema: &SchemaRef,
        batches: &[RecordBatch],
    ) -> Result<(u64, crate::byte_sink::WrittenObject), WriteError> {
        if self.stopped() {
            return Err(WriteError::Cancelled);
        }
        let sink = ObjectSink::new(self.destination.store.clone(), self.destination.object(path)?, self.runtime.clone());
        let mut encoder = encoders::open(output, schema, Box::new(sink), None)?;
        let cells: Vec<Vec<TextCell>> = vec![Vec::new(); schema.fields().len()];
        let mut rows = 0;
        for batch in batches {
            encoder.write(batch, &cells, &RowFaults::default())?;
            rows += batch.num_rows() as u64;
        }
        Ok((rows, encoder.finish()?.object))
    }

    fn fault_record(&self, path: &str, fault: &doppeldata_core::StreamFaultRecord) -> FaultRecord {
        let line = match self.output {
            Output::Csv(settings) => Some(fault.record + 1 + u64::from(settings.header)),
            _ => None,
        };
        let other = format!("{}.{}", fault.due_stem, self.layout.extension);
        let mut detail = Map::new();
        detail.insert("commit_ts".into(), Value::from(fault.commit.clone()));
        detail.insert("record".into(), Value::from(fault.record));
        match fault.kind {
            StreamFault::OutOfOrder => detail.insert("due_file".into(), Value::from(other)),
            StreamFault::DuplicateDelivery => detail.insert("original_file".into(), Value::from(other)),
        };
        FaultRecord {
            file: path.into(),
            line,
            row: Some(fault.row),
            kind: ListedFault::Stream(fault.kind),
            column: None,
            detail,
        }
    }

    fn written(self) -> Written {
        let error = self.error.into_inner().expect("no worker panics holding it");
        let unfinished = self.next.load(Ordering::Relaxed) < self.list.len();
        let cancelled = unfinished && self.cancel.is_some_and(|cancel| cancel.load(Ordering::Relaxed));
        let sorted = |mut files: Vec<StreamFile>| {
            files.sort_by(|a, b| a.path.cmp(&b.path));
            files
        };
        let mut load = self.load.into_inner().expect("no worker panics holding it");
        load.sort_by(|a, b| a.path.cmp(&b.path));
        let mut faults = self.faults.into_inner().expect("no worker panics holding it");
        faults.sort_by(|a, b| a.file.cmp(&b.file));
        Written {
            load,
            changes: sorted(self.changes.into_inner().expect("no worker panics holding it")),
            expected: sorted(self.expected.into_inner().expect("no worker panics holding it")),
            faults,
            error: error.or_else(|| cancelled.then_some(WriteError::Cancelled)),
        }
    }
}

fn stream_file(path: String, (rows, object): (u64, crate::byte_sink::WrittenObject)) -> StreamFile {
    StreamFile {
        path,
        rows,
        bytes: object.bytes,
        sha256: object.sha256,
    }
}
