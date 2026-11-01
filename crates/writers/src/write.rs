//! The parallel write. Worker threads take file ranges in order, generate each range's blocks and
//! encode them into objects. Every wait on the upload runtime happens on a thread this module
//! spawns, never the caller's, so a caller inside an async runtime is never blocked on from within
//! it.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::btree_map::Entry;
use std::io::Write as _;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Instant;
use std::time::SystemTime;

use doppeldata_core::BLOCK_ROWS;
use doppeldata_core::DatasetIdentity;
use doppeldata_core::Plan;
use doppeldata_core::ScenarioFault;
use doppeldata_core::format_settings::Output;
use doppeldata_core::manifest::FAULTS_FILE_NAME;
use doppeldata_core::manifest::FaultRecord;
use doppeldata_core::manifest::MANIFEST_FILE_NAME;
use doppeldata_core::manifest::Manifest;
use doppeldata_core::manifest::ManifestFile;
use doppeldata_core::manifest::ManifestIdentity;
use doppeldata_core::manifest::ManifestProfile;
use doppeldata_core::manifest::PARTIAL_MANIFEST_FILE_NAME;
use doppeldata_core::profile::provenance;
use object_store::PutMode;
use tokio::runtime::Handle;
use tokio::runtime::RuntimeFlavor;

use crate::WriteError;
use crate::byte_sink::ByteSink;
use crate::byte_sink::ObjectSink;
use crate::cdc;
use crate::destination::Destination;
use crate::encoders;
use crate::encoders::Faulting;
use crate::encoders::FileEncoder;
use crate::layout::FileLayout;
use crate::layout::FileRange;
use crate::layout::LayoutError;
use crate::layout::layout;
use crate::layout::layout_from_manifest;
use crate::partition::PartitionValues;
use crate::partition::Partitioner;
use crate::regenerated_files::differing_files;
use crate::regenerated_files::differing_stream_files;
use crate::size_estimate::check_invalid_utf8_reach;
use crate::write_options::WriteOptions;

/// How to run a write. None of it changes a byte written.
#[derive(Debug, Clone, Default)]
pub struct RunOptions {
    /// Threads that generate and encode. Zero takes one per core.
    pub threads: usize,
    /// Delete what an earlier run of this tool left at the destination, where otherwise a
    /// destination holding anything is refused. A write with it takes over a destination another
    /// write is still filling, so two writes with `replace` must never share one.
    pub replace: bool,
    /// Set from any thread to stop the write between blocks. Unless every range was already written,
    /// the write then fails with [`WriteError::Cancelled`] and a partial manifest.
    pub cancel: Option<Arc<AtomicBool>>,
    /// A multi-thread tokio runtime to upload on, such as the one the caller's store already uses.
    /// The write blocks the calling thread, so that thread must not be the runtime's only worker. A
    /// current-thread runtime is refused. None builds one for the write and shuts it down after.
    pub runtime: Option<Handle>,
}

/// A write that failed.
#[derive(Debug, thiserror::Error)]
#[error("{error}")]
pub struct WriteFailure {
    #[source]
    pub error: WriteError,
    /// What the run wrote, marked partial when a range is missing. None when it failed before
    /// claiming the destination.
    pub manifest: Option<Box<Manifest>>,
    /// Whether `manifest` is in the store: as `_manifest.json` when whole, as
    /// `_manifest.partial.json` when partial.
    pub manifest_stored: bool,
    /// Why storing a partial manifest failed, where `error` is why the run stopped.
    pub manifest_error: Option<Box<WriteError>>,
}

impl From<WriteError> for WriteFailure {
    fn from(error: WriteError) -> Self {
        Self {
            error,
            manifest: None,
            manifest_stored: false,
            manifest_error: None,
        }
    }
}

/// Upload threads the write builds when the caller supplies no runtime. They wait on the network
/// while the worker threads encode, so two keep every part in flight.
const UPLOAD_THREADS: usize = 2;

/// Writes `plan`'s dataset to `destination`, and the manifest beside it, which is also returned.
pub fn write(plan: &Plan, options: &WriteOptions, run: &RunOptions, destination: &Destination) -> Result<Manifest, WriteFailure> {
    let layout = layout(plan, &options.layout, &options.output).map_err(WriteError::from)?;
    match plan.cdc() {
        Some(cdc) => cdc::write(plan, cdc, &layout, new_manifest(plan, options, destination), run, destination),
        None => execute(plan, &layout, new_manifest(plan, options, destination), run, destination),
    }
}

/// Refuses what [`write`] refuses of `plan` and `options` before it claims a destination: a layout
/// the files cannot take, a format that cannot encode them, or a fault they cannot hold. A caller
/// runs it before opening the destination, which for a local directory creates it.
pub fn check_writable(plan: &Plan, options: &WriteOptions) -> Result<(), WriteError> {
    let layout = layout(plan, &options.layout, &options.output)?;
    check_encodable(plan, &layout, &options.output)?;
    check_invalid_utf8_reach(plan, &layout, &options.output)
}

/// Refuses a write split across processes under an op that orders the whole table.
fn refuse_split(plan: &Plan) -> Result<(), WriteError> {
    match plan.one_process_op() {
        Some((field, op)) => Err(LayoutError::Scenario {
            fault: ScenarioFault::OpNeedsOneProcess,
            field: field.into(),
            message: format!("{op} orders the whole table, so one process writes the dataset; call write"),
        }
        .into()),
        None => Ok(()),
    }
}

/// Claims `destination` for the dataset [`write`] would write there, as `write` does before its
/// first object. Any process holding the same plan and options then writes ranges with
/// [`write_ranges`], and one calls [`commit`] with every range's records.
pub fn claim(plan: &Plan, options: &WriteOptions, run: &RunOptions, destination: &Destination) -> Result<(), WriteFailure> {
    refuse_split(plan)?;
    let layout = layout(plan, &options.layout, &options.output).map_err(WriteError::from)?;
    let manifest = claimed(plan, &layout, new_manifest(plan, options, destination))?;
    on_upload_runtime(run, |runtime| Ok(runtime.block_on(destination.prepare(run.replace, &manifest))?))
}

/// The record a claim stores as `_manifest.partial.json`, once the output's format can encode the
/// files `layout` makes, and they can hold every fault the scenario names.
fn claimed(plan: &Plan, layout: &FileLayout, mut manifest: Manifest) -> Result<Manifest, WriteError> {
    check_encodable(plan, layout, &manifest.output)?;
    check_invalid_utf8_reach(plan, layout, &manifest.output)?;
    manifest.partial = true;
    manifest.written_at = now_rfc3339();
    Ok(manifest)
}

/// What [`write_ranges`] wrote: each object's record, and each fault `malformed_rows` placed in
/// them, by path and then in the order written.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct RangesWritten {
    pub files: Vec<ManifestFile>,
    pub faults: Vec<FaultRecord>,
}

/// Writes the objects of the ranges numbered `ranges` in the layout [`write`] makes, and returns
/// each object's record. It neither claims the destination nor stores a manifest, so the objects
/// are the bytes `write` writes for those ranges.
pub fn write_ranges(
    plan: &Plan,
    options: &WriteOptions,
    ranges: &[u32],
    run: &RunOptions,
    destination: &Destination,
) -> Result<RangesWritten, WriteFailure> {
    refuse_split(plan)?;
    let layout = layout(plan, &options.layout, &options.output).map_err(WriteError::from)?;
    let selected = ranges
        .iter()
        .map(|&range| {
            layout.ranges.get(range as usize).copied().ok_or(WriteError::RangeOutOfLayout {
                range,
                ranges: layout.ranges.len(),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    check_encodable(plan, &layout, &options.output)?;
    on_upload_runtime(run, |runtime| {
        let written = write_ranges_on(plan, &layout, &selected, &options.output, run, destination, runtime);
        match written.error {
            Some(error) => Err(error.into()),
            None => Ok(RangesWritten {
                files: written.files,
                faults: written.faults,
            }),
        }
    })
}

/// How a write split across processes ran, which its manifest records.
#[derive(Debug, Clone, Copy, Default)]
pub struct SplitRun {
    /// The tasks the ranges were divided among.
    pub tasks: u32,
    /// The threads each task wrote on.
    pub threads: u32,
    /// From the claim to the commit.
    pub elapsed_ms: u64,
}

/// Records a write [`claim`] claimed and [`write_ranges`] filled: `_manifest.json` when `written`
/// holds an object of every range in the layout, replacing the claim, and `_faults.jsonl` before
/// it under `malformed_rows`. Otherwise it stores the partial manifest under the claim's name and
/// fails with [`WriteError::RangesUnrecorded`].
pub fn commit(
    plan: &Plan,
    options: &WriteOptions,
    written: RangesWritten,
    split: SplitRun,
    run: &RunOptions,
    destination: &Destination,
) -> Result<Manifest, WriteFailure> {
    refuse_split(plan)?;
    let layout = layout(plan, &options.layout, &options.output).map_err(WriteError::from)?;
    let files: BTreeMap<String, ManifestFile> = written.files.into_iter().map(|file| (file.path.clone(), file)).collect();
    let mut manifest = new_manifest(plan, options, destination);
    manifest.files = files.into_values().collect();
    manifest.threads = split.threads;
    manifest.tasks = Some(split.tasks);
    manifest.elapsed_ms = split.elapsed_ms;
    let mut faults = written.faults;
    faults.sort_by(|a, b| a.file.cmp(&b.file));
    let faults = plan.malformed().map(|_| faults);
    on_upload_runtime(run, |runtime| finish(runtime, destination, &layout, manifest, faults, None))
}

fn new_manifest(plan: &Plan, options: &WriteOptions, destination: &Destination) -> Manifest {
    let mut manifest = Manifest::new(
        plan.identity(),
        profile_record(plan, options),
        options.output.clone(),
        options.layout.clone(),
        destination.url.clone(),
    );
    manifest.scenario = options.scenario.clone();
    manifest
}

fn partitioner(plan: &Plan, layout: &FileLayout) -> Partitioner {
    Partitioner::new(&plan.schema(), &layout.partition_by, layout.partition_style)
}

/// The partitioner of the files past a schema drift's cut, where the plan's scenario drifts.
fn drifted_partitioner(plan: &Plan, layout: &FileLayout) -> Option<Partitioner> {
    plan.drift()
        .map(|drift| Partitioner::new(&drift.schema(), &layout.partition_by, layout.partition_style))
}

/// Refuses a write whose format cannot encode a file `layout` makes, in either shape a drift gives.
fn check_encodable(plan: &Plan, layout: &FileLayout, output: &Output) -> Result<(), WriteError> {
    encoders::check(output, partitioner(plan, layout).file_schema())?;
    if let Some(drifted) = drifted_partitioner(plan, layout) {
        encoders::check(output, drifted.file_schema())?;
    }
    Ok(())
}

/// Refuses a `manifest` that [`regenerate`] cannot write again from `plan`: a partial one, one
/// whose files embed the profile, whose JSON a plan never holds, one recording a write to one
/// file, whose name no part takes, and one whose identity is not the plan's.
pub fn check_regenerable(plan: &Plan, manifest: &Manifest) -> Result<(), WriteError> {
    if manifest.partial {
        return Err(WriteError::PartialManifest);
    }
    if manifest.profile.embedded {
        return Err(WriteError::EmbeddedProfileManifest);
    }
    if let Some(file) = &manifest.one_file {
        return Err(WriteError::OneFileManifest { file: file.clone() });
    }
    check_identity(plan.identity(), &manifest.dataset_sha256, &manifest.identity)
}

/// Refuses a plan's `identity` where it is not the one a manifest records as `recorded`, whose
/// SHA-256 is `recorded_sha256`, naming each key the two hold differently.
pub(crate) fn check_identity(identity: &DatasetIdentity, recorded_sha256: &str, recorded: &ManifestIdentity) -> Result<(), WriteError> {
    if identity.sha256() == recorded_sha256 {
        return Ok(());
    }
    Err(WriteError::IdentityMismatch {
        plan: identity.sha256(),
        manifest: recorded_sha256.to_string(),
        differing: identity_differences(&ManifestIdentity::from(identity), recorded),
    })
}

/// Writes every file a whole `manifest` names again, from `plan`, once [`check_regenerable`]
/// passes, then compares every field of each object's record with the manifest's. The fresh
/// manifest records the manifest's profile and scenario.
pub fn regenerate(plan: &Plan, manifest: &Manifest, run: &RunOptions, destination: &Destination) -> Result<Manifest, WriteFailure> {
    check_regenerable(plan, manifest)?;
    let identity = plan.identity();
    let layout = layout_from_manifest(plan, manifest).map_err(WriteError::from)?;
    let mut fresh = Manifest::new(
        identity,
        manifest.profile.clone(),
        manifest.output.clone(),
        manifest.layout.clone(),
        destination.url.clone(),
    );
    fresh.scenario = manifest.scenario.clone();
    let written = match plan.cdc() {
        Some(cdc) => cdc::write(plan, cdc, &layout, fresh, run, destination)?,
        None => execute(plan, &layout, fresh, run, destination)?,
    };
    let mut differ = differing_files(&manifest.files, &written.files);
    differ.extend(differing_stream_files(manifest.cdc.as_ref(), written.cdc.as_ref()));
    if differ.is_empty() {
        Ok(written)
    } else {
        Err(WriteFailure {
            error: WriteError::RegeneratedFilesDiffer { files: differ },
            manifest: Some(Box::new(written)),
            manifest_stored: true,
            manifest_error: None,
        })
    }
}

/// The keys of the identity whose values differ between the plan's and the manifest's record.
fn identity_differences(plan: &ManifestIdentity, recorded: &ManifestIdentity) -> Vec<String> {
    let as_map = |identity: &ManifestIdentity| match serde_json::to_value(identity).expect("an identity always serializes") {
        serde_json::Value::Object(map) => map,
        _ => unreachable!("an identity serializes as an object"),
    };
    let (plan, recorded) = (as_map(plan), as_map(recorded));
    let keys: BTreeSet<&String> = plan.keys().chain(recorded.keys()).collect();
    keys.into_iter()
        .filter(|key| plan.get(*key) != recorded.get(*key))
        .cloned()
        .collect()
}

fn profile_record(plan: &Plan, options: &WriteOptions) -> ManifestProfile {
    let schema = plan.schema();
    let metadata = schema.metadata();
    ManifestProfile {
        name: metadata.get(provenance::PROFILE_NAME).cloned().unwrap_or_default(),
        path: options.profile_path.clone(),
        embedded: options.output.embedded().is_some(),
    }
}

/// What every worker shares.
struct Run<'a> {
    plan: &'a Plan,
    layout: &'a FileLayout,
    output: &'a Output,
    partitioner: Partitioner,
    drifted_partitioner: Option<Partitioner>,
    destination: &'a Destination,
    runtime: Handle,
    prints_text_cells: bool,
    files: Mutex<Vec<ManifestFile>>,
    faults: Mutex<Vec<FaultRecord>>,
}

/// Whether a run's jobs stop: one failed, or the caller cancelled.
pub(crate) struct Stop<'a> {
    failed: AtomicBool,
    cancel: Option<&'a AtomicBool>,
}

impl Stop<'_> {
    pub(crate) fn stopped(&self) -> bool {
        self.failed.load(Ordering::Relaxed) || self.cancelled()
    }

    fn cancelled(&self) -> bool {
        self.cancel.is_some_and(|cancel| cancel.load(Ordering::Relaxed))
    }
}

/// Runs `work` over `jobs` on up to `threads` threads, each taking the next job in turn, until one
/// fails or `cancel` is set. Returns the first failure, else [`WriteError::Cancelled`] where a job
/// was left unfinished by `cancel`. A job checks the [`Stop`] it is handed between its steps.
pub(crate) fn run_jobs<J: Sync>(
    jobs: &[J],
    threads: usize,
    cancel: Option<&AtomicBool>,
    work: impl Fn(&J, &Stop) -> Result<(), WriteError> + Sync,
) -> Option<WriteError> {
    let stop = Stop {
        failed: AtomicBool::new(false),
        cancel,
    };
    let (next, completed, first_error) = (AtomicUsize::new(0), AtomicUsize::new(0), Mutex::new(None));
    thread::scope(|scope| {
        for _ in 0..threads.min(jobs.len()).max(1) {
            scope.spawn(|| {
                while !stop.stopped() {
                    let Some(job) = jobs.get(next.fetch_add(1, Ordering::Relaxed)) else {
                        return;
                    };
                    if let Err(error) = work(job, &stop) {
                        if !matches!(error, WriteError::Cancelled) {
                            first_error.lock().expect("no worker panics holding it").get_or_insert(error);
                        }
                        stop.failed.store(true, Ordering::Relaxed);
                        return;
                    }
                    completed.fetch_add(1, Ordering::Relaxed);
                }
            });
        }
    });
    let unfinished = completed.load(Ordering::Relaxed) < jobs.len();
    let cancelled = unfinished && stop.cancelled();
    first_error
        .into_inner()
        .expect("no worker panics holding it")
        .or_else(|| cancelled.then_some(WriteError::Cancelled))
}

impl Run<'_> {
    /// Generates `range`'s blocks, takes the rows filed with the range's generated rows, and
    /// encodes them into one object per partition value. A range at or past a schema drift's cut
    /// takes them in the drifted shape. A block shared with the neighboring range is generated by
    /// both.
    fn write_range(&self, range: &FileRange, stop: &Stop) -> Result<(), WriteError> {
        let mut open: BTreeMap<PartitionValues, (String, Box<dyn FileEncoder>, u64)> = BTreeMap::new();
        let end = range.first_row + range.rows;
        let drifted = self.plan.drift().is_some_and(|drift| range.first_row >= drift.first_row());
        let partitioner = match (drifted, &self.drifted_partitioner) {
            (true, Some(drifted)) => drifted,
            _ => &self.partitioner,
        };
        for block in range.first_row / BLOCK_ROWS..=(end - 1) / BLOCK_ROWS {
            if stop.stopped() {
                return Err(WriteError::Cancelled);
            }
            let block_first = block * BLOCK_ROWS;
            let mut placed = self.plan.placed_block(block, self.prints_text_cells)?;
            if drifted {
                placed = self.plan.drifted(block, placed);
            }
            let start = range.first_row.max(block_first) - block_first;
            let stop = end.min(block_first + BLOCK_ROWS) - block_first;
            let placed = placed.rows_homed_in(start as u32, stop as u32);
            for piece in partitioner.split(placed)? {
                let (_, encoder, rows) = match open.entry(piece.values) {
                    Entry::Occupied(entry) => entry.into_mut(),
                    Entry::Vacant(entry) => {
                        let directory = partitioner.directory(entry.key());
                        let relative = format!("{directory}{}", self.layout.file_name(range));
                        let sink = ObjectSink::new(
                            self.destination.store.clone(),
                            self.destination.object(&relative)?,
                            self.runtime.clone(),
                        );
                        let faulting = self.plan.malformed().map(|malformed| Faulting {
                            file: malformed.file_fault(range.index, &directory),
                        });
                        let encoder = encoders::open(self.output, partitioner.file_schema(), Box::new(sink), faulting)?;
                        entry.insert((relative, encoder, 0))
                    }
                };
                encoder.write(&piece.batch, &piece.cells, &piece.faults)?;
                *rows += piece.batch.num_rows() as u64;
            }
        }
        for (values, (path, encoder, rows)) in open {
            let finished = encoder.finish()?;
            let faults: Vec<FaultRecord> = finished
                .faults
                .into_iter()
                .map(|record| FaultRecord {
                    file: path.clone(),
                    ..record
                })
                .collect();
            self.faults.lock().expect("no worker panics holding it").extend(faults);
            self.files.lock().expect("no worker panics holding it").push(ManifestFile {
                path,
                range: range.index,
                first_row: range.first_row,
                range_rows: range.rows,
                rows,
                bytes: finished.object.bytes,
                sha256: finished.object.sha256,
                partition: partitioner.record(values),
            });
        }
        Ok(())
    }
}

/// Writes `faults` as `_faults.jsonl`, one JSON object a line, through the sink a data object
/// takes, so a long list uploads in parts.
fn store_faults(runtime: &Handle, destination: &Destination, faults: &[FaultRecord]) -> Result<(), WriteError> {
    let mut sink = ObjectSink::new(destination.store.clone(), destination.object(FAULTS_FILE_NAME)?, runtime.clone());
    let mut line = Vec::new();
    for fault in faults {
        line.clear();
        serde_json::to_writer(&mut line, fault).expect("a fault record always serializes");
        line.push(b'\n');
        sink.write_all(&line)?;
    }
    Box::new(sink).finish()?;
    Ok(())
}

pub(crate) fn now_rfc3339() -> String {
    chrono::DateTime::<chrono::Utc>::from(SystemTime::now()).to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

pub(crate) fn worker_threads(threads: usize) -> usize {
    match threads {
        0 => thread::available_parallelism().map_or(1, |count| count.get()),
        threads => threads,
    }
}

/// Runs `work` on a thread this module spawns, with `run`'s upload runtime or one built for the
/// call, so no caller's thread is ever blocked on from within a runtime.
pub(crate) fn on_upload_runtime<T: Send>(
    run: &RunOptions,
    work: impl FnOnce(&Handle) -> Result<T, WriteFailure> + Send,
) -> Result<T, WriteFailure> {
    if run
        .runtime
        .as_ref()
        .is_some_and(|handle| handle.runtime_flavor() != RuntimeFlavor::MultiThread)
    {
        return Err(WriteError::CurrentThreadRuntime.into());
    }
    thread::scope(|scope| {
        scope
            .spawn(move || {
                let owned_runtime;
                let runtime = match &run.runtime {
                    Some(handle) => handle.clone(),
                    None => {
                        owned_runtime = tokio::runtime::Builder::new_multi_thread()
                            .worker_threads(UPLOAD_THREADS)
                            .thread_name("doppeldata-upload")
                            .enable_all()
                            .build()
                            .map_err(WriteError::from)?;
                        owned_runtime.handle().clone()
                    }
                };
                work(&runtime)
            })
            .join()
            .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
    })
}

/// What writing some ranges left: every object completed and the faults placed in them, in path
/// order, and why the rest stopped.
struct WrittenRanges {
    files: Vec<ManifestFile>,
    faults: Vec<FaultRecord>,
    error: Option<WriteError>,
}

/// Writes `ranges` of `layout` on `run.threads` worker threads, uploading on `runtime`.
fn write_ranges_on(
    plan: &Plan,
    layout: &FileLayout,
    ranges: &[FileRange],
    output: &Output,
    run: &RunOptions,
    destination: &Destination,
    runtime: &Handle,
) -> WrittenRanges {
    let shared = Run {
        plan,
        layout,
        output,
        partitioner: partitioner(plan, layout),
        drifted_partitioner: drifted_partitioner(plan, layout),
        destination,
        runtime: runtime.clone(),
        prints_text_cells: output.is_text(),
        files: Mutex::new(Vec::new()),
        faults: Mutex::new(Vec::new()),
    };
    let error = run_jobs(ranges, worker_threads(run.threads), run.cancel.as_deref(), |range, stop| {
        shared.write_range(range, stop)
    });
    let mut files = shared.files.into_inner().expect("no worker panics holding it");
    files.sort_by(|a, b| a.path.cmp(&b.path));
    let mut faults = shared.faults.into_inner().expect("no worker panics holding it");
    faults.sort_by(|a, b| a.file.cmp(&b.file));
    WrittenRanges { files, faults, error }
}

fn execute(
    plan: &Plan,
    layout: &FileLayout,
    manifest: Manifest,
    run: &RunOptions,
    destination: &Destination,
) -> Result<Manifest, WriteFailure> {
    let started = Instant::now();
    let mut manifest = claimed(plan, layout, manifest)?;
    manifest.threads = worker_threads(run.threads) as u32;
    on_upload_runtime(run, |runtime| {
        runtime.block_on(destination.prepare(run.replace, &manifest))?;
        let written = write_ranges_on(plan, layout, &layout.ranges, &manifest.output, run, destination, runtime);
        manifest.files = written.files;
        manifest.elapsed_ms = started.elapsed().as_millis() as u64;
        let faults = plan.malformed().map(|_| written.faults);
        finish(runtime, destination, layout, manifest, faults, written.error)
    })
}

/// Stores the record of a run that wrote `manifest.files`, and `faults` before it where the run
/// wrote under `malformed_rows`. It is partial, and the run fails, when `error` stopped the run or
/// a range of `layout` has no object recorded.
pub(crate) fn finish(
    runtime: &Handle,
    destination: &Destination,
    layout: &FileLayout,
    mut manifest: Manifest,
    faults: Option<Vec<FaultRecord>>,
    error: Option<WriteError>,
) -> Result<Manifest, WriteFailure> {
    let recorded: BTreeSet<u32> = manifest.files.iter().map(|file| file.range).collect();
    let missing: Vec<u32> = layout
        .ranges
        .iter()
        .map(|range| range.index)
        .filter(|index| !recorded.contains(index))
        .collect();
    let error = error.or_else(|| {
        missing.first().map(|&first| WriteError::RangesUnrecorded {
            missing: missing.len(),
            ranges: layout.ranges.len(),
            first,
        })
    });
    manifest.partial = error.is_some();
    manifest.written_at = now_rfc3339();
    if let Some(faults) = &faults {
        let mut counts = BTreeMap::new();
        for fault in faults {
            *counts.entry(fault.kind).or_insert(0) += 1;
        }
        manifest.faults = Some(counts);
    }
    record(runtime, destination, manifest, faults, error)
}

/// Stores the finished run's manifest: as `_manifest.json` once every object is whole, replacing the
/// claim, and as `_manifest.partial.json`, over the claim, when `error` stopped the run. The run's
/// `faults` are stored first, as `_faults.jsonl`, so a stopped run lists the faults of the objects
/// it completed. A whole run whose list cannot be stored leaves a partial manifest of every object.
fn record(
    runtime: &Handle,
    destination: &Destination,
    mut manifest: Manifest,
    faults: Option<Vec<FaultRecord>>,
    error: Option<WriteError>,
) -> Result<Manifest, WriteFailure> {
    let failure = |error, manifest, manifest_stored, manifest_error| WriteFailure {
        error,
        manifest: Some(Box::new(manifest)),
        manifest_stored,
        manifest_error,
    };
    let error = match faults.as_ref().map(|faults| store_faults(runtime, destination, faults)) {
        Some(Err(unlisted)) => {
            // The counts would describe a list that is not there.
            manifest.faults = None;
            manifest.partial = true;
            Some(error.unwrap_or(unlisted))
        }
        _ => error,
    };
    if let Some(error) = error {
        let stored = runtime.block_on(destination.put_manifest(PARTIAL_MANIFEST_FILE_NAME, &manifest, PutMode::Overwrite));
        return Err(failure(error, manifest, stored.is_ok(), stored.err().map(Box::new)));
    }
    if let Err(error) = runtime.block_on(destination.put_manifest(MANIFEST_FILE_NAME, &manifest, PutMode::Overwrite)) {
        return Err(failure(error, manifest, false, None));
    }
    if let Err(error) = runtime.block_on(destination.remove(PARTIAL_MANIFEST_FILE_NAME)) {
        let error = WriteError::ClaimNotRemoved {
            url: destination.url.trim_end_matches('/').to_string(),
            error: Box::new(error),
        };
        return Err(failure(error, manifest, true, None));
    }
    Ok(manifest)
}
