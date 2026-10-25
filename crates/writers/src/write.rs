//! The parallel write. Worker threads take file ranges in order, generate each range's blocks and
//! encode them into objects. Every wait on the upload runtime happens on a thread this module
//! spawns, never the caller's, so a caller inside an async runtime is never blocked on from within
//! it.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::btree_map::Entry;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Instant;
use std::time::SystemTime;

use doppeldata_core::BLOCK_ROWS;
use doppeldata_core::Plan;
use doppeldata_core::TextCell;
use doppeldata_core::format_settings::Output;
use doppeldata_core::manifest::MANIFEST_FILE_NAME;
use doppeldata_core::manifest::Manifest;
use doppeldata_core::manifest::ManifestFile;
use doppeldata_core::manifest::ManifestProfile;
use doppeldata_core::manifest::PARTIAL_MANIFEST_FILE_NAME;
use doppeldata_core::profile::provenance;
use object_store::PutMode;
use tokio::runtime::Handle;
use tokio::runtime::RuntimeFlavor;

use crate::WriteError;
use crate::byte_sink::ObjectSink;
use crate::destination::Destination;
use crate::encoders;
use crate::encoders::FileEncoder;
use crate::layout::FileLayout;
use crate::layout::FileRange;
use crate::layout::layout;
use crate::layout::layout_from_manifest;
use crate::partition::PartitionValues;
use crate::partition::Partitioner;
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
    execute(plan, &layout, new_manifest(plan, options, destination), run, destination)
}

/// Claims `destination` for the dataset [`write`] would write there, as `write` does before its
/// first object. Any process holding the same plan and options then writes ranges with
/// [`write_ranges`], and one calls [`commit`] with every range's records.
pub fn claim(plan: &Plan, options: &WriteOptions, run: &RunOptions, destination: &Destination) -> Result<(), WriteFailure> {
    let layout = layout(plan, &options.layout, &options.output).map_err(WriteError::from)?;
    let manifest = claimed(plan, &layout, new_manifest(plan, options, destination))?;
    on_upload_runtime(run, |runtime| Ok(runtime.block_on(destination.prepare(run.replace, &manifest))?))
}

/// The record a claim stores as `_manifest.partial.json`, once the output's format can encode the
/// files `layout` makes.
fn claimed(plan: &Plan, layout: &FileLayout, mut manifest: Manifest) -> Result<Manifest, WriteError> {
    encoders::check(&manifest.output, partitioner(plan, layout).file_schema())?;
    manifest.partial = true;
    manifest.written_at = now_rfc3339();
    Ok(manifest)
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
) -> Result<Vec<ManifestFile>, WriteFailure> {
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
    encoders::check(&options.output, partitioner(plan, &layout).file_schema())?;
    on_upload_runtime(run, |runtime| {
        let written = write_ranges_on(plan, &layout, &selected, &options.output, run, destination, runtime);
        match written.error {
            Some(error) => Err(error.into()),
            None => Ok(written.files),
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

/// Records a write [`claim`] claimed and [`write_ranges`] filled: `_manifest.json` when `files`
/// holds an object of every range in the layout, replacing the claim. Otherwise it stores the
/// partial manifest under the claim's name and fails with [`WriteError::RangesUnrecorded`].
pub fn commit(
    plan: &Plan,
    options: &WriteOptions,
    files: Vec<ManifestFile>,
    split: SplitRun,
    run: &RunOptions,
    destination: &Destination,
) -> Result<Manifest, WriteFailure> {
    let layout = layout(plan, &options.layout, &options.output).map_err(WriteError::from)?;
    let files: BTreeMap<String, ManifestFile> = files.into_iter().map(|file| (file.path.clone(), file)).collect();
    let mut manifest = new_manifest(plan, options, destination);
    manifest.files = files.into_values().collect();
    manifest.threads = split.threads;
    manifest.tasks = Some(split.tasks);
    manifest.elapsed_ms = split.elapsed_ms;
    on_upload_runtime(run, |runtime| finish(runtime, destination, &layout, manifest, None))
}

fn new_manifest(plan: &Plan, options: &WriteOptions, destination: &Destination) -> Manifest {
    Manifest::new(
        plan.identity(),
        profile_record(plan, options.profile_path.clone()),
        options.output.clone(),
        options.layout.clone(),
        destination.url.clone(),
    )
}

fn partitioner(plan: &Plan, layout: &FileLayout) -> Partitioner {
    Partitioner::new(&plan.schema(), &layout.partition_by, layout.partition_style)
}

/// Writes every file a whole `manifest` names again, from `plan`, which must have the manifest's
/// identity, then compares each object's path, rows, bytes and SHA-256 with the record. A partial
/// manifest is refused.
pub fn regenerate(plan: &Plan, manifest: &Manifest, run: &RunOptions, destination: &Destination) -> Result<Manifest, WriteFailure> {
    if manifest.partial {
        return Err(WriteError::PartialManifest.into());
    }
    let identity = plan.identity().digest();
    if identity != manifest.dataset_digest {
        return Err(WriteError::IdentityMismatch {
            plan: identity,
            manifest: manifest.dataset_digest.clone(),
        }
        .into());
    }
    let layout = layout_from_manifest(plan, manifest).map_err(WriteError::from)?;
    let fresh = Manifest::new(
        plan.identity(),
        manifest.profile.clone(),
        manifest.output.clone(),
        manifest.layout.clone(),
        destination.url.clone(),
    );
    let written = execute(plan, &layout, fresh, run, destination)?;
    let differ = differing_files(&manifest.files, &written.files);
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

/// The paths whose record differs between `recorded` and `written`, or that only one of them lists.
fn differing_files(recorded: &[ManifestFile], written: &[ManifestFile]) -> Vec<String> {
    let recorded: BTreeMap<&str, &ManifestFile> = recorded.iter().map(|file| (file.path.as_str(), file)).collect();
    let written: BTreeMap<&str, &ManifestFile> = written.iter().map(|file| (file.path.as_str(), file)).collect();
    let paths: BTreeSet<&str> = recorded.keys().chain(written.keys()).copied().collect();
    paths
        .into_iter()
        .filter(|path| recorded.get(path) != written.get(path))
        .map(String::from)
        .collect()
}

fn profile_record(plan: &Plan, path: Option<String>) -> ManifestProfile {
    let schema = plan.schema();
    let metadata = schema.metadata();
    ManifestProfile {
        name: metadata.get(provenance::PROFILE_NAME).cloned().unwrap_or_default(),
        version: metadata
            .get(provenance::PROFILE_VERSION)
            .and_then(|version| version.parse().ok())
            .unwrap_or_default(),
        path,
    }
}

/// What every worker shares.
struct Run<'a> {
    plan: &'a Plan,
    layout: &'a FileLayout,
    ranges: &'a [FileRange],
    output: &'a Output,
    partitioner: Partitioner,
    destination: &'a Destination,
    runtime: Handle,
    prints_text_cells: bool,
    cancel: Option<Arc<AtomicBool>>,
    next_range: AtomicUsize,
    completed_ranges: AtomicUsize,
    stop: AtomicBool,
    files: Mutex<Vec<ManifestFile>>,
    error: Mutex<Option<WriteError>>,
}

impl Run<'_> {
    fn stopped(&self) -> bool {
        self.stop.load(Ordering::Relaxed) || self.cancel.as_ref().is_some_and(|cancel| cancel.load(Ordering::Relaxed))
    }

    fn work(&self) {
        while !self.stopped() {
            let Some(range) = self.ranges.get(self.next_range.fetch_add(1, Ordering::Relaxed)) else {
                return;
            };
            match self.write_range(range) {
                Ok(()) => {
                    self.completed_ranges.fetch_add(1, Ordering::Relaxed);
                }
                Err(error) => {
                    if !matches!(error, WriteError::Cancelled) {
                        self.error.lock().expect("no worker panics holding it").get_or_insert(error);
                    }
                    self.stop.store(true, Ordering::Relaxed);
                    return;
                }
            }
        }
    }

    /// Generates `range`'s blocks, slices the rows the range holds, and encodes them into one
    /// object per partition value. A block shared with the neighboring range is generated by both.
    fn write_range(&self, range: &FileRange) -> Result<(), WriteError> {
        let mut open: BTreeMap<PartitionValues, (String, Box<dyn FileEncoder>, u64)> = BTreeMap::new();
        let end = range.first_row + range.rows;
        for block in range.first_row / BLOCK_ROWS..=(end - 1) / BLOCK_ROWS {
            if self.stopped() {
                return Err(WriteError::Cancelled);
            }
            let block_first = block * BLOCK_ROWS;
            let batch = self.plan.generate_block(block)?;
            let start = range.first_row.max(block_first) - block_first;
            let stop = end.min(block_first + batch.num_rows() as u64) - block_first;
            let batch = batch.slice(start as usize, (stop - start) as usize);
            let cells = match self.prints_text_cells {
                true => slice_cells(self.plan.text_cells(block)?, start as u32, stop as u32),
                false => vec![Vec::new(); batch.num_columns()],
            };
            for piece in self.partitioner.split(batch, cells)? {
                let (_, encoder, rows) = match open.entry(piece.values) {
                    Entry::Occupied(entry) => entry.into_mut(),
                    Entry::Vacant(entry) => {
                        let relative = format!("{}{}", self.partitioner.directory(entry.key()), self.layout.file_name(range));
                        let sink = ObjectSink::new(
                            self.destination.store.clone(),
                            self.destination.object(&relative)?,
                            self.runtime.clone(),
                        );
                        let encoder = encoders::open(self.output, self.partitioner.file_schema(), Box::new(sink))?;
                        entry.insert((relative, encoder, 0))
                    }
                };
                encoder.write(&piece.batch, &piece.cells)?;
                *rows += piece.batch.num_rows() as u64;
            }
        }
        for (values, (path, encoder, rows)) in open {
            let written = encoder.finish()?;
            self.files.lock().expect("no worker panics holding it").push(ManifestFile {
                path,
                range: range.index,
                first_row: range.first_row,
                range_rows: range.rows,
                rows,
                bytes: written.bytes,
                sha256: written.sha256,
                partition: self.partitioner.record(values),
            });
        }
        Ok(())
    }
}

/// The cells of rows `start..stop` of a block, placed by their row within that slice.
fn slice_cells(cells: Vec<Vec<TextCell>>, start: u32, stop: u32) -> Vec<Vec<TextCell>> {
    cells
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
        .collect()
}

fn now_rfc3339() -> String {
    chrono::DateTime::<chrono::Utc>::from(SystemTime::now()).to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn worker_threads(threads: usize) -> usize {
    match threads {
        0 => thread::available_parallelism().map_or(1, |count| count.get()),
        threads => threads,
    }
}

/// Runs `work` on a thread this module spawns, with `run`'s upload runtime or one built for the
/// call, so no caller's thread is ever blocked on from within a runtime.
fn on_upload_runtime<T: Send>(run: &RunOptions, work: impl FnOnce(&Handle) -> Result<T, WriteFailure> + Send) -> Result<T, WriteFailure> {
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

/// What writing some ranges left: every object completed, in path order, and why the rest stopped.
struct WrittenRanges {
    files: Vec<ManifestFile>,
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
        ranges,
        output,
        partitioner: partitioner(plan, layout),
        destination,
        runtime: runtime.clone(),
        prints_text_cells: output.is_text(),
        cancel: run.cancel.clone(),
        next_range: AtomicUsize::new(0),
        completed_ranges: AtomicUsize::new(0),
        stop: AtomicBool::new(false),
        files: Mutex::new(Vec::new()),
        error: Mutex::new(None),
    };
    let workers = worker_threads(run.threads).min(ranges.len()).max(1);
    thread::scope(|workers_scope| {
        for _ in 0..workers {
            workers_scope.spawn(|| shared.work());
        }
    });
    let unwritten = shared.completed_ranges.load(Ordering::Relaxed) < ranges.len();
    let cancelled = unwritten && shared.cancel.as_ref().is_some_and(|cancel| cancel.load(Ordering::Relaxed));
    let error = shared
        .error
        .into_inner()
        .expect("no worker panics holding it")
        .or_else(|| cancelled.then_some(WriteError::Cancelled));
    let mut files = shared.files.into_inner().expect("no worker panics holding it");
    files.sort_by(|a, b| a.path.cmp(&b.path));
    WrittenRanges { files, error }
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
        finish(runtime, destination, layout, manifest, written.error)
    })
}

/// Stores the record of a run that wrote `manifest.files`. It is partial, and the run fails, when
/// `error` stopped the run or a range of `layout` has no object recorded.
fn finish(
    runtime: &Handle,
    destination: &Destination,
    layout: &FileLayout,
    mut manifest: Manifest,
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
    record(runtime, destination, manifest, error)
}

/// Stores the finished run's manifest: as `_manifest.json` once every object is whole, replacing the
/// claim, and as `_manifest.partial.json`, over the claim, when `error` stopped the run.
fn record(runtime: &Handle, destination: &Destination, manifest: Manifest, error: Option<WriteError>) -> Result<Manifest, WriteFailure> {
    let failure = |error, manifest, manifest_stored, manifest_error| WriteFailure {
        error,
        manifest: Some(Box::new(manifest)),
        manifest_stored,
        manifest_error,
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
