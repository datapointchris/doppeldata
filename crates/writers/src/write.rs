//! The parallel write. Worker threads take file ranges in order, generate each range's blocks and
//! encode them into objects. Every wait on the upload runtime happens on a thread this module
//! spawns, never the caller's, so a caller inside an async runtime is never blocked on from within
//! it.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Instant;
use std::time::SystemTime;

use bytes::Bytes;
use doppeldata_core::BLOCK_ROWS;
use doppeldata_core::Plan;
use doppeldata_core::TextCell;
use doppeldata_core::manifest::LayoutSettings;
use doppeldata_core::manifest::MANIFEST_FILE_NAME;
use doppeldata_core::manifest::Manifest;
use doppeldata_core::manifest::ManifestFile;
use doppeldata_core::manifest::ManifestProfile;
use doppeldata_core::manifest::Output;
use doppeldata_core::profile::provenance;
use object_store::ObjectStoreExt;
use tokio::runtime::Handle;

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

/// What to write: the format and its settings, how rows split into files, and the profile's path
/// for the manifest to name.
#[derive(Debug, Clone, Default)]
pub struct WriteOptions {
    pub output: Output,
    pub layout: LayoutSettings,
    pub profile_path: Option<String>,
}

/// How to run a write. None of it changes a byte written.
#[derive(Debug, Clone, Default)]
pub struct RunOptions {
    /// Threads that generate and encode. Zero takes one per core.
    pub threads: usize,
    /// Delete the objects an earlier run of this tool left at the destination, where otherwise a
    /// destination holding anything is refused.
    pub replace: bool,
    /// Set from any thread to stop the write between blocks. The write then fails with
    /// [`WriteError::Cancelled`] and a partial manifest.
    pub cancel: Option<Arc<AtomicBool>>,
    /// A multi-thread tokio runtime to upload on, such as the one the caller's store already uses.
    /// None builds one for the write and shuts it down after.
    pub runtime: Option<Handle>,
}

/// A write that failed. Once the destination was prepared, `manifest` lists every object that
/// completed and is marked partial, and was written beside them where the store allowed.
#[derive(Debug, thiserror::Error)]
#[error("{error}")]
pub struct WriteFailure {
    #[source]
    pub error: WriteError,
    pub manifest: Option<Box<Manifest>>,
}

impl From<WriteError> for WriteFailure {
    fn from(error: WriteError) -> Self {
        Self { error, manifest: None }
    }
}

/// Upload threads the write builds when the caller supplies no runtime. They wait on the network
/// while the worker threads encode, so two keep every part in flight.
const UPLOAD_THREADS: usize = 2;

/// Writes `plan`'s dataset to `destination`, and the manifest beside it, which is also returned.
pub fn write(plan: &Plan, options: &WriteOptions, run: &RunOptions, destination: &Destination) -> Result<Manifest, WriteFailure> {
    let layout = layout(plan, &options.layout, &options.output).map_err(WriteError::from)?;
    let manifest = Manifest::new(
        plan.identity(),
        profile_record(plan, options.profile_path.clone()),
        options.output.clone(),
        options.layout.clone(),
        destination.url.clone(),
    );
    execute(plan, &layout, manifest, run, destination)
}

/// Writes every file `manifest` names again, from `plan`, which must have the manifest's identity.
/// Each object's SHA-256 matches the manifest's under the same tool version.
pub fn regenerate(plan: &Plan, manifest: &Manifest, run: &RunOptions, destination: &Destination) -> Result<Manifest, WriteFailure> {
    let identity = plan.identity().digest();
    if identity != manifest.identity {
        return Err(WriteError::IdentityMismatch {
            plan: identity,
            manifest: manifest.identity.clone(),
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
    execute(plan, &layout, fresh, run, destination)
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
        digest: plan.identity().profile_digest().to_string(),
        path,
    }
}

/// What every worker shares.
struct Run<'a> {
    plan: &'a Plan,
    layout: &'a FileLayout,
    output: &'a Output,
    partitioner: Partitioner,
    destination: &'a Destination,
    runtime: Handle,
    prints_text_cells: bool,
    cancel: Option<Arc<AtomicBool>>,
    next_range: AtomicUsize,
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
            let Some(range) = self.layout.ranges.get(self.next_range.fetch_add(1, Ordering::Relaxed)) else {
                return;
            };
            if let Err(error) = self.write_range(range) {
                if !matches!(error, WriteError::Cancelled) {
                    self.error.lock().expect("no worker panics holding it").get_or_insert(error);
                }
                self.stop.store(true, Ordering::Relaxed);
                return;
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
                first_row: range.first_row,
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

fn execute(
    plan: &Plan,
    layout: &FileLayout,
    mut manifest: Manifest,
    run: &RunOptions,
    destination: &Destination,
) -> Result<Manifest, WriteFailure> {
    let started = Instant::now();
    let partitioner = Partitioner::new(&plan.schema(), &layout.partition_by, layout.partition_style);
    encoders::check(&manifest.output, partitioner.file_schema())?;
    let threads = match run.threads {
        0 => thread::available_parallelism().map_or(1, |count| count.get()),
        threads => threads,
    };
    manifest.threads = threads as u32;
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
                runtime.block_on(destination.prepare(run.replace))?;
                let shared = Run {
                    plan,
                    layout,
                    output: &manifest.output,
                    partitioner,
                    destination,
                    runtime: runtime.clone(),
                    prints_text_cells: encoders::prints_text_cells(&manifest.output),
                    cancel: run.cancel.clone(),
                    next_range: AtomicUsize::new(0),
                    stop: AtomicBool::new(false),
                    files: Mutex::new(Vec::new()),
                    error: Mutex::new(None),
                };
                let workers = threads.min(layout.ranges.len()).max(1);
                thread::scope(|workers_scope| {
                    for _ in 0..workers {
                        workers_scope.spawn(|| shared.work());
                    }
                });
                let cancelled = shared.cancel.as_ref().is_some_and(|cancel| cancel.load(Ordering::Relaxed));
                let error = shared
                    .error
                    .into_inner()
                    .expect("no worker panics holding it")
                    .or_else(|| cancelled.then_some(WriteError::Cancelled));
                let mut files = shared.files.into_inner().expect("no worker panics holding it");
                files.sort_by(|a, b| a.path.cmp(&b.path));
                manifest.files = files;
                manifest.partial = error.is_some();
                manifest.elapsed_ms = started.elapsed().as_millis() as u64;
                manifest.written_at =
                    chrono::DateTime::<chrono::Utc>::from(SystemTime::now()).to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
                let stored = runtime.block_on(put_manifest(destination, &manifest));
                match error.map_or(stored, Err) {
                    Ok(()) => Ok(manifest),
                    Err(error) => Err(WriteFailure {
                        error,
                        manifest: Some(Box::new(manifest)),
                    }),
                }
            })
            .join()
            .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
    })
}

async fn put_manifest(destination: &Destination, manifest: &Manifest) -> Result<(), WriteError> {
    let mut text = serde_json::to_vec_pretty(manifest).expect("a manifest always serializes");
    text.push(b'\n');
    destination
        .store
        .put(&destination.object(MANIFEST_FILE_NAME)?, Bytes::from(text).into())
        .await?;
    Ok(())
}
