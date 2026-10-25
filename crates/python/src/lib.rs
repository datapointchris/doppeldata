use pyo3::prelude::*;

/// doppeldata's engine, compiled.
#[pymodule]
mod _core {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::OnceLock;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    use arrow_array::RecordBatch;
    use arrow_array::RecordBatchReader;
    use arrow_schema::ArrowError;
    use arrow_schema::SchemaRef;
    use doppeldata_core::PlanError;
    use doppeldata_core::manifest::ManifestFile;
    use doppeldata_core::profile::Profile;
    use doppeldata_writers::Destination;
    use doppeldata_writers::DestinationUrl;
    use doppeldata_writers::FailureKind;
    use doppeldata_writers::RunOptions;
    use doppeldata_writers::SplitRun;
    use doppeldata_writers::WriteChoices;
    use doppeldata_writers::WriteError;
    use doppeldata_writers::WriteFailure;
    use object_store::ObjectStore;
    use object_store::client::HttpError;
    use object_store::client::HttpErrorKind;
    use object_store::memory::InMemory;
    use pyo3::exceptions::PyValueError;
    use pyo3::prelude::*;
    use pyo3::types::PyDict;
    use pyo3_arrow::PyRecordBatchReader;
    use pyo3_arrow::PyTable;
    use pyo3_object_store::PyS3Store;
    use serde_json::json;

    /// The engine's release, which is the tag the wheel was built from.
    #[pymodule_export]
    const VERSION: &str = doppeldata_core::VERSION;

    /// The profile format version the engine reads.
    #[pymodule_export]
    const FORMAT_VERSION: u64 = doppeldata_core::profile::FORMAT_VERSION;

    /// The fewest rows a stored literal or cell rests on, where a profile states no `privacy.k_min`.
    #[pymodule_export]
    const DEFAULT_K_MIN: u64 = doppeldata_core::profile::DEFAULT_K_MIN;

    /// The rows in each batch the engine generates, every one but the last.
    #[pymodule_export]
    const BLOCK_ROWS: u64 = doppeldata_core::BLOCK_ROWS;

    /// The version of the draws the engine makes, part of every dataset's identity.
    #[pymodule_export]
    const STREAM_VERSION: u32 = doppeldata_core::STREAM_VERSION;

    /// The rows in each file a write makes when neither the caller nor the profile sizes them.
    #[pymodule_export]
    const DEFAULT_FILE_ROWS: u64 = doppeldata_core::manifest::DEFAULT_FILE_ROWS;

    /// How often a call waiting on the engine checks for a signal, such as the interrupt Ctrl-C sends.
    const SIGNAL_CHECK: Duration = Duration::from_millis(100);

    /// The file name under ``doppeldata/schemas/`` of each JSON Schema the library ships, by name.
    #[pyfunction]
    fn schema_file_names() -> BTreeMap<&'static str, String> {
        doppeldata_core::published_schemas::all()
            .into_iter()
            .map(|schema| (schema.name, schema.file_name))
            .collect()
    }

    /// Whether each scipy.stats family a profile's ``fit`` can name is discrete, by its name. Any
    /// other family leaves the column a linear tail.
    #[pyfunction]
    fn fitted_families() -> BTreeMap<&'static str, bool> {
        doppeldata_core::FITTED_FAMILIES
            .iter()
            .map(|family| (family.name, family.discrete))
            .collect()
    }

    /// The anchored regular expression grex generalizes the values to, with each digit as a digit
    /// class, or None when it would hold a literal or a character class only some values have.
    #[pyfunction]
    fn generalize_pattern(values: Vec<String>) -> Option<String> {
        doppeldata_core::generalized_pattern::generalize(&values)
    }

    /// Whether ``pattern`` holds no literal only some values have, as the privacy floor admits a
    /// stored pattern: every class is ``\d`` or whole ASCII digit and letter ranges. None when it
    /// does not parse.
    #[pyfunction]
    fn pattern_is_generalized(pattern: &str) -> Option<bool> {
        doppeldata_core::generalized_pattern::is_generalized(pattern)
    }

    /// The value's shape, as a profile stores it: each ASCII upper-case letter as A, lower-case
    /// letter as a and digit as 9, and every other character as itself.
    #[pyfunction]
    fn shape_of(value: &str) -> String {
        doppeldata_core::shape_of(value)
    }

    /// The determinism contract's SHA-256 over every value and text cell generated from a profile,
    /// made on ``threads`` threads the way ``Plan.generate`` makes its blocks. A wheel's build
    /// compares it with the hash the contract pins.
    #[pyfunction]
    #[pyo3(signature = (profile_json, profile_digest, seed, rows, *, threads))]
    fn determinism_values_digest(
        py: Python<'_>,
        profile_json: &str,
        profile_digest: &str,
        seed: u64,
        rows: u64,
        threads: usize,
    ) -> PyResult<String> {
        let plan = Plan::new(py, profile_json, profile_digest, seed, rows, "the profile")?;
        let plan = &*plan.plan;
        let cancel = AtomicBool::new(false);
        let (blocks, signal) = detached(py, &cancel, || {
            in_parallel(plan.block_count(), threads, &cancel, |block| {
                let batch = plan.generate_block(block).expect("every block below block_count is in range");
                (batch, plan.text_cells(block).expect("every block below block_count is in range"))
            })
        });
        if let Some(signal) = signal {
            return Err(signal);
        }
        Ok(doppeldata_core::values_digest(&blocks.expect("only a signal cancels")))
    }

    /// A table profile resolved for generation: ``rows`` rows from ``seed``. ``source`` names the
    /// profile in an error.
    #[pyclass(frozen)]
    struct Plan {
        profile: Arc<Profile>,
        plan: Arc<doppeldata_core::Plan>,
    }

    #[pymethods]
    impl Plan {
        #[new]
        fn new(py: Python<'_>, profile_json: &str, profile_digest: &str, seed: u64, rows: u64, source: &str) -> PyResult<Self> {
            let profile: Profile = serde_json::from_str(profile_json)
                .map_err(|error| PyValueError::new_err(format!("{source} does not match the engine's profile model: {error}")))?;
            match py.detach(|| doppeldata_core::plan(&profile, profile_digest, seed, rows)) {
                Ok(plan) => Ok(Self {
                    profile: Arc::new(profile),
                    plan: Arc::new(plan),
                }),
                Err(error) => Err(plan_refused(py, source, error)),
            }
        }

        /// The dataset identity's RFC 8785 canonical JSON: the five values its rows are a pure
        /// function of.
        #[getter]
        fn identity_json(&self) -> String {
            self.plan.identity().canonical_json()
        }

        /// ``sha256:`` and the hex SHA-256 of the identity's canonical JSON.
        #[getter]
        fn dataset_digest(&self) -> String {
            self.plan.identity().digest()
        }

        #[getter]
        fn block_count(&self) -> u64 {
            self.plan.block_count()
        }

        /// Each top-level column's name, type as a profile's ``physical_type`` spells it, what draws
        /// its values, and the share of rows drawn null, in schema order.
        fn column_samplers(&self) -> Vec<(String, String, String, f64)> {
            self.plan
                .column_samplers()
                .into_iter()
                .map(|column| {
                    let physical_type = doppeldata_core::physical_type_name(&column.data_type);
                    (column.name, physical_type, column.sampler, column.null_share)
                })
                .collect()
        }

        /// Every row, as one table holding a batch per block, generated on ``threads`` threads,
        /// zero taking one per core. An interrupt stops it and is raised.
        #[pyo3(signature = (*, threads))]
        fn generate(&self, py: Python<'_>, threads: usize) -> PyResult<PyTable> {
            let plan = &*self.plan;
            let cancel = AtomicBool::new(false);
            let (batches, signal) = detached(py, &cancel, || {
                in_parallel(plan.block_count(), threads, &cancel, |block| {
                    plan.generate_block(block).expect("every block below block_count is in range")
                })
            });
            if let Some(signal) = signal {
                return Err(signal);
            }
            PyTable::try_new(batches.expect("only a signal cancels"), plan.schema())
        }

        /// The blocks in order, as a reader. ``threads`` threads, zero taking one per core, each
        /// make the blocks ahead of the reader, and each holds at most two.
        #[pyo3(signature = (*, threads))]
        fn stream(&self, threads: usize) -> PyRecordBatchReader {
            let count = self.plan.block_count();
            PyRecordBatchReader::new(Box::new(BlockStream::start(self.plan.clone(), 0..count, threads)))
        }

        /// Blocks ``start`` up to ``stop`` in order, as a reader made the way ``stream`` makes
        /// every block, so they are the batches ``stream`` yields for those blocks.
        #[pyo3(signature = (start, stop, *, threads))]
        fn stream_blocks(&self, start: u64, stop: u64, threads: usize) -> PyResult<PyRecordBatchReader> {
            let count = self.plan.block_count();
            if start > stop || stop > count {
                return Err(PyValueError::new_err(format!(
                    "blocks {start} to {stop} are not a run within the plan's {count} blocks"
                )));
            }
            Ok(PyRecordBatchReader::new(Box::new(BlockStream::start(
                self.plan.clone(),
                start..stop,
                threads,
            ))))
        }

        /// What to write: the profile's recorded settings, with ``choices_json`` laid over them. It
        /// is a JSON object holding any of ``format``, ``writer``, ``file_rows``, ``partition_by``,
        /// ``partition_style`` and ``profile_path``.
        fn write_options(&self, py: Python<'_>, choices_json: &str) -> PyResult<WriteOptions> {
            let choices: WriteChoices =
                serde_json::from_str(choices_json).map_err(|error| refused(py, WriteError::Settings(error.to_string())))?;
            let options = doppeldata_writers::WriteOptions::chosen(&self.profile, choices).map_err(|error| refused(py, error))?;
            Ok(WriteOptions { options })
        }

        /// Each row range the write would make, as a JSON list of objects keyed as the manifest
        /// keys its files: ``path``, ``range``, ``first_row`` and ``range_rows``, with
        /// ``objects_at_most``. A partitioned range's path holds ``*`` for each partition value.
        fn ranges_json(&self, py: Python<'_>, options: PyRef<'_, WriteOptions>) -> PyResult<String> {
            let ranges = doppeldata_writers::planned_ranges(&self.plan, &options.options).map_err(|error| refused(py, error))?;
            let ranges: Vec<_> = ranges
                .into_iter()
                .map(|range| {
                    json!({
                        "path": range.path,
                        "range": range.range,
                        "first_row": range.first_row,
                        "range_rows": range.range_rows,
                        "objects_at_most": range.objects_at_most,
                    })
                })
                .collect();
            Ok(serde_json::Value::from(ranges).to_string())
        }

        /// The bytes the write would store, its manifest left out, from its first blocks encoded
        /// with its settings.
        fn estimate_bytes(&self, py: Python<'_>, options: PyRef<'_, WriteOptions>) -> PyResult<u64> {
            let (plan, options) = (&*self.plan, &options.options);
            py.detach(|| doppeldata_writers::estimate_bytes(plan, options))
                .map_err(|error| refused(py, error))
        }

        /// Writes the dataset to ``destination``, a URL ``destination_scheme`` accepts, and returns
        /// the manifest as JSON. An S3 URL's store is built from ``s3_store``: the keyword arguments
        /// pyo3-object_store's ``S3Store`` takes, such as ``config`` and ``credential_provider``. An
        /// interrupt cancels the write, which leaves a partial manifest and raises.
        #[pyo3(signature = (options, destination, *, threads, replace, s3_store=None))]
        fn write(
            &self,
            py: Python<'_>,
            options: PyRef<'_, WriteOptions>,
            destination: &str,
            threads: usize,
            replace: bool,
            s3_store: Option<Bound<'_, PyDict>>,
        ) -> PyResult<String> {
            let destination = destination_at(py, destination, s3_store)?;
            let (run, cancel) = run_options(threads, replace);
            let (plan, options) = (&*self.plan, &options.options);
            let manifest = settled(py, &cancel, || doppeldata_writers::write(plan, options, &run, &destination))?;
            Ok(serde_json::to_string(&manifest).expect("a manifest always serializes"))
        }

        /// Claims ``destination`` for the dataset ``write`` would write there, as ``write`` does
        /// before its first object. Any process then writes ranges with ``write_ranges``, and one
        /// calls ``commit``.
        #[pyo3(signature = (options, destination, *, replace, s3_store=None))]
        fn claim(
            &self,
            py: Python<'_>,
            options: PyRef<'_, WriteOptions>,
            destination: &str,
            replace: bool,
            s3_store: Option<Bound<'_, PyDict>>,
        ) -> PyResult<()> {
            let destination = destination_at(py, destination, s3_store)?;
            let (run, cancel) = run_options(1, replace);
            let (plan, options) = (&*self.plan, &options.options);
            settled(py, &cancel, || doppeldata_writers::claim(plan, options, &run, &destination))
        }

        /// Writes the objects of the ranges numbered ``ranges`` in the layout ``ranges_json``
        /// lists, the bytes ``write`` writes for them, on one thread, and returns their manifest
        /// records as a JSON list. It neither claims ``destination`` nor stores a manifest.
        #[pyo3(signature = (options, destination, ranges, *, s3_store=None))]
        fn write_ranges(
            &self,
            py: Python<'_>,
            options: PyRef<'_, WriteOptions>,
            destination: &str,
            ranges: Vec<u32>,
            s3_store: Option<Bound<'_, PyDict>>,
        ) -> PyResult<String> {
            let destination = destination_at(py, destination, s3_store)?;
            let (run, cancel) = run_options(SPLIT_TASK_THREADS, false);
            let (plan, options) = (&*self.plan, &options.options);
            let files = settled(py, &cancel, || {
                doppeldata_writers::write_ranges(plan, options, &ranges, &run, &destination)
            })?;
            Ok(serde_json::to_string(&files).expect("a manifest record always serializes"))
        }

        /// Stores the manifest of a write ``claim`` claimed, from every record ``write_ranges``
        /// returned, given as one JSON list, and returns it as JSON. ``tasks`` is how many the
        /// ranges were divided among, and ``elapsed_ms`` the time from the claim. A range with no
        /// record leaves the partial manifest under the claim's name, and raises.
        #[pyo3(signature = (options, destination, files_json, *, tasks, elapsed_ms, s3_store=None))]
        fn commit(
            &self,
            options: PyRef<'_, WriteOptions>,
            destination: &str,
            files_json: &str,
            tasks: u32,
            elapsed_ms: u64,
            s3_store: Option<Bound<'_, PyDict>>,
        ) -> PyResult<String> {
            let py = options.py();
            let files: Vec<ManifestFile> = serde_json::from_str(files_json)
                .map_err(|error| PyValueError::new_err(format!("files_json is not a list of manifest records: {error}")))?;
            let destination = destination_at(py, destination, s3_store)?;
            let (run, cancel) = run_options(1, false);
            let (plan, options) = (&*self.plan, &options.options);
            let split = SplitRun {
                tasks,
                threads: SPLIT_TASK_THREADS as u32,
                elapsed_ms,
            };
            let manifest = settled(py, &cancel, || {
                doppeldata_writers::commit(plan, options, files, split, &run, &destination)
            })?;
            Ok(serde_json::to_string(&manifest).expect("a manifest always serializes"))
        }
    }

    /// The threads ``write_ranges`` writes on. A task scheduler such as Spark already gives each
    /// task a core, so a task's ranges are written in turn.
    const SPLIT_TASK_THREADS: usize = 1;

    /// The destination ``url`` names, with an S3 URL's store built from ``s3_store``.
    fn destination_at(py: Python<'_>, url: &str, s3_store: Option<Bound<'_, PyDict>>) -> PyResult<Destination> {
        Ok(match DestinationUrl::parse(url).map_err(|error| refused(py, error))? {
            DestinationUrl::S3 { bucket, prefix } => s3_destination(py, &bucket, prefix, url, s3_store)?,
            DestinationUrl::Memory => Destination::new(Arc::new(InMemory::new()), Default::default(), url),
            DestinationUrl::Local(dir) => Destination::local(dir).map_err(|error| refused(py, error))?,
        })
    }

    /// A write's run on ``threads`` threads, with the cancel flag a signal sets.
    fn run_options(threads: usize, replace: bool) -> (RunOptions, Arc<AtomicBool>) {
        let cancel = Arc::new(AtomicBool::new(false));
        let run = RunOptions {
            threads,
            replace,
            cancel: Some(cancel.clone()),
            runtime: None,
        };
        (run, cancel)
    }

    /// Runs a writers call detached, as the library's result or its ``WriteFailed``, or
    /// ``WriteInterrupted`` when a signal stopped it.
    fn settled<T: Send>(py: Python<'_>, cancel: &AtomicBool, call: impl FnOnce() -> Result<T, WriteFailure> + Send) -> PyResult<T> {
        match detached(py, cancel, call) {
            (Ok(_), Some(signal)) => Err(signal),
            (Ok(value), None) => Ok(value),
            (Err(failure), signal) => Err(write_failed(py, failure, signal.is_some())),
        }
    }

    /// The settings a write takes, from ``Plan.write_options``.
    #[pyclass(frozen)]
    struct WriteOptions {
        options: doppeldata_writers::WriteOptions,
    }

    #[pymethods]
    impl WriteOptions {
        /// The format, every setting it writes with, and the layout, as the manifest records them.
        #[getter]
        fn settings_json(&self) -> String {
            let mut settings = serde_json::to_value(&self.options.output).expect("an output always serializes");
            settings["layout"] = serde_json::to_value(&self.options.layout).expect("a layout always serializes");
            settings.to_string()
        }
    }

    /// A run of the plan's blocks in order, each made ahead on one of a few threads that take
    /// turns, so the reader holds at most two blocks per thread.
    struct BlockStream {
        schema: SchemaRef,
        blocks: Vec<mpsc::Receiver<RecordBatch>>,
        next: u64,
        stop: u64,
        start: u64,
    }

    impl BlockStream {
        fn start(plan: Arc<doppeldata_core::Plan>, run: std::ops::Range<u64>, threads: usize) -> Self {
            let (start, stop) = (run.start, run.end);
            let workers = worker_threads(threads).min((stop - start) as usize).max(1);
            let blocks = (0..workers)
                .map(|worker| {
                    let (sender, receiver) = mpsc::sync_channel(1);
                    let plan = plan.clone();
                    thread::Builder::new()
                        .name("doppeldata-stream".into())
                        .spawn(move || {
                            for block in (start + worker as u64..stop).step_by(workers) {
                                let batch = plan.generate_block(block).expect("every block below block_count is in range");
                                if sender.send(batch).is_err() {
                                    return;
                                }
                            }
                        })
                        .expect("the stream's threads start");
                    receiver
                })
                .collect();
            Self {
                schema: plan.schema(),
                blocks,
                next: start,
                stop,
                start,
            }
        }
    }

    impl Iterator for BlockStream {
        type Item = Result<RecordBatch, ArrowError>;

        fn next(&mut self) -> Option<Self::Item> {
            if self.next == self.stop {
                return None;
            }
            let receiver = &self.blocks[((self.next - self.start) % self.blocks.len() as u64) as usize];
            self.next += 1;
            Some(
                receiver
                    .recv()
                    .map_err(|_| ArrowError::ComputeError(format!("the thread making block {} stopped", self.next - 1))),
            )
        }
    }

    impl RecordBatchReader for BlockStream {
        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }
    }

    fn worker_threads(threads: usize) -> usize {
        match threads {
            0 => thread::available_parallelism().map_or(1, |count| count.get()),
            threads => threads,
        }
    }

    /// ``make(block)`` for each of ``count`` blocks on ``threads`` threads, zero taking one per
    /// core, in block order. None when ``cancel`` was set before every block was made.
    fn in_parallel<T: Send + Sync>(count: u64, threads: usize, cancel: &AtomicBool, make: impl Fn(u64) -> T + Sync) -> Option<Vec<T>> {
        let slots: Vec<OnceLock<T>> = (0..count).map(|_| OnceLock::new()).collect();
        let next = AtomicUsize::new(0);
        thread::scope(|scope| {
            for _ in 0..worker_threads(threads).min(slots.len()).max(1) {
                scope.spawn(|| {
                    while !cancel.load(Ordering::Relaxed) {
                        let block = next.fetch_add(1, Ordering::Relaxed);
                        let Some(slot) = slots.get(block) else {
                            return;
                        };
                        let _ = slot.set(make(block as u64));
                    }
                });
            }
        });
        slots.into_iter().map(OnceLock::into_inner).collect()
    }

    /// Runs ``work`` on its own thread with the GIL released, and checks for a signal while it
    /// runs. A signal sets ``cancel``, which ``work`` watches, and its error is returned beside
    /// ``work``'s result once ``work`` stops. Python runs a signal's handler on the main thread
    /// only, so a call on any other thread is never interrupted.
    fn detached<T: Send>(py: Python<'_>, cancel: &AtomicBool, work: impl FnOnce() -> T + Send) -> (T, Option<PyErr>) {
        py.detach(|| {
            thread::scope(|scope| {
                let waiting = thread::current();
                let worker = scope.spawn(move || {
                    let result = work();
                    waiting.unpark();
                    result
                });
                let mut signal = None;
                while !worker.is_finished() {
                    thread::park_timeout(SIGNAL_CHECK);
                    if signal.is_none()
                        && !worker.is_finished()
                        && let Err(error) = Python::attach(|py| py.check_signals())
                    {
                        cancel.store(true, Ordering::Relaxed);
                        signal = Some(error);
                    }
                }
                let result = worker.join().unwrap_or_else(|panic| std::panic::resume_unwind(panic));
                (result, signal)
            })
        })
    }

    /// ``bucket`` and ``prefix`` at ``url``, through a pyo3-object_store ``S3Store`` built with
    /// ``store_kwargs``.
    fn s3_destination(
        py: Python<'_>,
        bucket: &str,
        prefix: object_store::path::Path,
        url: &str,
        store_kwargs: Option<Bound<'_, PyDict>>,
    ) -> PyResult<Destination> {
        let store: PyS3Store = py.get_type::<PyS3Store>().call((bucket,), store_kwargs.as_ref())?.extract()?;
        let store: Arc<dyn ObjectStore> = store.into_inner();
        Ok(Destination::new(store, prefix, url))
    }

    /// What ``url`` names as a write's destination: ``("local", None)``, ``("s3", bucket)`` or
    /// ``("memory", None)``. Any other scheme raises the library's ``WriteFailed``.
    #[pyfunction]
    fn destination_scheme(py: Python<'_>, url: &str) -> PyResult<(&'static str, Option<String>)> {
        Ok(match DestinationUrl::parse(url).map_err(|error| refused(py, error))? {
            DestinationUrl::Local(_) => ("local", None),
            DestinationUrl::S3 { bucket, .. } => ("s3", Some(bucket)),
            DestinationUrl::Memory => ("memory", None),
        })
    }

    /// The library's ``UngenerableProfile``, naming the column or section the engine refused.
    fn plan_refused(py: Python<'_>, source: &str, error: PlanError) -> PyErr {
        let message = match &error {
            PlanError::Column { message, .. } => message.clone(),
            PlanError::ProfileDigest { .. } => return PyValueError::new_err(error.to_string()),
            _ => error.to_string(),
        };
        library_error(|| {
            let module = py.import("doppeldata.profile_format")?;
            let issue = module.getattr("Issue")?.call1((error.code(), error.field(), message))?;
            module.getattr("UngenerableProfile")?.call1((source, vec![issue]))
        })
    }

    /// The library's ``WriteFailed`` for an error raised before the write claimed its destination.
    fn refused(py: Python<'_>, error: WriteError) -> PyErr {
        write_failed(py, WriteFailure::from(error), false)
    }

    /// The library's ``WriteFailed``, carrying the manifest of what the run wrote where it has one.
    /// ``interrupted`` says a signal arrived, which raises ``WriteInterrupted`` instead.
    fn write_failed(py: Python<'_>, failure: WriteFailure, interrupted: bool) -> PyErr {
        let (code, kind) = match failure.error.code() {
            _ if unanswered(&failure.error) => ("no-answer", "unanswered"),
            (code, FailureKind::Refused) => (code, "refused"),
            (code, FailureKind::Failed) => (code, "failed"),
        };
        let mut message = match failure.error {
            WriteError::Cancelled if interrupted => "interrupted, so the write stopped before every file was written".to_string(),
            _ => failure.error.to_string(),
        };
        if let Some(error) = &failure.manifest_error {
            message.push_str(&format!("; its partial manifest was not stored: {error}"));
        }
        let manifest = failure
            .manifest
            .map(|manifest| serde_json::to_string(&manifest).expect("a manifest always serializes"));
        library_error(|| {
            let kwargs = PyDict::new(py);
            kwargs.set_item("kind", kind)?;
            kwargs.set_item("manifest_json", manifest)?;
            kwargs.set_item("manifest_stored", failure.manifest_stored)?;
            let class = match interrupted {
                true => "WriteInterrupted",
                false => "WriteFailed",
            };
            py.import("doppeldata.errors")?.getattr(class)?.call((code, message), Some(&kwargs))
        })
    }

    /// Whether the store stopped answering: a connection or request that failed or timed out on
    /// every retry. A 5xx or 429 on every retry reads as ``store``, because object_store keeps
    /// the status inside an error type it does not export.
    fn unanswered(error: &WriteError) -> bool {
        let WriteError::Store(error) = error else {
            return false;
        };
        let mut source: Option<&(dyn std::error::Error + 'static)> = Some(error);
        while let Some(current) = source {
            if let Some(http) = current.downcast_ref::<HttpError>() {
                return matches!(
                    http.kind(),
                    HttpErrorKind::Connect | HttpErrorKind::Request | HttpErrorKind::Timeout | HttpErrorKind::Interrupted
                );
            }
            source = current.source();
        }
        false
    }

    /// The exception ``build`` makes, or the error building it raised.
    fn library_error<'py>(build: impl FnOnce() -> PyResult<Bound<'py, PyAny>>) -> PyErr {
        match build() {
            Ok(exception) => PyErr::from_value(exception),
            Err(error) => error,
        }
    }

    /// Why the engine cannot generate from ``pattern``, as planning a profile that stores it
    /// refuses it, or None when it can.
    #[pyfunction]
    fn pattern_problem(pattern: &str) -> Option<String> {
        doppeldata_core::pattern_problem(pattern)
    }

    /// Whether each value holds the check digit ``kind`` names, such as ``luhn``, where generation
    /// writes one: its last digit, computed from the digits before it.
    #[pyfunction]
    fn check_digits_hold(kind: &str, values: Vec<String>) -> PyResult<Vec<bool>> {
        let kind: doppeldata_core::profile::CheckDigit = kind.parse().map_err(pyo3::exceptions::PyValueError::new_err)?;
        Ok(values.iter().map(|value| doppeldata_core::check_digit_holds(kind, value)).collect())
    }
}
