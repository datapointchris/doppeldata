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
    use doppeldata_core::profile::Profile;
    use doppeldata_writers::Destination;
    use doppeldata_writers::RunOptions;
    use doppeldata_writers::WriteChoices;
    use doppeldata_writers::WriteError;
    use doppeldata_writers::WriteFailure;
    use object_store::ObjectStore;
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

    /// The rows in each batch the engine generates, every one but the last.
    #[pymodule_export]
    const BLOCK_ROWS: u64 = doppeldata_core::BLOCK_ROWS;

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

        /// Each top-level column's name, Arrow type, what draws its values, and the share of rows
        /// drawn null, in schema order.
        fn column_samplers(&self) -> Vec<(String, String, String, f64)> {
            self.plan
                .column_samplers()
                .into_iter()
                .map(|column| (column.name, column.data_type.to_string(), column.sampler, column.null_share))
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
            PyRecordBatchReader::new(Box::new(BlockStream::start(self.plan.clone(), threads)))
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

        /// Each file the write would hold, as JSON: its name, its range and the range's rows. A
        /// partitioned range is written once under each partition value's directory.
        fn layout_json(&self, py: Python<'_>, options: PyRef<'_, WriteOptions>) -> PyResult<String> {
            let layout = doppeldata_writers::layout(&self.plan, &options.options.layout, &options.options.output)
                .map_err(|error| refused(py, WriteError::Layout(error)))?;
            let files: Vec<_> = layout
                .ranges
                .iter()
                .map(|range| json!({"path": layout.file_name(range), "range": range.index, "first_row": range.first_row, "rows": range.rows}))
                .collect();
            Ok(json!({"files": files}).to_string())
        }

        /// The bytes the write would store, its manifest left out, from the first four blocks
        /// encoded with its settings.
        fn estimate_bytes(&self, py: Python<'_>, options: PyRef<'_, WriteOptions>) -> PyResult<u64> {
            let (plan, options) = (&*self.plan, &options.options);
            py.detach(|| doppeldata_writers::estimate_bytes(plan, options))
                .map_err(|error| refused(py, error))
        }

        /// Writes the dataset to ``destination``, a local directory or an ``s3://`` URL, and returns
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
            let destination = match destination.strip_prefix("s3://") {
                Some(_) => s3_destination(py, destination, s3_store)?,
                None if destination.contains("://") => {
                    let message = format!("{destination} is not a local path or an s3:// URL, the destinations doppeldata writes to");
                    return Err(refused(py, WriteError::Settings(message)));
                }
                None => Destination::local(destination).map_err(|error| refused(py, error))?,
            };
            let cancel = Arc::new(AtomicBool::new(false));
            let run = RunOptions {
                threads,
                replace,
                cancel: Some(cancel.clone()),
                runtime: None,
            };
            let (plan, options) = (&*self.plan, &options.options);
            let (written, signal) = detached(py, &cancel, || doppeldata_writers::write(plan, options, &run, &destination));
            match (written, signal) {
                (Ok(_), Some(signal)) => Err(signal),
                (Ok(manifest), None) => Ok(serde_json::to_string(&manifest).expect("a manifest always serializes")),
                (Err(failure), signal) => Err(write_failed(py, failure, signal.is_some())),
            }
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

    /// The plan's blocks in order, each made ahead on one of a few threads that take turns, so the
    /// reader holds at most two blocks per thread.
    struct BlockStream {
        schema: SchemaRef,
        blocks: Vec<mpsc::Receiver<RecordBatch>>,
        next: u64,
        count: u64,
    }

    impl BlockStream {
        fn start(plan: Arc<doppeldata_core::Plan>, threads: usize) -> Self {
            let count = plan.block_count();
            let workers = worker_threads(threads).min(count as usize).max(1);
            let blocks = (0..workers)
                .map(|worker| {
                    let (sender, receiver) = mpsc::sync_channel(1);
                    let plan = plan.clone();
                    thread::Builder::new()
                        .name("doppeldata-stream".into())
                        .spawn(move || {
                            for block in (worker as u64..count).step_by(workers) {
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
                next: 0,
                count,
            }
        }
    }

    impl Iterator for BlockStream {
        type Item = Result<RecordBatch, ArrowError>;

        fn next(&mut self) -> Option<Self::Item> {
            if self.next == self.count {
                return None;
            }
            let receiver = &self.blocks[(self.next % self.blocks.len() as u64) as usize];
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

    /// An S3 destination at ``url``, through a pyo3-object_store ``S3Store`` built with
    /// ``store_kwargs``.
    fn s3_destination(py: Python<'_>, url: &str, store_kwargs: Option<Bound<'_, PyDict>>) -> PyResult<Destination> {
        let rest = url.strip_prefix("s3://").expect("the caller checked the scheme");
        let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
        let store: PyS3Store = py.get_type::<PyS3Store>().call((bucket,), store_kwargs.as_ref())?.extract()?;
        let store: Arc<dyn ObjectStore> = store.into_inner();
        let prefix = object_store::path::Path::parse(prefix.trim_end_matches('/')).map_err(|error| {
            refused(
                py,
                WriteError::ObjectName {
                    name: prefix.into(),
                    reason: error.to_string(),
                },
            )
        })?;
        Ok(Destination::new(store, prefix, url))
    }

    /// The library's ``UngenerableProfile``, naming the column or section the engine refused.
    fn plan_refused(py: Python<'_>, source: &str, error: PlanError) -> PyErr {
        let (code, field, message) = match &error {
            PlanError::Column { code, field, message } => (code.code(), field.clone(), message.clone()),
            PlanError::Bundle { .. } => ("bundle", "/kind".to_string(), error.to_string()),
            PlanError::NoColumns { .. } => ("no-columns", "/columns".to_string(), error.to_string()),
            PlanError::DuplicateColumn { .. } => ("duplicate-column", "/columns".to_string(), error.to_string()),
            PlanError::ProfileDigest { .. } => return PyValueError::new_err(error.to_string()),
        };
        library_error(|| {
            let module = py.import("doppeldata.profile_format")?;
            let issue = module.getattr("Issue")?.call1((code, field, message))?;
            module.getattr("UngenerableProfile")?.call1((source, vec![issue]))
        })
    }

    /// The code a failed write is reported by, and the exit status a command ends on: 2 for what
    /// the caller fixes by changing the call, 1 for any other failure.
    fn failure_code(error: &WriteError) -> (&'static str, u8) {
        match error {
            WriteError::Layout(_) => ("invalid-layout", 2),
            WriteError::Settings(_) => ("invalid-settings", 2),
            WriteError::UnsupportedColumn { .. } => ("unsupported-column", 2),
            WriteError::DestinationNotEmpty { .. } => ("destination-not-empty", 2),
            WriteError::ForeignObject { .. } => ("foreign-object", 2),
            WriteError::Unclaimable { .. } => ("unclaimable", 2),
            WriteError::ObjectName { .. } => ("invalid-object-name", 2),
            WriteError::DestinationClaimed { .. } => ("destination-claimed", 1),
            WriteError::CurrentThreadRuntime => ("current-thread-runtime", 1),
            WriteError::IdentityMismatch { .. } => ("identity-mismatch", 1),
            WriteError::PartialManifest => ("partial-manifest", 1),
            WriteError::RegeneratedFilesDiffer { .. } => ("regenerated-files-differ", 1),
            WriteError::Cancelled => ("cancelled", 1),
            WriteError::Format { .. } => ("unformattable-value", 1),
            WriteError::Block(_) => ("block-out-of-range", 1),
            WriteError::Store(_) => ("store", 1),
            WriteError::Arrow(_) => ("arrow", 1),
            WriteError::Parquet(_) => ("parquet", 1),
            WriteError::Io(_) => ("io", 1),
        }
    }

    /// The library's ``WriteFailed`` for an error raised before the write claimed its destination.
    fn refused(py: Python<'_>, error: WriteError) -> PyErr {
        write_failed(py, WriteFailure::from(error), false)
    }

    /// The library's ``WriteFailed``, carrying the manifest of what the run wrote where it has one.
    /// ``interrupted`` says a signal arrived, which raises ``WriteInterrupted`` instead.
    fn write_failed(py: Python<'_>, failure: WriteFailure, interrupted: bool) -> PyErr {
        let (code, exit_code) = failure_code(&failure.error);
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
            kwargs.set_item("exit_code", exit_code)?;
            kwargs.set_item("manifest_json", manifest)?;
            kwargs.set_item("manifest_stored", failure.manifest_stored)?;
            let class = match interrupted {
                true => "WriteInterrupted",
                false => "WriteFailed",
            };
            py.import("doppeldata.errors")?.getattr(class)?.call((code, message), Some(&kwargs))
        })
    }

    /// The exception ``build`` makes, or the error building it raised.
    fn library_error<'py>(build: impl FnOnce() -> PyResult<Bound<'py, PyAny>>) -> PyErr {
        match build() {
            Ok(exception) => PyErr::from_value(exception),
            Err(error) => error,
        }
    }
}
