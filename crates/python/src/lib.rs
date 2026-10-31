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
    use doppeldata_core::AppliedScenario;
    use doppeldata_core::PlanError;
    use doppeldata_core::PlanInputs;
    use doppeldata_core::column_overrides::ColumnOverride;
    use doppeldata_core::profile::Profile;
    use doppeldata_core::scenario::Scenario;
    use doppeldata_writers::Destination;
    use doppeldata_writers::DestinationUrl;
    use doppeldata_writers::FailureKind;
    use doppeldata_writers::LayoutError;
    use doppeldata_writers::RangesWritten;
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
    use pyo3::types::PyString;
    use pyo3_arrow::PyRecordBatchReader;
    use pyo3_arrow::PyTable;
    use pyo3_object_store::PyS3Store;
    use serde_json::json;

    /// The engine's release, which is the tag the wheel was built from.
    #[pymodule_export]
    const VERSION: &str = doppeldata_core::VERSION;

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

    /// The name of the record a whole write leaves beside its files.
    #[pymodule_export]
    const MANIFEST_FILE_NAME: &str = doppeldata_core::manifest::MANIFEST_FILE_NAME;

    /// The metadata key a file holds the whole profile's canonical JSON under, where its write embeds it.
    #[pymodule_export]
    const PROFILE_KEY: &str = doppeldata_core::profile::provenance::PROFILE;

    /// The metadata key a file holds its scenario's canonical document under, where its write embeds the profile.
    #[pymodule_export]
    const SCENARIO_KEY: &str = doppeldata_core::profile::provenance::SCENARIO;

    /// The metadata key every Parquet and Arrow IPC file holds its dataset's seed under.
    #[pymodule_export]
    const SEED_KEY: &str = doppeldata_core::profile::provenance::SEED;

    /// The metadata key every Parquet and Arrow IPC file holds its dataset's generated rows under.
    #[pymodule_export]
    const DATASET_ROWS_KEY: &str = doppeldata_core::profile::provenance::DATASET_ROWS;

    /// The metadata key every Parquet and Arrow IPC file holds its profile's digest under.
    #[pymodule_export]
    const PROFILE_DIGEST_KEY: &str = doppeldata_core::profile::provenance::PROFILE_DIGEST;

    /// The metadata key a Parquet or Arrow IPC file written under a scenario holds its digest under.
    #[pymodule_export]
    const SCENARIO_DIGEST_KEY: &str = doppeldata_core::profile::provenance::SCENARIO_DIGEST;

    /// The metadata key every Parquet and Arrow IPC file holds the engine's stream version under.
    #[pymodule_export]
    const STREAM_VERSION_KEY: &str = doppeldata_core::profile::provenance::STREAM_VERSION;

    /// The metadata key every Parquet and Arrow IPC file holds its dataset's digest under.
    #[pymodule_export]
    const DATASET_DIGEST_KEY: &str = doppeldata_core::profile::provenance::DATASET_DIGEST;

    /// The name of the claim a write holds its destination with, and the record a stopped write leaves.
    #[pymodule_export]
    const PARTIAL_MANIFEST_FILE_NAME: &str = doppeldata_core::manifest::PARTIAL_MANIFEST_FILE_NAME;

    /// The name of the list of faults a write under `malformed_rows` leaves beside its files.
    #[pymodule_export]
    const FAULTS_FILE_NAME: &str = doppeldata_core::manifest::FAULTS_FILE_NAME;

    /// The head of a text file the profiler reads its dialect, JSON layout and encoding from.
    #[pymodule_export]
    const FIRST_BLOCK_BYTES: u64 = doppeldata_writers::FIRST_BLOCK_BYTES;

    /// How often a call waiting on the engine checks for a signal, such as the interrupt Ctrl-C sends.
    const SIGNAL_CHECK: Duration = Duration::from_millis(100);

    /// The file name under ``doppeldata/schemas/`` of each JSON Schema the library ships, by name.
    #[pyfunction]
    fn schema_file_names() -> BTreeMap<&'static str, &'static str> {
        doppeldata_core::published_schemas::all()
            .into_iter()
            .map(|schema| (schema.name, schema.file_name))
            .collect()
    }

    /// Each ``fanout.top_share`` field's name and the rank, at scale 1, its share is held at,
    /// busiest first.
    #[pyfunction]
    fn top_share_ranks() -> Vec<(&'static str, u64)> {
        doppeldata_core::profile::TopShare::PINS.to_vec()
    }

    type PerturbationFacts = (String, Vec<String>, bool, bool, bool);

    /// Each of a valid scenario's perturbations, in list order: its id, the columns it writes, adds,
    /// drops or renames, whether it shapes the files a write makes rather than the rows, whether it
    /// acts across several files, so a one-file write cannot hold it, and whether it orders the whole
    /// table, so one process writes the dataset.
    #[pyfunction]
    fn perturbation_facts(scenario_json: &str) -> PyResult<Vec<PerturbationFacts>> {
        let scenario = read_scenario(scenario_json)?;
        let ids = scenario
            .resolved_ids()
            .map_err(|duplicate| PyValueError::new_err(format!("two perturbations have the id {:?}", duplicate.id)))?;
        Ok(scenario
            .perturbations
            .iter()
            .zip(ids)
            .map(|(perturbation, id)| {
                let written = perturbation.written_columns().into_iter().map(String::from).collect();
                (
                    id,
                    written,
                    perturbation.shapes_files(),
                    perturbation.spans_files(),
                    perturbation.needs_one_process(),
                )
            })
            .collect())
    }

    /// The op whose perturbation an expectation's metric measures, such as ``hot_keys`` for
    /// ``hot_keys.share``.
    #[pyfunction]
    fn expected_metric_op(metric: &str) -> PyResult<&'static str> {
        serde_json::from_value::<doppeldata_core::scenario::ExpectedMetric>(metric.into())
            .map(|metric| metric.op())
            .map_err(|_| PyValueError::new_err(format!("{metric:?} is not a metric an expectation can name")))
    }

    /// The fault list a write to the one file ``file_name`` keeps beside it, such as
    /// ``_orders.csv.faults.jsonl``.
    #[pyfunction]
    fn one_file_faults_name(file_name: &str) -> String {
        doppeldata_core::manifest::one_file_faults_name(file_name)
    }

    /// The key-value metadata of the local Parquet or Arrow IPC file at ``path``, or None for a
    /// file of neither format. Raises OSError where the file cannot be opened, and ValueError where
    /// its footer or schema does not read.
    #[pyfunction]
    fn data_file_metadata(path: std::path::PathBuf) -> PyResult<Option<BTreeMap<String, String>>> {
        doppeldata_writers::file_metadata(&path).map_err(|error| match error {
            doppeldata_writers::FileMetadataError::Io(error) => error.into(),
            other => PyValueError::new_err(other.to_string()),
        })
    }

    /// Whether an expectation's metric measures one column, so the expectation names ``column``.
    #[pyfunction]
    fn expected_metric_takes_column(metric: &str) -> PyResult<bool> {
        serde_json::from_value::<doppeldata_core::scenario::ExpectedMetric>(metric.into())
            .map(|metric| metric.takes_column())
            .map_err(|_| PyValueError::new_err(format!("{metric:?} is not a metric an expectation can name")))
    }

    /// Of the fault kind ``kind`` and files written as ``format``: whether the kind breaks whole
    /// files, whether the files can hold it, and whether ``data check`` reads them back as one table
    /// while they hold it.
    #[pyfunction]
    fn fault_kind_facts(kind: &str, format: &str) -> PyResult<(bool, bool, bool)> {
        let fault = serde_json::from_value::<doppeldata_core::scenario::FaultKind>(kind.into())
            .map_err(|_| PyValueError::new_err(format!("{kind:?} is not a fault kind malformed_rows writes")))?;
        let output = serde_json::from_value::<doppeldata_core::format_settings::Output>(json!({"format": format, "writer": {}}))
            .map_err(|_| PyValueError::new_err(format!("{format:?} is not a format doppeldata writes")))?;
        Ok((fault.is_file_kind(), fault.formats().hold(&output), fault.check_reads_back(&output)))
    }

    /// ``profile_json`` with the scenario's patch applied as the engine applies it, and each column
    /// the patch names that the profile does not hold.
    #[pyfunction]
    fn patched_profile(profile_json: &str, scenario_json: &str) -> PyResult<(String, Vec<String>)> {
        let scenario = read_scenario(scenario_json)?;
        let profile: serde_json::Value =
            serde_json::from_str(profile_json).map_err(|error| PyValueError::new_err(format!("the profile is not JSON: {error}")))?;
        let (patched, unknown) = match &scenario.patch {
            Some(patch) => doppeldata_core::scenario::patched_profile(&profile, patch),
            None => (profile, Vec::new()),
        };
        Ok((patched.to_string(), unknown))
    }

    fn read_scenario(scenario_json: &str) -> PyResult<Scenario> {
        serde_json::from_str(scenario_json)
            .map_err(|error| PyValueError::new_err(format!("the scenario does not match the engine's scenario model: {error}")))
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
    /// with the scenario ``scenario`` gives as its JSON and its digest, made on ``threads`` threads
    /// the way ``Plan.generate`` makes its blocks. Under ``cdc`` it also covers every change file
    /// and the expected table. A wheel's build compares it with the hash the contract pins.
    #[pyfunction]
    #[pyo3(signature = (profile_json, profile_digest, seed, rows, *, threads, scenario=None))]
    fn determinism_values_digest(
        py: Python<'_>,
        profile_json: &str,
        profile_digest: &str,
        seed: u64,
        rows: u64,
        threads: usize,
        scenario: Option<(String, String)>,
    ) -> PyResult<String> {
        let scenario = scenario.map(|(json, digest)| (json, digest, "the scenario".to_string()));
        let plan = Plan::new(
            &PyString::new(py, profile_json),
            profile_digest,
            seed,
            rows,
            "the profile",
            scenario,
            None,
        )?;
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
        let blocks = blocks.expect("only a signal cancels");
        doppeldata_core::plan_values_digest(plan, &blocks, threads.max(1))
            .map_err(|error| scenario_refused(py, "the scenario", error.code(), error.field(), &plan_message(&error)))
    }

    /// A table profile resolved for generation: ``rows`` rows from ``seed``. ``source`` names the
    /// profile in an error. ``scenario``, where given, is the scenario applied: its JSON with each
    /// perturbation's id resolved, its digest, and the name an error gives it. The profile is the
    /// one given, and the engine applies the scenario's patch to it. ``overrides``, where given, is
    /// a JSON list of each pinned column and its values, as ``column_overrides::ColumnOverride``
    /// reads it, and a value its column cannot hold raises ValueError.
    #[pyclass(frozen)]
    struct Plan {
        profile: Arc<Profile>,
        plan: Arc<doppeldata_core::Plan>,
        scenario_source: Option<String>,
    }

    #[pymethods]
    impl Plan {
        #[new]
        #[pyo3(signature = (profile_json, profile_digest, seed, rows, source, scenario=None, overrides=None))]
        fn new(
            profile_json: &Bound<'_, PyString>,
            profile_digest: &str,
            seed: u64,
            rows: u64,
            source: &str,
            scenario: Option<(String, String, String)>,
            overrides: Option<&str>,
        ) -> PyResult<Self> {
            let py = profile_json.py();
            let profile_json = profile_json.to_str()?;
            let overrides: Vec<ColumnOverride> = match overrides {
                Some(json) => serde_json::from_str(json)
                    .map_err(|error| PyValueError::new_err(format!("the overrides do not match the engine's model: {error}")))?,
                None => Vec::new(),
            };
            let read: Option<(Scenario, String, String)> = scenario
                .map(|(json, digest, scenario_source)| {
                    serde_json::from_str(&json)
                        .map(|read| (read, digest, scenario_source.clone()))
                        .map_err(|error| {
                            PyValueError::new_err(format!("{scenario_source} does not match the engine's scenario model: {error}"))
                        })
                })
                .transpose()?;
            let not_the_model =
                |error: serde_json::Error| PyValueError::new_err(format!("{source} does not match the engine's profile model: {error}"));
            let document: serde_json::Value = serde_json::from_str(profile_json).map_err(not_the_model)?;
            let document = match &read {
                Some((scenario, _, scenario_source)) => scenario
                    .patched_profile(&document)
                    .map_err(|error| scenario_refused(py, scenario_source, error.code(), error.field(), &plan_message(&error)))?,
                None => document,
            };
            let profile: Profile = serde_json::from_value(document).map_err(not_the_model)?;
            let applied = read.as_ref().map(|(scenario, digest, _)| AppliedScenario { scenario, digest });
            let scenario_source = read.as_ref().map(|(_, _, scenario_source)| scenario_source.clone());
            let inputs = PlanInputs {
                scenario: applied,
                overrides: &overrides,
            };
            match py.detach(|| doppeldata_core::plan_with(&profile, profile_digest, seed, rows, inputs)) {
                Ok(plan) => Ok(Self {
                    profile: Arc::new(profile),
                    plan: Arc::new(plan),
                    scenario_source,
                }),
                Err(error) if error.in_scenario() => Err(scenario_refused(
                    py,
                    scenario_source.as_deref().unwrap_or("the scenario"),
                    error.code(),
                    error.field(),
                    &plan_message(&error),
                )),
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

        /// The rows the files hold: those generated, and every copy the scenario plans.
        #[getter]
        fn rows_written(&self) -> u64 {
            self.plan.rows_written()
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

        /// The names of the columns each Gaussian copula draws together, each group sorted.
        fn copula_groups(&self) -> Vec<Vec<String>> {
            self.plan.copula_groups()
        }

        /// The JSON pointer of each dependence entry the profile states and the plan does not draw,
        /// each child's sampler naming why.
        fn set_aside(&self) -> Vec<String> {
            self.plan.set_aside()
        }

        /// The JSON Pointer into the profile of each column, nested fields included, that the plan
        /// generates as text, with the ``type_mix`` type its own draws are written as, in profile
        /// order. A column is text where its type mix holds values of other types, or where a bundle
        /// copies its values from a parent's text.
        fn type_mix_pointers(&self) -> Vec<(String, &'static str)> {
            self.plan
                .type_mix_pointers()
                .into_iter()
                .map(|(pointer, own)| (pointer, doppeldata_core::raw_type_name(own)))
                .collect()
        }

        /// Each perturbation's id and the columns it writes as planned, in the scenario's order.
        /// A ``hot_keys`` on a foreign key writes every column its relationship draws or copies.
        fn perturbation_writes(&self) -> Vec<(String, Vec<String>)> {
            self.plan.perturbation_writes().to_vec()
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
                serde_json::from_str(choices_json).map_err(|error| self.refused(py, WriteError::Settings(error.to_string())))?;
            let options = doppeldata_writers::WriteOptions::chosen(&self.profile, self.plan.file_rows(), choices)
                .map_err(|error| self.refused(py, error))?;
            Ok(WriteOptions { options })
        }

        /// Each row range the write would make, as a JSON list of objects keyed as the manifest
        /// keys its files: ``path``, ``range``, ``first_row`` and ``range_rows``, with
        /// ``objects_at_most``. A partitioned range's path holds ``*`` for each partition value.
        fn ranges_json(&self, py: Python<'_>, options: PyRef<'_, WriteOptions>) -> PyResult<String> {
            let ranges = doppeldata_writers::planned_ranges(&self.plan, &options.options).map_err(|error| self.refused(py, error))?;
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

        /// The change stream a ``cdc`` write makes beside its full load, as a JSON object keyed
        /// ``style``, ``prefix``, ``expected_prefix``, ``changes``, ``change_files_at_most`` and
        /// ``expected_parts``, or None without a ``cdc``. Every change is drawn to count it.
        fn stream_json(&self, py: Python<'_>, options: PyRef<'_, WriteOptions>) -> PyResult<Option<String>> {
            let (plan, options) = (&*self.plan, &options.options);
            let stream = py
                .detach(|| doppeldata_writers::planned_stream(plan, options))
                .map_err(|error| self.refused(py, error))?;
            Ok(stream.map(|stream| {
                json!({
                    "style": stream.style,
                    "prefix": stream.prefix,
                    "expected_prefix": stream.expected_prefix,
                    "changes": stream.changes,
                    "change_files_at_most": stream.change_files_at_most,
                    "expected_parts": stream.expected_parts,
                })
                .to_string()
            }))
        }

        /// The names of the columns each file of the rows holds, in order: the plan's less those
        /// ``partition_by`` names, or under ``cdc`` the full load's record.
        fn file_columns(&self, partition_by: Vec<String>) -> Vec<String> {
            doppeldata_writers::file_columns(&self.plan, &partition_by)
        }

        /// The format and every setting the write's manifest records, as ``WriteOptions``'s
        /// ``settings_json`` spells them, with a ``cdc`` stream's timestamp unit applied.
        fn written_settings_json(&self, options: PyRef<'_, WriteOptions>) -> String {
            let output = doppeldata_writers::planned_output(&self.plan, &options.options);
            let mut settings = serde_json::to_value(&output).expect("an output always serializes");
            settings["layout"] = serde_json::to_value(&options.options.layout).expect("a layout always serializes");
            settings.to_string()
        }

        /// The bytes the write would store, its manifest left out, from its first blocks encoded
        /// with its settings.
        fn estimate_bytes(&self, py: Python<'_>, options: PyRef<'_, WriteOptions>) -> PyResult<u64> {
            let (plan, options) = (&*self.plan, &options.options);
            py.detach(|| doppeldata_writers::estimate_bytes(plan, options))
                .map_err(|error| self.refused(py, error))
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
            let manifest = self.settled(py, &cancel, || doppeldata_writers::write(plan, options, &run, &destination))?;
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
            self.settled(py, &cancel, || doppeldata_writers::claim(plan, options, &run, &destination))
        }

        /// Writes the objects of the ranges numbered ``ranges`` in the layout ``ranges_json``
        /// lists, the bytes ``write`` writes for them, on one thread. It returns their manifest
        /// records and the faults ``malformed_rows`` placed in them as one JSON object, ``files``
        /// and ``faults``. It neither claims ``destination`` nor stores a manifest.
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
            let written = self.settled(py, &cancel, || {
                doppeldata_writers::write_ranges(plan, options, &ranges, &run, &destination)
            })?;
            Ok(serde_json::to_string(&written).expect("a manifest record always serializes"))
        }

        /// Stores the manifest of a write ``claim`` claimed, and the fault list under
        /// ``malformed_rows``, from what each ``write_ranges`` call returned, each string as it
        /// returned it, and returns the manifest as JSON. ``tasks`` is how many the ranges were
        /// divided among, and ``elapsed_ms`` the time from the claim. A range with no record leaves
        /// the partial manifest under the claim's name, and raises.
        #[pyo3(signature = (options, destination, written, *, tasks, elapsed_ms, s3_store=None))]
        fn commit(
            &self,
            options: PyRef<'_, WriteOptions>,
            destination: &str,
            written: Vec<String>,
            tasks: u32,
            elapsed_ms: u64,
            s3_store: Option<Bound<'_, PyDict>>,
        ) -> PyResult<String> {
            let py = options.py();
            let mut files = RangesWritten::default();
            for part in &written {
                let part: RangesWritten = serde_json::from_str(part)
                    .map_err(|error| PyValueError::new_err(format!("written holds a string write_ranges did not return: {error}")))?;
                files.files.extend(part.files);
                files.faults.extend(part.faults);
            }
            let destination = destination_at(py, destination, s3_store)?;
            let (run, cancel) = run_options(1, false);
            let (plan, options) = (&*self.plan, &options.options);
            let split = SplitRun {
                tasks,
                threads: SPLIT_TASK_THREADS as u32,
                elapsed_ms,
            };
            let manifest = self.settled(py, &cancel, || {
                doppeldata_writers::commit(plan, options, files, split, &run, &destination)
            })?;
            Ok(serde_json::to_string(&manifest).expect("a manifest always serializes"))
        }
    }

    impl Plan {
        /// The library's ``WriteFailed`` for an error raised before the write claimed its
        /// destination, or its ``InvalidScenario`` where the files cannot hold the scenario.
        fn refused(&self, py: Python<'_>, error: WriteError) -> PyErr {
            write_failed(py, WriteFailure::from(error), false, self.scenario_source.as_deref())
        }

        /// Runs a writers call detached, as the library's result or its ``WriteFailed``, or
        /// ``WriteInterrupted`` when a signal stopped it, or ``InvalidScenario`` where the files
        /// cannot hold the scenario.
        fn settled<T: Send>(
            &self,
            py: Python<'_>,
            cancel: &AtomicBool,
            call: impl FnOnce() -> Result<T, WriteFailure> + Send,
        ) -> PyResult<T> {
            match detached(py, cancel, call) {
                (Ok(_), Some(signal)) => Err(signal),
                (Ok(value), None) => Ok(value),
                (Err(failure), signal) => Err(write_failed(py, failure, signal.is_some(), self.scenario_source.as_deref())),
            }
        }
    }

    /// A bundle resolved for generation: a ``Plan`` for each table, every parent before its children,
    /// at ``scale`` times each table's row count from ``seed``. ``bundle_json`` holds every table
    /// inline. ``scenarios``, where given, is each scenario's JSON with its perturbation ids
    /// resolved and the source an error names it by, beside the digest of the whole set.
    #[pyclass(frozen)]
    struct BundlePlan {
        plan: Arc<doppeldata_core::BundlePlan>,
        tables: Vec<(String, Py<Plan>)>,
        patched: String,
    }

    #[pymethods]
    impl BundlePlan {
        #[new]
        #[pyo3(signature = (bundle_json, bundle_digest, seed, scale, source, scenarios=None))]
        fn new(
            py: Python<'_>,
            bundle_json: &str,
            bundle_digest: &str,
            seed: u64,
            scale: f64,
            source: &str,
            scenarios: Option<(Vec<(String, String)>, String)>,
        ) -> PyResult<Self> {
            let not_the_model =
                |error: serde_json::Error| PyValueError::new_err(format!("{source} does not match the engine's profile model: {error}"));
            let document: serde_json::Value = serde_json::from_str(bundle_json).map_err(not_the_model)?;
            let mut read: BTreeMap<String, Scenario> = BTreeMap::new();
            let mut sources: BTreeMap<String, String> = BTreeMap::new();
            let digest = scenarios.as_ref().map(|(_, digest)| digest.clone());
            for (json, scenario_source) in scenarios.map(|(listed, _)| listed).unwrap_or_default() {
                let scenario: Scenario = serde_json::from_str(&json).map_err(|error| {
                    PyValueError::new_err(format!("{scenario_source} does not match the engine's scenario model: {error}"))
                })?;
                let Some(table) = scenario.table.clone() else {
                    let message = "a bundle's scenario names the table it bends";
                    return Err(scenario_refused(py, &scenario_source, "unknown-table", "/table", message));
                };
                if read.contains_key(&table) {
                    let message = format!("another scenario bends {table}; compose a table's scenarios into one");
                    return Err(scenario_refused(py, &scenario_source, "unknown-table", "/table", &message));
                }
                sources.insert(table.clone(), scenario_source);
                read.insert(table, scenario);
            }
            let source_of = |error: &PlanError| {
                let table = match error {
                    PlanError::Scenario { .. } | PlanError::ScenarioDigest { .. } => read.keys().next(),
                    _ => None,
                };
                table
                    .and_then(|table| sources.get(table))
                    .map_or("the scenario", String::as_str)
                    .to_string()
            };
            let patched = doppeldata_core::scenario::patched_bundle(&document, &read).map_err(|error| {
                let scenario_source = source_of(&error);
                scenario_refused(py, &scenario_source, error.code(), error.field(), &plan_message(&error))
            })?;
            let patched_json = patched.to_string();
            let Profile::Bundle(bundle) = serde_json::from_value(patched).map_err(not_the_model)? else {
                return Err(PyValueError::new_err(format!("{source} is a table profile, which Plan plans")));
            };
            let applied = digest
                .as_deref()
                .map(|digest| doppeldata_core::BundleScenarios { digest, tables: &read });
            let planned = py.detach(|| doppeldata_core::plan_bundle(&bundle, bundle_digest, applied, seed, scale));
            let plan = match planned {
                Ok(plan) => plan,
                Err(error) if error.in_scenario() => {
                    let scenario_source = source_of(&error);
                    return Err(scenario_refused(
                        py,
                        &scenario_source,
                        error.code(),
                        error.field(),
                        &plan_message(&error),
                    ));
                }
                Err(error) => return Err(plan_refused(py, source, error)),
            };
            let mut tables = Vec::new();
            for (name, table_plan) in plan.tables() {
                let Some(doppeldata_core::profile::BundleTable::Inline(table)) = bundle.tables.get(name) else {
                    unreachable!("the engine plans only inline tables")
                };
                let held = Plan {
                    profile: Arc::new(Profile::Table(table.clone())),
                    plan: table_plan.clone(),
                    scenario_source: sources.get(name).cloned(),
                };
                tables.push((name.to_string(), Py::new(py, held)?));
            }
            Ok(Self {
                plan: Arc::new(plan),
                tables,
                patched: patched_json,
            })
        }

        /// The bundle as the plan draws it, as JSON: every table inline, each scenario's patch
        /// applied to its table and to the relationships drawing that table's columns.
        #[getter]
        fn bundle_json(&self) -> String {
            self.patched.clone()
        }

        /// The bundle's own identity as RFC 8785 canonical JSON: no table, and every table's rows summed.
        #[getter]
        fn identity_json(&self) -> String {
            self.plan.identity().canonical_json()
        }

        #[getter]
        fn dataset_digest(&self) -> String {
            self.plan.identity().digest()
        }

        /// Each table's name and plan, every parent before its children.
        fn tables(&self, py: Python<'_>) -> Vec<(String, Py<Plan>)> {
            self.tables.iter().map(|(name, plan)| (name.clone(), plan.clone_ref(py))).collect()
        }

        /// Each relationship as JSON, in the bundle's order: its name, pointer, child and parent
        /// tables and columns, what draws its columns instead where another relationship does, each
        /// ``inherited`` entry and ``ordinal`` set aside with why, and the columns it copies and
        /// numbers where it is drawn.
        #[getter]
        fn relationships_json(&self) -> PyResult<String> {
            serde_json::to_string(self.plan.relationships()).map_err(|error| PyValueError::new_err(error.to_string()))
        }

        /// The pointer of each relationship, ``inherited`` entry and ``ordinal`` the bundle states and
        /// the plan does not draw.
        fn set_aside(&self) -> Vec<String> {
            self.plan.set_aside()
        }

        /// Claims ``destination`` for the bundle and returns the claim as JSON, which ``commit``
        /// takes back. ``profile_json`` and ``scenario_json`` are the manifest's ``profile`` and
        /// ``scenario`` records. Each table is then written to ``<destination>/<table>``.
        #[pyo3(signature = (destination, profile_json, scenario_json, *, replace, s3_store=None))]
        fn claim(
            &self,
            py: Python<'_>,
            destination: &str,
            profile_json: &str,
            scenario_json: Option<&str>,
            replace: bool,
            s3_store: Option<Bound<'_, PyDict>>,
        ) -> PyResult<String> {
            let unreadable = |error: serde_json::Error| refused(py, WriteError::Settings(error.to_string()));
            let profile = serde_json::from_str(profile_json).map_err(unreadable)?;
            let scenario = scenario_json.map(serde_json::from_str).transpose().map_err(unreadable)?;
            let destination = destination_at(py, destination, s3_store)?;
            let claim = doppeldata_writers::bundle_manifest(&self.plan, profile, scenario, &destination);
            let (run, cancel) = run_options(1, replace);
            match detached(py, &cancel, || doppeldata_writers::claim_bundle(&claim, &run, &destination)) {
                (Ok(()), None) => Ok(serde_json::to_string(&claim).expect("a manifest always serializes")),
                (Ok(()), Some(signal)) => Err(signal),
                (Err(failure), signal) => Err(write_failed(py, failure, signal.is_some(), None)),
            }
        }

        /// Stores the bundle's manifest and returns it as JSON: whole as ``_manifest.json`` once
        /// every table is, else partial under the claim's name. ``claim_json`` is what ``claim``
        /// returned, ``written`` each table's manifest JSON as its write returned it, and
        /// ``elapsed_ms`` the time from the claim. The manifest takes the most threads and tasks
        /// any table used.
        #[pyo3(signature = (destination, claim_json, written, *, elapsed_ms, s3_store=None))]
        fn commit(
            &self,
            py: Python<'_>,
            destination: &str,
            claim_json: &str,
            written: Vec<String>,
            elapsed_ms: u64,
            s3_store: Option<Bound<'_, PyDict>>,
        ) -> PyResult<String> {
            let unreadable = |error: serde_json::Error| PyValueError::new_err(format!("not a manifest commit takes: {error}"));
            let mut claim: doppeldata_core::manifest::BundleManifest = serde_json::from_str(claim_json).map_err(unreadable)?;
            claim.elapsed_ms = elapsed_ms;
            let written: Vec<doppeldata_core::manifest::Manifest> = written
                .iter()
                .map(|json| serde_json::from_str(json))
                .collect::<Result<_, _>>()
                .map_err(unreadable)?;
            let destination = destination_at(py, destination, s3_store)?;
            let (run, cancel) = run_options(1, false);
            match detached(py, &cancel, || {
                doppeldata_writers::commit_bundle(claim, &written, &run, &destination)
            }) {
                (Ok(manifest), None) => Ok(serde_json::to_string(&manifest).expect("a manifest always serializes")),
                (Ok(_), Some(signal)) => Err(signal),
                (Err(failure), signal) => Err(write_failed(py, failure, signal.is_some(), None)),
            }
        }
    }

    /// The determinism contract's SHA-256 over every table a bundle generates at ``scale``, each
    /// table's blocks made on ``threads`` threads, the tables parent first.
    #[pyfunction]
    #[pyo3(signature = (bundle_json, bundle_digest, seed, scale, *, threads))]
    fn determinism_bundle_values_digest(
        py: Python<'_>,
        bundle_json: &str,
        bundle_digest: &str,
        seed: u64,
        scale: f64,
        threads: usize,
    ) -> PyResult<String> {
        let bundle = BundlePlan::new(py, bundle_json, bundle_digest, seed, scale, "the bundle", None)?;
        let cancel = AtomicBool::new(false);
        let mut tables = Vec::new();
        for (_, table) in &bundle.tables {
            let plan = &*table.get().plan;
            let (blocks, signal) = detached(py, &cancel, || {
                in_parallel(plan.block_count(), threads, &cancel, |block| {
                    let batch = plan.generate_block(block).expect("every block below block_count is in range");
                    (batch, plan.text_cells(block).expect("every block below block_count is in range"))
                })
            });
            if let Some(signal) = signal {
                return Err(signal);
            }
            tables.push(blocks.expect("only a signal cancels"));
        }
        Ok(doppeldata_core::bundle_values_digest(&tables))
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

    /// What a refusal says, without the pointer and code its display wraps it in.
    fn plan_message(error: &PlanError) -> String {
        match error {
            PlanError::Column { message, .. }
            | PlanError::Scenario { message, .. }
            | PlanError::Relationship { message, .. }
            | PlanError::Override { message, .. } => message.clone(),
            _ => error.to_string(),
        }
    }

    /// The library's ``UngenerableProfile``, naming the column or section the engine refused.
    fn plan_refused(py: Python<'_>, source: &str, error: PlanError) -> PyErr {
        if let PlanError::ProfileDigest { .. } = error {
            return PyValueError::new_err(error.to_string());
        }
        let message = plan_message(&error);
        library_error(|| {
            let module = py.import("doppeldata.profile_format")?;
            let issue = module.getattr("Issue")?.call1((error.code(), error.field(), message))?;
            module.getattr("UngenerableProfile")?.call1((source, vec![issue]))
        })
    }

    /// The library's ``InvalidScenario`` for ``source``, naming the perturbation refused at ``field``.
    fn scenario_refused(py: Python<'_>, source: &str, code: &str, field: &str, message: &str) -> PyErr {
        library_error(|| {
            let issue = py
                .import("doppeldata.profile_format")?
                .getattr("Issue")?
                .call1((code, field, message))?;
            py.import("doppeldata.scenario_format")?
                .getattr("InvalidScenario")?
                .call1((source, vec![issue]))
        })
    }

    /// The ids of a scenario's perturbations, and the index and id of one repeating an earlier's.
    type ResolvedIds = (Vec<String>, Option<(usize, String)>);

    /// Each perturbation's id in ``scenario_json``, in list order. Where one repeats an earlier
    /// one's, the list is empty and the second item is its index and id.
    #[pyfunction]
    fn scenario_ids(scenario_json: &str) -> PyResult<ResolvedIds> {
        let scenario = read_scenario(scenario_json)?;
        Ok(match scenario.resolved_ids() {
            Ok(ids) => (ids, None),
            Err(duplicate) => (Vec::new(), Some((duplicate.index, duplicate.id))),
        })
    }

    /// The library's ``WriteFailed`` for an error raised before the write claimed its destination.
    fn refused(py: Python<'_>, error: WriteError) -> PyErr {
        write_failed(py, WriteFailure::from(error), false, None)
    }

    /// The library's ``WriteFailed``, carrying the manifest of what the run wrote where it has one.
    /// ``interrupted`` says a signal arrived, which raises ``WriteInterrupted`` instead. A layout
    /// the scenario named ``scenario_source`` cannot hold raises its ``InvalidScenario``.
    fn write_failed(py: Python<'_>, failure: WriteFailure, interrupted: bool, scenario_source: Option<&str>) -> PyErr {
        if let WriteError::Layout(LayoutError::Scenario { field, message, .. }) = &failure.error {
            let (code, _) = failure.error.code();
            return scenario_refused(py, scenario_source.unwrap_or("the scenario"), code, field, message);
        }
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

    /// The fewest and most characters each provider's values hold, by the provider's name, read
    /// from the lists and templates it draws from.
    #[pyfunction]
    fn provider_lengths() -> BTreeMap<String, (usize, usize)> {
        doppeldata_core::provider_lengths()
    }

    /// ``pattern`` with its repetitions bounded so every value drawn from it holds at least
    /// ``min_length`` and at most ``max_length`` characters, either None where unbounded. None where
    /// no bounds do. A pattern whose values already fit is returned as given.
    #[pyfunction]
    #[pyo3(signature = (pattern, min_length, max_length))]
    fn pattern_within_lengths(pattern: &str, min_length: Option<usize>, max_length: Option<usize>) -> Option<String> {
        doppeldata_core::pattern_within_lengths(pattern, min_length, max_length)
    }

    /// Whether each value holds the check digit ``kind`` names, such as ``luhn``, where generation
    /// writes one: its last digit, computed from the digits before it.
    #[pyfunction]
    fn check_digits_hold(kind: &str, values: Vec<String>) -> PyResult<Vec<bool>> {
        let kind: doppeldata_core::profile::CheckDigit = kind.parse().map_err(pyo3::exceptions::PyValueError::new_err)?;
        Ok(values.iter().map(|value| doppeldata_core::check_digit_holds(kind, value)).collect())
    }
}
