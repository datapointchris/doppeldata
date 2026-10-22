use pyo3::prelude::*;

/// doppeldata's engine, compiled.
#[pymodule]
mod _core {
    use std::collections::BTreeMap;

    use pyo3::prelude::*;

    /// The engine's release, which is the tag the wheel was built from.
    #[pymodule_export]
    const VERSION: &str = doppeldata_core::VERSION;

    /// The profile format version the engine reads.
    #[pymodule_export]
    const FORMAT_VERSION: u64 = doppeldata_core::profile::FORMAT_VERSION;

    /// The file name under ``doppeldata/schemas/`` of each JSON Schema the library ships, by name.
    #[pyfunction]
    fn schema_file_names() -> BTreeMap<&'static str, String> {
        doppeldata_core::published_schemas::all()
            .into_iter()
            .map(|schema| (schema.name, schema.file_name))
            .collect()
    }
}
