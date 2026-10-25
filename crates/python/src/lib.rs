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
}
