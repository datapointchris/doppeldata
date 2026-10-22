use pyo3::prelude::*;

/// doppeldata's engine, compiled.
#[pymodule]
mod _core {
    use pyo3::exceptions::PyValueError;
    use pyo3::prelude::*;

    /// The engine's release, which is the tag the wheel was built from.
    #[pymodule_export]
    const VERSION: &str = doppeldata_core::VERSION;

    /// Reads a profile from its canonical JSON and returns ``sha256:<hex>`` over those bytes, or
    /// raises ValueError when the engine cannot read it.
    #[pyfunction]
    fn profile_digest(canonical: &[u8]) -> PyResult<String> {
        doppeldata_core::profile::digest(canonical)
            .map_err(|error| PyValueError::new_err(format!("the engine cannot read this profile: {error}")))
    }
}
