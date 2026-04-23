//! PyO3 bindings exposing the Rust API to Python as `kithairon._native`.

use pyo3::prelude::*;

#[pymodule(gil_used = false)]
fn _native(_m: &Bound<'_, PyModule>) -> PyResult<()> {
    Ok(())
}
