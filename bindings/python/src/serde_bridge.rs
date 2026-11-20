//! Bidirectional conversion helpers between Python objects and `serde_json::Value`.

use pyo3::prelude::*;
use pythonize::{depythonize, pythonize};

/// Convert a Python object (`dict`, `list`, primitives) into a `serde_json::Value`.
pub fn py_to_json(obj: &Bound<'_, PyAny>) -> PyResult<serde_json::Value> {
    depythonize(obj).map_err(Into::into)
}

/// Convert a `serde_json::Value` into a Python object (`dict`, `list`, primitives).
pub fn json_to_py<'py>(py: Python<'py>, val: &serde_json::Value) -> PyResult<Bound<'py, PyAny>> {
    pythonize(py, val).map_err(Into::into)
}

/// Round-trip a Python object through `serde_json::Value` via `pythonize` (for FFI testing).
#[pyfunction]
pub fn _roundtrip_json<'py>(
    py: Python<'py>,
    obj: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let val = py_to_json(obj)?;
    json_to_py(py, &val)
}
