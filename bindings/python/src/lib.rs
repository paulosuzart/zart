use std::time::Duration;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

mod backend;
mod scheduler;
mod serde_bridge;
mod worker;

use backend::PyPgBackend;
use scheduler::{PyDurableScheduler, PyExecutionRecord, PyScheduleResult};
use worker::{PyWorker, PyWorkerBuilder};

/// Convert a non-negative finite number of seconds into a `Duration`.
pub(crate) fn duration_from_secs(secs: f64, name: &str) -> PyResult<Duration> {
    if !secs.is_finite() || secs < 0.0 {
        return Err(PyValueError::new_err(format!(
            "{name} must be a non-negative finite number"
        )));
    }
    Ok(Duration::from_secs_f64(secs))
}

#[pymodule]
fn _zart(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyPgBackend>()?;
    m.add_class::<PyDurableScheduler>()?;
    m.add_class::<PyScheduleResult>()?;
    m.add_class::<PyExecutionRecord>()?;
    m.add_class::<PyWorkerBuilder>()?;
    m.add_class::<PyWorker>()?;
    m.add_function(wrap_pyfunction!(serde_bridge::_roundtrip_json, m)?)?;
    Ok(())
}
