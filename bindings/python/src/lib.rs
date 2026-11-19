use std::sync::OnceLock;
use std::time::Duration;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

mod api;
mod backend;
mod durable;
mod errors;
mod scheduler;
mod serde_bridge;
mod step;
mod trx;
mod worker;

use backend::PyPgBackend;
use durable::{PyDurableTask, PyExecutionFailure};
use scheduler::{PyDurableScheduler, PyExecutionRecord, PyScheduleResult};
use step::{PyRetryPolicy, PyStep};
use trx::{PyClientTrx, PyTrx};
use worker::{PyWorker, PyWorkerBuilder};

/// Convert a non-negative finite number of seconds into a `Duration`.
pub(crate) fn duration_from_secs(secs: f64, name: &str) -> PyResult<Duration> {
    if !secs.is_finite() || secs < 0.0 {
        return Err(PyValueError::new_err(format!(
            "{name} must be a non-negative finite number"
        )));
    }
    Duration::try_from_secs_f64(secs)
        .map_err(|_| PyValueError::new_err(format!("{name} is out of range for Duration")))
}

/// Resolve cached `TaskLocals` or capture the current Python asyncio loop's locals.
pub(crate) fn resolve_task_locals(
    py: Python<'_>,
    locals_lock: &OnceLock<pyo3_async_runtimes::TaskLocals>,
) -> PyResult<pyo3_async_runtimes::TaskLocals> {
    if let Some(locals) = locals_lock.get() {
        return Ok(locals.clone());
    }
    let current = pyo3_async_runtimes::tokio::get_current_locals(py)?;
    let _ = locals_lock.set(current.clone());
    Ok(current)
}

#[pymodule]
fn _zart(m: &Bound<'_, PyModule>) -> PyResult<()> {
    errors::register_exceptions(m)?;
    api::register_api(m)?;
    m.add_class::<PyPgBackend>()?;
    m.add_class::<PyDurableScheduler>()?;
    m.add_class::<PyScheduleResult>()?;
    m.add_class::<PyExecutionRecord>()?;
    m.add_class::<PyWorkerBuilder>()?;
    m.add_class::<PyWorker>()?;
    m.add_class::<PyRetryPolicy>()?;
    m.add_class::<PyStep>()?;
    m.add_class::<PyDurableTask>()?;
    m.add_class::<PyExecutionFailure>()?;
    m.add_class::<PyTrx>()?;
    m.add_class::<PyClientTrx>()?;
    m.add_function(wrap_pyfunction!(serde_bridge::_roundtrip_json, m)?)?;
    Ok(())
}
