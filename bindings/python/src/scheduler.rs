use std::sync::Arc;

use pyo3::exceptions::{PyLookupError, PyRuntimeError, PyTimeoutError, PyValueError};
use pyo3::prelude::*;

use crate::backend::PyPgBackend;
use crate::duration_from_secs;
use crate::serde_bridge::{json_to_py, py_to_json};

fn scheduler_err_to_py(err: zart::SchedulerError) -> PyErr {
    match err {
        zart::SchedulerError::WaitTimedOut(msg) => PyTimeoutError::new_err(msg),
        zart::SchedulerError::ExecutionNotFound(msg) => PyLookupError::new_err(msg),
        zart::SchedulerError::Serialization(e) => PyValueError::new_err(e.to_string()),
        zart::SchedulerError::Deserialization(msg) => PyValueError::new_err(msg),
        other => PyRuntimeError::new_err(other.to_string()),
    }
}

/// Result returned when scheduling a durable execution.
#[pyclass(name = "ScheduleResult", get_all, frozen)]
pub struct PyScheduleResult {
    pub task_id: String,
    pub execution_time: String,
}

#[pymethods]
impl PyScheduleResult {
    fn __repr__(&self) -> String {
        format!(
            "ScheduleResult(task_id={:?}, execution_time={:?})",
            self.task_id, self.execution_time
        )
    }
}

/// Snapshot of a durable execution record.
#[pyclass(name = "ExecutionRecord", get_all, frozen)]
pub struct PyExecutionRecord {
    pub execution_id: String,
    pub task_name: String,
    pub payload: Py<PyAny>,
    pub status: String,
    pub result: Option<Py<PyAny>>,
    pub scheduled_at: String,
    pub completed_at: Option<String>,
    pub version: i32,
}

impl PyExecutionRecord {
    fn from_rust(record: zart::ExecutionRecord) -> PyResult<Self> {
        Python::attach(|py| {
            let payload = json_to_py(py, &record.payload)?.unbind();
            let result = match record.result {
                Some(ref val) => Some(json_to_py(py, val)?.unbind()),
                None => None,
            };
            Ok(Self {
                execution_id: record.execution_id,
                task_name: record.task_name,
                payload,
                status: record.status.to_string(),
                result,
                scheduled_at: record.scheduled_at.to_rfc3339(),
                completed_at: record.completed_at.map(|dt| dt.to_rfc3339()),
                version: record.version,
            })
        })
    }
}

#[pymethods]
impl PyExecutionRecord {
    fn __repr__(&self) -> String {
        format!(
            "ExecutionRecord(execution_id={:?}, task_name={:?}, status={:?})",
            self.execution_id, self.task_name, self.status
        )
    }
}

/// Control-plane client for starting, querying, and awaiting durable executions.
#[pyclass(name = "DurableScheduler", frozen)]
pub struct PyDurableScheduler {
    inner: Arc<zart::DurableScheduler>,
}

#[pymethods]
impl PyDurableScheduler {
    /// Create a `DurableScheduler` from a `PgBackend`.
    #[new]
    fn new(backend: &PyPgBackend) -> Self {
        let sched = zart::DurableScheduler::from_backend(&*backend.inner);
        Self {
            inner: Arc::new(sched),
        }
    }

    /// Start a new durable execution with a JSON-compatible payload.
    fn start<'py>(
        &self,
        py: Python<'py>,
        execution_id: String,
        task_name: String,
        payload: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let payload_json = py_to_json(payload)?;
        let sched = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let res = sched
                .start(&execution_id, &task_name, payload_json)
                .await
                .map_err(scheduler_err_to_py)?;
            Ok(PyScheduleResult {
                task_id: res.task_id,
                execution_time: res.execution_time.to_rfc3339(),
            })
        })
    }

    /// Fetch the current status and metadata of a durable execution.
    fn status<'py>(&self, py: Python<'py>, execution_id: String) -> PyResult<Bound<'py, PyAny>> {
        let sched = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let record = sched
                .status(&execution_id)
                .await
                .map_err(scheduler_err_to_py)?;
            PyExecutionRecord::from_rust(record)
        })
    }

    /// Wait until the execution reaches a terminal state and return its `ExecutionRecord`.
    #[pyo3(signature = (execution_id, timeout_secs = 30.0, poll_interval_secs = None))]
    fn wait<'py>(
        &self,
        py: Python<'py>,
        execution_id: String,
        timeout_secs: f64,
        poll_interval_secs: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let timeout = duration_from_secs(timeout_secs, "timeout_secs")?;
        let poll_interval = poll_interval_secs
            .map(|s| duration_from_secs(s, "poll_interval_secs"))
            .transpose()?;
        let sched = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let record = sched
                .wait(&execution_id, timeout, poll_interval)
                .await
                .map_err(scheduler_err_to_py)?;
            PyExecutionRecord::from_rust(record)
        })
    }

    /// Wait until the execution completes and return its deserialized JSON result.
    #[pyo3(signature = (execution_id, timeout_secs = 30.0, poll_interval_secs = None))]
    fn wait_completion<'py>(
        &self,
        py: Python<'py>,
        execution_id: String,
        timeout_secs: f64,
        poll_interval_secs: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let timeout = duration_from_secs(timeout_secs, "timeout_secs")?;
        let poll_interval = poll_interval_secs
            .map(|s| duration_from_secs(s, "poll_interval_secs"))
            .transpose()?;
        let sched = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let value: serde_json::Value = sched
                .wait_completion(&execution_id, timeout, poll_interval)
                .await
                .map_err(scheduler_err_to_py)?;
            Python::attach(|py| Ok(json_to_py(py, &value)?.unbind()))
        })
    }

    /// Cancel a running or scheduled durable execution.
    fn cancel<'py>(&self, py: Python<'py>, execution_id: String) -> PyResult<Bound<'py, PyAny>> {
        let sched = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            sched
                .cancel(&execution_id)
                .await
                .map_err(scheduler_err_to_py)
        })
    }

    /// Deliver an external event payload to a waiting durable execution.
    fn offer_event<'py>(
        &self,
        py: Python<'py>,
        execution_id: String,
        event_name: String,
        payload: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let payload_json = py_to_json(payload)?;
        let sched = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            sched
                .offer_event(&execution_id, &event_name, payload_json)
                .await
                .map_err(scheduler_err_to_py)
        })
    }
}
