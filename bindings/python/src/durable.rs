use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use pyo3::prelude::*;
use tokio::sync::mpsc;

use crate::api::{
    PyDispatcher, PyExecutionInfo, ZartCommand, handle_command, invoke_with_dispatcher,
};
use crate::errors::ZartControlFlowSignal;
use crate::serde_bridge::{json_to_py, py_to_json};
use crate::{duration_from_secs, resolve_task_locals};

/// Description of why `on_failure` was invoked on a `DurableTask`.
#[pyclass(name = "ExecutionFailure", frozen, from_py_object)]
#[derive(Clone)]
pub struct PyExecutionFailure {
    #[pyo3(get)]
    pub kind: String,
    #[pyo3(get)]
    pub step: Option<String>,
    raw_json: Option<serde_json::Value>,
    #[pyo3(get)]
    pub attempts: Option<usize>,
}

impl PyExecutionFailure {
    fn from_rust(failure: zart::ExecutionFailure) -> Self {
        match failure {
            zart::ExecutionFailure::StepFailed { step, raw } => Self {
                kind: "step_failed".to_string(),
                step: Some(step),
                raw_json: Some(raw),
                attempts: None,
            },
            zart::ExecutionFailure::ExecutionDeadlineExceeded => Self {
                kind: "execution_deadline_exceeded".to_string(),
                step: None,
                raw_json: None,
                attempts: None,
            },
            zart::ExecutionFailure::RetriesExhausted { attempts } => Self {
                kind: "retries_exhausted".to_string(),
                step: None,
                raw_json: None,
                attempts: Some(attempts),
            },
        }
    }
}

#[pymethods]
impl PyExecutionFailure {
    #[getter]
    fn raw<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match &self.raw_json {
            Some(v) => json_to_py(py, v),
            None => Ok(py.None().into_bound(py)),
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "ExecutionFailure(kind={:?}, step={:?}, attempts={:?})",
            self.kind, self.step, self.attempts
        )
    }
}

/// A durable workflow handler registered with `WorkerBuilder.register_durable(...)`.
#[pyclass(name = "DurableTask", frozen, from_py_object)]
#[derive(Clone)]
pub struct PyDurableTask {
    #[pyo3(get)]
    pub name: String,
    pub(crate) handler: Arc<Py<PyAny>>,
    pub(crate) timeout: Option<Duration>,
    #[pyo3(get)]
    pub max_retries: usize,
    pub(crate) on_failure: Option<Arc<Py<PyAny>>>,
    pub(crate) locals: Arc<OnceLock<pyo3_async_runtimes::TaskLocals>>,
}

#[pymethods]
impl PyDurableTask {
    #[new]
    #[pyo3(signature = (name, handler, timeout_secs = None, max_retries = 0, on_failure = None))]
    fn new(
        py: Python<'_>,
        name: String,
        handler: Py<PyAny>,
        timeout_secs: Option<f64>,
        max_retries: usize,
        on_failure: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        let timeout = timeout_secs
            .map(|s| duration_from_secs(s, "timeout_secs"))
            .transpose()?;
        let locals = Arc::new(OnceLock::new());
        if let Ok(current) = pyo3_async_runtimes::tokio::get_current_locals(py) {
            let _ = locals.set(current);
        }
        Ok(Self {
            name,
            handler: Arc::new(handler),
            timeout,
            max_retries,
            on_failure: on_failure.map(Arc::new),
            locals,
        })
    }

    #[getter]
    fn timeout_secs(&self) -> Option<f64> {
        self.timeout.map(|d| d.as_secs_f64())
    }

    fn __repr__(&self) -> String {
        format!(
            "DurableTask(name={:?}, max_retries={}, timeout_secs={:?})",
            self.name,
            self.max_retries,
            self.timeout.map(|d| d.as_secs_f64()),
        )
    }
}

fn unwrap_py_task_err(
    py: Python<'_>,
    err: PyErr,
    cf_slot: &Arc<Mutex<Option<zart::StepError>>>,
) -> zart::TaskError {
    if err.is_instance_of::<ZartControlFlowSignal>(py) {
        if let Ok(mut guard) = cf_slot.lock()
            && let Some(step_err) = guard.take()
        {
            return zart::TaskError::from(step_err);
        }
        return zart::TaskError::Cancelled;
    }
    let msg = err
        .value(py)
        .str()
        .map(|s| s.to_string())
        .unwrap_or_else(|_| "Python handler failed".to_string());
    zart::TaskError::StepFailed {
        step: "handler".to_string(),
        source: zart::StepError::Failed {
            step: "handler".to_string(),
            reason: msg,
        },
    }
}

#[async_trait]
impl zart::DurableExecution for PyDurableTask {
    type Data = serde_json::Value;
    type Output = serde_json::Value;

    fn max_retries(&self) -> usize {
        self.max_retries
    }

    fn timeout(&self) -> Option<Duration> {
        self.timeout
    }

    async fn run(&self, data: Self::Data) -> Result<Self::Output, zart::TaskError> {
        let info = PyExecutionInfo::from_rust(zart::context());
        let (tx, mut rx) = mpsc::unbounded_channel::<ZartCommand>();
        let cf_slot = Arc::new(Mutex::new(None));
        let locals_lock = self.locals.clone();
        let handler = self.handler.clone();

        let dispatcher = PyDispatcher::for_body(tx, info, locals_lock.clone());

        let fut = Python::attach(|py| -> PyResult<_> {
            let locals = resolve_task_locals(py, &locals_lock)?;
            let py_data = json_to_py(py, &data)?;
            let coro = invoke_with_dispatcher(py, dispatcher, handler.bind(py), Some(py_data))?;
            pyo3_async_runtimes::into_future_with_locals(&locals, coro)
        })
        .map_err(|e| Python::attach(|py| unwrap_py_task_err(py, e, &cf_slot)))?;

        tokio::pin!(fut);

        loop {
            tokio::select! {
                res = &mut fut => {
                    return match res {
                        Ok(py_val) => Python::attach(|py| py_to_json(py_val.bind(py)))
                            .map_err(|e| Python::attach(|py| unwrap_py_task_err(py, e, &cf_slot))),
                        Err(py_err) => Err(Python::attach(|py| unwrap_py_task_err(py, py_err, &cf_slot))),
                    };
                }
                Some(cmd) = rx.recv() => {
                    handle_command(cmd, &cf_slot).await;
                }
            }
        }
    }

    async fn on_failure(
        &self,
        data: Self::Data,
        failure: zart::ExecutionFailure,
    ) -> Result<Self::Output, zart::TaskError> {
        let Some(ref cb) = self.on_failure else {
            return Err(zart::TaskError::Cancelled);
        };
        let py_failure = PyExecutionFailure::from_rust(failure);
        let locals_lock = self.locals.clone();
        let cb = cb.clone();
        let cf_slot = Arc::new(Mutex::new(None));

        let fut = Python::attach(|py| -> PyResult<_> {
            let locals = resolve_task_locals(py, &locals_lock)?;
            let py_data = json_to_py(py, &data)?;
            let py_fail = Py::new(py, py_failure)?;
            let coro = cb.bind(py).call1((py_data, py_fail))?;
            pyo3_async_runtimes::into_future_with_locals(&locals, coro)
        })
        .map_err(|e| Python::attach(|py| unwrap_py_task_err(py, e, &cf_slot)))?;

        let py_res = fut
            .await
            .map_err(|e| Python::attach(|py| unwrap_py_task_err(py, e, &cf_slot)))?;

        Python::attach(|py| py_to_json(py_res.bind(py)))
            .map_err(|e| Python::attach(|py| unwrap_py_task_err(py, e, &cf_slot)))
    }
}
