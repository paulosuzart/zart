use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use tokio::sync::{mpsc, oneshot};

use crate::duration_from_secs;
use crate::errors::{
    PyStepError, ZartControlFlowSignal, ZartDeadlineExceededError, ZartRetryExhaustedError,
    ZartStepFailedError, ZartStepTimeoutError,
};
use crate::serde_bridge::{json_to_py, py_to_json};
use crate::step::PyStep;

static DISPATCHER_VAR: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
static TRX_PROVIDER_VAR: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
static ENTRY_HELPER: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

fn create_context_var(py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
    let cv_mod = py.import("contextvars")?;
    let kwargs = pyo3::types::PyDict::new(py);
    kwargs.set_item("default", py.None())?;
    let var = cv_mod.call_method("ContextVar", (name,), Some(&kwargs))?;
    Ok(var.unbind())
}

fn get_dispatcher_var(py: Python<'_>) -> PyResult<&Bound<'_, PyAny>> {
    DISPATCHER_VAR
        .get_or_try_init(py, || create_context_var(py, "_zart_dispatcher"))
        .map(|obj| obj.bind(py))
}

fn get_trx_provider_var(py: Python<'_>) -> PyResult<&Bound<'_, PyAny>> {
    TRX_PROVIDER_VAR
        .get_or_try_init(py, || create_context_var(py, "_zart_trx_provider"))
        .map(|obj| obj.bind(py))
}

fn get_entry_helper(py: Python<'_>) -> PyResult<&Bound<'_, PyAny>> {
    ENTRY_HELPER
        .get_or_try_init(py, || -> PyResult<Py<PyAny>> {
            let globals = pyo3::types::PyDict::new(py);
            let code = c"async def _zart_entry(cv, disp, fn, has_arg, arg, pvar, prov):\n    token = cv.set(disp)\n    ptoken = pvar.set(prov) if prov is not None else None\n    try:\n        if has_arg:\n            return await fn(arg)\n        return await fn()\n    finally:\n        if ptoken is not None:\n            pvar.reset(ptoken)\n        cv.reset(token)\n";
            py.run(code, Some(&globals), None)?;
            let helper = globals
                .get_item("_zart_entry")?
                .ok_or_else(|| PyRuntimeError::new_err("missing _zart_entry helper"))?;
            Ok(helper.unbind())
        })
        .map(|obj| obj.bind(py))
}

/// Invoke a Python async callable with `disp` bound to `_zart_dispatcher` in the task's context.
pub(crate) fn invoke_with_dispatcher<'py>(
    py: Python<'py>,
    disp: PyDispatcher,
    callable: &Bound<'py, PyAny>,
    arg: Option<Bound<'py, PyAny>>,
    trx_provider: Option<&Py<PyAny>>,
) -> PyResult<Bound<'py, PyAny>> {
    let cv = get_dispatcher_var(py)?;
    let pvar = get_trx_provider_var(py)?;
    let helper = get_entry_helper(py)?;
    let disp_py = Py::new(py, disp)?;
    let prov_py = match trx_provider {
        Some(p) => p.bind(py).clone(),
        None => py.None().into_bound(py),
    };
    match arg {
        Some(a) => helper.call1((cv, disp_py, callable, true, a, pvar, prov_py)),
        None => helper.call1((cv, disp_py, callable, false, py.None(), pvar, prov_py)),
    }
}

fn current_dispatcher(py: Python<'_>) -> PyResult<Py<PyDispatcher>> {
    let cv = get_dispatcher_var(py)?;
    let val = cv.call_method0("get")?;
    if val.is_none() {
        return Err(PyRuntimeError::new_err(
            "zart.* functions must be called inside a @durable workflow or @step execution",
        ));
    }
    Ok(val.extract::<Py<PyDispatcher>>()?)
}

/// Read-only view of the current execution metadata.
#[pyclass(name = "ExecutionInfo", frozen, from_py_object)]
#[derive(Clone)]
pub struct PyExecutionInfo {
    #[pyo3(get)]
    pub execution_id: String,
    #[pyo3(get)]
    pub task_name: String,
    pub data: Arc<serde_json::Value>,
    #[pyo3(get)]
    pub current_attempt: usize,
    #[pyo3(get)]
    pub max_retries: Option<usize>,
}

impl PyExecutionInfo {
    pub(crate) fn from_rust(info: zart::ExecutionInfo) -> Self {
        Self {
            execution_id: info.execution_id,
            task_name: info.task_name,
            data: Arc::new(info.data),
            current_attempt: info.current_attempt,
            max_retries: info.max_retries,
        }
    }
}

#[pymethods]
impl PyExecutionInfo {
    #[getter]
    fn data<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        json_to_py(py, &self.data)
    }

    /// Returns `True` if this is a retry attempt (`current_attempt > 0`).
    fn is_retry(&self) -> bool {
        self.current_attempt > 0
    }

    fn __repr__(&self) -> String {
        format!(
            "ExecutionInfo(execution_id={:?}, task_name={:?}, current_attempt={})",
            self.execution_id, self.task_name, self.current_attempt
        )
    }
}

/// Three-way outcome of a step execution (`ok`, `business_err`, or `zart_err`).
#[pyclass(name = "StepOutcome", frozen, from_py_object)]
#[derive(Clone)]
pub struct PyStepOutcome {
    #[pyo3(get)]
    pub kind: String,
    value_json: Option<serde_json::Value>,
    error_json: Option<serde_json::Value>,
    error_message: Option<String>,
}

impl PyStepOutcome {
    pub(crate) fn ok(val: serde_json::Value) -> Self {
        Self {
            kind: "ok".to_string(),
            value_json: Some(val),
            error_json: None,
            error_message: None,
        }
    }

    pub(crate) fn business_err(err: PyStepError) -> Self {
        let msg = err.to_string();
        let err_json = serde_json::to_value(&err).unwrap_or(serde_json::Value::Null);
        Self {
            kind: "business_err".to_string(),
            value_json: None,
            error_json: Some(err_json),
            error_message: Some(msg),
        }
    }

    pub(crate) fn zart_err(err: zart::ZartStepError) -> Self {
        let msg = err.to_string();
        let err_json = match &err {
            zart::ZartStepError::RetryExhausted {
                step,
                attempts,
                last_error,
            } => serde_json::json!({
                "kind": "retry_exhausted",
                "step": step,
                "attempts": attempts,
                "last_error": last_error,
            }),
            zart::ZartStepError::TimedOut { step, duration } => serde_json::json!({
                "kind": "timed_out",
                "step": step,
                "duration_secs": duration.as_secs_f64(),
            }),
            zart::ZartStepError::DeadlineExceeded { step } => serde_json::json!({
                "kind": "deadline_exceeded",
                "step": step,
            }),
        };
        Self {
            kind: "zart_err".to_string(),
            value_json: None,
            error_json: Some(err_json),
            error_message: Some(msg),
        }
    }

    pub(crate) fn from_step_error(err: &zart::StepError) -> Self {
        let msg = err.to_string();
        let err_json = serde_json::json!({
            "kind": "step_error",
            "message": msg,
        });
        Self {
            kind: "zart_err".to_string(),
            value_json: None,
            error_json: Some(err_json),
            error_message: Some(msg),
        }
    }
}

#[pymethods]
impl PyStepOutcome {
    #[getter]
    fn is_ok(&self) -> bool {
        self.kind == "ok"
    }

    #[getter]
    fn is_business_err(&self) -> bool {
        self.kind == "business_err"
    }

    #[getter]
    fn is_zart_err(&self) -> bool {
        self.kind == "zart_err"
    }

    #[getter]
    fn value<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match &self.value_json {
            Some(v) => json_to_py(py, v),
            None => Ok(py.None().into_bound(py)),
        }
    }

    #[getter]
    fn error<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match &self.error_json {
            Some(e) => json_to_py(py, e),
            None => Ok(py.None().into_bound(py)),
        }
    }

    /// Return the value if `is_ok`, or raise `ZartStepFailedError`.
    fn unwrap<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        if let Some(ref val) = self.value_json {
            json_to_py(py, val)
        } else {
            Err(ZartStepFailedError::new_err(
                self.error_message
                    .clone()
                    .unwrap_or_else(|| "Step failed".to_string()),
            ))
        }
    }

    fn __repr__(&self) -> String {
        format!("StepOutcome(kind={:?})", self.kind)
    }
}

/// Handle to a step scheduled for parallel execution via `schedule()`.
#[pyclass(name = "StepHandle", frozen, from_py_object)]
#[derive(Clone)]
pub struct PyStepHandle {
    #[pyo3(get)]
    pub step_name: String,
    pub(crate) inner: Arc<Mutex<Option<zart::StepHandle<serde_json::Value>>>>,
}

#[pymethods]
impl PyStepHandle {
    fn __repr__(&self) -> String {
        format!("StepHandle(step_name={:?})", self.step_name)
    }
}

pub(crate) enum ZartCommand {
    Require {
        step: PyStep,
        reply: oneshot::Sender<PyResult<serde_json::Value>>,
    },
    Step {
        step: PyStep,
        reply: oneshot::Sender<PyResult<PyStepOutcome>>,
    },
    StepOr {
        step: PyStep,
        default: serde_json::Value,
        reply: oneshot::Sender<PyResult<serde_json::Value>>,
    },
    Schedule {
        step: PyStep,
        reply: oneshot::Sender<PyResult<PyStepHandle>>,
    },
    Wait {
        handles: Vec<PyStepHandle>,
        reply: oneshot::Sender<PyResult<Vec<PyStepOutcome>>>,
    },
    Sleep {
        name: String,
        duration: Duration,
        reply: oneshot::Sender<PyResult<()>>,
    },
    SleepUntil {
        name: String,
        wake_time: chrono::DateTime<chrono::Utc>,
        reply: oneshot::Sender<PyResult<()>>,
    },
    WaitForEvent {
        name: String,
        timeout: Option<Duration>,
        reply: oneshot::Sender<PyResult<serde_json::Value>>,
    },
    Capture {
        name: String,
        func: Py<PyAny>,
        reply: oneshot::Sender<PyResult<serde_json::Value>>,
    },
    Now {
        name: String,
        reply: oneshot::Sender<PyResult<String>>,
    },
}

#[pyclass(name = "_Dispatcher", frozen)]
pub(crate) struct PyDispatcher {
    tx: Option<mpsc::UnboundedSender<ZartCommand>>,
    info: PyExecutionInfo,
    locals: Arc<OnceLock<pyo3_async_runtimes::TaskLocals>>,
    pool: Option<sqlx::PgPool>,
    trx_provider: Option<Arc<Py<PyAny>>>,
}

impl PyDispatcher {
    pub(crate) fn for_body(
        tx: mpsc::UnboundedSender<ZartCommand>,
        info: PyExecutionInfo,
        locals: Arc<OnceLock<pyo3_async_runtimes::TaskLocals>>,
        pool: Option<sqlx::PgPool>,
        trx_provider: Option<Arc<Py<PyAny>>>,
    ) -> Self {
        Self {
            tx: Some(tx),
            info,
            locals,
            pool,
            trx_provider,
        }
    }

    pub(crate) fn for_step(info: PyExecutionInfo) -> Self {
        Self {
            tx: None,
            info,
            locals: Arc::new(OnceLock::new()),
            pool: None,
            trx_provider: None,
        }
    }

    fn sender(&self) -> PyResult<&mpsc::UnboundedSender<ZartCommand>> {
        self.tx.as_ref().ok_or_else(|| {
            PyRuntimeError::new_err(
                "zart scheduling functions (require, step, schedule, sleep, wait, ...) \
                 cannot be called from within a step body",
            )
        })
    }

    fn attach_locals(&self, step: &PyStep) {
        if let Some(l) = self.locals.get() {
            let _ = step.locals.set(l.clone());
        }
        if let Some(ref p) = self.pool {
            let _ = step.pool.set(p.clone());
        }
        if let Some(ref prov) = self.trx_provider {
            let _ = step.trx_provider.set(prov.clone());
        }
    }
}

pub(crate) fn step_err_to_py(
    err: zart::StepError,
    cf_slot: &Arc<Mutex<Option<zart::StepError>>>,
) -> PyErr {
    match err {
        e @ (zart::StepError::Scheduled { .. } | zart::StepError::StepExecuted { .. }) => {
            let msg = e.to_string();
            if let Ok(mut guard) = cf_slot.lock() {
                *guard = Some(e);
            }
            ZartControlFlowSignal::new_err(msg)
        }
        zart::StepError::Timeout { .. } => ZartStepTimeoutError::new_err(err.to_string()),
        zart::StepError::DeadlineExceeded { .. } => {
            ZartDeadlineExceededError::new_err(err.to_string())
        }
        zart::StepError::RetryExhausted { .. } => ZartRetryExhaustedError::new_err(err.to_string()),
        other => ZartStepFailedError::new_err(other.to_string()),
    }
}

pub(crate) fn task_err_to_py(
    err: zart::TaskError,
    cf_slot: &Arc<Mutex<Option<zart::StepError>>>,
) -> PyErr {
    match err {
        zart::TaskError::StepFailed { source, .. } => step_err_to_py(source, cf_slot),
        other => ZartStepFailedError::new_err(other.to_string()),
    }
}

/// Execute a `ZartCommand` directly inside the Tokio task where `ZART_CTX` & `ZART_PHASE` are set.
pub(crate) async fn handle_command(
    cmd: ZartCommand,
    cf_slot: &Arc<Mutex<Option<zart::StepError>>>,
) {
    match cmd {
        ZartCommand::Require { step, reply } => {
            let res = zart::require(step)
                .await
                .map_err(|e| task_err_to_py(e, cf_slot));
            let _ = reply.send(res);
        }
        ZartCommand::Step { step, reply } => {
            let res = match zart::step(step).await {
                Ok(zart::StepOutcome::Ok(v)) => Ok(PyStepOutcome::ok(v)),
                Ok(zart::StepOutcome::BusinessErr(e)) => Ok(PyStepOutcome::business_err(e)),
                Ok(zart::StepOutcome::ZartErr(e)) => Ok(PyStepOutcome::zart_err(e)),
                Err(e) => Err(step_err_to_py(e, cf_slot)),
            };
            let _ = reply.send(res);
        }
        ZartCommand::StepOr {
            step,
            default,
            reply,
        } => {
            let res = zart::step_or(step, default)
                .await
                .map_err(|e| step_err_to_py(e, cf_slot));
            let _ = reply.send(res);
        }
        ZartCommand::Schedule { step, reply } => {
            let step_name = zart::ZartStep::step_name(&step).into_owned();
            let handle = zart::schedule(step);
            let _ = reply.send(Ok(PyStepHandle {
                step_name,
                inner: Arc::new(Mutex::new(Some(handle))),
            }));
        }
        ZartCommand::Wait { handles, reply } => {
            let mut rust_handles = Vec::with_capacity(handles.len());
            for h in handles {
                let taken = h
                    .inner
                    .lock()
                    .ok()
                    .and_then(|mut g| g.take());
                match taken {
                    Some(rh) => rust_handles.push(rh),
                    None => {
                        let _ = reply.send(Err(PyRuntimeError::new_err(format!(
                            "StepHandle '{}' was already awaited",
                            h.step_name
                        ))));
                        return;
                    }
                }
            }
            let res = match zart::wait(rust_handles).await {
                Ok(results) => Ok(results
                    .into_iter()
                    .map(|r| match r {
                        Ok(v) => PyStepOutcome::ok(v),
                        Err(e) => PyStepOutcome::from_step_error(&e),
                    })
                    .collect()),
                Err(e) => Err(step_err_to_py(e, cf_slot)),
            };
            let _ = reply.send(res);
        }
        ZartCommand::Sleep {
            name,
            duration,
            reply,
        } => {
            let res = zart::sleep(&name, duration)
                .await
                .map_err(|e| step_err_to_py(e, cf_slot));
            let _ = reply.send(res);
        }
        ZartCommand::SleepUntil {
            name,
            wake_time,
            reply,
        } => {
            let res = zart::sleep_until(&name, wake_time)
                .await
                .map_err(|e| step_err_to_py(e, cf_slot));
            let _ = reply.send(res);
        }
        ZartCommand::WaitForEvent {
            name,
            timeout,
            reply,
        } => {
            let res = zart::wait_for_event(&name, timeout)
                .await
                .map_err(|e| step_err_to_py(e, cf_slot));
            let _ = reply.send(res);
        }
        ZartCommand::Capture { name, func, reply } => {
            let mut py_err: Option<PyErr> = None;
            let res = zart::capture(&name, || {
                Python::attach(|py| match func.bind(py).call0() {
                    Ok(v) => match py_to_json(&v) {
                        Ok(j) => Some(j),
                        Err(e) => {
                            py_err = Some(e);
                            None
                        }
                    },
                    Err(e) => {
                        py_err = Some(e);
                        None
                    }
                })
            })
            .await;
            if let Some(err) = py_err {
                let _ = reply.send(Err(err));
                return;
            }
            let out = match res {
                Ok(Some(v)) => Ok(v),
                Ok(None) => Err(PyRuntimeError::new_err("capture callback failed")),
                Err(e) => Err(step_err_to_py(e, cf_slot)),
            };
            let _ = reply.send(out);
        }
        ZartCommand::Now { name, reply } => {
            let res = zart::now(&name)
                .await
                .map(|dt| dt.to_rfc3339())
                .map_err(|e| step_err_to_py(e, cf_slot));
            let _ = reply.send(res);
        }
    }
}

fn dispatch_raw<T>(
    py: Python<'_>,
    step: Option<&PyStep>,
    build_cmd: impl FnOnce(oneshot::Sender<PyResult<T>>) -> ZartCommand,
) -> PyResult<oneshot::Receiver<PyResult<T>>> {
    let disp = current_dispatcher(py)?;
    let disp_ref = disp.bind(py).borrow();
    if let Some(s) = step {
        disp_ref.attach_locals(s);
    }
    let tx = disp_ref.sender()?.clone();
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(build_cmd(reply_tx))
        .map_err(|_| PyRuntimeError::new_err("Zart execution dispatcher closed"))?;
    Ok(reply_rx)
}

fn dispatch_async<'py, T>(
    py: Python<'py>,
    step: Option<&PyStep>,
    build_cmd: impl FnOnce(oneshot::Sender<PyResult<T>>) -> ZartCommand,
) -> PyResult<Bound<'py, PyAny>>
where
    T: Send + 'static,
    for<'a> T: pyo3::IntoPyObject<'a>,
    for<'a> <T as pyo3::IntoPyObject<'a>>::Output: pyo3::IntoPyObject<'a>,
    for<'a> <T as pyo3::IntoPyObject<'a>>::Error: Into<PyErr>,
{
    let reply_rx = dispatch_raw(py, step, build_cmd)?;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        reply_rx
            .await
            .map_err(|_| PyRuntimeError::new_err("Zart execution dispatcher dropped reply"))?
    })
}

fn dispatch_async_json<'py>(
    py: Python<'py>,
    step: Option<&PyStep>,
    build_cmd: impl FnOnce(oneshot::Sender<PyResult<serde_json::Value>>) -> ZartCommand,
) -> PyResult<Bound<'py, PyAny>> {
    let reply_rx = dispatch_raw(py, step, build_cmd)?;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let val = reply_rx
            .await
            .map_err(|_| PyRuntimeError::new_err("Zart execution dispatcher dropped reply"))??;
        Python::attach(|py| Ok(json_to_py(py, &val)?.unbind()))
    })
}

/// Execute a step that must succeed; raises `ZartStepFailedError` on failure.
#[pyfunction]
fn require<'py>(py: Python<'py>, step: PyStep) -> PyResult<Bound<'py, PyAny>> {
    dispatch_async_json(py, Some(&step.clone()), |reply| ZartCommand::Require {
        step,
        reply,
    })
}

/// Execute a step and return its three-way `StepOutcome`.
#[pyfunction]
fn step<'py>(py: Python<'py>, step: PyStep) -> PyResult<Bound<'py, PyAny>> {
    dispatch_async(py, Some(&step.clone()), |reply| ZartCommand::Step {
        step,
        reply,
    })
}

/// Execute a step, returning `default` on any failure.
#[pyfunction]
fn step_or<'py>(
    py: Python<'py>,
    step: PyStep,
    default: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let default_json = py_to_json(default)?;
    dispatch_async_json(py, Some(&step.clone()), |reply| ZartCommand::StepOr {
        step,
        default: default_json,
        reply,
    })
}

/// Execute a step, returning `StepOutcome` so Python can invoke a fallback on business error.
#[pyfunction]
fn step_or_else<'py>(
    py: Python<'py>,
    step: PyStep,
    fallback: Py<PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let reply_rx = dispatch_raw(py, Some(&step.clone()), |reply| ZartCommand::Step {
        step,
        reply,
    })?;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let outcome = reply_rx
            .await
            .map_err(|_| PyRuntimeError::new_err("Zart execution dispatcher dropped reply"))??;
        Python::attach(|py| match outcome.kind.as_str() {
            "ok" => Ok(outcome.value(py)?.unbind()),
            "business_err" => {
                let err_val = outcome.error(py)?;
                let res = fallback.bind(py).call1((err_val,))?;
                Ok(res.unbind())
            }
            _ => Err(ZartStepFailedError::new_err(
                outcome
                    .error_message
                    .unwrap_or_else(|| "Step failed".to_string()),
            )),
        })
    })
}

/// Schedule a step for parallel execution without awaiting it yet.
#[pyfunction]
fn schedule(py: Python<'_>, step: PyStep) -> PyResult<PyStepHandle> {
    let reply_rx = dispatch_raw(py, Some(&step.clone()), |reply| ZartCommand::Schedule {
        step,
        reply,
    })?;
    reply_rx
        .blocking_recv()
        .map_err(|_| PyRuntimeError::new_err("Zart execution dispatcher dropped reply"))?
}

/// Wait for all scheduled `StepHandle`s to complete, preserving order.
#[pyfunction]
fn wait<'py>(py: Python<'py>, handles: Vec<PyStepHandle>) -> PyResult<Bound<'py, PyAny>> {
    dispatch_async(py, None, |reply| ZartCommand::Wait { handles, reply })
}

/// Suspend execution durably for `duration_secs` seconds.
#[pyfunction]
fn sleep<'py>(py: Python<'py>, name: String, duration_secs: f64) -> PyResult<Bound<'py, PyAny>> {
    let duration = duration_from_secs(duration_secs, "duration_secs")?;
    dispatch_async(py, None, |reply| ZartCommand::Sleep {
        name,
        duration,
        reply,
    })
}

/// Suspend execution durably until the given ISO-8601 UTC timestamp.
#[pyfunction]
fn sleep_until<'py>(
    py: Python<'py>,
    name: String,
    wake_time_iso: String,
) -> PyResult<Bound<'py, PyAny>> {
    let wake_time = chrono::DateTime::parse_from_rfc3339(&wake_time_iso)
        .map_err(|e| PyValueError::new_err(format!("Invalid ISO-8601 timestamp: {e}")))?
        .with_timezone(&chrono::Utc);
    dispatch_async(py, None, |reply| ZartCommand::SleepUntil {
        name,
        wake_time,
        reply,
    })
}

/// Wait for an external event to be delivered to this execution.
#[pyfunction]
#[pyo3(signature = (name, timeout_secs = None))]
fn wait_for_event<'py>(
    py: Python<'py>,
    name: String,
    timeout_secs: Option<f64>,
) -> PyResult<Bound<'py, PyAny>> {
    let timeout = timeout_secs
        .map(|s| duration_from_secs(s, "timeout_secs"))
        .transpose()?;
    dispatch_async_json(py, None, |reply| ZartCommand::WaitForEvent {
        name,
        timeout,
        reply,
    })
}

/// Capture a synchronous pure computation durably.
#[pyfunction]
fn capture<'py>(py: Python<'py>, name: String, func: Py<PyAny>) -> PyResult<Bound<'py, PyAny>> {
    dispatch_async_json(py, None, |reply| ZartCommand::Capture {
        name,
        func,
        reply,
    })
}

/// Capture the current UTC timestamp durably (returns ISO-8601 string).
#[pyfunction]
fn now<'py>(py: Python<'py>, name: String) -> PyResult<Bound<'py, PyAny>> {
    dispatch_async(py, None, |reply| ZartCommand::Now { name, reply })
}

/// Return read-only `ExecutionInfo` for the current durable execution or step.
#[pyfunction]
fn context(py: Python<'_>) -> PyResult<PyExecutionInfo> {
    let disp = current_dispatcher(py)?;
    let info = disp.bind(py).borrow().info.clone();
    Ok(info)
}

/// Return the worker-level default transaction provider (if configured via `WorkerBuilder.trx_provider`).
#[pyfunction]
fn _get_worker_trx_provider(py: Python<'_>) -> PyResult<Py<PyAny>> {
    let pvar = get_trx_provider_var(py)?;
    let val = pvar.call_method0("get")?;
    Ok(val.unbind())
}

/// Register Phase 2 API classes and functions on `_zart`.
pub fn register_api(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyExecutionInfo>()?;
    m.add_class::<PyStepOutcome>()?;
    m.add_class::<PyStepHandle>()?;
    m.add_function(wrap_pyfunction!(require, m)?)?;
    m.add_function(wrap_pyfunction!(step, m)?)?;
    m.add_function(wrap_pyfunction!(step_or, m)?)?;
    m.add_function(wrap_pyfunction!(step_or_else, m)?)?;
    m.add_function(wrap_pyfunction!(schedule, m)?)?;
    m.add_function(wrap_pyfunction!(wait, m)?)?;
    m.add_function(wrap_pyfunction!(sleep, m)?)?;
    m.add_function(wrap_pyfunction!(sleep_until, m)?)?;
    m.add_function(wrap_pyfunction!(wait_for_event, m)?)?;
    m.add_function(wrap_pyfunction!(capture, m)?)?;
    m.add_function(wrap_pyfunction!(now, m)?)?;
    m.add_function(wrap_pyfunction!(context, m)?)?;
    m.add_function(wrap_pyfunction!(_get_worker_trx_provider, m)?)?;
    Ok(())
}
