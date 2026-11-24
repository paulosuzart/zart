use std::borrow::Cow;
use std::future::Future;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::api::{PyDispatcher, PyExecutionInfo, invoke_with_dispatcher};
use crate::errors::PyStepError;
use crate::serde_bridge::py_to_json;
use crate::trx::PyTrx;
use crate::{duration_from_secs, resolve_task_locals};

/// Retry policy configuration for steps and durable tasks.
#[pyclass(name = "RetryPolicy", frozen, from_py_object)]
#[derive(Clone, Debug)]
pub struct PyRetryPolicy {
    pub(crate) inner: zart::RetryConfig,
}

#[pymethods]
impl PyRetryPolicy {
    /// No retries — the first failure is terminal.
    #[staticmethod]
    fn none() -> Self {
        Self {
            inner: zart::RetryConfig::none(),
        }
    }

    /// Retry `attempts` times with a fixed `delay_secs` between each attempt.
    #[staticmethod]
    fn fixed(attempts: usize, delay_secs: f64) -> PyResult<Self> {
        let delay = duration_from_secs(delay_secs, "delay_secs")?;
        Ok(Self {
            inner: zart::RetryConfig::fixed(attempts, delay),
        })
    }

    /// Retry `attempts` times with exponential backoff starting at `initial_delay_secs`.
    #[staticmethod]
    #[pyo3(signature = (attempts, initial_delay_secs, max_delay_secs = None, multiplier = 2.0))]
    fn exponential(
        attempts: usize,
        initial_delay_secs: f64,
        max_delay_secs: Option<f64>,
        multiplier: f64,
    ) -> PyResult<Self> {
        if !multiplier.is_finite() || multiplier < 1.0 {
            return Err(PyValueError::new_err(
                "multiplier must be a finite number >= 1.0",
            ));
        }
        let initial_delay = duration_from_secs(initial_delay_secs, "initial_delay_secs")?;
        let max_delay = max_delay_secs
            .map(|s| duration_from_secs(s, "max_delay_secs"))
            .transpose()?;
        Ok(Self {
            inner: zart::RetryConfig {
                max_attempts: attempts,
                initial_delay,
                backoff_multiplier: multiplier,
                max_delay,
            },
        })
    }

    /// Retry `attempts` times with linear (constant-interval) backoff.
    #[staticmethod]
    #[pyo3(signature = (attempts, initial_delay_secs, max_delay_secs = None))]
    fn linear(
        attempts: usize,
        initial_delay_secs: f64,
        max_delay_secs: Option<f64>,
    ) -> PyResult<Self> {
        let initial_delay = duration_from_secs(initial_delay_secs, "initial_delay_secs")?;
        let max_delay = max_delay_secs
            .map(|s| duration_from_secs(s, "max_delay_secs"))
            .transpose()?;
        Ok(Self {
            inner: zart::RetryConfig {
                max_attempts: attempts,
                initial_delay,
                backoff_multiplier: 1.0,
                max_delay,
            },
        })
    }

    #[getter]
    fn max_attempts(&self) -> usize {
        self.inner.max_attempts
    }

    #[getter]
    fn initial_delay_secs(&self) -> f64 {
        self.inner.initial_delay.as_secs_f64()
    }

    #[getter]
    fn backoff_multiplier(&self) -> f64 {
        self.inner.backoff_multiplier
    }

    #[getter]
    fn max_delay_secs(&self) -> Option<f64> {
        self.inner.max_delay.map(|d| d.as_secs_f64())
    }

    fn __repr__(&self) -> String {
        format!(
            "RetryPolicy(max_attempts={}, initial_delay_secs={}, backoff_multiplier={}, max_delay_secs={:?})",
            self.inner.max_attempts,
            self.inner.initial_delay.as_secs_f64(),
            self.inner.backoff_multiplier,
            self.inner.max_delay.map(|d| d.as_secs_f64()),
        )
    }
}

pub(crate) fn parse_timeout_scope(scope: Option<&str>) -> PyResult<zart::TimeoutScope> {
    match scope {
        None | Some("global") => Ok(zart::TimeoutScope::Global),
        Some("per_attempt") => Ok(zart::TimeoutScope::PerAttempt),
        Some(other) => Err(PyValueError::new_err(format!(
            "invalid timeout_scope: '{other}'. Expected 'global' or 'per_attempt'"
        ))),
    }
}

/// A step invocation specification created by `@step` and executed via `require`/`step`/`schedule`.
#[pyclass(name = "StepSpec", frozen, from_py_object)]
#[derive(Clone)]
pub struct PyStep {
    pub(crate) name: String,
    pub(crate) callable: Arc<Py<PyAny>>,
    pub(crate) retry: Option<zart::RetryConfig>,
    pub(crate) timeout: Option<Duration>,
    pub(crate) timeout_scope: zart::TimeoutScope,
    pub(crate) inject_trx: bool,
    pub(crate) locals: Arc<OnceLock<pyo3_async_runtimes::TaskLocals>>,
    pub(crate) pool: Arc<OnceLock<sqlx::PgPool>>,
    pub(crate) trx_provider: Arc<OnceLock<Arc<Py<PyAny>>>>,
}

#[pymethods]
impl PyStep {
    #[new]
    #[pyo3(signature = (name, callable, retry = None, timeout_secs = None, timeout_scope = None, inject_trx = false))]
    fn new(
        py: Python<'_>,
        name: String,
        callable: Py<PyAny>,
        retry: Option<PyRetryPolicy>,
        timeout_secs: Option<f64>,
        timeout_scope: Option<String>,
        inject_trx: bool,
    ) -> PyResult<Self> {
        let timeout = timeout_secs
            .map(|s| duration_from_secs(s, "timeout_secs"))
            .transpose()?;
        let scope = parse_timeout_scope(timeout_scope.as_deref())?;
        let locals = Arc::new(OnceLock::new());
        if let Ok(current) = pyo3_async_runtimes::tokio::get_current_locals(py) {
            let _ = locals.set(current);
        }
        Ok(Self {
            name,
            callable: Arc::new(callable),
            retry: retry.map(|r| r.inner),
            timeout,
            timeout_scope: scope,
            inject_trx,
            locals,
            pool: Arc::new(OnceLock::new()),
            trx_provider: Arc::new(OnceLock::new()),
        })
    }

    /// Return a copy of this step with an overridden tracking identity.
    fn named(&self, id: String) -> Self {
        Self {
            name: id,
            callable: self.callable.clone(),
            retry: self.retry.clone(),
            timeout: self.timeout,
            timeout_scope: self.timeout_scope,
            inject_trx: self.inject_trx,
            locals: self.locals.clone(),
            pool: self.pool.clone(),
            trx_provider: self.trx_provider.clone(),
        }
    }

    #[getter]
    fn name(&self) -> &str {
        &self.name
    }

    fn __repr__(&self) -> String {
        format!("StepSpec(name={:?})", self.name)
    }
}

impl zart::ZartStep for PyStep {
    type Output = serde_json::Value;
    type Error = PyStepError;

    fn step_name(&self) -> Cow<'static, str> {
        Cow::Owned(self.name.clone())
    }

    fn retry_config(&self) -> Option<zart::RetryConfig> {
        self.retry.clone()
    }

    fn timeout(&self) -> Option<Duration> {
        self.timeout
    }

    fn timeout_scope(&self) -> zart::TimeoutScope {
        self.timeout_scope
    }

    fn run(&self) -> impl Future<Output = Result<Self::Output, Self::Error>> + Send {
        let callable = self.callable.clone();
        let locals_lock = self.locals.clone();
        let inject_trx = self.inject_trx;
        let pool_lock = self.pool.clone();
        let trx_provider_lock = self.trx_provider.clone();
        let info = PyExecutionInfo::from_rust(zart::context());

        async move {
            let mut trx_arc = None;
            let py_trx = if inject_trx {
                let pool = pool_lock.get().ok_or_else(|| PyStepError {
                    error_type: "RuntimeError".to_string(),
                    message: "No PgPool available for zart.trx()".to_string(),
                    data: None,
                })?;
                let zart_trx = zart::trx(pool).await.map_err(|e| PyStepError {
                    error_type: "RuntimeError".to_string(),
                    message: e.to_string(),
                    data: None,
                })?;
                let wrapper = PyTrx::new(zart_trx);
                trx_arc = Some(wrapper.inner.clone());
                Some(wrapper)
            } else {
                None
            };

            let dispatcher = PyDispatcher::for_step(info);
            let prov_ref = trx_provider_lock.get().map(|a| a.as_ref());
            let fut = Python::attach(|py| -> PyResult<_> {
                let locals = resolve_task_locals(py, &locals_lock)?;
                let arg = match py_trx {
                    Some(t) => Some(Py::new(py, t)?.into_bound(py).into_any()),
                    None => None,
                };
                let coro =
                    invoke_with_dispatcher(py, dispatcher, callable.bind(py), arg, prov_ref)?;
                pyo3_async_runtimes::into_future_with_locals(&locals, coro)
            })
            .map_err(|e| Python::attach(|py| PyStepError::from_py_err(py, e)));

            let fut = match fut {
                Ok(f) => f,
                Err(e) => {
                    if let Some(arc) = trx_arc {
                        let _ = arc.lock().await.take();
                    }
                    return Err(e);
                }
            };

            let py_res = fut.await;

            // Deterministically drop ZartTrx before returning to Zart's ZartStepCompletion.
            if let Some(arc) = trx_arc {
                let _ = arc.lock().await.take();
            }

            let py_res =
                py_res.map_err(|e| Python::attach(|py| PyStepError::from_py_err(py, e)))?;

            Python::attach(|py| py_to_json(py_res.bind(py)))
                .map_err(|e| Python::attach(|py| PyStepError::from_py_err(py, e)))
        }
    }
}
