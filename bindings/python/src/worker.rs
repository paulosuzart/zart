use std::sync::{Arc, Mutex};

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;

use crate::backend::PyPgBackend;
use crate::duration_from_secs;

/// Builder for configuring and constructing a Zart `Worker`.
#[pyclass(name = "WorkerBuilder")]
pub struct PyWorkerBuilder {
    backend: Arc<zart::PgBackend>,
    config: Mutex<zart::WorkerConfig>,
}

#[pymethods]
impl PyWorkerBuilder {
    /// Create a `WorkerBuilder` backed by a `PgBackend`.
    #[new]
    fn new(backend: &PyPgBackend) -> Self {
        Self {
            backend: backend.inner.clone(),
            config: Mutex::new(zart::WorkerConfig::default()),
        }
    }

    /// Set the fallback polling interval in seconds.
    fn poll_interval<'py>(
        slf: PyRef<'py, Self>,
        secs: f64,
    ) -> PyResult<PyRef<'py, Self>> {
        let dur = duration_from_secs(secs, "secs")?;
        let mut cfg = slf
            .config
            .lock()
            .map_err(|_| PyRuntimeError::new_err("WorkerBuilder config lock poisoned"))?;
        cfg.poll_interval = dur;
        drop(cfg);
        Ok(slf)
    }

    /// Set the maximum number of concurrent tasks executed by the worker.
    fn max_concurrent_tasks<'py>(
        slf: PyRef<'py, Self>,
        max_tasks: usize,
    ) -> PyResult<PyRef<'py, Self>> {
        if max_tasks == 0 {
            return Err(PyValueError::new_err(
                "max_concurrent_tasks must be at least 1",
            ));
        }
        let mut cfg = slf
            .config
            .lock()
            .map_err(|_| PyRuntimeError::new_err("WorkerBuilder config lock poisoned"))?;
        cfg.max_concurrent_tasks = max_tasks;
        drop(cfg);
        Ok(slf)
    }

    /// Set the maximum number of tasks fetched per poll cycle.
    fn max_tasks_per_poll<'py>(
        slf: PyRef<'py, Self>,
        max_tasks: usize,
    ) -> PyResult<PyRef<'py, Self>> {
        if max_tasks == 0 {
            return Err(PyValueError::new_err(
                "max_tasks_per_poll must be at least 1",
            ));
        }
        let mut cfg = slf
            .config
            .lock()
            .map_err(|_| PyRuntimeError::new_err("WorkerBuilder config lock poisoned"))?;
        cfg.max_tasks_per_poll = max_tasks;
        drop(cfg);
        Ok(slf)
    }

    /// Set the graceful shutdown timeout in seconds.
    fn shutdown_timeout<'py>(
        slf: PyRef<'py, Self>,
        secs: f64,
    ) -> PyResult<PyRef<'py, Self>> {
        let dur = duration_from_secs(secs, "secs")?;
        let mut cfg = slf
            .config
            .lock()
            .map_err(|_| PyRuntimeError::new_err("WorkerBuilder config lock poisoned"))?;
        cfg.shutdown_timeout = dur;
        drop(cfg);
        Ok(slf)
    }

    /// Set the orphan recovery timeout in seconds.
    fn orphan_timeout<'py>(
        slf: PyRef<'py, Self>,
        secs: f64,
    ) -> PyResult<PyRef<'py, Self>> {
        let dur = duration_from_secs(secs, "secs")?;
        let mut cfg = slf
            .config
            .lock()
            .map_err(|_| PyRuntimeError::new_err("WorkerBuilder config lock poisoned"))?;
        cfg.orphan_timeout = dur;
        drop(cfg);
        Ok(slf)
    }

    /// Enable or disable PostgreSQL `LISTEN/NOTIFY` wakeup hints.
    fn enable_listen_notify<'py>(
        slf: PyRef<'py, Self>,
        enabled: bool,
    ) -> PyResult<PyRef<'py, Self>> {
        let mut cfg = slf
            .config
            .lock()
            .map_err(|_| PyRuntimeError::new_err("WorkerBuilder config lock poisoned"))?;
        cfg.enable_listen_notify = enabled;
        drop(cfg);
        Ok(slf)
    }

    /// Build the configured `Worker`.
    fn build(&self) -> PyResult<PyWorker> {
        let cfg = self
            .config
            .lock()
            .map_err(|_| PyRuntimeError::new_err("WorkerBuilder config lock poisoned"))?
            .clone();
        let worker = zart::WorkerBuilder::from_backend(&*self.backend)
            .config(cfg)
            .build();
        Ok(PyWorker {
            inner: Arc::new(worker),
        })
    }
}

/// Polls the task queue and dispatches tasks to registered handlers.
#[pyclass(name = "Worker", frozen)]
pub struct PyWorker {
    inner: Arc<zart::Worker>,
}

#[pymethods]
impl PyWorker {
    /// Start the worker polling loop until `stop()` is called.
    fn run<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let worker = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            worker.run().await;
            Ok(())
        })
    }

    /// Signal the worker to shut down gracefully.
    fn stop(&self) {
        self.inner.stop();
    }
}
