use std::sync::{Arc, Mutex, OnceLock};

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;

use crate::backend::PyPgBackend;
use crate::durable::PyDurableTask;
use crate::duration_from_secs;

/// Builder for configuring and constructing a Zart `Worker`.
#[pyclass(name = "WorkerBuilder")]
pub struct PyWorkerBuilder {
    backend: Arc<zart::PgBackend>,
    config: Mutex<zart::WorkerConfig>,
    durable_tasks: Mutex<Vec<PyDurableTask>>,
    trx_provider: Mutex<Option<Arc<Py<PyAny>>>>,
}

impl PyWorkerBuilder {
    /// Acquire the config mutex, apply `f`, and return `slf` for fluent chaining.
    fn update_config<'py>(
        slf: PyRef<'py, Self>,
        f: impl FnOnce(&mut zart::WorkerConfig),
    ) -> PyResult<PyRef<'py, Self>> {
        let mut cfg = slf
            .config
            .lock()
            .map_err(|_| PyRuntimeError::new_err("WorkerBuilder config lock poisoned"))?;
        f(&mut cfg);
        drop(cfg);
        Ok(slf)
    }
}

#[pymethods]
impl PyWorkerBuilder {
    /// Create a `WorkerBuilder` backed by a `PgBackend`.
    #[new]
    fn new(backend: &PyPgBackend) -> Self {
        Self {
            backend: backend.inner.clone(),
            config: Mutex::new(zart::WorkerConfig::default()),
            durable_tasks: Mutex::new(Vec::new()),
            trx_provider: Mutex::new(None),
        }
    }

    /// Register a worker-wide default transaction/session provider for `zart.trx()`.
    fn trx_provider<'py>(slf: PyRef<'py, Self>, provider: Py<PyAny>) -> PyResult<PyRef<'py, Self>> {
        let mut guard = slf
            .trx_provider
            .lock()
            .map_err(|_| PyRuntimeError::new_err("WorkerBuilder trx_provider lock poisoned"))?;
        *guard = Some(Arc::new(provider));
        drop(guard);
        Ok(slf)
    }

    /// Register a `DurableTask` handler on this worker.
    fn register_durable<'py>(
        slf: PyRef<'py, Self>,
        task: PyDurableTask,
    ) -> PyResult<PyRef<'py, Self>> {
        let mut tasks = slf
            .durable_tasks
            .lock()
            .map_err(|_| PyRuntimeError::new_err("WorkerBuilder tasks lock poisoned"))?;
        tasks.push(task);
        drop(tasks);
        Ok(slf)
    }

    /// Set the fallback polling interval in seconds.
    fn poll_interval<'py>(slf: PyRef<'py, Self>, secs: f64) -> PyResult<PyRef<'py, Self>> {
        let dur = duration_from_secs(secs, "secs")?;
        Self::update_config(slf, |cfg| cfg.poll_interval = dur)
    }

    /// Set the maximum number of concurrent tasks executed by the worker (must be at least 1).
    fn max_concurrent_tasks<'py>(
        slf: PyRef<'py, Self>,
        max_tasks: usize,
    ) -> PyResult<PyRef<'py, Self>> {
        if max_tasks == 0 {
            return Err(PyValueError::new_err(
                "max_concurrent_tasks must be at least 1",
            ));
        }
        Self::update_config(slf, |cfg| cfg.max_concurrent_tasks = max_tasks)
    }

    /// Set the maximum number of tasks fetched per poll cycle (must be at least 1).
    fn max_tasks_per_poll<'py>(
        slf: PyRef<'py, Self>,
        max_tasks: usize,
    ) -> PyResult<PyRef<'py, Self>> {
        if max_tasks == 0 {
            return Err(PyValueError::new_err(
                "max_tasks_per_poll must be at least 1",
            ));
        }
        Self::update_config(slf, |cfg| cfg.max_tasks_per_poll = max_tasks)
    }

    /// Set the graceful shutdown timeout in seconds.
    fn shutdown_timeout<'py>(slf: PyRef<'py, Self>, secs: f64) -> PyResult<PyRef<'py, Self>> {
        let dur = duration_from_secs(secs, "secs")?;
        Self::update_config(slf, |cfg| cfg.shutdown_timeout = dur)
    }

    /// Set the orphan recovery timeout in seconds.
    fn orphan_timeout<'py>(slf: PyRef<'py, Self>, secs: f64) -> PyResult<PyRef<'py, Self>> {
        let dur = duration_from_secs(secs, "secs")?;
        Self::update_config(slf, |cfg| cfg.orphan_timeout = dur)
    }

    /// Set the task lease heartbeat interval in seconds.
    ///
    /// Pass `None` to use the default (`orphan_timeout / 3`), `0.0` to disable
    /// heartbeating, or a positive number of seconds to set an explicit interval.
    fn heartbeat_interval<'py>(
        slf: PyRef<'py, Self>,
        secs: Option<f64>,
    ) -> PyResult<PyRef<'py, Self>> {
        let interval = secs.map(|s| duration_from_secs(s, "secs")).transpose()?;
        Self::update_config(slf, |cfg| cfg.heartbeat_interval = interval)
    }

    /// Enable or disable PostgreSQL `LISTEN/NOTIFY` wakeup hints.
    fn enable_listen_notify<'py>(
        slf: PyRef<'py, Self>,
        enabled: bool,
    ) -> PyResult<PyRef<'py, Self>> {
        Self::update_config(slf, |cfg| cfg.enable_listen_notify = enabled)
    }

    /// Build the configured `Worker`.
    fn build(&self) -> PyResult<PyWorker> {
        let cfg = self
            .config
            .lock()
            .map_err(|_| PyRuntimeError::new_err("WorkerBuilder config lock poisoned"))?
            .clone();
        let tasks = self
            .durable_tasks
            .lock()
            .map_err(|_| PyRuntimeError::new_err("WorkerBuilder tasks lock poisoned"))?
            .clone();
        let trx_provider = self
            .trx_provider
            .lock()
            .map_err(|_| PyRuntimeError::new_err("WorkerBuilder trx_provider lock poisoned"))?
            .clone();

        let pool = self.backend.pool().clone();
        let mut builder = zart::WorkerBuilder::from_backend(&*self.backend).config(cfg);
        let mut task_locals = Vec::with_capacity(tasks.len());
        for task in tasks {
            let _ = task.pool.set(pool.clone());
            if let Some(ref prov) = trx_provider {
                let _ = task.trx_provider.set(prov.clone());
            }
            task_locals.push(task.locals.clone());
            let name = task.name.clone();
            builder = builder.register_durable_task(&name, task);
        }

        let worker = builder.build();
        Ok(PyWorker {
            inner: Arc::new(worker),
            task_locals,
        })
    }
}

/// Polls the task queue and dispatches tasks to registered handlers.
#[pyclass(name = "Worker", frozen)]
pub struct PyWorker {
    inner: Arc<zart::Worker>,
    task_locals: Vec<Arc<OnceLock<pyo3_async_runtimes::TaskLocals>>>,
}

#[pymethods]
impl PyWorker {
    /// Start the worker polling loop until `stop()` is called.
    fn run<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        if let Ok(locals) = pyo3_async_runtimes::tokio::get_current_locals(py) {
            for slot in &self.task_locals {
                let _ = slot.set(locals.clone());
            }
        }
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
