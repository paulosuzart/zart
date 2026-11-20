use std::collections::HashSet;
use std::sync::{Arc, Mutex, OnceLock};

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;

use crate::backend::PyPgBackend;
use crate::durable::PyDurableTask;
use crate::duration_from_secs;
use crate::recurring::{PyOverlapPolicy, PyRecurrence};
use crate::serde_bridge::py_to_json;

#[derive(Clone)]
struct RecurringTaskEntry {
    task: PyDurableTask,
    id_template: String,
    recurrence: zart_scheduler::Recurrence,
    overlap: zart::OverlapPolicy,
    initial_data: serde_json::Value,
}

/// Builder for configuring and constructing a Zart `Worker`.
#[pyclass(name = "WorkerBuilder")]
pub struct PyWorkerBuilder {
    backend: Arc<zart::PgBackend>,
    config: Mutex<zart::WorkerConfig>,
    durable_tasks: Mutex<Vec<PyDurableTask>>,
    recurring_tasks: Mutex<Vec<RecurringTaskEntry>>,
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
            recurring_tasks: Mutex::new(Vec::new()),
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

    /// Register a recurring `DurableTask` that fires on a `Recurrence` schedule.
    ///
    /// `id_template` must contain `{occurrence}` (e.g. `"daily-report-{occurrence}"`).
    /// Automatically registers `task` as a durable handler if not already registered.
    #[pyo3(signature = (task, id_template, recurrence, overlap = PyOverlapPolicy::SkipIfRunning, initial_data = None))]
    fn register_recurring_durable<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        task: PyDurableTask,
        id_template: String,
        recurrence: PyRecurrence,
        overlap: PyOverlapPolicy,
        initial_data: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<PyRef<'py, Self>> {
        if !id_template.contains("{occurrence}") {
            return Err(PyValueError::new_err(format!(
                "id_template must contain {{occurrence}} placeholder, got: {id_template:?}"
            )));
        }
        let payload_json = match initial_data {
            Some(obj) => {
                let serde_mod = py.import("zart.serde")?;
                let normalized = serde_mod.call_method1("to_json_value", (obj,))?;
                py_to_json(&normalized)?
            }
            None => serde_json::Value::Null,
        };
        let mut recurring = slf
            .recurring_tasks
            .lock()
            .map_err(|_| PyRuntimeError::new_err("WorkerBuilder recurring_tasks lock poisoned"))?;
        recurring.push(RecurringTaskEntry {
            task,
            id_template,
            recurrence: recurrence.inner,
            overlap: overlap.into(),
            initial_data: payload_json,
        });
        drop(recurring);
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
        let recurring = self
            .recurring_tasks
            .lock()
            .map_err(|_| PyRuntimeError::new_err("WorkerBuilder recurring_tasks lock poisoned"))?
            .clone();
        let trx_provider = self
            .trx_provider
            .lock()
            .map_err(|_| PyRuntimeError::new_err("WorkerBuilder trx_provider lock poisoned"))?
            .clone();

        let pool = self.backend.pool().clone();
        let mut builder = zart::WorkerBuilder::from_backend(&*self.backend).config(cfg);
        let mut task_locals = Vec::with_capacity(tasks.len() + recurring.len());
        let mut registered_names = HashSet::with_capacity(tasks.len() + recurring.len());

        for task in tasks {
            let _ = task.pool.set(pool.clone());
            if let Some(ref prov) = trx_provider {
                let _ = task.trx_provider.set(prov.clone());
            }
            task_locals.push(task.locals.clone());
            let name = task.name.clone();
            registered_names.insert(name.clone());
            builder = builder.register_durable_task(&name, task);
        }

        for entry in recurring {
            let name = entry.task.name.clone();
            if !registered_names.contains(&name) {
                let _ = entry.task.pool.set(pool.clone());
                if let Some(ref prov) = trx_provider {
                    let _ = entry.task.trx_provider.set(prov.clone());
                }
                task_locals.push(entry.task.locals.clone());
                registered_names.insert(name.clone());
                builder = builder.register_durable_task(&name, entry.task);
            }
            builder = builder.register_recurring_durable::<PyDurableTask>(
                &name,
                &entry.id_template,
                entry.recurrence,
                entry.overlap,
                entry.initial_data,
            );
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
