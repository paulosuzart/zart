use std::sync::{Arc, Mutex};
use std::time::Duration;

use napi::bindgen_prelude::*;
use napi_derive::napi;

use crate::backend::PgBackend;
use crate::durable::{TsDurableTask, make_handler};
use crate::errors::schema_err;
use crate::step::millis;

/// Builder for a `Worker` with configuration and durable handler registration.
#[napi]
pub struct WorkerBuilder {
    backend: Arc<zart::PgBackend>,
    config: Mutex<zart::WorkerConfig>,
    tasks: Mutex<Vec<TsDurableTask>>,
}

fn dur(s: f64, name: &str) -> Result<Duration> {
    Duration::try_from_secs_f64(s).map_err(|e| schema_err(format!("invalid {name}: {e}")))
}

#[napi]
impl WorkerBuilder {
    #[napi(factory)]
    pub fn from_backend(backend: &PgBackend) -> WorkerBuilder {
        WorkerBuilder {
            backend: backend.inner.clone(),
            config: Mutex::new(zart::WorkerConfig::default()),
            tasks: Mutex::new(Vec::new()),
        }
    }

    fn with_cfg(&self, f: impl FnOnce(&mut zart::WorkerConfig)) -> Result<()> {
        let mut g = self
            .config
            .lock()
            .map_err(|_| schema_err("config lock poisoned"))?;
        f(&mut g);
        Ok(())
    }

    #[napi]
    pub fn poll_interval_secs(&self, secs: f64) -> Result<()> {
        let d = dur(secs, "pollIntervalSecs")?;
        self.with_cfg(|c| c.poll_interval = d)
    }

    #[napi]
    pub fn max_concurrent_tasks(&self, n: u32) -> Result<()> {
        self.with_cfg(|c| c.max_concurrent_tasks = n as usize)
    }

    #[napi]
    pub fn max_tasks_per_poll(&self, n: u32) -> Result<()> {
        self.with_cfg(|c| c.max_tasks_per_poll = n as usize)
    }

    #[napi]
    pub fn shutdown_timeout_secs(&self, secs: f64) -> Result<()> {
        let d = dur(secs, "shutdownTimeoutSecs")?;
        self.with_cfg(|c| c.shutdown_timeout = d)
    }

    /// Register a JS durable handler: `handler(dispatcher, data) => Promise<json>`.
    #[napi(
        ts_args_type = "name: string, handler: (dispatcher: NativeDispatcher, data: any) => Promise<any>, timeoutSecs?: number | undefined | null, maxRetries?: number | undefined | null"
    )]
    pub fn register_durable(
        &self,
        env: Env,
        name: String,
        handler: JsFunction,
        timeout_secs: Option<f64>,
        max_retries: Option<u32>,
    ) -> Result<()> {
        let timeout = timeout_secs
            .map(|s| millis(s * 1000.0, "timeoutSecs"))
            .transpose()?;
        let task = TsDurableTask {
            name,
            handler: std::sync::Arc::new(make_handler(&env, &handler)?),
            timeout,
            max_retries: max_retries.unwrap_or(0) as usize,
            pool: self.backend.pool().clone(),
        };
        self.tasks
            .lock()
            .map_err(|_| schema_err("tasks lock poisoned"))?
            .push(task);
        Ok(())
    }

    #[napi]
    pub fn build(&self) -> Result<Worker> {
        let cfg = self
            .config
            .lock()
            .map_err(|_| schema_err("config lock poisoned"))?
            .clone();
        let tasks = self
            .tasks
            .lock()
            .map_err(|_| schema_err("tasks lock poisoned"))?
            .clone();
        let mut builder = zart::WorkerBuilder::from_backend(&*self.backend).config(cfg);
        for task in tasks {
            let name = task.name.clone();
            builder = builder.register_durable_task(&name, task);
        }
        let worker = builder.build();
        Ok(Worker {
            inner: Arc::new(worker),
        })
    }
}

#[napi]
pub struct Worker {
    inner: Arc<zart::Worker>,
}

#[napi]
impl Worker {
    /// Run the polling loop until `stop()` is called.
    #[napi]
    pub async fn run(&self) -> Result<()> {
        self.inner.run().await;
        Ok(())
    }

    #[napi]
    pub fn stop(&self) {
        self.inner.stop();
    }
}
