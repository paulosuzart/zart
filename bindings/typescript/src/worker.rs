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

fn validate_min_one(n: u32, name: &str) -> Result<usize> {
    if n == 0 {
        return Err(schema_err(format!("{name} must be at least 1")));
    }
    Ok(n as usize)
}

fn heartbeat_dur(secs: Option<f64>) -> Result<Option<Duration>> {
    secs.map(|s| dur(s, "heartbeatIntervalSecs")).transpose()
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

    /// Set the fallback polling interval in seconds.
    ///
    /// Rejects negative or non-finite values with `ZartSchemaError`.
    #[napi]
    pub fn poll_interval_secs(&self, secs: f64) -> Result<()> {
        let d = dur(secs, "pollIntervalSecs")?;
        self.with_cfg(|c| c.poll_interval = d)
    }

    /// Set the maximum number of concurrent tasks (must be >= 1; otherwise `ZartSchemaError`).
    #[napi]
    pub fn max_concurrent_tasks(&self, n: u32) -> Result<()> {
        let v = validate_min_one(n, "maxConcurrentTasks")?;
        self.with_cfg(|c| c.max_concurrent_tasks = v)
    }

    /// Set the maximum number of tasks fetched per poll cycle (must be >= 1; otherwise `ZartSchemaError`).
    #[napi]
    pub fn max_tasks_per_poll(&self, n: u32) -> Result<()> {
        let v = validate_min_one(n, "maxTasksPerPoll")?;
        self.with_cfg(|c| c.max_tasks_per_poll = v)
    }

    /// Set the graceful shutdown timeout in seconds.
    ///
    /// Rejects negative or non-finite values with `ZartSchemaError`.
    #[napi]
    pub fn shutdown_timeout_secs(&self, secs: f64) -> Result<()> {
        let d = dur(secs, "shutdownTimeoutSecs")?;
        self.with_cfg(|c| c.shutdown_timeout = d)
    }

    /// Set the orphan recovery timeout in seconds.
    ///
    /// Rejects negative or non-finite values with `ZartSchemaError`.
    #[napi]
    pub fn orphan_timeout_secs(&self, secs: f64) -> Result<()> {
        let d = dur(secs, "orphanTimeoutSecs")?;
        self.with_cfg(|c| c.orphan_timeout = d)
    }

    /// Set the lease heartbeat interval in seconds.
    ///
    /// Pass `None` (`null`/`undefined`) to restore the default (`orphanTimeout / 3`), or `0` to
    /// disable heartbeating. Rejects negative or non-finite values with `ZartSchemaError`.
    #[napi]
    pub fn heartbeat_interval_secs(&self, secs: Option<f64>) -> Result<()> {
        let d = heartbeat_dur(secs)?;
        self.with_cfg(|c| c.heartbeat_interval = d)
    }

    /// Enable or disable PostgreSQL `LISTEN/NOTIFY` wakeup hints.
    #[napi]
    pub fn enable_listen_notify(&self, enabled: bool) -> Result<()> {
        self.with_cfg(|c| c.enable_listen_notify = enabled)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_schema_err<T: std::fmt::Debug>(res: Result<T>, needle: &str) {
        let err = res.expect_err("expected ZartSchemaError");
        let v: serde_json::Value = serde_json::from_str(&err.reason).unwrap();
        assert_eq!(v["_tag"], "ZartSchemaError");
        assert!(
            v["message"].as_str().unwrap().contains(needle),
            "expected '{}' to contain '{}'",
            v["message"],
            needle
        );
    }

    #[test]
    fn validates_zero_concurrency_and_poll_limits() {
        assert_schema_err(
            validate_min_one(0, "maxConcurrentTasks"),
            "maxConcurrentTasks must be at least 1",
        );
        assert_schema_err(
            validate_min_one(0, "maxTasksPerPoll"),
            "maxTasksPerPoll must be at least 1",
        );
        assert_eq!(validate_min_one(12, "maxConcurrentTasks").unwrap(), 12);
        assert_eq!(validate_min_one(4, "maxTasksPerPoll").unwrap(), 4);
    }

    #[test]
    fn validates_durations_and_heartbeat_option() {
        assert_eq!(
            dur(120.0, "orphanTimeoutSecs").unwrap(),
            Duration::from_secs(120)
        );
        assert_eq!(
            heartbeat_dur(Some(20.0)).unwrap(),
            Some(Duration::from_secs(20))
        );
        assert_eq!(heartbeat_dur(Some(0.0)).unwrap(), Some(Duration::ZERO));
        assert_eq!(heartbeat_dur(None).unwrap(), None);

        assert_schema_err(dur(-1.0, "orphanTimeoutSecs"), "orphanTimeoutSecs");
        assert_schema_err(dur(f64::NAN, "orphanTimeoutSecs"), "orphanTimeoutSecs");
        assert_schema_err(heartbeat_dur(Some(-0.5)), "heartbeatIntervalSecs");
        assert_schema_err(heartbeat_dur(Some(f64::INFINITY)), "heartbeatIntervalSecs");
    }
}
