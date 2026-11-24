use std::sync::{Arc, Mutex};
use std::time::Duration;

use napi::bindgen_prelude::*;
use napi_derive::napi;

use crate::backend::PgBackend;
use crate::durable::{TsDurableTask, make_handler, make_on_failure_handler};
use crate::errors::schema_err;
use crate::step::millis;

/// Recurrence specification passed from JS to `WorkerBuilder.registerRecurringDurable`.
#[napi(object)]
#[derive(Clone)]
pub struct NativeRecurrence {
    /// `"fixed_delay"` or `"cron"`.
    pub kind: String,
    /// Delay in milliseconds for `"fixed_delay"`.
    pub duration_ms: Option<f64>,
    /// 6-field cron expression for `"cron"`.
    pub expression: Option<String>,
    /// IANA timezone name for `"cron"` (e.g. `"UTC"`, `"America/Sao_Paulo"`).
    pub timezone: Option<String>,
}

#[derive(Clone)]
struct RecurringTaskEntry {
    task: TsDurableTask,
    id_template: String,
    recurrence: zart_scheduler::Recurrence,
    overlap: zart::OverlapPolicy,
    initial_data: serde_json::Value,
}

/// Builder for a `Worker` with configuration and durable handler registration.
#[napi]
pub struct WorkerBuilder {
    backend: Arc<zart::PgBackend>,
    config: Mutex<zart::WorkerConfig>,
    tasks: Mutex<Vec<TsDurableTask>>,
    recurring_tasks: Mutex<Vec<RecurringTaskEntry>>,
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

fn parse_recurrence(spec: NativeRecurrence) -> Result<zart_scheduler::Recurrence> {
    match spec.kind.as_str() {
        "fixed_delay" => {
            let ms = spec
                .duration_ms
                .ok_or_else(|| schema_err("durationMs is required for fixed_delay recurrence"))?;
            let dur = millis(ms, "durationMs")?;
            let duration_ms = u64::try_from(dur.as_millis())
                .map_err(|_| schema_err("durationMs is out of range"))?;
            Ok(zart_scheduler::Recurrence::FixedDelay { duration_ms })
        }
        "cron" => {
            let expression = spec
                .expression
                .filter(|s| !s.trim().is_empty())
                .ok_or_else(|| schema_err("expression cannot be empty for cron recurrence"))?;
            let timezone = spec
                .timezone
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| "UTC".to_string());
            Ok(zart_scheduler::Recurrence::Cron {
                expression,
                timezone,
            })
        }
        other => Err(schema_err(format!("invalid recurrence kind: '{other}'"))),
    }
}

fn parse_overlap_policy(overlap: Option<&str>) -> Result<zart::OverlapPolicy> {
    match overlap {
        None | Some("SkipIfRunning") => Ok(zart::OverlapPolicy::SkipIfRunning),
        Some("CancelAndRestart") => Ok(zart::OverlapPolicy::CancelAndRestart),
        Some("AlwaysStart") => Ok(zart::OverlapPolicy::AlwaysStart),
        Some(other) => Err(schema_err(format!("invalid overlapPolicy: '{other}'"))),
    }
}

#[napi]
impl WorkerBuilder {
    #[napi(factory)]
    pub fn from_backend(backend: &PgBackend) -> WorkerBuilder {
        WorkerBuilder {
            backend: backend.inner.clone(),
            config: Mutex::new(zart::WorkerConfig::default()),
            tasks: Mutex::new(Vec::new()),
            recurring_tasks: Mutex::new(Vec::new()),
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

    fn make_task(
        &self,
        env: &Env,
        name: String,
        handler: &JsFunction,
        timeout_ms: Option<f64>,
        max_retries: Option<u32>,
        on_failure: Option<&JsFunction>,
    ) -> Result<TsDurableTask> {
        let timeout = timeout_ms.map(|ms| millis(ms, "timeoutMs")).transpose()?;
        let on_failure = match on_failure {
            Some(f) => Some(std::sync::Arc::new(make_on_failure_handler(env, f)?)),
            None => None,
        };
        Ok(TsDurableTask {
            name,
            handler: std::sync::Arc::new(make_handler(env, handler)?),
            timeout,
            max_retries: max_retries.unwrap_or(0) as usize,
            pool: self.backend.pool().clone(),
            on_failure,
        })
    }

    /// Register a JS durable task with handler, sub-second timeout (ms), max retries,
    /// and optional failure recovery callback: `on_failure(data, failure) => Promise<json>`.
    #[napi(
        ts_args_type = "name: string, handler: (dispatcher: NativeDispatcher, data: any) => Promise<any>, timeoutMs?: number | undefined | null, maxRetries?: number | undefined | null, onFailure?: ((data: any, failure: any) => Promise<any>) | undefined | null"
    )]
    pub fn register_durable(
        &self,
        env: Env,
        name: String,
        handler: JsFunction,
        timeout_ms: Option<f64>,
        max_retries: Option<u32>,
        on_failure: Option<JsFunction>,
    ) -> Result<()> {
        let task = self.make_task(
            &env,
            name,
            &handler,
            timeout_ms,
            max_retries,
            on_failure.as_ref(),
        )?;
        self.tasks
            .lock()
            .map_err(|_| schema_err("tasks lock poisoned"))?
            .push(task);
        Ok(())
    }

    /// Register a recurring durable task with schedule, overlap policy, and optional initial data.
    #[napi(
        ts_args_type = "name: string, handler: (dispatcher: NativeDispatcher, data: any) => Promise<any>, idTemplate: string, recurrence: NativeRecurrence, overlap?: string | undefined | null, initialData?: any, timeoutMs?: number | undefined | null, maxRetries?: number | undefined | null, onFailure?: ((data: any, failure: any) => Promise<any>) | undefined | null"
    )]
    #[allow(clippy::too_many_arguments)]
    pub fn register_recurring_durable(
        &self,
        env: Env,
        name: String,
        handler: JsFunction,
        id_template: String,
        recurrence: NativeRecurrence,
        overlap: Option<String>,
        initial_data: Option<serde_json::Value>,
        timeout_ms: Option<f64>,
        max_retries: Option<u32>,
        on_failure: Option<JsFunction>,
    ) -> Result<()> {
        if !id_template.contains("{occurrence}") {
            return Err(schema_err(format!(
                "idTemplate must contain '{{occurrence}}' placeholder, got: '{id_template}'"
            )));
        }
        let recurrence = parse_recurrence(recurrence)?;
        let overlap = parse_overlap_policy(overlap.as_deref())?;
        let task = self.make_task(
            &env,
            name,
            &handler,
            timeout_ms,
            max_retries,
            on_failure.as_ref(),
        )?;
        self.recurring_tasks
            .lock()
            .map_err(|_| schema_err("recurring_tasks lock poisoned"))?
            .push(RecurringTaskEntry {
                task,
                id_template,
                recurrence,
                overlap,
                initial_data: initial_data.unwrap_or(serde_json::Value::Null),
            });
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
        let recurring_tasks = self
            .recurring_tasks
            .lock()
            .map_err(|_| schema_err("recurring_tasks lock poisoned"))?
            .clone();
        let mut builder = zart::WorkerBuilder::from_backend(&*self.backend).config(cfg);
        for task in tasks {
            let name = task.name.clone();
            builder = builder.register_durable_task(&name, task);
        }
        for entry in recurring_tasks {
            let name = entry.task.name.clone();
            builder = builder.register_durable_task(&name, entry.task);
            builder = builder.register_recurring_durable::<TsDurableTask>(
                &name,
                &entry.id_template,
                entry.recurrence,
                entry.overlap,
                entry.initial_data,
            );
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

    #[test]
    fn parses_recurrence_and_overlap_policy() {
        assert!(matches!(
            parse_recurrence(NativeRecurrence {
                kind: "fixed_delay".into(),
                duration_ms: Some(500.0),
                expression: None,
                timezone: None,
            })
            .unwrap(),
            zart_scheduler::Recurrence::FixedDelay { duration_ms: 500 }
        ));
        assert!(matches!(
            parse_recurrence(NativeRecurrence {
                kind: "cron".into(),
                duration_ms: None,
                expression: Some("0 */5 * * * *".into()),
                timezone: None,
            })
            .unwrap(),
            zart_scheduler::Recurrence::Cron { ref timezone, .. } if timezone == "UTC"
        ));
        assert_schema_err(
            parse_recurrence(NativeRecurrence {
                kind: "fixed_delay".into(),
                duration_ms: Some(-1.0),
                expression: None,
                timezone: None,
            }),
            "durationMs",
        );
        assert_schema_err(
            parse_recurrence(NativeRecurrence {
                kind: "cron".into(),
                duration_ms: None,
                expression: Some("   ".into()),
                timezone: None,
            }),
            "expression",
        );
        assert_schema_err(
            parse_recurrence(NativeRecurrence {
                kind: "unknown".into(),
                duration_ms: None,
                expression: None,
                timezone: None,
            }),
            "invalid recurrence kind",
        );

        assert!(matches!(
            parse_overlap_policy(None).unwrap(),
            zart::OverlapPolicy::SkipIfRunning
        ));
        assert!(matches!(
            parse_overlap_policy(Some("CancelAndRestart")).unwrap(),
            zart::OverlapPolicy::CancelAndRestart
        ));
        assert!(matches!(
            parse_overlap_policy(Some("AlwaysStart")).unwrap(),
            zart::OverlapPolicy::AlwaysStart
        ));
        assert_schema_err(parse_overlap_policy(Some("BadPolicy")), "overlapPolicy");
    }
}
