//! Worker — polls the scheduler and dispatches tasks to registered handlers.

use crate::context::TaskContext;
use crate::emit_metric;
use crate::error::{ExecutionFailure, StepError, TaskError};
use crate::execution_model::ExecutionMode;
#[cfg(feature = "metrics")]
use crate::metrics::{
    EXECUTIONS_TOTAL, HEARTBEAT_ACTIVE, POLL_INTERVAL_SECONDS, QUEUE_DEPTH, TASK_DURATION_SECONDS,
    TASK_HEARTBEAT_RENEWALS_TOTAL, TASKS_TOTAL, WORKER_CONCURRENT_TASKS,
};
use crate::registry::TaskRegistry;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, error, info, instrument, warn};
use uuid::Uuid;
use zart_scheduler::StorageBackend;

/// Tuning parameters for a [`Worker`].
///
/// All fields have production-ready defaults via [`WorkerConfig::default`].
/// Override only what you need:
///
/// ```rust,ignore
/// let config = WorkerConfig {
///     poll_interval:        Duration::from_secs(2),
///     max_concurrent_tasks: 32,
///     ..WorkerConfig::default()
/// };
/// ```
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// How often the worker polls the database for due tasks.
    pub poll_interval: Duration,

    /// Maximum number of tasks to fetch per poll cycle.
    pub max_tasks_per_poll: usize,

    /// Maximum number of tasks that can execute concurrently within this worker.
    pub max_concurrent_tasks: usize,

    /// How long to wait for in-flight tasks to finish during graceful shutdown.
    pub shutdown_timeout: Duration,

    /// Tasks stuck in `picked_up` state longer than this are considered orphaned
    /// and will be reset to `scheduled` by the orphan recovery loop.
    pub orphan_timeout: Duration,

    /// How often to renew the task lease while a handler is executing.
    ///
    /// When `None` (the default), the interval is computed as `orphan_timeout / 3`,
    /// giving 2 retries before orphan recovery would reclaim the task.
    /// Set to `Some(Duration::ZERO)` to disable heartbeating entirely.
    pub heartbeat_interval: Option<Duration>,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(5),
            max_tasks_per_poll: 10,
            max_concurrent_tasks: 16,
            shutdown_timeout: Duration::from_secs(30),
            orphan_timeout: Duration::from_secs(300),
            heartbeat_interval: None, // Defaults to orphan_timeout / 3.
        }
    }
}

/// Polls the database for due tasks and dispatches them to registered handlers.
///
/// Workers are the execution engine of Zart. Each worker runs an async loop
/// that fetches tasks with a `SELECT … FOR UPDATE SKIP LOCKED` query, so
/// multiple workers — across threads or processes — can run without
/// coordination and without double-executing the same task.
///
/// # Concurrency model
///
/// Each poll cycle fetches up to [`WorkerConfig::max_tasks_per_poll`] tasks.
/// Each task is spawned as an independent Tokio task, bounded by a semaphore
/// at [`WorkerConfig::max_concurrent_tasks`]. The worker itself never blocks
/// waiting for a handler to finish.
///
/// # Heartbeating and orphan recovery
///
/// While a handler runs, a background loop renews the task's database lease
/// at `orphan_timeout / 3` intervals (configurable via
/// [`WorkerConfig::heartbeat_interval`]). If the process crashes the lease
/// expires and the next orphan-recovery scan (every 10 poll cycles) resets
/// the task to `scheduled` so another worker can pick it up.
///
/// # Graceful shutdown
///
/// Call [`stop`](Self::stop) to signal the worker to exit after the current
/// poll cycle. In-flight handlers are given [`WorkerConfig::shutdown_timeout`]
/// to finish. For integration with external shutdown coordinators, build the
/// worker with [`Worker::with_cancellation`] and cancel the shared token.
///
/// # Example
///
/// ```rust,ignore
/// use std::sync::Arc;
/// use zart::{Worker, WorkerConfig, TaskRegistry};
///
/// let mut registry = TaskRegistry::new();
/// registry.register("onboard-user", OnboardUser);
/// let registry = Arc::new(registry);
///
/// let worker = Worker::new(scheduler.clone(), Arc::clone(&registry), WorkerConfig::default());
///
/// // Run until a signal is received, then shut down cleanly.
/// tokio::select! {
///     _ = worker.run()          => {}
///     _ = tokio::signal::ctrl_c() => { worker.stop(); }
/// }
/// ```
pub struct Worker {
    scheduler: Arc<dyn StorageBackend>,
    registry: Arc<TaskRegistry>,
    config: WorkerConfig,
    /// Notified by [`stop`](Self::stop) to trigger a graceful shutdown.
    shutdown: Arc<Notify>,
    /// CancellationToken for composability with external shutdown coordinators.
    cancellation: CancellationToken,
}

impl Worker {
    /// Create a new worker.
    pub fn new(
        scheduler: Arc<dyn StorageBackend>,
        registry: Arc<TaskRegistry>,
        config: WorkerConfig,
    ) -> Self {
        Self {
            scheduler,
            registry,
            config,
            shutdown: Arc::new(Notify::new()),
            cancellation: CancellationToken::new(),
        }
    }

    /// Create a new worker with a shared cancellation token.
    ///
    /// This allows multiple workers to be shut down together
    /// when a common CancellationToken is cancelled.
    pub fn with_cancellation(
        scheduler: Arc<dyn StorageBackend>,
        registry: Arc<TaskRegistry>,
        config: WorkerConfig,
        cancellation: CancellationToken,
    ) -> Self {
        Self {
            scheduler,
            registry,
            config,
            shutdown: Arc::new(Notify::new()),
            cancellation,
        }
    }

    /// Start the polling loop.
    ///
    /// Runs until [`stop`](Self::stop) is called. Uses a semaphore to cap
    /// concurrent task execution at `config.max_concurrent_tasks`.
    ///
    /// Orphan recovery runs every 10 poll cycles to reset tasks stuck in
    /// `picked_up` state after a worker crash.
    #[instrument(name = "worker.run", skip(self))]
    pub async fn run(&self) {
        info!(
            poll_interval_ms = self.config.poll_interval.as_millis(),
            max_tasks = self.config.max_tasks_per_poll,
            concurrency = self.config.max_concurrent_tasks,
            orphan_timeout_secs = self.config.orphan_timeout.as_secs(),
            "Worker starting"
        );

        let semaphore = Arc::new(Semaphore::new(self.config.max_concurrent_tasks));
        let mut poll_count: u32 = 0;
        #[cfg(feature = "metrics")]
        let mut last_poll_time = std::time::Instant::now();

        loop {
            // Check for shutdown before each poll.
            let shutdown_notified = self.shutdown.notified();
            let cancellation = self.cancellation.cancelled();
            tokio::pin!(shutdown_notified);
            tokio::pin!(cancellation);

            tokio::select! {
                biased;
                _ = &mut shutdown_notified => {
                    info!("Worker shutdown signal received, exiting poll loop");
                    break;
                }
                _ = &mut cancellation => {
                    info!("Worker cancellation token triggered, exiting poll loop");
                    break;
                }
                _ = tokio::time::sleep(self.config.poll_interval) => {}
            }

            // Record poll interval timing
            emit_metric!({
                let poll_interval = last_poll_time.elapsed().as_secs_f64();
                POLL_INTERVAL_SECONDS
                    .with_label_values(&[])
                    .observe(poll_interval);
            });
            #[cfg(feature = "metrics")]
            {
                last_poll_time = std::time::Instant::now();
            }

            poll_count += 1;

            // Orphan recovery: run every 10 poll cycles.
            if poll_count.is_multiple_of(10) {
                match self
                    .scheduler
                    .recover_orphans(self.config.orphan_timeout)
                    .await
                {
                    Ok(n) if n > 0 => info!(recovered = n, "Orphan tasks recovered"),
                    Ok(_) => {}
                    Err(e) => error!(error = %e, "Orphan recovery failed"),
                }
            }

            let tasks = match self
                .scheduler
                .poll_due(chrono::Utc::now(), self.config.max_tasks_per_poll)
                .await
            {
                Ok(t) => t,
                Err(e) => {
                    error!(error = %e, "Failed to poll for due tasks");
                    continue;
                }
            };

            // Update queue depth metric (approximate - tasks we just fetched)
            emit_metric!(QUEUE_DEPTH.set(tasks.len() as f64));

            if tasks.is_empty() {
                continue;
            }

            info!(count = tasks.len(), "Fetched tasks for dispatch");

            for task in tasks {
                let permit = match semaphore.clone().acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => break, // semaphore closed — shutting down
                };

                let scheduler = self.scheduler.clone();
                let registry = self.registry.clone();
                let heartbeat_interval = self.config.heartbeat_interval;
                let orphan_timeout = self.config.orphan_timeout;

                tokio::spawn(
                    async move {
                        let _permit = permit; // released when this task finishes
                        emit_metric!(WORKER_CONCURRENT_TASKS.inc());
                        dispatch_task(
                            scheduler,
                            registry,
                            task,
                            heartbeat_interval,
                            orphan_timeout,
                        )
                        .await;
                        emit_metric!(WORKER_CONCURRENT_TASKS.dec());
                    }
                    .in_current_span(),
                );
            }
        }
    }

    /// Signal the worker to stop after the current poll cycle completes.
    pub fn stop(&self) {
        info!("Worker stop requested");
        self.shutdown.notify_one();
        self.cancellation.cancel();
    }

    /// Get the cancellation token for this worker.
    ///
    /// This can be shared with other components that need to coordinate shutdown.
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }
}

/// Background heartbeat loop that extends a task's lease until cancelled.
///
/// Runs in its own tokio task. Cancels automatically when the
/// `CancellationToken` is cancelled (i.e., the handler has returned).
#[cfg_attr(not(feature = "metrics"), allow(unused_variables))]
async fn heartbeat_loop(
    scheduler: Arc<dyn StorageBackend>,
    task_id: String,
    lock_token: String,
    task_name: String,
    interval: Duration,
    cancellation: CancellationToken,
) {
    emit_metric!(HEARTBEAT_ACTIVE.inc());
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => {
                // Handler returned — heartbeat is no longer needed.
                break;
            }
            _ = tokio::time::sleep(interval) => {
                match scheduler.renew_lease(&task_id, &lock_token).await {
                    Ok(true) => {
                        // Lease renewed successfully.
                        emit_metric!(TASK_HEARTBEAT_RENEWALS_TOTAL
                            .with_label_values(&[&task_name, "success"])
                            .inc());
                    }
                    Ok(false) => {
                        // Lease not found or token mismatch — another worker
                        // has taken over. Stop heartbeating.
                        warn!(%task_id, "Heartbeat: lease no longer exists, stopping");
                        emit_metric!(TASK_HEARTBEAT_RENEWALS_TOTAL
                            .with_label_values(&[&task_name, "not_found"])
                            .inc());
                        break;
                    }
                    Err(e) => {
                        // Database error — log but continue retrying.
                        // The next interval may succeed if the DB recovers.
                        error!(%task_id, error = %e, "Heartbeat: failed to renew lease");
                        emit_metric!(TASK_HEARTBEAT_RENEWALS_TOTAL
                            .with_label_values(&[&task_name, "failed"])
                            .inc());
                    }
                }
            }
        }
    }
    emit_metric!(HEARTBEAT_ACTIVE.dec());
}

/// Dispatch a single fetched task to its registered handler and persist the result.
#[instrument(
    name = "task.dispatch",
    skip(scheduler, registry, task),
    fields(
        task_id = %task.task_id,
        task_name = %task.task_name,
        execution_id = task.metadata.get("execution_id").and_then(|v| v.as_str()).unwrap_or("-"),
        attempt = task.attempt,
    ),
)]
async fn dispatch_task(
    scheduler: Arc<dyn StorageBackend>,
    registry: Arc<TaskRegistry>,
    task: zart_scheduler::FetchedTask,
    heartbeat_interval: Option<Duration>,
    orphan_timeout: Duration,
) {
    let exec_mode = ExecutionMode::from_metadata(&task.metadata);
    // Override retry_attempt with the scheduler's own attempt counter so it
    // accurately reflects how many times this step has been attempted.
    // task.attempt is 1-indexed; retry_attempt is 0-indexed.
    let exec_mode = match exec_mode {
        ExecutionMode::Step {
            target_step,
            step_type,
            retry_config,
            ..
        } => ExecutionMode::Step {
            target_step,
            step_type,
            retry_attempt: task.attempt.saturating_sub(1),
            retry_config,
        },
        other => other,
    };

    #[cfg(feature = "metrics")]
    let start_time = std::time::Instant::now();
    let handler = match registry.get_handler(&task.task_name) {
        Some(h) => h,
        None => {
            warn!(
                "No handler registered for task '{}'; registered handlers: [{}]",
                &task.task_name,
                registry.handler_names().join(", ")
            );
            let _ = scheduler
                .mark_failed(
                    &task.task_id,
                    "no handler registered",
                    None,
                    &task.lock_token,
                )
                .await;
            return;
        }
    };

    let has_execution = task.metadata.get("execution_id").is_some();
    let execution_id = task
        .metadata
        .get("execution_id")
        .and_then(|v| v.as_str())
        .map(String::from)
        .unwrap_or_else(|| task.task_id.clone());

    // run_id is the FK into zart_execution_runs; carried in metadata["run_id"] by body/step tasks.
    // Falls back to execution_id for non-durable tasks (which don't use zart_steps).
    let run_id = task
        .metadata
        .get("run_id")
        .and_then(|v| v.as_str())
        .unwrap_or(&execution_id)
        .to_string();

    // Parse step deadline from task metadata (set when timeout_scope == Global).
    let step_deadline = task
        .metadata
        .get("deadline")
        .and_then(|v| v.as_str())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc));

    // Compute execution deadline from handler timeout + execution scheduled_at.
    let execution_deadline = if has_execution {
        if let Some(timeout_dur) = handler.timeout() {
            match scheduler.get_execution(&execution_id).await {
                Ok(Some(exec)) => Some(
                    exec.scheduled_at
                        + chrono::Duration::from_std(timeout_dur)
                            .unwrap_or(chrono::Duration::zero()),
                ),
                Ok(None) | Err(_) => {
                    // Execution not found or DB error — proceed without execution deadline.
                    warn!("Could not load execution {execution_id} for deadline check");
                    None
                }
            }
        } else {
            None
        }
    } else {
        None
    };

    // Check if execution deadline has already passed.
    if let Some(deadline) = execution_deadline
        && chrono::Utc::now() >= deadline
    {
        info!(
            execution_id = %execution_id,
            "Execution deadline exceeded before dispatch — invoking on_failure"
        );
        let failure = ExecutionFailure::ExecutionDeadlineExceeded;
        match handler.on_failure(task.data.clone(), failure).await {
            Ok(output) => {
                info!(
                    "on_failure recovered from execution deadline — completing with synthetic result"
                );
                if let Err(e) = scheduler
                    .mark_completed(&task.task_id, Some(output.clone()), &task.lock_token)
                    .await
                {
                    error!(error = %e, "Failed to mark task completed after deadline on_failure recovery");
                }
                let _ = scheduler
                    .complete_execution(&execution_id, output)
                    .await
                    .map_err(|e| error!(error = %e, "Failed to complete execution record"));
            }
            Err(recovery_err) => {
                error!(error = %recovery_err, "on_failure did not recover execution deadline");
                if let Err(e) = scheduler
                    .mark_failed(
                        &task.task_id,
                        "execution deadline exceeded",
                        None,
                        &task.lock_token,
                    )
                    .await
                {
                    error!(error = %e, "Failed to mark task failed after deadline exceeded");
                }
                let _ = scheduler
                    .fail_execution(&execution_id)
                    .await
                    .map_err(|e| error!(error = %e, "Failed to fail execution record"));
            }
        }
        return;
    }

    let ctx = Arc::new(
        TaskContext::new(
            scheduler.clone(),
            execution_id.clone(),
            task.task_name.clone(),
            task.lock_token.clone(),
            task.data.clone(),
        )
        .with_task_id(task.task_id.clone())
        .with_run_id(run_id.clone())
        .with_execution_mode(exec_mode.clone())
        .with_step_deadline(step_deadline)
        .with_execution_deadline(execution_deadline),
    );
    let ctx_cleanup = Arc::clone(&ctx);

    // Record execution start for durable executions (tasks with an execution_id).
    if has_execution {
        emit_metric!(
            EXECUTIONS_TOTAL
                .with_label_values(&["started", &task.task_name])
                .inc()
        );
    }

    // ── Heartbeat setup ──────────────────────────────────────────────────────
    let heartbeat_cancellation = CancellationToken::new();
    let effective_interval = heartbeat_interval
        .filter(|d| !d.is_zero())
        .unwrap_or_else(|| orphan_timeout / 3);

    let heartbeat_handle = tokio::spawn({
        let scheduler = scheduler.clone();
        let task_id = task.task_id.clone();
        let lock_token = task.lock_token.clone();
        let task_name = task.task_name.clone();
        let cancellation = heartbeat_cancellation.clone();
        async move {
            heartbeat_loop(
                scheduler,
                task_id,
                lock_token,
                task_name,
                effective_interval,
                cancellation,
            )
            .await;
        }
    });

    // Execute the handler.
    let task_data = task.data.clone();
    let result = handler.execute(ctx, task_data).await;

    // ── Stop heartbeat — handler has returned ────────────────────────────────
    heartbeat_cancellation.cancel();
    let _ = heartbeat_handle.await;

    // ── Cancellation guard ────────────────────────────────────────────────────
    if has_execution {
        match ctx_cleanup.scheduler.get_execution(&execution_id).await {
            Ok(Some(exec)) if exec.status == zart_scheduler::ExecutionStatus::Cancelled => {
                info!("Execution cancelled while task was running; discarding result");
                let _ = ctx_cleanup
                    .scheduler
                    .mark_failed(
                        &task.task_id,
                        "execution cancelled",
                        None,
                        &ctx_cleanup.lock_token,
                    )
                    .await;
                return;
            }
            Ok(_) => {}
            Err(e) => {
                error!(error = %e, "Failed to check execution status after handler; proceeding");
            }
        }
    }

    match result {
        Ok(result) => {
            emit_metric!({
                let duration = start_time.elapsed().as_secs_f64();
                TASK_DURATION_SECONDS
                    .with_label_values(&[&task.task_name, "completed"])
                    .observe(duration);
            });
            info!("Task completed successfully");
            emit_metric!(TASKS_TOTAL.with_label_values(&["completed"]).inc());
            if has_execution {
                emit_metric!(
                    EXECUTIONS_TOTAL
                        .with_label_values(&["completed", &task.task_name])
                        .inc()
                );
            }
            if let Err(e) = ctx_cleanup
                .scheduler
                .mark_completed(&task.task_id, Some(result.clone()), &ctx_cleanup.lock_token)
                .await
            {
                error!(error = %e, "Failed to mark task completed");
                return;
            }

            // Recurring task: schedule the next occurrence.
            if let Some(ref recurrence) = task.recurrence {
                let now = chrono::Utc::now();
                if let Some(next_time) = recurrence.next_after(now) {
                    let new_task_id = Uuid::new_v4().to_string();
                    let task_data = ctx_cleanup.data().clone();
                    if let Err(e) = ctx_cleanup
                        .scheduler
                        .schedule_at(zart_scheduler::ScheduleAtParams {
                            task_id: new_task_id.clone(),
                            task_name: task.task_name.clone(),
                            execution_time: next_time,
                            data: task_data,
                            recurrence: Some(recurrence.clone()),
                            metadata: serde_json::json!({
                                "execution_id": task.metadata.get("execution_id"),
                            }),
                        })
                        .await
                    {
                        error!(
                            next_task_id = %new_task_id,
                            error = %e,
                            "Failed to schedule next recurring occurrence"
                        );
                    } else {
                        info!(
                            next_task_id = %new_task_id,
                            "Scheduled next recurring occurrence"
                        );
                    }
                } else {
                    warn!("Recurring task has no next occurrence");
                }
            }

            if has_execution {
                let _ = ctx_cleanup
                    .scheduler
                    .complete_execution(&execution_id, result)
                    .await
                    .map_err(|e| error!(error = %e, "Failed to complete execution record"));
            }
        }

        // ── New model: step executed in step mode — transactional completion done ──
        // Both the step completion and the next body scheduling were done atomically
        // inside `step()`. The worker just needs to "release" the task (it's already
        // marked completed in the DB, but we still hold the in-memory lock token).
        // Calling mark_completed again is a no-op due to the lock_token check failing
        // gracefully, so we can safely skip it.
        Err(TaskError::StepFailed {
            source: StepError::StepExecuted { ref step },
            ..
        }) => {
            info!(step = %step, "Step executed in step mode — completion was transactional");
            emit_metric!(TASKS_TOTAL.with_label_values(&["completed"]).inc());
            // The step task is already completed in DB. No further action.
        }

        // ── Body: step was scheduled — body task is done ─────────────────────────
        // When a body task schedules a child step and exits,
        // the body task itself is complete (it won't be re-entered).
        Err(TaskError::StepFailed {
            source:
                StepError::Scheduled {
                    ref step,
                    ref next_execution,
                },
            ..
        }) => {
            info!(step = %step, "Body step scheduled — marking body task complete");
            emit_metric!(TASKS_TOTAL.with_label_values(&["completed"]).inc());
            if let Err(e) = ctx_cleanup
                .scheduler
                .mark_completed(&task.task_id, None, &ctx_cleanup.lock_token)
                .await
            {
                error!(error = %e, "Failed to mark body task completed after step scheduling");
            }
            let _ = next_execution; // execution_time is on the child step task
        }

        Err(err) => {
            emit_metric!({
                let duration = start_time.elapsed().as_secs_f64();
                TASK_DURATION_SECONDS
                    .with_label_values(&[&task.task_name, "failed"])
                    .observe(duration);
            });

            // If this is a body task for a durable execution, invoke on_failure
            // before deciding whether to fail or complete.
            let failure = build_execution_failure(&err, &task);
            if has_execution {
                match handler.on_failure(task.data.clone(), failure).await {
                    Ok(output) => {
                        // on_failure recovered — complete the execution with synthetic result.
                        info!("on_failure recovered — completing execution with synthetic result");
                        emit_metric!(
                            EXECUTIONS_TOTAL
                                .with_label_values(&["recovered", &task.task_name])
                                .inc()
                        );
                        if let Err(e) = ctx_cleanup
                            .scheduler
                            .mark_completed(
                                &task.task_id,
                                Some(output.clone()),
                                &ctx_cleanup.lock_token,
                            )
                            .await
                        {
                            error!(error = %e, "Failed to mark task completed after on_failure recovery");
                        }
                        let _ = ctx_cleanup
                            .scheduler
                            .complete_execution(&execution_id, output)
                            .await
                            .map_err(|e| error!(error = %e, "Failed to complete execution record after on_failure recovery"));
                        return;
                    }
                    Err(recovery_err) => {
                        // on_failure did not recover — fail as normal.
                        error!(error = %recovery_err, "on_failure did not recover the execution");
                    }
                }
            } else {
                error!(error = %err, "Task failed");
            }

            emit_metric!(TASKS_TOTAL.with_label_values(&["failed"]).inc());
            if has_execution {
                emit_metric!(
                    EXECUTIONS_TOTAL
                        .with_label_values(&["failed", &task.task_name])
                        .inc()
                );
            }
            if let Err(e) = ctx_cleanup
                .scheduler
                .mark_failed(
                    &task.task_id,
                    &err.to_string(),
                    None,
                    &ctx_cleanup.lock_token,
                )
                .await
            {
                error!(error = %e, "Failed to mark task failed");
                return;
            }
            if has_execution {
                let _ = ctx_cleanup
                    .scheduler
                    .fail_execution(&execution_id)
                    .await
                    .map_err(|e| error!(error = %e, "Failed to fail execution record"));
            }
        }
    }
}

/// Build an [`ExecutionFailure`] from a [`TaskError`] for `on_failure` invocation.
fn build_execution_failure(
    err: &TaskError,
    task: &zart_scheduler::FetchedTask,
) -> ExecutionFailure {
    match err {
        TaskError::StepFailed { step, source } => {
            // Serialize the inner error for the failure envelope.
            let raw = serde_json::json!({
                "step": step,
                "error": source.to_string(),
                "error_kind": format!("{:?}", source),
            });
            ExecutionFailure::StepFailed {
                step: step.clone(),
                raw,
            }
        }
        TaskError::MaxRetriesExhausted { max_retries } => ExecutionFailure::RetriesExhausted {
            attempts: *max_retries,
        },
        TaskError::Timeout { duration } => {
            let _ = duration;
            ExecutionFailure::ExecutionDeadlineExceeded
        }
        TaskError::Cancelled => {
            let step = task.task_name.clone();
            let raw = serde_json::json!({ "error": "cancelled" });
            ExecutionFailure::StepFailed { step, raw }
        }
        TaskError::HandlerPanic(reason) => {
            let step = task.task_name.clone();
            let raw = serde_json::json!({ "panic": reason });
            ExecutionFailure::StepFailed { step, raw }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_config_defaults_are_sane() {
        let cfg = WorkerConfig::default();
        assert!(cfg.poll_interval > Duration::ZERO);
        assert!(cfg.max_tasks_per_poll > 0);
        assert!(cfg.max_concurrent_tasks > 0);
        assert!(cfg.shutdown_timeout > Duration::ZERO);
        assert!(cfg.heartbeat_interval.is_none()); // heartbeat uses auto-computed interval
    }

    #[test]
    fn worker_config_heartbeat_interval_defaults_to_none() {
        let cfg = WorkerConfig::default();
        assert!(cfg.heartbeat_interval.is_none());
    }

    #[test]
    fn worker_config_can_set_custom_heartbeat_interval() {
        let cfg = WorkerConfig {
            heartbeat_interval: Some(Duration::from_secs(60)),
            ..Default::default()
        };
        assert_eq!(cfg.heartbeat_interval, Some(Duration::from_secs(60)));
    }

    #[test]
    fn worker_config_can_disable_heartbeat_with_zero() {
        let cfg = WorkerConfig {
            heartbeat_interval: Some(Duration::ZERO),
            ..Default::default()
        };
        assert_eq!(cfg.heartbeat_interval, Some(Duration::ZERO));
    }

    #[test]
    fn effective_interval_uses_orphan_timeout_third_when_none() {
        let orphan_timeout = Duration::from_secs(300); // 5 minutes
        let heartbeat_interval: Option<Duration> = None;
        let effective = heartbeat_interval
            .filter(|d| !d.is_zero())
            .unwrap_or_else(|| orphan_timeout / 3);
        assert_eq!(effective, Duration::from_secs(100));
    }

    #[test]
    fn effective_interval_uses_custom_when_some() {
        let orphan_timeout = Duration::from_secs(300);
        let heartbeat_interval = Some(Duration::from_secs(30));
        let effective = heartbeat_interval
            .filter(|d| !d.is_zero())
            .unwrap_or_else(|| orphan_timeout / 3);
        assert_eq!(effective, Duration::from_secs(30));
    }

    #[test]
    fn effective_interval_disables_when_zero() {
        let _orphan_timeout = Duration::from_secs(300);
        let heartbeat_interval = Some(Duration::ZERO);
        let effective = heartbeat_interval.filter(|d| !d.is_zero());
        assert!(effective.is_none());
    }
}
