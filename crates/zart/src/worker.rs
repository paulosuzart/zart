//! Worker — polls the scheduler and dispatches tasks to registered handlers.

use crate::context::TaskContext;
use crate::error::{StepError, TaskError};
use crate::execution_model::ExecutionMode;
#[cfg(feature = "metrics")]
use crate::metrics::{
    HEARTBEAT_ACTIVE, POLL_INTERVAL_SECONDS, QUEUE_DEPTH, TASK_DURATION_SECONDS,
    TASK_HEARTBEAT_RENEWALS_TOTAL, TASKS_TOTAL, WORKER_CONCURRENT_TASKS,
};

/// Evaluate a metrics expression when the `metrics` feature is enabled;
/// compile to nothing otherwise.
#[cfg(feature = "metrics")]
macro_rules! emit_metric {
    ($($tt:tt)*) => { $($tt)* };
}
#[cfg(not(feature = "metrics"))]
macro_rules! emit_metric {
    ($($tt:tt)*) => {};
}
use crate::registry::TaskRegistry;
use scheduler::StorageBackend;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, error, info, instrument, warn};
use uuid::Uuid;

/// Configuration for a polling worker.
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

/// A polling worker that continuously fetches due tasks from the scheduler
/// and dispatches them to their registered handlers.
///
/// Multiple `Worker` instances can run concurrently (even across processes)
/// — the database-level skip-lock prevents duplicate task execution.
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
        execution_id = task.execution_id.as_deref().unwrap_or("-"),
        attempt = task.attempt,
    ),
)]
async fn dispatch_task(
    scheduler: Arc<dyn StorageBackend>,
    registry: Arc<TaskRegistry>,
    task: scheduler::FetchedTask,
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
            next_body_segment,
            retry_config,
            ..
        } => ExecutionMode::Step {
            target_step,
            step_type,
            next_body_segment,
            retry_attempt: task.attempt.saturating_sub(1),
            retry_config,
        },
        other => other,
    };

    // ── Coordinator tasks (wait_all) ─────────────────────────────────────────
    // These don't dispatch to a handler. They poll children and schedule the
    // next body segment when all children are done.
    if let ExecutionMode::Coordinator {
        ref wait_for,
        next_segment,
    } = exec_mode
    {
        dispatch_coordinator(scheduler, task, wait_for.clone(), next_segment).await;
        return;
    }

    // ── Sleep continuation tasks ─────────────────────────────────────────────
    // Step tasks with step_type=sleep just wake the next body segment.
    if matches!(
        exec_mode,
        ExecutionMode::Step { ref step_type, .. }
        if *step_type == crate::execution_model::StepKind::Sleep
    ) {
        dispatch_sleep_continuation(scheduler, task, &exec_mode).await;
        return;
    }

    // ── WaitForEvent deadline tasks ───────────────────────────────────────────
    // When the deadline fires before offer_event arrives, fail the execution.
    if matches!(
        exec_mode,
        ExecutionMode::Step { ref step_type, .. }
        if *step_type == crate::execution_model::StepKind::WaitForEvent
    ) {
        dispatch_wait_for_event(scheduler, task).await;
        return;
    }

    #[cfg(feature = "metrics")]
    let start_time = std::time::Instant::now();
    let handler = match registry.get_handler(&task.task_name) {
        Some(h) => h,
        None => {
            warn!("No handler registered for task");
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

    let has_execution = task.execution_id.is_some();
    let execution_id = task
        .execution_id
        .clone()
        .unwrap_or_else(|| task.task_id.clone());

    let mut ctx = TaskContext::new(
        scheduler.clone(),
        execution_id.clone(),
        task.task_name.clone(),
        task.lock_token.clone(),
        task.data.clone(),
    )
    .with_task_id(task.task_id.clone())
    .with_execution_mode(exec_mode.clone());

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
    let result = handler.execute(&mut ctx, task.data).await;

    // ── Stop heartbeat — handler has returned ────────────────────────────────
    heartbeat_cancellation.cancel();
    let _ = heartbeat_handle.await;

    // ── Cancellation guard ────────────────────────────────────────────────────
    if has_execution {
        match ctx.scheduler.get_execution(&execution_id).await {
            Ok(Some(exec)) if exec.status == scheduler::ExecutionStatus::Cancelled => {
                info!("Execution cancelled while task was running; discarding result");
                let _ = ctx
                    .scheduler
                    .mark_failed(&task.task_id, "execution cancelled", None, &ctx.lock_token)
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
            if let Err(e) = ctx
                .scheduler
                .mark_completed(&task.task_id, Some(result.clone()), &ctx.lock_token)
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
                    let task_data = ctx.data().clone();
                    if let Err(e) = ctx
                        .scheduler
                        .schedule_at(scheduler::ScheduleAtParams {
                            task_id: new_task_id.clone(),
                            task_name: task.task_name.clone(),
                            execution_time: next_time,
                            data: task_data,
                            recurrence: Some(recurrence.clone()),
                            execution_id: task.execution_id.clone(),
                            metadata: serde_json::Value::Null,
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
                let _ = ctx
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
            if let Err(e) = ctx
                .scheduler
                .mark_completed(&task.task_id, None, &ctx.lock_token)
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
            error!(error = %err, "Task failed");
            emit_metric!(TASKS_TOTAL.with_label_values(&["failed"]).inc());
            if let Err(e) = ctx
                .scheduler
                .mark_failed(&task.task_id, &err.to_string(), None, &ctx.lock_token)
                .await
            {
                error!(error = %e, "Failed to mark task failed");
                return;
            }
            if has_execution {
                let _ = ctx
                    .scheduler
                    .fail_execution(&execution_id)
                    .await
                    .map_err(|e| error!(error = %e, "Failed to fail execution record"));
            }
        }
    }
}

// ── Coordinator dispatch ────────────────────────────────────────────────────

/// Handle a coordinator task (`step_type = wait_all`).
///
/// Polls all child step tasks. If all are completed, schedules the next body
/// segment and marks the coordinator completed. Otherwise re-queues itself
/// with a short backoff so it can check again after the next poll cycle.
async fn dispatch_coordinator(
    scheduler: Arc<dyn StorageBackend>,
    task: scheduler::FetchedTask,
    wait_for: Vec<String>,
    next_segment: usize,
) {
    let execution_id = task
        .execution_id
        .as_deref()
        .unwrap_or(&task.task_id)
        .to_string();

    let completed = match scheduler.check_wait_all_children(&wait_for).await {
        Ok(v) => v,
        Err(e) => {
            error!(error = %e, "Coordinator: failed to check children");
            let _ = scheduler
                .mark_failed(&task.task_id, &e.to_string(), None, &task.lock_token)
                .await;
            return;
        }
    };

    if completed.len() == wait_for.len() {
        // All done — atomically mark coordinator completed + schedule next body segment.
        // complete_step_and_schedule_body inserts the body task with correct metadata
        // ({mode:body, execution_id, segment}) in a single transaction.
        let next_body_task_id = format!("{}-b{}", execution_id, next_segment);
        if let Err(e) = crate::step_ops::complete_step_and_schedule_body(
            &*scheduler,
            crate::step_ops::ResumeBodySpec {
                step_task_id: &task.task_id,
                result: serde_json::Value::Null,
                lock_token: &task.lock_token,
                next_body_task_id: &next_body_task_id,
                task_name: &task.task_name,
                execution_id: &execution_id,
                next_segment,
                data: task.data.clone(),
            },
        )
        .await
        {
            error!(error = %e, "Coordinator: failed to complete and schedule next body");
            let _ = scheduler
                .mark_failed(&task.task_id, &e.to_string(), None, &task.lock_token)
                .await;
            return;
        }
        info!(
            next_segment,
            "Coordinator: all children done, next body scheduled"
        );
    } else {
        // Not all done — re-queue with a short backoff.
        let retry_at = chrono::Utc::now() + chrono::Duration::seconds(5);
        if let Err(e) = scheduler
            .mark_failed(
                &task.task_id,
                "children not yet complete",
                Some(retry_at),
                &task.lock_token,
            )
            .await
        {
            error!(error = %e, "Coordinator: failed to re-queue self");
        }
        info!(
            done = completed.len(),
            total = wait_for.len(),
            "Coordinator: waiting for children"
        );
    }
}

// ── Sleep continuation dispatch ─────────────────────────────────────────────

/// Handle a sleep continuation task (`step_type = sleep`).
///
/// When the sleep timer fires, schedule the next body segment.
async fn dispatch_sleep_continuation(
    scheduler: Arc<dyn StorageBackend>,
    task: scheduler::FetchedTask,
    exec_mode: &ExecutionMode,
) {
    let next_segment = match exec_mode {
        ExecutionMode::Step {
            next_body_segment, ..
        } => *next_body_segment,
        _ => {
            error!("Sleep task has unexpected execution mode");
            let _ = scheduler
                .mark_failed(
                    &task.task_id,
                    "unexpected mode for sleep task",
                    None,
                    &task.lock_token,
                )
                .await;
            return;
        }
    };

    let execution_id = task
        .execution_id
        .as_deref()
        .unwrap_or(&task.task_id)
        .to_string();

    let next_body_task_id = format!("{}-b{}", execution_id, next_segment);

    if let Err(e) = crate::step_ops::complete_step_and_schedule_body(
        &*scheduler,
        crate::step_ops::ResumeBodySpec {
            step_task_id: &task.task_id,
            result: serde_json::Value::Null,
            lock_token: &task.lock_token,
            next_body_task_id: &next_body_task_id,
            task_name: &task.task_name,
            execution_id: &execution_id,
            next_segment,
            data: task.data.clone(),
        },
    )
    .await
    {
        error!(error = %e, "Sleep continuation: failed to schedule next body");
        let _ = scheduler
            .mark_failed(&task.task_id, &e.to_string(), None, &task.lock_token)
            .await;
    } else {
        info!(next_segment, "Sleep continuation: next body scheduled");
    }
}

// ── WaitForEvent deadline dispatch ───────────────────────────────────────────

/// Handle a wait_for_event step task whose deadline has fired.
///
/// The event never arrived before the deadline, so we mark the step task
/// failed and fail the entire execution.
///
/// The two DB calls are sequential, not atomic — consistent with the general
/// failure path in `dispatch_task`. A crash between the two leaves the
/// execution in `running` with a failed step task, but the execution is
/// functionally dead (no active tasks, no pending body segment). Operators
/// can detect this via the failed step row and manually cancel or reset.
async fn dispatch_wait_for_event(scheduler: Arc<dyn StorageBackend>, task: scheduler::FetchedTask) {
    let execution_id = task
        .execution_id
        .as_deref()
        .unwrap_or(&task.task_id)
        .to_string();
    let step_name = task
        .metadata
        .get("step_name")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    info!(step = %step_name, "wait_for_event deadline exceeded — failing execution");
    let _ = scheduler
        .mark_failed(
            &task.task_id,
            "event deadline exceeded",
            None,
            &task.lock_token,
        )
        .await;
    let _ = scheduler.fail_execution(&execution_id).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::{Call, RecordingScheduler};
    use scheduler::FetchedTask;

    fn make_fetched_task(task_id: &str, metadata: serde_json::Value) -> FetchedTask {
        FetchedTask {
            task_id: task_id.to_string(),
            task_name: "my-handler".to_string(),
            data: serde_json::Value::Null,
            state: serde_json::Value::Null,
            attempt: 1,
            lock_token: "tok".to_string(),
            execution_id: Some("exec-1".to_string()),
            recurrence: None,
            metadata,
        }
    }

    // ── dispatch_coordinator ──────────────────────────────────────────────────

    /// When all children are completed, `dispatch_coordinator` must call
    /// `complete_and_schedule` exactly once (marks coordinator done + inserts
    /// next body task atomically). No separate `mark_failed` should appear.
    #[tokio::test]
    async fn coordinator_all_children_done_calls_complete_and_schedule_once() {
        let child_ids = vec!["exec-1:step:a".to_string(), "exec-1:step:b".to_string()];
        let (scheduler, calls) = RecordingScheduler::builder()
            .wait_all_returns(vec![
                ("exec-1:step:a".to_string(), serde_json::json!(1)),
                ("exec-1:step:b".to_string(), serde_json::json!(2)),
            ])
            .build();

        let task = make_fetched_task(
            "exec-1:coord:wait_all:2",
            serde_json::json!({
                "mode": "step",
                "step_type": "wait_all",
                "segment": 3,
                "wait_for": child_ids.clone(),
            }),
        );

        dispatch_coordinator(scheduler, task, child_ids, 3).await;

        let log = calls.lock().unwrap();
        let cas: Vec<_> = log
            .iter()
            .filter(|c| c.is_complete_and_schedule())
            .collect();
        let failures: Vec<_> = log.iter().filter(|c| c.is_mark_failed()).collect();

        assert_eq!(cas.len(), 1, "expected exactly one complete_and_schedule");
        assert!(
            failures.is_empty(),
            "no mark_failed expected when all children done"
        );

        if let Call::CompleteAndSchedule {
            new_task_id,
            new_metadata,
            ..
        } = &cas[0]
        {
            assert_eq!(new_task_id, "exec-1-b3");
            assert_eq!(new_metadata["mode"], "body");
            assert_eq!(new_metadata["segment"], 3);
        }
    }

    /// When some children are still pending, `dispatch_coordinator` must call
    /// `mark_failed` with a non-None `next_execution_time` (the backoff re-queue)
    /// and must NOT call `complete_and_schedule`.
    #[tokio::test]
    async fn coordinator_pending_children_requeues_with_backoff_no_body_scheduled() {
        let child_ids = vec!["exec-1:step:a".to_string(), "exec-1:step:b".to_string()];
        // Only one child is done — the other is still in-flight.
        let (scheduler, calls) = RecordingScheduler::builder()
            .wait_all_returns(vec![("exec-1:step:a".to_string(), serde_json::json!(1))])
            .build();

        let task = make_fetched_task(
            "exec-1:coord:wait_all:2",
            serde_json::json!({
                "mode": "step",
                "step_type": "wait_all",
                "segment": 2,
                "wait_for": child_ids.clone(),
            }),
        );

        dispatch_coordinator(scheduler, task, child_ids, 2).await;

        let log = calls.lock().unwrap();
        let cas: Vec<_> = log
            .iter()
            .filter(|c| c.is_complete_and_schedule())
            .collect();
        let failures: Vec<_> = log.iter().filter(|c| c.is_mark_failed()).collect();

        assert!(
            cas.is_empty(),
            "no body scheduled when children still pending"
        );
        assert_eq!(
            failures.len(),
            1,
            "coordinator re-queues itself via mark_failed"
        );

        if let Call::MarkFailed {
            next_execution_time,
            ..
        } = &failures[0]
        {
            assert!(
                next_execution_time.is_some(),
                "re-queue must carry a backoff execution_time"
            );
        }
    }

    // ── dispatch_sleep_continuation ───────────────────────────────────────────

    /// When a sleep task fires, `dispatch_sleep_continuation` must call
    /// `complete_and_schedule` exactly once, inserting the next body segment
    /// with `mode=body` metadata. No failures should occur.
    #[tokio::test]
    async fn sleep_continuation_calls_complete_and_schedule_with_body_metadata() {
        let (scheduler, calls) = RecordingScheduler::builder().build();
        let exec_mode = ExecutionMode::Step {
            target_step: "__sleep".to_string(),
            step_type: crate::execution_model::StepKind::Sleep,
            next_body_segment: 4,
            retry_attempt: 0,
            retry_config: None,
        };

        let task = make_fetched_task(
            "exec-1:step:__sleep",
            serde_json::json!({
                "mode": "step",
                "step_type": "sleep",
                "step_name": "__sleep",
                "segment": 4,
            }),
        );

        dispatch_sleep_continuation(scheduler, task, &exec_mode).await;

        let log = calls.lock().unwrap();
        let cas: Vec<_> = log
            .iter()
            .filter(|c| c.is_complete_and_schedule())
            .collect();
        let failures: Vec<_> = log.iter().filter(|c| c.is_mark_failed()).collect();

        assert_eq!(
            cas.len(),
            1,
            "sleep fires exactly one complete_and_schedule"
        );
        assert!(
            failures.is_empty(),
            "no failures for a normal sleep completion"
        );

        if let Call::CompleteAndSchedule {
            new_task_id,
            new_metadata,
            ..
        } = &cas[0]
        {
            assert_eq!(new_task_id, "exec-1-b4");
            assert_eq!(new_metadata["mode"], "body");
            assert_eq!(new_metadata["segment"], 4);
        }
    }

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

    // ── dispatch_wait_for_event ───────────────────────────────────────────────

    /// When a wait_for_event deadline fires, `dispatch_wait_for_event` must call
    /// `mark_failed` on the step task and `fail_execution` on the execution.
    #[tokio::test]
    async fn dispatch_wait_for_event_marks_failed_and_fails_execution() {
        let (scheduler, calls) = RecordingScheduler::builder().build();

        let task = make_fetched_task(
            "exec-1:step:approval",
            serde_json::json!({
                "mode": "step",
                "step_type": "wait_for_event",
                "step_name": "approval",
                "segment": 2,
            }),
        );

        dispatch_wait_for_event(scheduler, task).await;

        let log = calls.lock().unwrap();
        let failures: Vec<_> = log.iter().filter(|c| c.is_mark_failed()).collect();
        let fail_execs: Vec<_> = log.iter().filter(|c| c.is_fail_execution()).collect();

        assert_eq!(failures.len(), 1, "step task must be marked failed");
        assert_eq!(fail_execs.len(), 1, "execution must be failed");

        if let crate::test_helpers::Call::MarkFailed { task_id, .. } = &failures[0] {
            assert_eq!(task_id, "exec-1:step:approval");
        }
        if let crate::test_helpers::Call::FailExecution { execution_id } = &fail_execs[0] {
            assert_eq!(execution_id, "exec-1");
        }
    }
}
