//! Worker — polls the scheduler and dispatches tasks to registered handlers.

use crate::context::{ExecutionState, TaskContext};
use crate::error::{StepError, TaskError};
use crate::metrics::{
    POLL_INTERVAL_SECONDS, QUEUE_DEPTH, TASK_DURATION_SECONDS, TASKS_TOTAL, WORKER_CONCURRENT_TASKS,
};
use crate::registry::TaskRegistry;
use scheduler::Scheduler;
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

    /// When enabled, steps scheduled via `step()` are executed immediately in-memory
    /// without returning early. This reduces latency between sequential steps but
    /// blocks the worker for the duration of each step.
    ///
    /// This is equivalent to using `step_immediate()` for all steps, but applies
    /// globally to the entire durable execution.
    ///
    /// **Trade-off**: lower latency vs. reduced fault tolerance. The worker cannot
    /// pick up other tasks while a step is executing. If the worker crashes, orphan
    /// recovery will eventually reset the in-flight step.
    pub immediate_steps: bool,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(5),
            max_tasks_per_poll: 10,
            max_concurrent_tasks: 16,
            shutdown_timeout: Duration::from_secs(30),
            orphan_timeout: Duration::from_secs(300),
            immediate_steps: false, // Disabled by default for maximum fault tolerance.
        }
    }
}

/// A polling worker that continuously fetches due tasks from the scheduler
/// and dispatches them to their registered handlers.
///
/// Multiple `Worker` instances can run concurrently (even across processes)
/// — the database-level skip-lock prevents duplicate task execution.
pub struct Worker<S: Scheduler> {
    scheduler: Arc<S>,
    registry: Arc<TaskRegistry<S>>,
    config: WorkerConfig,
    /// Notified by [`stop`](Self::stop) to trigger a graceful shutdown.
    shutdown: Arc<Notify>,
    /// CancellationToken for composability with external shutdown coordinators.
    cancellation: CancellationToken,
}

impl<S: Scheduler + 'static> Worker<S> {
    /// Create a new worker.
    pub fn new(scheduler: Arc<S>, registry: Arc<TaskRegistry<S>>, config: WorkerConfig) -> Self {
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
        scheduler: Arc<S>,
        registry: Arc<TaskRegistry<S>>,
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
            let poll_interval = last_poll_time.elapsed().as_secs_f64();
            POLL_INTERVAL_SECONDS
                .with_label_values(&[])
                .observe(poll_interval);
            last_poll_time = std::time::Instant::now();

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
            QUEUE_DEPTH.set(tasks.len() as f64);

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
                let immediate_steps = self.config.immediate_steps;

                tokio::spawn(
                    async move {
                        let _permit = permit; // released when this task finishes
                        WORKER_CONCURRENT_TASKS.inc();
                        dispatch_task_with_immediate(scheduler, registry, task, immediate_steps).await;
                        WORKER_CONCURRENT_TASKS.dec();
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

/// Dispatch a single fetched task to its registered handler and persist the result.
///
/// This is a convenience wrapper that calls [`dispatch_task_with_immediate`] with
/// `immediate_steps = false`.
#[allow(dead_code)]
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
async fn dispatch_task<S: Scheduler + 'static>(
    scheduler: Arc<S>,
    registry: Arc<TaskRegistry<S>>,
    task: scheduler::FetchedTask,
) {
    dispatch_task_with_immediate(scheduler, registry, task, false).await
}

/// Dispatch a single fetched task with optional immediate step execution.
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
async fn dispatch_task_with_immediate<S: Scheduler + 'static>(
    scheduler: Arc<S>,
    registry: Arc<TaskRegistry<S>>,
    task: scheduler::FetchedTask,
    immediate_steps: bool,
) {
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

    let state: ExecutionState = serde_json::from_value(task.state.clone()).unwrap_or_default();
    let has_execution = task.execution_id.is_some();
    let execution_id = task
        .execution_id
        .clone()
        .unwrap_or_else(|| task.task_id.clone());

    let mut ctx = TaskContext::new(
        scheduler.clone(),
        execution_id.clone(),
        task.task_name.clone(),
        state,
        task.lock_token.clone(),
        task.data.clone(),
    );

    // Apply immediate_steps mode if configured.
    if immediate_steps {
        ctx = ctx.with_immediate_steps();
    }

    let result = handler.execute(&mut ctx, task.data).await;

    // ── Cancellation guard ────────────────────────────────────────────────────
    // A durable execution can be cancelled while its task is already `picked_up`
    // (cancel_execution only touches `scheduled` rows). We must check here —
    // BEFORE persisting any result — so that we don't:
    //   • overwrite the `cancelled` execution status with `completed` or `failed`
    //   • re-queue the task via update_task_state (StepError::Scheduled path),
    //     which would set it back to `scheduled` and cause infinite re-runs.
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
            Ok(_) => {} // Not cancelled, proceed normally.
            Err(e) => {
                error!(error = %e, "Failed to check execution status after handler; proceeding");
            }
        }
    }

    match result {
        Ok(result) => {
            let duration = start_time.elapsed().as_secs_f64();
            TASK_DURATION_SECONDS
                .with_label_values(&[&task.task_name, "completed"])
                .observe(duration);
            info!("Task completed successfully");
            TASKS_TOTAL.with_label_values(&["completed"]).inc();
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
                    // Use the data stored in the context (same value, not moved).
                    let task_data = ctx.data().clone();
                    if let Err(e) = ctx
                        .scheduler
                        .schedule_at(
                            &new_task_id,
                            &task.task_name,
                            next_time,
                            task_data,
                            Some(recurrence.clone()),
                            task.execution_id.as_deref(),
                        )
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
                let _ = ctx.scheduler.complete_execution(&execution_id, result).await
                    .map_err(|e| error!(error = %e, "Failed to complete execution record"));
            }
        }

        // Control-flow: a step was just scheduled (first time) or a retry is pending.
        // Persist the updated state and re-queue at `next_execution` (or immediately).
        Err(TaskError::StepFailed {
            source:
                StepError::Scheduled {
                    ref step,
                    ref next_execution,
                },
            ..
        }) => {
            let exec_time = next_execution.unwrap_or_else(chrono::Utc::now);
            info!(
                step = %step,
                "Step scheduled — persisting state and re-queuing",
            );
            let state_json = match serde_json::to_value(&ctx.state) {
                Ok(v) => v,
                Err(e) => {
                    error!(error = %e, "Failed to serialize execution state");
                    return;
                }
            };
            if let Err(e) = ctx
                .scheduler
                .update_task_state(&task.task_id, state_json, exec_time, &ctx.lock_token)
                .await
            {
                error!(error = %e, "Failed to update task state");
            }
        }

        // Control-flow: a step is waiting for an external event.
        // Persist state with a far-future execution time to avoid busy-looping.
        // `offer_event` will atomically reset the execution time to NOW().
        Err(TaskError::StepFailed {
            source: StepError::WaitingForEvent { ref event },
            ..
        }) => {
            // Park 24 h in the future; offer_event will wake it up earlier.
            let exec_time = chrono::Utc::now() + chrono::Duration::hours(24);
            info!(event = %event, "Step waiting for event — parking task");
            let state_json = match serde_json::to_value(&ctx.state) {
                Ok(v) => v,
                Err(e) => {
                    error!(error = %e, "Failed to serialize execution state");
                    return;
                }
            };
            if let Err(e) = ctx
                .scheduler
                .update_task_state(&task.task_id, state_json, exec_time, &ctx.lock_token)
                .await
            {
                error!(error = %e, "Failed to park task for event wait");
            }
        }

        Err(err) => {
            let duration = start_time.elapsed().as_secs_f64();
            TASK_DURATION_SECONDS
                .with_label_values(&[&task.task_name, "failed"])
                .observe(duration);
            error!(error = %err, "Task failed");
            TASKS_TOTAL.with_label_values(&["failed"]).inc();
            if let Err(e) = ctx
                .scheduler
                .mark_failed(&task.task_id, &err.to_string(), None, &ctx.lock_token)
                .await
            {
                error!(error = %e, "Failed to mark task failed");
                return;
            }
            if has_execution {
                let _ = ctx.scheduler.fail_execution(&execution_id).await
                    .map_err(|e| error!(error = %e, "Failed to fail execution record"));
            }
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
        assert!(!cfg.immediate_steps); // immediate_steps is disabled by default
    }

    #[test]
    fn worker_config_can_enable_immediate_steps() {
        let cfg = WorkerConfig {
            immediate_steps: true,
            ..Default::default()
        };
        assert!(cfg.immediate_steps);
    }
}
