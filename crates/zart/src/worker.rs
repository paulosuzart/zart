//! Worker — polls the scheduler and dispatches tasks to registered handlers.

use crate::context::{ExecutionState, TaskContext};
use crate::error::{StepError, TaskError};
use crate::registry::TaskRegistry;
use scheduler::Scheduler;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, Semaphore};
use tracing::{error, info, warn};

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
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(5),
            max_tasks_per_poll: 10,
            max_concurrent_tasks: 16,
            shutdown_timeout: Duration::from_secs(30),
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
}

impl<S: Scheduler + 'static> Worker<S> {
    /// Create a new worker.
    pub fn new(scheduler: Arc<S>, registry: Arc<TaskRegistry<S>>, config: WorkerConfig) -> Self {
        Self {
            scheduler,
            registry,
            config,
            shutdown: Arc::new(Notify::new()),
        }
    }

    /// Start the polling loop.
    ///
    /// Runs until [`stop`](Self::stop) is called. Uses a semaphore to cap
    /// concurrent task execution at `config.max_concurrent_tasks`.
    pub async fn run(&self) {
        info!(
            poll_interval_ms = self.config.poll_interval.as_millis(),
            max_tasks = self.config.max_tasks_per_poll,
            concurrency = self.config.max_concurrent_tasks,
            "Worker starting"
        );

        let semaphore = Arc::new(Semaphore::new(self.config.max_concurrent_tasks));

        loop {
            // Check for shutdown before each poll.
            let shutdown_notified = self.shutdown.notified();
            tokio::pin!(shutdown_notified);

            tokio::select! {
                biased;
                _ = &mut shutdown_notified => {
                    info!("Worker shutdown signal received, exiting poll loop");
                    break;
                }
                _ = tokio::time::sleep(self.config.poll_interval) => {}
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

                tokio::spawn(async move {
                    let _permit = permit; // released when this task finishes
                    dispatch_task(scheduler, registry, task).await;
                });
            }
        }
    }

    /// Signal the worker to stop after the current poll cycle completes.
    pub fn stop(&self) {
        info!("Worker stop requested");
        self.shutdown.notify_one();
    }
}

/// Dispatch a single fetched task to its registered handler and persist the result.
async fn dispatch_task<S: Scheduler + 'static>(
    scheduler: Arc<S>,
    registry: Arc<TaskRegistry<S>>,
    task: scheduler::FetchedTask,
) {
    let handler = match registry.get_handler(&task.task_name) {
        Some(h) => h,
        None => {
            warn!(task_id = %task.task_id, task_name = %task.task_name, "No handler registered");
            let _ = scheduler
                .mark_failed(&task.task_id, "no handler registered", None, &task.lock_token)
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

    let result = handler.execute(&mut ctx, task.data).await;
    // All scheduler calls below go through ctx.scheduler so the field is used,
    // and ctx.lock_token provides the lock token without re-borrowing `task`.
    match result {
        Ok(result) => {
            info!(task_id = %task.task_id, "Task completed successfully");
            if let Err(e) = ctx
                .scheduler
                .mark_completed(&task.task_id, Some(result.clone()), &ctx.lock_token)
                .await
            {
                error!(task_id = %task.task_id, error = %e, "Failed to mark task completed");
                return;
            }
            if has_execution {
                let _ = ctx.scheduler.complete_execution(&execution_id, result).await
                    .map_err(|e| error!(execution_id = %execution_id, error = %e, "Failed to complete execution record"));
            }
        }

        // Control-flow: a step was just scheduled for the first time.
        // Persist the updated state (step is now SCHEDULED) and re-queue immediately.
        Err(TaskError::StepFailed {
            source: StepError::Scheduled { ref step },
            ..
        }) => {
            info!(task_id = %task.task_id, step = %step, "Step scheduled — persisting state and re-queuing");
            let state_json = match serde_json::to_value(&ctx.state) {
                Ok(v) => v,
                Err(e) => {
                    error!(task_id = %task.task_id, error = %e, "Failed to serialize execution state");
                    return;
                }
            };
            if let Err(e) = ctx
                .scheduler
                .update_task_state(&task.task_id, state_json, chrono::Utc::now(), &ctx.lock_token)
                .await
            {
                error!(task_id = %task.task_id, error = %e, "Failed to update task state");
            }
        }

        Err(err) => {
            error!(task_id = %task.task_id, error = %err, "Task failed");
            if let Err(e) = ctx
                .scheduler
                .mark_failed(&task.task_id, &err.to_string(), None, &ctx.lock_token)
                .await
            {
                error!(task_id = %task.task_id, error = %e, "Failed to mark task failed");
                return;
            }
            if has_execution {
                let _ = ctx.scheduler.fail_execution(&execution_id).await
                    .map_err(|e| error!(execution_id = %execution_id, error = %e, "Failed to fail execution record"));
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
    }
}
