//! Generic scheduler worker — polls the task queue and dispatches to registered handlers.

use std::sync::Arc;
use std::time::Duration;

use sqlx::PgConnection;
use tokio::sync::{Notify, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, error, info, instrument, warn};

use crate::ops::ExecutionOps;
use crate::registry::TaskRegistry;
use crate::store::TaskScheduler;
use crate::task::{SchedulerTaskError, TaskInstance};
use crate::types::FetchedTask;
use crate::worker_config::WorkerConfig;

/// Polls the task queue and dispatches tasks to registered [`ScheduledTask`](crate::ScheduledTask) handlers.
///
/// The worker is generic: it knows nothing about durable executions, steps, or
/// runs. All application-specific logic lives in the handlers registered in
/// the [`TaskRegistry`].
///
/// # Dispatch model
///
/// For each fetched task the worker:
/// 1. Opens a database transaction via `TaskScheduler::begin`.
/// 2. Creates an [`ExecutionOps`] wrapping the transaction connection.
/// 3. Calls the registered handler's `execute(instance, ops)`.
/// 4. Commits on `Ok(())` (defaulting to `ops.complete(None)` if the handler
///    did not call any ops method), or rolls back on `Err` and calls
///    `mark_failed` outside the transaction.
///
/// # Heartbeating
///
/// While a handler runs, a background task renews the row's lease at
/// `orphan_timeout / 3` intervals so the row is not reclaimed by orphan
/// recovery. Configurable via [`WorkerConfig::heartbeat_interval`].
pub struct Worker {
    scheduler: Arc<dyn TaskScheduler>,
    registry: Arc<TaskRegistry>,
    config: WorkerConfig,
    shutdown: Arc<Notify>,
    cancellation: CancellationToken,
}

impl Worker {
    /// Create a new worker.
    pub fn new(
        scheduler: Arc<dyn TaskScheduler>,
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
    /// when a common `CancellationToken` is cancelled.
    pub fn with_cancellation(
        scheduler: Arc<dyn TaskScheduler>,
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
    /// Runs until [`stop`](Self::stop) is called or the cancellation token is
    /// triggered. Orphan recovery runs every 10 poll cycles.
    #[instrument(name = "scheduler_worker.run", skip(self))]
    pub async fn run(&self) {
        info!(
            poll_interval_ms = self.config.poll_interval.as_millis(),
            max_tasks = self.config.max_tasks_per_poll,
            concurrency = self.config.max_concurrent_tasks,
            orphan_timeout_secs = self.config.orphan_timeout.as_secs(),
            "Scheduler worker starting"
        );

        let semaphore = Arc::new(Semaphore::new(self.config.max_concurrent_tasks));
        let mut poll_count: u32 = 0;

        loop {
            let shutdown_notified = self.shutdown.notified();
            let cancellation = self.cancellation.cancelled();
            tokio::pin!(shutdown_notified);
            tokio::pin!(cancellation);

            tokio::select! {
                biased;
                _ = &mut shutdown_notified => {
                    info!("Scheduler worker shutdown signal received, exiting poll loop");
                    break;
                }
                _ = &mut cancellation => {
                    info!("Scheduler worker cancellation token triggered, exiting poll loop");
                    break;
                }
                _ = tokio::time::sleep(self.config.poll_interval) => {}
            }

            poll_count += 1;

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
                        let _permit = permit;
                        dispatch_task(
                            scheduler,
                            registry,
                            task,
                            heartbeat_interval,
                            orphan_timeout,
                        )
                        .await;
                    }
                    .in_current_span(),
                );
            }
        }
    }

    /// Signal the worker to stop after the current poll cycle completes.
    pub fn stop(&self) {
        info!("Scheduler worker stop requested");
        self.shutdown.notify_one();
        self.cancellation.cancel();
    }

    /// Get the cancellation token for this worker.
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }
}

/// Background heartbeat loop that extends a task's lease until cancelled.
async fn heartbeat_loop(
    scheduler: Arc<dyn TaskScheduler>,
    task_id: String,
    lock_token: String,
    task_name: String,
    interval: Duration,
    cancellation: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => break,
            _ = tokio::time::sleep(interval) => {
                match scheduler.renew_lease(&task_id, &lock_token).await {
                    Ok(true) => {}
                    Ok(false) => {
                        warn!(%task_id, %task_name, "Heartbeat: lease no longer exists, stopping");
                        break;
                    }
                    Err(e) => {
                        error!(%task_id, error = %e, "Heartbeat: failed to renew lease");
                    }
                }
            }
        }
    }
}

/// Dispatch a single fetched task to its registered handler.
#[instrument(
    name = "scheduler.dispatch_task",
    skip(scheduler, registry, task),
    fields(
        task_id = %task.task_id,
        task_name = %task.task_name,
        attempt = task.attempt,
    )
)]
async fn dispatch_task(
    scheduler: Arc<dyn TaskScheduler>,
    registry: Arc<TaskRegistry>,
    task: FetchedTask,
    heartbeat_interval: Option<Duration>,
    orphan_timeout: Duration,
) {
    let handler = match registry.get(&task.task_name) {
        Some(h) => h,
        None => {
            warn!(
                "No handler registered for task '{}'; registered handlers: [{}]",
                task.task_name,
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

    let instance = TaskInstance {
        task_id: task.task_id.clone(),
        task_name: task.task_name.clone(),
        data: task.data,
        metadata: task.metadata,
        lock_token: task.lock_token.clone(),
        attempt: task.attempt as u32,
    };

    // Start heartbeat
    let hb_cancel = CancellationToken::new();
    let effective_interval = heartbeat_interval
        .filter(|d| !d.is_zero())
        .unwrap_or_else(|| orphan_timeout / 3);
    let hb_handle = tokio::spawn({
        let scheduler = scheduler.clone();
        let task_id = task.task_id.clone();
        let lock_token = task.lock_token.clone();
        let task_name = task.task_name.clone();
        let cancel = hb_cancel.clone();
        async move {
            heartbeat_loop(
                scheduler,
                task_id,
                lock_token,
                task_name,
                effective_interval,
                cancel,
            )
            .await;
        }
    });

    // Begin transaction
    let mut tx = match scheduler.begin().await {
        Ok(tx) => tx,
        Err(e) => {
            hb_cancel.cancel();
            let _ = hb_handle.await;
            error!(error = %e, "Failed to begin transaction for task dispatch");
            let _ = scheduler
                .mark_failed(&task.task_id, &e.to_string(), None, &task.lock_token)
                .await;
            return;
        }
    };

    // Execute handler with ExecutionOps scoped to the transaction connection.
    // The inner block releases the borrow on `tx` so we can commit/rollback after.
    let execute_result: Result<(), SchedulerTaskError> = {
        let conn: &mut PgConnection = &mut tx;
        let mut ops =
            ExecutionOps::new(conn, scheduler.as_ref(), &task.task_id, &task.lock_token);
        let result = handler.execute(&instance, &mut ops).await;
        // Default: complete with no result if the handler returned Ok without setting an outcome.
        if result.is_ok() && !ops.outcome_set() {
            ops.complete(None)
                .await
                .map_err(SchedulerTaskError::Storage)
        } else {
            result
        }
    };

    // Stop heartbeat — handler has returned.
    hb_cancel.cancel();
    let _ = hb_handle.await;

    match execute_result {
        Ok(()) => {
            if let Err(e) = tx.commit().await {
                error!(error = %e, "Failed to commit task transaction");
            } else {
                info!("Task committed successfully");
            }
        }
        Err(e) => {
            let _ = tx.rollback().await;
            if let Err(fe) = scheduler
                .mark_failed(&task.task_id, &e.to_string(), None, &task.lock_token)
                .await
            {
                error!(error = %fe, "Failed to mark task failed after rollback");
            }
        }
    }
}
