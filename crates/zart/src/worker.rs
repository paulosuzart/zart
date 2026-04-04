//! Worker — polls the scheduler and dispatches tasks to registered handlers.

use crate::registry::TaskRegistry;
use scheduler::Scheduler;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info};

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
}

impl<S: Scheduler + 'static> Worker<S> {
    /// Create a new worker.
    pub fn new(scheduler: Arc<S>, registry: Arc<TaskRegistry<S>>, config: WorkerConfig) -> Self {
        Self {
            scheduler,
            registry,
            config,
        }
    }

    /// Start the polling loop.
    ///
    /// Runs until the process receives a shutdown signal or [`stop`](Self::stop)
    /// is called. Implements exponential backoff on consecutive empty polls.
    pub async fn run(&self) {
        info!(
            poll_interval_ms = self.config.poll_interval.as_millis(),
            max_tasks = self.config.max_tasks_per_poll,
            concurrency = self.config.max_concurrent_tasks,
            "Worker starting"
        );

        // TODO(M2): implement the full polling loop with task dispatch,
        // semaphore-based concurrency, and graceful shutdown.
        loop {
            tokio::time::sleep(self.config.poll_interval).await;

            match self
                .scheduler
                .poll_due(chrono::Utc::now(), self.config.max_tasks_per_poll)
                .await
            {
                Ok(tasks) if tasks.is_empty() => {
                    // No tasks due — continue polling.
                }
                Ok(tasks) => {
                    info!(count = tasks.len(), "Fetched tasks for execution");
                    // TODO(M2): dispatch each task to its registered handler.
                    let _ = tasks;
                }
                Err(e) => {
                    error!(error = %e, "Failed to poll for due tasks");
                }
            }
        }
    }

    /// Signal the worker to stop after completing in-flight tasks.
    ///
    /// The worker will finish any tasks currently executing and then exit.
    pub fn stop(&self) {
        // TODO(M2): implement graceful shutdown via CancellationToken.
        info!("Worker stop requested");
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
