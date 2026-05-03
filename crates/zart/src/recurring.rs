//! Recurring durable executions — fire a [`DurableExecution`] on a schedule.
//!
//! [`RecurringDurableTask`] implements [`ScheduledTask`] so it can be registered
//! in the scheduler registry. Each time the underlying cron fires it starts (or
//! skips / cancels) a durable execution according to the chosen [`OverlapPolicy`].
//!
//! The occurrence counter is stored in the task's `metadata["occurrence"]` field
//! and incremented on every successful dispatch.
//!
//! ## Occurrence counter — best-effort guarantee
//!
//! The counter increment is **best-effort**. If the process crashes after the
//! durable execution has been started but before the reschedule that writes the
//! updated metadata completes, the counter will not be incremented for that tick.
//! On recovery the same `execution_id` will be attempted again; the duplicate
//! insert is silently ignored via `ExecutionAlreadyExists` handling so no double
//! execution occurs, but the counter will be one behind.

use crate::durable::DurableScheduler;
use crate::error::SchedulerError;
use crate::registry::DurableExecution;
use async_trait::async_trait;
use serde_json::json;
use std::marker::PhantomData;
use std::sync::Arc;
use zart_core::types::ExecutionStatus;
use zart_scheduler::task::{CompletionHandler, SchedulerTaskError, TaskInstance};
use zart_scheduler::{OnComplete, ScheduledTask};

/// Determines what happens when a new occurrence fires while a previous
/// execution is still running.
#[derive(Debug, Clone, Default)]
pub enum OverlapPolicy {
    #[default]
    /// Do nothing if a non-terminal execution with the same ID already exists.
    SkipIfRunning,
    /// Cancel the running execution and start a fresh one.
    CancelAndRestart,
    /// Always start a new execution regardless of overlap.
    AlwaysStart,
}

/// A scheduled task that starts a [`DurableExecution`] on every occurrence.
///
/// Registered via [`crate::WorkerBuilder::register_recurring_durable`].
pub struct RecurringDurableTask<H: DurableExecution> {
    /// The task name used when the durable handler was registered.
    pub(crate) handler_name: String,
    /// Template for building the execution ID. `{occurrence}` is replaced with
    /// the current occurrence counter.
    pub(crate) id_template: String,
    /// What to do when a previous execution is still running.
    pub(crate) overlap: OverlapPolicy,
    /// The scheduler used to start / query / cancel executions.
    pub(crate) scheduler: Arc<DurableScheduler>,
    pub(crate) _marker: PhantomData<H>,
}

#[async_trait]
impl<H: DurableExecution + Send + Sync> ScheduledTask for RecurringDurableTask<H> {
    async fn execute(
        &self,
        instance: &TaskInstance,
    ) -> Result<Box<dyn CompletionHandler>, SchedulerTaskError> {
        // Read the occurrence counter from metadata (default 0).
        let occurrence: u64 = instance
            .metadata
            .get("occurrence")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        let execution_id = self
            .id_template
            .replace("{occurrence}", &occurrence.to_string());

        match &self.overlap {
            OverlapPolicy::AlwaysStart => {
                self.start_execution(&execution_id, &instance.data).await?;
            }
            OverlapPolicy::SkipIfRunning => {
                // No TOCTOU race here: the scheduler holds a row-level lock on
                // the `zart_tasks` row for this recurring task. Only one worker
                // can execute this method for a given tick at a time, so the
                // `is_running` check and the subsequent `start_execution` are
                // effectively serialized.
                if self.is_running(&execution_id).await? {
                    // Skip — don't increment the occurrence counter.
                    return Ok(OnComplete::done());
                }
                self.start_execution(&execution_id, &instance.data).await?;
            }
            OverlapPolicy::CancelAndRestart => {
                // Cancel any non-terminal execution with this ID.
                // Only swallow ExecutionNotFound (no prior run); propagate all other errors.
                match self.scheduler.cancel(&execution_id).await {
                    Ok(_) | Err(SchedulerError::ExecutionNotFound(_)) => {}
                    Err(e) => return Err(SchedulerTaskError::Failed(e.to_string())),
                }
                self.start_execution(&execution_id, &instance.data).await?;
            }
        }

        // Build a completion handler that merges the incremented occurrence
        // back into the task metadata via `ops.set_metadata`.
        let next_occurrence = occurrence + 1;
        Ok(Box::new(RecurringCompletion { next_occurrence }))
    }
}

impl<H: DurableExecution + Send + Sync> RecurringDurableTask<H> {
    /// Return `true` if an execution with `execution_id` is in a non-terminal state.
    async fn is_running(&self, execution_id: &str) -> Result<bool, SchedulerTaskError> {
        match self.scheduler.status(execution_id).await {
            Ok(record) => Ok(matches!(
                record.status,
                ExecutionStatus::Scheduled | ExecutionStatus::Running
            )),
            Err(SchedulerError::ExecutionNotFound(_)) => Ok(false),
            Err(e) => Err(SchedulerTaskError::Failed(e.to_string())),
        }
    }

    /// Start the durable execution, ignoring `ExecutionAlreadyExists` for idempotency.
    async fn start_execution(
        &self,
        execution_id: &str,
        data: &serde_json::Value,
    ) -> Result<(), SchedulerTaskError> {
        match self
            .scheduler
            .start(execution_id, &self.handler_name, data.clone())
            .await
        {
            Ok(_) => Ok(()),
            Err(SchedulerError::ExecutionAlreadyExists(_, _)) => Ok(()),
            Err(e) => Err(SchedulerTaskError::Failed(e.to_string())),
        }
    }
}

/// Completion handler that sets the updated occurrence counter in task metadata.
struct RecurringCompletion {
    next_occurrence: u64,
}

#[async_trait]
impl CompletionHandler for RecurringCompletion {
    async fn complete(
        self: Box<Self>,
        mut ops: zart_scheduler::ExecutionOps,
        recurrence: Option<&zart_scheduler::Recurrence>,
        execution_time: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), zart_scheduler::StorageError> {
        // Merge the incremented occurrence into the task metadata.
        ops.set_metadata(json!({ "occurrence": self.next_occurrence }));

        // Delegate to the standard OnComplete behaviour (auto-reschedule if
        // recurrence is set).
        let inner = OnComplete {
            result: None,
            schedule_next: vec![],
        };
        Box::new(inner)
            .complete(ops, recurrence, execution_time)
            .await
    }
}
