//! High-level durable execution entry point.
//!
//! [`DurableScheduler`] wraps the underlying [`Scheduler`] and provides
//! execution-aware operations: starting executions with idempotency keys,
//! querying status, and waiting for completion.

use crate::error::SchedulerError;
use crate::registry::TaskRegistry;
use scheduler::{DurableStorage, ExecutionRecord, ExecutionStatus, ScheduleResult, Scheduler};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

// Maximum duration for `wait_with_timeout` as per the spec.
const MAX_WAIT_SECS: u64 = 30;

/// High-level entry point for durable executions.
///
/// Wraps the underlying [`Scheduler`] and coordinates:
/// - inserting an execution record in `zart_executions`
/// - scheduling the root task in `zart_tasks`
/// - querying and waiting for execution completion
pub struct DurableScheduler<S: Scheduler + DurableStorage> {
    scheduler: Arc<S>,
    #[allow(dead_code)]
    registry: Arc<TaskRegistry<S>>,
}

impl<S: Scheduler + DurableStorage> DurableScheduler<S> {
    /// Create a new `DurableScheduler`.
    pub fn new(scheduler: Arc<S>, registry: Arc<TaskRegistry<S>>) -> Self {
        Self {
            scheduler,
            registry,
        }
    }

    /// Start a new durable execution with a typed input value.
    ///
    /// The `execution_id` is an idempotency key: if an execution with that ID
    /// already exists the call is a no-op (the existing row is unchanged) and
    /// the same [`ScheduleResult`] shape is returned.
    pub async fn start_typed<T: serde::Serialize>(
        &self,
        execution_id: &str,
        task_name: &str,
        data: &T,
    ) -> Result<ScheduleResult, SchedulerError> {
        let payload = serde_json::to_value(data)?;
        self.start(execution_id, task_name, payload).await
    }

    /// Start a new durable execution with a raw JSON payload.
    ///
    /// If an execution with this ID already exists and is in a terminal state
    /// (completed, failed, cancelled), it will be reset to "scheduled" so it
    /// can be retried. If it exists and is **not** in a terminal state,
    /// [`SchedulerError::ExecutionAlreadyExists`] is returned.
    pub async fn start(
        &self,
        execution_id: &str,
        task_name: &str,
        payload: serde_json::Value,
    ) -> Result<ScheduleResult, SchedulerError> {
        // Check if execution already exists.
        if let Some(existing) = self.scheduler.get_execution(execution_id).await? {
            match existing.status {
                // Still running — don't create a duplicate.
                ExecutionStatus::Scheduled | ExecutionStatus::Running => {
                    return Err(SchedulerError::ExecutionAlreadyExists(
                        execution_id.to_string(),
                        existing.status,
                    ));
                }
                // Terminal state — reset so we can retry.
                ExecutionStatus::Completed
                | ExecutionStatus::Failed
                | ExecutionStatus::Cancelled => {
                    self.scheduler.reset_execution(execution_id, payload.clone()).await?;
                }
            }
        } else {
            // First time — insert the record.
            self.scheduler
                .start_execution(execution_id, task_name, payload.clone())
                .await?;
        }

        // Schedule the root task that drives the execution.
        let task_id = Uuid::new_v4().to_string();
        let result = self
            .scheduler
            .schedule_now(&task_id, task_name, payload, Some(execution_id))
            .await?;

        Ok(result)
    }

    /// Return the current status of a durable execution.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::ExecutionNotFound`] if no execution with the
    /// given ID exists.
    pub async fn status(&self, execution_id: &str) -> Result<ExecutionRecord, SchedulerError> {
        self.scheduler
            .get_execution(execution_id)
            .await?
            .ok_or_else(|| SchedulerError::ExecutionNotFound(execution_id.to_string()))
    }

    /// Block until the execution reaches a terminal state (completed, failed,
    /// or cancelled), polling every `poll_interval` (default: 500 ms).
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::WaitTimedOut`] if `timeout` elapses before
    /// the execution finishes.
    pub async fn wait(
        &self,
        execution_id: &str,
        timeout: Duration,
        poll_interval: Option<Duration>,
    ) -> Result<ExecutionRecord, SchedulerError> {
        let interval = poll_interval.unwrap_or(Duration::from_millis(500));
        let deadline = tokio::time::Instant::now() + timeout;

        loop {
            let record = self.status(execution_id).await?;

            match record.status {
                ExecutionStatus::Completed
                | ExecutionStatus::Failed
                | ExecutionStatus::Cancelled => return Ok(record),
                _ => {}
            }

            if tokio::time::Instant::now() >= deadline {
                return Err(SchedulerError::WaitTimedOut(execution_id.to_string()));
            }

            tokio::time::sleep(interval).await;
        }
    }

    /// Like [`wait`](Self::wait) but caps the maximum wait at 30 seconds.
    ///
    /// Returns [`SchedulerError::WaitTimedOut`] if the execution does not
    /// reach a terminal state within `max_duration` (or 30 s, whichever is less).
    pub async fn wait_with_timeout(
        &self,
        execution_id: &str,
        max_duration: Duration,
    ) -> Result<ExecutionRecord, SchedulerError> {
        let capped = max_duration.min(Duration::from_secs(MAX_WAIT_SECS));
        self.wait(execution_id, capped, None).await
    }

    /// Cancel a running or scheduled durable execution.
    ///
    /// Returns `true` if the execution was found and cancelled, `false` if it
    /// was already in a terminal state or did not exist.
    pub async fn cancel(&self, execution_id: &str) -> Result<bool, SchedulerError> {
        Ok(self.scheduler.cancel_execution(execution_id).await?)
    }

    /// Deliver an external event to a waiting execution.
    ///
    /// Atomically injects `payload` into the execution's task state under
    /// `event_name` and reschedules the task for immediate pickup.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::ExecutionNotFound`] if no scheduled task for
    /// the given execution ID was found (not waiting or does not exist).
    pub async fn offer_event(
        &self,
        execution_id: &str,
        event_name: &str,
        payload: serde_json::Value,
    ) -> Result<(), SchedulerError> {
        let found = self
            .scheduler
            .reschedule_with_event(execution_id, event_name, payload)
            .await?;
        if !found {
            return Err(SchedulerError::ExecutionNotFound(execution_id.to_string()));
        }
        Ok(())
    }

    /// List durable execution records with optional filters.
    ///
    /// Results are ordered by `scheduled_at DESC`.
    pub async fn list_executions(
        &self,
        status: Option<ExecutionStatus>,
        task_name: Option<String>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<ExecutionRecord>, SchedulerError> {
        Ok(self
            .scheduler
            .list_executions(status, task_name.as_deref(), limit, offset)
            .await?)
    }
}
