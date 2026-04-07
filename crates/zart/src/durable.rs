//! High-level durable execution entry point.
//!
//! [`DurableScheduler`] wraps the underlying [`Scheduler`] and provides
//! execution-aware operations: starting executions with idempotency keys,
//! querying status, and waiting for completion.

use crate::emit_metric;
use crate::error::SchedulerError;
#[cfg(feature = "metrics")]
use crate::metrics::EVENTS_DELIVERED_TOTAL;
use scheduler::{ExecutionRecord, ExecutionStatus, ScheduleAtParams, ScheduleResult, StorageBackend};
use std::sync::Arc;
use std::time::Duration;

// Maximum duration for `wait_with_timeout` as per the spec.
const MAX_WAIT_SECS: u64 = 30;

/// High-level entry point for durable executions.
///
/// Wraps the underlying scheduler backend and coordinates:
/// - inserting an execution record in `zart_executions`
/// - scheduling the root task in `zart_tasks`
/// - querying and waiting for execution completion
pub struct DurableScheduler {
    scheduler: Arc<dyn StorageBackend>,
}

impl DurableScheduler {
    /// Create a new `DurableScheduler`.
    pub fn new(scheduler: Arc<dyn StorageBackend>) -> Self {
        Self { scheduler }
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
        let run_id: String;

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
                    // reset_execution returns the new run_id directly.
                    run_id = self
                        .scheduler
                        .reset_execution(execution_id, payload.clone())
                        .await?;
                }
            }
        } else {
            // First time — insert the record.
            self.scheduler
                .start_execution(execution_id, task_name, payload.clone())
                .await?;
            run_id = format!("{execution_id}:run:0");
        }

        // Schedule the root task that drives the execution.
        // The task_id is "{run_id}:body:start" — deterministic and debuggable.
        let task_id = format!("{run_id}:body:start");
        let metadata = serde_json::json!({ "mode": "body", "run_id": run_id });
        let result = self
            .scheduler
            .schedule_at(ScheduleAtParams {
                task_id: task_id.clone(),
                task_name: task_name.to_string(),
                execution_time: chrono::Utc::now(),
                data: payload,
                recurrence: None,
                execution_id: Some(execution_id.to_string()),
                metadata,
            })
            .await
            .map_err(SchedulerError::Database)?;

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
    /// Atomically marks the event's step task completed with `payload` and
    /// schedules the next body segment. Races cleanly with the deadline worker:
    /// if the deadline already fired and the step task is no longer `scheduled`,
    /// returns [`SchedulerError::ExecutionNotFound`].
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::ExecutionNotFound`] if no scheduled
    /// wait_for_event step task was found for the given execution ID and event name.
    pub async fn offer_event(
        &self,
        execution_id: &str,
        event_name: &str,
        payload: serde_json::Value,
    ) -> Result<(), SchedulerError> {
        let result = self
            .scheduler
            .complete_event_step_and_schedule_body(execution_id, event_name, payload)
            .await;
        match result {
            Ok(true) => {
                emit_metric!(
                    EVENTS_DELIVERED_TOTAL
                        .with_label_values(&[event_name, "delivered"])
                        .inc()
                );
                Ok(())
            }
            Ok(false) => {
                emit_metric!(
                    EVENTS_DELIVERED_TOTAL
                        .with_label_values(&[event_name, "failed"])
                        .inc()
                );
                Err(SchedulerError::ExecutionNotFound(execution_id.to_string()))
            }
            Err(e) => {
                emit_metric!(
                    EVENTS_DELIVERED_TOTAL
                        .with_label_values(&[event_name, "failed"])
                        .inc()
                );
                Err(e.into())
            }
        }
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
