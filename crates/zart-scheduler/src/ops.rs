//! Imperative execution operations available to a task during its execution slot.

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::PgConnection;
use std::sync::Arc;

use crate::error::StorageError;
use crate::store::TaskScheduler;
use crate::types::{ScheduleAtParams, ScheduleResult};

/// Scheduler operations available to a [`ScheduledTask`](crate::ScheduledTask) during execution.
///
/// All methods write through the `PgConnection` owned by the worker for this
/// task slot. The worker commits the connection when `execute` returns `Ok(())`,
/// or rolls back on `Err`. This means completion, rescheduling, and any chained
/// task scheduling are all atomic within the same transaction.
///
/// # Usage
///
/// Tasks call `ops.complete(result)` when done, `ops.reschedule(at)` for an
/// intentional re-run, and `ops.schedule(params)` to enqueue a follow-up task —
/// all within the same transaction.
///
/// If `execute` returns `Ok(())` without calling any of these methods, the
/// worker defaults to `ops.complete(None)` before committing.
pub struct ExecutionOps<'a> {
    conn: &'a mut PgConnection,
    scheduler: Arc<dyn TaskScheduler>,
    task_id: &'a str,
    lock_token: &'a str,
    /// Tracks whether `complete` or `reschedule` was called.
    /// The worker defaults to `complete(None)` on `Ok(())` if neither was called.
    outcome_set: bool,
}

impl<'a> ExecutionOps<'a> {
    pub(crate) fn new(
        conn: &'a mut PgConnection,
        scheduler: Arc<dyn TaskScheduler>,
        task_id: &'a str,
        lock_token: &'a str,
    ) -> Self {
        Self {
            conn,
            scheduler,
            task_id,
            lock_token,
            outcome_set: false,
        }
    }

    /// Whether `complete` or `reschedule` has already been called.
    pub(crate) fn outcome_set(&self) -> bool {
        self.outcome_set
    }

    /// Returns a clone of the scheduler associated with these operations.
    pub fn scheduler(&self) -> Arc<dyn TaskScheduler> {
        self.scheduler.clone()
    }

    /// Mark this task completed with an optional result value.
    ///
    /// Writes the completion record to the shared connection. The worker commits
    /// after `execute` returns `Ok(())`.
    pub async fn complete(&mut self, result: Option<Value>) -> Result<(), StorageError> {
        self.scheduler
            .mark_completed_in_tx(self.conn, self.task_id, result, self.lock_token)
            .await?;
        self.outcome_set = true;
        Ok(())
    }

    /// Reschedule this task for a future execution time.
    ///
    /// Use for intentional delays or retry-with-backoff inside the task.
    /// The new execution is written to the shared connection and committed
    /// atomically with any other writes in this slot.
    pub async fn reschedule(&mut self, at: DateTime<Utc>) -> Result<(), StorageError> {
        self.scheduler
            .update_task_state_in_tx(self.conn, self.task_id, Value::Null, at, self.lock_token)
            .await?;
        self.outcome_set = true;
        Ok(())
    }

    /// Schedule a new task within the same transaction (task chaining).
    ///
    /// Allows one task to atomically enqueue a successor before completing.
    /// The successor is inserted to the shared connection and committed together
    /// with the current task's completion.
    pub async fn schedule(
        &mut self,
        params: ScheduleAtParams,
    ) -> Result<ScheduleResult, StorageError> {
        self.scheduler.schedule_at_in_tx(self.conn, params).await
    }
}
