//! Task-queue operations trait — owned by `zart-scheduler`.
//!
//! [`TaskScheduler`] defines the lifecycle of task rows: schedule, poll,
//! complete, fail, cancel. Implementations backed by PostgreSQL live in this
//! crate (`PostgresTaskScheduler`); the `zart` crate consumes the trait
//! without providing its own implementation.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::PgConnection;

use crate::Recurrence;
use crate::StorageError;
use crate::types::{CompleteAndScheduleParams, FetchedTask, ScheduleAtParams, ScheduleResult};

/// Task-queue operations: schedule, poll, and lifecycle management.
///
/// Replaces the deprecated `Scheduler` trait. Existing code using `dyn Scheduler`
/// continues to work via the blanket impl in `lib.rs`.
#[async_trait]
pub trait TaskScheduler: Send + Sync {
    /// Schedule a task for immediate execution.
    async fn schedule_now(
        &self,
        task_id: &str,
        task_name: &str,
        data: serde_json::Value,
    ) -> Result<ScheduleResult, StorageError>;

    /// Schedule a task for execution at a specific point in time.
    async fn schedule_at(&self, params: ScheduleAtParams) -> Result<ScheduleResult, StorageError>;

    /// Schedule a recurring task that automatically reschedules based on the recurrence rule.
    async fn schedule_recurring(
        &self,
        task_id: &str,
        task_name: &str,
        recurrence: Recurrence,
        data: serde_json::Value,
    ) -> Result<ScheduleResult, StorageError> {
        self.schedule_at(ScheduleAtParams {
            task_id: task_id.to_string(),
            task_name: task_name.to_string(),
            execution_time: chrono::Utc::now(),
            data,
            recurrence: Some(recurrence),
            metadata: serde_json::Value::Null,
        })
        .await
    }

    /// Schedule a task within the caller's transaction.
    ///
    /// Default implementation returns `NotImplemented`.
    #[allow(unused_variables)]
    async fn schedule_at_in_tx(
        &self,
        conn: &mut PgConnection,
        params: ScheduleAtParams,
    ) -> Result<ScheduleResult, StorageError> {
        Err(StorageError::NotImplemented("schedule_at_in_tx"))
    }

    /// Poll for tasks due for execution using `SKIP LOCKED` semantics.
    async fn poll_due(
        &self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<FetchedTask>, StorageError>;

    /// Persist step progress between re-entries of a running task.
    async fn update_task_state(
        &self,
        task_id: &str,
        state: serde_json::Value,
        next_execution_time: DateTime<Utc>,
        lock_token: &str,
    ) -> Result<(), StorageError>;

    /// Reschedule a task within a caller-owned transaction.
    ///
    /// Default implementation returns `NotImplemented`.
    async fn update_task_state_in_tx(
        &self,
        conn: &mut PgConnection,
        task_id: &str,
        state: serde_json::Value,
        next_execution_time: DateTime<Utc>,
        lock_token: &str,
    ) -> Result<(), StorageError> {
        let _ = (conn, task_id, state, next_execution_time, lock_token);
        Err(StorageError::NotImplemented("update_task_state_in_tx"))
    }

    /// Mark a task as successfully completed (auto-commit, no transaction).
    ///
    /// Prefer `mark_completed_in_tx` when step SQL writes must be atomic with
    /// the completion update. This method acquires its own connection and
    /// commits immediately — use it only from non-transactional paths or tests.
    async fn mark_completed(
        &self,
        task_id: &str,
        result: Option<serde_json::Value>,
        lock_token: &str,
    ) -> Result<(), StorageError>;

    /// Mark a task as completed within a caller-owned transaction.
    ///
    /// Default implementation returns `NotImplemented`.
    async fn mark_completed_in_tx(
        &self,
        conn: &mut PgConnection,
        task_id: &str,
        result: Option<serde_json::Value>,
        lock_token: &str,
    ) -> Result<(), StorageError> {
        let _ = (conn, task_id, result, lock_token);
        Err(StorageError::NotImplemented("mark_completed_in_tx"))
    }

    /// Mark a task as failed, optionally rescheduling for retry.
    async fn mark_failed(
        &self,
        task_id: &str,
        error: &str,
        next_execution_time: Option<DateTime<Utc>>,
        lock_token: &str,
    ) -> Result<(), StorageError>;

    /// Mark a task as failed within a caller-owned transaction.
    ///
    /// Default implementation returns `NotImplemented`.
    async fn mark_failed_in_tx(
        &self,
        conn: &mut PgConnection,
        task_id: &str,
        error: &str,
        next_execution_time: Option<DateTime<Utc>>,
        lock_token: &str,
    ) -> Result<(), StorageError> {
        let _ = (conn, task_id, error, next_execution_time, lock_token);
        Err(StorageError::NotImplemented("mark_failed_in_tx"))
    }

    /// Cancel a scheduled task. Returns `true` if the task was found and cancelled.
    async fn cancel_task(&self, task_id: &str) -> Result<bool, StorageError>;

    /// Delete a task record permanently.
    async fn delete_task(&self, task_id: &str) -> Result<(), StorageError>;

    /// Begin a new database transaction.
    ///
    /// Returns a `'static` transaction — the connection is acquired from the
    /// underlying pool and is not tied to the `&self` borrow. This allows
    /// callers to hold the transaction independently of the scheduler reference.
    ///
    /// Default implementation returns `NotImplemented`.
    async fn begin(&self) -> Result<sqlx::Transaction<'static, sqlx::Postgres>, StorageError> {
        Err(StorageError::NotImplemented("begin"))
    }

    /// Reset tasks stuck in `picked_up` longer than `stale_timeout`.
    ///
    /// Default implementation returns `Ok(0)`.
    async fn recover_orphans(
        &self,
        stale_timeout: std::time::Duration,
    ) -> Result<usize, StorageError> {
        let _ = stale_timeout;
        Ok(0)
    }

    /// Extend the lease of a running task to prevent orphan recovery.
    ///
    /// Default implementation returns `Ok(false)`.
    async fn renew_lease(&self, _task_id: &str, _lock_token: &str) -> Result<bool, StorageError> {
        Ok(false)
    }

    /// Atomically complete one task and schedule a successor in a single transaction.
    ///
    /// Default implementation returns `NotImplemented`.
    async fn complete_and_schedule(
        &self,
        params: CompleteAndScheduleParams,
    ) -> Result<(), StorageError> {
        let _ = params;
        Err(StorageError::NotImplemented("complete_and_schedule"))
    }
}
