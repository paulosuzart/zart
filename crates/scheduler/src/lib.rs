//! Core scheduling primitives for Zart.
//!
//! This crate provides the [`Scheduler`] trait and associated types that define
//! the contract for task scheduling, polling, and storage backends.
//!
//! # Architecture
//!
//! The scheduler operates at Layer 1 of the Zart stack: individual tasks.
//! Each task is a row in the database that gets picked up by a worker via
//! `SKIP LOCKED`, executed, and either completed, failed, or rescheduled.
//!
//! # Examples
//!
//! ```rust
//! use scheduler::{Scheduler, ScheduleResult};
//! ```

pub mod error;
pub mod recurrence;
pub mod types;

#[cfg(feature = "postgres")]
pub mod postgres;

pub use error::StorageError;
pub use recurrence::Recurrence;
pub use types::{FetchedTask, ScheduleResult, TaskStatus};

#[cfg(feature = "postgres")]
pub use postgres::PostgresScheduler;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

/// A task scheduler that manages task lifecycle: schedule, poll, complete, fail, cancel.
///
/// Implementors provide the concrete storage backend (PostgreSQL, SQLite, etc.).
/// The skip-lock polling mechanism ensures tasks are never processed by two workers
/// simultaneously.
#[async_trait]
pub trait Scheduler: Send + Sync {
    /// Schedule a task for immediate execution.
    ///
    /// Uses the current time as the `execution_time`.
    async fn schedule_now(
        &self,
        task_id: &str,
        task_name: &str,
        data: serde_json::Value,
        execution_id: Option<&str>,
    ) -> Result<ScheduleResult, StorageError>;

    /// Schedule a task for execution at a specific point in time.
    async fn schedule_at(
        &self,
        task_id: &str,
        task_name: &str,
        execution_time: DateTime<Utc>,
        data: serde_json::Value,
        recurrence: Option<Recurrence>,
        execution_id: Option<&str>,
    ) -> Result<ScheduleResult, StorageError>;

    /// Poll for tasks that are due for execution.
    ///
    /// Uses `SELECT ... FOR UPDATE SKIP LOCKED` semantics so that multiple workers
    /// can poll concurrently without duplicate processing.
    ///
    /// Returns up to `limit` tasks whose `execution_time <= now`.
    async fn poll_due(
        &self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<FetchedTask>, StorageError>;

    /// Update the state of a task that is currently running.
    ///
    /// Used by the durable execution runtime to persist step progress between re-entries.
    async fn update_task_state(
        &self,
        task_id: &str,
        state: serde_json::Value,
        next_execution_time: DateTime<Utc>,
        lock_token: &str,
    ) -> Result<(), StorageError>;

    /// Mark a task as successfully completed.
    async fn mark_completed(
        &self,
        task_id: &str,
        result: Option<serde_json::Value>,
        lock_token: &str,
    ) -> Result<(), StorageError>;

    /// Mark a task as failed. Optionally reschedules it for retry.
    async fn mark_failed(
        &self,
        task_id: &str,
        error: &str,
        next_execution_time: Option<DateTime<Utc>>,
        lock_token: &str,
    ) -> Result<(), StorageError>;

    /// Cancel a scheduled task. Returns `true` if the task was found and cancelled.
    async fn cancel_task(&self, task_id: &str) -> Result<bool, StorageError>;

    /// Delete a task record permanently.
    async fn delete_task(&self, task_id: &str) -> Result<(), StorageError>;

    /// Run database migrations required by this backend.
    async fn run_migrations(&self) -> Result<(), StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    /// A minimal in-memory scheduler stub for unit testing.
    struct StubScheduler;

    #[async_trait]
    impl Scheduler for StubScheduler {
        async fn schedule_now(
            &self,
            task_id: &str,
            _task_name: &str,
            _data: serde_json::Value,
            _execution_id: Option<&str>,
        ) -> Result<ScheduleResult, StorageError> {
            Ok(ScheduleResult {
                task_id: task_id.to_string(),
                execution_time: Utc::now(),
            })
        }

        async fn schedule_at(
            &self,
            task_id: &str,
            _task_name: &str,
            execution_time: DateTime<Utc>,
            _data: serde_json::Value,
            _recurrence: Option<Recurrence>,
            _execution_id: Option<&str>,
        ) -> Result<ScheduleResult, StorageError> {
            Ok(ScheduleResult {
                task_id: task_id.to_string(),
                execution_time,
            })
        }

        async fn poll_due(
            &self,
            _now: DateTime<Utc>,
            _limit: usize,
        ) -> Result<Vec<FetchedTask>, StorageError> {
            Ok(vec![])
        }

        async fn update_task_state(
            &self,
            _task_id: &str,
            _state: serde_json::Value,
            _next_execution_time: DateTime<Utc>,
            _lock_token: &str,
        ) -> Result<(), StorageError> {
            Ok(())
        }

        async fn mark_completed(
            &self,
            _task_id: &str,
            _result: Option<serde_json::Value>,
            _lock_token: &str,
        ) -> Result<(), StorageError> {
            Ok(())
        }

        async fn mark_failed(
            &self,
            _task_id: &str,
            _error: &str,
            _next_execution_time: Option<DateTime<Utc>>,
            _lock_token: &str,
        ) -> Result<(), StorageError> {
            Ok(())
        }

        async fn cancel_task(&self, _task_id: &str) -> Result<bool, StorageError> {
            Ok(true)
        }

        async fn delete_task(&self, _task_id: &str) -> Result<(), StorageError> {
            Ok(())
        }

        async fn run_migrations(&self) -> Result<(), StorageError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn schedule_now_returns_task_id() {
        let scheduler = Arc::new(StubScheduler);
        let result = scheduler
            .schedule_now("task-1", "my-task", serde_json::json!({}), None)
            .await
            .unwrap();
        assert_eq!(result.task_id, "task-1");
    }

    #[tokio::test]
    async fn poll_due_returns_empty_for_stub() {
        let scheduler = Arc::new(StubScheduler);
        let tasks = scheduler.poll_due(Utc::now(), 10).await.unwrap();
        assert!(tasks.is_empty());
    }

    #[tokio::test]
    async fn cancel_task_returns_true_for_stub() {
        let scheduler = Arc::new(StubScheduler);
        let cancelled = scheduler.cancel_task("task-1").await.unwrap();
        assert!(cancelled);
    }
}
