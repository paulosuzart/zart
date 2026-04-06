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
pub use types::{
    CompleteAndScheduleParams, ExecutionRecord, ExecutionStatus, FetchedTask, ScheduleAtParams,
    ScheduleResult, StepLookup, TaskStatus,
};

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
        params: ScheduleAtParams,
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

    /// Reset tasks that have been stuck in `picked_up` state longer than `stale_timeout`.
    ///
    /// A task becomes an orphan when the worker that locked it crashes without
    /// releasing the lock. This method resets orphans back to `scheduled` so they
    /// can be picked up again.
    ///
    /// Returns the number of tasks recovered.
    async fn recover_orphans(
        &self,
        stale_timeout: std::time::Duration,
    ) -> Result<usize, StorageError> {
        let _ = stale_timeout;
        Ok(0)
    }

    /// Extend the lease of a task by updating `locked_at` to the current time.
    ///
    /// Returns `true` if the lease was renewed (task exists and lock token matches).
    /// Returns `false` if the task was not found, the lock token doesn't match,
    /// or the task is no longer in `picked_up` state.
    ///
    /// Used by the worker's background heartbeat loop to prevent orphan recovery
    /// from reclaiming legitimately long-running tasks.
    async fn renew_lease(&self, _task_id: &str, _lock_token: &str) -> Result<bool, StorageError> {
        Ok(false)
    }

    /// Atomically mark one task as completed and insert a new task in a single transaction.
    ///
    /// Used by the execution model to complete a step/coordinator/sleep task and
    /// schedule the next body segment without a gap between the two operations.
    async fn complete_and_schedule(
        &self,
        params: CompleteAndScheduleParams,
    ) -> Result<(), StorageError> {
        let _ = params;
        Err(StorageError::NotImplemented("complete_and_schedule"))
    }
}

/// Storage operations for durable executions and the per-row step model.
///
/// Extends [`Scheduler`] for backends that support the `zart_executions` table
/// and the execution-model step rows. Implement this alongside [`Scheduler`]
/// to enable [`DurableScheduler`], [`Worker`], and [`TaskContext`] in their
/// full durable-execution mode.
///
/// A plain task-queue backend only needs to implement [`Scheduler`].
#[async_trait]
pub trait DurableStorage: Send + Sync {
    /// Insert a new durable execution record into `zart_executions`.
    async fn start_execution(
        &self,
        execution_id: &str,
        task_name: &str,
        payload: serde_json::Value,
    ) -> Result<(), StorageError> {
        let _ = (execution_id, task_name, payload);
        Err(StorageError::NotImplemented("start_execution"))
    }

    /// Mark a durable execution as successfully completed.
    async fn complete_execution(
        &self,
        execution_id: &str,
        result: serde_json::Value,
    ) -> Result<(), StorageError> {
        let _ = (execution_id, result);
        Err(StorageError::NotImplemented("complete_execution"))
    }

    /// Mark a durable execution as failed.
    async fn fail_execution(&self, execution_id: &str) -> Result<(), StorageError> {
        let _ = execution_id;
        Err(StorageError::NotImplemented("fail_execution"))
    }

    /// Fetch a durable execution record by ID.
    async fn get_execution(
        &self,
        execution_id: &str,
    ) -> Result<Option<ExecutionRecord>, StorageError> {
        let _ = execution_id;
        Err(StorageError::NotImplemented("get_execution"))
    }

    /// Cancel a running or scheduled durable execution.
    async fn cancel_execution(&self, execution_id: &str) -> Result<bool, StorageError> {
        let _ = execution_id;
        Err(StorageError::NotImplemented("cancel_execution"))
    }

    /// List durable execution records with optional filters.
    async fn list_executions(
        &self,
        status: Option<ExecutionStatus>,
        task_name: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<ExecutionRecord>, StorageError> {
        let _ = (status, task_name, limit, offset);
        Err(StorageError::NotImplemented("list_executions"))
    }

    /// Atomically complete a wait_for_event step task and schedule the next body segment.
    ///
    /// Finds the step task matching `execution_id` + `event_name` that is still
    /// `scheduled` (i.e. the deadline has not yet fired), marks it completed with
    /// `payload`, and inserts the next body task (derived from the step task's metadata).
    ///
    /// Returns `true` if the step was found and completed, `false` if not found
    /// (event delivered too late — deadline fired and worker already holds the task).
    async fn complete_event_step_and_schedule_body(
        &self,
        execution_id: &str,
        event_name: &str,
        payload: serde_json::Value,
    ) -> Result<bool, StorageError> {
        let _ = (execution_id, event_name, payload);
        Err(StorageError::NotImplemented(
            "complete_event_step_and_schedule_body",
        ))
    }

    /// Reset a terminal execution so it can be retried.
    async fn reset_execution(
        &self,
        execution_id: &str,
        payload: serde_json::Value,
    ) -> Result<(), StorageError> {
        let _ = (execution_id, payload);
        Err(StorageError::NotImplemented("reset_execution"))
    }

    /// Look up a step task by `execution_id` + `step_name`.
    async fn get_step_status(
        &self,
        execution_id: &str,
        step_name: &str,
    ) -> Result<Option<StepLookup>, StorageError> {
        let _ = (execution_id, step_name);
        Err(StorageError::NotImplemented("get_step_status"))
    }

    /// Check whether all wait_all children are completed.
    async fn check_wait_all_children(
        &self,
        wait_for_task_ids: &[String],
    ) -> Result<Vec<(String, serde_json::Value)>, StorageError> {
        let _ = wait_for_task_ids;
        Err(StorageError::NotImplemented("check_wait_all_children"))
    }
}

/// Combined backend trait for schedulers that support both task-queue and
/// durable-execution storage.
///
/// A blanket impl covers every concrete type that already satisfies both
/// [`Scheduler`] and [`DurableStorage`], so backends don't need to implement
/// this trait explicitly — they just implement the two component traits.
///
/// Use `Arc<dyn StorageBackend>` wherever you need a type-erased backend.
pub trait StorageBackend: Scheduler + DurableStorage + Send + Sync {}
impl<T: Scheduler + DurableStorage + Send + Sync> StorageBackend for T {}

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
            params: ScheduleAtParams,
        ) -> Result<ScheduleResult, StorageError> {
            Ok(ScheduleResult {
                task_id: params.task_id,
                execution_time: params.execution_time,
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

    struct StubDurableStorage;

    #[async_trait]
    impl Scheduler for StubDurableStorage {
        async fn schedule_now(
            &self,
            task_id: &str,
            _: &str,
            _: serde_json::Value,
            _: Option<&str>,
        ) -> Result<ScheduleResult, StorageError> {
            Ok(ScheduleResult {
                task_id: task_id.to_string(),
                execution_time: Utc::now(),
            })
        }
        async fn schedule_at(
            &self,
            params: ScheduleAtParams,
        ) -> Result<ScheduleResult, StorageError> {
            Ok(ScheduleResult {
                task_id: params.task_id,
                execution_time: params.execution_time,
            })
        }
        async fn poll_due(
            &self,
            _: DateTime<Utc>,
            _: usize,
        ) -> Result<Vec<FetchedTask>, StorageError> {
            Ok(vec![])
        }
        async fn update_task_state(
            &self,
            _: &str,
            _: serde_json::Value,
            _: DateTime<Utc>,
            _: &str,
        ) -> Result<(), StorageError> {
            Ok(())
        }
        async fn mark_completed(
            &self,
            _: &str,
            _: Option<serde_json::Value>,
            _: &str,
        ) -> Result<(), StorageError> {
            Ok(())
        }
        async fn mark_failed(
            &self,
            _: &str,
            _: &str,
            _: Option<DateTime<Utc>>,
            _: &str,
        ) -> Result<(), StorageError> {
            Ok(())
        }
        async fn cancel_task(&self, _: &str) -> Result<bool, StorageError> {
            Ok(true)
        }
        async fn delete_task(&self, _: &str) -> Result<(), StorageError> {
            Ok(())
        }
        async fn run_migrations(&self) -> Result<(), StorageError> {
            Ok(())
        }
    }

    impl DurableStorage for StubDurableStorage {}

    #[tokio::test]
    async fn complete_event_step_and_schedule_body_not_implemented_by_default() {
        let storage = StubDurableStorage;
        let result = storage
            .complete_event_step_and_schedule_body("exec-1", "approval", serde_json::json!({}))
            .await;
        assert!(matches!(result, Err(StorageError::NotImplemented(_))));
    }
}
