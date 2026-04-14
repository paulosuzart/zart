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
pub mod pause_storage;
pub mod recurrence;
pub mod types;

#[cfg(feature = "postgres")]
pub mod postgres;

pub use error::StorageError;
pub use recurrence::Recurrence;
pub use types::{
    CompleteAndScheduleParams, CompleteStepAndScheduleBodyParams, CompleteStepNoResumeParams,
    CompleteWaitGroupChildParams, EventDeliveryResult, ExecutionRecord, ExecutionRunRecord,
    ExecutionSortField, ExecutionStats, ExecutionStatus, ExecutionTrigger,
    FailWaitGroupChildParams, FetchedTask, ListExecutionsParams, RescheduleStepForRetryParams,
    ScheduleAtParams, ScheduleResult, ScheduleStepParams, SortOrder, StepAttemptRow,
    StepAttemptStatus, StepKind, StepLookup, StepResultKind, StepRow, StepStatus, TaskStatus,
    UpsertWaitGroupStepParams,
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
    ) -> Result<ScheduleResult, StorageError>;

    /// Schedule a task for execution at a specific point in time.
    async fn schedule_at(&self, params: ScheduleAtParams) -> Result<ScheduleResult, StorageError>;

    /// Schedule a task within the caller's transaction.
    ///
    /// The caller is responsible for committing or rolling back the transaction.
    /// Default implementation returns `NotImplemented`.
    #[allow(unused_variables)]
    async fn schedule_at_in_tx(
        &self,
        conn: &mut sqlx::PgConnection,
        params: ScheduleAtParams,
    ) -> Result<ScheduleResult, StorageError> {
        Err(StorageError::NotImplemented("schedule_at_in_tx"))
    }

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

    /// Begin a new database transaction.
    ///
    /// Used by internal methods that need to coordinate multiple writes
    /// in a single transaction.
    ///
    /// # Lifetime note
    ///
    /// The returned `Transaction<'_, Postgres>` borrows from `&self`. Callers
    /// must commit or roll back the transaction before dropping the borrow.
    /// This method is intended only for short-lived internal coordination
    /// (e.g. `DurableScheduler::start`). Do not store the transaction beyond
    /// the calling scope.
    async fn begin(&self) -> Result<sqlx::Transaction<'_, sqlx::Postgres>, StorageError> {
        Err(StorageError::NotImplemented("begin"))
    }

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
/// to enable `DurableScheduler`, `Worker`, and `TaskContext` in their
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

    /// Insert a new durable execution record within the caller's transaction.
    ///
    /// The caller is responsible for committing or rolling back the transaction.
    /// Default implementation returns `NotImplemented`.
    #[allow(unused_variables)]
    async fn start_execution_in_tx(
        &self,
        conn: &mut sqlx::PgConnection,
        execution_id: &str,
        task_name: &str,
        payload: serde_json::Value,
    ) -> Result<(), StorageError> {
        Err(StorageError::NotImplemented("start_execution_in_tx"))
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
        params: ListExecutionsParams,
    ) -> Result<Vec<ExecutionRecord>, StorageError> {
        let _ = params;
        Err(StorageError::NotImplemented("list_executions"))
    }

    /// Deliver an external event to a waiting execution.
    ///
    /// Backends should atomically attempt to complete the matching
    /// `wait_for_event` step and schedule the next body task.
    /// The return value distinguishes successful delivery, duplicate delivery,
    /// and missing registration.
    async fn deliver_event(
        &self,
        execution_id: &str,
        event_name: &str,
        payload: serde_json::Value,
    ) -> Result<EventDeliveryResult, StorageError> {
        let _ = (execution_id, event_name, payload);
        Err(StorageError::NotImplemented("deliver_event"))
    }

    /// Back-compat shim: completes a wait_for_event step and schedules body.
    ///
    /// New callers should use `deliver_event`. This helper maps:
    /// - `Delivered` -> `true`
    /// - `AlreadyDelivered`/`NotRegistered` -> `false`
    async fn complete_event_step_and_schedule_body(
        &self,
        execution_id: &str,
        event_name: &str,
        payload: serde_json::Value,
    ) -> Result<bool, StorageError> {
        match self
            .deliver_event(execution_id, event_name, payload)
            .await?
        {
            EventDeliveryResult::Delivered => Ok(true),
            EventDeliveryResult::AlreadyDelivered | EventDeliveryResult::NotRegistered => Ok(false),
        }
    }

    /// Reset a terminal execution so it can be retried.
    ///
    /// Creates a new run at run_index+1, updates `current_run_id`, and returns
    /// the new `run_id` (e.g. `"{execution_id}:run:1"`).
    async fn reset_execution(
        &self,
        execution_id: &str,
        payload: serde_json::Value,
    ) -> Result<String, StorageError> {
        let _ = (execution_id, payload);
        Err(StorageError::NotImplemented("reset_execution"))
    }

    /// Look up a step by `run_id` + `step_name` in `zart_steps`.
    async fn get_step_status(
        &self,
        run_id: &str,
        step_name: &str,
    ) -> Result<Option<StepLookup>, StorageError> {
        let _ = (run_id, step_name);
        Err(StorageError::NotImplemented("get_step_status"))
    }

    /// Get the current `run_id` for an execution.
    async fn get_current_run_id(&self, execution_id: &str) -> Result<Option<String>, StorageError> {
        let _ = execution_id;
        Err(StorageError::NotImplemented("get_current_run_id"))
    }

    /// List all runs for an execution, ordered by `run_index ASC`.
    async fn list_runs(&self, execution_id: &str) -> Result<Vec<ExecutionRunRecord>, StorageError> {
        let _ = execution_id;
        Err(StorageError::NotImplemented("list_runs"))
    }

    /// Check whether all wait_all children are completed.
    async fn check_wait_all_children(
        &self,
        wait_for_task_ids: &[String],
    ) -> Result<Vec<(String, serde_json::Value)>, StorageError> {
        let _ = wait_for_task_ids;
        Err(StorageError::NotImplemented("check_wait_all_children"))
    }

    /// Look up a step by run_id + step_name.
    async fn get_step(
        &self,
        run_id: &str,
        step_name: &str,
    ) -> Result<Option<StepRow>, StorageError> {
        let _ = (run_id, step_name);
        Err(StorageError::NotImplemented("get_step"))
    }

    /// List all steps for a run.
    async fn list_steps(&self, run_id: &str) -> Result<Vec<StepRow>, StorageError> {
        let _ = run_id;
        Err(StorageError::NotImplemented("list_steps"))
    }

    /// Upsert (insert-if-absent) a wait-group step row.
    ///
    /// This is idempotent and safe on body replay.
    async fn upsert_wait_group_step(
        &self,
        params: UpsertWaitGroupStepParams,
    ) -> Result<(), StorageError> {
        let _ = params;
        Err(StorageError::NotImplemented("upsert_wait_group_step"))
    }

    /// Complete a wait-group child and atomically decrement the parent group's
    /// `wg_remaining`. If this child reaches `wg_threshold`, the backend should
    /// also insert the next body task in the same transaction.
    async fn complete_wait_group_child(
        &self,
        params: CompleteWaitGroupChildParams,
    ) -> Result<bool, StorageError> {
        let _ = params;
        Err(StorageError::NotImplemented("complete_wait_group_child"))
    }

    /// Record a wait-group child failure with compare-and-set semantics.
    ///
    /// Returns `true` only for the first failing child that flips
    /// `wg_first_failed` from false to true.
    async fn fail_wait_group_child(
        &self,
        params: FailWaitGroupChildParams,
    ) -> Result<bool, StorageError> {
        let _ = params;
        Err(StorageError::NotImplemented("fail_wait_group_child"))
    }

    /// Recover wait-group orphans where the group has already triggered but the
    /// corresponding body task was never inserted.
    ///
    /// Returns the number of recovered body tasks inserted.
    async fn recover_wait_group_orphans(&self) -> Result<usize, StorageError> {
        Err(StorageError::NotImplemented("recover_wait_group_orphans"))
    }

    /// Insert a task row and a step row atomically.
    async fn schedule_step(
        &self,
        params: ScheduleStepParams,
    ) -> Result<ScheduleResult, StorageError> {
        let _ = params;
        Err(StorageError::NotImplemented("schedule_step"))
    }

    /// Atomically complete a step+task, record the attempt, and schedule the next body task.
    async fn complete_step_and_schedule_body(
        &self,
        params: CompleteStepAndScheduleBodyParams,
    ) -> Result<(), StorageError> {
        let _ = params;
        Err(StorageError::NotImplemented(
            "complete_step_and_schedule_body",
        ))
    }

    /// Complete a step and schedule the next body task within the caller's transaction.
    ///
    /// The caller is responsible for committing or rolling back the transaction.
    /// Default implementation returns `NotImplemented`.
    #[allow(unused_variables)]
    async fn complete_step_and_schedule_body_in_tx(
        &self,
        conn: &mut sqlx::PgConnection,
        params: CompleteStepAndScheduleBodyParams,
    ) -> Result<(), StorageError> {
        Err(StorageError::NotImplemented(
            "complete_step_and_schedule_body_in_tx",
        ))
    }

    /// Atomically complete a step+task and record the attempt (no body continuation).
    ///
    /// Used for wait_all children — the coordinator polls and schedules the body when all are done.
    async fn complete_step_no_resume(
        &self,
        params: CompleteStepNoResumeParams,
    ) -> Result<(), StorageError> {
        let _ = params;
        Err(StorageError::NotImplemented("complete_step_no_resume"))
    }

    /// Atomically record a failed step attempt, update the retry count, and reschedule the task.
    async fn reschedule_step_for_retry(
        &self,
        params: RescheduleStepForRetryParams,
    ) -> Result<(), StorageError> {
        let _ = params;
        Err(StorageError::NotImplemented("reschedule_step_for_retry"))
    }

    /// Write a step row as immediately completed. No task row is created.
    ///
    /// Used for capture steps — synchronous, non-parking values that are
    /// persisted on first encounter and returned from DB on replay.
    /// No-op on conflict — replay safety via idempotent upsert.
    async fn insert_completed_step(
        &self,
        run_id: &str,
        step_name: &str,
        step_kind: StepKind,
        result: serde_json::Value,
    ) -> Result<(), StorageError> {
        let _ = (run_id, step_name, step_kind, result);
        Err(StorageError::NotImplemented("insert_completed_step"))
    }

    /// Retry a single dead step within the current run.
    ///
    /// Finds the dead step by `run_id` + `step_name`, creates a new task for it
    /// with `retry_attempt = 0`, and sets the run status back to `running`.
    /// No new run is started — scoped to the current run.
    ///
    /// # Errors
    /// - [`StorageError::StepNotFound`] if no step exists for the given name
    /// - [`StorageError::StepStatusMismatch`] if the step is not in `dead` status
    async fn admin_retry_step(
        &self,
        run_id: &str,
        step_name: &str,
        triggered_by: Option<&str>,
    ) -> Result<String, StorageError> {
        let _ = (run_id, step_name, triggered_by);
        Err(StorageError::NotImplemented("admin_retry_step"))
    }

    /// Restart an entire execution from scratch.
    ///
    /// Archives the current run to `zart_execution_runs` (preserving history),
    /// creates a new run with `trigger = 'restart'`, and schedules a fresh
    /// body task at segment 0.
    ///
    /// If `new_payload` is `Some`, it replaces the execution's payload.
    /// Returns the new `run_id`.
    async fn admin_restart_execution(
        &self,
        execution_id: &str,
        new_payload: Option<serde_json::Value>,
        triggered_by: Option<&str>,
    ) -> Result<String, StorageError> {
        let _ = (execution_id, new_payload, triggered_by);
        Err(StorageError::NotImplemented("admin_restart_execution"))
    }

    /// Selectively rerun a subset of steps while preserving others.
    ///
    /// Archives the current run, starts a new run, and schedules a fresh body
    /// task. Failed/dead steps are always rerun. `force_rerun` steps are
    /// rerun even if completed. `preserve` steps are carried forward (ignored
    /// for failed/dead steps).
    ///
    /// # Returns
    ///
    /// A tuple of `(new_run_id, effective_rerun_steps)`.
    async fn admin_rerun_steps(
        &self,
        execution_id: &str,
        force_rerun: &[String],
        preserve: &[String],
        triggered_by: Option<&str>,
    ) -> Result<(String, Vec<String>), StorageError> {
        let _ = (execution_id, force_rerun, preserve, triggered_by);
        Err(StorageError::NotImplemented("admin_rerun_steps"))
    }

    /// Count executions grouped by status.
    async fn execution_stats(&self) -> Result<ExecutionStats, StorageError> {
        Err(StorageError::NotImplemented("execution_stats"))
    }

    /// List all step attempts for a run.
    async fn list_step_attempts(&self, run_id: &str) -> Result<Vec<StepAttemptRow>, StorageError> {
        let _ = run_id;
        Err(StorageError::NotImplemented("list_step_attempts"))
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
            .schedule_now("task-1", "my-task", serde_json::json!({}))
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
