//! Focused public store traits for the Zart storage layer.
//!
//! Each trait owns one domain:
//!
//! | Trait | Domain |
//! |---|---|
//! | [`TaskScheduler`] | Task-queue lifecycle (schedule, poll, complete, fail) |
//! | [`ExecutionStore`] | Durable execution records and run primitives |
//! | [`StepStore`] | Step scheduling, completion, retry, and queries |
//! | [`WaitGroupStore`] | Wait-group coordination |
//! | [`EventStore`] | External event delivery and execution statistics |
//! | [`PauseStorage`] | Pause rule storage |

pub mod pause_storage;

pub use pause_storage::{PauseRule, PauseRuleFilter, PauseSnapshot, PauseStorage, PauseStore};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::PgConnection;

use crate::error::StorageError;
use crate::types::{
    CompleteAndScheduleParams, CompleteStepAndScheduleBodyParams, CompleteStepNoResumeParams,
    CompleteWaitGroupChildParams, EventDeliveryResult, ExecutionRecord, ExecutionRunRecord,
    ExecutionStats, FailWaitGroupChildParams, FetchedTask, ListExecutionsParams,
    RescheduleStepForRetryParams, ScheduleAtParams, ScheduleResult, ScheduleStepParams,
    StepAttemptRow, StepKind, StepLookup, StepRow, UpsertWaitGroupStepParams,
};

// ── TaskScheduler ─────────────────────────────────────────────────────────────

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

    /// Mark a task as successfully completed.
    async fn mark_completed(
        &self,
        task_id: &str,
        result: Option<serde_json::Value>,
        lock_token: &str,
    ) -> Result<(), StorageError>;

    /// Mark a task as failed, optionally rescheduling for retry.
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
    /// Default implementation returns `NotImplemented`.
    async fn begin(&self) -> Result<sqlx::Transaction<'_, sqlx::Postgres>, StorageError> {
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

// ── ExecutionStore ────────────────────────────────────────────────────────────

/// Durable execution lifecycle and run primitives.
///
/// Covers the `zart_executions` and `zart_execution_runs` tables.
#[async_trait]
pub trait ExecutionStore: Send + Sync {
    /// Insert a new durable execution record.
    async fn start_execution(
        &self,
        execution_id: &str,
        task_name: &str,
        payload: serde_json::Value,
    ) -> Result<(), StorageError>;

    /// Insert a new durable execution record within the caller's transaction.
    ///
    /// Default implementation returns `NotImplemented`.
    #[allow(unused_variables)]
    async fn start_execution_in_tx(
        &self,
        conn: &mut PgConnection,
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
    ) -> Result<(), StorageError>;

    /// Mark a durable execution as failed.
    async fn fail_execution(&self, execution_id: &str) -> Result<(), StorageError>;

    /// Fetch a durable execution record by ID.
    async fn get_execution(
        &self,
        execution_id: &str,
    ) -> Result<Option<ExecutionRecord>, StorageError>;

    /// Cancel a running or scheduled durable execution.
    async fn cancel_execution(&self, execution_id: &str) -> Result<bool, StorageError>;

    /// List durable execution records with optional filters.
    async fn list_executions(
        &self,
        params: ListExecutionsParams,
    ) -> Result<Vec<ExecutionRecord>, StorageError>;

    /// Get the current `run_id` for an execution.
    async fn get_current_run_id(&self, execution_id: &str) -> Result<Option<String>, StorageError>;

    /// List all runs for an execution, ordered by `run_index ASC`.
    async fn list_runs(&self, execution_id: &str) -> Result<Vec<ExecutionRunRecord>, StorageError>;

    /// Reset a terminal execution so it can be retried.
    async fn reset_execution(
        &self,
        execution_id: &str,
        payload: serde_json::Value,
    ) -> Result<String, StorageError>;

    /// Atomically validate a step is `dead`, create a retry task, and reset the run.
    async fn retry_dead_step(
        &self,
        run_id: &str,
        step_name: &str,
        triggered_by: Option<&str>,
    ) -> Result<String, StorageError>;

    /// Archive the current run and start a fresh one, scheduling a new body task.
    async fn restart_run(
        &self,
        execution_id: &str,
        new_payload: Option<serde_json::Value>,
        trigger: &str,
        triggered_by: Option<&str>,
    ) -> Result<String, StorageError>;

    /// Create a new run for an execution and return the new `run_id`.
    ///
    /// This is a fine-grained primitive: it inserts the run row but does NOT
    /// update `current_run_id`. Call `set_current_run` afterwards.
    /// Default implementation returns `NotImplemented`.
    #[allow(unused_variables)]
    async fn create_run(
        &self,
        execution_id: &str,
        payload: serde_json::Value,
        trigger: &str,
        triggered_by: Option<&str>,
    ) -> Result<String, StorageError> {
        Err(StorageError::NotImplemented("create_run"))
    }

    /// Set the `current_run_id` for an execution to `run_id`.
    ///
    /// Fine-grained primitive — use after `create_run` to atomically advance
    /// the execution to a new run within a caller-managed transaction.
    /// Default implementation returns `NotImplemented`.
    #[allow(unused_variables)]
    async fn set_current_run(&self, execution_id: &str, run_id: &str) -> Result<(), StorageError> {
        Err(StorageError::NotImplemented("set_current_run"))
    }
}

// ── StepStore ─────────────────────────────────────────────────────────────────

/// Step scheduling, completion, retry, and query operations.
///
/// Covers the `zart_steps` and `zart_step_attempts` tables.
#[async_trait]
pub trait StepStore: Send + Sync {
    /// Look up a step by `run_id` + `step_name`.
    async fn get_step_status(
        &self,
        run_id: &str,
        step_name: &str,
    ) -> Result<Option<StepLookup>, StorageError>;

    /// Look up a step row by `run_id` + `step_name`.
    async fn get_step(
        &self,
        run_id: &str,
        step_name: &str,
    ) -> Result<Option<StepRow>, StorageError>;

    /// List all steps for a run.
    async fn list_steps(&self, run_id: &str) -> Result<Vec<StepRow>, StorageError>;

    /// List all step attempts for a run.
    async fn list_step_attempts(&self, run_id: &str) -> Result<Vec<StepAttemptRow>, StorageError>;

    /// Insert a task row and a step row atomically.
    async fn schedule_step(
        &self,
        params: ScheduleStepParams,
    ) -> Result<ScheduleResult, StorageError>;

    /// Atomically complete a step+task and schedule the next body task.
    async fn complete_step_and_schedule_body(
        &self,
        params: CompleteStepAndScheduleBodyParams,
    ) -> Result<(), StorageError>;

    /// Complete a step and schedule the next body task within the caller's transaction.
    ///
    /// Default implementation returns `NotImplemented`.
    #[allow(unused_variables)]
    async fn complete_step_and_schedule_body_in_tx(
        &self,
        conn: &mut PgConnection,
        params: CompleteStepAndScheduleBodyParams,
    ) -> Result<(), StorageError> {
        Err(StorageError::NotImplemented(
            "complete_step_and_schedule_body_in_tx",
        ))
    }

    /// Atomically complete a step+task without scheduling a body continuation.
    async fn complete_step_no_resume(
        &self,
        params: CompleteStepNoResumeParams,
    ) -> Result<(), StorageError>;

    /// Record a failed step attempt and reschedule the task for retry.
    async fn reschedule_step_for_retry(
        &self,
        params: RescheduleStepForRetryParams,
    ) -> Result<(), StorageError>;

    /// Write a step row as immediately completed (no task row created).
    ///
    /// Used for capture steps. Idempotent via `ON CONFLICT DO NOTHING`.
    async fn insert_completed_step(
        &self,
        run_id: &str,
        step_name: &str,
        step_kind: StepKind,
        result: serde_json::Value,
    ) -> Result<(), StorageError>;

    /// Check whether all `wait_all` children are completed.
    async fn check_wait_all_children(
        &self,
        wait_for_task_ids: &[String],
    ) -> Result<Vec<(String, serde_json::Value)>, StorageError>;
}

// ── WaitGroupStore ────────────────────────────────────────────────────────────

/// Wait-group coordination operations.
#[async_trait]
pub trait WaitGroupStore: Send + Sync {
    /// Upsert (insert-if-absent) a wait-group step row.
    async fn upsert_wait_group_step(
        &self,
        params: UpsertWaitGroupStepParams,
    ) -> Result<(), StorageError>;

    /// Complete a wait-group child and decrement the parent's `wg_remaining`.
    ///
    /// Returns `true` if this child triggered the threshold.
    async fn complete_wait_group_child(
        &self,
        params: CompleteWaitGroupChildParams,
    ) -> Result<bool, StorageError>;

    /// Record a wait-group child failure with compare-and-set semantics.
    ///
    /// Returns `true` only for the first failing child that flips
    /// `wg_first_failed` from false to true.
    async fn fail_wait_group_child(
        &self,
        params: FailWaitGroupChildParams,
    ) -> Result<bool, StorageError>;

    /// Recover wait-group orphans where the group triggered but the body task
    /// was never inserted. Returns the number of recovered body tasks.
    async fn recover_wait_group_orphans(&self) -> Result<usize, StorageError>;
}

// ── StorageBackend ────────────────────────────────────────────────────────────

/// Combined backend trait — the single type-erased handle for all storage operations.
///
/// Use `Arc<dyn StorageBackend>` wherever a fully-capable backend is needed.
/// `PostgresStorage` (in `zart`) satisfies this bound automatically via blanket impls.
///
/// Composed from:
/// - [`TaskScheduler`] — task queue lifecycle
/// - [`ExecutionStore`] — execution records and run primitives
/// - [`StepStore`] — step scheduling, completion, and query
/// - [`WaitGroupStore`] — wait-group coordination
/// - [`EventStore`] — event delivery and statistics
/// - [`pause_storage::PauseStorage`] — pause rules
pub trait StorageBackend:
    TaskScheduler
    + ExecutionStore
    + StepStore
    + WaitGroupStore
    + EventStore
    + pause_storage::PauseStorage
    + Send
    + Sync
{
}

impl<
    T: TaskScheduler
        + ExecutionStore
        + StepStore
        + WaitGroupStore
        + EventStore
        + pause_storage::PauseStorage
        + Send
        + Sync,
> StorageBackend for T
{
}

// ── EventStore ────────────────────────────────────────────────────────────────

/// External event delivery and read-only execution statistics.
#[async_trait]
pub trait EventStore: Send + Sync {
    /// Deliver an external event to a waiting `wait_for_event` step.
    ///
    /// Atomically completes the matching step and schedules the next body task.
    async fn deliver_event(
        &self,
        execution_id: &str,
        event_name: &str,
        payload: serde_json::Value,
    ) -> Result<EventDeliveryResult, StorageError>;

    /// Convenience wrapper: maps `deliver_event` result to a boolean.
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

    /// Return aggregate execution counts grouped by status.
    async fn execution_stats(&self) -> Result<ExecutionStats, StorageError>;
}
