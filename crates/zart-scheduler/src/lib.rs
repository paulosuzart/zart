//! Core scheduling primitives for Zart.
//!
//! This crate provides the storage traits and associated types that define
//! the contract for task scheduling, polling, and storage backends.
//!
//! # Architecture
//!
//! The scheduler operates at Layer 1 of the Zart stack: individual tasks.
//! Each task is a row in the database that gets picked up by a worker via
//! `SKIP LOCKED`, executed, and either completed, failed, or rescheduled.
//!
//! ## Focused store traits (Phase 3)
//!
//! [`TaskScheduler`], [`ExecutionStore`], [`StepStore`], [`WaitGroupStore`],
//! and [`EventStore`] replace the old monolithic [`Scheduler`] /
//! [`DurableStorage`] surfaces. Implement the focused traits; the deprecated
//! blanket impls keep existing code compiling through the migration window.

pub mod error;
pub mod pause_storage;
pub mod recurrence;
pub mod store;
pub mod task_metadata;
pub mod types;

pub(crate) mod repository;

pub mod postgres;

pub use error::StorageError;
pub use recurrence::Recurrence;
pub use store::{EventStore, ExecutionStore, StepStore, TaskScheduler, WaitGroupStore};
pub use task_metadata::{StepMetaType, TaskMetadata};
pub use types::{
    CompleteAndScheduleParams, CompleteStepAndScheduleBodyParams, CompleteStepNoResumeParams,
    CompleteWaitGroupChildParams, EventDeliveryResult, ExecutionRecord, ExecutionRunRecord,
    ExecutionSortField, ExecutionStats, ExecutionStatus, ExecutionTrigger,
    FailWaitGroupChildParams, FetchedTask, ListExecutionsParams, RescheduleStepForRetryParams,
    ScheduleAtParams, ScheduleResult, ScheduleStepParams, SortOrder, StepAttemptRow,
    StepAttemptStatus, StepKind, StepLookup, StepResultKind, StepRow, StepStatus, TaskStatus,
    UpsertWaitGroupStepParams,
};

pub use postgres::{PostgresScheduler, TableNames, TableNamesError};

use async_trait::async_trait;
use pause_storage::PauseStorage;

/// Deprecated: use [`TaskScheduler`] instead.
///
/// `Scheduler` is now an empty supertrait of `TaskScheduler`. Any type that
/// implements `TaskScheduler` automatically satisfies `Scheduler` via the
/// blanket impl below. Migrate by replacing `impl Scheduler for T { … }` with
/// `impl TaskScheduler for T { … }` — the method signatures are identical.
#[deprecated(
    since = "0.2.0",
    note = "Implement TaskScheduler instead; Scheduler is a blanket alias."
)]
pub trait Scheduler: TaskScheduler {}

#[allow(deprecated)]
impl<T: TaskScheduler> Scheduler for T {}

/// Deprecated: use the focused store traits instead.
///
/// [`DurableStorage`] is replaced by [`ExecutionStore`] + [`StepStore`] +
/// [`WaitGroupStore`] + [`EventStore`]. Implement all four focused traits;
/// the blanket impl at the bottom of this block gives `DurableStorage` for
/// free so existing call sites continue compiling through the migration window.
///
/// # Migration
///
/// ```text
/// // Before
/// impl DurableStorage for MyBackend { … }
///
/// // After
/// impl ExecutionStore for MyBackend { … }
/// impl StepStore for MyBackend { … }
/// impl WaitGroupStore for MyBackend { … }
/// impl EventStore for MyBackend { … }
/// ```
#[deprecated(
    since = "0.2.0",
    note = "Implement ExecutionStore + StepStore + WaitGroupStore + EventStore instead."
)]
#[async_trait]
pub trait DurableStorage:
    ExecutionStore + StepStore + WaitGroupStore + EventStore + Send + Sync
{
    // ── ExecutionStore delegation ─────────────────────────────────────────────

    async fn start_execution(
        &self,
        execution_id: &str,
        task_name: &str,
        payload: serde_json::Value,
    ) -> Result<(), StorageError> {
        ExecutionStore::start_execution(self, execution_id, task_name, payload).await
    }

    #[allow(unused_variables)]
    async fn start_execution_in_tx(
        &self,
        conn: &mut sqlx::PgConnection,
        execution_id: &str,
        task_name: &str,
        payload: serde_json::Value,
    ) -> Result<(), StorageError> {
        ExecutionStore::start_execution_in_tx(self, conn, execution_id, task_name, payload).await
    }

    async fn complete_execution(
        &self,
        execution_id: &str,
        result: serde_json::Value,
    ) -> Result<(), StorageError> {
        ExecutionStore::complete_execution(self, execution_id, result).await
    }

    async fn fail_execution(&self, execution_id: &str) -> Result<(), StorageError> {
        ExecutionStore::fail_execution(self, execution_id).await
    }

    async fn get_execution(
        &self,
        execution_id: &str,
    ) -> Result<Option<ExecutionRecord>, StorageError> {
        ExecutionStore::get_execution(self, execution_id).await
    }

    async fn cancel_execution(&self, execution_id: &str) -> Result<bool, StorageError> {
        ExecutionStore::cancel_execution(self, execution_id).await
    }

    async fn list_executions(
        &self,
        params: ListExecutionsParams,
    ) -> Result<Vec<ExecutionRecord>, StorageError> {
        ExecutionStore::list_executions(self, params).await
    }

    async fn deliver_event(
        &self,
        execution_id: &str,
        event_name: &str,
        payload: serde_json::Value,
    ) -> Result<EventDeliveryResult, StorageError> {
        EventStore::deliver_event(self, execution_id, event_name, payload).await
    }

    async fn complete_event_step_and_schedule_body(
        &self,
        execution_id: &str,
        event_name: &str,
        payload: serde_json::Value,
    ) -> Result<bool, StorageError> {
        EventStore::complete_event_step_and_schedule_body(self, execution_id, event_name, payload)
            .await
    }

    async fn reset_execution(
        &self,
        execution_id: &str,
        payload: serde_json::Value,
    ) -> Result<String, StorageError> {
        ExecutionStore::reset_execution(self, execution_id, payload).await
    }

    async fn get_step_status(
        &self,
        run_id: &str,
        step_name: &str,
    ) -> Result<Option<StepLookup>, StorageError> {
        StepStore::get_step_status(self, run_id, step_name).await
    }

    async fn get_current_run_id(&self, execution_id: &str) -> Result<Option<String>, StorageError> {
        ExecutionStore::get_current_run_id(self, execution_id).await
    }

    async fn list_runs(&self, execution_id: &str) -> Result<Vec<ExecutionRunRecord>, StorageError> {
        ExecutionStore::list_runs(self, execution_id).await
    }

    async fn check_wait_all_children(
        &self,
        wait_for_task_ids: &[String],
    ) -> Result<Vec<(String, serde_json::Value)>, StorageError> {
        StepStore::check_wait_all_children(self, wait_for_task_ids).await
    }

    async fn get_step(
        &self,
        run_id: &str,
        step_name: &str,
    ) -> Result<Option<StepRow>, StorageError> {
        StepStore::get_step(self, run_id, step_name).await
    }

    async fn list_steps(&self, run_id: &str) -> Result<Vec<StepRow>, StorageError> {
        StepStore::list_steps(self, run_id).await
    }

    async fn upsert_wait_group_step(
        &self,
        params: UpsertWaitGroupStepParams,
    ) -> Result<(), StorageError> {
        WaitGroupStore::upsert_wait_group_step(self, params).await
    }

    async fn complete_wait_group_child(
        &self,
        params: CompleteWaitGroupChildParams,
    ) -> Result<bool, StorageError> {
        WaitGroupStore::complete_wait_group_child(self, params).await
    }

    async fn fail_wait_group_child(
        &self,
        params: FailWaitGroupChildParams,
    ) -> Result<bool, StorageError> {
        WaitGroupStore::fail_wait_group_child(self, params).await
    }

    async fn recover_wait_group_orphans(&self) -> Result<usize, StorageError> {
        WaitGroupStore::recover_wait_group_orphans(self).await
    }

    async fn schedule_step(
        &self,
        params: ScheduleStepParams,
    ) -> Result<ScheduleResult, StorageError> {
        StepStore::schedule_step(self, params).await
    }

    async fn complete_step_and_schedule_body(
        &self,
        params: CompleteStepAndScheduleBodyParams,
    ) -> Result<(), StorageError> {
        StepStore::complete_step_and_schedule_body(self, params).await
    }

    #[allow(unused_variables)]
    async fn complete_step_and_schedule_body_in_tx(
        &self,
        conn: &mut sqlx::PgConnection,
        params: CompleteStepAndScheduleBodyParams,
    ) -> Result<(), StorageError> {
        StepStore::complete_step_and_schedule_body_in_tx(self, conn, params).await
    }

    async fn complete_step_no_resume(
        &self,
        params: CompleteStepNoResumeParams,
    ) -> Result<(), StorageError> {
        StepStore::complete_step_no_resume(self, params).await
    }

    async fn reschedule_step_for_retry(
        &self,
        params: RescheduleStepForRetryParams,
    ) -> Result<(), StorageError> {
        StepStore::reschedule_step_for_retry(self, params).await
    }

    async fn insert_completed_step(
        &self,
        run_id: &str,
        step_name: &str,
        step_kind: StepKind,
        result: serde_json::Value,
    ) -> Result<(), StorageError> {
        StepStore::insert_completed_step(self, run_id, step_name, step_kind, result).await
    }

    async fn retry_dead_step(
        &self,
        run_id: &str,
        step_name: &str,
        triggered_by: Option<&str>,
    ) -> Result<String, StorageError> {
        ExecutionStore::retry_dead_step(self, run_id, step_name, triggered_by).await
    }

    async fn restart_run(
        &self,
        execution_id: &str,
        new_payload: Option<serde_json::Value>,
        trigger: &str,
        triggered_by: Option<&str>,
    ) -> Result<String, StorageError> {
        ExecutionStore::restart_run(self, execution_id, new_payload, trigger, triggered_by).await
    }

    async fn execution_stats(&self) -> Result<ExecutionStats, StorageError> {
        EventStore::execution_stats(self).await
    }

    async fn list_step_attempts(&self, run_id: &str) -> Result<Vec<StepAttemptRow>, StorageError> {
        StepStore::list_step_attempts(self, run_id).await
    }
}

/// Blanket impl: any type implementing the four focused store traits
/// automatically satisfies the deprecated `DurableStorage` trait.
#[allow(deprecated)]
impl<T: ExecutionStore + StepStore + WaitGroupStore + EventStore + Send + Sync> DurableStorage
    for T
{
}

/// Combined backend trait — the single type-erased handle for all storage operations.
///
/// Use `Arc<dyn StorageBackend>` wherever a fully-capable backend is needed.
/// `PostgresScheduler` satisfies this bound automatically via blanket impls.
///
/// Composed from:
/// - [`TaskScheduler`] — task queue lifecycle
/// - [`ExecutionStore`] — execution records and run primitives
/// - [`StepStore`] — step scheduling, completion, and query
/// - [`WaitGroupStore`] — wait-group coordination
/// - [`EventStore`] — event delivery and statistics
/// - [`PauseStorage`](crate::pause_storage::PauseStorage) — pause rules
pub trait StorageBackend:
    TaskScheduler
    + ExecutionStore
    + StepStore
    + WaitGroupStore
    + EventStore
    + PauseStorage
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
        + PauseStorage
        + Send
        + Sync,
> StorageBackend for T
{
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};
    use std::sync::Arc;

    /// A minimal in-memory stub implementing TaskScheduler.
    struct StubScheduler;

    #[async_trait]
    impl TaskScheduler for StubScheduler {
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

    /// Minimal stub that implements the four focused store traits so the
    /// deprecated `DurableStorage` blanket applies automatically.
    struct StubDurableStorage;

    #[async_trait]
    impl TaskScheduler for StubDurableStorage {
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

    // ExecutionStore — all required methods return NotImplemented
    #[async_trait]
    impl ExecutionStore for StubDurableStorage {
        async fn start_execution(
            &self,
            _: &str,
            _: &str,
            _: serde_json::Value,
        ) -> Result<(), StorageError> {
            Err(StorageError::NotImplemented("start_execution"))
        }
        async fn complete_execution(
            &self,
            _: &str,
            _: serde_json::Value,
        ) -> Result<(), StorageError> {
            Err(StorageError::NotImplemented("complete_execution"))
        }
        async fn fail_execution(&self, _: &str) -> Result<(), StorageError> {
            Err(StorageError::NotImplemented("fail_execution"))
        }
        async fn get_execution(&self, _: &str) -> Result<Option<ExecutionRecord>, StorageError> {
            Err(StorageError::NotImplemented("get_execution"))
        }
        async fn cancel_execution(&self, _: &str) -> Result<bool, StorageError> {
            Err(StorageError::NotImplemented("cancel_execution"))
        }
        async fn list_executions(
            &self,
            _: ListExecutionsParams,
        ) -> Result<Vec<ExecutionRecord>, StorageError> {
            Err(StorageError::NotImplemented("list_executions"))
        }
        async fn get_current_run_id(&self, _: &str) -> Result<Option<String>, StorageError> {
            Err(StorageError::NotImplemented("get_current_run_id"))
        }
        async fn list_runs(&self, _: &str) -> Result<Vec<ExecutionRunRecord>, StorageError> {
            Err(StorageError::NotImplemented("list_runs"))
        }
        async fn reset_execution(
            &self,
            _: &str,
            _: serde_json::Value,
        ) -> Result<String, StorageError> {
            Err(StorageError::NotImplemented("reset_execution"))
        }
        async fn retry_dead_step(
            &self,
            _: &str,
            _: &str,
            _: Option<&str>,
        ) -> Result<String, StorageError> {
            Err(StorageError::NotImplemented("retry_dead_step"))
        }
        async fn restart_run(
            &self,
            _: &str,
            _: Option<serde_json::Value>,
            _: &str,
            _: Option<&str>,
        ) -> Result<String, StorageError> {
            Err(StorageError::NotImplemented("restart_run"))
        }
    }

    #[async_trait]
    impl StepStore for StubDurableStorage {
        async fn get_step_status(
            &self,
            _: &str,
            _: &str,
        ) -> Result<Option<StepLookup>, StorageError> {
            Err(StorageError::NotImplemented("get_step_status"))
        }
        async fn get_step(&self, _: &str, _: &str) -> Result<Option<StepRow>, StorageError> {
            Err(StorageError::NotImplemented("get_step"))
        }
        async fn list_steps(&self, _: &str) -> Result<Vec<StepRow>, StorageError> {
            Err(StorageError::NotImplemented("list_steps"))
        }
        async fn list_step_attempts(&self, _: &str) -> Result<Vec<StepAttemptRow>, StorageError> {
            Err(StorageError::NotImplemented("list_step_attempts"))
        }
        async fn schedule_step(
            &self,
            _: ScheduleStepParams,
        ) -> Result<ScheduleResult, StorageError> {
            Err(StorageError::NotImplemented("schedule_step"))
        }
        async fn complete_step_and_schedule_body(
            &self,
            _: CompleteStepAndScheduleBodyParams,
        ) -> Result<(), StorageError> {
            Err(StorageError::NotImplemented(
                "complete_step_and_schedule_body",
            ))
        }
        async fn complete_step_no_resume(
            &self,
            _: CompleteStepNoResumeParams,
        ) -> Result<(), StorageError> {
            Err(StorageError::NotImplemented("complete_step_no_resume"))
        }
        async fn reschedule_step_for_retry(
            &self,
            _: RescheduleStepForRetryParams,
        ) -> Result<(), StorageError> {
            Err(StorageError::NotImplemented("reschedule_step_for_retry"))
        }
        async fn insert_completed_step(
            &self,
            _: &str,
            _: &str,
            _: StepKind,
            _: serde_json::Value,
        ) -> Result<(), StorageError> {
            Err(StorageError::NotImplemented("insert_completed_step"))
        }
        async fn check_wait_all_children(
            &self,
            _: &[String],
        ) -> Result<Vec<(String, serde_json::Value)>, StorageError> {
            Err(StorageError::NotImplemented("check_wait_all_children"))
        }
    }

    #[async_trait]
    impl WaitGroupStore for StubDurableStorage {
        async fn upsert_wait_group_step(
            &self,
            _: UpsertWaitGroupStepParams,
        ) -> Result<(), StorageError> {
            Err(StorageError::NotImplemented("upsert_wait_group_step"))
        }
        async fn complete_wait_group_child(
            &self,
            _: CompleteWaitGroupChildParams,
        ) -> Result<bool, StorageError> {
            Err(StorageError::NotImplemented("complete_wait_group_child"))
        }
        async fn fail_wait_group_child(
            &self,
            _: FailWaitGroupChildParams,
        ) -> Result<bool, StorageError> {
            Err(StorageError::NotImplemented("fail_wait_group_child"))
        }
        async fn recover_wait_group_orphans(&self) -> Result<usize, StorageError> {
            Err(StorageError::NotImplemented("recover_wait_group_orphans"))
        }
    }

    // EventStore — deliver_event returns NotImplemented; the default
    // complete_event_step_and_schedule_body propagates that error.
    #[async_trait]
    impl EventStore for StubDurableStorage {
        async fn deliver_event(
            &self,
            _: &str,
            _: &str,
            _: serde_json::Value,
        ) -> Result<EventDeliveryResult, StorageError> {
            Err(StorageError::NotImplemented("deliver_event"))
        }
        async fn execution_stats(&self) -> Result<ExecutionStats, StorageError> {
            Err(StorageError::NotImplemented("execution_stats"))
        }
    }

    #[tokio::test]
    async fn complete_event_step_and_schedule_body_not_implemented_by_default() {
        let storage = StubDurableStorage;
        // EventStore::complete_event_step_and_schedule_body delegates to deliver_event,
        // which returns NotImplemented above.
        let result = EventStore::complete_event_step_and_schedule_body(
            &storage,
            "exec-1",
            "approval",
            serde_json::json!({}),
        )
        .await;
        assert!(matches!(result, Err(StorageError::NotImplemented(_))));
    }
}
