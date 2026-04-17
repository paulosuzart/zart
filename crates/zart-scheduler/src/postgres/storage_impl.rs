//! Delegation-only implementation of the [`DurableStorage`] trait for [`PostgresScheduler`].
//!
//! Every method delegates to a domain-specific repository:
//! - `ExecutionRepository` for execution lifecycle and run queries
//! - `AdminRepository` for admin operations (restart, rerun, retry, reset)
//! - `StepRepository` for step operations
//! - `WaitGroupRepository` for wait-group coordination
//! - `EventRepository` for event delivery and execution statistics

use async_trait::async_trait;
use sqlx::PgConnection;

use super::PostgresScheduler;
use crate::repository::{
    AdminRepository, EventRepository, ExecutionRepository, StepRepository, WaitGroupRepository,
};
use crate::{
    CompleteStepAndScheduleBodyParams, CompleteStepNoResumeParams, CompleteWaitGroupChildParams,
    DurableStorage, EventDeliveryResult, ExecutionRecord, ExecutionRunRecord, ExecutionStats,
    FailWaitGroupChildParams, ListExecutionsParams, RescheduleStepForRetryParams, ScheduleResult,
    ScheduleStepParams, StepAttemptRow, StepKind, StepLookup, StepRow, StorageError,
    UpsertWaitGroupStepParams,
};

#[async_trait]
impl DurableStorage for PostgresScheduler {
    async fn start_execution(
        &self,
        execution_id: &str,
        task_name: &str,
        payload: serde_json::Value,
    ) -> Result<(), StorageError> {
        ExecutionRepository::start_execution(self, execution_id, task_name, payload).await
    }

    async fn start_execution_in_tx(
        &self,
        conn: &mut PgConnection,
        execution_id: &str,
        task_name: &str,
        payload: serde_json::Value,
    ) -> Result<(), StorageError> {
        ExecutionRepository::start_execution_in_tx(self, conn, execution_id, task_name, payload)
            .await
    }

    async fn complete_execution(
        &self,
        execution_id: &str,
        result: serde_json::Value,
    ) -> Result<(), StorageError> {
        ExecutionRepository::complete_execution(self, execution_id, result).await
    }

    async fn fail_execution(&self, execution_id: &str) -> Result<(), StorageError> {
        ExecutionRepository::fail_execution(self, execution_id).await
    }

    async fn get_execution(
        &self,
        execution_id: &str,
    ) -> Result<Option<ExecutionRecord>, StorageError> {
        ExecutionRepository::get_execution(self, execution_id).await
    }

    async fn cancel_execution(&self, execution_id: &str) -> Result<bool, StorageError> {
        ExecutionRepository::cancel_execution(self, execution_id).await
    }

    async fn list_executions(
        &self,
        params: ListExecutionsParams,
    ) -> Result<Vec<ExecutionRecord>, StorageError> {
        ExecutionRepository::list_executions(self, params).await
    }

    async fn deliver_event(
        &self,
        execution_id: &str,
        event_name: &str,
        payload: serde_json::Value,
    ) -> Result<EventDeliveryResult, StorageError> {
        EventRepository::deliver_event(self, execution_id, event_name, payload).await
    }

    async fn reset_execution(
        &self,
        execution_id: &str,
        payload: serde_json::Value,
    ) -> Result<String, StorageError> {
        AdminRepository::reset_execution(self, execution_id, payload).await
    }

    async fn get_step_status(
        &self,
        run_id: &str,
        step_name: &str,
    ) -> Result<Option<StepLookup>, StorageError> {
        StepRepository::get_step_status(self, run_id, step_name).await
    }

    async fn get_current_run_id(&self, execution_id: &str) -> Result<Option<String>, StorageError> {
        ExecutionRepository::get_current_run_id(self, execution_id).await
    }

    async fn list_runs(&self, execution_id: &str) -> Result<Vec<ExecutionRunRecord>, StorageError> {
        ExecutionRepository::list_runs(self, execution_id).await
    }

    async fn check_wait_all_children(
        &self,
        wait_for_task_ids: &[String],
    ) -> Result<Vec<(String, serde_json::Value)>, StorageError> {
        StepRepository::check_wait_all_children(self, wait_for_task_ids).await
    }

    async fn get_step(
        &self,
        run_id: &str,
        step_name: &str,
    ) -> Result<Option<StepRow>, StorageError> {
        StepRepository::get_step(self, run_id, step_name).await
    }

    async fn list_steps(&self, run_id: &str) -> Result<Vec<StepRow>, StorageError> {
        StepRepository::list_steps(self, run_id).await
    }

    async fn upsert_wait_group_step(
        &self,
        params: UpsertWaitGroupStepParams,
    ) -> Result<(), StorageError> {
        WaitGroupRepository::upsert_wait_group_step(self, params).await
    }

    async fn complete_wait_group_child(
        &self,
        params: CompleteWaitGroupChildParams,
    ) -> Result<bool, StorageError> {
        WaitGroupRepository::complete_wait_group_child(self, params).await
    }

    async fn fail_wait_group_child(
        &self,
        params: FailWaitGroupChildParams,
    ) -> Result<bool, StorageError> {
        WaitGroupRepository::fail_wait_group_child(self, params).await
    }

    async fn recover_wait_group_orphans(&self) -> Result<usize, StorageError> {
        WaitGroupRepository::recover_wait_group_orphans(self).await
    }

    async fn schedule_step(
        &self,
        params: ScheduleStepParams,
    ) -> Result<ScheduleResult, StorageError> {
        StepRepository::schedule_step(self, params).await
    }

    async fn complete_step_and_schedule_body(
        &self,
        params: CompleteStepAndScheduleBodyParams,
    ) -> Result<(), StorageError> {
        StepRepository::complete_step_and_schedule_body(self, params).await
    }

    async fn complete_step_and_schedule_body_in_tx(
        &self,
        conn: &mut PgConnection,
        params: CompleteStepAndScheduleBodyParams,
    ) -> Result<(), StorageError> {
        StepRepository::complete_step_and_schedule_body_in_tx(self, conn, params).await
    }

    async fn complete_step_no_resume(
        &self,
        params: CompleteStepNoResumeParams,
    ) -> Result<(), StorageError> {
        StepRepository::complete_step_no_resume(self, params).await
    }

    async fn reschedule_step_for_retry(
        &self,
        params: RescheduleStepForRetryParams,
    ) -> Result<(), StorageError> {
        StepRepository::reschedule_step_for_retry(self, params).await
    }

    async fn insert_completed_step(
        &self,
        run_id: &str,
        step_name: &str,
        step_kind: StepKind,
        result: serde_json::Value,
    ) -> Result<(), StorageError> {
        StepRepository::insert_completed_step(self, run_id, step_name, step_kind, result).await
    }

    async fn retry_dead_step(
        &self,
        run_id: &str,
        step_name: &str,
        triggered_by: Option<&str>,
    ) -> Result<String, StorageError> {
        AdminRepository::retry_dead_step(self, run_id, step_name, triggered_by).await
    }

    async fn restart_run(
        &self,
        execution_id: &str,
        new_payload: Option<serde_json::Value>,
        trigger: &str,
        triggered_by: Option<&str>,
    ) -> Result<String, StorageError> {
        AdminRepository::restart_run(self, execution_id, new_payload, trigger, triggered_by).await
    }

    async fn execution_stats(&self) -> Result<ExecutionStats, StorageError> {
        EventRepository::execution_stats(self).await
    }

    async fn list_step_attempts(&self, run_id: &str) -> Result<Vec<StepAttemptRow>, StorageError> {
        StepRepository::list_step_attempts(self, run_id).await
    }
}
