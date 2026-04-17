//! Delegation-only implementation of the [`DurableStorage`] trait for [`PostgresScheduler`].
//!
//! Every method delegates to a domain-specific extension trait:
//! - `ExecutionStorage` for execution lifecycle and run queries
//! - `AdminStorage` for admin operations (restart, rerun, retry, reset)
//! - `StepStorage` for step operations
//! - `WaitGroupStorage` for wait-group coordination
//! - `EventStorage` for event delivery and execution statistics

use async_trait::async_trait;
use sqlx::PgConnection;

use super::{
    AdminStorage, EventStorage, ExecutionStorage, PostgresScheduler, StepStorage, WaitGroupStorage,
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
        ExecutionStorage::start_execution(self, execution_id, task_name, payload).await
    }

    async fn start_execution_in_tx(
        &self,
        conn: &mut PgConnection,
        execution_id: &str,
        task_name: &str,
        payload: serde_json::Value,
    ) -> Result<(), StorageError> {
        ExecutionStorage::start_execution_in_tx(self, conn, execution_id, task_name, payload).await
    }

    async fn complete_execution(
        &self,
        execution_id: &str,
        result: serde_json::Value,
    ) -> Result<(), StorageError> {
        ExecutionStorage::complete_execution(self, execution_id, result).await
    }

    async fn fail_execution(&self, execution_id: &str) -> Result<(), StorageError> {
        ExecutionStorage::fail_execution(self, execution_id).await
    }

    async fn get_execution(
        &self,
        execution_id: &str,
    ) -> Result<Option<ExecutionRecord>, StorageError> {
        ExecutionStorage::get_execution(self, execution_id).await
    }

    async fn cancel_execution(&self, execution_id: &str) -> Result<bool, StorageError> {
        ExecutionStorage::cancel_execution(self, execution_id).await
    }

    async fn list_executions(
        &self,
        params: ListExecutionsParams,
    ) -> Result<Vec<ExecutionRecord>, StorageError> {
        ExecutionStorage::list_executions(self, params).await
    }

    async fn deliver_event(
        &self,
        execution_id: &str,
        event_name: &str,
        payload: serde_json::Value,
    ) -> Result<EventDeliveryResult, StorageError> {
        EventStorage::deliver_event(self, execution_id, event_name, payload).await
    }

    async fn reset_execution(
        &self,
        execution_id: &str,
        payload: serde_json::Value,
    ) -> Result<String, StorageError> {
        AdminStorage::reset_execution(self, execution_id, payload).await
    }

    async fn get_step_status(
        &self,
        run_id: &str,
        step_name: &str,
    ) -> Result<Option<StepLookup>, StorageError> {
        StepStorage::get_step_status(self, run_id, step_name).await
    }

    async fn get_current_run_id(&self, execution_id: &str) -> Result<Option<String>, StorageError> {
        ExecutionStorage::get_current_run_id(self, execution_id).await
    }

    async fn list_runs(&self, execution_id: &str) -> Result<Vec<ExecutionRunRecord>, StorageError> {
        ExecutionStorage::list_runs(self, execution_id).await
    }

    async fn check_wait_all_children(
        &self,
        wait_for_task_ids: &[String],
    ) -> Result<Vec<(String, serde_json::Value)>, StorageError> {
        StepStorage::check_wait_all_children(self, wait_for_task_ids).await
    }

    async fn get_step(
        &self,
        run_id: &str,
        step_name: &str,
    ) -> Result<Option<StepRow>, StorageError> {
        StepStorage::get_step(self, run_id, step_name).await
    }

    async fn list_steps(&self, run_id: &str) -> Result<Vec<StepRow>, StorageError> {
        StepStorage::list_steps(self, run_id).await
    }

    async fn upsert_wait_group_step(
        &self,
        params: UpsertWaitGroupStepParams,
    ) -> Result<(), StorageError> {
        WaitGroupStorage::upsert_wait_group_step(self, params).await
    }

    async fn complete_wait_group_child(
        &self,
        params: CompleteWaitGroupChildParams,
    ) -> Result<bool, StorageError> {
        WaitGroupStorage::complete_wait_group_child(self, params).await
    }

    async fn fail_wait_group_child(
        &self,
        params: FailWaitGroupChildParams,
    ) -> Result<bool, StorageError> {
        WaitGroupStorage::fail_wait_group_child(self, params).await
    }

    async fn recover_wait_group_orphans(&self) -> Result<usize, StorageError> {
        WaitGroupStorage::recover_wait_group_orphans(self).await
    }

    async fn schedule_step(
        &self,
        params: ScheduleStepParams,
    ) -> Result<ScheduleResult, StorageError> {
        StepStorage::schedule_step(self, params).await
    }

    async fn complete_step_and_schedule_body(
        &self,
        params: CompleteStepAndScheduleBodyParams,
    ) -> Result<(), StorageError> {
        StepStorage::complete_step_and_schedule_body(self, params).await
    }

    async fn complete_step_and_schedule_body_in_tx(
        &self,
        conn: &mut PgConnection,
        params: CompleteStepAndScheduleBodyParams,
    ) -> Result<(), StorageError> {
        StepStorage::complete_step_and_schedule_body_in_tx(self, conn, params).await
    }

    async fn complete_step_no_resume(
        &self,
        params: CompleteStepNoResumeParams,
    ) -> Result<(), StorageError> {
        StepStorage::complete_step_no_resume(self, params).await
    }

    async fn reschedule_step_for_retry(
        &self,
        params: RescheduleStepForRetryParams,
    ) -> Result<(), StorageError> {
        StepStorage::reschedule_step_for_retry(self, params).await
    }

    async fn insert_completed_step(
        &self,
        run_id: &str,
        step_name: &str,
        step_kind: StepKind,
        result: serde_json::Value,
    ) -> Result<(), StorageError> {
        StepStorage::insert_completed_step(self, run_id, step_name, step_kind, result).await
    }

    async fn admin_retry_step(
        &self,
        run_id: &str,
        step_name: &str,
        triggered_by: Option<&str>,
    ) -> Result<String, StorageError> {
        AdminStorage::admin_retry_step(self, run_id, step_name, triggered_by).await
    }

    async fn admin_restart_execution(
        &self,
        execution_id: &str,
        new_payload: Option<serde_json::Value>,
        triggered_by: Option<&str>,
    ) -> Result<String, StorageError> {
        AdminStorage::admin_restart_execution(self, execution_id, new_payload, triggered_by).await
    }

    async fn admin_rerun_steps(
        &self,
        execution_id: &str,
        force_rerun: &[String],
        preserve: &[String],
        triggered_by: Option<&str>,
    ) -> Result<(String, Vec<String>), StorageError> {
        AdminStorage::admin_rerun_steps(self, execution_id, force_rerun, preserve, triggered_by)
            .await
    }

    async fn execution_stats(&self) -> Result<ExecutionStats, StorageError> {
        EventStorage::execution_stats(self).await
    }

    async fn list_step_attempts(&self, run_id: &str) -> Result<Vec<StepAttemptRow>, StorageError> {
        StepStorage::list_step_attempts(self, run_id).await
    }
}
