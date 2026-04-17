//! Repository trait for step and step-attempt table access.
//!
//! Scoped to `zart_steps` and `zart_step_attempts`.
//! Raw data access only — no business logic.

use sqlx::PgConnection;

use crate::{
    CompleteStepAndScheduleBodyParams, CompleteStepNoResumeParams, RescheduleStepForRetryParams,
    ScheduleResult, ScheduleStepParams, StepAttemptRow, StepKind, StepLookup, StepRow,
    StorageError,
};

/// Internal repository for step and step-attempt row access.
/// Not part of the public API — used to modularize the `DurableStorage` impl.
pub(crate) trait StepRepository: Sized {
    async fn get_step_status(
        &self,
        run_id: &str,
        step_name: &str,
    ) -> Result<Option<StepLookup>, StorageError>;

    async fn get_step(
        &self,
        run_id: &str,
        step_name: &str,
    ) -> Result<Option<StepRow>, StorageError>;

    async fn list_steps(&self, run_id: &str) -> Result<Vec<StepRow>, StorageError>;

    async fn schedule_step(
        &self,
        params: ScheduleStepParams,
    ) -> Result<ScheduleResult, StorageError>;

    async fn complete_step_and_schedule_body(
        &self,
        params: CompleteStepAndScheduleBodyParams,
    ) -> Result<(), StorageError>;

    async fn complete_step_and_schedule_body_in_tx(
        &self,
        conn: &mut PgConnection,
        params: CompleteStepAndScheduleBodyParams,
    ) -> Result<(), StorageError>;

    async fn complete_step_no_resume(
        &self,
        params: CompleteStepNoResumeParams,
    ) -> Result<(), StorageError>;

    async fn reschedule_step_for_retry(
        &self,
        params: RescheduleStepForRetryParams,
    ) -> Result<(), StorageError>;

    async fn insert_completed_step(
        &self,
        run_id: &str,
        step_name: &str,
        step_kind: StepKind,
        result: serde_json::Value,
    ) -> Result<(), StorageError>;

    async fn check_wait_all_children(
        &self,
        wait_for_task_ids: &[String],
    ) -> Result<Vec<(String, serde_json::Value)>, StorageError>;

    async fn list_step_attempts(&self, run_id: &str) -> Result<Vec<StepAttemptRow>, StorageError>;
}
