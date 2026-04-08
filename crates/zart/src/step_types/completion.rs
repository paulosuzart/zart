//! Completion behavior implementations for declarative step dispatch (v3 scaffold).
//!
//! This module intentionally provides a conservative, backward-compatible
//! scaffold that can be wired in during phased cutover.

use async_trait::async_trait;
use scheduler::{
    CompleteWaitGroupChildParams, FailWaitGroupChildParams, StorageBackend, StorageError,
};

use crate::step_ops;
use crate::step_types::{CompletionBehavior, CompletionOutcome, CompletionSpec, StepResult};

/// Completion behavior for regular steps and sleep transitions:
/// complete current step task and schedule next body task.
pub struct ScheduleNextBody;

#[async_trait]
impl CompletionBehavior for ScheduleNextBody {
    async fn complete(
        &self,
        scheduler: &dyn StorageBackend,
        spec: CompletionSpec,
    ) -> Result<(), StorageError> {
        let serialized = match spec.result {
            StepResult::Executed(v) | StepResult::Cached(v) => v,
            StepResult::Transition => serde_json::Value::Null,
        };

        let next_body_task_id = format!("{}:body:after:{}", spec.run_id, spec.step_name);

        step_ops::complete_step_and_schedule_body(
            scheduler,
            step_ops::ResumeBodySpec {
                step_task_id: &spec.step_task_id,
                step_id: &spec.step_id,
                result: serialized,
                lock_token: &spec.worker_id,
                next_body_task_id: &next_body_task_id,
                task_name: &spec.task_name,
                run_id: &spec.run_id,
                data: spec.data,
                attempt_number: spec.attempt_number,
            },
        )
        .await
    }
}

/// Completion behavior for wait-group children:
/// atomically decrement wait-group remaining and maybe schedule body.
/// Returns successfully regardless of trigger result; trigger is a backend concern.
pub struct DecrementAndMaybeResume;

#[async_trait]
impl CompletionBehavior for DecrementAndMaybeResume {
    async fn complete(
        &self,
        scheduler: &dyn StorageBackend,
        spec: CompletionSpec,
    ) -> Result<(), StorageError> {
        let group_step_name = match spec.wait_group_step_name {
            Some(name) => name,
            None => {
                return Err(StorageError::NotImplemented(
                    "missing wait_group_step_name for wait-group child completion",
                ));
            }
        };

        let serialized = match spec.result {
            StepResult::Executed(v) | StepResult::Cached(v) => v,
            StepResult::Transition => serde_json::Value::Null,
        };

        let next_body_task_id = format!("{}:body:after:{}", spec.run_id, group_step_name);

        let _triggered = scheduler
            .complete_wait_group_child(CompleteWaitGroupChildParams {
                run_id: spec.run_id,
                group_step_name,
                child_step_task_id: spec.step_task_id,
                child_step_id: spec.step_id,
                child_result: serialized,
                lock_token: spec.worker_id,
                attempt_number: spec.attempt_number,
                next_body_task_id,
                task_name: spec.task_name,
                data: spec.data,
            })
            .await?;

        Ok(())
    }
}

/// Completion behavior for wait-group child failure:
/// compare-and-set first failure flag on group and fail execution once.
pub struct FailWaitGroup;

#[async_trait]
impl CompletionBehavior for FailWaitGroup {
    async fn complete(
        &self,
        scheduler: &dyn StorageBackend,
        spec: CompletionSpec,
    ) -> Result<(), StorageError> {
        let group_step_name = match spec.wait_group_step_name {
            Some(name) => name,
            None => {
                return Err(StorageError::NotImplemented(
                    "missing wait_group_step_name for wait-group failure path",
                ));
            }
        };

        let error = match spec.outcome {
            CompletionOutcome::Failure { error } => error,
            CompletionOutcome::Success => {
                "wait-group child failed without explicit error".to_string()
            }
        };

        let was_first = scheduler
            .fail_wait_group_child(FailWaitGroupChildParams {
                run_id: spec.run_id.clone(),
                group_step_name,
                child_step_task_id: spec.step_task_id,
                child_step_id: spec.step_id,
                error,
                lock_token: spec.worker_id,
                attempt_number: spec.attempt_number,
            })
            .await?;

        if was_first {
            scheduler.fail_execution(&spec.execution_id).await?;
        }

        Ok(())
    }
}

/// Completion behavior for wait_for_event deadline path:
/// fail step task and fail execution.
pub struct FailExecutionOnDeadline;

#[async_trait]
impl CompletionBehavior for FailExecutionOnDeadline {
    async fn complete(
        &self,
        scheduler: &dyn StorageBackend,
        spec: CompletionSpec,
    ) -> Result<(), StorageError> {
        let reason = match spec.outcome {
            CompletionOutcome::Failure { error } => error,
            CompletionOutcome::Success => "event deadline exceeded".to_string(),
        };

        scheduler
            .mark_failed(&spec.step_task_id, &reason, None, &spec.worker_id)
            .await?;

        scheduler.fail_execution(&spec.execution_id).await?;
        Ok(())
    }
}
