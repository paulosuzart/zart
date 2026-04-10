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

        let canonical_step_id = format!("{}:step:{}", spec.run_id, spec.step_name);

        step_ops::complete_step_and_schedule_body(
            scheduler,
            step_ops::ResumeBodySpec {
                step_task_id: &spec.step_task_id,
                step_id: &canonical_step_id,
                result: serialized,
                result_kind: crate::step_types::ResultKind::Ok,
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

        let canonical_child_step_id = format!("{}:step:{}", spec.run_id, spec.step_name);

        let _triggered = scheduler
            .complete_wait_group_child(CompleteWaitGroupChildParams {
                run_id: spec.run_id,
                group_step_name,
                child_step_task_id: spec.step_task_id,
                child_step_id: canonical_child_step_id,
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

        let canonical_child_step_id = format!("{}:step:{}", spec.run_id, spec.step_name);

        let was_first = scheduler
            .fail_wait_group_child(FailWaitGroupChildParams {
                run_id: spec.run_id.clone(),
                group_step_name,
                child_step_task_id: spec.step_task_id,
                child_step_id: canonical_child_step_id,
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

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use chrono::{DateTime, Utc};
    use scheduler::{
        CompleteAndScheduleParams, DurableStorage, EventDeliveryResult, FetchedTask,
        RescheduleStepForRetryParams, ScheduleAtParams, ScheduleResult, ScheduleStepParams,
        Scheduler, StepLookup, StepRow, UpsertWaitGroupStepParams,
    };
    use std::sync::{Arc, Mutex};

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Recorded {
        FailWaitGroupChild,
        FailExecution(String),
    }

    struct TestStorage {
        calls: Arc<Mutex<Vec<Recorded>>>,
        fail_wait_group_child_result: bool,
    }

    impl TestStorage {
        fn new(fail_wait_group_child_result: bool) -> (Self, Arc<Mutex<Vec<Recorded>>>) {
            let calls = Arc::new(Mutex::new(Vec::new()));
            (
                Self {
                    calls: calls.clone(),
                    fail_wait_group_child_result,
                },
                calls,
            )
        }
    }

    #[async_trait]
    impl Scheduler for TestStorage {
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
            Ok(false)
        }

        async fn delete_task(&self, _task_id: &str) -> Result<(), StorageError> {
            Ok(())
        }

        async fn run_migrations(&self) -> Result<(), StorageError> {
            Ok(())
        }

        async fn complete_and_schedule(
            &self,
            _params: CompleteAndScheduleParams,
        ) -> Result<(), StorageError> {
            Ok(())
        }
    }

    #[async_trait]
    impl DurableStorage for TestStorage {
        async fn start_execution(
            &self,
            _execution_id: &str,
            _task_name: &str,
            _payload: serde_json::Value,
        ) -> Result<(), StorageError> {
            Ok(())
        }

        async fn complete_execution(
            &self,
            _execution_id: &str,
            _result: serde_json::Value,
        ) -> Result<(), StorageError> {
            Ok(())
        }

        async fn fail_execution(&self, execution_id: &str) -> Result<(), StorageError> {
            self.calls
                .lock()
                .unwrap()
                .push(Recorded::FailExecution(execution_id.to_string()));
            Ok(())
        }

        async fn get_execution(
            &self,
            _execution_id: &str,
        ) -> Result<Option<scheduler::ExecutionRecord>, StorageError> {
            Ok(None)
        }

        async fn cancel_execution(&self, _execution_id: &str) -> Result<bool, StorageError> {
            Ok(false)
        }

        async fn list_executions(
            &self,
            _status: Option<scheduler::ExecutionStatus>,
            _task_name: Option<&str>,
            _limit: usize,
            _offset: usize,
        ) -> Result<Vec<scheduler::ExecutionRecord>, StorageError> {
            Ok(vec![])
        }

        async fn deliver_event(
            &self,
            _execution_id: &str,
            _event_name: &str,
            _payload: serde_json::Value,
        ) -> Result<EventDeliveryResult, StorageError> {
            Ok(EventDeliveryResult::NotRegistered)
        }

        async fn reset_execution(
            &self,
            _execution_id: &str,
            _payload: serde_json::Value,
        ) -> Result<String, StorageError> {
            Ok("run:1".to_string())
        }

        async fn get_step_status(
            &self,
            _run_id: &str,
            _step_name: &str,
        ) -> Result<Option<StepLookup>, StorageError> {
            Ok(None)
        }

        async fn check_wait_all_children(
            &self,
            _wait_for_task_ids: &[String],
        ) -> Result<Vec<(String, serde_json::Value)>, StorageError> {
            Ok(vec![])
        }

        async fn get_step(
            &self,
            _run_id: &str,
            _step_name: &str,
        ) -> Result<Option<StepRow>, StorageError> {
            Ok(None)
        }

        async fn list_steps(&self, _run_id: &str) -> Result<Vec<StepRow>, StorageError> {
            Ok(vec![])
        }

        async fn upsert_wait_group_step(
            &self,
            _params: UpsertWaitGroupStepParams,
        ) -> Result<(), StorageError> {
            Ok(())
        }

        async fn complete_wait_group_child(
            &self,
            _params: CompleteWaitGroupChildParams,
        ) -> Result<bool, StorageError> {
            Ok(false)
        }

        async fn fail_wait_group_child(
            &self,
            _params: FailWaitGroupChildParams,
        ) -> Result<bool, StorageError> {
            self.calls
                .lock()
                .unwrap()
                .push(Recorded::FailWaitGroupChild);
            Ok(self.fail_wait_group_child_result)
        }

        async fn recover_wait_group_orphans(&self) -> Result<usize, StorageError> {
            Ok(0)
        }

        async fn schedule_step(
            &self,
            _params: ScheduleStepParams,
        ) -> Result<ScheduleResult, StorageError> {
            Ok(ScheduleResult {
                task_id: "noop".to_string(),
                execution_time: Utc::now(),
            })
        }

        async fn complete_step_and_schedule_body(
            &self,
            _params: scheduler::CompleteStepAndScheduleBodyParams,
        ) -> Result<(), StorageError> {
            Ok(())
        }

        async fn complete_step_no_resume(
            &self,
            _params: scheduler::CompleteStepNoResumeParams,
        ) -> Result<(), StorageError> {
            Ok(())
        }

        async fn reschedule_step_for_retry(
            &self,
            _params: RescheduleStepForRetryParams,
        ) -> Result<(), StorageError> {
            Ok(())
        }

        async fn insert_completed_step(
            &self,
            _run_id: &str,
            _step_name: &str,
            _step_kind: &str,
            _result: serde_json::Value,
        ) -> Result<(), StorageError> {
            Ok(())
        }
    }

    fn fail_wait_group_spec() -> CompletionSpec {
        CompletionSpec {
            step_task_id: "exec-1:step:child-a".to_string(),
            step_id: "exec-1:step:child-a".to_string(),
            step_name: "child-a".to_string(),
            worker_id: "lock-1".to_string(),
            task_name: "task-a".to_string(),
            run_id: "exec-1:run:0".to_string(),
            execution_id: "exec-1".to_string(),
            data: serde_json::json!({}),
            attempt_number: 1,
            result: StepResult::Transition,
            wait_group_step_name: Some("__wg__all__abc".to_string()),
            outcome: CompletionOutcome::Failure {
                error: "boom".to_string(),
            },
        }
    }

    #[tokio::test]
    async fn fail_wait_group_first_failure_routes_to_fail_execution() {
        let (storage, calls) = TestStorage::new(true);

        let res = FailWaitGroup
            .complete(&storage, fail_wait_group_spec())
            .await;
        assert!(res.is_ok());

        let log = calls.lock().unwrap().clone();
        assert_eq!(
            log,
            vec![
                Recorded::FailWaitGroupChild,
                Recorded::FailExecution("exec-1".to_string())
            ]
        );
    }

    #[tokio::test]
    async fn fail_wait_group_non_first_failure_does_not_fail_execution() {
        let (storage, calls) = TestStorage::new(false);

        let res = FailWaitGroup
            .complete(&storage, fail_wait_group_spec())
            .await;
        assert!(res.is_ok());

        let log = calls.lock().unwrap().clone();
        assert_eq!(log, vec![Recorded::FailWaitGroupChild]);
    }
}
