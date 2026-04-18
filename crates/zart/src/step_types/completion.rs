//! Completion behavior implementations for declarative step dispatch (v3 scaffold).
//!
//! This module intentionally provides a conservative, backward-compatible
//! scaffold that can be wired in during phased cutover.

use async_trait::async_trait;
use zart_scheduler::{
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
                execution_id: &spec.execution_id,
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
                execution_id: spec.execution_id,
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
    use std::sync::{Arc, Mutex};
    use zart_scheduler::pause_storage::PauseStorage;
    use zart_scheduler::{
        CompleteAndScheduleParams, CompleteStepAndScheduleBodyParams, CompleteStepNoResumeParams,
        CompleteWaitGroupChildParams, EventDeliveryResult, EventStore, ExecutionRecord,
        ExecutionRunRecord, ExecutionStats, ExecutionStore, FailWaitGroupChildParams, FetchedTask,
        ListExecutionsParams, RescheduleStepForRetryParams, ScheduleAtParams, ScheduleResult,
        ScheduleStepParams, StepAttemptRow, StepKind, StepLookup, StepRow, StepStore,
        TaskScheduler, UpsertWaitGroupStepParams, WaitGroupStore,
    };

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
    impl TaskScheduler for TestStorage {
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
            Ok(false)
        }
        async fn delete_task(&self, _: &str) -> Result<(), StorageError> {
            Ok(())
        }
        async fn run_migrations(&self) -> Result<(), StorageError> {
            Ok(())
        }
        async fn complete_and_schedule(
            &self,
            _: CompleteAndScheduleParams,
        ) -> Result<(), StorageError> {
            Ok(())
        }
    }

    #[async_trait]
    impl ExecutionStore for TestStorage {
        async fn start_execution(
            &self,
            _: &str,
            _: &str,
            _: serde_json::Value,
        ) -> Result<(), StorageError> {
            Ok(())
        }
        async fn complete_execution(
            &self,
            _: &str,
            _: serde_json::Value,
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
        async fn get_execution(&self, _: &str) -> Result<Option<ExecutionRecord>, StorageError> {
            Ok(None)
        }
        async fn cancel_execution(&self, _: &str) -> Result<bool, StorageError> {
            Ok(false)
        }
        async fn list_executions(
            &self,
            _: ListExecutionsParams,
        ) -> Result<Vec<ExecutionRecord>, StorageError> {
            Ok(vec![])
        }
        async fn get_current_run_id(&self, _: &str) -> Result<Option<String>, StorageError> {
            Ok(None)
        }
        async fn list_runs(&self, _: &str) -> Result<Vec<ExecutionRunRecord>, StorageError> {
            Ok(vec![])
        }
        async fn reset_execution(
            &self,
            _: &str,
            _: serde_json::Value,
        ) -> Result<String, StorageError> {
            Ok("run:1".to_string())
        }
        async fn retry_dead_step(
            &self,
            _: &str,
            _: &str,
            _: Option<&str>,
        ) -> Result<String, StorageError> {
            Ok(String::new())
        }
        async fn restart_run(
            &self,
            _: &str,
            _: Option<serde_json::Value>,
            _: &str,
            _: Option<&str>,
        ) -> Result<String, StorageError> {
            Ok(String::new())
        }
    }

    #[async_trait]
    impl StepStore for TestStorage {
        async fn get_step_status(
            &self,
            _: &str,
            _: &str,
        ) -> Result<Option<StepLookup>, StorageError> {
            Ok(None)
        }
        async fn get_step(&self, _: &str, _: &str) -> Result<Option<StepRow>, StorageError> {
            Ok(None)
        }
        async fn list_steps(&self, _: &str) -> Result<Vec<StepRow>, StorageError> {
            Ok(vec![])
        }
        async fn list_step_attempts(&self, _: &str) -> Result<Vec<StepAttemptRow>, StorageError> {
            Ok(vec![])
        }
        async fn schedule_step(
            &self,
            _: ScheduleStepParams,
        ) -> Result<ScheduleResult, StorageError> {
            Ok(ScheduleResult {
                task_id: "noop".to_string(),
                execution_time: Utc::now(),
            })
        }
        async fn complete_step_and_schedule_body(
            &self,
            _: CompleteStepAndScheduleBodyParams,
        ) -> Result<(), StorageError> {
            Ok(())
        }
        async fn complete_step_no_resume(
            &self,
            _: CompleteStepNoResumeParams,
        ) -> Result<(), StorageError> {
            Ok(())
        }
        async fn reschedule_step_for_retry(
            &self,
            _: RescheduleStepForRetryParams,
        ) -> Result<(), StorageError> {
            Ok(())
        }
        async fn insert_completed_step(
            &self,
            _: &str,
            _: &str,
            _: StepKind,
            _: serde_json::Value,
        ) -> Result<(), StorageError> {
            Ok(())
        }
        async fn check_wait_all_children(
            &self,
            _: &[String],
        ) -> Result<Vec<(String, serde_json::Value)>, StorageError> {
            Ok(vec![])
        }
    }

    #[async_trait]
    impl WaitGroupStore for TestStorage {
        async fn upsert_wait_group_step(
            &self,
            _: UpsertWaitGroupStepParams,
        ) -> Result<(), StorageError> {
            Ok(())
        }
        async fn complete_wait_group_child(
            &self,
            _: CompleteWaitGroupChildParams,
        ) -> Result<bool, StorageError> {
            Ok(false)
        }
        async fn fail_wait_group_child(
            &self,
            _: FailWaitGroupChildParams,
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
    }

    #[async_trait]
    impl EventStore for TestStorage {
        async fn deliver_event(
            &self,
            _: &str,
            _: &str,
            _: serde_json::Value,
        ) -> Result<EventDeliveryResult, StorageError> {
            Ok(EventDeliveryResult::NotRegistered)
        }
        async fn execution_stats(&self) -> Result<ExecutionStats, StorageError> {
            Ok(ExecutionStats {
                scheduled: 0,
                running: 0,
                completed: 0,
                failed: 0,
                cancelled: 0,
            })
        }
    }

    impl PauseStorage for TestStorage {}

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
