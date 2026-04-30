//! Free functions that implement execution-model-specific scheduling.
//!
//! These compose the generic `Scheduler` primitives (`schedule_at`,
//! `complete_and_schedule`, `mark_completed`) to perform operations specific
//! to the per-row step execution model.
//!
//! Keeping this logic here means `PostgresScheduler` remains a clean,
//! generic storage backend with no execution-model knowledge.

use crate::store::StorageBackend;
use zart_core::task_metadata::StepMetaType;
use zart_core::types::{
    CompleteStepNoResumeParams, RescheduleStepForRetryParams, ScheduleResult, ScheduleStepParams,
    StepKind,
};
use zart_core::{StorageError, TaskMetadata};

/// Parameters for [`schedule_step_task`].
pub struct StepTaskSpec<'a> {
    pub task_id: &'a str,
    pub run_id: &'a str,
    pub execution_id: &'a str,
    pub step_name: &'a str,
    pub data: serde_json::Value,
    pub retry_config: Option<&'a crate::retry::RetryConfig>,
    /// Deadline for this step (used when timeout_scope == Global).
    /// When set, written to task metadata as an RFC3339 timestamp.
    pub deadline: Option<chrono::DateTime<chrono::Utc>>,
}

/// Parameters for [`schedule_wait_group_child_task`].
pub struct WaitGroupChildSpec<'a> {
    pub task_id: &'a str,
    pub run_id: &'a str,
    pub execution_id: &'a str,
    pub step_name: &'a str,
    pub wait_group_step_name: &'a str,
    pub data: serde_json::Value,
}

/// Parameters for [`schedule_wait_for_event_task`].
pub struct EventStepSpec<'a> {
    pub task_id: &'a str,
    pub run_id: &'a str,
    pub execution_id: &'a str,
    pub event_name: &'a str,
    pub data: serde_json::Value,
    pub deadline: Option<chrono::DateTime<chrono::Utc>>,
}

/// Insert a new step task row for a sequential (non-wait_all) step.
///
/// This function is transactional: it inserts both the task row and the step row
/// atomically via the scheduler's transaction API.
pub async fn schedule_step_task(
    scheduler: &dyn StorageBackend,
    spec: StepTaskSpec<'_>,
) -> Result<ScheduleResult, StorageError> {
    let execution_id = spec.execution_id;

    let retry_config_json = spec
        .retry_config
        .map(serde_json::to_value)
        .transpose()
        .map_err(|e| StorageError::Database(Box::new(e)))?;

    let metadata = TaskMetadata::Step {
        step_type: StepMetaType::Step,
        run_id: spec.run_id.to_string(),
        execution_id: execution_id.to_string(),
        step_name: spec.step_name.to_string(),
        retry_attempt: 0,
        retry_config: retry_config_json.clone(),
        deadline: spec.deadline.map(|d| d.to_rfc3339()),
        is_wait_all_child: None,
        wg_step_name: None,
    }
    .to_json_value();

    scheduler
        .schedule_step(ScheduleStepParams {
            task_id: spec.task_id.to_string(),
            task_name: crate::TASK_NAME.to_string(),
            run_id: spec.run_id.to_string(),
            step_name: spec.step_name.to_string(),
            step_kind: StepKind::Step,
            execution_time: chrono::Utc::now(),
            data: spec.data,
            metadata,
            retry_config: retry_config_json,
        })
        .await
}

/// Reschedule a failed step task for retry after a delay.
///
/// Records the failed attempt, updates the retry count on the step row, and
/// reschedules the task to be picked up after `retry_time`.
pub async fn reschedule_step_for_retry(
    scheduler: &dyn StorageBackend,
    step_task_id: &str,
    attempt_number: usize,
    error: &str,
    retry_time: chrono::DateTime<chrono::Utc>,
    lock_token: &str,
) -> Result<(), StorageError> {
    scheduler
        .reschedule_step_for_retry(RescheduleStepForRetryParams {
            step_task_id: step_task_id.to_string(),
            attempt_number,
            error: error.to_string(),
            retry_time,
            lock_token: lock_token.to_string(),
        })
        .await
}

/// Insert a wait-group child step task.
///
/// This is the canonical helper for scheduling child rows used by wait-group
/// barrier body behavior.
pub async fn schedule_wait_group_child_task(
    scheduler: &dyn StorageBackend,
    spec: WaitGroupChildSpec<'_>,
) -> Result<ScheduleResult, StorageError> {
    let metadata = TaskMetadata::Step {
        step_type: StepMetaType::Step,
        run_id: spec.run_id.to_string(),
        execution_id: spec.execution_id.to_string(),
        step_name: spec.step_name.to_string(),
        retry_attempt: 0,
        retry_config: None,
        deadline: None,
        is_wait_all_child: Some(true),
        wg_step_name: Some(spec.wait_group_step_name.to_string()),
    }
    .to_json_value();

    let mut data_obj = match spec.data {
        serde_json::Value::Object(map) => map,
        _ => serde_json::Map::new(),
    };
    data_obj.insert(
        "wg_step_name".to_string(),
        serde_json::Value::String(spec.wait_group_step_name.to_string()),
    );
    let data = serde_json::Value::Object(data_obj);

    scheduler
        .schedule_step(ScheduleStepParams {
            task_id: spec.task_id.to_string(),
            task_name: crate::TASK_NAME.to_string(),
            run_id: spec.run_id.to_string(),
            step_name: spec.step_name.to_string(),
            step_kind: StepKind::Step,
            execution_time: chrono::Utc::now(),
            data,
            metadata,
            retry_config: None,
        })
        .await
}

/// Backward-compatible alias for legacy `wait_all` call sites.
///
/// Prefer [`schedule_wait_group_child_task`] for new code.
pub async fn schedule_wait_all_child(
    scheduler: &dyn StorageBackend,
    spec: WaitGroupChildSpec<'_>,
) -> Result<ScheduleResult, StorageError> {
    schedule_wait_group_child_task(scheduler, spec).await
}

/// Complete a wait_all child step without scheduling a body continuation.
///
/// Wait-group completion behavior handles parent progression.
pub async fn complete_step_no_resume(
    scheduler: &dyn StorageBackend,
    step_task_id: &str,
    step_id: &str,
    result: serde_json::Value,
    lock_token: &str,
    attempt_number: usize,
) -> Result<(), StorageError> {
    scheduler
        .complete_step_no_resume(CompleteStepNoResumeParams {
            step_task_id: step_task_id.to_string(),
            step_id: step_id.to_string(),
            result,
            lock_token: lock_token.to_string(),
            attempt_number,
        })
        .await
}

/// Insert a wait_for_event step task row.
///
/// If `spec.deadline` is `None`, the task is scheduled for `DateTime::MAX_UTC`
/// (year 262142) so it effectively never fires unless `offer_event` arrives first.
///
/// **Trade-off**: using a sentinel far-future timestamp is simpler than a dedicated
/// `parked` status or a nullable `execution_time`, but it means the scheduler's
/// `poll_due` query will never return this row under normal conditions. The row is
/// only completed via `complete_event_step_and_schedule_body`, which bypasses the
/// `execution_time` check entirely. Operators querying for "scheduled" tasks with
/// far-future times can identify these as pending event waits.
pub async fn schedule_wait_for_event_task(
    scheduler: &dyn StorageBackend,
    spec: EventStepSpec<'_>,
) -> Result<ScheduleResult, StorageError> {
    let execution_time = spec
        .deadline
        .unwrap_or(chrono::DateTime::<chrono::Utc>::MAX_UTC);
    let execution_id = spec.execution_id;
    let metadata = TaskMetadata::Step {
        step_type: StepMetaType::WaitForEvent,
        run_id: spec.run_id.to_string(),
        execution_id: execution_id.to_string(),
        step_name: spec.event_name.to_string(),
        retry_attempt: 0,
        retry_config: None,
        deadline: None,
        is_wait_all_child: None,
        wg_step_name: None,
    }
    .to_json_value();

    scheduler
        .schedule_step(ScheduleStepParams {
            task_id: spec.task_id.to_string(),
            task_name: crate::TASK_NAME.to_string(),
            run_id: spec.run_id.to_string(),
            step_name: spec.event_name.to_string(),
            step_kind: StepKind::WaitForEvent,
            execution_time,
            data: spec.data,
            metadata,
            retry_config: None,
        })
        .await
}

/// Schedule a sleep continuation task.
pub async fn schedule_sleep_task(
    scheduler: &dyn StorageBackend,
    sleep_task_id: &str,
    run_id: &str,
    execution_id: &str,
    step_name: &str,
    wake_time: chrono::DateTime<chrono::Utc>,
    data: serde_json::Value,
) -> Result<ScheduleResult, StorageError> {
    let metadata = TaskMetadata::Step {
        step_type: StepMetaType::Sleep,
        run_id: run_id.to_string(),
        execution_id: execution_id.to_string(),
        step_name: step_name.to_string(),
        retry_attempt: 0,
        retry_config: None,
        deadline: None,
        is_wait_all_child: None,
        wg_step_name: None,
    }
    .to_json_value();

    scheduler
        .schedule_step(ScheduleStepParams {
            task_id: sleep_task_id.to_string(),
            task_name: crate::TASK_NAME.to_string(),
            run_id: run_id.to_string(),
            step_name: step_name.to_string(),
            step_kind: StepKind::Sleep,
            execution_time: wake_time,
            data,
            metadata,
            retry_config: None,
        })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use chrono::Utc;
    use std::sync::{Arc, Mutex};
    use zart_core::store::pause_storage::PauseStorage;
    use zart_core::store::{EventStore, ExecutionStore, StepStore, WaitGroupStore};
    use zart_core::types::{
        CompleteStepNoResumeParams, CompleteWaitGroupChildParams, EventDeliveryResult,
        ExecutionRecord, ExecutionRunRecord, ExecutionStats, FailWaitGroupChildParams, FetchedTask,
        ListExecutionsParams, RescheduleStepForRetryParams, ScheduleAtParams, StepAttemptRow,
        StepKind, StepLookup, StepRow, UpsertWaitGroupStepParams,
    };
    use zart_core::{StorageError, TaskMetadata};
    use zart_scheduler::TaskScheduler;

    struct CapturingStorage {
        last_metadata: Arc<Mutex<Option<serde_json::Value>>>,
    }

    impl CapturingStorage {
        fn new() -> Self {
            Self {
                last_metadata: Arc::new(Mutex::new(None)),
            }
        }

        fn captured_metadata(&self) -> Option<serde_json::Value> {
            self.last_metadata.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl TaskScheduler for CapturingStorage {
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
            _now: chrono::DateTime<chrono::Utc>,
            _limit: usize,
        ) -> Result<Vec<FetchedTask>, StorageError> {
            Ok(vec![])
        }

        async fn update_task_state(
            &self,
            _task_id: &str,
            _state: serde_json::Value,
            _next_execution_time: chrono::DateTime<chrono::Utc>,
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
            _next_execution_time: Option<chrono::DateTime<chrono::Utc>>,
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
    }

    #[async_trait]
    impl ExecutionStore for CapturingStorage {
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
        async fn fail_execution(&self, _: &str) -> Result<(), StorageError> {
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
            Ok(String::new())
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
    impl StepStore for CapturingStorage {
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
            params: ScheduleStepParams,
        ) -> Result<ScheduleResult, StorageError> {
            *self.last_metadata.lock().unwrap() = Some(params.metadata.clone());
            Ok(ScheduleResult {
                task_id: params.task_id,
                execution_time: params.execution_time,
            })
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

        async fn copy_steps_to_run(
            &self,
            _from: &str,
            _to: &str,
            _names: &[String],
        ) -> Result<(), StorageError> {
            Ok(())
        }
    }

    #[async_trait]
    impl WaitGroupStore for CapturingStorage {
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
            Ok(false)
        }
        async fn recover_wait_group_orphans(&self) -> Result<usize, StorageError> {
            Ok(0)
        }
    }

    #[async_trait]
    impl EventStore for CapturingStorage {
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

    impl PauseStorage for CapturingStorage {}

    #[tokio::test]
    async fn schedule_wait_group_child_task_writes_wg_step_name_metadata_key() {
        let storage = CapturingStorage::new();

        let result = schedule_wait_group_child_task(
            &storage,
            WaitGroupChildSpec {
                task_id: "exec-1:step:child-a",
                run_id: "exec-1:run:0",
                execution_id: "exec-1",
                step_name: "child-a",
                wait_group_step_name: "__wg__all__group-1",
                data: serde_json::json!({"x": 1}),
            },
        )
        .await;

        assert!(result.is_ok());

        let raw = storage
            .captured_metadata()
            .expect("expected schedule_step metadata to be captured");

        let meta = TaskMetadata::from_json_value(raw)
            .expect("captured metadata must parse as TaskMetadata");

        assert_eq!(meta.run_id(), "exec-1:run:0");
        assert_eq!(meta.execution_id(), "exec-1");
        assert_eq!(meta.step_name(), Some("child-a"));
        assert!(meta.is_wait_all_child(), "is_wait_all_child must be true");
        assert_eq!(meta.wg_step_name(), Some("__wg__all__group-1"));
    }
}
