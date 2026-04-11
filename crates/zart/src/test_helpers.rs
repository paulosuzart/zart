//! Shared test helpers for the zart crate.
//!
//! Compiled only in test mode (see `#[cfg(test)] mod test_helpers` in lib.rs).
//!
//! The centrepiece is [`RecordingScheduler`]: a mock that implements both
//! [`Scheduler`] and [`DurableStorage`], records every call it receives, and
//! returns pre-configured responses for `get_step_status` and
//! `check_wait_all_children`. Tests use it to assert *which* DB operations the
//! execution model triggers and *how many* task rows are inserted per scenario.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use scheduler::{
    CompleteAndScheduleParams, CompleteStepAndScheduleBodyParams, CompleteStepNoResumeParams,
    CompleteWaitGroupChildParams, DurableStorage, FailWaitGroupChildParams, FetchedTask,
    RescheduleStepForRetryParams, ScheduleAtParams, ScheduleResult, ScheduleStepParams, Scheduler,
    StepKind, StepLookup, StepResultKind, StorageError, TaskStatus, UpsertWaitGroupStepParams,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

// ── Recorded call enum ─────────────────────────────────────────────────────────

/// A single scheduler method invocation captured by [`RecordingScheduler`].
#[derive(Debug, Clone)]
pub enum Call {
    ScheduleAt {
        task_id: String,
        execution_time: DateTime<Utc>,
        metadata: serde_json::Value,
    },
    CompleteAndSchedule,
    MarkCompleted {
        task_id: String,
    },
    MarkFailed {
        task_id: String,
        next_execution_time: Option<DateTime<Utc>>,
    },
    CheckWaitAllChildren,
    CompleteEventStepAndScheduleBody,
    #[allow(dead_code)]
    RescheduleForRetry {
        step_task_id: String,
    },
    #[allow(dead_code)]
    CompleteStep {
        step_task_id: String,
    },
}

impl Call {
    pub fn is_schedule_at(&self) -> bool {
        matches!(self, Self::ScheduleAt { .. })
    }

    pub fn is_mark_completed(&self) -> bool {
        matches!(self, Self::MarkCompleted { .. })
    }
    pub fn is_mark_failed(&self) -> bool {
        matches!(self, Self::MarkFailed { .. })
    }
    #[allow(dead_code)]
    pub fn is_reschedule_for_retry(&self) -> bool {
        matches!(self, Self::RescheduleForRetry { .. })
    }
    #[allow(dead_code)]
    pub fn is_complete_step(&self) -> bool {
        matches!(self, Self::CompleteStep { .. })
    }
}

// ── RecordingScheduler ─────────────────────────────────────────────────────────

/// A mock scheduler that records all method calls and returns configurable responses.
///
/// Construct with [`RecordingScheduler::builder()`], configure step/wait_all
/// responses, then call `.build()` to get `(Arc<RecordingScheduler>, call_log)`.
/// After running the code under test, inspect the call log to verify the
/// correct DB operations were issued.
pub struct RecordingScheduler {
    pub calls: Arc<Mutex<Vec<Call>>>,
    step_responses: HashMap<(String, String), Option<StepLookup>>,
    wait_all_response: Vec<(String, serde_json::Value)>,
}

impl RecordingScheduler {
    pub fn builder() -> RecordingSchedulerBuilder {
        RecordingSchedulerBuilder {
            step_responses: HashMap::new(),
            wait_all_response: vec![],
        }
    }
}

pub struct RecordingSchedulerBuilder {
    step_responses: HashMap<(String, String), Option<StepLookup>>,
    wait_all_response: Vec<(String, serde_json::Value)>,
}

impl RecordingSchedulerBuilder {
    /// `get_step_status(run_id, step)` → `Ok(Some(Completed { result }))`.
    pub fn step_completed(mut self, run_id: &str, step: &str, result: serde_json::Value) -> Self {
        self.step_responses.insert(
            (run_id.into(), step.into()),
            Some(StepLookup {
                task_id: format!("{run_id}:step:{step}"),
                status: TaskStatus::Completed,
                result: Some(result),
                result_kind: Some(StepResultKind::Ok),
            }),
        );
        self
    }

    /// `get_step_status(run_id, step)` → `Ok(Some(Scheduled))` (in-flight).
    pub fn step_in_flight(mut self, run_id: &str, step: &str) -> Self {
        self.step_responses.insert(
            (run_id.into(), step.into()),
            Some(StepLookup {
                task_id: format!("{run_id}:step:{step}"),
                status: TaskStatus::Scheduled,
                result: None,
                result_kind: None,
            }),
        );
        self
    }

    /// Produce an `(Arc<RecordingScheduler>, Arc<Mutex<Vec<Call>>>)` pair.
    ///
    /// Keep both handles: pass the `Arc<RecordingScheduler>` to the code under
    /// test; inspect the call log after it runs.
    pub fn build(self) -> (Arc<RecordingScheduler>, Arc<Mutex<Vec<Call>>>) {
        let calls = Arc::new(Mutex::new(vec![]));
        let scheduler = Arc::new(RecordingScheduler {
            calls: calls.clone(),
            step_responses: self.step_responses,
            wait_all_response: self.wait_all_response,
        });
        (scheduler, calls)
    }
}

// ── Scheduler impl ─────────────────────────────────────────────────────────────

#[async_trait]
impl Scheduler for RecordingScheduler {
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

    async fn schedule_at(&self, params: ScheduleAtParams) -> Result<ScheduleResult, StorageError> {
        let execution_time = params.execution_time;
        self.calls.lock().unwrap().push(Call::ScheduleAt {
            task_id: params.task_id.clone(),
            execution_time,
            metadata: params.metadata,
        });
        Ok(ScheduleResult {
            task_id: params.task_id,
            execution_time,
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
        _next: DateTime<Utc>,
        _lock: &str,
    ) -> Result<(), StorageError> {
        Ok(())
    }

    async fn mark_completed(
        &self,
        task_id: &str,
        _result: Option<serde_json::Value>,
        _lock_token: &str,
    ) -> Result<(), StorageError> {
        self.calls.lock().unwrap().push(Call::MarkCompleted {
            task_id: task_id.to_string(),
        });
        Ok(())
    }

    async fn mark_failed(
        &self,
        task_id: &str,
        _error: &str,
        next_execution_time: Option<DateTime<Utc>>,
        _lock_token: &str,
    ) -> Result<(), StorageError> {
        self.calls.lock().unwrap().push(Call::MarkFailed {
            task_id: task_id.to_string(),
            next_execution_time,
        });
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

    async fn complete_and_schedule(
        &self,
        _params: CompleteAndScheduleParams,
    ) -> Result<(), StorageError> {
        self.calls.lock().unwrap().push(Call::CompleteAndSchedule);
        Ok(())
    }
}

// ── DurableStorage impl ─────────────────────────────────────────────────────────

#[async_trait]
impl DurableStorage for RecordingScheduler {
    async fn get_step_status(
        &self,
        run_id: &str,
        step_name: &str,
    ) -> Result<Option<StepLookup>, StorageError> {
        let key = (run_id.to_string(), step_name.to_string());
        // Not configured → Ok(None) = step row not yet inserted.
        Ok(self.step_responses.get(&key).and_then(|v| v.clone()))
    }

    async fn check_wait_all_children(
        &self,
        _wait_for_task_ids: &[String],
    ) -> Result<Vec<(String, serde_json::Value)>, StorageError> {
        self.calls.lock().unwrap().push(Call::CheckWaitAllChildren);
        Ok(self.wait_all_response.clone())
    }

    async fn complete_event_step_and_schedule_body(
        &self,
        _execution_id: &str,
        _event_name: &str,
        _payload: serde_json::Value,
    ) -> Result<bool, StorageError> {
        self.calls
            .lock()
            .unwrap()
            .push(Call::CompleteEventStepAndScheduleBody);
        Ok(true)
    }

    async fn fail_execution(&self, _execution_id: &str) -> Result<(), StorageError> {
        Ok(())
    }

    async fn schedule_step(
        &self,
        params: ScheduleStepParams,
    ) -> Result<ScheduleResult, StorageError> {
        let execution_time = params.execution_time;
        let task_id = params.task_id.clone();
        self.calls.lock().unwrap().push(Call::ScheduleAt {
            task_id: task_id.clone(),
            execution_time,
            metadata: params.metadata,
        });
        Ok(ScheduleResult {
            task_id,
            execution_time,
        })
    }

    async fn complete_step_and_schedule_body(
        &self,
        params: CompleteStepAndScheduleBodyParams,
    ) -> Result<(), StorageError> {
        let mut calls = self.calls.lock().unwrap();
        calls.push(Call::MarkCompleted {
            task_id: params.step_task_id,
        });
        let body_metadata = serde_json::json!({
            "mode": "body",
            "run_id": params.run_id,
        });
        calls.push(Call::ScheduleAt {
            task_id: params.next_body_task_id,
            execution_time: Utc::now(),
            metadata: body_metadata,
        });
        let _ = (params.result, params.lock_token);
        Ok(())
    }

    async fn complete_step_no_resume(
        &self,
        params: CompleteStepNoResumeParams,
    ) -> Result<(), StorageError> {
        self.calls.lock().unwrap().push(Call::MarkCompleted {
            task_id: params.step_task_id,
        });
        let _ = (params.result, params.lock_token);
        Ok(())
    }

    async fn reschedule_step_for_retry(
        &self,
        params: RescheduleStepForRetryParams,
    ) -> Result<(), StorageError> {
        self.calls.lock().unwrap().push(Call::MarkFailed {
            task_id: params.step_task_id,
            next_execution_time: Some(params.retry_time),
        });
        Ok(())
    }

    async fn upsert_wait_group_step(
        &self,
        _params: UpsertWaitGroupStepParams,
    ) -> Result<(), StorageError> {
        Ok(())
    }

    async fn complete_wait_group_child(
        &self,
        params: CompleteWaitGroupChildParams,
    ) -> Result<bool, StorageError> {
        self.calls.lock().unwrap().push(Call::MarkCompleted {
            task_id: params.child_step_task_id,
        });
        Ok(false)
    }

    async fn fail_wait_group_child(
        &self,
        _params: FailWaitGroupChildParams,
    ) -> Result<bool, StorageError> {
        Ok(false)
    }

    async fn insert_completed_step(
        &self,
        _run_id: &str,
        _step_name: &str,
        _step_kind: StepKind,
        _result: serde_json::Value,
    ) -> Result<(), StorageError> {
        Ok(())
    }
}

// ── Task-local test helper ─────────────────────────────────────────────────────

/// Run `fut` with both `ZART_CTX` and `ZART_PHASE` task-locals set.
///
/// Use this in unit tests that need to exercise free functions that read
/// the task-local context (e.g., `zart::context()`, `zart::step()`).
///
/// # Example
///
/// ```rust,ignore
/// let (scheduler, _) = RecordingScheduler::builder().build();
/// let ctx = Arc::new(make_body_ctx(scheduler));
///
/// let info = with_test_ctx(ctx, crate::local::Phase::Body, async {
///     zart::context()
/// }).await;
/// assert_eq!(info.execution_id, "exec-1");
/// ```
pub async fn with_test_ctx<F, T>(
    ctx: std::sync::Arc<crate::context::TaskContext>,
    phase: crate::local::Phase,
    fut: F,
) -> T
where
    F: std::future::Future<Output = T>,
{
    crate::local::ZART_CTX
        .scope(ctx, crate::local::ZART_PHASE.scope(phase, fut))
        .await
}
