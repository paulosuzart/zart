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
    DurableStorage, FetchedTask, Recurrence, ScheduleResult, Scheduler, StepLookup, StorageError,
    TaskStatus,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

// ── Recorded call enum ─────────────────────────────────────────────────────────

/// A single scheduler method invocation captured by [`RecordingScheduler`].
#[derive(Debug, Clone)]
pub enum Call {
    ScheduleAt {
        task_id: String,
        task_name: String,
        execution_time: DateTime<Utc>,
        execution_id: Option<String>,
        metadata: serde_json::Value,
    },
    CompleteAndSchedule {
        completed_task_id: String,
        result: Option<serde_json::Value>,
        new_task_id: String,
        new_task_name: String,
        new_execution_time: DateTime<Utc>,
        new_execution_id: Option<String>,
        new_metadata: serde_json::Value,
    },
    MarkCompleted {
        task_id: String,
        result: Option<serde_json::Value>,
    },
    MarkFailed {
        task_id: String,
        error: String,
        next_execution_time: Option<DateTime<Utc>>,
    },
    CheckWaitAllChildren {
        task_ids: Vec<String>,
    },
}

impl Call {
    pub fn is_schedule_at(&self) -> bool {
        matches!(self, Self::ScheduleAt { .. })
    }
    pub fn is_complete_and_schedule(&self) -> bool {
        matches!(self, Self::CompleteAndSchedule { .. })
    }
    pub fn is_mark_completed(&self) -> bool {
        matches!(self, Self::MarkCompleted { .. })
    }
    pub fn is_mark_failed(&self) -> bool {
        matches!(self, Self::MarkFailed { .. })
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
    /// `get_step_status(exec_id, step)` → `Ok(None)` (step row not yet inserted).
    pub fn step_not_found(mut self, exec_id: &str, step: &str) -> Self {
        self.step_responses.insert((exec_id.into(), step.into()), None);
        self
    }

    /// `get_step_status(exec_id, step)` → `Ok(Some(Completed { result }))`.
    pub fn step_completed(mut self, exec_id: &str, step: &str, result: serde_json::Value) -> Self {
        self.step_responses.insert(
            (exec_id.into(), step.into()),
            Some(StepLookup {
                task_id: format!("{exec_id}:step:{step}"),
                status: TaskStatus::Completed,
                result: Some(result),
            }),
        );
        self
    }

    /// `get_step_status(exec_id, step)` → `Ok(Some(Scheduled))` (in-flight).
    pub fn step_in_flight(mut self, exec_id: &str, step: &str) -> Self {
        self.step_responses.insert(
            (exec_id.into(), step.into()),
            Some(StepLookup {
                task_id: format!("{exec_id}:step:{step}"),
                status: TaskStatus::Scheduled,
                result: None,
            }),
        );
        self
    }

    /// `check_wait_all_children(...)` → returns these completed `(task_id, result)` pairs.
    pub fn wait_all_returns(mut self, results: Vec<(String, serde_json::Value)>) -> Self {
        self.wait_all_response = results;
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
        _execution_id: Option<&str>,
    ) -> Result<ScheduleResult, StorageError> {
        Ok(ScheduleResult { task_id: task_id.to_string(), execution_time: Utc::now() })
    }

    async fn schedule_at(
        &self,
        task_id: &str,
        task_name: &str,
        execution_time: DateTime<Utc>,
        _data: serde_json::Value,
        _recurrence: Option<Recurrence>,
        execution_id: Option<&str>,
        metadata: serde_json::Value,
    ) -> Result<ScheduleResult, StorageError> {
        self.calls.lock().unwrap().push(Call::ScheduleAt {
            task_id: task_id.to_string(),
            task_name: task_name.to_string(),
            execution_time,
            execution_id: execution_id.map(String::from),
            metadata,
        });
        Ok(ScheduleResult { task_id: task_id.to_string(), execution_time })
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
        result: Option<serde_json::Value>,
        _lock_token: &str,
    ) -> Result<(), StorageError> {
        self.calls
            .lock()
            .unwrap()
            .push(Call::MarkCompleted { task_id: task_id.to_string(), result });
        Ok(())
    }

    async fn mark_failed(
        &self,
        task_id: &str,
        error: &str,
        next_execution_time: Option<DateTime<Utc>>,
        _lock_token: &str,
    ) -> Result<(), StorageError> {
        self.calls.lock().unwrap().push(Call::MarkFailed {
            task_id: task_id.to_string(),
            error: error.to_string(),
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
        completed_task_id: &str,
        result: Option<serde_json::Value>,
        _lock_token: &str,
        new_task_id: &str,
        new_task_name: &str,
        new_execution_time: DateTime<Utc>,
        _new_data: serde_json::Value,
        new_execution_id: Option<&str>,
        new_metadata: serde_json::Value,
    ) -> Result<(), StorageError> {
        self.calls.lock().unwrap().push(Call::CompleteAndSchedule {
            completed_task_id: completed_task_id.to_string(),
            result,
            new_task_id: new_task_id.to_string(),
            new_task_name: new_task_name.to_string(),
            new_execution_time,
            new_execution_id: new_execution_id.map(String::from),
            new_metadata,
        });
        Ok(())
    }
}

// ── DurableStorage impl ─────────────────────────────────────────────────────────

#[async_trait]
impl DurableStorage for RecordingScheduler {
    async fn get_step_status(
        &self,
        execution_id: &str,
        step_name: &str,
    ) -> Result<Option<StepLookup>, StorageError> {
        let key = (execution_id.to_string(), step_name.to_string());
        // Not configured → Ok(None) = step row not yet inserted.
        Ok(self.step_responses.get(&key).and_then(|v| v.clone()))
    }

    async fn check_wait_all_children(
        &self,
        wait_for_task_ids: &[String],
    ) -> Result<Vec<(String, serde_json::Value)>, StorageError> {
        self.calls.lock().unwrap().push(Call::CheckWaitAllChildren {
            task_ids: wait_for_task_ids.to_vec(),
        });
        Ok(self.wait_all_response.clone())
    }
}
