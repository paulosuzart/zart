//! Task execution context — the interface through which durable step execution is managed.

use crate::error::StepError;
use crate::retry::RetryConfig;
use scheduler::Scheduler;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// The status of an individual step within a durable execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepStatus {
    /// The step has been registered and scheduled but not yet run.
    Scheduled,
    /// The step completed successfully (result is stored in DB).
    Completed,
}

/// Persisted record for a single step within a durable execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepRecord {
    /// Current lifecycle status of the step.
    pub status: StepStatus,
    /// JSON-serialized return value from the step lambda (set when `Completed`).
    pub result: Option<serde_json::Value>,
    /// Task ID of the underlying scheduled task running this step.
    pub in_task_id: Option<String>,
    /// How many times this step has been retried.
    pub retry_attempt: usize,
}

/// The persistent state associated with a durable execution.
///
/// This struct is serialized to JSON and stored in the `state` column of `zart_tasks`.
/// It is re-loaded on every re-entry so that completed steps can be skipped.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ExecutionState {
    /// Map of `step_name → StepRecord` tracking each step's progress.
    pub steps: HashMap<String, StepRecord>,
    /// Arbitrary execution-level metadata, mutable across re-entries.
    pub data: serde_json::Value,
    /// How many times the entire durable execution has been retried.
    pub retry_count: usize,
}

/// The context passed to a [`TaskHandler::run`] implementation.
///
/// Provides the step execution API (`step`, `step_with_retry`, `sleep`, etc.)
/// and access to the initial payload and execution metadata.
///
/// The context is generic over the [`Scheduler`] so that the scheduler backend
/// can be swapped (PostgreSQL, SQLite, in-memory for testing, etc.).
pub struct TaskContext<S: Scheduler> {
    /// The underlying scheduler (used to schedule step tasks).
    pub(crate) scheduler: Arc<S>,
    /// Unique identifier of the enclosing durable execution.
    execution_id: String,
    /// Registered name of the task handler.
    task_name: String,
    /// Mutable in-memory state; written back to the DB on re-schedule.
    pub(crate) state: ExecutionState,
    /// Opaque lock token from the current pick-up. Required for scheduler calls.
    pub(crate) lock_token: String,
    /// The original JSON payload supplied when the execution was started.
    data: serde_json::Value,
}

impl<S: Scheduler> TaskContext<S> {
    /// Construct a new `TaskContext`.
    ///
    /// Called by the [`Worker`] when it picks up a task.
    pub fn new(
        scheduler: Arc<S>,
        execution_id: impl Into<String>,
        task_name: impl Into<String>,
        state: ExecutionState,
        lock_token: impl Into<String>,
        data: serde_json::Value,
    ) -> Self {
        Self {
            scheduler,
            execution_id: execution_id.into(),
            task_name: task_name.into(),
            state,
            lock_token: lock_token.into(),
            data,
        }
    }

    /// Execute a named step.
    ///
    /// # Control flow
    ///
    /// - **Step absent**: persists `Scheduled`, schedules the step task, returns
    ///   `Err(StepError::Scheduled)`. The runtime catches this and exits the handler.
    /// - **Step `Scheduled`**: runs the lambda. On success, persists `Completed` and
    ///   returns `Ok(T)`.
    /// - **Step `Completed`**: deserializes the stored result and returns `Ok(T)`
    ///   immediately (lambda not called).
    ///
    /// # Errors
    ///
    /// Returns [`StepError`] on failure. [`StepError::Scheduled`] is a control-flow
    /// signal, not a real error.
    pub async fn step<T, F, Fut>(
        &mut self,
        step_name: &str,
        step_fn: F,
    ) -> Result<T, StepError>
    where
        T: Serialize + for<'de> Deserialize<'de>,
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, StepError>>,
    {
        self.step_with_retry(step_name, RetryConfig::none(), step_fn)
            .await
    }

    /// Execute a named step with a retry policy.
    ///
    /// Behaves identically to [`step`](Self::step), but retries the lambda up to
    /// `retry_config.max_attempts` times with the configured backoff on failure.
    pub async fn step_with_retry<T, F, Fut>(
        &mut self,
        step_name: &str,
        _retry_config: RetryConfig,
        step_fn: F,
    ) -> Result<T, StepError>
    where
        T: Serialize + for<'de> Deserialize<'de>,
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, StepError>>,
    {
        match self.state.steps.get(step_name) {
            // Step already completed — return cached result.
            Some(StepRecord {
                status: StepStatus::Completed,
                result: Some(v),
                ..
            }) => {
                let value = v.clone();
                let result: T = serde_json::from_value(value).map_err(|e| {
                    StepError::Failed {
                        step: step_name.to_string(),
                        reason: format!("failed to deserialize cached result: {e}"),
                    }
                })?;
                Ok(result)
            }

            // Step scheduled — execute the lambda now.
            Some(StepRecord {
                status: StepStatus::Scheduled,
                ..
            }) => {
                let result = step_fn().await?;
                let serialized = serde_json::to_value(&result).map_err(|e| {
                    StepError::Failed {
                        step: step_name.to_string(),
                        reason: format!("failed to serialize result: {e}"),
                    }
                })?;
                self.state.steps.insert(
                    step_name.to_string(),
                    StepRecord {
                        status: StepStatus::Completed,
                        result: Some(serialized),
                        in_task_id: None,
                        retry_attempt: 0,
                    },
                );
                Ok(result)
            }

            // Step not yet seen — schedule it.
            None | Some(_) => {
                self.state.steps.insert(
                    step_name.to_string(),
                    StepRecord {
                        status: StepStatus::Scheduled,
                        result: None,
                        in_task_id: None,
                        retry_attempt: 0,
                    },
                );
                Err(StepError::Scheduled {
                    step: step_name.to_string(),
                })
            }
        }
    }

    /// Suspend execution for `duration`, resuming at `now + duration`.
    ///
    /// Implemented as a step named `"__sleep"` to leverage the standard
    /// step scheduling mechanism.
    pub async fn sleep(&mut self, _duration: std::time::Duration) -> Result<(), StepError> {
        // TODO(M2): schedule a wake-up task and return StepError::Scheduled.
        todo!("Implement in M2")
    }

    /// Suspend execution until `wake_time`.
    pub async fn sleep_until(
        &mut self,
        _wake_time: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), StepError> {
        // TODO(M2): schedule a wake-up task and return StepError::Scheduled.
        todo!("Implement in M2")
    }

    /// Wait for an external event to be delivered to this execution.
    ///
    /// Blocks until [`DurableScheduler::offer_event`] is called with the matching
    /// `event_name` and `execution_id`. Optionally times out after `timeout`.
    pub async fn wait_for_event<T>(
        &mut self,
        _event_name: &str,
        _timeout: Option<std::time::Duration>,
    ) -> Result<T, StepError>
    where
        T: for<'de> Deserialize<'de>,
    {
        // TODO(M5): implement event waiting.
        todo!("Implement in M5")
    }

    /// Returns the original JSON payload provided when the execution was started.
    pub fn data(&self) -> &serde_json::Value {
        &self.data
    }

    /// Mutate the execution-level data (persisted on next re-schedule).
    pub fn set_data(&mut self, data: serde_json::Value) {
        self.data = data;
    }

    /// Returns the unique ID of the enclosing durable execution.
    pub fn execution_id(&self) -> &str {
        &self.execution_id
    }

    /// Returns the registered name of this task handler.
    pub fn task_name(&self) -> &str {
        &self.task_name
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use scheduler::{FetchedTask, Recurrence, ScheduleResult, Scheduler, StorageError};
    use std::sync::Arc;

    struct NoopScheduler;

    #[async_trait::async_trait]
    impl Scheduler for NoopScheduler {
        async fn schedule_now(
            &self,
            task_id: &str,
            _task_name: &str,
            _data: serde_json::Value,
            _execution_id: Option<&str>,
        ) -> Result<ScheduleResult, StorageError> {
            Ok(ScheduleResult {
                task_id: task_id.to_string(),
                execution_time: chrono::Utc::now(),
            })
        }

        async fn schedule_at(
            &self,
            task_id: &str,
            _task_name: &str,
            execution_time: chrono::DateTime<chrono::Utc>,
            _data: serde_json::Value,
            _recurrence: Option<Recurrence>,
            _execution_id: Option<&str>,
        ) -> Result<ScheduleResult, StorageError> {
            Ok(ScheduleResult {
                task_id: task_id.to_string(),
                execution_time,
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
            Ok(true)
        }

        async fn delete_task(&self, _task_id: &str) -> Result<(), StorageError> {
            Ok(())
        }

        async fn run_migrations(&self) -> Result<(), StorageError> {
            Ok(())
        }
    }

    fn make_ctx() -> TaskContext<NoopScheduler> {
        TaskContext::new(
            Arc::new(NoopScheduler),
            "exec-1",
            "test-task",
            ExecutionState::default(),
            "lock-token",
            serde_json::json!({}),
        )
    }

    #[tokio::test]
    async fn step_schedules_on_first_call() {
        let mut ctx = make_ctx();
        let result = ctx
            .step("my-step", || async { Ok::<i32, StepError>(42) })
            .await;
        assert!(
            matches!(result, Err(StepError::Scheduled { ref step }) if step == "my-step")
        );
    }

    #[tokio::test]
    async fn step_runs_lambda_when_scheduled() {
        let mut ctx = make_ctx();
        // Inject the step as already Scheduled.
        ctx.state.steps.insert(
            "my-step".to_string(),
            StepRecord {
                status: StepStatus::Scheduled,
                result: None,
                in_task_id: None,
                retry_attempt: 0,
            },
        );
        let result = ctx
            .step("my-step", || async { Ok::<i32, StepError>(42) })
            .await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn step_returns_cached_result_when_completed() {
        let mut ctx = make_ctx();
        ctx.state.steps.insert(
            "my-step".to_string(),
            StepRecord {
                status: StepStatus::Completed,
                result: Some(serde_json::json!(99)),
                in_task_id: None,
                retry_attempt: 0,
            },
        );
        let result: Result<i32, _> = ctx
            .step("my-step", || async {
                Ok::<i32, StepError>(0) // unreachable; step is already completed
            })
            .await;
        assert_eq!(result.unwrap(), 99);
    }
}
