//! Task execution context — the interface through which durable step execution is managed.

use crate::error::StepError;
use crate::retry::RetryConfig;
use scheduler::Scheduler;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

// ── Attempt history ──────────────────────────────────────────────────────────

/// The outcome of a single step execution attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptStatus {
    /// The attempt is still in progress (transient, not persisted).
    Running,
    /// The attempt completed successfully.
    Completed,
    /// The attempt failed.
    Failed,
}

/// A record of one execution attempt for a step.
///
/// Each retry produces a new `StepAttempt`, preserving the full history
/// for observability: "Attempt 1 failed with X; Attempt 2 succeeded."
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepAttempt {
    /// 1-indexed attempt number (1 = first try, 2 = first retry, …).
    pub attempt_number: usize,
    /// When this attempt started executing.
    pub started_at: chrono::DateTime<chrono::Utc>,
    /// When this attempt finished (None if still running).
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Outcome of this attempt.
    pub status: AttemptStatus,
    /// Error message if the attempt failed.
    pub error: Option<String>,
    /// JSON result if the attempt succeeded.
    pub result: Option<serde_json::Value>,
}

// ── Step record ──────────────────────────────────────────────────────────────

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
    /// How many retries have been attempted (0 = no retries yet).
    pub retry_attempt: usize,
    /// The retry policy configured for this step (persisted for observability).
    pub retry_config: Option<RetryConfig>,
    /// Per-attempt history for observability.
    pub attempts: Vec<StepAttempt>,
}

// ── Execution state ──────────────────────────────────────────────────────────

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

// ── TaskContext ───────────────────────────────────────────────────────────────

/// The context passed to a [`TaskHandler::run`] implementation.
///
/// Provides the step execution API (`step`, `step_with_retry`, `step_with_timeout`, …)
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

    /// Execute a named step with no retries and no timeout.
    ///
    /// # Control flow
    ///
    /// - **Step absent**: persists `Scheduled`, returns `Err(StepError::Scheduled)`.
    ///   The runtime re-queues the task immediately.
    /// - **Step `Scheduled`**: runs the lambda. On success, persists `Completed`.
    /// - **Step `Completed`**: deserializes the stored result and returns `Ok(T)`
    ///   immediately (lambda not called).
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
    /// Behaves like [`step`](Self::step), but retries the lambda across re-entries
    /// (each retry is a separate task execution) with the configured backoff.
    ///
    /// Each attempt is recorded in [`StepRecord::attempts`] for observability.
    pub async fn step_with_retry<T, F, Fut>(
        &mut self,
        step_name: &str,
        retry_config: RetryConfig,
        step_fn: F,
    ) -> Result<T, StepError>
    where
        T: Serialize + for<'de> Deserialize<'de>,
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, StepError>>,
    {
        // Clone what we need so we don't hold an immutable borrow into the async call.
        let snapshot = self.state.steps.get(step_name).map(|r| {
            (r.status.clone(), r.result.clone(), r.retry_attempt)
        });

        match snapshot {
            // ── COMPLETED: return the cached result ───────────────────────────
            Some((StepStatus::Completed, Some(v), _)) => {
                serde_json::from_value(v).map_err(|e| StepError::Failed {
                    step: step_name.to_string(),
                    reason: format!("failed to deserialize cached result: {e}"),
                })
            }

            Some((StepStatus::Completed, None, _)) => Err(StepError::Failed {
                step: step_name.to_string(),
                reason: "step completed but result is missing".to_string(),
            }),

            // ── SCHEDULED: execute the lambda ─────────────────────────────────
            Some((StepStatus::Scheduled, _, retry_attempt)) => {
                let started_at = chrono::Utc::now();
                let outcome = step_fn().await;
                let completed_at = chrono::Utc::now();

                match outcome {
                    Ok(result) => {
                        let serialized = serde_json::to_value(&result).map_err(|e| {
                            StepError::Failed {
                                step: step_name.to_string(),
                                reason: format!("failed to serialize result: {e}"),
                            }
                        })?;

                        let record = self
                            .state
                            .steps
                            .get_mut(step_name)
                            .expect("step must exist in state");
                        record.status = StepStatus::Completed;
                        record.result = Some(serialized.clone());
                        record.attempts.push(StepAttempt {
                            attempt_number: retry_attempt + 1,
                            started_at,
                            completed_at: Some(completed_at),
                            status: AttemptStatus::Completed,
                            error: None,
                            result: Some(serialized),
                        });

                        Ok(result)
                    }

                    Err(e) => {
                        let error_str = e.to_string();
                        let next_attempt = retry_attempt + 1;

                        let record = self
                            .state
                            .steps
                            .get_mut(step_name)
                            .expect("step must exist in state");
                        record.attempts.push(StepAttempt {
                            attempt_number: retry_attempt + 1,
                            started_at,
                            completed_at: Some(completed_at),
                            status: AttemptStatus::Failed,
                            error: Some(error_str),
                            result: None,
                        });

                        // Check whether the retry policy allows another attempt.
                        if let Some(delay) = retry_config.delay_for(next_attempt) {
                            let next_execution = chrono::Utc::now()
                                + chrono::Duration::from_std(delay)
                                    .unwrap_or(chrono::Duration::zero());
                            record.retry_attempt = next_attempt;
                            Err(StepError::Scheduled {
                                step: step_name.to_string(),
                                next_execution: Some(next_execution),
                            })
                        } else if retry_config.max_attempts > 0 {
                            // Retries were configured and all exhausted.
                            Err(StepError::RetryExhausted {
                                step: step_name.to_string(),
                                attempts: next_attempt,
                            })
                        } else {
                            // No retries configured — propagate the original error.
                            Err(e)
                        }
                    }
                }
            }

            // ── ABSENT: register and schedule the step for the first time ─────
            None => {
                self.state.steps.insert(
                    step_name.to_string(),
                    StepRecord {
                        status: StepStatus::Scheduled,
                        result: None,
                        in_task_id: None,
                        retry_attempt: 0,
                        retry_config: Some(retry_config),
                        attempts: vec![],
                    },
                );
                Err(StepError::Scheduled {
                    step: step_name.to_string(),
                    next_execution: None,
                })
            }
        }
    }

    /// Execute a named step with a wall-clock timeout.
    ///
    /// If the lambda does not complete within `timeout`, the step is marked as
    /// [`StepError::Timeout`] — a real error, not a control-flow signal.
    /// No retries are applied; combine with [`step_with_retry`](Self::step_with_retry)
    /// at the call-site if both are needed.
    pub async fn step_with_timeout<T, F, Fut>(
        &mut self,
        step_name: &str,
        timeout: std::time::Duration,
        step_fn: F,
    ) -> Result<T, StepError>
    where
        T: Serialize + for<'de> Deserialize<'de>,
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, StepError>>,
    {
        let step_name_owned = step_name.to_string();
        self.step(step_name, move || async move {
            tokio::time::timeout(timeout, step_fn())
                .await
                .map_err(|_| StepError::Timeout {
                    step: step_name_owned,
                    duration: timeout,
                })?
        })
        .await
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

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use scheduler::{FetchedTask, Recurrence, ScheduleResult, Scheduler, StorageError};
    use std::sync::Arc;
    use std::time::Duration;

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

    // ── Basic step control-flow ───────────────────────────────────────────────

    #[tokio::test]
    async fn step_schedules_on_first_call() {
        let mut ctx = make_ctx();
        let result = ctx
            .step("my-step", || async { Ok::<i32, StepError>(42) })
            .await;
        assert!(
            matches!(result, Err(StepError::Scheduled { ref step, next_execution: None }) if step == "my-step")
        );
    }

    #[tokio::test]
    async fn step_runs_lambda_when_scheduled() {
        let mut ctx = make_ctx();
        ctx.state.steps.insert(
            "my-step".to_string(),
            StepRecord {
                status: StepStatus::Scheduled,
                result: None,
                in_task_id: None,
                retry_attempt: 0,
                retry_config: None,
                attempts: vec![],
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
                retry_config: None,
                attempts: vec![],
            },
        );
        let result: Result<i32, _> = ctx
            .step("my-step", || async {
                Ok::<i32, StepError>(0) // unreachable; step is already completed
            })
            .await;
        assert_eq!(result.unwrap(), 99);
    }

    #[tokio::test]
    async fn step_records_attempt_on_success() {
        let mut ctx = make_ctx();
        ctx.state.steps.insert(
            "my-step".to_string(),
            StepRecord {
                status: StepStatus::Scheduled,
                result: None,
                in_task_id: None,
                retry_attempt: 0,
                retry_config: None,
                attempts: vec![],
            },
        );
        let _ = ctx
            .step("my-step", || async { Ok::<i32, StepError>(7) })
            .await;
        let record = ctx.state.steps.get("my-step").unwrap();
        assert_eq!(record.attempts.len(), 1);
        assert_eq!(record.attempts[0].status, AttemptStatus::Completed);
        assert_eq!(record.attempts[0].attempt_number, 1);
    }

    // ── Retry logic ───────────────────────────────────────────────────────────

    #[tokio::test]
    async fn step_with_retry_returns_scheduled_on_failure_when_retries_remain() {
        let mut ctx = make_ctx();
        // Step is already in Scheduled state (first execution).
        ctx.state.steps.insert(
            "retry-step".to_string(),
            StepRecord {
                status: StepStatus::Scheduled,
                result: None,
                in_task_id: None,
                retry_attempt: 0,
                retry_config: None,
                attempts: vec![],
            },
        );

        let result = ctx
            .step_with_retry(
                "retry-step",
                RetryConfig::fixed(2, Duration::from_millis(100)),
                || async {
                    Err::<i32, _>(StepError::Failed {
                        step: "retry-step".to_string(),
                        reason: "transient error".to_string(),
                    })
                },
            )
            .await;

        // Should return Scheduled (control-flow) with a delay since retries remain.
        assert!(matches!(
            result,
            Err(StepError::Scheduled { next_execution: Some(_), .. })
        ));

        // Retry attempt counter must be bumped.
        let record = ctx.state.steps.get("retry-step").unwrap();
        assert_eq!(record.retry_attempt, 1);
        assert_eq!(record.attempts.len(), 1);
        assert_eq!(record.attempts[0].status, AttemptStatus::Failed);
    }

    #[tokio::test]
    async fn step_with_retry_exhausts_after_max_attempts() {
        let mut ctx = make_ctx();
        // Simulate: retry_attempt is already at max (2 out of 2 retries used).
        ctx.state.steps.insert(
            "retry-step".to_string(),
            StepRecord {
                status: StepStatus::Scheduled,
                result: None,
                in_task_id: None,
                retry_attempt: 2, // max_attempts = 2, so next would be attempt 3 which is > max
                retry_config: None,
                attempts: vec![],
            },
        );

        let result = ctx
            .step_with_retry(
                "retry-step",
                RetryConfig::fixed(2, Duration::from_millis(100)),
                || async {
                    Err::<i32, _>(StepError::Failed {
                        step: "retry-step".to_string(),
                        reason: "still failing".to_string(),
                    })
                },
            )
            .await;

        assert!(matches!(
            result,
            Err(StepError::RetryExhausted { attempts: 3, .. })
        ));
    }

    #[tokio::test]
    async fn step_with_retry_succeeds_after_previous_failures() {
        let mut ctx = make_ctx();
        // Step was previously retried once, now in Scheduled state for its 2nd attempt.
        ctx.state.steps.insert(
            "retry-step".to_string(),
            StepRecord {
                status: StepStatus::Scheduled,
                result: None,
                in_task_id: None,
                retry_attempt: 1,
                retry_config: None,
                attempts: vec![StepAttempt {
                    attempt_number: 1,
                    started_at: chrono::Utc::now(),
                    completed_at: Some(chrono::Utc::now()),
                    status: AttemptStatus::Failed,
                    error: Some("first failure".to_string()),
                    result: None,
                }],
            },
        );

        let result = ctx
            .step_with_retry(
                "retry-step",
                RetryConfig::fixed(2, Duration::from_millis(100)),
                || async { Ok::<i32, StepError>(42) },
            )
            .await;

        assert_eq!(result.unwrap(), 42);
        let record = ctx.state.steps.get("retry-step").unwrap();
        assert_eq!(record.status, StepStatus::Completed);
        assert_eq!(record.attempts.len(), 2);
        assert_eq!(record.attempts[1].status, AttemptStatus::Completed);
    }

    // ── Timeout ───────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn step_with_timeout_completes_within_deadline() {
        let mut ctx = make_ctx();
        ctx.state.steps.insert(
            "fast-step".to_string(),
            StepRecord {
                status: StepStatus::Scheduled,
                result: None,
                in_task_id: None,
                retry_attempt: 0,
                retry_config: None,
                attempts: vec![],
            },
        );
        let result = ctx
            .step_with_timeout(
                "fast-step",
                Duration::from_secs(5),
                || async { Ok::<i32, StepError>(10) },
            )
            .await;
        assert_eq!(result.unwrap(), 10);
    }

    #[tokio::test]
    async fn step_with_timeout_returns_timeout_error_when_exceeded() {
        let mut ctx = make_ctx();
        ctx.state.steps.insert(
            "slow-step".to_string(),
            StepRecord {
                status: StepStatus::Scheduled,
                result: None,
                in_task_id: None,
                retry_attempt: 0,
                retry_config: None,
                attempts: vec![],
            },
        );
        let result = ctx
            .step_with_timeout(
                "slow-step",
                Duration::from_millis(1),
                || async {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    Ok::<i32, StepError>(99)
                },
            )
            .await;
        assert!(matches!(result, Err(StepError::Timeout { .. })));
    }

    // ── Retry config serde ────────────────────────────────────────────────────

    #[test]
    fn retry_config_round_trips_through_json() {
        let cfg = RetryConfig::exponential(3, Duration::from_secs(2));
        let json = serde_json::to_string(&cfg).unwrap();
        let back: RetryConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(back.max_attempts, 3);
        assert_eq!(back.initial_delay, Duration::from_secs(2));
        assert_eq!(back.backoff_multiplier, 2.0);
    }

    #[test]
    fn execution_state_with_attempts_round_trips_through_json() {
        let mut state = ExecutionState::default();
        state.steps.insert(
            "s".to_string(),
            StepRecord {
                status: StepStatus::Completed,
                result: Some(serde_json::json!(1)),
                in_task_id: None,
                retry_attempt: 1,
                retry_config: Some(RetryConfig::fixed(2, Duration::from_millis(500))),
                attempts: vec![
                    StepAttempt {
                        attempt_number: 1,
                        started_at: chrono::Utc::now(),
                        completed_at: Some(chrono::Utc::now()),
                        status: AttemptStatus::Failed,
                        error: Some("oops".to_string()),
                        result: None,
                    },
                    StepAttempt {
                        attempt_number: 2,
                        started_at: chrono::Utc::now(),
                        completed_at: Some(chrono::Utc::now()),
                        status: AttemptStatus::Completed,
                        error: None,
                        result: Some(serde_json::json!(1)),
                    },
                ],
            },
        );

        let json = serde_json::to_string(&state).unwrap();
        let back: ExecutionState = serde_json::from_str(&json).unwrap();
        let record = back.steps.get("s").unwrap();
        assert_eq!(record.attempts.len(), 2);
        assert_eq!(record.retry_attempt, 1);
    }
}
