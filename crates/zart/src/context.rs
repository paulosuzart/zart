//! Task execution context — the interface through which durable step execution is managed.

use crate::error::StepError;
use crate::execution_model::ExecutionMode;
use crate::metrics::{STEP_DURATION_SECONDS, STEPS_TOTAL};
use crate::retry::RetryConfig;
use crate::step_ops;
use scheduler::{DurableStorage, Scheduler, StepLookup, TaskStatus};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tracing::instrument;

// ── Internal type alias ───────────────────────────────────────────────────────

/// A boxed, one-shot async function that yields a JSON-serialized step result.
/// Used internally by [`StepHandle`] to store a pending step lambda.
type PendingFn = Box<
    dyn FnOnce() -> Pin<
            Box<dyn Future<Output = Result<serde_json::Value, StepError>> + Send + 'static>,
        > + Send
        + 'static,
>;

// ── StepHandle ────────────────────────────────────────────────────────────────

/// A handle to a step registered for parallel execution via [`TaskContext::schedule_step`].
///
/// Collect handles from multiple `schedule_step` calls and pass them to
/// [`TaskContext::wait_all`] to execute them and collect results.
pub struct StepHandle<T> {
    pub(crate) step_name: String,
    /// The step lambda wrapped to produce a JSON value. `None` if the step is
    /// already completed (result is cached in state).
    pub(crate) pending: Option<PendingFn>,
    pub(crate) _marker: std::marker::PhantomData<fn() -> T>,
}

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
    /// The step is blocked waiting for an external event via `wait_for_event`.
    WaitingForEvent,
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
    /// For event-waiting steps: UTC deadline after which the wait times out.
    /// `None` means the step waits indefinitely.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_deadline: Option<chrono::DateTime<chrono::Utc>>,
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
    /// Buffered external events delivered via `offer_event`, keyed by event name.
    /// Consumed by `wait_for_event` on re-entry.
    #[serde(default)]
    pub pending_events: HashMap<String, serde_json::Value>,
}

// ── TaskContext ───────────────────────────────────────────────────────────────

/// The context passed to a [`TaskHandler::run`] implementation.
///
/// Provides the step execution API (`step`, `step_with_retry`, `step_with_timeout`, …)
/// and access to the initial payload and execution metadata.
///
/// The context is generic over the [`Scheduler`] so that the scheduler backend
/// can be swapped (PostgreSQL, SQLite, in-memory for testing, etc.).
pub struct TaskContext<S: Scheduler + DurableStorage> {
    /// The underlying scheduler (used to schedule step tasks).
    pub(crate) scheduler: Arc<S>,
    /// Unique identifier of the enclosing durable execution.
    execution_id: String,
    /// The `task_id` of the task currently being executed.
    /// Differs from `execution_id` for step/body-segment tasks in the new model.
    pub(crate) task_id: String,
    /// Registered name of the task handler.
    task_name: String,
    /// Mutable in-memory state; written back to the DB on re-schedule.
    pub(crate) state: ExecutionState,
    /// Opaque lock token from the current pick-up. Required for scheduler calls.
    pub(crate) lock_token: String,
    /// The original JSON payload supplied when the execution was started.
    data: serde_json::Value,
    /// When true, `step()` behaves like `step_immediate()` — executing steps
    /// in-memory without returning early.
    pub(crate) immediate_steps: bool,
    /// How this task should behave when executing steps.
    /// `Legacy` preserves the old in-state JSON model; `Body`/`Step` use per-row tasks.
    pub(crate) execution_mode: ExecutionMode,
}

impl<S: Scheduler + DurableStorage> TaskContext<S> {
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
        let execution_id = execution_id.into();
        let task_id = execution_id.clone();
        Self {
            scheduler,
            task_id,
            execution_id,
            task_name: task_name.into(),
            state,
            lock_token: lock_token.into(),
            data,
            immediate_steps: false,
            execution_mode: ExecutionMode::Legacy,
        }
    }

    /// Set the execution mode for this context (new execution model).
    pub fn with_execution_mode(mut self, mode: ExecutionMode) -> Self {
        self.execution_mode = mode;
        self
    }

    /// Set the underlying task_id (differs from execution_id in the new model).
    pub fn with_task_id(mut self, task_id: impl Into<String>) -> Self {
        self.task_id = task_id.into();
        self
    }

    /// Construct a new `TaskContext` with immediate step execution enabled.
    pub fn with_immediate_steps(mut self) -> Self {
        self.immediate_steps = true;
        self
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
    ///
    /// # Immediate mode
    ///
    /// If [`TaskContext::with_immediate_steps`] was used (or the worker has
    /// `immediate_steps` enabled), this method behaves like [`step_immediate`](Self::step_immediate)
    /// and executes the step without returning early.
    #[instrument(name = "step.execute", skip(self, step_fn), fields(step_name = step_name))]
    pub async fn step<T, F, Fut>(&mut self, step_name: &str, step_fn: F) -> Result<T, StepError>
    where
        T: Serialize + for<'de> Deserialize<'de>,
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, StepError>>,
    {
        match &self.execution_mode {
            ExecutionMode::Body { .. } => self.step_body_mode(step_name, step_fn).await,
            ExecutionMode::Step { target_step, .. } => {
                let target = target_step.clone();
                self.step_step_mode(&target, step_name, step_fn).await
            }
            // Legacy, Coordinator, or other — fall through to old behaviour.
            _ => {
                if self.immediate_steps {
                    self.step_immediate(step_name, step_fn).await
                } else {
                    self.step_with_retry(step_name, RetryConfig::none(), step_fn).await
                }
            }
        }
    }

    // ── New execution model — body mode ───────────────────────────────────────

    /// `step()` in body mode: look up the step task in the DB.
    ///
    /// - Completed → return cached result (no lambda execution).
    /// - Scheduled/PickedUp → step is in-flight, return `Err(Scheduled)` so body exits.
    /// - Not found → insert a new step task row, return `Err(Scheduled)`.
    async fn step_body_mode<T, F, Fut>(
        &mut self,
        step_name: &str,
        _step_fn: F,
    ) -> Result<T, StepError>
    where
        T: Serialize + for<'de> Deserialize<'de>,
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, StepError>>,
    {
        let lookup = self
            .scheduler
            .get_step_status(&self.execution_id, step_name)
            .await
            .map_err(|e| StepError::Failed {
                step: step_name.to_string(),
                reason: e.to_string(),
            })?;

        match lookup {
            Some(StepLookup { status: TaskStatus::Completed, result: Some(json), .. }) => {
                serde_json::from_value(json).map_err(|e| StepError::Failed {
                    step: step_name.to_string(),
                    reason: format!("failed to deserialize cached result: {e}"),
                })
            }
            Some(StepLookup { status: TaskStatus::Completed, result: None, .. }) => {
                Err(StepError::Failed {
                    step: step_name.to_string(),
                    reason: "step completed but result is missing".to_string(),
                })
            }
            Some(_) => {
                // Scheduled or PickedUp — step task exists, body should exit and wait.
                Err(StepError::Scheduled {
                    step: step_name.to_string(),
                    next_execution: None,
                })
            }
            None => {
                // First time: insert step task row and exit.
                let current_segment = match &self.execution_mode {
                    ExecutionMode::Body { segment } => *segment,
                    _ => 0,
                };
                let task_id = format!("{}:step:{}", self.execution_id, step_name);
                step_ops::schedule_step_task(
                    &*self.scheduler,
                    &task_id,
                    &self.task_name,
                    &self.execution_id,
                    step_name,
                    current_segment + 1,
                    self.data.clone(),
                )
                .await
                .map_err(|e| StepError::Failed {
                    step: step_name.to_string(),
                    reason: e.to_string(),
                })?;
                Err(StepError::Scheduled {
                    step: step_name.to_string(),
                    next_execution: None,
                })
            }
        }
    }

    // ── New execution model — step mode ───────────────────────────────────────

    /// `step()` in step mode: replay the body until the target step is reached.
    ///
    /// - Non-target steps → DB lookup, return cached result (must be completed).
    /// - Target step → execute lambda, complete transactionally, return `Err(StepExecuted)`.
    async fn step_step_mode<T, F, Fut>(
        &mut self,
        target: &str,
        step_name: &str,
        step_fn: F,
    ) -> Result<T, StepError>
    where
        T: Serialize + for<'de> Deserialize<'de>,
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, StepError>>,
    {
        if step_name == target {
            // Execute the lambda for this step.
            let result = step_fn().await?;
            let serialized = serde_json::to_value(&result).map_err(|e| StepError::Failed {
                step: step_name.to_string(),
                reason: format!("failed to serialize result: {e}"),
            })?;

            let (next_body_segment, is_wait_all_child) = match &self.execution_mode {
                ExecutionMode::Step { next_body_segment, .. } => {
                    // Check metadata for wait_all child flag (stored in execution_model module).
                    let wac = false; // sequential step: never a wait_all child
                    (*next_body_segment, wac)
                }
                _ => (1, false),
            };

            let step_task_id = self.task_id.clone();

            if is_wait_all_child {
                step_ops::complete_step_no_resume(
                    &*self.scheduler,
                    &step_task_id,
                    serialized,
                    &self.lock_token,
                )
                .await
                .map_err(|e| StepError::Failed {
                    step: step_name.to_string(),
                    reason: e.to_string(),
                })?;
            } else {
                let next_body_task_id =
                    format!("{}-b{}", self.execution_id, next_body_segment);
                step_ops::complete_step_and_schedule_body(
                    &*self.scheduler,
                    &step_task_id,
                    serialized,
                    &self.lock_token,
                    &next_body_task_id,
                    &self.task_name,
                    &self.execution_id,
                    next_body_segment,
                    self.data.clone(),
                )
                .await
                .map_err(|e| StepError::Failed {
                    step: step_name.to_string(),
                    reason: e.to_string(),
                })?;
            }

            Err(StepError::StepExecuted {
                step: step_name.to_string(),
            })
        } else {
            // Non-target: must be a previously completed step; return cached result.
            let lookup = self
                .scheduler
                .get_step_status(&self.execution_id, step_name)
                .await
                .map_err(|e| StepError::Failed {
                    step: step_name.to_string(),
                    reason: e.to_string(),
                })?;

            match lookup {
                Some(StepLookup { status: TaskStatus::Completed, result: Some(json), .. }) => {
                    serde_json::from_value(json).map_err(|e| StepError::Failed {
                        step: step_name.to_string(),
                        reason: format!("failed to deserialize cached result: {e}"),
                    })
                }
                Some(StepLookup { status: TaskStatus::Completed, result: None, .. }) => {
                    Err(StepError::Failed {
                        step: step_name.to_string(),
                        reason: "step completed but result is missing".to_string(),
                    })
                }
                _ => {
                    // Step not yet completed. This shouldn't happen in sequential flow
                    // but treat as "body must wait".
                    Err(StepError::Scheduled {
                        step: step_name.to_string(),
                        next_execution: None,
                    })
                }
            }
        }
    }

    /// Execute a named step with a retry policy.
    ///
    /// Behaves like [`step`](Self::step), but retries the lambda across re-entries
    /// (each retry is a separate task execution) with the configured backoff.
    ///
    /// Each attempt is recorded in [`StepRecord::attempts`] for observability.
    ///
    /// # Immediate mode
    ///
    /// If [`TaskContext::with_immediate_steps`] was used (or the worker has
    /// `immediate_steps` enabled), this method behaves like [`step_immediate_with_retry`](Self::step_immediate_with_retry).
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
        if self.immediate_steps {
            self.step_immediate_with_retry(step_name, retry_config, step_fn)
                .await
        } else {
            self.step_with_retry_inner(step_name, retry_config, step_fn)
                .await
        }
    }

    /// Internal implementation of step_with_retry (without immediate mode check).
    async fn step_with_retry_inner<T, F, Fut>(
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
        let snapshot = self
            .state
            .steps
            .get(step_name)
            .map(|r| (r.status.clone(), r.result.clone(), r.retry_attempt));

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
                        let serialized =
                            serde_json::to_value(&result).map_err(|e| StepError::Failed {
                                step: step_name.to_string(),
                                reason: format!("failed to serialize result: {e}"),
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

                        // Record metrics
                        let duration =
                            (completed_at - started_at).num_milliseconds() as f64 / 1000.0;
                        STEP_DURATION_SECONDS
                            .with_label_values(&[step_name, "completed"])
                            .observe(duration);
                        STEPS_TOTAL
                            .with_label_values(&["completed", step_name])
                            .inc();

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
                            STEPS_TOTAL
                                .with_label_values(&["retry_exhausted", step_name])
                                .inc();
                            Err(StepError::RetryExhausted {
                                step: step_name.to_string(),
                                attempts: next_attempt,
                            })
                        } else {
                            // No retries configured — propagate the original error.
                            STEPS_TOTAL.with_label_values(&["failed", step_name]).inc();
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
                        event_deadline: None,
                    },
                );
                Err(StepError::Scheduled {
                    step: step_name.to_string(),
                    next_execution: None,
                })
            }

            // Should not occur: event-waiting steps must use `wait_for_event`.
            Some((StepStatus::WaitingForEvent, _, _)) => Err(StepError::Failed {
                step: step_name.to_string(),
                reason: "step is in WaitingForEvent state; use wait_for_event instead".to_string(),
            }),
        }
    }

    /// Execute a named step immediately in memory without returning early.
    ///
    /// Unlike [`step`](Self::step), which persists the step as `Scheduled` and returns
    /// `Err(StepError::Scheduled)` to signal the worker to re-queue the task, this method
    /// writes the step to the database in a **locked** state and executes the lambda
    /// immediately on the same worker.
    ///
    /// # Crash safety
    ///
    /// The step is written to the database in a `picked_up` state so that if the worker
    /// crashes mid-execution, the step is recoverable by orphan cleanup. After the
    /// `orphan_timeout`, the step is reset to `scheduled` and can be picked up again.
    ///
    /// # Trade-offs
    ///
    /// - **Latency**: Near-zero (only DB write latency), no poll interval delay.
    /// - **Fault tolerance**: Reduced — the worker is blocked for the duration of the step.
    ///   If the worker dies, orphan recovery will eventually reset the step.
    /// - **Use case**: Fast sequential steps where low latency matters more than cross-worker
    ///   failover.
    ///
    /// # Control flow
    ///
    /// - **Step absent**: persists as `Scheduled` (locked), executes lambda immediately.
    /// - **Step `Completed`**: deserializes the stored result and returns `Ok(T)` immediately.
    /// - **Step `Scheduled`**: executes the lambda (retry scenario).
    #[instrument(name = "step.immediate", skip(self, step_fn), fields(step_name = step_name))]
    pub async fn step_immediate<T, F, Fut>(
        &mut self,
        step_name: &str,
        step_fn: F,
    ) -> Result<T, StepError>
    where
        T: Serialize + for<'de> Deserialize<'de>,
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, StepError>>,
    {
        self.step_immediate_with_retry(step_name, RetryConfig::none(), step_fn)
            .await
    }

    /// Execute a named step immediately in memory with a retry policy.
    ///
    /// Behaves like [`step_immediate`](Self::step_immediate), but retries the lambda
    /// across re-entries with the configured backoff.
    ///
    /// Each attempt is recorded in [`StepRecord::attempts`] for observability.
    pub async fn step_immediate_with_retry<T, F, Fut>(
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
        let snapshot = self
            .state
            .steps
            .get(step_name)
            .map(|r| (r.status.clone(), r.result.clone(), r.retry_attempt));

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

            // ── SCHEDULED or ABSENT: execute the lambda immediately ───────────
            _ => {
                // If the step doesn't exist yet, register it now.
                // Unlike step(), we don't return early — we execute immediately.
                if self.state.steps.get(step_name).is_none() {
                    self.state.steps.insert(
                        step_name.to_string(),
                        StepRecord {
                            status: StepStatus::Scheduled,
                            result: None,
                            in_task_id: None,
                            retry_attempt: 0,
                            retry_config: Some(retry_config.clone()),
                            attempts: vec![],
                            event_deadline: None,
                        },
                    );
                }

                let retry_attempt = self
                    .state
                    .steps
                    .get(step_name)
                    .map(|r| r.retry_attempt)
                    .unwrap_or(0);

                let started_at = chrono::Utc::now();
                let outcome = step_fn().await;
                let completed_at = chrono::Utc::now();

                match outcome {
                    Ok(result) => {
                        let serialized =
                            serde_json::to_value(&result).map_err(|e| StepError::Failed {
                                step: step_name.to_string(),
                                reason: format!("failed to serialize result: {e}"),
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

                        // Record metrics
                        let duration =
                            (completed_at - started_at).num_milliseconds() as f64 / 1000.0;
                        STEP_DURATION_SECONDS
                            .with_label_values(&[step_name, "completed"])
                            .observe(duration);
                        STEPS_TOTAL
                            .with_label_values(&["completed", step_name])
                            .inc();

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
                            STEPS_TOTAL
                                .with_label_values(&["retry_exhausted", step_name])
                                .inc();
                            Err(StepError::RetryExhausted {
                                step: step_name.to_string(),
                                attempts: next_attempt,
                            })
                        } else {
                            // No retries configured — propagate the original error.
                            STEPS_TOTAL.with_label_values(&["failed", step_name]).inc();
                            Err(e)
                        }
                    }
                }
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

    /// Register a step for parallel execution without waiting for it to complete.
    ///
    /// Unlike [`step`](Self::step), this method does **not** block. All registered
    /// steps run when [`wait_all`](Self::wait_all) is called.
    ///
    /// # Re-entry behaviour
    ///
    /// - **Step absent**: registers it as `Scheduled` and stores the lambda.
    /// - **Step `Scheduled`**: stores the lambda for execution in `wait_all`.
    /// - **Step `Completed`**: discards the lambda; `wait_all` will return the cached result.
    pub fn schedule_step<T, F, Fut>(&mut self, step_name: &str, step_fn: F) -> StepHandle<T>
    where
        T: Serialize + for<'de> Deserialize<'de> + Send + 'static,
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, StepError>> + Send + 'static,
    {
        let step_name_str = step_name.to_string();

        match &self.execution_mode {
            ExecutionMode::Body { .. } | ExecutionMode::Step { .. } => {
                // New model: schedule_step just returns a handle with the lambda.
                // Actual scheduling (DB insert) happens in wait_all.
                // In step mode, only the target step handle carries the lambda.
                let is_target = matches!(&self.execution_mode,
                    ExecutionMode::Step { target_step, .. } if target_step == step_name);

                let pending: Option<PendingFn> = if !matches!(&self.execution_mode, ExecutionMode::Step { .. }) || is_target {
                    let name_for_err = step_name_str.clone();
                    Some(Box::new(move || {
                        Box::pin(async move {
                            let result = step_fn().await?;
                            serde_json::to_value(result).map_err(|e| StepError::Failed {
                                step: name_for_err,
                                reason: format!("serialize error: {e}"),
                            })
                        })
                    }))
                } else {
                    // In step mode but not the target: lambda not needed.
                    None
                };

                StepHandle {
                    step_name: step_name_str,
                    pending,
                    _marker: std::marker::PhantomData,
                }
            }

            // Legacy mode: existing in-memory behaviour.
            _ => {
                let is_scheduled = match self.state.steps.get(step_name) {
                    None => {
                        self.state.steps.insert(
                            step_name.to_string(),
                            StepRecord {
                                status: StepStatus::Scheduled,
                                result: None,
                                in_task_id: None,
                                retry_attempt: 0,
                                retry_config: None,
                                attempts: vec![],
                                event_deadline: None,
                            },
                        );
                        true
                    }
                    Some(record) => record.status == StepStatus::Scheduled,
                };

                let pending: Option<PendingFn> = if is_scheduled {
                    let name_for_err = step_name_str.clone();
                    Some(Box::new(move || {
                        Box::pin(async move {
                            let result = step_fn().await?;
                            serde_json::to_value(result).map_err(|e| StepError::Failed {
                                step: name_for_err,
                                reason: format!("serialize error: {e}"),
                            })
                        })
                    }))
                } else {
                    None
                };

                StepHandle {
                    step_name: step_name_str,
                    pending,
                    _marker: std::marker::PhantomData,
                }
            }
        }
    }

    /// Wait for all handles returned by [`schedule_step`](Self::schedule_step) to complete.
    ///
    /// Steps that are `Scheduled` have their lambdas executed sequentially in
    /// the order supplied. Steps already `Completed` return their cached results.
    ///
    /// Returns `Ok(results)` where each element corresponds to one handle in order.
    /// An individual step failure appears as `Err(StepError)` inside the `Vec`;
    /// the outer `Err` is reserved for control-flow or programming errors.
    pub async fn wait_all<T>(
        &mut self,
        handles: Vec<StepHandle<T>>,
    ) -> Result<Vec<Result<T, StepError>>, StepError>
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        match &self.execution_mode {
            ExecutionMode::Body { segment } => {
                return self.wait_all_body_mode(handles, *segment).await;
            }
            ExecutionMode::Step { target_step, .. } => {
                let target = target_step.clone();
                return self.wait_all_step_mode(handles, &target).await;
            }
            _ => {} // fall through to legacy
        }

        let mut results = Vec::with_capacity(handles.len());

        for handle in handles {
            let snapshot = self
                .state
                .steps
                .get(&handle.step_name)
                .map(|r| (r.status.clone(), r.result.clone(), r.retry_attempt));

            match snapshot {
                // ── Already completed: return cached result ────────────────────
                Some((StepStatus::Completed, Some(json_val), _)) => {
                    let val = serde_json::from_value(json_val).map_err(|e| StepError::Failed {
                        step: handle.step_name.clone(),
                        reason: format!("deserialize error: {e}"),
                    })?;
                    results.push(Ok(val));
                }

                Some((StepStatus::Completed, None, _)) => {
                    return Err(StepError::Failed {
                        step: handle.step_name.clone(),
                        reason: "step completed but result is missing".to_string(),
                    });
                }

                // ── Scheduled: execute the lambda ──────────────────────────────
                Some((StepStatus::Scheduled, _, retry_attempt)) => {
                    if let Some(pending_fn) = handle.pending {
                        let started_at = chrono::Utc::now();
                        let json_result = pending_fn().await;
                        let completed_at = chrono::Utc::now();

                        match json_result {
                            Ok(json_val) => {
                                let record = self
                                    .state
                                    .steps
                                    .get_mut(&handle.step_name)
                                    .expect("step must exist in state");
                                record.status = StepStatus::Completed;
                                record.result = Some(json_val.clone());
                                record.attempts.push(StepAttempt {
                                    attempt_number: retry_attempt + 1,
                                    started_at,
                                    completed_at: Some(completed_at),
                                    status: AttemptStatus::Completed,
                                    error: None,
                                    result: Some(json_val.clone()),
                                });

                                let val = serde_json::from_value(json_val).map_err(|e| {
                                    StepError::Failed {
                                        step: handle.step_name.clone(),
                                        reason: format!("deserialize error: {e}"),
                                    }
                                })?;
                                results.push(Ok(val));
                            }
                            Err(e) => {
                                let record = self
                                    .state
                                    .steps
                                    .get_mut(&handle.step_name)
                                    .expect("step must exist in state");
                                record.attempts.push(StepAttempt {
                                    attempt_number: retry_attempt + 1,
                                    started_at,
                                    completed_at: Some(completed_at),
                                    status: AttemptStatus::Failed,
                                    error: Some(e.to_string()),
                                    result: None,
                                });
                                results.push(Err(e));
                            }
                        }
                    } else {
                        // This should not happen in normal usage (schedule_step always
                        // stores a pending fn for Scheduled steps). Treat as re-queue signal.
                        return Err(StepError::Scheduled {
                            step: handle.step_name.clone(),
                            next_execution: None,
                        });
                    }
                }

                // ── Waiting for event: not supported in wait_all ───────────────
                Some((StepStatus::WaitingForEvent, _, _)) => {
                    return Err(StepError::Failed {
                        step: handle.step_name.clone(),
                        reason: "event-waiting steps cannot be used with wait_all; use wait_for_event directly".to_string(),
                    });
                }

                // ── Step not registered: programming error ─────────────────────
                None => {
                    return Err(StepError::Failed {
                        step: handle.step_name.clone(),
                        reason: "step not found — call schedule_step before wait_all".to_string(),
                    });
                }
            }
        }

        Ok(results)
    }

    // ── New execution model — wait_all body mode ──────────────────────────────

    /// `wait_all` in body mode:
    /// 1. Ensure all child step task rows exist (insert if not).
    /// 2. If all are completed → return cached results.
    /// 3. Otherwise → schedule coordinator (if not already scheduled), return Err(Scheduled).
    async fn wait_all_body_mode<T>(
        &mut self,
        handles: Vec<StepHandle<T>>,
        segment: usize,
    ) -> Result<Vec<Result<T, StepError>>, StepError>
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        let next_segment = segment + 1;
        let coordinator_id = format!(
            "{}:coord:wait_all:{}",
            self.execution_id, next_segment
        );

        // Extract step names upfront so we don't hold &StepHandle<T> across await points
        // (PendingFn is Send but not Sync, so &StepHandle<T> is not Send).
        let step_names: Vec<String> = handles.iter().map(|h| h.step_name.clone()).collect();

        let mut all_completed = true;
        let mut child_ids: Vec<String> = Vec::with_capacity(step_names.len());

        for step_name in &step_names {
            let child_task_id = format!("{}:step:{}", self.execution_id, step_name);
            child_ids.push(child_task_id.clone());

            let lookup = self
                .scheduler
                .get_step_status(&self.execution_id, step_name)
                .await
                .map_err(|e| StepError::Failed {
                    step: step_name.clone(),
                    reason: e.to_string(),
                })?;

            match lookup {
                Some(StepLookup { status: TaskStatus::Completed, .. }) => {}
                Some(_) => {
                    all_completed = false;
                }
                None => {
                    all_completed = false;
                    step_ops::schedule_wait_all_child(
                        &*self.scheduler,
                        &child_task_id,
                        &self.task_name,
                        &self.execution_id,
                        step_name,
                        &coordinator_id,
                        self.data.clone(),
                    )
                    .await
                    .map_err(|e| StepError::Failed {
                        step: step_name.clone(),
                        reason: e.to_string(),
                    })?;
                }
            }
        }

        if all_completed {
            let mut results = Vec::with_capacity(step_names.len());
            for step_name in &step_names {
                let lookup = self
                    .scheduler
                    .get_step_status(&self.execution_id, step_name)
                    .await
                    .map_err(|e| StepError::Failed {
                        step: step_name.clone(),
                        reason: e.to_string(),
                    })?;
                match lookup {
                    Some(StepLookup { status: TaskStatus::Completed, result: Some(json), .. }) => {
                        let val = serde_json::from_value(json).map_err(|e| StepError::Failed {
                            step: step_name.clone(),
                            reason: format!("deserialize error: {e}"),
                        })?;
                        results.push(Ok(val));
                    }
                    _ => {
                        results.push(Err(StepError::Failed {
                            step: step_name.clone(),
                            reason: "step completed but result is missing".to_string(),
                        }));
                    }
                }
            }
            return Ok(results);
        }

        step_ops::schedule_coordinator(
            &*self.scheduler,
            &coordinator_id,
            &self.task_name,
            &self.execution_id,
            next_segment,
            child_ids,
            self.data.clone(),
        )
        .await
        .map_err(|e| StepError::Failed {
            step: "__wait_all".to_string(),
            reason: e.to_string(),
        })?;

        Err(StepError::Scheduled {
            step: "__wait_all".to_string(),
            next_execution: None,
        })
    }

    // ── New execution model — wait_all step mode ──────────────────────────────

    /// `wait_all` in step mode (executing a specific wait_all child):
    /// Find the target handle, execute its lambda, complete via `complete_step_no_resume`.
    async fn wait_all_step_mode<T>(
        &mut self,
        handles: Vec<StepHandle<T>>,
        target: &str,
    ) -> Result<Vec<Result<T, StepError>>, StepError>
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        for handle in handles {
            if handle.step_name == target {
                if let Some(pending_fn) = handle.pending {
                    let json_result = pending_fn().await;
                    match json_result {
                        Ok(json_val) => {
                            let step_task_id = self.task_id.clone();
                            step_ops::complete_step_no_resume(
                                &*self.scheduler,
                                &step_task_id,
                                json_val,
                                &self.lock_token,
                            )
                            .await
                            .map_err(|e| StepError::Failed {
                                step: target.to_string(),
                                reason: e.to_string(),
                            })?;
                            return Err(StepError::StepExecuted {
                                step: target.to_string(),
                            });
                        }
                        Err(e) => return Err(e),
                    }
                }
                // No pending fn (step already completed): nothing to do.
                return Err(StepError::StepExecuted {
                    step: target.to_string(),
                });
            }
        }
        // Target not found in handles — shouldn't happen in correct usage.
        Err(StepError::Failed {
            step: target.to_string(),
            reason: "target step not found in wait_all handles".to_string(),
        })
    }

    // ── Sleep ─────────────────────────────────────────────────────────────���───

    /// Suspend execution for `duration`, resuming at `now + duration`.
    pub async fn sleep(&mut self, duration: std::time::Duration) -> Result<(), StepError> {
        let wake_time = chrono::Utc::now()
            + chrono::Duration::from_std(duration).unwrap_or(chrono::Duration::zero());
        self.sleep_until(wake_time).await
    }

    /// Suspend execution until `wake_time`.
    pub async fn sleep_until(
        &mut self,
        wake_time: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), StepError> {
        match &self.execution_mode {
            ExecutionMode::Body { segment } => {
                let next_segment = segment + 1;
                let sleep_task_id =
                    format!("{}:sleep:{}", self.execution_id, next_segment);
                step_ops::schedule_sleep_task(
                    &*self.scheduler,
                    &sleep_task_id,
                    &self.task_name,
                    &self.execution_id,
                    next_segment,
                    wake_time,
                    self.data.clone(),
                )
                .await
                .map_err(|e| StepError::Failed {
                    step: "__sleep".to_string(),
                    reason: e.to_string(),
                })?;
                Err(StepError::Scheduled {
                    step: "__sleep".to_string(),
                    next_execution: None,
                })
            }
            _ => {
                // Legacy: no-op sleep (old model had todo!).
                // In step/coordinator mode sleep tasks are handled by the worker directly.
                Err(StepError::Scheduled {
                    step: "__sleep".to_string(),
                    next_execution: Some(wake_time),
                })
            }
        }
    }

    /// Wait for an external event to be delivered to this execution.
    ///
    /// The step is identified internally by `"__event_{event_name}"`.
    ///
    /// # Control flow
    ///
    /// - **First call** (step absent): registers the step as `WaitingForEvent`,
    ///   persists the optional deadline, and returns `Err(StepError::WaitingForEvent)`.
    ///   The worker will park the task with a far-future execution time.
    /// - **Subsequent calls with no event yet**: returns `Err(StepError::WaitingForEvent)`
    ///   again so the task stays parked. If the deadline has passed, returns
    ///   `Err(StepError::Timeout)`.
    /// - **After `offer_event`**: the event payload is in `pending_events`. The step
    ///   is marked `Completed`, the payload deserialized, and `Ok(T)` returned.
    /// - **Already completed**: returns the cached result without re-deserializing
    ///   from the live event map.
    pub async fn wait_for_event<T>(
        &mut self,
        event_name: &str,
        timeout: Option<std::time::Duration>,
    ) -> Result<T, StepError>
    where
        T: for<'de> Deserialize<'de>,
    {
        let step_key = format!("__event_{event_name}");

        let snapshot = self
            .state
            .steps
            .get(&step_key)
            .map(|r| (r.status.clone(), r.result.clone(), r.event_deadline));

        match snapshot {
            // ── Already completed — return cached result ───────────────────
            Some((StepStatus::Completed, Some(v), _)) => {
                serde_json::from_value(v).map_err(|e| StepError::Failed {
                    step: step_key,
                    reason: format!("failed to deserialize event result: {e}"),
                })
            }

            Some((StepStatus::Completed, None, _)) => Err(StepError::Failed {
                step: step_key,
                reason: "event step completed but result is missing".to_string(),
            }),

            // ── Waiting — check deadline and pending_events ────────────────
            Some((StepStatus::WaitingForEvent, _, deadline)) => {
                if deadline.is_some_and(|dl| chrono::Utc::now() > dl) {
                    return Err(StepError::Timeout {
                        step: step_key,
                        // Approximate; exact duration isn't tracked.
                        duration: timeout.unwrap_or(std::time::Duration::ZERO),
                    });
                }

                if let Some(payload) = self.state.pending_events.remove(event_name) {
                    let result: T =
                        serde_json::from_value(payload.clone()).map_err(|e| StepError::Failed {
                            step: step_key.clone(),
                            reason: format!("failed to deserialize event payload: {e}"),
                        })?;
                    let record = self
                        .state
                        .steps
                        .get_mut(&step_key)
                        .expect("step must exist");
                    record.status = StepStatus::Completed;
                    record.result = Some(payload);
                    Ok(result)
                } else {
                    Err(StepError::WaitingForEvent {
                        event: event_name.to_string(),
                    })
                }
            }

            // ── Absent — first call, register the step ────────────────────
            None => {
                let deadline = timeout.and_then(|d| {
                    chrono::Duration::from_std(d)
                        .ok()
                        .map(|cd| chrono::Utc::now() + cd)
                });

                self.state.steps.insert(
                    step_key,
                    StepRecord {
                        status: StepStatus::WaitingForEvent,
                        result: None,
                        in_task_id: None,
                        retry_attempt: 0,
                        retry_config: None,
                        attempts: vec![],
                        event_deadline: deadline,
                    },
                );

                Err(StepError::WaitingForEvent {
                    event: event_name.to_string(),
                })
            }

            // Shouldn't occur for event steps, but treat like absent.
            Some((StepStatus::Scheduled, _, _)) => Err(StepError::WaitingForEvent {
                event: event_name.to_string(),
            }),
        }
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
    use scheduler::{DurableStorage, FetchedTask, Recurrence, ScheduleResult, Scheduler, StorageError};
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
            _metadata: serde_json::Value,
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

    impl DurableStorage for NoopScheduler {}

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
                event_deadline: None,
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
                event_deadline: None,
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
                event_deadline: None,
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
                event_deadline: None,
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
            Err(StepError::Scheduled {
                next_execution: Some(_),
                ..
            })
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
                event_deadline: None,
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
                event_deadline: None,
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
                event_deadline: None,
            },
        );
        let result = ctx
            .step_with_timeout("fast-step", Duration::from_secs(5), || async {
                Ok::<i32, StepError>(10)
            })
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
                event_deadline: None,
            },
        );
        let result = ctx
            .step_with_timeout("slow-step", Duration::from_millis(1), || async {
                tokio::time::sleep(Duration::from_secs(60)).await;
                Ok::<i32, StepError>(99)
            })
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
                event_deadline: None,
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

    // ── schedule_step + wait_all ──────────────────────────────────────────────

    #[tokio::test]
    async fn schedule_step_registers_as_scheduled() {
        let mut ctx = make_ctx();
        let _handle = ctx.schedule_step("par-step", || async { Ok::<i32, StepError>(1) });
        let record = ctx.state.steps.get("par-step").unwrap();
        assert_eq!(record.status, StepStatus::Scheduled);
    }

    #[tokio::test]
    async fn wait_all_executes_scheduled_steps_and_returns_results() {
        let mut ctx = make_ctx();
        let h1 = ctx.schedule_step("s1", || async { Ok::<i32, StepError>(10) });
        let h2 = ctx.schedule_step("s2", || async { Ok::<i32, StepError>(20) });

        let results = ctx.wait_all(vec![h1, h2]).await.unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].as_ref().unwrap(), &10);
        assert_eq!(results[1].as_ref().unwrap(), &20);
    }

    #[tokio::test]
    async fn wait_all_marks_steps_completed_after_execution() {
        let mut ctx = make_ctx();
        let h = ctx.schedule_step("s1", || async { Ok::<i32, StepError>(42) });
        ctx.wait_all(vec![h]).await.unwrap();

        let record = ctx.state.steps.get("s1").unwrap();
        assert_eq!(record.status, StepStatus::Completed);
        assert_eq!(record.result, Some(serde_json::json!(42)));
    }

    #[tokio::test]
    async fn wait_all_returns_cached_result_for_completed_steps() {
        let mut ctx = make_ctx();
        // Pre-populate a completed step.
        ctx.state.steps.insert(
            "done".to_string(),
            StepRecord {
                status: StepStatus::Completed,
                result: Some(serde_json::json!(99)),
                in_task_id: None,
                retry_attempt: 0,
                retry_config: None,
                attempts: vec![],
                event_deadline: None,
            },
        );

        // schedule_step should detect the step is completed and not wrap the fn.
        let h = ctx.schedule_step("done", || async {
            Ok::<i32, StepError>(0) // unreachable
        });
        let results = ctx.wait_all(vec![h]).await.unwrap();
        assert_eq!(results[0].as_ref().unwrap(), &99);
    }

    #[tokio::test]
    async fn wait_all_collects_step_failures_in_results() {
        let mut ctx = make_ctx();
        let h = ctx.schedule_step("fail-step", || async {
            Err::<i32, _>(StepError::Failed {
                step: "fail-step".to_string(),
                reason: "bang".to_string(),
            })
        });
        let results = ctx.wait_all(vec![h]).await.unwrap();
        assert!(results[0].is_err());
    }

    #[tokio::test]
    async fn wait_all_records_attempt_on_success() {
        let mut ctx = make_ctx();
        let h = ctx.schedule_step("s", || async { Ok::<i32, StepError>(7) });
        ctx.wait_all(vec![h]).await.unwrap();

        let record = ctx.state.steps.get("s").unwrap();
        assert_eq!(record.attempts.len(), 1);
        assert_eq!(record.attempts[0].status, AttemptStatus::Completed);
    }

    // ── wait_for_event ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn wait_for_event_returns_waiting_on_first_call() {
        let mut ctx = make_ctx();
        let result: Result<serde_json::Value, _> = ctx.wait_for_event("my-event", None).await;
        assert!(
            matches!(result, Err(StepError::WaitingForEvent { ref event }) if event == "my-event")
        );
        // The step should now be registered.
        let step = ctx.state.steps.get("__event_my-event").unwrap();
        assert_eq!(step.status, StepStatus::WaitingForEvent);
    }

    #[tokio::test]
    async fn wait_for_event_still_waiting_when_no_event_in_pending() {
        let mut ctx = make_ctx();
        // Pre-register step in WaitingForEvent state.
        ctx.state.steps.insert(
            "__event_my-event".to_string(),
            StepRecord {
                status: StepStatus::WaitingForEvent,
                result: None,
                in_task_id: None,
                retry_attempt: 0,
                retry_config: None,
                attempts: vec![],
                event_deadline: None,
            },
        );

        let result: Result<serde_json::Value, _> = ctx.wait_for_event("my-event", None).await;
        assert!(matches!(result, Err(StepError::WaitingForEvent { .. })));
    }

    #[tokio::test]
    async fn wait_for_event_returns_payload_when_event_is_pending() {
        let mut ctx = make_ctx();
        ctx.state.steps.insert(
            "__event_approve".to_string(),
            StepRecord {
                status: StepStatus::WaitingForEvent,
                result: None,
                in_task_id: None,
                retry_attempt: 0,
                retry_config: None,
                attempts: vec![],
                event_deadline: None,
            },
        );
        ctx.state
            .pending_events
            .insert("approve".to_string(), serde_json::json!({"approved": true}));

        let result: Result<serde_json::Value, StepError> =
            ctx.wait_for_event("approve", None).await;
        assert!(result.is_ok());
        let val = result.unwrap();
        assert_eq!(val["approved"], true);

        // Step must be marked completed and pending_events cleared.
        let step = ctx.state.steps.get("__event_approve").unwrap();
        assert_eq!(step.status, StepStatus::Completed);
        assert!(!ctx.state.pending_events.contains_key("approve"));
    }

    #[tokio::test]
    async fn wait_for_event_returns_cached_result_when_completed() {
        let mut ctx = make_ctx();
        ctx.state.steps.insert(
            "__event_done".to_string(),
            StepRecord {
                status: StepStatus::Completed,
                result: Some(serde_json::json!(42)),
                in_task_id: None,
                retry_attempt: 0,
                retry_config: None,
                attempts: vec![],
                event_deadline: None,
            },
        );

        let result: Result<i32, StepError> = ctx.wait_for_event("done", None).await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn wait_for_event_returns_timeout_when_deadline_passed() {
        let mut ctx = make_ctx();
        // Set a deadline in the past.
        let past = chrono::Utc::now() - chrono::Duration::seconds(1);
        ctx.state.steps.insert(
            "__event_late".to_string(),
            StepRecord {
                status: StepStatus::WaitingForEvent,
                result: None,
                in_task_id: None,
                retry_attempt: 0,
                retry_config: None,
                attempts: vec![],
                event_deadline: Some(past),
            },
        );

        let result: Result<serde_json::Value, StepError> = ctx
            .wait_for_event("late", Some(Duration::from_secs(1)))
            .await;
        assert!(matches!(result, Err(StepError::Timeout { .. })));
    }

    // ── step_immediate ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn step_immediate_executes_lambda_on_first_call() {
        let mut ctx = make_ctx();
        // Unlike step(), step_immediate() does NOT return Scheduled on first call.
        let result = ctx
            .step_immediate("my-step", || async { Ok::<i32, StepError>(42) })
            .await;
        assert_eq!(result.unwrap(), 42);

        // Step should be marked as completed.
        let record = ctx.state.steps.get("my-step").unwrap();
        assert_eq!(record.status, StepStatus::Completed);
        assert_eq!(record.result, Some(serde_json::json!(42)));
    }

    #[tokio::test]
    async fn step_immediate_returns_cached_result_when_completed() {
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
                event_deadline: None,
            },
        );
        let result: Result<i32, _> = ctx
            .step_immediate("my-step", || async {
                Ok::<i32, StepError>(0) // unreachable
            })
            .await;
        assert_eq!(result.unwrap(), 99);
    }

    #[tokio::test]
    async fn step_immediate_records_attempt_on_success() {
        let mut ctx = make_ctx();
        let _ = ctx
            .step_immediate("my-step", || async { Ok::<i32, StepError>(7) })
            .await;
        let record = ctx.state.steps.get("my-step").unwrap();
        assert_eq!(record.attempts.len(), 1);
        assert_eq!(record.attempts[0].status, AttemptStatus::Completed);
        assert_eq!(record.attempts[0].attempt_number, 1);
    }

    #[tokio::test]
    async fn step_immediate_propagates_error_when_no_retries() {
        let mut ctx = make_ctx();
        let result = ctx
            .step_immediate("fail-step", || async {
                Err::<i32, _>(StepError::Failed {
                    step: "fail-step".to_string(),
                    reason: "intentional error".to_string(),
                })
            })
            .await;
        assert!(matches!(result, Err(StepError::Failed { .. })));
    }

    #[tokio::test]
    async fn step_immediate_with_retry_returns_scheduled_on_failure_when_retries_remain() {
        let mut ctx = make_ctx();
        let result = ctx
            .step_immediate_with_retry(
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
            Err(StepError::Scheduled {
                next_execution: Some(_),
                ..
            })
        ));

        // Retry attempt counter must be bumped.
        let record = ctx.state.steps.get("retry-step").unwrap();
        assert_eq!(record.retry_attempt, 1);
        assert_eq!(record.attempts.len(), 1);
        assert_eq!(record.attempts[0].status, AttemptStatus::Failed);
    }

    #[tokio::test]
    async fn step_immediate_with_retry_exhausts_after_max_attempts() {
        let mut ctx = make_ctx();
        // Simulate: retry_attempt is already at max (2 out of 2 retries used).
        ctx.state.steps.insert(
            "retry-step".to_string(),
            StepRecord {
                status: StepStatus::Scheduled,
                result: None,
                in_task_id: None,
                retry_attempt: 2,
                retry_config: None,
                attempts: vec![],
                event_deadline: None,
            },
        );

        let result = ctx
            .step_immediate_with_retry(
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
    async fn step_immediate_with_retry_succeeds_after_previous_failures() {
        let mut ctx = make_ctx();
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
                event_deadline: None,
            },
        );

        let result = ctx
            .step_immediate_with_retry(
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
}
