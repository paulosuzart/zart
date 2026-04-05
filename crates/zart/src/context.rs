//! Task execution context — the interface through which durable step execution is managed.

use crate::error::StepError;
use crate::execution_model::ExecutionMode;

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
    /// How this task should behave when executing steps.
    /// `Body`/`Step` use per-row tasks.
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
            execution_mode: ExecutionMode::Body { segment: 0 },
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

    /// Execute a named step with no retries and no timeout.
    ///
    /// # Control flow
    ///
    /// - **Step absent**: inserts a child step task row, returns `Err(StepError::Scheduled)`.
    ///   The body task is then marked complete; the step runs independently.
    /// - **Step `Completed`**: deserializes the stored result and returns `Ok(T)`
    ///   immediately (lambda not called).
    #[instrument(name = "step.execute", skip(self, step_fn), fields(step_name = step_name))]
    pub async fn step<T, F, Fut>(&mut self, step_name: &str, step_fn: F) -> Result<T, StepError>
    where
        T: Serialize + for<'de> Deserialize<'de>,
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, StepError>>,
    {
        self.step_internal(step_name, None, step_fn).await
    }

    /// Internal dispatcher for `step` and `step_with_retry`, sharing the same logic.
    async fn step_internal<T, F, Fut>(
        &mut self,
        step_name: &str,
        retry_config: Option<RetryConfig>,
        step_fn: F,
    ) -> Result<T, StepError>
    where
        T: Serialize + for<'de> Deserialize<'de>,
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, StepError>>,
    {
        match &self.execution_mode {
            ExecutionMode::Body { .. } => self.step_body_mode(step_name, retry_config, step_fn).await,
            ExecutionMode::Step { target_step, .. } => {
                let target = target_step.clone();
                self.step_step_mode(&target, step_name, step_fn).await
            }
            ExecutionMode::Coordinator { .. } => Err(StepError::Failed {
                step: step_name.to_string(),
                reason: "step() called in coordinator mode — not supported".to_string(),
            }),
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
        retry_config: Option<RetryConfig>,
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
                    retry_config.as_ref(),
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
            let lambda_result = step_fn().await;
            let result = match lambda_result {
                Ok(v) => v,
                Err(e) => {
                    // Check if retry is configured and allowed.
                    let (retry_config, retry_attempt) = match &self.execution_mode {
                        ExecutionMode::Step { retry_config, retry_attempt, .. } => {
                            (retry_config.clone(), *retry_attempt)
                        }
                        _ => (None, 0),
                    };

                    if let Some(rc) = retry_config {
                        if let Some(delay) = rc.delay_for(retry_attempt + 1) {
                            let retry_time = chrono::Utc::now()
                                + chrono::Duration::from_std(delay)
                                    .unwrap_or(chrono::Duration::zero());
                            // Reschedule this step task for retry.
                            if let Err(sched_err) = step_ops::reschedule_step_for_retry(
                                &*self.scheduler,
                                &self.task_id,
                                &e.to_string(),
                                retry_time,
                                &self.lock_token,
                            )
                            .await
                            {
                                return Err(StepError::Failed {
                                    step: step_name.to_string(),
                                    reason: format!("failed to schedule retry: {sched_err}"),
                                });
                            }
                            // Signal the worker that this step task managed its own transition.
                            return Err(StepError::StepExecuted {
                                step: step_name.to_string(),
                            });
                        }
                    }

                    return Err(e);
                }
            };
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
    /// In body mode, embeds the retry config in the step task's metadata so the
    /// worker can reschedule on failure. In step mode, uses the config from the
    /// task metadata (already loaded into `execution_mode`) to retry on failure.
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
        self.step_internal(step_name, Some(retry_config), step_fn).await
    }

    /// Execute a named step immediately.
    ///
    /// In the new execution model, this delegates to [`step`](Self::step).
    /// Kept for API compatibility with existing handlers.
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
        self.step(step_name, step_fn).await
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

        // schedule_step just returns a handle with the lambda.
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
                self.wait_all_body_mode(handles, *segment).await
            }
            ExecutionMode::Step { target_step, .. } => {
                let target = target_step.clone();
                self.wait_all_step_mode(handles, &target).await
            }
            _ => Err(StepError::Failed {
                step: "wait_all".to_string(),
                reason: "unexpected execution mode".to_string(),
            }),
        }
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
            _ => Err(StepError::Failed {
                step: "__sleep".to_string(),
                reason: "sleep_until called in unexpected execution mode".to_string(),
            }),
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

    // ── New execution model: call-counting tests ──────────────────────────────
    //
    // These tests use RecordingScheduler to assert exactly which DB operations
    // each execution-model code path triggers and how many task rows are created.

    use crate::test_helpers::{Call, RecordingScheduler};

    fn make_body_ctx(
        scheduler: std::sync::Arc<RecordingScheduler>,
        segment: usize,
    ) -> TaskContext<RecordingScheduler> {
        TaskContext::new(
            scheduler,
            "exec-1",
            "test-task",
            ExecutionState::default(),
            "lock-tok",
            serde_json::json!({"input": "data"}),
        )
        .with_execution_mode(ExecutionMode::Body { segment })
    }

    fn make_step_ctx(
        scheduler: std::sync::Arc<RecordingScheduler>,
        target: &str,
        next_body_segment: usize,
    ) -> TaskContext<RecordingScheduler> {
        let task_id = format!("exec-1:step:{target}");
        TaskContext::new(
            scheduler,
            "exec-1",
            "test-task",
            ExecutionState::default(),
            "lock-tok",
            serde_json::json!({"input": "data"}),
        )
        .with_task_id(task_id)
        .with_execution_mode(ExecutionMode::Step {
            target_step: target.to_string(),
            step_type: crate::execution_model::StepKind::Step,
            next_body_segment,
            retry_attempt: 0,
            retry_config: None,
        })
    }

    // ── body mode: step() ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn body_mode_first_step_inserts_exactly_one_task_row() {
        let (scheduler, calls) = RecordingScheduler::builder().build();
        let mut ctx = make_body_ctx(scheduler, 0);

        let result = ctx.step("charge-card", || async { Ok::<u32, StepError>(99) }).await;

        assert!(
            matches!(result, Err(StepError::Scheduled { ref step, .. }) if step == "charge-card"),
            "first step call in body mode must return Scheduled"
        );

        let log = calls.lock().unwrap();
        let inserts: Vec<_> = log.iter().filter(|c| c.is_schedule_at()).collect();
        assert_eq!(inserts.len(), 1, "exactly one task row inserted");

        if let Call::ScheduleAt { task_id, metadata, .. } = &inserts[0] {
            assert_eq!(task_id, "exec-1:step:charge-card");
            assert_eq!(metadata["mode"], "step");
            assert_eq!(metadata["step_type"], "step");
            assert_eq!(metadata["step_name"], "charge-card");
            assert_eq!(metadata["segment"], 1, "next_body_segment = current segment + 1");
            assert_eq!(metadata["execution_id"], "exec-1");
        } else {
            panic!("unexpected call variant");
        }
    }

    #[tokio::test]
    async fn body_mode_completed_step_returns_cached_result_with_zero_db_writes() {
        let (scheduler, calls) = RecordingScheduler::builder()
            .step_completed("exec-1", "charge-card", serde_json::json!(42))
            .build();
        let mut ctx = make_body_ctx(scheduler, 1);

        let mut lambda_called = false;
        let result: Result<i32, _> = ctx
            .step("charge-card", || {
                lambda_called = true;
                async { Ok::<i32, StepError>(0) }
            })
            .await;

        assert_eq!(result.unwrap(), 42, "cached result must be returned");
        assert!(!lambda_called, "lambda must not run for a completed step");

        let log = calls.lock().unwrap();
        assert_eq!(log.iter().filter(|c| c.is_schedule_at()).count(), 0);
        assert_eq!(log.iter().filter(|c| c.is_complete_and_schedule()).count(), 0);
    }

    #[tokio::test]
    async fn body_mode_inflight_step_returns_scheduled_without_inserting_duplicate() {
        let (scheduler, calls) = RecordingScheduler::builder()
            .step_in_flight("exec-1", "charge-card")
            .build();
        let mut ctx = make_body_ctx(scheduler, 1);

        let result = ctx.step("charge-card", || async { Ok::<u32, StepError>(1) }).await;

        assert!(matches!(result, Err(StepError::Scheduled { .. })));
        let log = calls.lock().unwrap();
        assert_eq!(
            log.iter().filter(|c| c.is_schedule_at()).count(),
            0,
            "step row already exists; must not insert a duplicate"
        );
    }

    // ── step mode: target and non-target steps ────────────────────────────────

    #[tokio::test]
    async fn step_mode_target_step_executes_lambda_and_atomically_completes() {
        let (scheduler, calls) = RecordingScheduler::builder().build();
        let mut ctx = make_step_ctx(scheduler, "charge-card", 1);

        let mut lambda_called = false;
        let result: Result<u32, _> = ctx
            .step("charge-card", || {
                lambda_called = true;
                async { Ok::<u32, StepError>(99) }
            })
            .await;

        assert!(
            matches!(result, Err(StepError::StepExecuted { ref step }) if step == "charge-card"),
            "target step must return StepExecuted (transactional completion)"
        );
        assert!(lambda_called, "lambda must execute for the target step");

        let log = calls.lock().unwrap();
        assert_eq!(log.iter().filter(|c| c.is_schedule_at()).count(), 0, "no new rows in step mode");

        let cas: Vec<_> = log.iter().filter(|c| c.is_complete_and_schedule()).collect();
        assert_eq!(cas.len(), 1, "complete_and_schedule called exactly once");

        if let Call::CompleteAndSchedule { completed_task_id, new_task_id, new_metadata, .. } =
            &cas[0]
        {
            assert_eq!(completed_task_id, "exec-1:step:charge-card");
            assert_eq!(new_task_id, "exec-1-b1");
            assert_eq!(new_metadata["mode"], "body");
            assert_eq!(new_metadata["segment"], 1);
            assert_eq!(new_metadata["execution_id"], "exec-1");
        } else {
            panic!("unexpected call variant");
        }
    }

    #[tokio::test]
    async fn step_mode_nontarget_step_reads_cache_with_zero_writes() {
        let (scheduler, calls) = RecordingScheduler::builder()
            .step_completed("exec-1", "step-one", serde_json::json!(21))
            .build();
        let mut ctx = make_step_ctx(scheduler, "step-two", 2);

        let mut lambda_called = false;
        let result: Result<i32, _> = ctx
            .step("step-one", || {
                lambda_called = true;
                async { Ok::<i32, StepError>(0) }
            })
            .await;

        assert_eq!(result.unwrap(), 21, "should return the cached result");
        assert!(!lambda_called, "lambda must not run for a non-target step");

        let log = calls.lock().unwrap();
        assert_eq!(log.iter().filter(|c| c.is_schedule_at()).count(), 0);
        assert_eq!(log.iter().filter(|c| c.is_complete_and_schedule()).count(), 0);
    }

    // ── body mode: wait_all ───────────────────────────────────────────────────

    #[tokio::test]
    async fn wait_all_body_mode_n_unscheduled_steps_creates_n_children_plus_one_coordinator() {
        // All three steps are unconfigured → get_step_status returns Ok(None).
        let (scheduler, calls) = RecordingScheduler::builder().build();
        let mut ctx = make_body_ctx(scheduler, 0);

        let h1 = ctx.schedule_step("step-a", || async { Ok::<u32, StepError>(1) });
        let h2 = ctx.schedule_step("step-b", || async { Ok::<u32, StepError>(2) });
        let h3 = ctx.schedule_step("step-c", || async { Ok::<u32, StepError>(3) });
        let result = ctx.wait_all(vec![h1, h2, h3]).await;

        assert!(matches!(result, Err(StepError::Scheduled { .. })));

        let log = calls.lock().unwrap();
        let inserts: Vec<&serde_json::Value> = log
            .iter()
            .filter_map(|c| {
                if let Call::ScheduleAt { metadata, .. } = c { Some(metadata) } else { None }
            })
            .collect();

        assert_eq!(inserts.len(), 4, "3 child step rows + 1 coordinator = 4 total inserts");

        let children: Vec<_> = inserts
            .iter()
            .filter(|m| m.get("is_wait_all_child").and_then(|v| v.as_bool()).unwrap_or(false))
            .collect();
        assert_eq!(children.len(), 3, "three children each marked is_wait_all_child=true");

        let coordinators: Vec<_> =
            inserts.iter().filter(|m| m["step_type"] == "wait_all").collect();
        assert_eq!(coordinators.len(), 1, "exactly one coordinator task");
        assert_eq!(coordinators[0]["segment"], 1, "coordinator targets the next body segment");
        assert_eq!(coordinators[0]["mode"], "step");
    }

    #[tokio::test]
    async fn wait_all_body_mode_all_completed_returns_results_with_zero_new_tasks() {
        let (scheduler, calls) = RecordingScheduler::builder()
            .step_completed("exec-1", "step-a", serde_json::json!(10))
            .step_completed("exec-1", "step-b", serde_json::json!(20))
            .build();
        let mut ctx = make_body_ctx(scheduler, 1);

        let h1 = ctx.schedule_step("step-a", || async { Ok::<u32, StepError>(99) });
        let h2 = ctx.schedule_step("step-b", || async { Ok::<u32, StepError>(99) });
        let results = ctx.wait_all(vec![h1, h2]).await.unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(*results[0].as_ref().unwrap(), 10u32);
        assert_eq!(*results[1].as_ref().unwrap(), 20u32);

        let log = calls.lock().unwrap();
        assert_eq!(
            log.iter().filter(|c| c.is_schedule_at()).count(),
            0,
            "all steps already completed — no new rows inserted"
        );
        assert_eq!(log.iter().filter(|c| c.is_complete_and_schedule()).count(), 0);
    }

    // ── step mode: wait_all child execution ───────────────────────────────────

    #[tokio::test]
    async fn wait_all_step_mode_target_child_calls_mark_completed_once_not_complete_and_schedule() {
        let (scheduler, calls) = RecordingScheduler::builder().build();

        let mut ctx = TaskContext::new(
            scheduler,
            "exec-1",
            "test-task",
            ExecutionState::default(),
            "lock-tok",
            serde_json::json!({}),
        )
        .with_task_id("exec-1:step:step-b".to_string())
        .with_execution_mode(ExecutionMode::Step {
            target_step: "step-b".to_string(),
            step_type: crate::execution_model::StepKind::Step,
            next_body_segment: 1,
            retry_attempt: 0,
            retry_config: None,
        });

        let h1 = ctx.schedule_step("step-a", || async { Ok::<u32, StepError>(0) });
        let h2 = ctx.schedule_step("step-b", || async { Ok::<u32, StepError>(2) });
        let h3 = ctx.schedule_step("step-c", || async { Ok::<u32, StepError>(0) });
        let result = ctx.wait_all(vec![h1, h2, h3]).await;

        assert!(
            matches!(result, Err(StepError::StepExecuted { ref step }) if step == "step-b"),
            "wait_all child must return StepExecuted"
        );

        let log = calls.lock().unwrap();

        let mc: Vec<_> = log
            .iter()
            .filter_map(|c| {
                if let Call::MarkCompleted { task_id, .. } = c { Some(task_id.as_str()) } else { None }
            })
            .collect();
        assert_eq!(mc.len(), 1, "complete_step_no_resume delegates to mark_completed once");
        assert_eq!(mc[0], "exec-1:step:step-b");

        assert_eq!(
            log.iter().filter(|c| c.is_complete_and_schedule()).count(),
            0,
            "coordinator handles body scheduling; wait_all children must NOT call complete_and_schedule"
        );
        assert_eq!(log.iter().filter(|c| c.is_schedule_at()).count(), 0);
    }

    // ── body mode: sleep ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn sleep_body_mode_inserts_one_sleep_task_with_exact_wake_time() {
        let (scheduler, calls) = RecordingScheduler::builder().build();
        let mut ctx = make_body_ctx(scheduler, 0);

        let wake_time = chrono::Utc::now() + chrono::Duration::hours(1);
        let result = ctx.sleep_until(wake_time).await;

        assert!(matches!(result, Err(StepError::Scheduled { .. })));

        let log = calls.lock().unwrap();
        let inserts: Vec<_> = log
            .iter()
            .filter_map(|c| {
                if let Call::ScheduleAt { task_id, execution_time, metadata, .. } = c {
                    Some((task_id, execution_time, metadata))
                } else {
                    None
                }
            })
            .collect();

        assert_eq!(inserts.len(), 1, "sleep inserts exactly one task row");
        let (task_id, exec_time, meta) = inserts[0];
        assert_eq!(task_id, "exec-1:sleep:1");
        assert_eq!(meta["mode"], "step");
        assert_eq!(meta["step_type"], "sleep");
        assert_eq!(meta["segment"], 1);
        assert_eq!(meta["execution_id"], "exec-1");
        let diff = (*exec_time - wake_time).num_seconds().abs();
        assert!(diff < 1, "sleep task execution_time must equal the requested wake_time");
    }

    // ── step_with_retry: new execution model ──────────────────────────────────

    /// Helper: make a step-mode context with a retry config embedded.
    fn make_step_ctx_with_retry(
        scheduler: std::sync::Arc<RecordingScheduler>,
        target: &str,
        next_body_segment: usize,
        retry_attempt: usize,
        retry_config: RetryConfig,
    ) -> TaskContext<RecordingScheduler> {
        let task_id = format!("exec-1:step:{target}");
        TaskContext::new(
            scheduler,
            "exec-1",
            "test-task",
            ExecutionState::default(),
            "lock-tok",
            serde_json::json!({}),
        )
        .with_task_id(task_id)
        .with_execution_mode(ExecutionMode::Step {
            target_step: target.to_string(),
            step_type: crate::execution_model::StepKind::Step,
            next_body_segment,
            retry_attempt,
            retry_config: Some(retry_config),
        })
    }

    /// In body mode, `step_with_retry` must embed the retry_config in the
    /// scheduled step task's metadata so the step task can retry on failure.
    #[tokio::test]
    async fn body_mode_step_with_retry_embeds_retry_config_in_metadata() {
        let (scheduler, calls) = RecordingScheduler::builder().build();
        let mut ctx = make_body_ctx(scheduler, 0);

        let config = RetryConfig::fixed(3, Duration::from_secs(5));
        let result = ctx
            .step_with_retry("charge-card", config, || async {
                Ok::<u32, StepError>(99)
            })
            .await;

        assert!(
            matches!(result, Err(StepError::Scheduled { ref step, .. }) if step == "charge-card"),
            "step_with_retry in body mode returns Scheduled on first call"
        );

        let log = calls.lock().unwrap();
        let inserts: Vec<_> = log.iter().filter(|c| c.is_schedule_at()).collect();
        assert_eq!(inserts.len(), 1, "exactly one task row inserted");

        if let Call::ScheduleAt { metadata, .. } = &inserts[0] {
            assert!(
                metadata.get("retry_config").is_some(),
                "retry_config must be present in step task metadata"
            );
            let embedded: RetryConfig =
                serde_json::from_value(metadata["retry_config"].clone()).unwrap();
            assert_eq!(embedded.max_attempts, 3);
        }
    }

    /// When the step lambda fails and retries remain, `step_step_mode` must call
    /// `mark_failed` with a future execution time (the retry delay) and return
    /// `StepExecuted` so the worker does not also call `mark_failed`.
    #[tokio::test]
    async fn step_mode_failure_with_retries_remaining_schedules_retry_via_mark_failed() {
        let (scheduler, calls) = RecordingScheduler::builder().build();
        // retry_attempt=0 means this is the first attempt; 3 retries are allowed.
        let mut ctx = make_step_ctx_with_retry(
            scheduler,
            "charge-card",
            1,
            0,
            RetryConfig::fixed(3, Duration::from_secs(10)),
        );

        let result = ctx
            .step("charge-card", || async {
                Err::<u32, _>(StepError::Failed {
                    step: "charge-card".to_string(),
                    reason: "card declined".to_string(),
                })
            })
            .await;

        // Worker receives StepExecuted — it does nothing further (no double mark_failed).
        assert!(
            matches!(result, Err(StepError::StepExecuted { ref step }) if step == "charge-card"),
            "must return StepExecuted so the worker skips its own mark_failed"
        );

        let log = calls.lock().unwrap();
        let failures: Vec<_> = log.iter().filter(|c| c.is_mark_failed()).collect();
        assert_eq!(failures.len(), 1, "exactly one mark_failed call for the retry");

        if let Call::MarkFailed { task_id, next_execution_time, .. } = &failures[0] {
            assert_eq!(task_id, "exec-1:step:charge-card");
            assert!(
                next_execution_time.is_some(),
                "retry must carry a future execution_time for the delay"
            );
            let delay_secs = (*next_execution_time.as_ref().unwrap() - chrono::Utc::now())
                .num_seconds();
            assert!(delay_secs > 0, "retry must be in the future");
        }
    }

    /// When all retries are exhausted the original error propagates and
    /// `mark_failed` is NOT called (the worker handles task failure itself).
    #[tokio::test]
    async fn step_mode_failure_with_retries_exhausted_propagates_error() {
        let (scheduler, calls) = RecordingScheduler::builder().build();
        // retry_attempt=3 means 3 retries have already happened; max is 3.
        let mut ctx = make_step_ctx_with_retry(
            scheduler,
            "charge-card",
            1,
            3,
            RetryConfig::fixed(3, Duration::from_secs(10)),
        );

        let result = ctx
            .step("charge-card", || async {
                Err::<u32, _>(StepError::Failed {
                    step: "charge-card".to_string(),
                    reason: "still declining".to_string(),
                })
            })
            .await;

        assert!(
            matches!(result, Err(StepError::Failed { .. })),
            "error must propagate when retries are exhausted"
        );

        // The worker's generic Err arm calls mark_failed; step_step_mode must NOT.
        let log = calls.lock().unwrap();
        assert_eq!(
            log.iter().filter(|c| c.is_mark_failed()).count(),
            0,
            "step_step_mode must not call mark_failed when retries are exhausted"
        );
    }

    /// A successful step in step mode must NOT trigger a retry — it must
    /// complete transactionally and schedule the next body segment as usual.
    #[tokio::test]
    async fn step_mode_success_with_retry_config_completes_normally() {
        let (scheduler, calls) = RecordingScheduler::builder().build();
        let mut ctx = make_step_ctx_with_retry(
            scheduler,
            "charge-card",
            1,
            0,
            RetryConfig::fixed(3, Duration::from_secs(10)),
        );

        let result = ctx
            .step("charge-card", || async { Ok::<u32, StepError>(42) })
            .await;

        assert!(
            matches!(result, Err(StepError::StepExecuted { ref step }) if step == "charge-card"),
            "successful step must return StepExecuted"
        );

        let log = calls.lock().unwrap();
        assert_eq!(
            log.iter().filter(|c| c.is_mark_failed()).count(),
            0,
            "no mark_failed on success"
        );
        assert_eq!(
            log.iter().filter(|c| c.is_complete_and_schedule()).count(),
            1,
            "complete_and_schedule called once to commit step and schedule next body"
        );
    }
}
