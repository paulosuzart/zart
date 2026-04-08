//! TaskContext — the primary interface for durable step execution.
//!
//! This module contains [`TaskContext`], which provides the step execution API
//! (`execute_step`, `schedule_step`, `wait_all`, `sleep_until`, `wait_for_event`)
//! and access to the initial payload and execution metadata.

use crate::error::StepError;
use crate::execution_model::ExecutionMode;

use crate::retry::RetryConfig;
use crate::step_types::{StepDefId, StepRequest, StepResult};
use scheduler::StorageBackend;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;
use tracing::instrument;

use super::state::{PendingFn, StepHandle};
use super::step_context::StepContext;
use super::step_trait::ZartStep;

// ── TaskContext ───────────────────────────────────────────────────────────────

/// Provides the step execution API (`step`, `step_with_retry`, `step_with_timeout`, …)
/// and access to the initial payload and execution metadata.
pub struct TaskContext {
    /// The underlying scheduler (used to schedule step tasks).
    pub(crate) scheduler: Arc<dyn StorageBackend>,
    /// Unique identifier of the enclosing durable execution.
    execution_id: String,
    /// The `run_id` of the current execution run (`zart_execution_runs.run_id`).
    /// Extracted from `task.metadata["run_id"]` by the worker.
    /// Used as the FK for step rows and as the prefix of step task IDs.
    run_id: String,
    /// The `task_id` of the task currently being executed.
    /// Differs from `execution_id` for step/body-segment tasks in the new model.
    pub(crate) task_id: String,
    /// Registered name of the task handler.
    task_name: String,
    /// Opaque lock token from the current pick-up. Required for scheduler calls.
    pub(crate) lock_token: String,
    /// The original JSON payload supplied when the execution was started.
    data: serde_json::Value,
    /// How this task should behave when executing steps.
    /// `Body`/`Step` use per-row tasks.
    pub(crate) execution_mode: ExecutionMode,
    /// Step names whose cached result has been returned during the current body re-run.
    ///
    /// The database enforces `task_id PRIMARY KEY` which prevents duplicate step task rows
    /// (each step ID is `{run_id}:step:{step_name}`). However, that constraint only
    /// applies at INSERT time. On body re-run, the framework returns cached Completed results
    /// without re-inserting — so two calls with the same step name in a loop would silently
    /// return the same cached value for both iterations.
    ///
    /// This set provides a fast-fail complement to the DB constraint: if a step name is
    /// encountered twice after its cached result has already been returned in the same
    /// body re-run, return an error immediately with a clear diagnosis.
    seen_step_names: HashSet<String>,
}

impl TaskContext {
    /// Construct a new `TaskContext`.
    ///
    /// Called by the [`Worker`] when it picks up a task.
    pub fn new(
        scheduler: Arc<dyn StorageBackend>,
        execution_id: impl Into<String>,
        task_name: impl Into<String>,
        lock_token: impl Into<String>,
        data: serde_json::Value,
    ) -> Self {
        let execution_id = execution_id.into();
        let task_id = execution_id.clone();
        let run_id = execution_id.clone();
        Self {
            scheduler,
            task_id,
            execution_id,
            run_id,
            task_name: task_name.into(),
            lock_token: lock_token.into(),
            data,
            execution_mode: ExecutionMode::Body,
            seen_step_names: HashSet::new(),
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

    /// Set the run_id for the current execution run.
    ///
    /// This must be called by the worker with the `run_id` from `task.metadata["run_id"]`
    /// so that step rows are inserted with the correct FK into `zart_execution_runs`.
    pub fn with_run_id(mut self, run_id: impl Into<String>) -> Self {
        self.run_id = run_id.into();
        self
    }

    /// Construct a [`StepContext`] with the current execution metadata.
    ///
    /// This is used internally to pass read-only metadata to step closures.
    pub(crate) fn step_context(&self) -> StepContext {
        let (current_attempt, max_retries) = match &self.execution_mode {
            ExecutionMode::Step {
                retry_attempt,
                retry_config,
                ..
            } => (
                *retry_attempt,
                retry_config.as_ref().map(|rc| rc.max_attempts),
            ),
            _ => (0, None),
        };
        StepContext {
            execution_id: self.execution_id.clone(),
            task_name: self.task_name.clone(),
            current_attempt,
            max_retries,
        }
    }

    // ── Internal step execution helpers (used by execute_step) ────────────────

    /// Internal dispatcher for `execute_step`, delegating orchestration to
    /// declarative dispatch/behaviors.
    async fn step_internal<T, F, Fut>(
        &mut self,
        step_name: &str,
        _retry_config: Option<RetryConfig>,
        step_fn: F,
    ) -> Result<T, StepError>
    where
        T: Serialize + for<'de> Deserialize<'de>,
        F: FnOnce(StepContext) -> Fut,
        Fut: std::future::Future<Output = Result<T, StepError>>,
    {
        match &self.execution_mode {
            ExecutionMode::Body => {
                let result = crate::step_types::dispatch::step_internal(
                    StepDefId::Step,
                    self,
                    StepRequest::new_step(step_name, _retry_config.as_ref()),
                    None,
                )
                .await;

                match result {
                    Ok(v) => {
                        if !self.seen_step_names.insert(step_name.to_string()) {
                            return Err(StepError::Failed {
                                step: step_name.to_string(),
                                reason: format!(
                                    "duplicate step name '{step_name}' in the same execution — \
                                     each call must produce a unique step name. \
                                     Use a {{field}} template in #[zart_step] (e.g. \"my-step-{{index}}\") \
                                     or call `.with_id(\"...\")` at the call site."
                                ),
                            });
                        }
                        Ok(v)
                    }
                    Err(e) => Err(e),
                }
            }
            ExecutionMode::Step { target_step, .. } => {
                let target = target_step.clone();

                if step_name == target {
                    let immediate_outcome = match step_fn(self.step_context()).await {
                        Ok(v) => serde_json::to_value(v)
                            .map(StepResult::Executed)
                            .map_err(|e| StepError::Failed {
                                step: step_name.to_string(),
                                reason: format!("failed to serialize result: {e}"),
                            }),
                        Err(e) => Err(e),
                    };

                    crate::step_types::dispatch::step_internal_target_step(
                        StepDefId::Step,
                        self,
                        step_name,
                        immediate_outcome,
                    )
                    .await
                } else {
                    crate::step_types::dispatch::step_internal(
                        StepDefId::Step,
                        self,
                        StepRequest::new_step(step_name, None),
                        None,
                    )
                    .await
                }
            }
        }
    }

    /// Execute a [`ZartStep`] struct with automatic retry and timeout handling.
    ///
    /// This is the framework-level entry point for step execution when using the
    /// `ZartStep` trait (either manually implemented or generated by `#[zart_step]`).
    ///
    /// # How it works
    ///
    /// 1. Reads `step.step_name()` for tracking
    /// 2. Reads `step.retry_config()` and applies retries if set
    /// 3. Reads `step.timeout()` and applies timeout if set
    /// 4. Calls `step.run(ctx)` to execute the step logic
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Manual ZartStep
    /// let (city, state) = ctx.execute_step(LookupZipStep {
    ///     client: &client,
    ///     zip_code: &data.zip_code,
    /// }).await?;
    ///
    /// // Or via #[zart_step] macro (step functions return the struct)
    /// let (city, state) = ctx.execute_step(lookup_zip(&client, &data.zip_code)).await?;
    /// ```
    #[instrument(name = "step.execute_typed", skip(self, step), fields(step_name = tracing::field::Empty))]
    pub async fn execute_step<S: ZartStep + Send>(
        &mut self,
        step: S,
    ) -> Result<S::Output, StepError> {
        let step_name = step.step_name();
        let retry_config = step.retry_config();
        let timeout_duration = step.timeout();

        // Set the span field
        tracing::Span::current().record("step_name", step_name.as_ref());

        // Build the step logic
        let step_fn = move |sctx: StepContext| {
            let this = step;
            async move { this.run(sctx).await }
        };

        // Apply timeout wrapper if needed, then delegate to step_internal
        match timeout_duration {
            Some(timeout_dur) => {
                let retry_cfg = retry_config;
                let step_name_owned = step_name.to_string();
                let wrapped_fn = move |sctx: StepContext| {
                    let f = step_fn(sctx);
                    async move {
                        tokio::time::timeout(timeout_dur, f).await.map_err(|_| {
                            StepError::Timeout {
                                step: step_name_owned.clone(),
                                duration: timeout_dur,
                            }
                        })?
                    }
                };
                self.step_internal(&step_name, retry_cfg, wrapped_fn).await
            }
            None => self.step_internal(&step_name, retry_config, step_fn).await,
        }
    }

    /// Register a [`ZartStep`] for parallel execution without waiting for it to complete.
    ///
    /// Unlike [`execute_step`](Self::execute_step), this method does **not** block. All registered
    /// steps run when [`wait_all`](Self::wait_all) is called.
    ///
    /// # Re-entry behaviour
    ///
    /// - **Step absent**: registers it as `Scheduled` and stores the lambda.
    /// - **Step `Scheduled`**: stores the lambda for execution in `wait_all`.
    /// - **Step `Completed`**: discards the lambda; `wait_all` will return the cached result.
    pub fn schedule_step<S: ZartStep + Send + 'static>(
        &mut self,
        step: S,
    ) -> StepHandle<S::Output> {
        let step_name = step.step_name();
        let step_name_str = step_name.to_string();

        // schedule_step just returns a handle with the lambda.
        // Actual scheduling (DB insert) happens in wait_all.
        // In step mode, only the target step handle carries the lambda.
        let is_target = matches!(&self.execution_mode,
            ExecutionMode::Step { target_step, .. } if target_step.as_str() == step_name.as_ref());

        let pending: Option<PendingFn> =
            if !matches!(&self.execution_mode, ExecutionMode::Step { .. }) || is_target {
                let name_for_err = step_name_str.clone();
                Some(Box::new(move |sctx: StepContext| {
                    let step = step;
                    Box::pin(async move {
                        let result = step.run(sctx).await?;
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
    /// Orchestration is delegated to declarative dispatch:
    /// - body mode routes through wait-group barrier behavior
    /// - step mode routes the target child through wait-group child behavior
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
            ExecutionMode::Body => {
                let group_step_name = format!("__wg__all__{}", uuid::Uuid::new_v4());

                // Extract names before awaits; avoid borrowing non-Sync handles across await points.
                let step_names: Vec<String> = handles.iter().map(|h| h.step_name.clone()).collect();

                let req = StepRequest::new_wait_group_barrier(&group_step_name, &step_names, 0);

                let json = crate::step_types::dispatch::step_internal::<serde_json::Value>(
                    StepDefId::WaitGroupBarrier,
                    self,
                    req,
                    None,
                )
                .await?;

                // If body behavior reports completion, deserialize results by handle order.
                if let serde_json::Value::Array(values) = json {
                    let mut results = Vec::with_capacity(values.len());
                    for (idx, value) in values.into_iter().enumerate() {
                        let step_name = step_names
                            .get(idx)
                            .cloned()
                            .unwrap_or_else(|| "__wait_all".to_string());

                        if value.is_null() {
                            results.push(Err(StepError::Failed {
                                step: step_name,
                                reason: "step completed but result is missing".to_string(),
                            }));
                        } else {
                            let val =
                                serde_json::from_value(value).map_err(|e| StepError::Failed {
                                    step: step_name,
                                    reason: format!("deserialize error: {e}"),
                                })?;
                            results.push(Ok(val));
                        }
                    }
                    Ok(results)
                } else {
                    Err(StepError::Failed {
                        step: "__wait_all".to_string(),
                        reason: "wait-group barrier returned non-array payload".to_string(),
                    })
                }
            }
            ExecutionMode::Step { target_step, .. } => {
                let target = target_step.clone();

                for handle in handles {
                    if handle.step_name == target {
                        if let Some(pending_fn) = handle.pending {
                            let req = StepRequest::new_wait_group_child(&target);
                            let _ =
                                crate::step_types::dispatch::step_internal::<serde_json::Value>(
                                    StepDefId::WaitGroupChild,
                                    self,
                                    req,
                                    Some(pending_fn),
                                )
                                .await?;
                            return Err(StepError::StepExecuted {
                                step: target.to_string(),
                            });
                        }

                        // No pending fn (already completed target child): treat as handled.
                        return Err(StepError::StepExecuted {
                            step: target.to_string(),
                        });
                    }
                }

                Err(StepError::Failed {
                    step: target.to_string(),
                    reason: "target step not found in wait_all handles".to_string(),
                })
            }
        }
    }

    // ── Sleep ────────────────────────────────────────────────────────────────

    /// Suspend execution for `duration`, resuming at `now + duration`.
    ///
    /// The `step_name` must be a stable, unique string within this execution body.
    /// It is used as the database key for durably persisting the sleep checkpoint.
    /// Treat it like a migration name — do not change it after the execution has started.
    ///
    /// # Duplicate name detection
    ///
    /// If the same `step_name` is used twice in one execution body, returns an error.
    /// Each sleep call must have a unique name so the framework can skip it on replay.
    pub async fn sleep(
        &mut self,
        step_name: &str,
        duration: std::time::Duration,
    ) -> Result<(), StepError> {
        let wake_time = chrono::Utc::now()
            + chrono::Duration::from_std(duration).unwrap_or(chrono::Duration::zero());
        self.sleep_until(step_name, wake_time).await
    }

    /// Suspend execution until `wake_time`.
    ///
    /// The `step_name` must be a stable, unique string within this execution body.
    /// See [`sleep`](Self::sleep) for details.
    pub async fn sleep_until(
        &mut self,
        step_name: &str,
        wake_time: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), StepError> {
        if !self.seen_step_names.insert(step_name.to_string()) {
            return Err(StepError::Failed {
                step: step_name.to_string(),
                reason: format!(
                    "duplicate sleep name '{step_name}' — each sleep must have a unique stable ID"
                ),
            });
        }
        let req = StepRequest::new_sleep(step_name, wake_time);
        crate::step_types::dispatch::step_internal::<serde_json::Value>(
            StepDefId::Sleep,
            self,
            req,
            None,
        )
        .await
        .map(|_| ())
    }

    /// Wait for an external event to be delivered to this execution.
    ///
    /// This method captures deadline intent and delegates orchestration to
    /// declarative dispatch/behaviors for both body and step replay modes.
    pub async fn wait_for_event<T>(
        &mut self,
        event_name: &str,
        timeout: Option<std::time::Duration>,
    ) -> Result<T, StepError>
    where
        T: for<'de> Deserialize<'de>,
    {
        let deadline = timeout.and_then(|d| {
            chrono::Duration::from_std(d)
                .ok()
                .map(|cd| chrono::Utc::now() + cd)
        });

        let req = StepRequest::new_wait_for_event(event_name, deadline);
        let json = crate::step_types::dispatch::step_internal::<serde_json::Value>(
            StepDefId::WaitForEvent,
            self,
            req,
            None,
        )
        .await?;

        serde_json::from_value(json).map_err(|e| StepError::Failed {
            step: event_name.to_string(),
            reason: format!("failed to deserialize event result: {e}"),
        })
    }

    // ── Capture ─────────────────────────────────────────────────────────────

    /// Capture a synchronous, pure value durably.
    ///
    /// On first body run: evaluates `f()`, writes the result as a completed step row,
    /// returns the value — body walk continues without parking.
    /// On replay: returns the cached DB value; `f` is never called.
    ///
    /// The `step_name` must be a stable, unique string within this execution body.
    /// Treat it like a migration name — do not change it after the execution has started.
    pub async fn capture<T, F>(&mut self, step_name: &str, f: F) -> Result<T, StepError>
    where
        T: Serialize + for<'de> Deserialize<'de>,
        F: FnOnce() -> T,
    {
        if !self.seen_step_names.insert(step_name.to_string()) {
            return Err(StepError::Failed {
                step: step_name.to_string(),
                reason: format!(
                    "duplicate capture name '{step_name}' — each capture must have a unique stable ID"
                ),
            });
        }
        crate::step_types::dispatch::capture_internal(self, step_name, f).await
    }

    /// Capture the current UTC time durably.
    ///
    /// Shorthand for `ctx.capture(step_name, chrono::Utc::now)`.
    pub async fn now(
        &mut self,
        step_name: &str,
    ) -> Result<chrono::DateTime<chrono::Utc>, StepError> {
        self.capture(step_name, chrono::Utc::now).await
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

    /// Returns the current run identifier.
    pub(crate) fn run_id(&self) -> &str {
        &self.run_id
    }

    /// Returns the registered name of this task handler.
    pub fn task_name(&self) -> &str {
        &self.task_name
    }

    /// Returns the registered name of this task handler (crate-visible accessor).
    pub(crate) fn task_name_internal(&self) -> &str {
        &self.task_name
    }

    /// Returns the current retry attempt number (0-indexed) for this step.
    ///
    /// Returns `0` if this is the first attempt or if no retry is configured.
    /// Returns `1` for the first retry, `2` for the second retry, etc.
    ///
    /// This is useful for implementing intentional failure patterns in examples
    /// or for logging/debugging retry behavior.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Inside a ZartStep::run implementation:
    /// if ctx.current_attempt() == 0 {
    ///     // Simulate transient failure on first attempt
    ///     return Err(StepError::Failed { step: "my-step".into(), reason: "Temporary failure".into() });
    /// }
    /// // Succeed on retry
    /// Ok(SuccessResult { message: "Succeeded!" })
    /// ```
    pub fn current_attempt(&self) -> usize {
        match &self.execution_mode {
            ExecutionMode::Step { retry_attempt, .. } => *retry_attempt,
            _ => 0,
        }
    }

    /// Returns the maximum number of retry attempts configured for this step.
    ///
    /// Returns `None` if no retry policy is configured for the current step.
    /// Returns `Some(n)` where `n` is the max retry count from the [`RetryConfig`].
    ///
    /// Note: This is the maximum number of *retries*, not total attempts.
    /// Total attempts = `max_retries + 1` (initial attempt + retries).
    pub fn max_retries(&self) -> Option<usize> {
        match &self.execution_mode {
            ExecutionMode::Step { retry_config, .. } => {
                retry_config.as_ref().map(|rc| rc.max_attempts)
            }
            _ => None,
        }
    }

    /// Returns `true` if this is a retry attempt (i.e., not the first attempt).
    ///
    /// Equivalent to `ctx.current_attempt() > 0`.
    /// Useful for conditional logic that should only run on retries.
    pub fn is_retry_attempt(&self) -> bool {
        self.current_attempt() > 0
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────
