//! TaskContext — the primary interface for durable step execution.
//!
//! This module contains [`TaskContext`], which provides the step execution API
//! (`execute_step`, `schedule_step`, `wait_all`, `sleep_until`, `wait_for_event`)
//! and access to the initial payload and execution metadata.

use crate::emit_metric;
use crate::error::StepError;
use crate::execution_model::ExecutionMode;
#[cfg(feature = "metrics")]
use crate::metrics::{STEP_DURATION_SECONDS, STEPS_TOTAL};

use crate::retry::RetryConfig;
use crate::step_ops;
use scheduler::{StepLookup, StorageBackend, TaskStatus};
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

    /// Internal dispatcher for step execution, routing to body or step mode.
    async fn step_internal<T, F, Fut>(
        &mut self,
        step_name: &str,
        retry_config: Option<RetryConfig>,
        step_fn: F,
    ) -> Result<T, StepError>
    where
        T: Serialize + for<'de> Deserialize<'de>,
        F: FnOnce(StepContext) -> Fut,
        Fut: std::future::Future<Output = Result<T, StepError>>,
    {
        match &self.execution_mode {
            ExecutionMode::Body => self.step_body_mode(step_name, retry_config, step_fn).await,
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

    /// Step execution in body mode: look up the step task in the DB.
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
        F: FnOnce(StepContext) -> Fut,
        Fut: std::future::Future<Output = Result<T, StepError>>,
    {
        let lookup = self
            .scheduler
            .get_step_status(&self.run_id, step_name)
            .await
            .map_err(|e| StepError::Failed {
                step: step_name.to_string(),
                reason: e.to_string(),
            })?;

        match lookup {
            Some(StepLookup {
                status: TaskStatus::Completed,
                result: Some(json),
                ..
            }) => {
                // Complement to the DB `task_id PRIMARY KEY` constraint: the DB prevents
                // duplicate step task rows at INSERT time, but on re-run the framework
                // returns cached Completed results without inserting. Two calls with the
                // same step name in a loop would silently return the same cached value.
                // Fast-fail here with a clear error before wrong data is returned.
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
                serde_json::from_value(json).map_err(|e| StepError::Failed {
                    step: step_name.to_string(),
                    reason: format!("failed to deserialize cached result: {e}"),
                })
            }
            Some(StepLookup {
                status: TaskStatus::Completed,
                result: None,
                ..
            }) => Err(StepError::Failed {
                step: step_name.to_string(),
                reason: "step completed but result is missing".to_string(),
            }),
            Some(_) => {
                // Scheduled or PickedUp — step task exists, body should exit and wait.
                emit_metric!(
                    STEPS_TOTAL
                        .with_label_values(&["scheduled", step_name])
                        .inc()
                );
                Err(StepError::Scheduled {
                    step: step_name.to_string(),
                    next_execution: None,
                })
            }
            None => {
                // First time: insert step task row and exit.
                // step_id = "{run_id}:step:{step_name}" — unique per run per step.
                emit_metric!(
                    STEPS_TOTAL
                        .with_label_values(&["scheduled", step_name])
                        .inc()
                );
                let task_id = format!("{}:step:{}", self.run_id, step_name);
                step_ops::schedule_step_task(
                    &*self.scheduler,
                    step_ops::StepTaskSpec {
                        task_id: &task_id,
                        task_name: &self.task_name,
                        run_id: &self.run_id,
                        step_name,
                        data: self.data.clone(),
                        retry_config: retry_config.as_ref(),
                    },
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

    /// Step execution in step mode: replay the body until the target step is reached.
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
        F: FnOnce(StepContext) -> Fut,
        Fut: std::future::Future<Output = Result<T, StepError>>,
    {
        if step_name == target {
            // Execute the lambda for this step.
            #[cfg(feature = "metrics")]
            let step_timer = STEP_DURATION_SECONDS
                .with_label_values(&[step_name, "completed"])
                .start_timer();

            let lambda_result = step_fn(self.step_context()).await;
            let result = match lambda_result {
                Ok(v) => v,
                Err(e) => {
                    // Check if retry is configured and allowed.
                    let (retry_config, retry_attempt) = match &self.execution_mode {
                        ExecutionMode::Step {
                            retry_config,
                            retry_attempt,
                            ..
                        } => (retry_config.clone(), *retry_attempt),
                        _ => (None, 0),
                    };

                    if let Some(rc) = retry_config
                        && let Some(delay) = rc.delay_for(retry_attempt + 1)
                    {
                        let retry_time = chrono::Utc::now()
                            + chrono::Duration::from_std(delay).unwrap_or(chrono::Duration::zero());
                        // Reschedule this step task for retry.
                        if let Err(sched_err) = step_ops::reschedule_step_for_retry(
                            &*self.scheduler,
                            &self.task_id,
                            retry_attempt + 1,
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
                        emit_metric!({
                            step_timer.observe_duration();
                            STEPS_TOTAL
                                .with_label_values(&["completed", step_name])
                                .inc();
                        });
                        return Err(StepError::StepExecuted {
                            step: step_name.to_string(),
                        });
                    }

                    // Step failed with no retry.
                    emit_metric!({
                        step_timer.stop_and_discard();
                        let fail_timer = STEP_DURATION_SECONDS
                            .with_label_values(&[step_name, "failed"])
                            .start_timer();
                        fail_timer.observe_duration();
                        STEPS_TOTAL.with_label_values(&["failed", step_name]).inc();
                    });
                    return Err(e);
                }
            };

            // Step completed successfully.
            emit_metric!({
                step_timer.observe_duration();
                STEPS_TOTAL
                    .with_label_values(&["completed", step_name])
                    .inc();
            });
            let serialized = serde_json::to_value(&result).map_err(|e| StepError::Failed {
                step: step_name.to_string(),
                reason: format!("failed to serialize result: {e}"),
            })?;

            // Sequential step mode: never a wait_all child.
            let is_wait_all_child = false;

            let step_task_id = self.task_id.clone();

            if is_wait_all_child {
                let attempt_number = match &self.execution_mode {
                    ExecutionMode::Step { retry_attempt, .. } => *retry_attempt + 1,
                    _ => 1,
                };
                step_ops::complete_step_no_resume(
                    &*self.scheduler,
                    &step_task_id,
                    &step_task_id,
                    serialized,
                    &self.lock_token,
                    attempt_number,
                )
                .await
                .map_err(|e| StepError::Failed {
                    step: step_name.to_string(),
                    reason: e.to_string(),
                })?;
            } else {
                let next_body_task_id = format!("{}:body:after:{}", self.run_id, step_name);
                step_ops::complete_step_and_schedule_body(
                    &*self.scheduler,
                    step_ops::ResumeBodySpec {
                        step_task_id: &step_task_id,
                        step_id: &step_task_id,
                        result: serialized,
                        lock_token: &self.lock_token,
                        next_body_task_id: &next_body_task_id,
                        task_name: &self.task_name,
                        run_id: &self.run_id,
                        data: self.data.clone(),
                        attempt_number: {
                            match &self.execution_mode {
                                ExecutionMode::Step { retry_attempt, .. } => *retry_attempt + 1,
                                _ => 1,
                            }
                        },
                    },
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
                .get_step_status(&self.run_id, step_name)
                .await
                .map_err(|e| StepError::Failed {
                    step: step_name.to_string(),
                    reason: e.to_string(),
                })?;

            match lookup {
                Some(StepLookup {
                    status: TaskStatus::Completed,
                    result: Some(json),
                    ..
                }) => serde_json::from_value(json).map_err(|e| StepError::Failed {
                    step: step_name.to_string(),
                    reason: format!("failed to deserialize cached result: {e}"),
                }),
                Some(StepLookup {
                    status: TaskStatus::Completed,
                    result: None,
                    ..
                }) => Err(StepError::Failed {
                    step: step_name.to_string(),
                    reason: "step completed but result is missing".to_string(),
                }),
                _ => Err(StepError::Scheduled {
                    step: step_name.to_string(),
                    next_execution: None,
                }),
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
            ExecutionMode::Body => self.wait_all_body_mode(handles).await,
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
    ) -> Result<Vec<Result<T, StepError>>, StepError>
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        let coordinator_id = format!("{}:coord:wait_all:{}", self.run_id, uuid::Uuid::new_v4());

        // Extract step names upfront so we don't hold &StepHandle<T> across await points
        // (PendingFn is Send but not Sync, so &StepHandle<T> is not Send).
        let step_names: Vec<String> = handles.iter().map(|h| h.step_name.clone()).collect();

        let mut all_completed = true;
        let mut child_ids: Vec<String> = Vec::with_capacity(step_names.len());

        for step_name in &step_names {
            let child_task_id = format!("{}:step:{}", self.run_id, step_name);
            child_ids.push(child_task_id.clone());

            let lookup = self
                .scheduler
                .get_step_status(&self.run_id, step_name)
                .await
                .map_err(|e| StepError::Failed {
                    step: step_name.clone(),
                    reason: e.to_string(),
                })?;

            match lookup {
                Some(StepLookup {
                    status: TaskStatus::Completed,
                    ..
                }) => {}
                Some(_) => {
                    all_completed = false;
                }
                None => {
                    all_completed = false;
                    step_ops::schedule_wait_all_child(
                        &*self.scheduler,
                        &child_task_id,
                        &self.task_name,
                        &self.run_id,
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
                    .get_step_status(&self.run_id, step_name)
                    .await
                    .map_err(|e| StepError::Failed {
                        step: step_name.clone(),
                        reason: e.to_string(),
                    })?;
                match lookup {
                    Some(StepLookup {
                        status: TaskStatus::Completed,
                        result: Some(json),
                        ..
                    }) => {
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
            &self.run_id,
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
                    let json_result = pending_fn(self.step_context()).await;
                    match json_result {
                        Ok(json_val) => {
                            let step_task_id = self.task_id.clone();
                            let attempt_number = match &self.execution_mode {
                                ExecutionMode::Step { retry_attempt, .. } => *retry_attempt + 1,
                                _ => 1,
                            };
                            step_ops::complete_step_no_resume(
                                &*self.scheduler,
                                &step_task_id,
                                &step_task_id,
                                json_val,
                                &self.lock_token,
                                attempt_number,
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
            ExecutionMode::Body => {
                let sleep_task_id = format!("{}:step:__sleep", self.run_id);
                step_ops::schedule_sleep_task(
                    &*self.scheduler,
                    &sleep_task_id,
                    &self.task_name,
                    &self.run_id,
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
    /// # Control flow (body mode — first encounter)
    ///
    /// 1. Queries the DB for an existing step task row (`execution_id:step:event_name`).
    /// 2. If absent: inserts a `wait_for_event` step task with `execution_time = deadline`
    ///    (or `DateTime::MAX` when no timeout) and returns `Err(StepError::Scheduled)`.
    ///    The body task is then marked complete.
    /// 3. If `Completed`: deserializes the stored payload and returns `Ok(T)`.
    /// 4. If in-flight: returns `Err(StepError::Scheduled)` so the body exits and waits.
    ///
    /// # Control flow (step mode — replay)
    ///
    /// Looks up the step by name. If completed, returns the cached payload.
    /// Otherwise returns `Err(StepError::Scheduled)`.
    pub async fn wait_for_event<T>(
        &mut self,
        event_name: &str,
        timeout: Option<std::time::Duration>,
    ) -> Result<T, StepError>
    where
        T: for<'de> Deserialize<'de>,
    {
        match &self.execution_mode {
            ExecutionMode::Body => self.wait_for_event_body_mode(event_name, timeout).await,
            ExecutionMode::Step { .. } => self.wait_for_event_step_mode(event_name).await,
            ExecutionMode::Coordinator { .. } => Err(StepError::Failed {
                step: event_name.to_string(),
                reason: "wait_for_event() called in coordinator mode — not supported".to_string(),
            }),
        }
    }

    async fn wait_for_event_body_mode<T>(
        &mut self,
        event_name: &str,
        timeout: Option<std::time::Duration>,
    ) -> Result<T, StepError>
    where
        T: for<'de> Deserialize<'de>,
    {
        let lookup = self
            .scheduler
            .get_step_status(&self.run_id, event_name)
            .await
            .map_err(|e| StepError::Failed {
                step: event_name.to_string(),
                reason: e.to_string(),
            })?;

        match lookup {
            Some(StepLookup {
                status: TaskStatus::Completed,
                result: Some(json),
                ..
            }) => serde_json::from_value(json).map_err(|e| StepError::Failed {
                step: event_name.to_string(),
                reason: format!("failed to deserialize event result: {e}"),
            }),
            Some(StepLookup {
                status: TaskStatus::Completed,
                result: None,
                ..
            }) => Err(StepError::Failed {
                step: event_name.to_string(),
                reason: "event step completed but result is missing".to_string(),
            }),
            Some(_) => {
                // Step task exists but not yet completed (scheduled or picked_up).
                emit_metric!(
                    STEPS_TOTAL
                        .with_label_values(&["waiting_for_event", event_name])
                        .inc()
                );
                Err(StepError::Scheduled {
                    step: event_name.to_string(),
                    next_execution: None,
                })
            }
            None => {
                // First call: insert a wait_for_event step task row.
                emit_metric!(
                    STEPS_TOTAL
                        .with_label_values(&["waiting_for_event", event_name])
                        .inc()
                );
                let deadline = timeout.and_then(|d| {
                    chrono::Duration::from_std(d)
                        .ok()
                        .map(|cd| chrono::Utc::now() + cd)
                });
                let task_id = format!("{}:step:{}", self.run_id, event_name);
                step_ops::schedule_wait_for_event_task(
                    &*self.scheduler,
                    step_ops::EventStepSpec {
                        task_id: &task_id,
                        task_name: &self.task_name,
                        run_id: &self.run_id,
                        event_name,
                        data: self.data.clone(),
                        deadline,
                    },
                )
                .await
                .map_err(|e| StepError::Failed {
                    step: event_name.to_string(),
                    reason: e.to_string(),
                })?;
                Err(StepError::Scheduled {
                    step: event_name.to_string(),
                    next_execution: None,
                })
            }
        }
    }

    async fn wait_for_event_step_mode<T>(&self, event_name: &str) -> Result<T, StepError>
    where
        T: for<'de> Deserialize<'de>,
    {
        let lookup = self
            .scheduler
            .get_step_status(&self.run_id, event_name)
            .await
            .map_err(|e| StepError::Failed {
                step: event_name.to_string(),
                reason: e.to_string(),
            })?;

        match lookup {
            Some(StepLookup {
                status: TaskStatus::Completed,
                result: Some(json),
                ..
            }) => serde_json::from_value(json).map_err(|e| StepError::Failed {
                step: event_name.to_string(),
                reason: format!("failed to deserialize event result: {e}"),
            }),
            Some(StepLookup {
                status: TaskStatus::Completed,
                result: None,
                ..
            }) => Err(StepError::Failed {
                step: event_name.to_string(),
                reason: "event step completed but result is missing".to_string(),
            }),
            _ => {
                // Shouldn't happen in well-formed sequential flow.
                Err(StepError::Scheduled {
                    step: event_name.to_string(),
                    next_execution: None,
                })
            }
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
