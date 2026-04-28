//! TaskContext — the primary interface for durable step execution.
//!
//! This module contains [`TaskContext`], which provides the step execution API
//! (`execute_step`, `schedule_step`, `wait_all`, `sleep_until`, `wait_for_event`)
//! and access to the initial payload and execution metadata.

use crate::error::StepError;
use crate::execution_model::ExecutionMode;
use crate::retry::RetryConfig;
use crate::step_ops;
use crate::step_types::{StepDefId, StepRequest, StepResult};
use crate::store::StorageBackend;
use crate::timeout::TimeoutScope;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::instrument;
use zart_core::types::StepResultKind;
use zart_scheduler::TaskScheduler;

use super::state::{PendingFn, StepHandle};
use super::step_context::StepContext;
use super::step_trait::ZartStep;

// ── TaskContext ───────────────────────────────────────────────────────────────

/// Provides the step execution API (`execute_step`, `schedule_step`, `wait_all`, `sleep`, etc.)
/// and access to the initial payload and execution metadata.
///
/// Users typically do not interact with this type directly — use the `zart::*` free functions
/// instead. `TaskContext` is a framework-internal type that implements the scheduling logic.
pub struct TaskContext {
    /// The underlying scheduler (used for step store operations).
    pub(crate) scheduler: Arc<dyn StorageBackend>,
    /// Task-queue scheduler (used for task lifecycle operations).
    #[allow(dead_code)]
    pub(crate) task_scheduler: Arc<dyn TaskScheduler>,
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
    pub(crate) data: serde_json::Value,
    /// How this task should behave when executing steps.
    /// `Body`/`Step` use per-row tasks.
    pub(crate) execution_mode: ExecutionMode,
    /// The deadline for this step task (parsed from task metadata).
    /// Set when timeout_scope == Global. Absent for PerAttempt or no timeout.
    pub(crate) step_deadline: Option<chrono::DateTime<chrono::Utc>>,
    /// The deadline for the enclosing execution (parsed from task metadata or execution row).
    /// Set when the durable handler has a timeout. Used to cap step timeouts.
    pub(crate) execution_deadline: Option<chrono::DateTime<chrono::Utc>>,
}

impl TaskContext {
    /// Construct a new `TaskContext`.
    ///
    /// Called by the worker when it picks up a task.
    pub fn new(
        scheduler: Arc<dyn StorageBackend>,
        task_scheduler: Arc<dyn TaskScheduler>,
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
            task_scheduler,
            task_id,
            execution_id,
            run_id,
            task_name: task_name.into(),
            lock_token: lock_token.into(),
            data,
            execution_mode: ExecutionMode::Body,
            step_deadline: None,
            execution_deadline: None,
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

    /// Set the step deadline (parsed from task metadata).
    pub fn with_step_deadline(mut self, deadline: Option<chrono::DateTime<chrono::Utc>>) -> Self {
        self.step_deadline = deadline;
        self
    }

    /// Set the execution deadline (parsed from task metadata or execution row).
    pub fn with_execution_deadline(
        mut self,
        deadline: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Self {
        self.execution_deadline = deadline;
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
            current_attempt,
            max_retries,
        }
    }

    // ── Internal step execution helpers (used by execute_step) ────────────────

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
    /// 4. Calls `step.run()` to execute the step logic
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Via zart::step free function (preferred):
    /// let result = zart::step(my_step()).await?;
    ///
    /// // Or directly on TaskContext (internal):
    /// let result = ctx.execute_step(my_step()).await?;
    /// ```
    #[instrument(name = "step.execute_typed", skip(self, step), fields(step_name = tracing::field::Empty))]
    pub async fn execute_step<S: ZartStep + Send>(
        &self,
        step: S,
    ) -> Result<crate::error::StepOutcome<S::Output, S::Error>, StepError> {
        use crate::error::{StepOutcome, ZartStepError};
        use crate::step_ops;
        use crate::step_types::{ResultKind as RK, StepDefId};

        let step_name = step.step_name();
        let timeout_duration = step.timeout();
        let step_name_owned = step_name.to_string();

        // In step mode, retry config comes from the execution mode (set by the worker).
        // In body mode, retry config comes from the step itself (embedded in metadata).
        let retry_config = match &self.execution_mode {
            ExecutionMode::Step { retry_config, .. } => retry_config.clone(),
            _ => step.retry_config(),
        };

        // Capture timeout_scope before the closure moves `step`.
        let timeout_scope = step.timeout_scope();

        // Set the span field
        tracing::Span::current().record("step_name", step_name.as_ref());

        // Build the step logic that serializes both T and E.
        let step_name_for_closure = step_name_owned.clone();
        let run_with_serialization = move || {
            let this = step;
            async move {
                let result: Result<(serde_json::Value, RK), StepError> = match this.run().await {
                    Ok(v) => {
                        let json = serde_json::to_value(&v).map_err(|e| StepError::Failed {
                            step: step_name_for_closure.clone(),
                            reason: format!("failed to serialize step output: {e}"),
                        })?;
                        Ok((json, RK::Ok))
                    }
                    Err(e) => {
                        let json = serde_json::to_value(&e).map_err(|_| StepError::Failed {
                            step: step_name_for_closure.clone(),
                            reason: format!(
                                "failed to serialize step error (does {:?} impl Serialize?)",
                                std::any::type_name::<S::Error>()
                            ),
                        })?;
                        Ok((json, RK::Err))
                    }
                };
                result
            }
        };

        match &self.execution_mode {
            ExecutionMode::Body => {
                // Body mode: look up via body behavior, deserialize based on ResultKind.
                // For Global scope, pass the timeout so body behavior computes a deadline.
                // For PerAttempt scope, timeout is applied at execution time (no deadline in metadata).
                let timeout_for_request =
                    timeout_duration.filter(|_| matches!(timeout_scope, TimeoutScope::Global));
                let req = crate::step_types::StepRequest::new_step(
                    &step_name_owned,
                    retry_config.as_ref(),
                    timeout_for_request,
                );
                let (json, kind) = StepDefId::Step.body_behavior().handle(self, &req).await?;

                match kind {
                    RK::Ok => {
                        let v = serde_json::from_value(json).map_err(|e| StepError::Failed {
                            step: step_name_owned,
                            reason: format!("failed to deserialize step output: {e}"),
                        })?;
                        Ok(StepOutcome::Ok(v))
                    }
                    RK::Err => {
                        let e: S::Error =
                            serde_json::from_value(json).map_err(|e| StepError::Failed {
                                step: step_name_owned,
                                reason: format!("failed to deserialize step error: {e}"),
                            })?;
                        Ok(StepOutcome::BusinessErr(e))
                    }
                    RK::RetryExhausted => {
                        let _e: S::Error = serde_json::from_value(json.clone()).map_err(|e| {
                            StepError::Failed {
                                step: step_name_owned.clone(),
                                reason: format!("failed to deserialize step error: {e}"),
                            }
                        })?;
                        Ok(StepOutcome::ZartErr(ZartStepError::RetryExhausted {
                            step: step_name_owned,
                            attempts: 0,
                            last_error: json,
                        }))
                    }
                    RK::TimedOut => Ok(StepOutcome::ZartErr(ZartStepError::TimedOut {
                        step: step_name_owned,
                        duration: timeout_duration.unwrap_or_default(),
                    })),
                    RK::DeadlineExceeded => {
                        Ok(StepOutcome::ZartErr(ZartStepError::DeadlineExceeded {
                            step: step_name_owned,
                        }))
                    }
                }
            }
            ExecutionMode::Step { target_step, .. } => {
                let target = target_step.clone();

                if step_name.as_ref() != target {
                    // Non-target step: just look it up (should be completed).
                    let req =
                        crate::step_types::StepRequest::new_step(&step_name_owned, None, None);
                    let (json, _kind) = StepDefId::Step.body_behavior().handle(self, &req).await?;
                    // Non-target steps in step mode should always be Ok.
                    let v = serde_json::from_value(json).map_err(|e| StepError::Failed {
                        step: step_name_owned,
                        reason: format!("failed to deserialize cached step output: {e}"),
                    })?;
                    return Ok(StepOutcome::Ok(v));
                }

                // Target step: run the lambda and handle completion.
                // Compute effective timeout based on scope and deadlines.
                let effective_timeout =
                    |ctx: &TaskContext, timeout_dur: std::time::Duration, scope: TimeoutScope| {
                        match scope {
                            TimeoutScope::Global => {
                                if let Some(deadline) = ctx.step_deadline {
                                    let now = chrono::Utc::now();
                                    if now >= deadline {
                                        return None;
                                    }
                                    let remaining = deadline - now;
                                    let capped = if let Some(exec_deadline) = ctx.execution_deadline
                                    {
                                        if exec_deadline < deadline {
                                            (exec_deadline - now).to_std().ok()
                                        } else {
                                            remaining.to_std().ok()
                                        }
                                    } else {
                                        remaining.to_std().ok()
                                    };
                                    capped.filter(|d| !d.is_zero())
                                } else {
                                    Some(timeout_dur)
                                }
                            }
                            TimeoutScope::PerAttempt => Some(timeout_dur),
                        }
                    };

                // Wrap run_with_serialization so the step body executes in Phase::Step.
                // This allows `zart::trx()` to detect it is inside a step invocation.
                let step_ctx_for_phase = self.step_context();
                let run_with_serialization = move || {
                    crate::local::ZART_PHASE.scope(
                        crate::local::Phase::Step(step_ctx_for_phase),
                        run_with_serialization(),
                    )
                };

                // Step execution with timeout handling.
                // The STEP_TRX task-local is now set up by ZartTask::execute()
                // so that `zart::trx()` can register a transaction.
                {
                    let (json, kind) = if let Some(timeout_dur) = timeout_duration {
                        if matches!(timeout_scope, TimeoutScope::Global) {
                            if let Some(deadline) = self.step_deadline {
                                if chrono::Utc::now() >= deadline {
                                    let dummy = serde_json::Value::String(
                                        "step deadline reached".to_string(),
                                    );
                                    (dummy, RK::TimedOut)
                                } else {
                                    let remaining =
                                        effective_timeout(self, timeout_dur, timeout_scope);
                                    if let Some(dur) = remaining {
                                        match tokio::time::timeout(dur, run_with_serialization())
                                            .await
                                        {
                                            Ok(result) => result?,
                                            Err(_) => {
                                                let dummy = serde_json::Value::String(format!(
                                                    "step timed out after {:?}",
                                                    dur
                                                ));
                                                (dummy, RK::TimedOut)
                                            }
                                        }
                                    } else {
                                        run_with_serialization().await?
                                    }
                                }
                            } else {
                                match tokio::time::timeout(timeout_dur, run_with_serialization())
                                    .await
                                {
                                    Ok(result) => result?,
                                    Err(_) => {
                                        let dummy = serde_json::Value::String(format!(
                                            "step timed out after {:?}",
                                            timeout_dur
                                        ));
                                        (dummy, RK::TimedOut)
                                    }
                                }
                            }
                        } else {
                            match tokio::time::timeout(timeout_dur, run_with_serialization()).await
                            {
                                Ok(result) => result?,
                                Err(_) => {
                                    let dummy = serde_json::Value::String(format!(
                                        "step timed out after {:?}",
                                        timeout_dur
                                    ));
                                    (dummy, RK::TimedOut)
                                }
                            }
                        }
                    } else {
                        run_with_serialization().await?
                    };

                    let (retry_attempt, _) = match &self.execution_mode {
                        ExecutionMode::Step {
                            retry_attempt,
                            retry_config,
                            ..
                        } => (*retry_attempt, retry_config.clone()),
                        _ => (0, None),
                    };

                    if matches!(kind, RK::Err) {
                        crate::trx_impl::rollback_trx()
                            .await
                            .map_err(|e| StepError::Failed {
                                step: step_name_owned.clone(),
                                reason: format!("failed to rollback step transaction: {e}"),
                            })?;

                        if let Some(next) = next_retry_time(&retry_config, retry_attempt) {
                            step_ops::reschedule_step_for_retry(
                                &*self.scheduler,
                                &self.task_id,
                                retry_attempt + 1,
                                "business error",
                                next,
                                &self.lock_token,
                            )
                            .await
                            .map_err(|e| StepError::Failed {
                                step: step_name_owned.clone(),
                                reason: format!("failed to schedule retry: {e}"),
                            })?;
                            return Err(StepError::StepExecuted {
                                step: step_name_owned,
                            });
                        }

                        let outcome_kind =
                            if retry_config.as_ref().is_some_and(|rc| rc.max_attempts > 0) {
                                RK::RetryExhausted
                            } else {
                                RK::Err
                            };
                        self.complete_step_and_schedule_body(json, outcome_kind, retry_attempt + 1)
                            .await?;
                        return Err(StepError::StepExecuted {
                            step: step_name_owned,
                        });
                    }

                    if matches!(kind, RK::TimedOut) {
                        crate::trx_impl::rollback_trx()
                            .await
                            .map_err(|e| StepError::Failed {
                                step: step_name_owned.clone(),
                                reason: format!("failed to rollback step transaction: {e}"),
                            })?;
                        self.complete_step_and_schedule_body(json, RK::TimedOut, retry_attempt + 1)
                            .await?;
                        return Err(StepError::StepExecuted {
                            step: step_name_owned,
                        });
                    }

                    self.complete_step_and_schedule_body(json, kind, retry_attempt + 1)
                        .await?;
                    Err(StepError::StepExecuted {
                        step: step_name_owned,
                    })
                }
            }
        }
    }

    /// Complete a target step: handle retry scheduling on failure,
    /// route to completion behavior on success.
    async fn complete_target_step(
        &self,
        step_def_id: StepDefId,
        step_name: &str,
        immediate_outcome: Result<StepResult, StepError>,
    ) -> Result<(), StepError> {
        let (retry_attempt, retry_config) = match &self.execution_mode {
            ExecutionMode::Step {
                retry_attempt,
                retry_config,
                ..
            } => (*retry_attempt, retry_config.clone()),
            ExecutionMode::Body => {
                return Err(StepError::Failed {
                    step: step_name.to_string(),
                    reason: "target-step path reached in body mode".to_string(),
                });
            }
        };

        let step_result = match immediate_outcome {
            Ok(r) => r,
            Err(err) => {
                if let Some(next) =
                    next_retry_time_with_error(err.to_string(), &retry_config, retry_attempt)
                {
                    step_ops::reschedule_step_for_retry(
                        &*self.scheduler,
                        &self.task_id,
                        retry_attempt + 1,
                        &next.error,
                        next.when,
                        &self.lock_token,
                    )
                    .await
                    .map_err(|e| StepError::Failed {
                        step: step_name.to_string(),
                        reason: format!("failed to schedule retry: {e}"),
                    })?;

                    return Ok(());
                }

                // Handle wait-group child failure: store hint and return StepExecuted.
                if step_def_id == StepDefId::WaitGroupChild {
                    let wait_group_step_name = self
                        .data()
                        .get("wg_step_name")
                        .and_then(|v| v.as_str())
                        .map(str::to_string);

                    if let Some(group_step_name) = wait_group_step_name {
                        // Store the failure hint for ZartTask to pick up.
                        crate::trx_impl::store_step_completion_hint(
                            crate::trx_impl::StepCompletionHint::WaitGroupChildFailure {
                                group_step_name: group_step_name.clone(),
                                error: err.to_string(),
                            },
                        )
                        .await;

                        // Write step SQL (failure) and park tx in STEP_TRX.
                        self.complete_step_and_schedule_body(
                            serde_json::json!({ "error": err.to_string() }),
                            crate::step_types::ResultKind::Err,
                            retry_attempt + 1,
                        )
                        .await?;

                        return Err(StepError::StepExecuted {
                            step: step_name.to_string(),
                        });
                    }
                }

                return Err(err);
            }
        };

        // Handle wait-group child success: store hint and return StepExecuted.
        if step_def_id == StepDefId::WaitGroupChild {
            let wait_group_step_name = self
                .data()
                .get("wg_step_name")
                .and_then(|v| v.as_str())
                .map(str::to_string);

            if let Some(group_step_name) = wait_group_step_name {
                // Store the success hint for ZartTask to pick up.
                crate::trx_impl::store_step_completion_hint(
                    crate::trx_impl::StepCompletionHint::WaitGroupChild {
                        group_step_name: group_step_name.clone(),
                    },
                )
                .await;

                // Write step SQL and park tx in STEP_TRX.
                self.complete_step_and_schedule_body(
                    serde_json::json!(null),
                    crate::step_types::ResultKind::Ok,
                    retry_attempt + 1,
                )
                .await?;

                return Err(StepError::StepExecuted {
                    step: step_name.to_string(),
                });
            }
        }

        // Regular step: write step SQL and park tx in STEP_TRX.
        let result_json = match step_result {
            StepResult::Executed(v) | StepResult::Cached(v) => v,
            StepResult::Transition => serde_json::Value::Null,
        };

        self.complete_step_and_schedule_body(
            result_json,
            crate::step_types::ResultKind::Ok,
            retry_attempt + 1,
        )
        .await?;

        Err(StepError::StepExecuted {
            step: step_name.to_string(),
        })
    }

    /// Write step SQL and park the transaction in STEP_TRX for ZartTask to retrieve.
    ///
    /// Uses the user's transaction from STEP_TRX if `zart::trx()` was called;
    /// otherwise the storage opens a fresh one. Does not commit — the commit
    /// happens atomically with scheduler bookkeeping inside ZartStepCompletion.
    async fn complete_step_and_schedule_body(
        &self,
        result: serde_json::Value,
        kind: crate::step_types::ResultKind,
        attempt_number: usize,
    ) -> Result<(), StepError> {
        use zart_core::types::CompleteStepAndScheduleBodyParams;

        let step_task_id = self.task_id.clone();
        let step_id = self.task_id.clone();
        let step_name = self
            .task_id
            .strip_prefix(&format!("{}:step:", self.run_id()))
            .unwrap_or(&self.task_id)
            .to_string();
        let next_body_task_id = format!("{}:body:after:{}", self.run_id(), step_name);

        let spec = CompleteStepAndScheduleBodyParams {
            run_id: self.run_id().to_string(),
            execution_id: self.execution_id().to_string(),
            step_task_id,
            step_id,
            result,
            result_kind: StepResultKind::from(kind),
            lock_token: self.lock_token.clone(),
            attempt_number,
            next_body_task_id,
            data: self.data().clone(),
        };

        // The storage impl takes the user's tx from STEP_TRX (or opens a fresh one),
        // writes step SQL, and parks the tx back in STEP_TRX for ZartStepCompletion.
        self.scheduler
            .complete_step_and_schedule_body(spec)
            .await
            .map_err(|e| StepError::Failed {
                step: self.task_id.clone(),
                reason: e.to_string(),
            })
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
    pub fn schedule_step<S: ZartStep + Send + 'static>(&self, step: S) -> StepHandle<S::Output> {
        let step_name = step.step_name();
        let step_name_str = step_name.to_string();

        // schedule_step just returns a handle with the lambda.
        // Actual scheduling (DB insert) happens in wait_all.
        // In step mode, only the target step handle carries the lambda.
        let is_target = matches!(&self.execution_mode,
            ExecutionMode::Step { target_step, .. } if target_step.as_str() == step_name.as_ref());

        let pending: Option<PendingFn> = if !matches!(
            &self.execution_mode,
            ExecutionMode::Step { .. }
        ) || is_target
        {
            let name_for_err = step_name_str.clone();
            Some(Box::new(move || {
                let step = step;
                Box::pin(async move {
                    match step.run().await {
                        Ok(result) => serde_json::to_value(result).map_err(|e| StepError::Failed {
                            step: name_for_err.clone(),
                            reason: format!("serialize error: {e}"),
                        }),
                        Err(e) => {
                            // Serialize the business error for storage; the framework
                            // handles retry/completion based on this outcome.
                            let err_json = serde_json::to_value(&e).map_err(|_| StepError::Failed {
                                    step: name_for_err.clone(),
                                    reason: format!(
                                        "failed to serialize step error (does {:?} impl Serialize?)",
                                        std::any::type_name::<S::Error>()
                                    ),
                                })?;
                            // Return a placeholder — the actual error kind is tracked
                            // by the step completion path.
                            Ok(err_json)
                        }
                    }
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
        &self,
        handles: Vec<StepHandle<T>>,
    ) -> Result<Vec<Result<T, StepError>>, StepError>
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        match &self.execution_mode {
            ExecutionMode::Body => {
                // Extract names before awaits; avoid borrowing non-Sync handles across await points.
                let step_names: Vec<String> = handles.iter().map(|h| h.step_name.clone()).collect();

                // Generate a deterministic group step name based on the sorted child step names.
                // This ensures the same wait_group is used across body task replays, preventing
                // orphaned children and lost completion counts.
                use std::collections::hash_map::DefaultHasher;
                use std::hash::{Hash, Hasher};
                let mut sorted_names = step_names.clone();
                sorted_names.sort();
                let names_joined = sorted_names.join("|");
                let mut hasher = DefaultHasher::new();
                names_joined.hash(&mut hasher);
                let hash_value = hasher.finish();
                let group_step_name = format!("__wg__all__{:x}", hash_value);

                let req = StepRequest::new_wait_group_barrier(&group_step_name, &step_names, 0);

                let (json, _kind) = StepDefId::WaitGroupBarrier
                    .body_behavior()
                    .handle(self, &req)
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
                            let immediate_outcome = StepDefId::WaitGroupChild
                                .step_behavior()
                                .handle(self, &target, Some(pending_fn))
                                .await;
                            self.complete_target_step(
                                StepDefId::WaitGroupChild,
                                &target,
                                immediate_outcome,
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
    pub async fn sleep(
        &self,
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
        &self,
        step_name: &str,
        wake_time: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), StepError> {
        let req = StepRequest::new_sleep(step_name, wake_time);

        match &self.execution_mode {
            ExecutionMode::Body => {
                StepDefId::Sleep.body_behavior().handle(self, &req).await?;
                Ok(())
            }
            ExecutionMode::Step { target_step, .. } => {
                if step_name != target_step {
                    StepDefId::Sleep
                        .step_behavior()
                        .handle(self, step_name, None)
                        .await?;
                    return Ok(());
                }
                let outcome = StepDefId::Sleep
                    .step_behavior()
                    .handle(self, step_name, None)
                    .await;
                self.complete_target_step(StepDefId::Sleep, step_name, outcome)
                    .await
            }
        }
    }

    /// Wait for an external event to be delivered to this execution.
    ///
    /// This method captures deadline intent and delegates orchestration to
    /// declarative dispatch/behaviors for both body and step replay modes.
    pub async fn wait_for_event<T>(
        &self,
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

        match &self.execution_mode {
            ExecutionMode::Body => {
                let (json, _kind) = StepDefId::WaitForEvent
                    .body_behavior()
                    .handle(self, &req)
                    .await?;
                serde_json::from_value(json).map_err(|e| StepError::Failed {
                    step: event_name.to_string(),
                    reason: format!("failed to deserialize event result: {e}"),
                })
            }
            ExecutionMode::Step { target_step, .. } => {
                if event_name != target_step {
                    let result = StepDefId::WaitForEvent
                        .step_behavior()
                        .handle(self, event_name, None)
                        .await?;
                    let json = match result {
                        StepResult::Executed(v) | StepResult::Cached(v) => v,
                        StepResult::Transition => serde_json::Value::Null,
                    };
                    return serde_json::from_value(json).map_err(|e| StepError::Failed {
                        step: event_name.to_string(),
                        reason: format!("failed to deserialize event result: {e}"),
                    });
                }

                let immediate_outcome = StepDefId::WaitForEvent
                    .step_behavior()
                    .handle(self, event_name, None)
                    .await;
                self.complete_target_step(StepDefId::WaitForEvent, event_name, immediate_outcome)
                    .await?;
                Err(StepError::StepExecuted {
                    step: event_name.to_string(),
                })
            }
        }
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
    pub async fn capture<T, F>(&self, step_name: &str, f: F) -> Result<T, StepError>
    where
        T: Serialize + for<'de> Deserialize<'de>,
        F: FnOnce() -> T,
    {
        use zart_core::types::StepKind;

        let lookup = self
            .scheduler
            .get_step_status(self.run_id(), step_name)
            .await
            .map_err(|e| StepError::Failed {
                step: step_name.to_string(),
                reason: e.to_string(),
            })?;

        if let Some(zart_core::types::StepLookup {
            status: zart_core::types::TaskStatus::Completed,
            result: Some(json),
            ..
        }) = lookup
        {
            return serde_json::from_value(json).map_err(|e| StepError::Failed {
                step: step_name.to_string(),
                reason: format!("deserialize capture result: {e}"),
            });
        }

        if matches!(self.execution_mode, ExecutionMode::Step { .. }) {
            return Err(StepError::Failed {
                step: step_name.to_string(),
                reason: format!(
                    "capture step '{step_name}' not found during step-mode replay — \
                     the step ID may have changed or the step was added after the execution started"
                ),
            });
        }

        let value = f();
        let json = serde_json::to_value(&value).map_err(|e| StepError::Failed {
            step: step_name.to_string(),
            reason: format!("serialize capture result: {e}"),
        })?;

        self.scheduler
            .insert_completed_step(self.run_id(), step_name, StepKind::Capture, json)
            .await
            .map_err(|e| StepError::Failed {
                step: step_name.to_string(),
                reason: e.to_string(),
            })?;

        Ok(value)
    }

    /// Capture the current UTC time durably.
    ///
    /// Shorthand for `ctx.capture(step_name, chrono::Utc::now)`.
    pub async fn now(&self, step_name: &str) -> Result<chrono::DateTime<chrono::Utc>, StepError> {
        self.capture(step_name, chrono::Utc::now).await
    }

    /// Returns the original JSON payload provided when the execution was started.
    pub fn data(&self) -> &serde_json::Value {
        &self.data
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

    /// Returns the current retry attempt number (0-indexed) for this step.
    ///
    /// Returns `0` if this is the first attempt or if no retry is configured.
    /// Returns `1` for the first retry, `2` for the second retry, etc.
    ///
    /// Users should prefer `zart::context().current_attempt` instead.
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

struct RetryPlan {
    when: chrono::DateTime<chrono::Utc>,
    error: String,
}

/// Compute the next retry time for a step, if retries remain.
fn next_retry_time(
    retry_config: &Option<RetryConfig>,
    retry_attempt: usize,
) -> Option<chrono::DateTime<chrono::Utc>> {
    let cfg = retry_config.as_ref()?;
    let delay = cfg.delay_for(retry_attempt + 1)?;
    Some(chrono::Utc::now() + chrono::Duration::from_std(delay).unwrap_or(chrono::Duration::zero()))
}

fn next_retry_time_with_error(
    error: String,
    retry_config: &Option<RetryConfig>,
    retry_attempt: usize,
) -> Option<RetryPlan> {
    let when = next_retry_time(retry_config, retry_attempt)?;
    Some(RetryPlan { when, error })
}

// ── Unit tests ────────────────────────────────────────────────────────────────
