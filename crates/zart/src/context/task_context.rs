//! TaskContext — the primary interface for durable step execution.
//!
//! This module contains [`TaskContext`], which provides the step execution API
//! (`execute_step`, `schedule_step`, `wait_all`, `sleep_until`, `wait_for_event`)
//! and access to the initial payload and execution metadata.

use crate::error::StepError;
use crate::execution_model::ExecutionMode;
use crate::retry::RetryConfig;
use crate::step_ops;
use crate::step_types::ResultKind;
use crate::store::StorageBackend;
use crate::timeout::TimeoutScope;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::instrument;
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
    /// Lazily populated per-run step status cache loaded in a single batch query.
    step_cache: tokio::sync::OnceCell<
        tokio::sync::RwLock<std::collections::HashMap<String, zart_core::types::StepLookup>>,
    >,
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
            step_cache: tokio::sync::OnceCell::new(),
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

    // ── Step state cache ──────────────────────────────────────────────────────

    /// Look up a step's status from the per-run batch cache, loading all step
    /// statuses for `run_id` in a single storage query on first access.
    pub(crate) async fn get_cached_step_status(
        &self,
        step_name: &str,
    ) -> Result<Option<zart_core::types::StepLookup>, StepError> {
        let lock = self
            .step_cache
            .get_or_try_init(|| async {
                let map = self
                    .scheduler
                    .list_step_statuses(&self.run_id)
                    .await
                    .map_err(|e| StepError::Failed {
                        step: step_name.to_string(),
                        reason: e.to_string(),
                    })?;
                Ok::<_, StepError>(tokio::sync::RwLock::new(map))
            })
            .await?;

        let guard = lock.read().await;
        Ok(guard.get(step_name).cloned())
    }

    /// Record a newly completed step (e.g. from `capture`) in the in-memory cache
    /// so subsequent lookups within the same execution do not re-query storage.
    pub(crate) async fn record_completed_step_in_cache(
        &self,
        step_name: &str,
        result: serde_json::Value,
    ) {
        if let Some(lock) = self.step_cache.get() {
            let mut guard = lock.write().await;
            guard.insert(
                step_name.to_string(),
                zart_core::types::StepLookup {
                    task_id: format!("{}:step:{}", self.run_id, step_name),
                    status: zart_core::types::TaskStatus::Completed,
                    result: Some(result),
                    result_kind: Some(zart_core::types::StepResultKind::Ok),
                },
            );
        }
    }

    /// Look up a step in the per-run cache; if completed return `(value, kind)`,
    /// if in-flight return `Err(StepError::Scheduled)`, and if absent invoke
    /// `schedule_fn` to persist the step task and return `Err(StepError::Scheduled)`.
    async fn lookup_or_schedule<F, Fut>(
        &self,
        step_name: &str,
        metric_label: Option<&str>,
        schedule_fn: F,
    ) -> Result<(serde_json::Value, ResultKind), StepError>
    where
        F: FnOnce(String) -> Fut,
        Fut: std::future::Future<
                Output = Result<zart_core::types::ScheduleResult, zart_core::StorageError>,
            >,
    {
        use zart_core::types::{StepLookup, TaskStatus};

        let lookup = self.get_cached_step_status(step_name).await?;

        match lookup {
            Some(StepLookup {
                status: TaskStatus::Completed,
                result,
                result_kind,
                ..
            }) => {
                let kind = result_kind.map(ResultKind::from).unwrap_or(ResultKind::Ok);
                Ok((result.unwrap_or(serde_json::Value::Null), kind))
            }
            Some(_) => {
                #[allow(unused_variables)]
                if let Some(label) = metric_label {
                    crate::emit_metric!(
                        crate::metrics::STEPS_TOTAL
                            .with_label_values(&[label, step_name])
                            .inc()
                    );
                }
                Err(StepError::Scheduled {
                    step: step_name.to_string(),
                    next_execution: None,
                })
            }
            None => {
                #[allow(unused_variables)]
                if let Some(label) = metric_label {
                    crate::emit_metric!(
                        crate::metrics::STEPS_TOTAL
                            .with_label_values(&[label, step_name])
                            .inc()
                    );
                }
                let task_id = format!("{}:step:{}", self.run_id, step_name);
                schedule_fn(task_id).await.map_err(|e| StepError::Failed {
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

    /// Look up a completed `wait_for_event` step during `ExecutionMode::Step` replay.
    async fn lookup_completed_event(
        &self,
        event_name: &str,
    ) -> Result<serde_json::Value, StepError> {
        use zart_core::types::{StepLookup, TaskStatus};

        match self.get_cached_step_status(event_name).await? {
            Some(StepLookup {
                status: TaskStatus::Completed,
                result: Some(json),
                ..
            }) => Ok(json),
            Some(StepLookup {
                status: TaskStatus::Completed,
                result: None,
                ..
            }) => Err(StepError::Failed {
                step: event_name.to_string(),
                reason: "event step completed but result is missing".to_string(),
            }),
            _ => Err(StepError::DeadlineExceeded {
                step: event_name.to_string(),
            }),
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
        use crate::step_types::ResultKind as RK;

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
                let deadline = timeout_for_request.map(|d| {
                    chrono::Utc::now()
                        + chrono::Duration::from_std(d).unwrap_or(chrono::Duration::zero())
                });
                let retry_ref = retry_config.as_ref();
                let step_name_ref = step_name_owned.as_str();
                let (json, kind) = self
                    .lookup_or_schedule(step_name_ref, Some("scheduled"), |task_id| async move {
                        step_ops::schedule_step_task(
                            &*self.scheduler,
                            step_ops::StepTaskSpec {
                                task_id: &task_id,
                                run_id: self.run_id(),
                                execution_id: self.execution_id(),
                                step_name: step_name_ref,
                                data: self.data().clone(),
                                retry_config: retry_ref,
                                deadline,
                            },
                        )
                        .await
                    })
                    .await?;

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
                    let step_name_ref = step_name_owned.as_str();
                    let (json, _kind) = self
                        .lookup_or_schedule(
                            step_name_ref,
                            Some("scheduled"),
                            |task_id| async move {
                                step_ops::schedule_step_task(
                                    &*self.scheduler,
                                    step_ops::StepTaskSpec {
                                        task_id: &task_id,
                                        run_id: self.run_id(),
                                        execution_id: self.execution_id(),
                                        step_name: step_name_ref,
                                        data: self.data().clone(),
                                        retry_config: None,
                                        deadline: None,
                                    },
                                )
                                .await
                            },
                        )
                        .await?;
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
                        crate::trx_impl::store_step_completion_hint(
                            crate::trx_impl::StepCompletionHint::RegularStep {
                                result: json,
                                result_kind: outcome_kind,
                                attempt_number: retry_attempt + 1,
                            },
                        )
                        .await;
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
                        crate::trx_impl::store_step_completion_hint(
                            crate::trx_impl::StepCompletionHint::RegularStep {
                                result: json,
                                result_kind: RK::TimedOut,
                                attempt_number: retry_attempt + 1,
                            },
                        )
                        .await;
                        return Err(StepError::StepExecuted {
                            step: step_name_owned,
                        });
                    }

                    crate::trx_impl::store_step_completion_hint(
                        crate::trx_impl::StepCompletionHint::RegularStep {
                            result: json,
                            result_kind: kind,
                            attempt_number: retry_attempt + 1,
                        },
                    )
                    .await;
                    Err(StepError::StepExecuted {
                        step: step_name_owned,
                    })
                }
            }
        }
    }

    /// Complete a target step: handle retry scheduling on failure,
    /// route to completion hint on success.
    async fn complete_target_step(
        &self,
        is_wait_group_child: bool,
        step_name: &str,
        immediate_outcome: Result<serde_json::Value, StepError>,
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

        let result_json = match immediate_outcome {
            Ok(v) => v,
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
                if is_wait_group_child {
                    let wait_group_step_name = self
                        .data()
                        .get("wg_step_name")
                        .and_then(|v| v.as_str())
                        .map(str::to_string);

                    if let Some(group_step_name) = wait_group_step_name {
                        crate::trx_impl::store_step_completion_hint(
                            crate::trx_impl::StepCompletionHint::WaitGroupChildFailure {
                                group_step_name: group_step_name.clone(),
                                error: err.to_string(),
                                attempt_number: retry_attempt + 1,
                            },
                        )
                        .await;

                        return Err(StepError::StepExecuted {
                            step: step_name.to_string(),
                        });
                    }
                }

                return Err(err);
            }
        };

        // Handle wait-group child success: store hint and return StepExecuted.
        if is_wait_group_child {
            let wait_group_step_name = self
                .data()
                .get("wg_step_name")
                .and_then(|v| v.as_str())
                .map(str::to_string);

            if let Some(group_step_name) = wait_group_step_name {
                crate::trx_impl::store_step_completion_hint(
                    crate::trx_impl::StepCompletionHint::WaitGroupChild {
                        group_step_name: group_step_name.clone(),
                        result: result_json,
                        result_kind: ResultKind::Ok,
                        attempt_number: retry_attempt + 1,
                    },
                )
                .await;

                return Err(StepError::StepExecuted {
                    step: step_name.to_string(),
                });
            }
        }

        // Regular step (or sleep/wait-for-event): store hint, no storage writes.
        crate::trx_impl::store_step_completion_hint(
            crate::trx_impl::StepCompletionHint::RegularStep {
                result: result_json,
                result_kind: ResultKind::Ok,
                attempt_number: retry_attempt + 1,
            },
        )
        .await;

        Err(StepError::StepExecuted {
            step: step_name.to_string(),
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

    async fn resolve_wait_group_barrier(
        &self,
        group_step_name: &str,
        child_names: &[String],
        threshold: i32,
    ) -> Result<Vec<serde_json::Value>, StepError> {
        use zart_core::types::{StepLookup, TaskStatus, UpsertWaitGroupStepParams};

        let total = i32::try_from(child_names.len()).map_err(|_| StepError::Failed {
            step: group_step_name.to_string(),
            reason: "too many wait-group handles to fit i32".to_string(),
        })?;

        // Upsert the wait-group row BEFORE scheduling any child tasks so that
        // `complete_wait_group_child` always finds a row to decrement.
        self.scheduler
            .upsert_wait_group_step(UpsertWaitGroupStepParams {
                run_id: self.run_id().to_string(),
                group_step_name: group_step_name.to_string(),
                total,
                threshold,
            })
            .await
            .map_err(|e| StepError::Failed {
                step: group_step_name.to_string(),
                reason: e.to_string(),
            })?;

        let mut all_completed = true;
        for child_name in child_names {
            match self.get_cached_step_status(child_name).await? {
                Some(StepLookup {
                    status: TaskStatus::Completed,
                    ..
                }) => {}
                Some(_) => {
                    all_completed = false;
                }
                None => {
                    all_completed = false;
                    let child_task_id = format!("{}:step:{}", self.run_id(), child_name);
                    step_ops::schedule_wait_group_child_task(
                        &*self.scheduler,
                        step_ops::WaitGroupChildSpec {
                            task_id: &child_task_id,
                            run_id: self.run_id(),
                            execution_id: self.execution_id(),
                            step_name: child_name,
                            wait_group_step_name: group_step_name,
                            data: self.data().clone(),
                        },
                    )
                    .await
                    .map_err(|e| StepError::Failed {
                        step: child_name.clone(),
                        reason: e.to_string(),
                    })?;
                }
            }
        }

        if !all_completed {
            return Err(StepError::Scheduled {
                step: group_step_name.to_string(),
                next_execution: None,
            });
        }

        let mut results = Vec::with_capacity(child_names.len());
        for child_name in child_names {
            let val = match self.get_cached_step_status(child_name).await? {
                Some(StepLookup {
                    status: TaskStatus::Completed,
                    result: Some(json),
                    ..
                }) => json,
                _ => serde_json::Value::Null,
            };
            results.push(val);
        }
        Ok(results)
    }

    /// Wait for all handles returned by [`schedule_step`](Self::schedule_step) to complete.
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
                use std::collections::hash_map::DefaultHasher;
                use std::hash::{Hash, Hasher};
                let mut sorted_names = step_names.clone();
                sorted_names.sort();
                let names_joined = sorted_names.join("|");
                let mut hasher = DefaultHasher::new();
                names_joined.hash(&mut hasher);
                let hash_value = hasher.finish();
                let group_step_name = format!("__wg__all__{:x}", hash_value);

                let values = self
                    .resolve_wait_group_barrier(&group_step_name, &step_names, 0)
                    .await?;

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
                        let val = serde_json::from_value(value).map_err(|e| StepError::Failed {
                            step: step_name,
                            reason: format!("deserialize error: {e}"),
                        })?;
                        results.push(Ok(val));
                    }
                }
                Ok(results)
            }
            ExecutionMode::Step { target_step, .. } => {
                let target = target_step.clone();

                for handle in handles {
                    if handle.step_name == target {
                        if let Some(pending_fn) = handle.pending {
                            let immediate_outcome = crate::local::ZART_PHASE
                                .scope(crate::local::Phase::Step(self.step_context()), pending_fn())
                                .await;
                            self.complete_target_step(true, &target, immediate_outcome)
                                .await?;
                            return Err(StepError::StepExecuted { step: target });
                        }

                        // No pending fn (already completed target child): treat as handled.
                        return Err(StepError::StepExecuted { step: target });
                    }
                }

                Err(StepError::Failed {
                    step: target,
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
        match &self.execution_mode {
            ExecutionMode::Body => {
                self.lookup_or_schedule(step_name, None, |task_id| async move {
                    step_ops::schedule_sleep_task(
                        &*self.scheduler,
                        &task_id,
                        self.run_id(),
                        self.execution_id(),
                        step_name,
                        wake_time,
                        self.data().clone(),
                    )
                    .await
                })
                .await?;
                Ok(())
            }
            ExecutionMode::Step { target_step, .. } => {
                if step_name != target_step {
                    return Ok(());
                }
                self.complete_target_step(false, step_name, Ok(serde_json::Value::Null))
                    .await
            }
        }
    }

    /// Wait for an external event to be delivered to this execution.
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

        match &self.execution_mode {
            ExecutionMode::Body => {
                let (json, _kind) = self
                    .lookup_or_schedule(
                        event_name,
                        Some("waiting_for_event"),
                        |task_id| async move {
                            step_ops::schedule_wait_for_event_task(
                                &*self.scheduler,
                                step_ops::EventStepSpec {
                                    task_id: &task_id,
                                    run_id: self.run_id(),
                                    execution_id: self.execution_id(),
                                    event_name,
                                    data: self.data().clone(),
                                    deadline,
                                },
                            )
                            .await
                        },
                    )
                    .await?;
                serde_json::from_value(json).map_err(|e| StepError::Failed {
                    step: event_name.to_string(),
                    reason: format!("failed to deserialize event result: {e}"),
                })
            }
            ExecutionMode::Step { target_step, .. } => {
                if event_name != target_step {
                    let json = self.lookup_completed_event(event_name).await?;
                    return serde_json::from_value(json).map_err(|e| StepError::Failed {
                        step: event_name.to_string(),
                        reason: format!("failed to deserialize event result: {e}"),
                    });
                }

                let immediate_outcome = self.lookup_completed_event(event_name).await;
                self.complete_target_step(false, event_name, immediate_outcome)
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

        let lookup = self.get_cached_step_status(step_name).await?;

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
            .insert_completed_step(self.run_id(), step_name, StepKind::Capture, json.clone())
            .await
            .map_err(|e| StepError::Failed {
                step: step_name.to_string(),
                reason: e.to_string(),
            })?;

        self.record_completed_step_in_cache(step_name, json).await;

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
