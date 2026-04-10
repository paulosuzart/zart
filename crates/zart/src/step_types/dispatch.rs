//! Declarative step dispatch entry points.
//!
//! This module routes `TaskContext` step operations through `StepDefId` behavior
//! traits. The worker always runs the handler body (the "walk"); what changes is
//! the step behavior during the walk:
//!
//! - **Body mode**: encountering a step for the first time → schedule + park
//! - **Step mode**: replaying the body to a target step → cache preceding steps,
//!   then resolve the target
//!
//! No step-kind conditionals exist here beyond mode extraction and delegation.

use crate::context::{PendingFn, TaskContext};
use crate::error::{StepError, TaskError};
use crate::execution_model::ExecutionMode;
use crate::retry::RetryConfig;
use crate::step_ops;
use crate::step_types::{CompletionOutcome, CompletionSpec, StepDefId, StepRequest, StepResult};
use scheduler::TaskStatus;
use serde::{Deserialize, Serialize};

/// TaskContext step internal entry point for declarative step handling.
///
/// Routes body-mode and step-mode behavior through `StepDefId` traits:
/// - body mode: lookup/schedule via body behavior
/// - step mode: cache non-target steps, resolve target via step behavior + completion
pub async fn step_internal<T>(
    step_def_id: StepDefId,
    ctx: &TaskContext,
    req: StepRequest<'_>,
    lambda: Option<PendingFn>,
) -> Result<T, StepError>
where
    T: for<'de> serde::Deserialize<'de> + serde::Serialize,
{
    let step_name = req.step_name;

    match &ctx.execution_mode {
        ExecutionMode::Body => {
            let json = step_def_id.body_behavior().handle(ctx, &req).await?;
            serde_json::from_value(json).map_err(|e| StepError::Failed {
                step: step_name.to_string(),
                reason: format!("failed to deserialize cached result: {e}"),
            })
        }

        ExecutionMode::Step { target_step, .. } => {
            if step_def_id == StepDefId::WaitGroupBarrier {
                return Err(StepError::Failed {
                    step: step_name.to_string(),
                    reason: "wait-group barrier request reached step mode".to_string(),
                });
            }

            let target_step = target_step.clone();

            if step_name != target_step {
                let result = step_def_id
                    .step_behavior()
                    .handle(ctx, step_name, None)
                    .await?;
                let json = match result {
                    StepResult::Executed(v) | StepResult::Cached(v) => v,
                    StepResult::Transition => serde_json::Value::Null,
                };
                return serde_json::from_value(json).map_err(|e| StepError::Failed {
                    step: step_name.to_string(),
                    reason: format!("failed to deserialize cached result: {e}"),
                });
            }

            let immediate_outcome = step_def_id
                .step_behavior()
                .handle(ctx, step_name, lambda)
                .await;
            step_internal_target_step(step_def_id, ctx, step_name, immediate_outcome).await
        }
    }
}

/// Target-step completion path with retry orchestration.
///
/// Called when the walk reaches the target step in step mode.
/// Handles retry scheduling on failure and routes to completion behavior on success.
pub async fn step_internal_target_step<T>(
    step_def_id: StepDefId,
    ctx: &TaskContext,
    step_name: &str,
    immediate_outcome: Result<StepResult, StepError>,
) -> Result<T, StepError>
where
    T: for<'de> serde::Deserialize<'de> + serde::Serialize,
{
    let (retry_attempt, retry_config) = match &ctx.execution_mode {
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
            if let Some(next) = next_retry_time(err.to_string(), &retry_config, retry_attempt) {
                step_ops::reschedule_step_for_retry(
                    &*ctx.scheduler,
                    &ctx.task_id,
                    retry_attempt + 1,
                    &next.error,
                    next.when,
                    &ctx.lock_token,
                )
                .await
                .map_err(|e| StepError::Failed {
                    step: step_name.to_string(),
                    reason: format!("failed to schedule retry: {e}"),
                })?;

                return Err(StepError::StepExecuted {
                    step: step_name.to_string(),
                });
            }

            if step_def_id == StepDefId::WaitGroupChild {
                let wait_group_step_name = ctx
                    .data()
                    .get("wg_step_name")
                    .and_then(|v| v.as_str())
                    .map(str::to_string);

                if let Some(group_step_name) = wait_group_step_name {
                    let spec = CompletionSpec {
                        step_task_id: ctx.task_id.clone(),
                        step_id: ctx.task_id.clone(),
                        step_name: step_name.to_string(),
                        worker_id: ctx.lock_token.clone(),
                        task_name: ctx.task_name().to_string(),
                        run_id: ctx.run_id().to_string(),
                        execution_id: ctx.execution_id().to_string(),
                        data: ctx.data().clone(),
                        attempt_number: retry_attempt + 1,
                        result: StepResult::Transition,
                        wait_group_step_name: Some(group_step_name),
                        outcome: CompletionOutcome::Failure {
                            error: err.to_string(),
                        },
                    };

                    step_def_id
                        .completion_behavior(&spec.outcome)
                        .complete(&*ctx.scheduler, spec)
                        .await
                        .map_err(|e| StepError::Failed {
                            step: step_name.to_string(),
                            reason: e.to_string(),
                        })?;

                    return Err(StepError::StepExecuted {
                        step: step_name.to_string(),
                    });
                }
            }

            return Err(err);
        }
    };

    let wait_group_step_name = if step_def_id == StepDefId::WaitGroupChild {
        ctx.data()
            .get("wg_step_name")
            .and_then(|v| v.as_str())
            .map(str::to_string)
    } else {
        None
    };

    let spec = CompletionSpec {
        step_task_id: ctx.task_id.clone(),
        step_id: ctx.task_id.clone(),
        step_name: step_name.to_string(),
        worker_id: ctx.lock_token.clone(),
        task_name: ctx.task_name().to_string(),
        run_id: ctx.run_id().to_string(),
        execution_id: ctx.execution_id().to_string(),
        data: ctx.data().clone(),
        attempt_number: retry_attempt + 1,
        result: step_result,
        wait_group_step_name,
        outcome: CompletionOutcome::Success,
    };

    step_def_id
        .completion_behavior(&spec.outcome)
        .complete(&*ctx.scheduler, spec)
        .await
        .map_err(|e| StepError::Failed {
            step: step_name.to_string(),
            reason: e.to_string(),
        })?;

    Err(StepError::StepExecuted {
        step: step_name.to_string(),
    })
}

/// Optional helper to adapt `StepError` into `TaskError` at call sites that
/// currently operate at task-level error boundaries.
pub fn into_task_error(task_name: &str, source: StepError) -> TaskError {
    TaskError::StepFailed {
        step: task_name.to_string(),
        source,
    }
}

/// Capture a synchronous, pure value durably.
///
/// On first body run: evaluates `f()`, writes the result as a completed step row,
/// returns the value — body walk continues without parking.
/// On replay: returns the cached DB value; `f` is never called.
///
/// In step mode, capture must always be pre-completed — never a park target.
pub async fn capture_internal<T, F>(
    ctx: &TaskContext,
    step_name: &str,
    f: F,
) -> Result<T, StepError>
where
    T: Serialize + for<'de> Deserialize<'de>,
    F: FnOnce() -> T,
{
    let lookup = ctx
        .scheduler
        .get_step_status(ctx.run_id(), step_name)
        .await
        .map_err(|e| StepError::Failed {
            step: step_name.to_string(),
            reason: e.to_string(),
        })?;

    if let Some(scheduler::StepLookup {
        status: TaskStatus::Completed,
        result: Some(json),
        ..
    }) = lookup
    {
        return serde_json::from_value(json).map_err(|e| StepError::Failed {
            step: step_name.to_string(),
            reason: format!("deserialize capture result: {e}"),
        });
    }

    if matches!(ctx.execution_mode, ExecutionMode::Step { .. }) {
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

    ctx.scheduler
        .insert_completed_step(ctx.run_id(), step_name, "capture", json)
        .await
        .map_err(|e| StepError::Failed {
            step: step_name.to_string(),
            reason: e.to_string(),
        })?;

    Ok(value)
}

struct RetryPlan {
    when: chrono::DateTime<chrono::Utc>,
    error: String,
}

fn next_retry_time(
    error: String,
    retry_config: &Option<RetryConfig>,
    retry_attempt: usize,
) -> Option<RetryPlan> {
    let cfg = retry_config.clone()?;
    let delay = cfg.delay_for(retry_attempt + 1)?;
    let when =
        chrono::Utc::now() + chrono::Duration::from_std(delay).unwrap_or(chrono::Duration::zero());

    Some(RetryPlan { when, error })
}
