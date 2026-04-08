//! Declarative step dispatch entry points (v3).
//!
//! This module routes worker/task execution through `StepDefId` behavior traits
//! while preserving current runtime semantics for phased cutover.

use std::sync::Arc;
use std::time::Duration;

use scheduler::{FetchedTask, StorageBackend};

use crate::context::{PendingFn, TaskContext};
use crate::error::{StepError, TaskError};
use crate::execution_model::ExecutionMode;
use crate::registry::TaskRegistry;
use crate::retry::RetryConfig;
use crate::step_ops;
use crate::step_types::{CompletionOutcome, CompletionSpec, StepDefId, StepResult};

/// Worker dispatch entry point for declarative step handling (v3).
///
/// This function intentionally focuses on special step-task paths that are
/// currently handled outside user handlers:
/// - sleep continuation
/// - wait_for_event deadline
///
/// Regular body/step task handler execution remains in the existing worker path
/// during phased migration.
pub async fn dispatch_task_v3(
    step_def_id: StepDefId,
    scheduler: Arc<dyn StorageBackend>,
    _registry: Arc<TaskRegistry>,
    task: FetchedTask,
    _heartbeat_interval: Option<Duration>,
    _orphan_timeout: Duration,
) {
    let exec_mode = ExecutionMode::from_metadata(&task.metadata);

    // Only step-mode tasks are handled here; body tasks continue through
    // the existing imperative worker flow during phased rollout.
    if !matches!(exec_mode, ExecutionMode::Step { .. }) {
        return;
    }

    // Keep existing semantics: only specialized non-lambda step kinds are
    // dispatched here. Lambda-backed step execution remains in handler replay.
    match step_def_id {
        StepDefId::Sleep => {
            // Equivalent to previous dispatch_sleep_continuation.
            let run_id = task
                .metadata
                .get("run_id")
                .and_then(|v| v.as_str())
                .or_else(|| task.metadata.get("execution_id").and_then(|v| v.as_str()))
                .unwrap_or(&task.task_id)
                .to_string();

            let spec = CompletionSpec {
                step_task_id: task.task_id.clone(),
                step_id: task.task_id.clone(),
                step_name: "__sleep".to_string(),
                worker_id: task.lock_token.clone(),
                task_name: task.task_name.clone(),
                run_id,
                execution_id: task
                    .metadata
                    .get("execution_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or(&task.task_id)
                    .to_string(),
                data: task.data.clone(),
                attempt_number: task.attempt,
                result: StepResult::Transition,
                wait_group_step_name: None,
                outcome: CompletionOutcome::Success,
            };

            if let Err(e) = step_def_id
                .completion_behavior(&spec.outcome)
                .complete(&*scheduler, spec)
                .await
            {
                let _ = scheduler
                    .mark_failed(&task.task_id, &e.to_string(), None, &task.lock_token)
                    .await;
            }
        }

        StepDefId::WaitForEvent => {
            // Equivalent to previous dispatch_wait_for_event deadline path.
            let execution_id = task
                .metadata
                .get("execution_id")
                .and_then(|v| v.as_str())
                .unwrap_or(&task.task_id)
                .to_string();
            let step_name = task
                .metadata
                .get("step_name")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string();

            let spec = CompletionSpec {
                step_task_id: task.task_id.clone(),
                step_id: task.task_id.clone(),
                step_name,
                worker_id: task.lock_token.clone(),
                task_name: task.task_name.clone(),
                run_id: task
                    .metadata
                    .get("run_id")
                    .and_then(|v| v.as_str())
                    .or_else(|| task.metadata.get("execution_id").and_then(|v| v.as_str()))
                    .unwrap_or(&task.task_id)
                    .to_string(),
                execution_id,
                data: task.data.clone(),
                attempt_number: task.attempt,
                result: StepResult::Transition,
                wait_group_step_name: None,
                outcome: CompletionOutcome::Failure {
                    error: "event deadline exceeded".to_string(),
                },
            };

            if let Err(e) = step_def_id
                .completion_behavior(&spec.outcome)
                .complete(&*scheduler, spec)
                .await
            {
                let _ = scheduler
                    .mark_failed(&task.task_id, &e.to_string(), None, &task.lock_token)
                    .await;
            }
        }

        // Regular step kinds are still executed through handler replay.
        StepDefId::Step | StepDefId::WaitGroupChild => {}
    }
}

/// TaskContext step internal entry point for declarative step handling (v3).
///
/// Routes body-mode and step-mode behavior through `StepDefId` traits, including:
/// - body lookup/scheduling
/// - step lambda execution or cache lookup
/// - completion routing
/// - retry scheduling for failures in step mode
pub async fn step_internal_v3<T>(
    step_def_id: StepDefId,
    ctx: &mut TaskContext,
    step_name: &str,
    lambda: Option<PendingFn>,
) -> Result<T, StepError>
where
    T: for<'de> serde::Deserialize<'de> + serde::Serialize,
{
    match &ctx.execution_mode {
        ExecutionMode::Body => {
            let json = step_def_id.body_behavior().handle(ctx, step_name).await?;
            serde_json::from_value(json).map_err(|e| StepError::Failed {
                step: step_name.to_string(),
                reason: format!("failed to deserialize cached result: {e}"),
            })
        }

        ExecutionMode::Step {
            target_step,
            retry_attempt,
            retry_config,
            ..
        } => {
            let target_step = target_step.clone();
            let retry_attempt = *retry_attempt;
            let retry_config = retry_config.clone();

            // Non-target steps: read cached result only.
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

            // Target step execution path.
            let step_result = match step_def_id
                .step_behavior()
                .handle(ctx, step_name, lambda)
                .await
            {
                Ok(r) => r,
                Err(err) => {
                    // Preserve retry semantics from legacy step path.
                    if let Some(next) =
                        next_retry_time(err.to_string(), &retry_config, retry_attempt)
                    {
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

                    // Wait-group child failure route.
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
    }
}

/// Optional helper to adapt `StepError` into `TaskError` at call sites that
/// currently operate at task-level error boundaries.
pub fn into_task_error(task_name: &str, source: StepError) -> TaskError {
    TaskError::StepFailed {
        step: task_name.to_string(),
        source,
    }
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
