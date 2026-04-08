//! Body-mode behavior implementations for declarative step dispatch (v3 scaffold).
//!
//! These implementations are intentionally conservative and backward-compatible
//! with the current runtime paths. They can be wired incrementally during cutover.

use async_trait::async_trait;
use scheduler::{StepLookup, TaskStatus};

use crate::TaskContext;
use crate::emit_metric;
use crate::error::StepError;
#[cfg(feature = "metrics")]
use crate::metrics::STEPS_TOTAL;
use crate::step_ops;
use crate::step_types::BodyBehavior;

/// Default body behavior for regular step-like entries:
/// - `step`
/// - `sleep`
/// - `wait_group_child`
///
/// Behavior:
/// 1. If completed in storage, return cached result.
/// 2. If scheduled/running, return `StepError::Scheduled`.
/// 3. If absent, schedule a step task and return `StepError::Scheduled`.
pub struct LookupOrSchedule;

#[async_trait]
impl BodyBehavior for LookupOrSchedule {
    async fn handle(
        &self,
        ctx: &mut TaskContext,
        step_name: &str,
    ) -> Result<serde_json::Value, StepError> {
        let lookup = ctx
            .scheduler
            .get_step_status(ctx.run_id(), step_name)
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
            }) => Ok(json),

            Some(StepLookup {
                status: TaskStatus::Completed,
                result: None,
                ..
            }) => Err(StepError::Failed {
                step: step_name.to_string(),
                reason: "step completed but result is missing".to_string(),
            }),

            Some(_) => {
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
                emit_metric!(
                    STEPS_TOTAL
                        .with_label_values(&["scheduled", step_name])
                        .inc()
                );

                let task_id = format!("{}:step:{}", ctx.run_id(), step_name);
                step_ops::schedule_step_task(
                    &*ctx.scheduler,
                    step_ops::StepTaskSpec {
                        task_id: &task_id,
                        task_name: ctx.task_name_internal(),
                        run_id: ctx.run_id(),
                        step_name,
                        data: ctx.data().clone(),
                        retry_config: None,
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
}

/// Body behavior specialized for `wait_for_event`.
///
/// Behavior:
/// 1. If completed, return cached event payload.
/// 2. If in-flight, return `StepError::Scheduled`.
/// 3. If absent, schedule event wait step with deadline metadata and return
///    `StepError::Scheduled`.
pub struct LookupOrScheduleEvent;

#[async_trait]
impl BodyBehavior for LookupOrScheduleEvent {
    async fn handle(
        &self,
        ctx: &mut TaskContext,
        step_name: &str,
    ) -> Result<serde_json::Value, StepError> {
        let lookup = ctx
            .scheduler
            .get_step_status(ctx.run_id(), step_name)
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
            }) => Ok(json),

            Some(StepLookup {
                status: TaskStatus::Completed,
                result: None,
                ..
            }) => Err(StepError::Failed {
                step: step_name.to_string(),
                reason: "event step completed but result is missing".to_string(),
            }),

            Some(_) => {
                emit_metric!(
                    STEPS_TOTAL
                        .with_label_values(&["waiting_for_event", step_name])
                        .inc()
                );
                Err(StepError::Scheduled {
                    step: step_name.to_string(),
                    next_execution: None,
                })
            }

            None => {
                emit_metric!(
                    STEPS_TOTAL
                        .with_label_values(&["waiting_for_event", step_name])
                        .inc()
                );

                // Scaffold behavior: no explicit timeout is carried by this trait
                // method yet, so we register a no-deadline wait. Deadline-aware
                // wiring will be added during dispatch integration phase.
                let task_id = format!("{}:step:{}", ctx.run_id(), step_name);
                step_ops::schedule_wait_for_event_task(
                    &*ctx.scheduler,
                    step_ops::EventStepSpec {
                        task_id: &task_id,
                        task_name: ctx.task_name_internal(),
                        run_id: ctx.run_id(),
                        event_name: step_name,
                        data: ctx.data().clone(),
                        deadline: None,
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
}
