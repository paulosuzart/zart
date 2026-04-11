//! Body-mode behavior implementations for declarative step dispatch.
//!
//! Each implementation handles how a step type behaves when the body is
//! replaying: look up the existing row, return the cached result if complete,
//! schedule a new row if absent, or signal in-flight if already scheduled.

use async_trait::async_trait;
use scheduler::{StepLookup, TaskStatus, UpsertWaitGroupStepParams};

use crate::TaskContext;
use crate::emit_metric;
use crate::error::StepError;
#[cfg(feature = "metrics")]
use crate::metrics::STEPS_TOTAL;
use crate::step_ops;
use crate::step_types::{BodyBehavior, ResultKind, StepRequest, StepRequestKind};

/// Default body behavior for regular step-like entries:
/// - `step`
/// - `wait_group_child`
///
/// Behavior:
/// 1. If completed in storage, return cached result + result_kind.
/// 2. If scheduled/running, return `StepError::Scheduled`.
/// 3. If absent, schedule a step task and return `StepError::Scheduled`.
pub struct LookupOrSchedule;

#[async_trait]
impl BodyBehavior for LookupOrSchedule {
    async fn handle(
        &self,
        ctx: &TaskContext,
        req: &StepRequest<'_>,
    ) -> Result<(serde_json::Value, ResultKind), StepError> {
        let step_name = req.step_name;

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
                result_kind,
                ..
            }) => {
                let kind = result_kind.map(ResultKind::from).unwrap_or(ResultKind::Ok);
                Ok((json, kind))
            }

            Some(StepLookup {
                status: TaskStatus::Completed,
                result: None,
                result_kind,
                ..
            }) => {
                let kind = result_kind.map(ResultKind::from).unwrap_or(ResultKind::Ok);
                Ok((serde_json::Value::Null, kind))
            }

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
                        retry_config: req.retry_config,
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

/// Body behavior for `sleep` steps.
///
/// Behavior:
/// 1. If completed, return `Null` (sleep carries no result value).
/// 2. If in-flight, return `StepError::Scheduled`.
/// 3. If absent, schedule the sleep step task with `wake_time` as its
///    `execution_time`, then return `StepError::Scheduled`.
pub struct LookupOrScheduleSleep;

#[async_trait]
impl BodyBehavior for LookupOrScheduleSleep {
    async fn handle(
        &self,
        ctx: &TaskContext,
        req: &StepRequest<'_>,
    ) -> Result<(serde_json::Value, ResultKind), StepError> {
        let wake_time = match req.kind {
            StepRequestKind::Sleep { wake_time } => wake_time,
            _ => {
                return Err(StepError::Failed {
                    step: req.step_name.to_string(),
                    reason: "LookupOrScheduleSleep called with non-sleep request".to_string(),
                });
            }
        };

        let step_name = req.step_name;

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
                ..
            }) => Ok((serde_json::Value::Null, ResultKind::Ok)),

            Some(_) => Err(StepError::Scheduled {
                step: step_name.to_string(),
                next_execution: None,
            }),

            None => {
                let task_id = format!("{}:step:{}", ctx.run_id(), step_name);
                step_ops::schedule_sleep_task(
                    &*ctx.scheduler,
                    &task_id,
                    ctx.task_name_internal(),
                    ctx.run_id(),
                    wake_time,
                    ctx.data().clone(),
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
/// 3. If absent, schedule event wait step with the deadline from the request
///    (or `DateTime::MAX_UTC` when no deadline), then return
///    `StepError::Scheduled`.
pub struct LookupOrScheduleEvent;

#[async_trait]
impl BodyBehavior for LookupOrScheduleEvent {
    async fn handle(
        &self,
        ctx: &TaskContext,
        req: &StepRequest<'_>,
    ) -> Result<(serde_json::Value, ResultKind), StepError> {
        let deadline = match req.kind {
            StepRequestKind::WaitForEvent { deadline } => deadline,
            _ => {
                return Err(StepError::Failed {
                    step: req.step_name.to_string(),
                    reason: "LookupOrScheduleEvent called with non-event request".to_string(),
                });
            }
        };

        let step_name = req.step_name;

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
                result_kind,
                ..
            }) => {
                let kind = result_kind.map(ResultKind::from).unwrap_or(ResultKind::Ok);
                Ok((json, kind))
            }

            Some(StepLookup {
                status: TaskStatus::Completed,
                result: None,
                result_kind,
                ..
            }) => {
                let kind = result_kind.map(ResultKind::from).unwrap_or(ResultKind::Ok);
                Ok((serde_json::Value::Null, kind))
            }

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

                let task_id = format!("{}:step:{}", ctx.run_id(), step_name);
                step_ops::schedule_wait_for_event_task(
                    &*ctx.scheduler,
                    step_ops::EventStepSpec {
                        task_id: &task_id,
                        task_name: ctx.task_name_internal(),
                        run_id: ctx.run_id(),
                        event_name: step_name,
                        data: ctx.data().clone(),
                        deadline,
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

/// Body behavior for wait-group barrier operations (`wait_all`, `wait_any`, etc.).
///
/// Behavior:
/// 1. For each child: if absent, schedule it with `wg_step_name` in metadata.
/// 2. Upsert the wait-group parent row.
/// 3. If all children completed: return a JSON array of their results.
/// 4. Otherwise: return `StepError::Scheduled`.
pub struct LookupOrScheduleWaitGroupBarrier;

#[async_trait]
impl BodyBehavior for LookupOrScheduleWaitGroupBarrier {
    async fn handle(
        &self,
        ctx: &TaskContext,
        req: &StepRequest<'_>,
    ) -> Result<(serde_json::Value, ResultKind), StepError> {
        let (group_step_name, child_names, threshold) = match &req.kind {
            StepRequestKind::WaitGroupBarrier {
                group_step_name,
                child_names,
                threshold,
            } => (*group_step_name, *child_names, *threshold),
            _ => {
                return Err(StepError::Failed {
                    step: req.step_name.to_string(),
                    reason: "LookupOrScheduleWaitGroupBarrier called with non-barrier request"
                        .to_string(),
                });
            }
        };

        let mut all_completed = true;

        for child_name in child_names {
            let child_task_id = format!("{}:step:{}", ctx.run_id(), child_name);

            let lookup = ctx
                .scheduler
                .get_step_status(ctx.run_id(), child_name)
                .await
                .map_err(|e| StepError::Failed {
                    step: child_name.clone(),
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
                    step_ops::schedule_wait_group_child_task(
                        &*ctx.scheduler,
                        &child_task_id,
                        ctx.task_name_internal(),
                        ctx.run_id(),
                        child_name,
                        group_step_name,
                        ctx.data().clone(),
                    )
                    .await
                    .map_err(|e| StepError::Failed {
                        step: child_name.clone(),
                        reason: e.to_string(),
                    })?;
                }
            }
        }

        let total = i32::try_from(child_names.len()).map_err(|_| StepError::Failed {
            step: group_step_name.to_string(),
            reason: "too many wait-group handles to fit i32".to_string(),
        })?;

        ctx.scheduler
            .upsert_wait_group_step(UpsertWaitGroupStepParams {
                run_id: ctx.run_id().to_string(),
                group_step_name: group_step_name.to_string(),
                total,
                threshold,
            })
            .await
            .map_err(|e| StepError::Failed {
                step: group_step_name.to_string(),
                reason: e.to_string(),
            })?;

        if all_completed {
            let mut results = Vec::with_capacity(child_names.len());
            for child_name in child_names {
                let lookup = ctx
                    .scheduler
                    .get_step_status(ctx.run_id(), child_name)
                    .await
                    .map_err(|e| StepError::Failed {
                        step: child_name.clone(),
                        reason: e.to_string(),
                    })?;
                match lookup {
                    Some(StepLookup {
                        status: TaskStatus::Completed,
                        result: Some(json),
                        ..
                    }) => {
                        results.push(json);
                    }
                    _ => {
                        results.push(serde_json::Value::Null);
                    }
                }
            }
            return Ok((serde_json::Value::Array(results), ResultKind::Ok));
        }

        Err(StepError::Scheduled {
            step: group_step_name.to_string(),
            next_execution: None,
        })
    }
}
