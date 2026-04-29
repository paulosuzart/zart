use crate::context::TaskContext;
use crate::error::{ExecutionFailure, StepError, TaskError};
use crate::execution_model::ExecutionMode;
use crate::registry::DurableRegistry;
use crate::store::StorageBackend;
use crate::trx_impl;
use async_trait::async_trait;
use std::sync::Arc;
use tracing::{error, info, warn};
use zart_core::TaskMetadata;
use zart_scheduler::{
    CompletionHandler, ScheduleAtParams, ScheduledTask, SchedulerTaskError, TaskInstance,
    TaskScheduler,
};

pub struct ZartTask {
    storage: Arc<dyn StorageBackend>,
    scheduler: Arc<dyn TaskScheduler>,
    registry: Arc<DurableRegistry>,
}

impl ZartTask {
    pub fn new(
        storage: Arc<dyn StorageBackend>,
        scheduler: Arc<dyn TaskScheduler>,
        registry: Arc<DurableRegistry>,
    ) -> Self {
        Self {
            storage,
            scheduler,
            registry,
        }
    }
}

#[async_trait]
impl ScheduledTask for ZartTask {
    async fn execute(
        &self,
        instance: &TaskInstance,
    ) -> Result<Box<dyn CompletionHandler>, SchedulerTaskError> {
        // 1. Parse metadata and extract handler name
        let typed_meta: Option<TaskMetadata> = match instance.metadata.get("mode") {
            Some(_) => match TaskMetadata::from_json_value(instance.metadata.clone()) {
                Ok(m) => Some(m),
                Err(e) => {
                    error!(error = %e, task_id = %instance.task_id, "Failed to parse task metadata");
                    return Err(SchedulerTaskError::HandlerPanic(
                        "invalid metadata".to_string(),
                    ));
                }
            },
            None => None,
        };

        let has_execution = typed_meta.is_some();
        let execution_id = typed_meta
            .as_ref()
            .map(|m| m.execution_id().to_string())
            .unwrap_or_else(|| instance.task_id.clone());

        let run_id = typed_meta
            .as_ref()
            .map(|m| m.run_id().to_string())
            .unwrap_or_else(|| execution_id.clone());

        let step_deadline = typed_meta
            .as_ref()
            .and_then(|m| m.deadline())
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.with_timezone(&chrono::Utc));

        let exec_mode = match &typed_meta {
            Some(m) => ExecutionMode::from_task_metadata(m),
            None => ExecutionMode::Body,
        };

        // Override retry_attempt
        let exec_mode = match exec_mode {
            ExecutionMode::Step {
                target_step,
                step_type,
                retry_config,
                ..
            } => ExecutionMode::Step {
                target_step,
                step_type,
                retry_attempt: instance.attempt.saturating_sub(1) as usize,
                retry_config,
            },
            other => other,
        };

        // 2. Load execution record — provides handler name and scheduled_at for deadline check.
        //    All __zart__ tasks are created with an execution_id in metadata; if one is missing
        //    the task row is corrupt and we cannot dispatch it.
        if !has_execution {
            warn!(task_id = %instance.task_id, "ZartTask received a task without execution context");
            return Err(SchedulerTaskError::HandlerPanic(
                "missing execution context".to_string(),
            ));
        }
        let execution = match self.storage.get_execution(&execution_id).await {
            Ok(Some(exec)) => exec,
            Ok(None) => {
                warn!(execution_id = %execution_id, "Execution record not found — task may be stale");
                return Err(SchedulerTaskError::HandlerPanic(format!(
                    "execution {execution_id} not found"
                )));
            }
            Err(e) => return Err(SchedulerTaskError::Storage(e)),
        };
        let handler_name = execution.task_name.as_str();

        // 3. Look up handler
        let handler = match self.registry.get_handler(handler_name) {
            Some(h) => h,
            None => {
                warn!(
                    handler_name = %handler_name,
                    "No handler registered for '{}'; registered handlers: [{}]",
                    handler_name,
                    self.registry.handler_names().join(", ")
                );
                return Err(SchedulerTaskError::HandlerPanic(format!(
                    "unknown handler: {handler_name}"
                )));
            }
        };

        // 4. Execution deadline (derived from execution.scheduled_at + handler timeout).
        let execution_deadline = handler.timeout().map(|dur| {
            execution.scheduled_at
                + chrono::Duration::from_std(dur).unwrap_or(chrono::Duration::zero())
        });

        if let Some(deadline) = execution_deadline
            && chrono::Utc::now() >= deadline
        {
            info!(execution_id = %execution_id, "Execution deadline exceeded before dispatch");
            let failure = ExecutionFailure::ExecutionDeadlineExceeded;
            match handler.on_failure(instance.data.clone(), failure).await {
                Ok(output) => {
                    info!("on_failure recovered from execution deadline");
                    let _ = self
                        .storage
                        .complete_execution(&execution_id, output.clone())
                        .await;
                    return Ok(Box::new(zart_scheduler::completion::OnComplete {
                        result: Some(output),
                        schedule_next: vec![],
                    }));
                }
                Err(recovery_err) => {
                    error!(error = %recovery_err, "on_failure did not recover execution deadline");
                    let _ = self.storage.fail_execution(&execution_id).await;
                    return Err(SchedulerTaskError::HandlerPanic(format!(
                        "deadline exceeded: {recovery_err}"
                    )));
                }
            }
        }

        // 4. Build TaskContext
        let ctx = Arc::new(
            TaskContext::new(
                self.storage.clone(),
                self.scheduler.clone(),
                execution_id.clone(),
                handler_name.to_string(),
                instance.lock_token.clone(),
                instance.data.clone(),
            )
            .with_task_id(instance.task_id.clone())
            .with_run_id(run_id.clone())
            .with_execution_mode(exec_mode.clone())
            .with_step_deadline(step_deadline)
            .with_execution_deadline(execution_deadline),
        );

        // 5. Execute handler within STEP_TRX scope so `zart::trx()` can register a transaction.
        //    We also capture the (tx, hint) inside the scope because STEP_TRX is only
        //    available while `with_step_trx` is on the stack — once it returns the
        //    task-local is gone and `take_step_trx` would return `(None, None)`.
        let (result, step_trx, step_hint) = trx_impl::with_step_trx(async {
            let res = handler.execute(ctx.clone(), instance.data.clone()).await;
            let (tx, hint) = trx_impl::take_step_trx().await;
            (res, tx, hint)
        })
        .await;

        // 6. Handle results — return appropriate CompletionHandler
        match result {
            Ok(val) => {
                info!("Task completed successfully");
                if has_execution {
                    let _ = self
                        .storage
                        .complete_execution(&execution_id, val.clone())
                        .await;
                }
                Ok(Box::new(zart_scheduler::completion::OnComplete {
                    result: Some(val),
                    schedule_next: vec![],
                }))
            }
            Err(TaskError::StepFailed {
                source: StepError::StepExecuted { ref step },
                ..
            }) => {
                info!(step = %step, "Step executed — selecting CompletionHandler");
                let tx = match step_trx {
                    Some(t) => t,
                    None => self
                        .scheduler
                        .begin()
                        .await
                        .map_err(SchedulerTaskError::Storage)?,
                };

                let step_id = instance.task_id.clone();

                // Select the correct CompletionHandler based on the hint.
                match step_hint {
                    Some(trx_impl::StepCompletionHint::WaitGroupChild {
                        group_step_name,
                        result,
                        result_kind,
                        attempt_number,
                    }) => {
                        info!(step = %step, "Returning ZartWaitGroupChildCompletion");
                        let child_step_task_id = instance.task_id.clone();
                        let child_step_id = format!("{}:step:{}", run_id, step);
                        let next_body_task_id =
                            format!("{}:body:after:{}", run_id, group_step_name);
                        let step_params = zart_core::types::WriteStepCompletionParams {
                            step_id,
                            attempt_number,
                            result: result.clone(),
                            result_kind: zart_core::types::StepResultKind::from(result_kind),
                        };
                        let params = zart_core::types::CompleteWaitGroupChildParams {
                            run_id: run_id.clone(),
                            execution_id: execution_id.clone(),
                            group_step_name,
                            child_step_task_id,
                            child_step_id,
                            child_result: result,
                            lock_token: instance.lock_token.clone(),
                            attempt_number: instance.attempt as usize,
                            next_body_task_id,
                            data: instance.data.clone(),
                        };
                        Ok(Box::new(
                            crate::step_completion::ZartWaitGroupChildCompletion {
                                storage: self.storage.clone(),
                                tx,
                                step_params,
                                params,
                            },
                        ))
                    }
                    Some(trx_impl::StepCompletionHint::WaitGroupChildFailure {
                        group_step_name,
                        error,
                        attempt_number,
                    }) => {
                        info!(step = %step, "Returning ZartWaitGroupFailureCompletion");
                        let child_step_task_id = instance.task_id.clone();
                        let child_step_id = format!("{}:step:{}", run_id, step);
                        let step_params = zart_core::types::WriteStepCompletionParams {
                            step_id,
                            attempt_number,
                            result: serde_json::json!({ "error": &error }),
                            result_kind: zart_core::types::StepResultKind::Err,
                        };
                        let params = zart_core::types::FailWaitGroupChildParams {
                            run_id: run_id.clone(),
                            group_step_name,
                            child_step_task_id,
                            child_step_id,
                            error,
                            lock_token: instance.lock_token.clone(),
                            attempt_number: instance.attempt as usize,
                        };
                        Ok(Box::new(
                            crate::step_completion::ZartWaitGroupFailureCompletion {
                                storage: self.storage.clone(),
                                tx,
                                step_params,
                                params,
                                execution_id: execution_id.clone(),
                            },
                        ))
                    }
                    Some(trx_impl::StepCompletionHint::RegularStep {
                        result,
                        result_kind,
                        attempt_number,
                    }) => {
                        info!(step = %step, "Returning ZartStepCompletion");
                        let step_params = zart_core::types::WriteStepCompletionParams {
                            step_id,
                            attempt_number,
                            result,
                            result_kind: zart_core::types::StepResultKind::from(result_kind),
                        };
                        let next_body_task_id = format!("{}:body:after:{}", run_id, step);
                        let next_body = ScheduleAtParams {
                            task_id: next_body_task_id,
                            task_name: crate::TASK_NAME.to_string(),
                            execution_time: chrono::Utc::now(),
                            data: instance.data.clone(),
                            recurrence: None,
                            metadata: zart_core::TaskMetadata::body(&run_id, &execution_id)
                                .to_json_value(),
                        };
                        Ok(Box::new(crate::step_completion::ZartStepCompletion {
                            storage: self.storage.clone(),
                            tx,
                            step_params,
                            next_body,
                        }))
                    }
                    None => {
                        // No hint — should not happen in normal flow; treat as regular step
                        // with a null result to avoid losing the tx.
                        info!(step = %step, "Returning ZartStepCompletion (no hint)");
                        let next_body_task_id = format!("{}:body:after:{}", run_id, step);
                        let step_params = zart_core::types::WriteStepCompletionParams {
                            step_id,
                            attempt_number: instance.attempt as usize,
                            result: serde_json::Value::Null,
                            result_kind: zart_core::types::StepResultKind::Ok,
                        };
                        let next_body = ScheduleAtParams {
                            task_id: next_body_task_id,
                            task_name: crate::TASK_NAME.to_string(),
                            execution_time: chrono::Utc::now(),
                            data: instance.data.clone(),
                            recurrence: None,
                            metadata: zart_core::TaskMetadata::body(&run_id, &execution_id)
                                .to_json_value(),
                        };
                        Ok(Box::new(crate::step_completion::ZartStepCompletion {
                            storage: self.storage.clone(),
                            tx,
                            step_params,
                            next_body,
                        }))
                    }
                }
            }
            Err(TaskError::StepFailed {
                source: StepError::Scheduled { ref step, .. },
                ..
            }) => {
                info!(step = %step, "Body step scheduled — marking body task complete");
                Ok(Box::new(zart_scheduler::completion::OnComplete {
                    result: None,
                    schedule_next: vec![],
                }))
            }
            Err(err) => {
                let failure = build_execution_failure(&err, instance);
                if has_execution {
                    match handler.on_failure(instance.data.clone(), failure).await {
                        Ok(output) => {
                            info!(
                                "on_failure recovered — completing execution with synthetic result"
                            );
                            let _ = self
                                .storage
                                .complete_execution(&execution_id, output.clone())
                                .await;
                            return Ok(Box::new(zart_scheduler::completion::OnComplete {
                                result: Some(output),
                                schedule_next: vec![],
                            }));
                        }
                        Err(recovery_err) => {
                            error!(error = %recovery_err, "on_failure did not recover the execution");
                        }
                    }
                }
                if has_execution {
                    let _ = self.storage.fail_execution(&execution_id).await;
                }
                Err(SchedulerTaskError::HandlerPanic(err.to_string()))
            }
        }
    }
}

fn build_execution_failure(err: &TaskError, task: &TaskInstance) -> ExecutionFailure {
    match err {
        TaskError::StepFailed { step, source } => {
            let raw = serde_json::json!({
                "step": step,
                "error": source.to_string(),
                "error_kind": format!("{:?}", source),
            });
            ExecutionFailure::StepFailed {
                step: step.clone(),
                raw,
            }
        }
        TaskError::MaxRetriesExhausted { max_retries } => ExecutionFailure::RetriesExhausted {
            attempts: *max_retries,
        },
        TaskError::Timeout { duration } => {
            let _ = duration;
            ExecutionFailure::ExecutionDeadlineExceeded
        }
        TaskError::Cancelled => {
            let step = task.task_name.clone();
            let raw = serde_json::json!({ "error": "cancelled" });
            ExecutionFailure::StepFailed { step, raw }
        }
        TaskError::HandlerPanic(reason) => {
            let step = task.task_name.clone();
            let raw = serde_json::json!({ "panic": reason });
            ExecutionFailure::StepFailed { step, raw }
        }
    }
}
