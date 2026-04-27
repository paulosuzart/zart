use crate::context::TaskContext;
use crate::error::{ExecutionFailure, StepError, TaskError};
use crate::execution_model::ExecutionMode;
use crate::registry::DurableRegistry;
use crate::store::StorageBackend;
use async_trait::async_trait;
use std::sync::Arc;
use tracing::{error, info, warn};
use zart_core::TaskMetadata;
use zart_scheduler::{ExecutionOps, ScheduledTask, SchedulerTaskError, TaskInstance};

pub struct ZartTask {
    storage: Arc<dyn StorageBackend>,
    registry: Arc<DurableRegistry>,
}

impl ZartTask {
    pub fn new(storage: Arc<dyn StorageBackend>, registry: Arc<DurableRegistry>) -> Self {
        Self { storage, registry }
    }
}

#[async_trait]
impl ScheduledTask for ZartTask {
    async fn execute(
        &self,
        instance: &TaskInstance,
        ops: &mut ExecutionOps<'_>,
    ) -> Result<(), SchedulerTaskError> {
        // 1. Parse metadata and extract handler name
        let typed_meta: Option<TaskMetadata> = match instance.metadata.get("mode") {
            Some(_) => match TaskMetadata::from_json_value(instance.metadata.clone()) {
                Ok(m) => Some(m),
                Err(e) => {
                    error!(error = %e, task_id = %instance.task_id, "Failed to parse task metadata");
                    ops.complete(None)
                        .await
                        .map_err(SchedulerTaskError::Storage)?;
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
            ops.complete(None)
                .await
                .map_err(SchedulerTaskError::Storage)?;
            return Err(SchedulerTaskError::HandlerPanic(
                "missing execution context".to_string(),
            ));
        }
        let execution = match self.storage.get_execution(&execution_id).await {
            Ok(Some(exec)) => exec,
            Ok(None) => {
                warn!(execution_id = %execution_id, "Execution record not found — task may be stale");
                ops.complete(None)
                    .await
                    .map_err(SchedulerTaskError::Storage)?;
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
                    ops.complete(Some(output.clone()))
                        .await
                        .map_err(SchedulerTaskError::Storage)?;
                    let _ = self.storage.complete_execution(&execution_id, output).await;
                }
                Err(recovery_err) => {
                    error!(error = %recovery_err, "on_failure did not recover execution deadline");
                    ops.complete(None)
                        .await
                        .map_err(SchedulerTaskError::Storage)?;
                    let _ = self.storage.fail_execution(&execution_id).await;
                    return Err(SchedulerTaskError::HandlerPanic(format!(
                        "deadline exceeded: {recovery_err}"
                    )));
                }
            }
            return Ok(());
        }

        // 4. Build TaskContext
        let ctx = Arc::new(
            TaskContext::new(
                self.storage.clone(),
                ops.scheduler().clone(),
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

        // 5. Execute handler
        let result = handler.execute(ctx.clone(), instance.data.clone()).await;

        // 6. Handle results via ExecutionOps
        match result {
            Ok(val) => {
                info!("Task completed successfully");
                ops.complete(Some(val.clone()))
                    .await
                    .map_err(SchedulerTaskError::Storage)?;
                if has_execution {
                    let _ = self.storage.complete_execution(&execution_id, val).await;
                }
            }
            Err(TaskError::StepFailed {
                source: StepError::StepExecuted { ref step },
                ..
            }) => {
                info!(step = %step, "Step executed in step mode — completion was transactional");
                // The step-completion path already committed its own transaction and cleared
                // worker_id. Tell the worker not to attempt a second complete/mark_failed.
                ops.mark_handled();
            }
            Err(TaskError::StepFailed {
                source: StepError::Scheduled { ref step, .. },
                ..
            }) => {
                info!(step = %step, "Body step scheduled — marking body task complete");
                ops.complete(None)
                    .await
                    .map_err(SchedulerTaskError::Storage)?;
            }
            Err(err) => {
                let failure = build_execution_failure(&err, instance);
                if has_execution {
                    match handler.on_failure(instance.data.clone(), failure).await {
                        Ok(output) => {
                            info!(
                                "on_failure recovered — completing execution with synthetic result"
                            );
                            ops.complete(Some(output.clone()))
                                .await
                                .map_err(SchedulerTaskError::Storage)?;
                            let _ = self.storage.complete_execution(&execution_id, output).await;
                        }
                        Err(recovery_err) => {
                            error!(error = %recovery_err, "on_failure did not recover the execution");
                        }
                    }
                }
                ops.complete(None)
                    .await
                    .map_err(SchedulerTaskError::Storage)?;
                if has_execution {
                    let _ = self.storage.fail_execution(&execution_id).await;
                }
                return Err(SchedulerTaskError::HandlerPanic(err.to_string()));
            }
        }

        Ok(())
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
