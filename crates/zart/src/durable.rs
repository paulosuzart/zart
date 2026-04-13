//! High-level durable execution entry point.
//!
//! [`DurableScheduler`] wraps the underlying `Scheduler` and provides
//! execution-aware operations: starting executions with idempotency keys,
//! querying status, and waiting for completion.

use crate::admin::{PauseRule, PauseScope, ResumeResult};
use crate::emit_metric;
use crate::error::SchedulerError;
#[cfg(feature = "metrics")]
use crate::metrics::EVENTS_DELIVERED_TOTAL;
use crate::registry::DurableExecution;
use scheduler::pause_storage::{PauseRule as StoragePauseRule, PauseRuleFilter, PauseStorage};
use scheduler::{
    ExecutionRecord, ExecutionStatus, ScheduleAtParams, ScheduleResult, StorageBackend,
};
use std::sync::Arc;
use std::time::Duration;

// Maximum duration for `wait_with_timeout` as per the spec.
const MAX_WAIT_SECS: u64 = 30;

/// High-level entry point for durable executions.
///
/// Wraps the underlying scheduler backend and coordinates:
/// - inserting an execution record in `zart_executions`
/// - scheduling the root task in `zart_tasks`
/// - querying and waiting for execution completion
pub struct DurableScheduler {
    scheduler: Arc<dyn StorageBackend>,
    pause_storage: Option<Arc<dyn PauseStorage>>,
}

impl DurableScheduler {
    /// Create a new `DurableScheduler`.
    pub fn new(scheduler: Arc<dyn StorageBackend>) -> Self {
        Self {
            scheduler,
            pause_storage: None,
        }
    }

    /// Create a new `DurableScheduler` with pause/resume support.
    pub fn with_pause(
        scheduler: Arc<dyn StorageBackend>,
        pause_storage: Arc<dyn PauseStorage>,
    ) -> Self {
        Self {
            scheduler,
            pause_storage: Some(pause_storage),
        }
    }

    /// Start a new durable execution with a raw JSON payload.
    ///
    /// If an execution with this ID already exists and is in a terminal state
    /// (completed, failed, cancelled), it will be reset to "scheduled" so it
    /// can be retried. If it exists and is **not** in a terminal state,
    /// [`SchedulerError::ExecutionAlreadyExists`] is returned.
    pub async fn start(
        &self,
        execution_id: &str,
        task_name: &str,
        payload: serde_json::Value,
    ) -> Result<ScheduleResult, SchedulerError> {
        // Check if execution already exists.
        let run_id: String;

        if let Some(existing) = self.scheduler.get_execution(execution_id).await? {
            match existing.status {
                // Still running — don't create a duplicate.
                ExecutionStatus::Scheduled | ExecutionStatus::Running => {
                    return Err(SchedulerError::ExecutionAlreadyExists(
                        execution_id.to_string(),
                        existing.status,
                    ));
                }
                // Terminal state — reset so we can retry.
                ExecutionStatus::Completed
                | ExecutionStatus::Failed
                | ExecutionStatus::Cancelled => {
                    // reset_execution returns the new run_id directly.
                    run_id = self
                        .scheduler
                        .reset_execution(execution_id, payload.clone())
                        .await?;
                }
            }
        } else {
            // First time — insert the record.
            self.scheduler
                .start_execution(execution_id, task_name, payload.clone())
                .await?;
            run_id = format!("{execution_id}:run:0");
        }

        // Schedule the root task that drives the execution.
        // The task_id is "{run_id}:body:start" — deterministic and debuggable.
        let task_id = format!("{run_id}:body:start");
        let metadata = serde_json::json!({
            "mode": "body",
            "run_id": run_id,
            "execution_id": execution_id.to_string(),
        });
        let result = self
            .scheduler
            .schedule_at(ScheduleAtParams {
                task_id: task_id.clone(),
                task_name: task_name.to_string(),
                execution_time: chrono::Utc::now(),
                data: payload,
                recurrence: None,
                metadata,
            })
            .await
            .map_err(SchedulerError::Database)?;

        Ok(result)
    }

    /// Return the current status of a durable execution.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::ExecutionNotFound`] if no execution with the
    /// given ID exists.
    pub async fn status(&self, execution_id: &str) -> Result<ExecutionRecord, SchedulerError> {
        self.scheduler
            .get_execution(execution_id)
            .await?
            .ok_or_else(|| SchedulerError::ExecutionNotFound(execution_id.to_string()))
    }

    /// Return the current `run_id` for a durable execution.
    ///
    /// Returns `None` if the execution has never been started.
    pub async fn get_current_run_id(
        &self,
        execution_id: &str,
    ) -> Result<Option<String>, SchedulerError> {
        Ok(self.scheduler.get_current_run_id(execution_id).await?)
    }

    /// List all runs for a durable execution.
    pub async fn list_runs(
        &self,
        execution_id: &str,
    ) -> Result<Vec<scheduler::ExecutionRunRecord>, SchedulerError> {
        Ok(self.scheduler.list_runs(execution_id).await?)
    }

    /// Block until the execution reaches a terminal state (completed, failed,
    /// or cancelled), polling every `poll_interval` (default: 500 ms).
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::WaitTimedOut`] if `timeout` elapses before
    /// the execution finishes.
    pub async fn wait(
        &self,
        execution_id: &str,
        timeout: Duration,
        poll_interval: Option<Duration>,
    ) -> Result<ExecutionRecord, SchedulerError> {
        let interval = poll_interval.unwrap_or(Duration::from_millis(500));
        let deadline = tokio::time::Instant::now() + timeout;

        loop {
            let record = self.status(execution_id).await?;

            match record.status {
                ExecutionStatus::Completed
                | ExecutionStatus::Failed
                | ExecutionStatus::Cancelled => return Ok(record),
                _ => {}
            }

            if tokio::time::Instant::now() >= deadline {
                return Err(SchedulerError::WaitTimedOut(execution_id.to_string()));
            }

            tokio::time::sleep(interval).await;
        }
    }

    /// Like [`wait`](Self::wait) but caps the maximum wait at 30 seconds.
    ///
    /// Returns [`SchedulerError::WaitTimedOut`] if the execution does not
    /// reach a terminal state within `max_duration` (or 30 s, whichever is less).
    pub async fn wait_with_timeout(
        &self,
        execution_id: &str,
        max_duration: Duration,
    ) -> Result<ExecutionRecord, SchedulerError> {
        let capped = max_duration.min(Duration::from_secs(MAX_WAIT_SECS));
        self.wait(execution_id, capped, None).await
    }

    /// Cancel a running or scheduled durable execution.
    ///
    /// Returns `true` if the execution was found and cancelled, `false` if it
    /// was already in a terminal state or did not exist.
    pub async fn cancel(&self, execution_id: &str) -> Result<bool, SchedulerError> {
        Ok(self.scheduler.cancel_execution(execution_id).await?)
    }

    /// Deliver an external event to a waiting execution.
    ///
    /// Atomically marks the event's step task completed with `payload` and
    /// schedules the next body segment. Races cleanly with the deadline worker:
    /// if the deadline already fired and the step task is no longer `scheduled`,
    /// returns [`SchedulerError::ExecutionNotFound`].
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::ExecutionNotFound`] if no scheduled
    /// wait_for_event step task was found for the given execution ID and event name.
    pub async fn offer_event(
        &self,
        execution_id: &str,
        event_name: &str,
        payload: serde_json::Value,
    ) -> Result<(), SchedulerError> {
        let result = self
            .scheduler
            .complete_event_step_and_schedule_body(execution_id, event_name, payload)
            .await;
        match result {
            Ok(true) => {
                emit_metric!(
                    EVENTS_DELIVERED_TOTAL
                        .with_label_values(&[event_name, "delivered"])
                        .inc()
                );
                Ok(())
            }
            Ok(false) => {
                emit_metric!(
                    EVENTS_DELIVERED_TOTAL
                        .with_label_values(&[event_name, "failed"])
                        .inc()
                );
                Err(SchedulerError::ExecutionNotFound(execution_id.to_string()))
            }
            Err(e) => {
                emit_metric!(
                    EVENTS_DELIVERED_TOTAL
                        .with_label_values(&[event_name, "failed"])
                        .inc()
                );
                Err(e.into())
            }
        }
    }

    /// List durable execution records with optional filters.
    ///
    /// Results are ordered by `scheduled_at DESC`.
    pub async fn list_executions(
        &self,
        status: Option<ExecutionStatus>,
        task_name: Option<String>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<ExecutionRecord>, SchedulerError> {
        Ok(self
            .scheduler
            .list_executions(status, task_name.as_deref(), limit, offset)
            .await?)
    }

    // ── Typed completion API ──────────────────────────────────────────────────

    /// Start execution for a specific `DurableExecution` handler.
    ///
    /// Infers the input type from `H::Data`. The `task_name` must match the
    /// name used when registering the handler in the `TaskRegistry`.
    ///
    /// # Errors
    ///
    /// - [`SchedulerError::ExecutionAlreadyExists`] if the execution is already running
    /// - [`SchedulerError::Database`] if the storage backend fails
    pub async fn start_for<H: DurableExecution>(
        &self,
        execution_id: &str,
        task_name: &str,
        input: &H::Data,
    ) -> Result<ScheduleResult, SchedulerError> {
        let payload = serde_json::to_value(input)?;
        self.start(execution_id, task_name, payload).await
    }

    /// Block until the execution reaches a terminal state, then deserialize
    /// the result to `T`.
    ///
    /// This is a convenience wrapper around [`Self::wait`] that eliminates
    /// manual `serde_json::from_value` calls.
    ///
    /// # Errors
    ///
    /// - [`SchedulerError::ExecutionNotFound`] if the execution doesn't exist
    /// - [`SchedulerError::WaitTimedOut`] if the timeout elapses
    /// - [`SchedulerError::Deserialization`] if the result can't be deserialized
    ///   to `T`, or if the execution completed with no result
    pub async fn wait_completion<T: serde::de::DeserializeOwned>(
        &self,
        execution_id: &str,
        timeout: Duration,
        poll_interval: Option<Duration>,
    ) -> Result<T, SchedulerError> {
        let record = self.wait(execution_id, timeout, poll_interval).await?;
        let result = record.result.ok_or_else(|| {
            SchedulerError::Deserialization("execution completed but had no result".to_string())
        })?;
        serde_json::from_value(result).map_err(|e| SchedulerError::Deserialization(e.to_string()))
    }

    /// Like [`Self::wait_completion`] but caps the maximum wait at 30 seconds.
    pub async fn wait_completion_with_timeout<T: serde::de::DeserializeOwned>(
        &self,
        execution_id: &str,
        max_duration: Duration,
    ) -> Result<T, SchedulerError> {
        let capped = max_duration.min(Duration::from_secs(MAX_WAIT_SECS));
        self.wait_completion(execution_id, capped, None).await
    }

    /// Wait for completion of an execution started for handler `H`,
    /// then deserialize the result to `H::Output`.
    ///
    /// This is the typed counterpart of [`Self::wait`] — use when you started
    /// an execution earlier and now want the typed result without manual
    /// deserialization.
    ///
    /// # Errors
    ///
    /// - [`SchedulerError::ExecutionNotFound`] if the execution doesn't exist
    /// - [`SchedulerError::WaitTimedOut`] if the timeout elapses
    /// - [`SchedulerError::Deserialization`] if the result can't be deserialized
    ///   to `H::Output`, or if the execution completed with no result
    pub async fn wait_for<H: DurableExecution>(
        &self,
        execution_id: &str,
        timeout: Duration,
    ) -> Result<H::Output, SchedulerError> {
        self.wait_completion(execution_id, timeout, None).await
    }

    /// Start execution for a specific `DurableExecution` handler, then block
    /// until completion and deserialize the result.
    ///
    /// Convenience for [`Self::start_for`] + [`Self::wait_for`].
    /// Infers input and output types from the handler's associated types.
    /// The `task_name` must match the name used when registering the handler
    /// in the `TaskRegistry`.
    ///
    /// # Errors
    ///
    /// - [`SchedulerError::ExecutionAlreadyExists`] if the execution is already running
    /// - [`SchedulerError::WaitTimedOut`] if the timeout elapses
    /// - [`SchedulerError::Deserialization`] if the result can't be deserialized
    ///   to `H::Output`, or if the execution completed with no result
    pub async fn start_and_wait_for<H: DurableExecution>(
        &self,
        execution_id: &str,
        task_name: &str,
        input: &H::Data,
        timeout: Duration,
    ) -> Result<H::Output, SchedulerError> {
        self.start_for::<H>(execution_id, task_name, input).await?;
        self.wait_for::<H>(execution_id, timeout).await
    }

    // ── Admin operations ──────────────────────────────────────────────────────

    /// Retry a single dead step within the current run.
    ///
    /// Finds the step by `run_id` + `step_name`. If the step is in `Dead`
    /// status, creates a new task for it and sets the run status back to
    /// `Running`. No new run is started — scoped to the current run.
    ///
    /// # Arguments
    ///
    /// * `run_id` — the run ID of the execution (e.g. `"exec-001:run:0"`).
    /// * `step_name` — the name of the step to retry (as declared in the handler).
    /// * `triggered_by` — optional operator identifier for audit logging.
    ///
    /// # Errors
    ///
    /// - [`SchedulerError::ExecutionNotFound`] if the step doesn't exist
    /// - [`SchedulerError::Database`] if the storage backend fails
    pub async fn retry_step(
        &self,
        run_id: &str,
        step_name: &str,
        triggered_by: Option<&str>,
    ) -> Result<String, SchedulerError> {
        Ok(self
            .scheduler
            .admin_retry_step(run_id, step_name, triggered_by)
            .await?)
    }

    /// Convenience wrapper around [`Self::retry_step`] that resolves the
    /// current run ID automatically.
    pub async fn retry_step_current_run(
        &self,
        execution_id: &str,
        step_name: &str,
        triggered_by: Option<&str>,
    ) -> Result<String, SchedulerError> {
        let run_id = self
            .scheduler
            .get_current_run_id(execution_id)
            .await?
            .ok_or_else(|| SchedulerError::ExecutionNotFound(execution_id.to_string()))?;
        self.retry_step(&run_id, step_name, triggered_by).await
    }

    /// Restart an entire execution from scratch.
    ///
    /// Archives the current run to `zart_execution_runs` (preserving history),
    /// creates a new run with `trigger = 'restart'`, and schedules a fresh
    /// body task.
    ///
    /// If `new_payload` is `Some`, it replaces the execution's payload.
    ///
    /// # Arguments
    ///
    /// * `execution_id` — the stable execution identifier.
    /// * `new_payload` — optional new payload (keeps existing if `None`).
    /// * `triggered_by` — optional operator identifier for audit logging.
    ///
    /// # Returns
    ///
    /// The new `run_id` (e.g. `"exec-001:run:1"`).
    pub async fn restart(
        &self,
        execution_id: &str,
        new_payload: Option<serde_json::Value>,
        triggered_by: Option<&str>,
    ) -> Result<String, SchedulerError> {
        Ok(self
            .scheduler
            .admin_restart_execution(execution_id, new_payload, triggered_by)
            .await?)
    }

    /// Selectively rerun a subset of steps while preserving others.
    ///
    /// Archives the current run, starts a new run with `trigger = 'selective_rerun'`,
    /// and schedules a fresh body task. The body replays from the top — completed
    /// steps are queried from the *previous* run so the handler can skip them.
    ///
    /// # Behavior
    ///
    /// - **Failed/dead steps** are always rerun (can't be preserved).
    /// - **`spec.force_rerun`** steps are rerun even if currently completed.
    /// - **`spec.preserve`** steps are carried forward — their results are returned
    ///   from the previous run instead of scheduling new tasks.
    /// - All other completed steps are preserved by default.
    ///
    /// # Returns
    ///
    /// `RerunResult` with the new run number and effective rerun set.
    pub async fn rerun_steps(
        &self,
        execution_id: &str,
        spec: crate::admin::RerunSpec,
    ) -> Result<crate::admin::RerunResult, SchedulerError> {
        let (new_run_id, effective_rerun) = self
            .scheduler
            .admin_rerun_steps(
                execution_id,
                &spec.force_rerun,
                &spec.preserve,
                spec.triggered_by.as_deref(),
            )
            .await?;

        // Parse run number from run_id (format: "exec-id:run:N").
        let run_number = new_run_id
            .rsplit_once(":run:")
            .and_then(|(_, n)| n.parse().ok())
            .unwrap_or(0);

        Ok(crate::admin::RerunResult {
            new_run_number: run_number,
            effective_rerun,
        })
    }

    // ── Pause / Resume ────────────────────────────────────────────────────────

    /// Create a pause rule.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::Database`] if pause storage is not configured
    /// or the database operation fails.
    pub async fn pause(&self, scope: PauseScope) -> Result<PauseRule, SchedulerError> {
        let pause_storage = self
            .pause_storage
            .as_ref()
            .ok_or_else(|| SchedulerError::PauseStorageNotConfigured)?;

        let rule_id = format!("pause-rule-{}", uuid::Uuid::new_v4());
        let rule = StoragePauseRule {
            rule_id: rule_id.clone(),
            execution_id: scope.execution_id,
            task_name: scope.task_name,
            step_pattern: scope.step_pattern,
            created_at: chrono::Utc::now(),
            expires_at: scope.expires_at,
            created_by: scope.triggered_by,
            deleted_at: None,
            deleted_by: None,
        };

        let created = pause_storage.create_pause_rule(rule).await?;

        Ok(PauseRule {
            rule_id: created.rule_id,
            scope: PauseScope {
                execution_id: created.execution_id,
                task_name: created.task_name,
                step_pattern: created.step_pattern,
                expires_at: created.expires_at,
                triggered_by: created.created_by,
            },
            created_at: created.created_at,
            deleted_at: created.deleted_at,
        })
    }

    /// Resume execution by soft-deleting matching pause rules.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::Database`] if pause storage is not configured
    /// or the database operation fails.
    pub async fn resume(&self, scope: PauseScope) -> Result<ResumeResult, SchedulerError> {
        let pause_storage = self
            .pause_storage
            .as_ref()
            .ok_or_else(|| SchedulerError::PauseStorageNotConfigured)?;

        // Find matching rules and soft-delete them.
        let filter = PauseRuleFilter {
            execution_id: scope.execution_id.clone(),
            task_name: scope.task_name.clone(),
            include_deleted: false,
        };
        let rules = pause_storage.list_pause_rules(filter).await?;

        let mut deleted = 0usize;
        for rule in &rules {
            if pause_storage
                .delete_pause_rule(&rule.rule_id, scope.triggered_by.as_deref())
                .await?
            {
                deleted += 1;
            }
        }

        Ok(ResumeResult {
            rules_deleted: deleted,
        })
    }

    /// Resume by deleting a specific pause rule by ID.
    pub async fn resume_rule_by_id(
        &self,
        rule_id: &str,
        deleted_by: Option<&str>,
    ) -> Result<bool, SchedulerError> {
        let pause_storage = self
            .pause_storage
            .as_ref()
            .ok_or_else(|| SchedulerError::PauseStorageNotConfigured)?;

        Ok(pause_storage.delete_pause_rule(rule_id, deleted_by).await?)
    }

    /// List active pause rules.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::Database`] if pause storage is not configured
    /// or the database operation fails.
    pub async fn list_pause_rules(
        &self,
        filter: Option<PauseRuleFilter>,
    ) -> Result<Vec<PauseRule>, SchedulerError> {
        let pause_storage = self
            .pause_storage
            .as_ref()
            .ok_or_else(|| SchedulerError::PauseStorageNotConfigured)?;

        let rules = pause_storage
            .list_pause_rules(filter.unwrap_or_default())
            .await?;

        Ok(rules
            .into_iter()
            .map(|r| PauseRule {
                rule_id: r.rule_id,
                scope: PauseScope {
                    execution_id: r.execution_id,
                    task_name: r.task_name,
                    step_pattern: r.step_pattern,
                    expires_at: r.expires_at,
                    triggered_by: r.created_by,
                },
                created_at: r.created_at,
                deleted_at: r.deleted_at,
            })
            .collect())
    }
}
