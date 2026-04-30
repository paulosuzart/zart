//! High-level durable execution entry point.
//!
//! [`DurableScheduler`] wraps the underlying `Scheduler` and provides
//! execution-aware operations: starting executions with idempotency keys,
//! querying status, and waiting for completion.

use crate::admin::{PauseRule, PauseScope, RerunResult, RerunSpec, ResumeResult};
use crate::emit_metric;
use crate::error::SchedulerError;
#[cfg(feature = "metrics")]
use crate::metrics::EVENTS_DELIVERED_TOTAL;
use crate::registry::DurableExecution;
use crate::service::{ExecutionService, PauseService};
use crate::store::{Backend, StorageBackend};
use std::sync::Arc;
use std::time::Duration;
use zart_core::TaskMetadata;
use zart_core::store::pause_storage::PauseRuleFilter;
use zart_core::types::{ExecutionRecord, ExecutionStatus, ListExecutionsParams};
use zart_scheduler::{ScheduleAtParams, ScheduleResult, TaskScheduler};

// Maximum duration for `wait_with_timeout` as per the spec.
const MAX_WAIT_SECS: u64 = 30;

/// Starts, queries, and awaits durable executions.
///
/// `DurableScheduler` is the public interface your application uses to trigger
/// workflows and read their status. Workers pick up and execute the work;
/// `DurableScheduler` is only the control plane.
///
/// # Execution IDs and idempotency
///
/// Every execution is identified by a caller-chosen string ID. Calling
/// [`start`](Self::start) with an ID that already exists in a non-terminal
/// state returns [`SchedulerError::ExecutionAlreadyExists`]. If the existing
/// execution is in a terminal state (completed / failed / cancelled), it is
/// reset and re-queued — useful for manual retries without changing the ID.
///
/// Choose IDs that are meaningful and stable: `"onboard-{user_id}"`,
/// `"invoice-{invoice_id}"`. This makes deduplication free — calling `start`
/// twice with the same ID from two concurrent request handlers is safe.
///
/// # Starting executions
///
/// | Method | Payload | Type-safe? |
/// |---|---|---|
/// | [`start`](Self::start) | `serde_json::Value` | No |
/// | [`start_for::<H>`](Self::start_for) | `H::Data` | Yes |
/// | [`start_in_tx`](Self::start_in_tx) | `serde_json::Value` | No, but atomic with caller tx |
/// | [`start_for_in_tx::<H>`](Self::start_for_in_tx) | `H::Data` | Yes, atomic with caller tx |
///
/// # Waiting for results
///
/// | Method | Returns |
/// |---|---|
/// | [`wait`](Self::wait) | Raw [`ExecutionRecord`] |
/// | [`wait_for::<H>`](Self::wait_for) | `H::Output` (typed) |
/// | [`wait_completion::<T>`](Self::wait_completion) | `T` (generic) |
///
/// Waiting polls the database on a short interval up to the specified
/// timeout. For production use, prefer event-driven notification (webhooks,
/// SSE) over long-polling `wait`.
///
/// # Example
///
/// ```rust,ignore
/// use zart::{DurableScheduler, prelude::*};
///
/// let sched = DurableScheduler::new(scheduler.clone());
///
/// // Fire and forget.
/// sched
///     .start_for::<OnboardUser>("onboard-alice", "onboard-user", &user_id)
///     .await?;
///
/// // Fire and wait (e.g., in a background job that needs the result).
/// let workspace_id = sched
///     .wait_for::<OnboardUser>("onboard-alice", Duration::from_secs(30), None)
///     .await?;
/// ```
pub struct DurableScheduler {
    storage: Arc<dyn StorageBackend>,
    scheduler: Arc<dyn TaskScheduler>,
    pause_service: PauseService,
    execution_service: ExecutionService,
}

impl DurableScheduler {
    /// Create a new `DurableScheduler`. Pause/resume is always enabled.
    pub fn new(storage: Arc<dyn StorageBackend>, scheduler: Arc<dyn TaskScheduler>) -> Self {
        let execution_service = ExecutionService::new(storage.clone());
        let pause_service = PauseService::new(storage.clone());
        Self {
            storage,
            scheduler,
            pause_service,
            execution_service,
        }
    }

    /// Create a `DurableScheduler` from any [`Backend`] implementation.
    ///
    /// This is the recommended production path. Pause/resume is always enabled.
    ///
    /// ```rust,no_run
    /// # async fn example() {
    /// use sqlx::PgPool;
    /// use zart::{DurableScheduler, postgres::PgBackend};
    ///
    /// let pool = PgPool::connect("postgres://localhost/mydb").await.unwrap();
    /// let pg = PgBackend::new(pool);
    /// let sched = DurableScheduler::from_backend(&pg);
    /// # }
    /// ```
    pub fn from_backend(backend: &impl Backend) -> Self {
        Self::new(backend.storage(), backend.scheduler())
    }

    /// Start a new durable execution with a raw JSON payload.
    ///
    /// If an execution with this ID already exists and is in a terminal state
    /// (completed, failed, cancelled), it will be reset to "scheduled" so it
    /// can be retried. If it exists and is **not** in a terminal state,
    /// [`SchedulerError::ExecutionAlreadyExists`] is returned.
    ///
    /// For first-time executions, the execution record and root body task are
    /// inserted in a single database transaction so that a crash between the
    /// two operations cannot leave an execution record with no scheduled task.
    ///
    /// To include the execution in your own caller-owned transaction, use
    /// [`Self::start_in_tx`] instead.
    pub async fn start(
        &self,
        execution_id: &str,
        task_name: &str,
        payload: serde_json::Value,
    ) -> Result<ScheduleResult, SchedulerError> {
        // Check if execution already exists (read-only, outside the transaction).
        let existing = self.storage.get_execution(execution_id).await?;
        let run_id: String;
        let reset_mode;

        match existing {
            Some(ref record) => {
                match record.status {
                    // Still running — don't create a duplicate.
                    ExecutionStatus::Scheduled | ExecutionStatus::Running => {
                        return Err(SchedulerError::ExecutionAlreadyExists(
                            execution_id.to_string(),
                            record.status.clone(),
                        ));
                    }
                    // Terminal state — reset so we can retry.
                    ExecutionStatus::Completed
                    | ExecutionStatus::Failed
                    | ExecutionStatus::Cancelled => {
                        // reset_execution returns the new run_id directly.
                        run_id = self
                            .storage
                            .reset_execution(execution_id, payload.clone())
                            .await?;
                        reset_mode = true;
                    }
                }
            }
            None => {
                run_id = format!("{execution_id}:run:0");
                reset_mode = false;
            }
        }

        // Schedule the root task. For first-time executions, the execution
        // record and body task are created in a single transaction.
        // The task_id is "{run_id}:body:start" — deterministic and debuggable.
        let task_id = format!("{run_id}:body:start");

        let params = ScheduleAtParams {
            task_id: task_id.clone(),
            task_name: crate::TASK_NAME.to_string(),
            execution_time: chrono::Utc::now(),
            data: payload.clone(),
            recurrence: None,
            metadata: TaskMetadata::body(&run_id, execution_id).to_json_value(),
        };

        if reset_mode {
            // For reset (retried) executions, just schedule the task.
            return self
                .scheduler
                .schedule_at(params)
                .await
                .map_err(SchedulerError::Database);
        }

        // First-time execution: use a single transaction to atomically
        // create the execution record and schedule the root task.
        let mut conn = self.scheduler.begin().await?;

        self.storage
            .start_execution_in_tx(&mut conn, execution_id, task_name, payload)
            .await?;

        let result = self.scheduler.schedule_at_in_tx(&mut conn, params).await?;

        conn.commit().await.map_err(|e| {
            SchedulerError::Database(zart_scheduler::StorageError::Database(Box::new(e)))
        })?;

        Ok(result)
    }

    /// Return the current status of a durable execution.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::ExecutionNotFound`] if no execution with the
    /// given ID exists.
    pub async fn status(&self, execution_id: &str) -> Result<ExecutionRecord, SchedulerError> {
        self.storage
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
        Ok(self.storage.get_current_run_id(execution_id).await?)
    }

    /// List all runs for a durable execution.
    pub async fn list_runs(
        &self,
        execution_id: &str,
    ) -> Result<Vec<zart_core::types::ExecutionRunRecord>, SchedulerError> {
        Ok(self.storage.list_runs(execution_id).await?)
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
        Ok(self.storage.cancel_execution(execution_id).await?)
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
            .storage
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
        params: ListExecutionsParams,
    ) -> Result<Vec<ExecutionRecord>, SchedulerError> {
        Ok(self.storage.list_executions(params).await?)
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

    // ── Transactional scheduling ──────────────────────────────────────────────

    /// Start a durable execution within the caller's transaction.
    ///
    /// The caller is responsible for committing or rolling back the transaction.
    /// If the transaction rolls back, no execution record or body task will exist.
    ///
    /// This enables atomic coordination between user database writes and
    /// durable execution scheduling. For example, inserting a user row and
    /// starting an onboarding execution in the same transaction.
    ///
    /// # Errors
    ///
    /// - [`SchedulerError::NotSupported`] if the execution already exists (reset-in-tx
    ///   is not supported in V1; use the non-transactional `start` for resets).
    /// - [`SchedulerError::Database`] if the storage backend fails.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let mut tx = pool.begin().await?;
    ///
    /// sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
    ///     .bind(user_id).bind(&email)
    ///     .execute(&mut *tx)
    ///     .await?;
    ///
    /// sched.start_in_tx(
    ///     &mut tx,
    ///     &format!("onboard-{user_id}"),
    ///     "onboarding",
    ///     json!({ "user_id": user_id }),
    /// ).await?;
    ///
    /// tx.commit().await?; // both operations commit atomically
    /// ```
    pub async fn start_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        execution_id: &str,
        task_name: &str,
        payload: serde_json::Value,
    ) -> Result<ScheduleResult, SchedulerError> {
        // Check if execution already exists — if so, we don't support reset-in-tx in V1.
        if let Some(_existing) = self.storage.get_execution(execution_id).await? {
            return Err(SchedulerError::NotSupported(
                "start_in_tx does not support resetting existing executions; use start() instead",
            ));
        }

        let run_id = format!("{execution_id}:run:0");
        let task_id = format!("{run_id}:body:start");

        let params = ScheduleAtParams {
            task_id: task_id.clone(),
            task_name: crate::TASK_NAME.to_string(),
            execution_time: chrono::Utc::now(),
            data: payload.clone(),
            recurrence: None,
            metadata: TaskMetadata::body(&run_id, execution_id).to_json_value(),
        };

        self.storage
            .start_execution_in_tx(tx, execution_id, task_name, payload)
            .await?;

        self.scheduler
            .schedule_at_in_tx(tx, params)
            .await
            .map_err(SchedulerError::Database)
    }

    /// Typed wrapper around [`Self::start_in_tx`].
    ///
    /// Infers the input type from `H::Data`. See [`Self::start_in_tx`] for
    /// the transaction ownership contract and error semantics.
    ///
    /// # Why no `start_and_wait_for_in_tx`?
    ///
    /// The transaction must be committed before waiting for completion makes
    /// sense (the worker can't poll an uncommitted task). The pattern is:
    ///
    /// ```rust,ignore
    /// sched.start_for_in_tx::<MyHandler>(&mut tx, id, task, &input).await?;
    /// tx.commit().await?;
    /// let result = sched.wait_for::<MyHandler>(id, timeout).await?;
    /// ```
    pub async fn start_for_in_tx<H: DurableExecution>(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        execution_id: &str,
        task_name: &str,
        input: &H::Data,
    ) -> Result<ScheduleResult, SchedulerError> {
        let payload = serde_json::to_value(input)?;
        self.start_in_tx(tx, execution_id, task_name, payload).await
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

    // ── Stats ────────────────────────────────────────────────────────────────

    /// Return aggregate execution counts grouped by status.
    pub async fn stats(&self) -> Result<zart_core::types::ExecutionStats, SchedulerError> {
        Ok(self.storage.execution_stats().await?)
    }

    /// Return full detail for an execution: the record, all runs, and the
    /// steps (with attempt history) for the specified run.
    ///
    /// If `run_id` is `None` the current (latest) run is used.
    pub async fn execution_detail(
        &self,
        execution_id: &str,
        run_id: Option<&str>,
    ) -> Result<crate::admin::ExecutionDetail, SchedulerError> {
        self.execution_service
            .execution_detail(execution_id, run_id)
            .await
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
    /// - [`SchedulerError::Database`] wrapping `StorageError::StepNotFound` if the step doesn't exist
    /// - [`SchedulerError::Database`] wrapping `StorageError::StepStatusMismatch` if not dead
    /// - [`SchedulerError::Database`] if the storage backend fails
    pub async fn retry_step(
        &self,
        run_id: &str,
        step_name: &str,
        triggered_by: Option<&str>,
    ) -> Result<String, SchedulerError> {
        self.execution_service
            .retry_step(run_id, step_name, triggered_by)
            .await
    }

    /// Convenience wrapper around [`Self::retry_step`] that resolves the
    /// current run ID automatically.
    pub async fn retry_step_current_run(
        &self,
        execution_id: &str,
        step_name: &str,
        triggered_by: Option<&str>,
    ) -> Result<String, SchedulerError> {
        self.execution_service
            .retry_step_current_run(execution_id, step_name, triggered_by)
            .await
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
        self.execution_service
            .restart(execution_id, new_payload, triggered_by)
            .await
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
        spec: RerunSpec,
    ) -> Result<RerunResult, SchedulerError> {
        self.execution_service.rerun_steps(execution_id, spec).await
    }

    // ── Pause / Resume ────────────────────────────────────────────────────────

    /// Create a pause rule.
    pub async fn pause(&self, scope: PauseScope) -> Result<PauseRule, SchedulerError> {
        self.pause_service.pause(scope).await
    }

    /// Resume execution by soft-deleting matching pause rules.
    pub async fn resume(&self, scope: PauseScope) -> Result<ResumeResult, SchedulerError> {
        self.pause_service.resume(scope).await
    }

    /// Resume by deleting a specific pause rule by ID.
    pub async fn resume_rule_by_id(
        &self,
        rule_id: &str,
        deleted_by: Option<&str>,
    ) -> Result<bool, SchedulerError> {
        self.pause_service
            .resume_rule_by_id(rule_id, deleted_by)
            .await
    }

    /// List active pause rules.
    pub async fn list_pause_rules(
        &self,
        filter: Option<PauseRuleFilter>,
    ) -> Result<Vec<PauseRule>, SchedulerError> {
        self.pause_service.list_rules(filter).await
    }
}
