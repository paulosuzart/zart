//! Service layer for execution admin operations.
//!
//! [`ExecutionService`] owns the business logic for admin operations — step
//! retry, execution restart, selective step rerun, and execution detail
//! assembly. It delegates to the raw SQL primitives on [`StorageBackend`]
//! and maps storage errors to [`SchedulerError`].
//!
//! The separation from [`DurableScheduler`] means that business rules (e.g.
//! effective-rerun computation) live here in Rust, while the storage layer
//! stays free of workflow policy.
//!
//! [`DurableScheduler`]: crate::durable::DurableScheduler

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::store::StorageBackend;
use zart_core::types::StepStatus;

use crate::admin::{ExecutionDetail, RerunResult, RerunSpec, StepWithAttempts};
use crate::error::SchedulerError;

/// Provides business-level admin operations over a [`StorageBackend`].
///
/// Callers obtain an instance via [`ExecutionService::new`] and then use it
/// through [`crate::durable::DurableScheduler`], which holds one internally.
pub struct ExecutionService {
    storage: Arc<dyn StorageBackend>,
}

impl ExecutionService {
    /// Create an `ExecutionService` backed by the given storage.
    pub fn new(storage: Arc<dyn StorageBackend>) -> Self {
        Self { storage }
    }

    /// Retry a single dead step within the given run.
    ///
    /// Delegates to `ExecutionStore::retry_dead_step` after resolving any
    /// run-level concerns. Returns the new task ID for the retried step.
    ///
    /// # Errors
    ///
    /// - [`SchedulerError::Database`] wrapping `StorageError::StepNotFound`
    ///   if no step exists with the given name.
    /// - [`SchedulerError::Database`] wrapping `StorageError::StepStatusMismatch`
    ///   if the step is not in `dead` status.
    pub async fn retry_step(
        &self,
        run_id: &str,
        step_name: &str,
        triggered_by: Option<&str>,
    ) -> Result<String, SchedulerError> {
        Ok(self
            .storage
            .retry_dead_step(run_id, step_name, triggered_by)
            .await?)
    }

    /// Retry a dead step by execution ID, resolving the current run automatically.
    ///
    /// # Errors
    ///
    /// - [`SchedulerError::ExecutionNotFound`] if no run exists for the execution.
    pub async fn retry_step_current_run(
        &self,
        execution_id: &str,
        step_name: &str,
        triggered_by: Option<&str>,
    ) -> Result<String, SchedulerError> {
        let run_id = self
            .storage
            .get_current_run_id(execution_id)
            .await?
            .ok_or_else(|| SchedulerError::ExecutionNotFound(execution_id.to_string()))?;
        self.retry_step(&run_id, step_name, triggered_by).await
    }

    /// Restart an entire execution from scratch.
    ///
    /// Archives the current run, creates a new run with `trigger = 'restart'`,
    /// and schedules a fresh body task.
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
            .storage
            .restart_run(execution_id, new_payload, "restart", triggered_by)
            .await?)
    }

    /// Selectively rerun a subset of steps while preserving others.
    ///
    /// Business logic (effective-rerun computation) runs here in Rust:
    /// - Dead steps are always rerun regardless of `spec.preserve`.
    /// - Steps in `spec.force_rerun` are rerun even if completed.
    /// - Steps in `spec.preserve` that are `completed` are carried forward.
    ///
    /// After computing the effective-rerun set the method calls
    /// `ExecutionStore::restart_run` with `trigger = 'selective_rerun'`.
    ///
    /// # Returns
    ///
    /// A [`RerunResult`] with the new run number and the effective rerun set.
    pub async fn rerun_steps(
        &self,
        execution_id: &str,
        spec: RerunSpec,
    ) -> Result<RerunResult, SchedulerError> {
        let run_id = self
            .storage
            .get_current_run_id(execution_id)
            .await?
            .ok_or_else(|| SchedulerError::ExecutionNotFound(execution_id.to_string()))?;

        let steps = self.storage.list_steps(&run_id).await?;

        // Always rerun dead steps; add force_rerun on top.
        let mut effective_rerun: HashSet<String> = spec.force_rerun.iter().cloned().collect();
        for step in &steps {
            if step.status == StepStatus::Dead {
                effective_rerun.insert(step.step_name.clone());
            }
        }

        // Remove preserved steps that are completed (dead steps can't be preserved).
        let preserve_set: HashSet<&str> = spec.preserve.iter().map(|s| s.as_str()).collect();
        let step_status_map: HashMap<&str, &StepStatus> = steps
            .iter()
            .map(|s| (s.step_name.as_str(), &s.status))
            .collect();
        for p in &preserve_set {
            if let Some(status) = step_status_map.get(p)
                && **status == StepStatus::Completed
            {
                effective_rerun.remove(*p);
            }
        }

        let new_run_id = self
            .storage
            .restart_run(
                execution_id,
                None,
                "selective_rerun",
                spec.triggered_by.as_deref(),
            )
            .await?;

        let run_number = new_run_id
            .rsplit_once(":run:")
            .and_then(|(_, n)| n.parse().ok())
            .unwrap_or(0);

        Ok(RerunResult {
            new_run_number: run_number,
            effective_rerun: effective_rerun.into_iter().collect(),
        })
    }

    /// Return full detail for an execution: the record, all runs, and the
    /// steps (with attempt history) for the specified run.
    ///
    /// If `run_id` is `None` the current (latest) run is used.
    pub async fn execution_detail(
        &self,
        execution_id: &str,
        run_id: Option<&str>,
    ) -> Result<ExecutionDetail, SchedulerError> {
        let execution = self
            .storage
            .get_execution(execution_id)
            .await?
            .ok_or_else(|| SchedulerError::ExecutionNotFound(execution_id.to_string()))?;

        let runs = self.storage.list_runs(execution_id).await?;

        let effective_run_id = match run_id {
            Some(id) => id.to_string(),
            None => match self.storage.get_current_run_id(execution_id).await? {
                Some(id) => id,
                None => {
                    return Ok(ExecutionDetail {
                        execution,
                        runs,
                        steps: vec![],
                    });
                }
            },
        };

        let step_rows = self.storage.list_steps(&effective_run_id).await?;
        let attempts = self.storage.list_step_attempts(&effective_run_id).await?;

        let steps: Vec<StepWithAttempts> = step_rows
            .into_iter()
            .map(|step| {
                let step_attempts: Vec<_> = attempts
                    .iter()
                    .filter(|a| a.step_id == step.step_id)
                    .cloned()
                    .collect();
                let retryable = step.status == zart_core::types::StepStatus::Dead;
                StepWithAttempts {
                    step,
                    attempts: step_attempts,
                    retryable,
                }
            })
            .collect();

        Ok(ExecutionDetail {
            execution,
            runs,
            steps,
        })
    }
}
