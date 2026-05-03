//! Execution operations available to a [`CompletionHandler`](crate::CompletionHandler).
//!
//! `ExecutionOps` is constructed by the worker and passed to
//! [`CompletionHandler::complete`](crate::CompletionHandler::complete).
//! It provides granular bookkeeping methods that handlers call to persist
//! task outcomes. Handlers may open fresh transactions or reuse one they
//! already hold.

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::Postgres;
use std::sync::Arc;

use crate::error::StorageError;
use crate::store::TaskScheduler;
use crate::types::ScheduleAtParams;

/// Scheduler operations for completing or rescheduling a task.
///
/// Constructed by the worker after [`ScheduledTask::execute`](crate::ScheduledTask::execute)
/// returns a [`CompletionHandler`](crate::CompletionHandler). The handler calls
/// these methods — rather than the scheduler directly — to perform bookkeeping.
///
/// # Transaction handling
///
/// - `complete` / `reschedule` open a fresh transaction internally and commit.
/// - `complete_in_tx` / `reschedule_in_tx` accept an already-open transaction,
///   append bookkeeping writes, then commit.
pub struct ExecutionOps {
    scheduler: Arc<dyn TaskScheduler>,
    task_id: String,
    lock_token: String,
    pending_metadata: Option<Value>,
}

impl ExecutionOps {
    pub(crate) fn new(scheduler: Arc<dyn TaskScheduler>, task_id: &str, lock_token: &str) -> Self {
        Self {
            scheduler,
            task_id: task_id.to_string(),
            lock_token: lock_token.to_string(),
            pending_metadata: None,
        }
    }

    /// Merge `v` into the task's `metadata` column on the next reschedule.
    ///
    /// New keys in `v` are added to existing metadata; existing keys are
    /// overwritten. The merge happens atomically with the reschedule SQL update
    /// using the Postgres `||` operator.
    ///
    /// This is a pure setter — calling it multiple times replaces the previous
    /// value. The metadata is only applied when `reschedule` or `reschedule_in_tx`
    /// is called.
    ///
    /// **Note:** calling `set_metadata` before `complete` or `complete_in_tx`
    /// has **no effect** — the pending metadata is only flushed on the
    /// `reschedule`/`reschedule_in_tx` paths.
    pub fn set_metadata(&mut self, v: Value) {
        self.pending_metadata = Some(v);
    }

    /// Complete the task: opens a fresh transaction, marks complete,
    /// schedules any follow-up tasks, then commits.
    pub async fn complete(
        &self,
        result: Option<Value>,
        schedule_next: Vec<ScheduleAtParams>,
    ) -> Result<(), StorageError> {
        let mut tx = self.scheduler.begin().await?;
        self.scheduler
            .mark_completed_in_tx(&mut tx, &self.task_id, result, &self.lock_token)
            .await?;
        for params in schedule_next {
            self.scheduler.schedule_at_in_tx(&mut tx, params).await?;
        }
        tx.commit()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;
        Ok(())
    }

    /// Complete using a provided transaction.
    ///
    /// Appends `mark_completed_in_tx` and any `schedule_at_in_tx` entries,
    /// then commits. The caller must not use `tx` after this call.
    pub async fn complete_in_tx(
        &self,
        mut tx: sqlx::Transaction<'static, Postgres>,
        result: Option<Value>,
        schedule_next: Vec<ScheduleAtParams>,
    ) -> Result<(), StorageError> {
        self.scheduler
            .mark_completed_in_tx(&mut tx, &self.task_id, result, &self.lock_token)
            .await?;
        for params in schedule_next {
            self.scheduler.schedule_at_in_tx(&mut tx, params).await?;
        }
        tx.commit()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;
        Ok(())
    }

    /// Reschedule the task: opens a fresh transaction, updates task state,
    /// then commits.
    ///
    /// If [`set_metadata`](Self::set_metadata) was called, the new metadata is
    /// merged into the task's `metadata` column using the Postgres `||` operator.
    pub async fn reschedule(&self, at: DateTime<Utc>) -> Result<(), StorageError> {
        let mut tx = self.scheduler.begin().await?;
        self.scheduler
            .update_task_state_in_tx(
                &mut tx,
                &self.task_id,
                Value::Null,
                at,
                &self.lock_token,
                self.pending_metadata.as_ref(),
            )
            .await?;
        tx.commit()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;
        Ok(())
    }

    /// Reschedule using a provided transaction.
    ///
    /// Appends the state update then commits.
    pub async fn reschedule_in_tx(
        &self,
        mut tx: sqlx::Transaction<'static, Postgres>,
        at: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        self.scheduler
            .update_task_state_in_tx(
                &mut tx,
                &self.task_id,
                Value::Null,
                at,
                &self.lock_token,
                self.pending_metadata.as_ref(),
            )
            .await?;
        tx.commit()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;
        Ok(())
    }
}
