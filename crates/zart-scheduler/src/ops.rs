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
        let tx = self.scheduler.begin().await?;
        self.complete_in_tx(tx, result, schedule_next).await
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
        let mut earliest_next: Option<DateTime<Utc>> = None;
        for params in schedule_next {
            earliest_next =
                Some(earliest_next.map_or(params.execution_time, |e| e.min(params.execution_time)));
            self.scheduler.schedule_at_in_tx(&mut tx, params).await?;
        }
        tx.commit()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;
        maybe_notify_local_wakeup(self.scheduler.as_ref(), earliest_next);
        Ok(())
    }

    /// Reschedule the task: opens a fresh transaction, updates task state,
    /// then commits.
    ///
    /// If [`set_metadata`](Self::set_metadata) was called, the new metadata is
    /// merged into the task's `metadata` column using the Postgres `||` operator.
    pub async fn reschedule(&self, at: DateTime<Utc>) -> Result<(), StorageError> {
        let tx = self.scheduler.begin().await?;
        self.reschedule_in_tx(tx, at).await
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
        maybe_notify_local_wakeup(self.scheduler.as_ref(), Some(at));
        Ok(())
    }
}

fn maybe_notify_local_wakeup(scheduler: &dyn TaskScheduler, earliest: Option<DateTime<Utc>>) {
    if earliest.is_some_and(|t| t <= Utc::now()) {
        scheduler.notify_local_wakeup();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use chrono::Duration;

    use super::*;
    use crate::{FetchedTask, ScheduleResult};

    struct CountingScheduler {
        wakeups: AtomicUsize,
    }

    impl CountingScheduler {
        fn new() -> Self {
            Self {
                wakeups: AtomicUsize::new(0),
            }
        }

        fn count(&self) -> usize {
            self.wakeups.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl TaskScheduler for CountingScheduler {
        async fn schedule_now(
            &self,
            _task_id: &str,
            _task_name: &str,
            _data: Value,
        ) -> Result<ScheduleResult, StorageError> {
            unreachable!()
        }

        async fn schedule_at(
            &self,
            _params: ScheduleAtParams,
        ) -> Result<ScheduleResult, StorageError> {
            unreachable!()
        }

        async fn poll_due(
            &self,
            _now: DateTime<Utc>,
            _limit: usize,
        ) -> Result<Vec<FetchedTask>, StorageError> {
            unreachable!()
        }

        async fn update_task_state(
            &self,
            _task_id: &str,
            _state: Value,
            _next_execution_time: DateTime<Utc>,
            _lock_token: &str,
        ) -> Result<(), StorageError> {
            unreachable!()
        }

        async fn mark_completed(
            &self,
            _task_id: &str,
            _result: Option<Value>,
            _lock_token: &str,
        ) -> Result<(), StorageError> {
            unreachable!()
        }

        async fn mark_completed_in_tx(
            &self,
            _conn: &mut sqlx::PgConnection,
            _task_id: &str,
            _result: Option<Value>,
            _lock_token: &str,
        ) -> Result<(), StorageError> {
            unreachable!()
        }

        async fn mark_failed(
            &self,
            _task_id: &str,
            _error: &str,
            _next_execution_time: Option<DateTime<Utc>>,
            _lock_token: &str,
        ) -> Result<(), StorageError> {
            unreachable!()
        }

        async fn mark_failed_in_tx(
            &self,
            _conn: &mut sqlx::PgConnection,
            _task_id: &str,
            _error: &str,
            _next_execution_time: Option<DateTime<Utc>>,
            _lock_token: &str,
        ) -> Result<(), StorageError> {
            unreachable!()
        }

        async fn cancel_task(&self, _task_id: &str) -> Result<bool, StorageError> {
            unreachable!()
        }

        async fn delete_task(&self, _task_id: &str) -> Result<(), StorageError> {
            unreachable!()
        }

        fn notify_local_wakeup(&self) {
            self.wakeups.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn maybe_notify_skips_none_and_future() {
        let scheduler = CountingScheduler::new();
        maybe_notify_local_wakeup(&scheduler, None);
        maybe_notify_local_wakeup(&scheduler, Some(Utc::now() + Duration::seconds(60)));
        assert_eq!(scheduler.count(), 0);
    }

    #[test]
    fn maybe_notify_fires_for_due_or_past() {
        let scheduler = CountingScheduler::new();
        maybe_notify_local_wakeup(&scheduler, Some(Utc::now() - Duration::seconds(1)));
        maybe_notify_local_wakeup(&scheduler, Some(Utc::now()));
        assert_eq!(scheduler.count(), 2);
    }

    #[tokio::test]
    async fn complete_and_reschedule_do_not_notify_when_begin_fails() {
        let scheduler = Arc::new(CountingScheduler::new());
        let ops = ExecutionOps::new(scheduler.clone(), "t1", "tok1");

        assert!(ops.complete(None, vec![]).await.is_err());
        assert!(ops.reschedule(Utc::now()).await.is_err());
        assert_eq!(scheduler.count(), 0);
    }
}
