//! `TaskScheduler` implementation for [`PostgresStorage`] — pure delegation.
//!
//! No task-queue SQL lives in this file. Every method forwards to the internal
//! [`zart_scheduler::PostgresTaskScheduler`] held by `PostgresStorage`. This
//! satisfies the `StorageBackend: TaskScheduler` supertrait bound without
//! duplicating any task-queue logic in the `zart` crate.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::PgConnection;
use zart_core::StorageError;
use zart_core::store::TaskScheduler;
use zart_core::types::{CompleteAndScheduleParams, FetchedTask, ScheduleAtParams, ScheduleResult};

use super::PostgresStorage;

#[async_trait]
impl TaskScheduler for PostgresStorage {
    async fn schedule_now(
        &self,
        task_id: &str,
        task_name: &str,
        data: serde_json::Value,
    ) -> Result<ScheduleResult, StorageError> {
        self.task_scheduler
            .schedule_now(task_id, task_name, data)
            .await
    }

    async fn schedule_at(&self, params: ScheduleAtParams) -> Result<ScheduleResult, StorageError> {
        self.task_scheduler.schedule_at(params).await
    }

    async fn schedule_at_in_tx(
        &self,
        conn: &mut PgConnection,
        params: ScheduleAtParams,
    ) -> Result<ScheduleResult, StorageError> {
        self.task_scheduler.schedule_at_in_tx(conn, params).await
    }

    async fn poll_due(
        &self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<FetchedTask>, StorageError> {
        self.task_scheduler.poll_due(now, limit).await
    }

    async fn update_task_state(
        &self,
        task_id: &str,
        state: serde_json::Value,
        next_execution_time: DateTime<Utc>,
        lock_token: &str,
    ) -> Result<(), StorageError> {
        self.task_scheduler
            .update_task_state(task_id, state, next_execution_time, lock_token)
            .await
    }

    async fn mark_completed(
        &self,
        task_id: &str,
        result: Option<serde_json::Value>,
        lock_token: &str,
    ) -> Result<(), StorageError> {
        self.task_scheduler
            .mark_completed(task_id, result, lock_token)
            .await
    }

    async fn mark_completed_in_tx(
        &self,
        conn: &mut PgConnection,
        task_id: &str,
        result: Option<serde_json::Value>,
        lock_token: &str,
    ) -> Result<(), StorageError> {
        self.task_scheduler
            .mark_completed_in_tx(conn, task_id, result, lock_token)
            .await
    }

    async fn mark_failed(
        &self,
        task_id: &str,
        error: &str,
        next_execution_time: Option<DateTime<Utc>>,
        lock_token: &str,
    ) -> Result<(), StorageError> {
        self.task_scheduler
            .mark_failed(task_id, error, next_execution_time, lock_token)
            .await
    }

    async fn cancel_task(&self, task_id: &str) -> Result<bool, StorageError> {
        self.task_scheduler.cancel_task(task_id).await
    }

    async fn delete_task(&self, task_id: &str) -> Result<(), StorageError> {
        self.task_scheduler.delete_task(task_id).await
    }

    async fn run_migrations(&self) -> Result<(), StorageError> {
        self.task_scheduler.run_migrations().await
    }

    async fn begin(&self) -> Result<sqlx::Transaction<'_, sqlx::Postgres>, StorageError> {
        self.task_scheduler.begin().await
    }

    async fn recover_orphans(
        &self,
        stale_timeout: std::time::Duration,
    ) -> Result<usize, StorageError> {
        self.task_scheduler.recover_orphans(stale_timeout).await
    }

    async fn renew_lease(&self, task_id: &str, lock_token: &str) -> Result<bool, StorageError> {
        self.task_scheduler.renew_lease(task_id, lock_token).await
    }

    async fn complete_and_schedule(
        &self,
        params: CompleteAndScheduleParams,
    ) -> Result<(), StorageError> {
        self.task_scheduler.complete_and_schedule(params).await
    }
}
