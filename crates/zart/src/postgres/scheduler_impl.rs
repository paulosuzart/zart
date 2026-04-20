//! Implementation of [`TaskScheduler`] for [`PostgresStorage`].

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::PgConnection;
use uuid::Uuid;
use zart_core::StorageError;
use zart_scheduler::{
    CompleteAndScheduleParams, FetchedTask, ScheduleAtParams, ScheduleResult, TaskScheduler,
    TaskStatus,
};

use super::PostgresStorage;
use super::sql_helpers::schedule_at_sql;

#[async_trait]
impl TaskScheduler for PostgresStorage {
    async fn schedule_now(
        &self,
        task_id: &str,
        task_name: &str,
        data: serde_json::Value,
    ) -> Result<ScheduleResult, StorageError> {
        self.schedule_at(ScheduleAtParams {
            task_id: task_id.to_string(),
            task_name: task_name.to_string(),
            execution_time: Utc::now(),
            data,
            recurrence: None,
            metadata: serde_json::Value::Null,
        })
        .await
    }

    async fn schedule_at(&self, params: ScheduleAtParams) -> Result<ScheduleResult, StorageError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;

        let result = schedule_at_sql(&mut tx, &params, &self.table_names).await?;

        tx.commit()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;
        Ok(result)
    }

    async fn schedule_at_in_tx(
        &self,
        conn: &mut PgConnection,
        params: ScheduleAtParams,
    ) -> Result<ScheduleResult, StorageError> {
        schedule_at_sql(conn, &params, &self.table_names).await
    }

    async fn poll_due(
        &self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<FetchedTask>, StorageError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;

        let rows: Vec<(
            String,
            String,
            serde_json::Value,
            serde_json::Value,
            i32,
            Option<serde_json::Value>,
            serde_json::Value,
        )> = sqlx::query_as(&format!(
            r#"
                SELECT task_id, task_name, data, state, attempt, recurrence, metadata
                FROM {tasks}
                WHERE status = 'scheduled'
                  AND execution_time <= $1
                ORDER BY execution_time ASC
                LIMIT $2
                FOR UPDATE SKIP LOCKED
                "#,
            tasks = self.table_names.tasks(),
        ))
        .bind(now)
        .bind(limit as i64)
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        if rows.is_empty() {
            tx.rollback()
                .await
                .map_err(|e| StorageError::Database(Box::new(e)))?;
            return Ok(vec![]);
        }

        let mut fetched = Vec::with_capacity(rows.len());

        for (task_id, task_name, data, state, attempt, recurrence_json, metadata) in rows {
            let lock_token = Uuid::new_v4().to_string();

            sqlx::query(&format!(
                r#"
                UPDATE {tasks}
                SET status     = 'picked_up',
                    locked_at  = NOW(),
                    worker_id  = $1,
                    attempt    = attempt + 1,
                    updated_at = NOW()
                WHERE task_id = $2
                "#,
                tasks = self.table_names.tasks(),
            ))
            .bind(&lock_token)
            .bind(&task_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;

            let recurrence = recurrence_json.and_then(|v| serde_json::from_value(v).ok());

            fetched.push(FetchedTask {
                task_id,
                task_name,
                data,
                state,
                attempt: attempt as usize + 1,
                lock_token,
                recurrence,
                metadata,
            });
        }

        tx.commit()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;

        Ok(fetched)
    }

    async fn update_task_state(
        &self,
        task_id: &str,
        state: serde_json::Value,
        next_execution_time: DateTime<Utc>,
        lock_token: &str,
    ) -> Result<(), StorageError> {
        let rows_affected = sqlx::query(&format!(
            r#"
            UPDATE {tasks}
            SET state          = $1,
                execution_time = $2,
                status         = 'scheduled',
                locked_at      = NULL,
                worker_id      = NULL,
                updated_at     = NOW()
            WHERE task_id  = $3
              AND worker_id = $4
            "#,
            tasks = self.table_names.tasks(),
        ))
        .bind(&state)
        .bind(next_execution_time)
        .bind(task_id)
        .bind(lock_token)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?
        .rows_affected();

        if rows_affected == 0 {
            return Err(StorageError::LockMismatch(task_id.to_string()));
        }
        Ok(())
    }

    async fn mark_completed(
        &self,
        task_id: &str,
        result: Option<serde_json::Value>,
        lock_token: &str,
    ) -> Result<(), StorageError> {
        let rows_affected = sqlx::query(&format!(
            r#"
            UPDATE {tasks}
            SET status       = 'completed',
                result       = $1,
                completed_at = NOW(),
                updated_at   = NOW(),
                locked_at    = NULL,
                worker_id    = NULL
            WHERE task_id  = $2
              AND worker_id = $3
            "#,
            tasks = self.table_names.tasks(),
        ))
        .bind(&result)
        .bind(task_id)
        .bind(lock_token)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?
        .rows_affected();

        if rows_affected == 0 {
            return Err(StorageError::LockMismatch(task_id.to_string()));
        }
        Ok(())
    }

    async fn mark_failed(
        &self,
        task_id: &str,
        error: &str,
        next_execution_time: Option<DateTime<Utc>>,
        lock_token: &str,
    ) -> Result<(), StorageError> {
        let (new_status, exec_time) = match next_execution_time {
            Some(t) => (TaskStatus::Scheduled, Some(t)),
            None => (TaskStatus::Failed, None),
        };

        let rows_affected = if let Some(t) = exec_time {
            sqlx::query(&format!(
                r#"
                UPDATE {tasks}
                SET status         = $1,
                    last_error     = $2,
                    execution_time = $3,
                    locked_at      = NULL,
                    worker_id      = NULL,
                    updated_at     = NOW()
                WHERE task_id  = $4
                  AND worker_id = $5
                "#,
                tasks = self.table_names.tasks(),
            ))
            .bind(new_status)
            .bind(error)
            .bind(t)
            .bind(task_id)
            .bind(lock_token)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?
            .rows_affected()
        } else {
            sqlx::query(&format!(
                r#"
                UPDATE {tasks}
                SET status     = $1,
                    last_error = $2,
                    locked_at  = NULL,
                    worker_id  = NULL,
                    updated_at = NOW()
                WHERE task_id  = $3
                  AND worker_id = $4
                "#,
                tasks = self.table_names.tasks(),
            ))
            .bind(new_status)
            .bind(error)
            .bind(task_id)
            .bind(lock_token)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?
            .rows_affected()
        };

        if rows_affected == 0 {
            return Err(StorageError::LockMismatch(task_id.to_string()));
        }
        Ok(())
    }

    async fn cancel_task(&self, task_id: &str) -> Result<bool, StorageError> {
        let rows_affected = sqlx::query(&format!(
            r#"
            UPDATE {tasks}
            SET status = 'cancelled', updated_at = NOW()
            WHERE task_id = $1 AND status = 'scheduled'
            "#,
            tasks = self.table_names.tasks(),
        ))
        .bind(task_id)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?
        .rows_affected();

        Ok(rows_affected > 0)
    }

    async fn delete_task(&self, task_id: &str) -> Result<(), StorageError> {
        sqlx::query(&format!(
            "DELETE FROM {tasks} WHERE task_id = $1",
            tasks = self.table_names.tasks(),
        ))
        .bind(task_id)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;
        Ok(())
    }

    async fn run_migrations(&self) -> Result<(), StorageError> {
        sqlx::migrate!("../zart-scheduler/migrations")
            .run(&self.pool)
            .await
            .map_err(|e| StorageError::Migration(e.to_string()))
    }

    async fn recover_orphans(
        &self,
        stale_timeout: std::time::Duration,
    ) -> Result<usize, StorageError> {
        let threshold = Utc::now()
            - chrono::Duration::from_std(stale_timeout).unwrap_or(chrono::Duration::seconds(300));

        let result = sqlx::query(&format!(
            r#"
            UPDATE {tasks}
            SET status     = 'scheduled',
                locked_at  = NULL,
                worker_id  = NULL,
                updated_at = NOW()
            WHERE status    = 'picked_up'
              AND locked_at < $1
            "#,
            tasks = self.table_names.tasks(),
        ))
        .bind(threshold)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        Ok(result.rows_affected() as usize)
    }

    async fn renew_lease(&self, task_id: &str, lock_token: &str) -> Result<bool, StorageError> {
        let rows_affected = sqlx::query(&format!(
            r#"
            UPDATE {tasks}
            SET locked_at  = NOW(),
                updated_at = NOW()
            WHERE task_id   = $1
              AND worker_id = $2
              AND status    = 'picked_up'
            "#,
            tasks = self.table_names.tasks(),
        ))
        .bind(task_id)
        .bind(lock_token)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?
        .rows_affected();

        Ok(rows_affected > 0)
    }

    async fn complete_and_schedule(
        &self,
        params: CompleteAndScheduleParams,
    ) -> Result<(), StorageError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;

        let rows = sqlx::query(&format!(
            r#"
            UPDATE {tasks}
            SET status       = 'completed',
                result       = $1,
                completed_at = NOW(),
                updated_at   = NOW(),
                locked_at    = NULL,
                worker_id    = NULL
            WHERE task_id   = $2
              AND worker_id = $3
            "#,
            tasks = self.table_names.tasks(),
        ))
        .bind(&params.result)
        .bind(&params.completed_task_id)
        .bind(&params.lock_token)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?
        .rows_affected();

        if rows == 0 {
            tx.rollback()
                .await
                .map_err(|e| StorageError::Database(Box::new(e)))?;
            return Err(StorageError::LockMismatch(params.completed_task_id));
        }

        sqlx::query(&format!(
            r#"
            INSERT INTO {tasks}
                (task_id, task_name, execution_time, data, metadata)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (task_id) DO NOTHING
            "#,
            tasks = self.table_names.tasks(),
        ))
        .bind(&params.new_task_id)
        .bind(&params.new_task_name)
        .bind(params.new_execution_time)
        .bind(&params.new_data)
        .bind(&params.new_metadata)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        tx.commit()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;
        Ok(())
    }

    async fn begin(&self) -> Result<sqlx::Transaction<'_, sqlx::Postgres>, StorageError> {
        self.pool
            .begin()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))
    }
}
