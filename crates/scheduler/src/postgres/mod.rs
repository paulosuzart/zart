//! PostgreSQL-backed implementation of the [`Scheduler`] trait.
//!
//! Uses `sqlx` with a `PgPool` for connection pooling. Task locking is
//! implemented with `SELECT … FOR UPDATE SKIP LOCKED` so multiple workers
//! can poll concurrently without processing the same task twice.
//!
//! # Migrations
//!
//! Call [`PostgresScheduler::run_migrations`] (or `just migrate`) once before
//! starting workers. It applies the embedded SQL files under `migrations/`.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    DurableStorage, ExecutionRecord, ExecutionStatus, FetchedTask, Recurrence, ScheduleResult,
    Scheduler, StepLookup, StorageError,
};

/// A [`Scheduler`] backed by a PostgreSQL database.
///
/// Create one with [`PostgresScheduler::new`], passing in an already-built
/// `sqlx::PgPool`. Call [`run_migrations`][Self::run_migrations] before first
/// use to ensure the schema is up to date.
pub struct PostgresScheduler {
    pool: PgPool,
}

impl PostgresScheduler {
    /// Create a new scheduler wrapping the given connection pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl Scheduler for PostgresScheduler {
    async fn schedule_now(
        &self,
        task_id: &str,
        task_name: &str,
        data: serde_json::Value,
        execution_id: Option<&str>,
    ) -> Result<ScheduleResult, StorageError> {
        let now = Utc::now();
        self.schedule_at(
            task_id,
            task_name,
            now,
            data,
            None,
            execution_id,
            serde_json::Value::Null,
        )
        .await
    }

    async fn schedule_at(
        &self,
        task_id: &str,
        task_name: &str,
        execution_time: DateTime<Utc>,
        data: serde_json::Value,
        recurrence: Option<Recurrence>,
        execution_id: Option<&str>,
        metadata: serde_json::Value,
    ) -> Result<ScheduleResult, StorageError> {
        let recurrence_json = recurrence
            .as_ref()
            .map(serde_json::to_value)
            .transpose()
            .map_err(|e| StorageError::Database(Box::new(e)))?;

        sqlx::query(
            r#"
            INSERT INTO zart_tasks
                (task_id, task_name, execution_time, data, recurrence, execution_id, metadata)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (task_id) DO NOTHING
            "#,
        )
        .bind(task_id)
        .bind(task_name)
        .bind(execution_time)
        .bind(&data)
        .bind(&recurrence_json)
        .bind(execution_id)
        .bind(&metadata)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        Ok(ScheduleResult {
            task_id: task_id.to_string(),
            execution_time,
        })
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

        // SELECT with SKIP LOCKED to avoid contention between concurrent workers.
        let rows: Vec<(
            String,
            String,
            serde_json::Value,
            serde_json::Value,
            i32,
            Option<String>,
            Option<serde_json::Value>,
            serde_json::Value,
        )> = sqlx::query_as(
            r#"
                SELECT task_id, task_name, data, state, attempt, execution_id, recurrence, metadata
                FROM zart_tasks
                WHERE status = 'scheduled'
                  AND execution_time <= $1
                ORDER BY execution_time ASC
                LIMIT $2
                FOR UPDATE SKIP LOCKED
                "#,
        )
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

        for (task_id, task_name, data, state, attempt, execution_id, recurrence_json, metadata) in
            rows
        {
            // Each task gets a unique lock token stored as `worker_id`.
            let lock_token = Uuid::new_v4().to_string();

            sqlx::query(
                r#"
                UPDATE zart_tasks
                SET status     = 'picked_up',
                    locked_at  = NOW(),
                    worker_id  = $1,
                    attempt    = attempt + 1,
                    updated_at = NOW()
                WHERE task_id = $2
                "#,
            )
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
                // Return the post-increment attempt count.
                attempt: attempt as usize + 1,
                lock_token,
                execution_id,
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
        let rows_affected = sqlx::query(
            r#"
            UPDATE zart_tasks
            SET state          = $1,
                execution_time = $2,
                status         = 'scheduled',
                locked_at      = NULL,
                worker_id      = NULL,
                updated_at     = NOW()
            WHERE task_id  = $3
              AND worker_id = $4
            "#,
        )
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
        let rows_affected = sqlx::query(
            r#"
            UPDATE zart_tasks
            SET status       = 'completed',
                result       = $1,
                completed_at = NOW(),
                updated_at   = NOW(),
                locked_at    = NULL,
                worker_id    = NULL
            WHERE task_id  = $2
              AND worker_id = $3
            "#,
        )
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
            Some(t) => ("scheduled", Some(t)),
            None => ("failed", None),
        };

        let rows_affected = if let Some(t) = exec_time {
            sqlx::query(
                r#"
                UPDATE zart_tasks
                SET status         = $1,
                    last_error     = $2,
                    execution_time = $3,
                    locked_at      = NULL,
                    worker_id      = NULL,
                    updated_at     = NOW()
                WHERE task_id  = $4
                  AND worker_id = $5
                "#,
            )
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
            sqlx::query(
                r#"
                UPDATE zart_tasks
                SET status     = $1,
                    last_error = $2,
                    locked_at  = NULL,
                    worker_id  = NULL,
                    updated_at = NOW()
                WHERE task_id  = $3
                  AND worker_id = $4
                "#,
            )
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
        let rows_affected = sqlx::query(
            r#"
            UPDATE zart_tasks
            SET status = 'cancelled', updated_at = NOW()
            WHERE task_id = $1 AND status = 'scheduled'
            "#,
        )
        .bind(task_id)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?
        .rows_affected();

        Ok(rows_affected > 0)
    }

    async fn delete_task(&self, task_id: &str) -> Result<(), StorageError> {
        sqlx::query("DELETE FROM zart_tasks WHERE task_id = $1")
            .bind(task_id)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;
        Ok(())
    }

    async fn run_migrations(&self) -> Result<(), StorageError> {
        sqlx::migrate!("./migrations")
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

        let result = sqlx::query(
            r#"
            UPDATE zart_tasks
            SET status     = 'scheduled',
                locked_at  = NULL,
                worker_id  = NULL,
                updated_at = NOW()
            WHERE status    = 'picked_up'
              AND locked_at < $1
            "#,
        )
        .bind(threshold)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        Ok(result.rows_affected() as usize)
    }

    async fn renew_lease(&self, task_id: &str, lock_token: &str) -> Result<bool, StorageError> {
        let rows_affected = sqlx::query(
            r#"
            UPDATE zart_tasks
            SET locked_at  = NOW(),
                updated_at = NOW()
            WHERE task_id   = $1
              AND worker_id = $2
              AND status    = 'picked_up'
            "#,
        )
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
        completed_task_id: &str,
        result: Option<serde_json::Value>,
        lock_token: &str,
        new_task_id: &str,
        new_task_name: &str,
        new_execution_time: DateTime<Utc>,
        new_data: serde_json::Value,
        new_execution_id: Option<&str>,
        new_metadata: serde_json::Value,
    ) -> Result<(), StorageError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;

        let rows = sqlx::query(
            r#"
            UPDATE zart_tasks
            SET status       = 'completed',
                result       = $1,
                completed_at = NOW(),
                updated_at   = NOW(),
                locked_at    = NULL,
                worker_id    = NULL
            WHERE task_id   = $2
              AND worker_id = $3
            "#,
        )
        .bind(&result)
        .bind(completed_task_id)
        .bind(lock_token)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?
        .rows_affected();

        if rows == 0 {
            tx.rollback()
                .await
                .map_err(|e| StorageError::Database(Box::new(e)))?;
            return Err(StorageError::LockMismatch(completed_task_id.to_string()));
        }

        sqlx::query(
            r#"
            INSERT INTO zart_tasks
                (task_id, task_name, execution_time, data, execution_id, metadata)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (task_id) DO NOTHING
            "#,
        )
        .bind(new_task_id)
        .bind(new_task_name)
        .bind(new_execution_time)
        .bind(&new_data)
        .bind(new_execution_id)
        .bind(&new_metadata)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        tx.commit()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;
        Ok(())
    }
}

#[async_trait]
impl DurableStorage for PostgresScheduler {
    async fn start_execution(
        &self,
        execution_id: &str,
        task_name: &str,
        payload: serde_json::Value,
    ) -> Result<(), StorageError> {
        sqlx::query(
            r#"
            INSERT INTO zart_executions (execution_id, task_name, payload)
            VALUES ($1, $2, $3)
            ON CONFLICT (execution_id) DO NOTHING
            "#,
        )
        .bind(execution_id)
        .bind(task_name)
        .bind(&payload)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;
        Ok(())
    }

    async fn complete_execution(
        &self,
        execution_id: &str,
        result: serde_json::Value,
    ) -> Result<(), StorageError> {
        sqlx::query(
            r#"
            UPDATE zart_executions
            SET status       = 'completed',
                result       = $1,
                completed_at = NOW(),
                updated_at   = NOW()
            WHERE execution_id = $2
            "#,
        )
        .bind(&result)
        .bind(execution_id)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;
        Ok(())
    }

    async fn fail_execution(&self, execution_id: &str) -> Result<(), StorageError> {
        sqlx::query(
            r#"
            UPDATE zart_executions
            SET status     = 'failed',
                updated_at = NOW()
            WHERE execution_id = $1
            "#,
        )
        .bind(execution_id)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;
        Ok(())
    }

    async fn get_execution(
        &self,
        execution_id: &str,
    ) -> Result<Option<ExecutionRecord>, StorageError> {
        let row: Option<(
            String,
            String,
            serde_json::Value,
            Option<serde_json::Value>,
            String,
            chrono::DateTime<chrono::Utc>,
            Option<chrono::DateTime<chrono::Utc>>,
            i32,
        )> = sqlx::query_as(
            r#"
                SELECT execution_id, task_name, payload, result, status,
                       scheduled_at, completed_at, version
                FROM zart_executions
                WHERE execution_id = $1
                "#,
        )
        .bind(execution_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        match row {
            None => Ok(None),
            Some((
                eid,
                task_name,
                payload,
                result,
                status_str,
                scheduled_at,
                completed_at,
                version,
            )) => {
                let status = status_str.parse::<ExecutionStatus>().map_err(|e| {
                    StorageError::Database(Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        e,
                    )))
                })?;
                Ok(Some(ExecutionRecord {
                    execution_id: eid,
                    task_name,
                    payload,
                    status,
                    result,
                    scheduled_at,
                    completed_at,
                    version,
                }))
            }
        }
    }

    async fn cancel_execution(&self, execution_id: &str) -> Result<bool, StorageError> {
        // Mark the execution record as cancelled.
        let exec_rows = sqlx::query(
            r#"
            UPDATE zart_executions
            SET status = 'cancelled', updated_at = NOW()
            WHERE execution_id = $1
              AND status IN ('scheduled', 'running')
            "#,
        )
        .bind(execution_id)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?
        .rows_affected();

        // Also cancel any not-yet-running task for this execution.
        sqlx::query(
            r#"
            UPDATE zart_tasks
            SET status = 'cancelled', updated_at = NOW()
            WHERE execution_id = $1 AND status = 'scheduled'
            "#,
        )
        .bind(execution_id)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        Ok(exec_rows > 0)
    }

    async fn list_executions(
        &self,
        status: Option<ExecutionStatus>,
        task_name: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<ExecutionRecord>, StorageError> {
        let status_str: Option<String> = status.map(|s| s.to_string());

        let rows: Vec<(
            String,
            String,
            serde_json::Value,
            Option<serde_json::Value>,
            String,
            chrono::DateTime<chrono::Utc>,
            Option<chrono::DateTime<chrono::Utc>>,
            i32,
        )> = sqlx::query_as(
            r#"
            SELECT execution_id, task_name, payload, result, status,
                   scheduled_at, completed_at, version
            FROM zart_executions
            WHERE ($1::TEXT IS NULL OR status    = $1)
              AND ($2::TEXT IS NULL OR task_name = $2)
            ORDER BY scheduled_at DESC
            LIMIT $3 OFFSET $4
            "#,
        )
        .bind(&status_str)
        .bind(task_name)
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        rows.into_iter()
            .map(
                |(eid, tname, payload, result, status_str, scheduled_at, completed_at, version)| {
                    let status = status_str.parse::<ExecutionStatus>().map_err(|e| {
                        StorageError::Database(Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            e,
                        )))
                    })?;
                    Ok(ExecutionRecord {
                        execution_id: eid,
                        task_name: tname,
                        payload,
                        status,
                        result,
                        scheduled_at,
                        completed_at,
                        version,
                    })
                },
            )
            .collect()
    }

    async fn complete_event_step_and_schedule_body(
        &self,
        execution_id: &str,
        event_name: &str,
        payload: serde_json::Value,
    ) -> Result<bool, StorageError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;

        // Mark the wait_for_event step task completed (only if still 'scheduled' — i.e.
        // the deadline worker has not yet picked it up).
        let row: Option<(String, serde_json::Value, i32)> = sqlx::query_as(
            r#"
            UPDATE zart_tasks
            SET status       = 'completed',
                result       = $3,
                completed_at = NOW(),
                updated_at   = NOW()
            WHERE execution_id           = $1
              AND metadata->>'step_type' = 'wait_for_event'
              AND metadata->>'step_name' = $2
              AND status                 = 'scheduled'
            RETURNING task_name, data, (metadata->>'segment')::int
            "#,
        )
        .bind(execution_id)
        .bind(event_name)
        .bind(&payload)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        let (task_name, data, next_segment) = match row {
            None => {
                tx.rollback()
                    .await
                    .map_err(|e| StorageError::Database(Box::new(e)))?;
                return Ok(false);
            }
            Some((t, d, s)) => (t, d, s as usize),
        };

        let next_body_task_id = format!("{}-b{}", execution_id, next_segment);
        let body_metadata = serde_json::json!({
            "mode": "body",
            "execution_id": execution_id,
            "segment": next_segment,
        });

        sqlx::query(
            r#"
            INSERT INTO zart_tasks (task_id, task_name, execution_time, data, execution_id, metadata)
            VALUES ($1, $2, NOW(), $3, $4, $5)
            ON CONFLICT (task_id) DO NOTHING
            "#,
        )
        .bind(&next_body_task_id)
        .bind(&task_name)
        .bind(&data)
        .bind(execution_id)
        .bind(&body_metadata)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        tx.commit()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;
        Ok(true)
    }

    async fn reset_execution(
        &self,
        execution_id: &str,
        payload: serde_json::Value,
    ) -> Result<(), StorageError> {
        sqlx::query(
            r#"
            UPDATE zart_executions
            SET status       = 'scheduled',
                payload      = $1,
                result       = NULL,
                completed_at = NULL,
                updated_at   = NOW()
            WHERE execution_id = $2
            "#,
        )
        .bind(&payload)
        .bind(execution_id)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;
        Ok(())
    }

    async fn get_step_status(
        &self,
        execution_id: &str,
        step_name: &str,
    ) -> Result<Option<StepLookup>, StorageError> {
        let task_id = format!("{execution_id}:step:{step_name}");

        let row: Option<(String, String, Option<serde_json::Value>)> = sqlx::query_as(
            r#"
            SELECT task_id, status, result
            FROM zart_tasks
            WHERE task_id = $1
            "#,
        )
        .bind(&task_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        match row {
            None => Ok(None),
            Some((tid, status_str, result)) => {
                let status = status_str.parse::<crate::TaskStatus>().map_err(|e| {
                    StorageError::Database(Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        e,
                    )))
                })?;
                Ok(Some(StepLookup {
                    task_id: tid,
                    status,
                    result,
                }))
            }
        }
    }

    async fn check_wait_all_children(
        &self,
        wait_for_task_ids: &[String],
    ) -> Result<Vec<(String, serde_json::Value)>, StorageError> {
        if wait_for_task_ids.is_empty() {
            return Ok(vec![]);
        }

        let rows: Vec<(String, Option<serde_json::Value>)> = sqlx::query_as(
            r#"
            SELECT task_id, result
            FROM zart_tasks
            WHERE task_id = ANY($1)
              AND status  = 'completed'
              AND result  IS NOT NULL
            "#,
        )
        .bind(wait_for_task_ids)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        Ok(rows
            .into_iter()
            .filter_map(|(id, r)| r.map(|v| (id, v)))
            .collect())
    }
}
