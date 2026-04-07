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
    CompleteAndScheduleParams, DurableStorage, ExecutionRecord, ExecutionStatus, FetchedTask,
    ScheduleAtParams, ScheduleResult, Scheduler, StepLookup, StorageError,
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
        self.schedule_at(ScheduleAtParams {
            task_id: task_id.to_string(),
            task_name: task_name.to_string(),
            execution_time: Utc::now(),
            data,
            recurrence: None,
            execution_id: execution_id.map(String::from),
            metadata: serde_json::Value::Null,
        })
        .await
    }

    async fn schedule_at(&self, params: ScheduleAtParams) -> Result<ScheduleResult, StorageError> {
        let recurrence_json = params
            .recurrence
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
        .bind(&params.task_id)
        .bind(&params.task_name)
        .bind(params.execution_time)
        .bind(&params.data)
        .bind(&recurrence_json)
        .bind(params.execution_id.as_deref())
        .bind(&params.metadata)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        Ok(ScheduleResult {
            task_id: params.task_id,
            execution_time: params.execution_time,
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
        params: CompleteAndScheduleParams,
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

        sqlx::query(
            r#"
            INSERT INTO zart_tasks
                (task_id, task_name, execution_time, data, execution_id, metadata)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (task_id) DO NOTHING
            "#,
        )
        .bind(&params.new_task_id)
        .bind(&params.new_task_name)
        .bind(params.new_execution_time)
        .bind(&params.new_data)
        .bind(params.new_execution_id.as_deref())
        .bind(&params.new_metadata)
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
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;

        // Insert execution record
        sqlx::query(
            r#"
            INSERT INTO zart_executions (execution_id, task_name)
            VALUES ($1, $2)
            ON CONFLICT (execution_id) DO NOTHING
            "#,
        )
        .bind(execution_id)
        .bind(task_name)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        // Check if a run already exists at index 0
        let run_exists: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS(
                SELECT 1 FROM zart_execution_runs
                WHERE execution_id = $1 AND run_index = 0
            )
            "#,
        )
        .bind(execution_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        // Create run row if it doesn't exist
        if !run_exists {
            let run_id = format!("{execution_id}:run:0");
            sqlx::query(
                r#"
                INSERT INTO zart_execution_runs
                    (run_id, execution_id, run_index, payload, trigger)
                VALUES ($1, $2, 0, $3, 'initial')
                ON CONFLICT DO NOTHING
                "#,
            )
            .bind(&run_id)
            .bind(execution_id)
            .bind(&payload)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;

            // Set current_run_id pointer
            sqlx::query(
                r#"
                UPDATE zart_executions
                SET current_run_id = $1
                WHERE execution_id = $2 AND current_run_id IS NULL
                "#,
            )
            .bind(&run_id)
            .bind(execution_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;
        }

        tx.commit()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;
        Ok(())
    }

    async fn complete_execution(
        &self,
        execution_id: &str,
        result: serde_json::Value,
    ) -> Result<(), StorageError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;

        // Update the current run row (zart_executions is a stable identity record with no
        // status/result columns — all run-level state lives in zart_execution_runs).
        sqlx::query(
            r#"
            UPDATE zart_execution_runs
            SET status       = 'completed',
                result       = $1,
                completed_at = NOW()
            WHERE execution_id = $2
              AND run_id = (SELECT current_run_id FROM zart_executions WHERE execution_id = $2)
            "#,
        )
        .bind(&result)
        .bind(execution_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        tx.commit()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;
        Ok(())
    }

    async fn fail_execution(&self, execution_id: &str) -> Result<(), StorageError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;

        // Update the current run row only (zart_executions has no status column).
        sqlx::query(
            r#"
            UPDATE zart_execution_runs
            SET status = 'failed'
            WHERE execution_id = $1
              AND run_id = (SELECT current_run_id FROM zart_executions WHERE execution_id = $1)
            "#,
        )
        .bind(execution_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        tx.commit()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;
        Ok(())
    }

    async fn get_execution(
        &self,
        execution_id: &str,
    ) -> Result<Option<ExecutionRecord>, StorageError> {
        // Get the current run's data from zart_execution_runs
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
                SELECT r.run_id, e.task_name, r.payload, r.result, r.status,
                       r.started_at, r.completed_at, 1
                FROM zart_executions e
                LEFT JOIN zart_execution_runs r ON e.current_run_id = r.run_id
                WHERE e.execution_id = $1
                "#,
        )
        .bind(execution_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        match row {
            None => Ok(None),
            Some((
                _run_id,
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
                    execution_id: execution_id.to_string(),
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

        // Get the current run_id
        let run_id: Option<String> = sqlx::query_scalar(
            r#"
            SELECT current_run_id FROM zart_executions WHERE execution_id = $1
            "#,
        )
        .bind(execution_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        let run_id = match run_id {
            None => {
                tx.rollback()
                    .await
                    .map_err(|e| StorageError::Database(Box::new(e)))?;
                return Ok(false);
            }
            Some(id) => id,
        };

        // Mark the wait_for_event step completed (only if still 'scheduled')
        let row: Option<(String, serde_json::Value)> = sqlx::query_as(
            r#"
            UPDATE zart_steps
            SET status       = 'completed',
                result       = $2,
                completed_at = NOW()
            WHERE run_id = $1
              AND step_name = $3
              AND step_kind = 'wait_for_event'
              AND status    = 'scheduled'
            RETURNING task_name, result
            "#,
        )
        .bind(&run_id)
        .bind(&payload)
        .bind(event_name)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        let (task_name, _result) = match row {
            None => {
                tx.rollback()
                    .await
                    .map_err(|e| StorageError::Database(Box::new(e)))?;
                return Ok(false);
            }
            Some((t, d)) => (t, d),
        };

        // Insert next body task
        let next_body_task_id = format!("{execution_id}:body:after:{event_name}");
        let body_metadata = serde_json::json!({
            "mode": "body",
            "run_id": run_id,
        });

        sqlx::query(
            r#"
            INSERT INTO zart_tasks (task_id, task_name, execution_time, data, metadata)
            VALUES ($1, $2, NOW(), $3, $4)
            ON CONFLICT (task_id) DO NOTHING
            "#,
        )
        .bind(&next_body_task_id)
        .bind(&task_name)
        .bind(&payload)
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
    ) -> Result<String, StorageError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;

        // Find the max run_index for this execution.
        let max_index: i32 = sqlx::query_scalar(
            r#"
            SELECT COALESCE(MAX(run_index), -1)
            FROM zart_execution_runs
            WHERE execution_id = $1
            "#,
        )
        .bind(execution_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        let next_index = max_index + 1;
        let new_run_id = format!("{execution_id}:run:{next_index}");

        // Insert a new run row for the restart.
        sqlx::query(
            r#"
            INSERT INTO zart_execution_runs
                (run_id, execution_id, run_index, payload, trigger)
            VALUES ($1, $2, $3, $4, 'restart')
            "#,
        )
        .bind(&new_run_id)
        .bind(execution_id)
        .bind(next_index)
        .bind(&payload)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        // Update the execution record to point to the new run.
        sqlx::query(
            r#"
            UPDATE zart_executions
            SET current_run_id = $1
            WHERE execution_id = $2
            "#,
        )
        .bind(&new_run_id)
        .bind(execution_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        tx.commit()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;
        Ok(new_run_id)
    }

    async fn get_step_status(
        &self,
        run_id: &str,
        step_name: &str,
    ) -> Result<Option<StepLookup>, StorageError> {
        // Query zart_steps by run_id + step_name — the authoritative source.
        // step_id = "{run_id}:step:{step_name}" matches the ID format used at scheduling time.
        let task_id = format!("{run_id}:step:{step_name}");

        let row: Option<(String, String, Option<serde_json::Value>)> = sqlx::query_as(
            r#"
            SELECT step_id, status, result
            FROM zart_steps
            WHERE step_id = $1
            "#,
        )
        .bind(&task_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        match row {
            None => Ok(None),
            Some((step_id, status_str, result)) => {
                // Map zart_steps status to TaskStatus
                let status = match status_str.as_str() {
                    "scheduled" => crate::TaskStatus::Scheduled,
                    "running" => crate::TaskStatus::PickedUp,
                    "completed" => crate::TaskStatus::Completed,
                    "dead" => crate::TaskStatus::Dead,
                    _ => crate::TaskStatus::Scheduled,
                };
                Ok(Some(StepLookup {
                    task_id: step_id,
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

    async fn get_step(
        &self,
        run_id: &str,
        step_name: &str,
    ) -> Result<Option<crate::StepRow>, StorageError> {
        use crate::StepRow;

        let row: Option<(
            String,
            String,
            String,
            String,
            Option<String>,
            String,
            i32,
            Option<serde_json::Value>,
            Option<serde_json::Value>,
            Option<String>,
            chrono::DateTime<chrono::Utc>,
            Option<chrono::DateTime<chrono::Utc>>,
        )> = sqlx::query_as(
            r#"
            SELECT step_id, run_id, step_name, step_kind, task_id,
                   status, retry_attempt, retry_config, result, last_error,
                   scheduled_at, completed_at
            FROM zart_steps
            WHERE run_id = $1 AND step_name = $2
            "#,
        )
        .bind(run_id)
        .bind(step_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        match row {
            None => Ok(None),
            Some((
                step_id,
                run_id,
                step_name,
                step_kind,
                task_id,
                status,
                retry_attempt,
                retry_config,
                result,
                last_error,
                scheduled_at,
                completed_at,
            )) => Ok(Some(StepRow {
                step_id,
                run_id,
                step_name,
                step_kind,
                task_id,
                status,
                retry_attempt,
                retry_config,
                result,
                last_error,
                scheduled_at,
                completed_at,
            })),
        }
    }

    async fn list_steps(&self, run_id: &str) -> Result<Vec<crate::StepRow>, StorageError> {
        use crate::StepRow;

        let rows: Vec<(
            String,
            String,
            String,
            String,
            Option<String>,
            String,
            i32,
            Option<serde_json::Value>,
            Option<serde_json::Value>,
            Option<String>,
            chrono::DateTime<chrono::Utc>,
            Option<chrono::DateTime<chrono::Utc>>,
        )> = sqlx::query_as(
            r#"
            SELECT step_id, run_id, step_name, step_kind, task_id,
                   status, retry_attempt, retry_config, result, last_error,
                   scheduled_at, completed_at
            FROM zart_steps
            WHERE run_id = $1
            ORDER BY scheduled_at ASC
            "#,
        )
        .bind(run_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        rows.into_iter()
            .map(
                |(
                    step_id,
                    run_id,
                    step_name,
                    step_kind,
                    task_id,
                    status,
                    retry_attempt,
                    retry_config,
                    result,
                    last_error,
                    scheduled_at,
                    completed_at,
                )| {
                    Ok(StepRow {
                        step_id,
                        run_id,
                        step_name,
                        step_kind,
                        task_id,
                        status,
                        retry_attempt,
                        retry_config,
                        result,
                        last_error,
                        scheduled_at,
                        completed_at,
                    })
                },
            )
            .collect()
    }

    async fn begin(&self) -> Result<Box<dyn crate::StepTransaction + Send>, StorageError> {
        // sqlx 0.8: Pool::begin() returns Transaction<'static, _> because PgPool is Arc-backed.
        // No unsafe transmute needed.
        let tx: sqlx::Transaction<'static, sqlx::Postgres> = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;
        Ok(Box::new(PgTransaction::new(tx)))
    }
}

// ── Transactional API for step table operations ───────────────────────────────

/// A transactional wrapper for atomic step+task operations.
///
/// Obtained via [`DurableStorage::begin`]. All writes to `zart_steps` and
/// `zart_tasks` should go through this to ensure atomicity.
pub struct PgTransaction {
    tx: sqlx::Transaction<'static, sqlx::Postgres>,
}

impl PgTransaction {
    fn new(tx: sqlx::Transaction<'static, sqlx::Postgres>) -> Self {
        Self { tx }
    }

    /// Insert a task row within this transaction.
    pub async fn insert_task(&mut self, params: ScheduleAtParams) -> Result<(), StorageError> {
        let recurrence_json = params
            .recurrence
            .as_ref()
            .map(serde_json::to_value)
            .transpose()
            .map_err(|e| StorageError::Database(Box::new(e)))?;

        sqlx::query(
            r#"
            INSERT INTO zart_tasks
                (task_id, task_name, execution_time, data, recurrence, metadata)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (task_id) DO NOTHING
            "#,
        )
        .bind(&params.task_id)
        .bind(&params.task_name)
        .bind(params.execution_time)
        .bind(&params.data)
        .bind(&recurrence_json)
        .bind(&params.metadata)
        .execute(&mut *self.tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        Ok(())
    }

    /// Insert a step row within this transaction.
    pub async fn insert_step(
        &mut self,
        step_id: &str,
        run_id: &str,
        step_name: &str,
        step_kind: &str,
        task_id: &str,
        retry_config: Option<&serde_json::Value>,
    ) -> Result<(), StorageError> {
        sqlx::query(
            r#"
            INSERT INTO zart_steps
                (step_id, run_id, step_name, step_kind, task_id, retry_config)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (step_id) DO NOTHING
            "#,
        )
        .bind(step_id)
        .bind(run_id)
        .bind(step_name)
        .bind(step_kind)
        .bind(task_id)
        .bind(retry_config)
        .execute(&mut *self.tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        Ok(())
    }

    /// Mark a step completed within this transaction.
    pub async fn complete_step(
        &mut self,
        step_id: &str,
        result: serde_json::Value,
        completed_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), StorageError> {
        sqlx::query(
            r#"
            UPDATE zart_steps
            SET status       = 'completed',
                result       = $1,
                completed_at = $2
            WHERE step_id = $3
            "#,
        )
        .bind(&result)
        .bind(completed_at)
        .bind(step_id)
        .execute(&mut *self.tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        Ok(())
    }

    /// Mark a task completed within this transaction.
    pub async fn mark_task_completed(
        &mut self,
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
            WHERE task_id   = $2
              AND worker_id = $3
            "#,
        )
        .bind(&result)
        .bind(task_id)
        .bind(lock_token)
        .execute(&mut *self.tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?
        .rows_affected();

        if rows_affected == 0 {
            return Err(StorageError::LockMismatch(task_id.to_string()));
        }
        Ok(())
    }

    /// Insert a body task row within this transaction.
    pub async fn insert_body_task(
        &mut self,
        task_id: &str,
        task_name: &str,
        _run_id: &str,
        execution_time: chrono::DateTime<chrono::Utc>,
        data: serde_json::Value,
        metadata: serde_json::Value,
    ) -> Result<(), StorageError> {
        sqlx::query(
            r#"
            INSERT INTO zart_tasks
                (task_id, task_name, execution_time, data, metadata)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (task_id) DO NOTHING
            "#,
        )
        .bind(task_id)
        .bind(task_name)
        .bind(execution_time)
        .bind(data)
        .bind(metadata)
        .execute(&mut *self.tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        Ok(())
    }

    /// Record a step attempt (completed or failed) in `zart_step_attempts`.
    pub async fn record_step_attempt(
        &mut self,
        attempt_id: &str,
        step_id: &str,
        attempt_number: usize,
        status: &str,
        result: Option<&serde_json::Value>,
        error: Option<&str>,
    ) -> Result<(), StorageError> {
        sqlx::query(
            r#"
            INSERT INTO zart_step_attempts
                (attempt_id, step_id, attempt_number, status, completed_at, result, error)
            VALUES ($1, $2, $3, $4, NOW(), $5, $6)
            ON CONFLICT (attempt_id) DO NOTHING
            "#,
        )
        .bind(attempt_id)
        .bind(step_id)
        .bind(attempt_number as i32)
        .bind(status)
        .bind(result)
        .bind(error)
        .execute(&mut *self.tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;
        Ok(())
    }

    /// Reschedule a step task for retry within this transaction.
    pub async fn mark_task_failed_for_retry(
        &mut self,
        task_id: &str,
        error: &str,
        retry_time: chrono::DateTime<chrono::Utc>,
        lock_token: &str,
    ) -> Result<(), StorageError> {
        let rows_affected = sqlx::query(
            r#"
            UPDATE zart_tasks
            SET status         = 'scheduled',
                last_error     = $1,
                execution_time = $2,
                locked_at      = NULL,
                worker_id      = NULL,
                updated_at     = NOW()
            WHERE task_id  = $3
              AND worker_id = $4
            "#,
        )
        .bind(error)
        .bind(retry_time)
        .bind(task_id)
        .bind(lock_token)
        .execute(&mut *self.tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?
        .rows_affected();
        if rows_affected == 0 {
            return Err(StorageError::LockMismatch(task_id.to_string()));
        }
        Ok(())
    }

    /// Update the retry count on a step row.
    pub async fn update_step_retry_count(
        &mut self,
        step_id: &str,
        new_retry_attempt: usize,
    ) -> Result<(), StorageError> {
        sqlx::query(
            r#"
            UPDATE zart_steps
            SET retry_attempt = $1,
                last_error    = NULL
            WHERE step_id = $2
            "#,
        )
        .bind(new_retry_attempt as i32)
        .bind(step_id)
        .execute(&mut *self.tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;
        Ok(())
    }

    /// Mark a step as dead (retries exhausted) within this transaction.
    pub async fn dead_step(&mut self, step_id: &str, error: &str) -> Result<(), StorageError> {
        sqlx::query(
            r#"
            UPDATE zart_steps
            SET status   = 'dead',
                last_error = $1
            WHERE step_id = $2
            "#,
        )
        .bind(error)
        .bind(step_id)
        .execute(&mut *self.tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        Ok(())
    }

    /// Commit this transaction.
    pub async fn commit(self) -> Result<(), StorageError> {
        self.tx
            .commit()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))
    }

    /// Roll back this transaction.
    pub async fn rollback(self) -> Result<(), StorageError> {
        self.tx
            .rollback()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))
    }
}

#[async_trait::async_trait]
impl crate::StepTransaction for PgTransaction {
    async fn insert_task(&mut self, params: ScheduleAtParams) -> Result<(), StorageError> {
        PgTransaction::insert_task(self, params).await
    }

    async fn insert_step(
        &mut self,
        step_id: &str,
        run_id: &str,
        step_name: &str,
        step_kind: &str,
        task_id: &str,
        retry_config: Option<&serde_json::Value>,
    ) -> Result<(), StorageError> {
        PgTransaction::insert_step(
            self,
            step_id,
            run_id,
            step_name,
            step_kind,
            task_id,
            retry_config,
        )
        .await
    }

    async fn complete_step(
        &mut self,
        step_id: &str,
        result: serde_json::Value,
        completed_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), StorageError> {
        PgTransaction::complete_step(self, step_id, result, completed_at).await
    }

    async fn mark_task_completed(
        &mut self,
        task_id: &str,
        result: Option<serde_json::Value>,
        lock_token: &str,
    ) -> Result<(), StorageError> {
        PgTransaction::mark_task_completed(self, task_id, result, lock_token).await
    }

    async fn insert_body_task(
        &mut self,
        task_id: &str,
        task_name: &str,
        run_id: &str,
        execution_time: chrono::DateTime<chrono::Utc>,
        data: serde_json::Value,
        metadata: serde_json::Value,
    ) -> Result<(), StorageError> {
        PgTransaction::insert_body_task(
            self,
            task_id,
            task_name,
            run_id,
            execution_time,
            data,
            metadata,
        )
        .await
    }

    async fn record_step_attempt(
        &mut self,
        attempt_id: &str,
        step_id: &str,
        attempt_number: usize,
        status: &str,
        result: Option<&serde_json::Value>,
        error: Option<&str>,
    ) -> Result<(), StorageError> {
        PgTransaction::record_step_attempt(self, attempt_id, step_id, attempt_number, status, result, error).await
    }

    async fn mark_task_failed_for_retry(
        &mut self,
        task_id: &str,
        error: &str,
        retry_time: chrono::DateTime<chrono::Utc>,
        lock_token: &str,
    ) -> Result<(), StorageError> {
        PgTransaction::mark_task_failed_for_retry(self, task_id, error, retry_time, lock_token).await
    }

    async fn update_step_retry_count(
        &mut self,
        step_id: &str,
        new_retry_attempt: usize,
    ) -> Result<(), StorageError> {
        PgTransaction::update_step_retry_count(self, step_id, new_retry_attempt).await
    }

    async fn dead_step(&mut self, step_id: &str, error: &str) -> Result<(), StorageError> {
        PgTransaction::dead_step(self, step_id, error).await
    }

    async fn commit(self: Box<Self>) -> Result<(), StorageError> {
        PgTransaction::commit(*self).await
    }

    async fn rollback(self: Box<Self>) -> Result<(), StorageError> {
        PgTransaction::rollback(*self).await
    }
}

