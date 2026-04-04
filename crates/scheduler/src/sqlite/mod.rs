//! SQLite-backed implementation of the [`Scheduler`] trait.
//!
//! Uses `sqlx` with an `SqlitePool` for connection pooling. SQLite does not
//! support `SKIP LOCKED`, so concurrent polling is serialized via
//! `BEGIN IMMEDIATE` transactions. This is sufficient for single-process or
//! light-concurrency deployments.
//!
//! # Migrations
//!
//! Call [`SqliteScheduler::run_migrations`] once before starting workers.
//! It applies the embedded SQL files under `migrations-sqlite/`.
//!
//! # Connection string
//!
//! Use a standard SQLite URL: `sqlite:path/to/zart.db` or
//! `sqlite::memory:` for an in-memory database (useful for tests).

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::SqlitePool;
use uuid::Uuid;

use crate::{
    ExecutionRecord, ExecutionStatus, FetchedTask, Recurrence, ScheduleResult, Scheduler,
    StorageError,
};

/// A [`Scheduler`] backed by a SQLite database.
///
/// Create one with [`SqliteScheduler::new`], passing in an already-built
/// `sqlx::SqlitePool`. Call [`run_migrations`][Self::run_migrations] before
/// first use to ensure the schema is up to date.
pub struct SqliteScheduler {
    pool: SqlitePool,
}

impl SqliteScheduler {
    /// Create a new scheduler wrapping the given connection pool.
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl Scheduler for SqliteScheduler {
    async fn schedule_now(
        &self,
        task_id: &str,
        task_name: &str,
        data: serde_json::Value,
        execution_id: Option<&str>,
    ) -> Result<ScheduleResult, StorageError> {
        let now = Utc::now();
        self.schedule_at(task_id, task_name, now, data, None, execution_id)
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
    ) -> Result<ScheduleResult, StorageError> {
        let recurrence_json = recurrence
            .as_ref()
            .map(serde_json::to_value)
            .transpose()
            .map_err(|e| StorageError::Database(Box::new(e)))?;

        sqlx::query(
            r#"
            INSERT INTO zart_tasks
                (task_id, task_name, execution_time, data, recurrence, execution_id)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            ON CONFLICT (task_id) DO NOTHING
            "#,
        )
        .bind(task_id)
        .bind(task_name)
        .bind(execution_time)
        .bind(&data)
        .bind(&recurrence_json)
        .bind(execution_id)
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
        // SQLite doesn't support SKIP LOCKED. We use BEGIN IMMEDIATE to get a
        // write lock, select due tasks, lock them, and commit.
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;

        // Lock the tasks table to prevent concurrent polls from grabbing the same rows.
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;

        let rows: Vec<(
            String,
            String,
            serde_json::Value,
            serde_json::Value,
            i32,
            Option<String>,
            Option<serde_json::Value>,
        )> = sqlx::query_as(
            r#"
                SELECT task_id, task_name, data, state, attempt, execution_id, recurrence
                FROM zart_tasks
                WHERE status = 'scheduled'
                  AND execution_time <= ?1
                ORDER BY execution_time ASC
                LIMIT ?2
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

        for (task_id, task_name, data, state, attempt, execution_id, recurrence_json) in rows {
            let lock_token = Uuid::new_v4().to_string();

            sqlx::query(
                r#"
                UPDATE zart_tasks
                SET status     = 'picked_up',
                    locked_at  = CURRENT_TIMESTAMP,
                    worker_id  = ?1,
                    attempt    = attempt + 1,
                    updated_at = CURRENT_TIMESTAMP
                WHERE task_id = ?2
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
                attempt: attempt as usize + 1,
                lock_token,
                execution_id,
                recurrence,
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
            SET state          = ?1,
                execution_time = ?2,
                status         = 'scheduled',
                locked_at      = NULL,
                worker_id      = NULL,
                updated_at     = CURRENT_TIMESTAMP
            WHERE task_id  = ?3
              AND worker_id = ?4
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
                result       = ?1,
                completed_at = CURRENT_TIMESTAMP,
                updated_at   = CURRENT_TIMESTAMP,
                locked_at    = NULL,
                worker_id    = NULL
            WHERE task_id  = ?2
              AND worker_id = ?3
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
                SET status         = ?1,
                    last_error     = ?2,
                    execution_time = ?3,
                    locked_at      = NULL,
                    worker_id      = NULL,
                    updated_at     = CURRENT_TIMESTAMP
                WHERE task_id  = ?4
                  AND worker_id = ?5
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
                SET status     = ?1,
                    last_error = ?2,
                    locked_at  = NULL,
                    worker_id  = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE task_id  = ?3
                  AND worker_id = ?4
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
            SET status = 'cancelled', updated_at = CURRENT_TIMESTAMP
            WHERE task_id = ?1 AND status = 'scheduled'
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
        sqlx::query("DELETE FROM zart_tasks WHERE task_id = ?1")
            .bind(task_id)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;
        Ok(())
    }

    async fn run_migrations(&self) -> Result<(), StorageError> {
        sqlx::migrate!("./migrations-sqlite")
            .run(&self.pool)
            .await
            .map_err(|e| StorageError::Migration(e.to_string()))
    }

    async fn start_execution(
        &self,
        execution_id: &str,
        task_name: &str,
        payload: serde_json::Value,
    ) -> Result<(), StorageError> {
        sqlx::query(
            r#"
            INSERT INTO zart_executions (execution_id, task_name, payload)
            VALUES (?1, ?2, ?3)
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
                result       = ?1,
                completed_at = CURRENT_TIMESTAMP,
                updated_at   = CURRENT_TIMESTAMP
            WHERE execution_id = ?2
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
                updated_at = CURRENT_TIMESTAMP
            WHERE execution_id = ?1
            "#,
        )
        .bind(execution_id)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;
        Ok(())
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
                updated_at = CURRENT_TIMESTAMP
            WHERE status    = 'picked_up'
              AND locked_at < ?1
            "#,
        )
        .bind(threshold)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        Ok(result.rows_affected() as usize)
    }

    async fn cancel_execution(&self, execution_id: &str) -> Result<bool, StorageError> {
        let exec_rows = sqlx::query(
            r#"
            UPDATE zart_executions
            SET status = 'cancelled', updated_at = CURRENT_TIMESTAMP
            WHERE execution_id = ?1
              AND status IN ('scheduled', 'running')
            "#,
        )
        .bind(execution_id)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?
        .rows_affected();

        sqlx::query(
            r#"
            UPDATE zart_tasks
            SET status = 'cancelled', updated_at = CURRENT_TIMESTAMP
            WHERE execution_id = ?1 AND status = 'scheduled'
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
            WHERE (?1 IS NULL OR status    = ?1)
              AND (?2 IS NULL OR task_name = ?2)
            ORDER BY scheduled_at DESC
            LIMIT ?3 OFFSET ?4
            "#,
        )
        .bind(status_str)
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

    async fn reschedule_with_event(
        &self,
        execution_id: &str,
        event_name: &str,
        payload: serde_json::Value,
    ) -> Result<bool, StorageError> {
        // Build the event object and merge it into pending_events using json_patch.
        let event_obj = serde_json::json!({ event_name: payload });

        // Use a transaction to atomically read current state, merge, and update.
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;

        // Read current pending_events JSON string from the state column.
        let current_events: Option<String> = sqlx::query_scalar(
            "SELECT json_extract(state, '$.pending_events') FROM zart_tasks WHERE execution_id = ?1 AND status = 'scheduled'",
        )
        .bind(execution_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        // Merge existing with new event.
        let merged = match current_events.flatten().filter(|v| !v.is_empty()) {
            Some(existing_str) => {
                let existing: serde_json::Value =
                    serde_json::from_str(&existing_str).unwrap_or_default();
                if let (Some(existing_obj), Some(new_obj)) =
                    (existing.as_object(), event_obj.as_object())
                {
                    let mut merged = existing_obj.clone();
                    for (k, v) in new_obj {
                        merged.insert(k.clone(), v.clone());
                    }
                    serde_json::to_value(merged)
                        .map_err(|e| StorageError::Database(Box::new(e)))?
                } else {
                    event_obj.clone()
                }
            }
            None => event_obj.clone(),
        };

        let merged_str = serde_json::to_string(&merged)
            .map_err(|e| StorageError::Database(Box::new(e)))?;

        let rows_affected = sqlx::query(
            r#"
            UPDATE zart_tasks
            SET state = json_set(
                    state,
                    '$.pending_events',
                    ?1
                ),
                execution_time = CURRENT_TIMESTAMP,
                updated_at     = CURRENT_TIMESTAMP
            WHERE execution_id = ?2
              AND status = 'scheduled'
            "#,
        )
        .bind(merged_str)
        .bind(execution_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?
        .rows_affected();

        tx.commit()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;

        Ok(rows_affected > 0)
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
                WHERE execution_id = ?1
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
}
