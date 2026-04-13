//! Implementation of the [`DurableStorage`] trait for [`PostgresScheduler`].

use async_trait::async_trait;
use chrono::Utc;
use sqlx::PgConnection;

use super::PostgresScheduler;
use super::sql_helpers::{complete_step_and_schedule_body_sql, start_execution_sql};
use crate::{
    CompleteStepAndScheduleBodyParams, CompleteStepNoResumeParams, CompleteWaitGroupChildParams,
    DurableStorage, EventDeliveryResult, ExecutionRecord, ExecutionStatus,
    FailWaitGroupChildParams, RescheduleStepForRetryParams, ScheduleStepParams, StepKind,
    StepLookup, StepResultKind, StepStatus, StorageError, TaskStatus, UpsertWaitGroupStepParams,
};
use std::collections::{HashMap, HashSet};

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

        start_execution_sql(&mut tx, execution_id, task_name, &payload).await?;

        tx.commit()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;
        Ok(())
    }

    /// Start a durable execution within the caller's transaction.
    ///
    /// The caller is responsible for committing or rolling back the transaction.
    /// If the transaction rolls back, no execution record or body task will exist.
    async fn start_execution_in_tx(
        &self,
        conn: &mut PgConnection,
        execution_id: &str,
        task_name: &str,
        payload: serde_json::Value,
    ) -> Result<(), StorageError> {
        start_execution_sql(conn, execution_id, task_name, &payload).await
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
            ExecutionStatus,
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
                status,
                scheduled_at,
                completed_at,
                version,
            )) => Ok(Some(ExecutionRecord {
                execution_id: execution_id.to_string(),
                task_name,
                payload,
                status,
                result,
                scheduled_at,
                completed_at,
                version,
            })),
        }
    }

    async fn cancel_execution(&self, execution_id: &str) -> Result<bool, StorageError> {
        // Mark the current run as cancelled (status lives in zart_execution_runs).
        let exec_rows = sqlx::query(
            r#"
            UPDATE zart_execution_runs
            SET status = 'cancelled', completed_at = NOW()
            WHERE run_id = (SELECT current_run_id FROM zart_executions WHERE execution_id = $1)
              AND status IN ('scheduled', 'running')
            "#,
        )
        .bind(execution_id)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?
        .rows_affected();

        // Also cancel any not-yet-running tasks for this execution.
        // Body tasks have execution_id in metadata; step tasks have run_id like "{execution_id}:run:N".
        sqlx::query(
            r#"
            UPDATE zart_tasks
            SET status = 'cancelled', updated_at = NOW()
            WHERE status = 'scheduled'
              AND (
                metadata->>'execution_id' = $1
                OR metadata->>'run_id' LIKE $1 || ':run:%'
              )
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
        let rows: Vec<(
            String,
            String,
            serde_json::Value,
            Option<serde_json::Value>,
            ExecutionStatus,
            chrono::DateTime<chrono::Utc>,
            Option<chrono::DateTime<chrono::Utc>>,
            i32,
        )> = sqlx::query_as(
            r#"
            SELECT e.execution_id, e.task_name, r.payload, r.result, r.status,
                   r.started_at, r.completed_at, 1
            FROM zart_executions e
            JOIN zart_execution_runs r ON e.current_run_id = r.run_id
            WHERE ($1::execution_status IS NULL OR r.status = $1)
              AND ($2::TEXT IS NULL OR e.task_name = $2)
            ORDER BY r.started_at DESC
            LIMIT $3 OFFSET $4
            "#,
        )
        .bind(status)
        .bind(task_name)
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        rows.into_iter()
            .map(
                |(eid, tname, payload, result, status, scheduled_at, completed_at, version)| {
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

    async fn deliver_event(
        &self,
        execution_id: &str,
        event_name: &str,
        payload: serde_json::Value,
    ) -> Result<EventDeliveryResult, StorageError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;

        let exec_row: Option<(String, String, serde_json::Value)> = sqlx::query_as(
            r#"
            SELECT e.current_run_id, e.task_name, r.payload
            FROM zart_executions e
            JOIN zart_execution_runs r ON r.run_id = e.current_run_id
            WHERE e.execution_id = $1
            "#,
        )
        .bind(execution_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        let (run_id, task_name, run_payload) = match exec_row {
            None => {
                tx.rollback()
                    .await
                    .map_err(|e| StorageError::Database(Box::new(e)))?;
                return Ok(EventDeliveryResult::NotRegistered);
            }
            Some(row) => row,
        };

        let completed_row: Option<(String,)> = sqlx::query_as(
            r#"
            SELECT step_id
            FROM zart_steps
            WHERE run_id = $1
              AND step_name = $2
              AND step_kind = 'wait_for_event'
              AND status = 'completed'
            "#,
        )
        .bind(&run_id)
        .bind(event_name)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        if completed_row.is_some() {
            tx.rollback()
                .await
                .map_err(|e| StorageError::Database(Box::new(e)))?;
            return Ok(EventDeliveryResult::AlreadyDelivered);
        }

        let step_row: Option<(String,)> = sqlx::query_as(
            r#"
            SELECT step_id
            FROM zart_steps
            WHERE run_id = $1
              AND step_name = $2
              AND step_kind = 'wait_for_event'
              AND status = 'scheduled'
            "#,
        )
        .bind(&run_id)
        .bind(event_name)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        let step_id = match step_row {
            None => {
                tx.rollback()
                    .await
                    .map_err(|e| StorageError::Database(Box::new(e)))?;
                return Ok(EventDeliveryResult::NotRegistered);
            }
            Some((sid,)) => sid,
        };

        let updated = sqlx::query(
            r#"
            UPDATE zart_steps
            SET status = 'completed',
                result = $1,
                completed_at = NOW()
            WHERE step_id = $2
              AND status = 'scheduled'
            "#,
        )
        .bind(&payload)
        .bind(&step_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?
        .rows_affected();

        if updated == 0 {
            tx.rollback()
                .await
                .map_err(|e| StorageError::Database(Box::new(e)))?;
            return Ok(EventDeliveryResult::NotRegistered);
        }

        let next_body_task_id = format!("{execution_id}:body:after:{event_name}");
        let body_metadata = serde_json::json!({
            "mode": "body",
            "run_id": run_id,
            "execution_id": execution_id,
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
        .bind(&run_payload)
        .bind(&body_metadata)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        tx.commit()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;

        Ok(EventDeliveryResult::Delivered)
    }

    async fn complete_event_step_and_schedule_body(
        &self,
        execution_id: &str,
        event_name: &str,
        payload: serde_json::Value,
    ) -> Result<bool, StorageError> {
        match self
            .deliver_event(execution_id, event_name, payload)
            .await?
        {
            EventDeliveryResult::Delivered => Ok(true),
            EventDeliveryResult::AlreadyDelivered | EventDeliveryResult::NotRegistered => Ok(false),
        }
    }

    async fn upsert_wait_group_step(
        &self,
        params: UpsertWaitGroupStepParams,
    ) -> Result<(), StorageError> {
        let step_id = format!("{}:step:{}", params.run_id, params.group_step_name);
        sqlx::query(
            r#"
            INSERT INTO zart_steps
                (step_id, run_id, step_name, step_kind, status,
                 wg_total, wg_remaining, wg_threshold, wg_first_failed)
            VALUES
                ($1, $2, $3, 'wait_group', 'scheduled',
                 $4, $4, $5, FALSE)
            ON CONFLICT (step_id) DO NOTHING
            "#,
        )
        .bind(&step_id)
        .bind(&params.run_id)
        .bind(&params.group_step_name)
        .bind(params.total)
        .bind(params.threshold)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;
        Ok(())
    }

    async fn complete_wait_group_child(
        &self,
        params: CompleteWaitGroupChildParams,
    ) -> Result<bool, StorageError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;

        let attempt_id = format!("{}:attempt:{}", params.child_step_id, params.attempt_number);
        sqlx::query(
            r#"
            INSERT INTO zart_step_attempts (attempt_id, step_id, attempt_number, status, completed_at, result, error)
            VALUES ($1, $2, $3, 'completed', NOW(), $4, NULL)
            ON CONFLICT (attempt_id) DO NOTHING
            "#,
        )
        .bind(&attempt_id)
        .bind(&params.child_step_id)
        .bind(params.attempt_number as i32)
        .bind(&params.child_result)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        sqlx::query(
            r#"
            UPDATE zart_steps
            SET status = 'completed', result = $1, completed_at = NOW()
            WHERE step_id = $2
            "#,
        )
        .bind(&params.child_result)
        .bind(&params.child_step_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        let rows_affected = sqlx::query(
            r#"
            UPDATE zart_tasks
            SET status = 'completed', result = $1, completed_at = NOW(), updated_at = NOW(), locked_at = NULL, worker_id = NULL
            WHERE task_id = $2 AND worker_id = $3
            "#,
        )
        .bind(&params.child_result)
        .bind(&params.child_step_task_id)
        .bind(&params.lock_token)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?
        .rows_affected();

        if rows_affected == 0 {
            tx.rollback()
                .await
                .map_err(|e| StorageError::Database(Box::new(e)))?;
            return Err(StorageError::LockMismatch(
                params.child_step_task_id.clone(),
            ));
        }

        let wg_row: Option<(i32, i32)> = sqlx::query_as(
            r#"
            UPDATE zart_steps
            SET wg_remaining = wg_remaining - 1
            WHERE run_id = $1
              AND step_name = $2
              AND step_kind = 'wait_group'
              AND wg_remaining IS NOT NULL
              AND wg_threshold IS NOT NULL
            RETURNING wg_remaining, wg_threshold
            "#,
        )
        .bind(&params.run_id)
        .bind(&params.group_step_name)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        let triggered = match wg_row {
            Some((remaining, threshold)) => remaining == threshold,
            None => false,
        };

        if triggered {
            let body_metadata = serde_json::json!({
                "mode": "body",
                "run_id": params.run_id,
                "execution_id": params.run_id.split(":run:").next().unwrap_or(&params.run_id),
            });
            sqlx::query(
                r#"
                INSERT INTO zart_tasks (task_id, task_name, execution_time, data, metadata)
                VALUES ($1, $2, NOW(), $3, $4)
                ON CONFLICT (task_id) DO NOTHING
                "#,
            )
            .bind(&params.next_body_task_id)
            .bind(&params.task_name)
            .bind(&params.data)
            .bind(&body_metadata)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;
        }

        tx.commit()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;
        Ok(triggered)
    }

    async fn fail_wait_group_child(
        &self,
        params: FailWaitGroupChildParams,
    ) -> Result<bool, StorageError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;

        let attempt_id = format!("{}:attempt:{}", params.child_step_id, params.attempt_number);
        sqlx::query(
            r#"
            INSERT INTO zart_step_attempts (attempt_id, step_id, attempt_number, status, completed_at, result, error)
            VALUES ($1, $2, $3, 'failed', NOW(), NULL, $4)
            ON CONFLICT (attempt_id) DO NOTHING
            "#,
        )
        .bind(&attempt_id)
        .bind(&params.child_step_id)
        .bind(params.attempt_number as i32)
        .bind(&params.error)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        sqlx::query(
            r#"
            UPDATE zart_steps
            SET status = 'dead', last_error = $1, completed_at = NOW()
            WHERE step_id = $2
            "#,
        )
        .bind(&params.error)
        .bind(&params.child_step_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        let rows_affected = sqlx::query(
            r#"
            UPDATE zart_tasks
            SET status = 'failed', last_error = $1, updated_at = NOW(), locked_at = NULL, worker_id = NULL
            WHERE task_id = $2 AND worker_id = $3
            "#,
        )
        .bind(&params.error)
        .bind(&params.child_step_task_id)
        .bind(&params.lock_token)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?
        .rows_affected();

        if rows_affected == 0 {
            tx.rollback()
                .await
                .map_err(|e| StorageError::Database(Box::new(e)))?;
            return Err(StorageError::LockMismatch(
                params.child_step_task_id.clone(),
            ));
        }

        let cas_row: Option<(bool,)> = sqlx::query_as(
            r#"
            UPDATE zart_steps
            SET wg_first_failed = TRUE
            WHERE run_id = $1
              AND step_name = $2
              AND step_kind = 'wait_group'
              AND COALESCE(wg_first_failed, FALSE) = FALSE
            RETURNING wg_first_failed
            "#,
        )
        .bind(&params.run_id)
        .bind(&params.group_step_name)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        tx.commit()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;

        Ok(cas_row.is_some())
    }

    async fn recover_wait_group_orphans(&self) -> Result<usize, StorageError> {
        let rows: Vec<(String, String, String)> = sqlx::query_as(
            r#"
            SELECT
                s.run_id,
                s.step_name,
                e.task_name
            FROM zart_steps s
            JOIN zart_execution_runs r ON r.run_id = s.run_id
            JOIN zart_executions e ON e.execution_id = r.execution_id
            WHERE s.step_kind = 'wait_group'
              AND s.wg_remaining IS NOT NULL
              AND s.wg_threshold IS NOT NULL
              AND s.wg_remaining = s.wg_threshold
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        let mut recovered = 0usize;
        for (run_id, step_name, task_name) in rows {
            let next_body_task_id = format!("{run_id}:body:after:{step_name}");
            let body_metadata = serde_json::json!({
                "mode": "body",
                "run_id": run_id,
                "execution_id": run_id.split(":run:").next().unwrap_or(&run_id),
            });

            let inserted = sqlx::query(
                r#"
                INSERT INTO zart_tasks (task_id, task_name, execution_time, data, metadata)
                SELECT $1, $2, NOW(), r.payload, $3
                FROM zart_execution_runs r
                WHERE r.run_id = $4
                ON CONFLICT (task_id) DO NOTHING
                "#,
            )
            .bind(&next_body_task_id)
            .bind(&task_name)
            .bind(&body_metadata)
            .bind(&run_id)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?
            .rows_affected();

            if inserted > 0 {
                recovered += 1;
            }
        }

        Ok(recovered)
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

        let row: Option<(
            String,
            StepStatus,
            Option<serde_json::Value>,
            Option<StepResultKind>,
        )> = sqlx::query_as(
            r#"
            SELECT step_id, status, result, result_kind
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
            Some((step_id, step_status, result, result_kind)) => {
                // Map step_status to the TaskStatus expected by StepLookup callers.
                let status = match step_status {
                    StepStatus::Scheduled => TaskStatus::Scheduled,
                    StepStatus::Running => TaskStatus::PickedUp,
                    StepStatus::Completed => TaskStatus::Completed,
                    StepStatus::Dead => TaskStatus::Dead,
                };
                Ok(Some(StepLookup {
                    task_id: step_id,
                    status,
                    result,
                    result_kind,
                }))
            }
        }
    }

    async fn get_current_run_id(&self, execution_id: &str) -> Result<Option<String>, StorageError> {
        let run_id: Option<String> = sqlx::query_scalar(
            r#"
            SELECT current_run_id FROM zart_executions WHERE execution_id = $1
            "#,
        )
        .bind(execution_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;
        Ok(run_id)
    }

    async fn list_runs(
        &self,
        execution_id: &str,
    ) -> Result<Vec<crate::ExecutionRunRecord>, StorageError> {
        let rows: Vec<(
            String,
            String,
            i32,
            serde_json::Value,
            ExecutionStatus,
            Option<serde_json::Value>,
            chrono::DateTime<chrono::Utc>,
            Option<chrono::DateTime<chrono::Utc>>,
            crate::ExecutionTrigger,
        )> = sqlx::query_as(
            r#"
            SELECT run_id, execution_id, run_index, payload, status,
                   result, started_at, completed_at, trigger
            FROM zart_execution_runs
            WHERE execution_id = $1
            ORDER BY run_index ASC
            "#,
        )
        .bind(execution_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        Ok(rows
            .into_iter()
            .map(
                |(
                    run_id,
                    execution_id,
                    run_index,
                    payload,
                    status,
                    result,
                    started_at,
                    completed_at,
                    trigger,
                )| crate::ExecutionRunRecord {
                    run_id,
                    execution_id,
                    run_index,
                    payload,
                    status,
                    result,
                    started_at,
                    completed_at,
                    trigger,
                },
            )
            .collect())
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
            StepKind,
            Option<String>,
            StepStatus,
            i32,
            Option<serde_json::Value>,
            Option<serde_json::Value>,
            Option<String>,
            Option<i32>,
            Option<i32>,
            Option<i32>,
            Option<bool>,
            chrono::DateTime<chrono::Utc>,
            Option<chrono::DateTime<chrono::Utc>>,
        )> = sqlx::query_as(
            r#"
            SELECT step_id, run_id, step_name, step_kind, task_id,
                   status, retry_attempt, retry_config, result, last_error,
                   wg_total, wg_remaining, wg_threshold, wg_first_failed,
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
                wg_total,
                wg_remaining,
                wg_threshold,
                wg_first_failed,
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
                wg_total,
                wg_remaining,
                wg_threshold,
                wg_first_failed,
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
            StepKind,
            Option<String>,
            StepStatus,
            i32,
            Option<serde_json::Value>,
            Option<serde_json::Value>,
            Option<String>,
            Option<i32>,
            Option<i32>,
            Option<i32>,
            Option<bool>,
            chrono::DateTime<chrono::Utc>,
            Option<chrono::DateTime<chrono::Utc>>,
        )> = sqlx::query_as(
            r#"
            SELECT step_id, run_id, step_name, step_kind, task_id,
                   status, retry_attempt, retry_config, result, last_error,
                   wg_total, wg_remaining, wg_threshold, wg_first_failed,
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
                    wg_total,
                    wg_remaining,
                    wg_threshold,
                    wg_first_failed,
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
                        wg_total,
                        wg_remaining,
                        wg_threshold,
                        wg_first_failed,
                        scheduled_at,
                        completed_at,
                    })
                },
            )
            .collect()
    }

    async fn schedule_step(
        &self,
        params: ScheduleStepParams,
    ) -> Result<crate::ScheduleResult, StorageError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;

        sqlx::query(
            r#"
            INSERT INTO zart_tasks (task_id, task_name, execution_time, data, metadata)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (task_id) DO NOTHING
            "#,
        )
        .bind(&params.task_id)
        .bind(&params.task_name)
        .bind(params.execution_time)
        .bind(&params.data)
        .bind(&params.metadata)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        let step_id = format!("{}:step:{}", params.run_id, params.step_name);
        sqlx::query(
            r#"
            INSERT INTO zart_steps (step_id, run_id, step_name, step_kind, task_id, retry_config)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (step_id) DO NOTHING
            "#,
        )
        .bind(&step_id)
        .bind(&params.run_id)
        .bind(&params.step_name)
        .bind(&params.step_kind)
        .bind(&params.task_id)
        .bind(&params.retry_config)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        tx.commit()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;

        Ok(crate::ScheduleResult {
            task_id: params.task_id,
            execution_time: params.execution_time,
        })
    }

    async fn complete_step_and_schedule_body(
        &self,
        params: CompleteStepAndScheduleBodyParams,
    ) -> Result<(), StorageError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;

        complete_step_and_schedule_body_sql(&mut tx, &params).await?;

        tx.commit()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;
        Ok(())
    }

    /// Complete a step and schedule the next body task within the caller's transaction.
    ///
    /// The caller is responsible for committing or rolling back the transaction.
    async fn complete_step_and_schedule_body_in_tx(
        &self,
        conn: &mut PgConnection,
        params: CompleteStepAndScheduleBodyParams,
    ) -> Result<(), StorageError> {
        complete_step_and_schedule_body_sql(conn, &params).await
    }

    async fn complete_step_no_resume(
        &self,
        params: CompleteStepNoResumeParams,
    ) -> Result<(), StorageError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;

        let attempt_id = format!("{}:attempt:{}", params.step_id, params.attempt_number);
        sqlx::query(
            r#"
            INSERT INTO zart_step_attempts (attempt_id, step_id, attempt_number, status, completed_at, result, error)
            VALUES ($1, $2, $3, 'completed', NOW(), $4, NULL)
            ON CONFLICT (attempt_id) DO NOTHING
            "#,
        )
        .bind(&attempt_id)
        .bind(&params.step_id)
        .bind(params.attempt_number as i32)
        .bind(&params.result)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        sqlx::query(
            r#"
            UPDATE zart_steps SET status = 'completed', result = $1, completed_at = $2 WHERE step_id = $3
            "#,
        )
        .bind(&params.result)
        .bind(Utc::now())
        .bind(&params.step_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        let rows_affected = sqlx::query(
            r#"
            UPDATE zart_tasks SET status = 'completed', result = $1, completed_at = NOW(), updated_at = NOW(), locked_at = NULL, worker_id = NULL
            WHERE task_id = $2 AND worker_id = $3
            "#,
        )
        .bind(&params.result)
        .bind(&params.step_task_id)
        .bind(&params.lock_token)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?
        .rows_affected();

        if rows_affected == 0 {
            tx.rollback()
                .await
                .map_err(|e| StorageError::Database(Box::new(e)))?;
            return Err(StorageError::LockMismatch(params.step_task_id.clone()));
        }

        tx.commit()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;
        Ok(())
    }

    async fn reschedule_step_for_retry(
        &self,
        params: RescheduleStepForRetryParams,
    ) -> Result<(), StorageError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;

        let attempt_id = format!("{}:attempt:{}", params.step_task_id, params.attempt_number);
        sqlx::query(
            r#"
            INSERT INTO zart_step_attempts (attempt_id, step_id, attempt_number, status, completed_at, result, error)
            VALUES ($1, $2, $3, 'failed', NOW(), NULL, $4)
            ON CONFLICT (attempt_id) DO NOTHING
            "#,
        )
        .bind(&attempt_id)
        .bind(&params.step_task_id)
        .bind(params.attempt_number as i32)
        .bind(&params.error)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        sqlx::query(
            r#"
            UPDATE zart_steps SET retry_attempt = $1, last_error = NULL WHERE step_id = $2
            "#,
        )
        .bind(params.attempt_number as i32 + 1)
        .bind(&params.step_task_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        let rows_affected = sqlx::query(
            r#"
            UPDATE zart_tasks SET status = 'scheduled', last_error = $1, execution_time = $2, locked_at = NULL, worker_id = NULL, updated_at = NOW()
            WHERE task_id = $3 AND worker_id = $4
            "#,
        )
        .bind(&params.error)
        .bind(params.retry_time)
        .bind(&params.step_task_id)
        .bind(&params.lock_token)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?
        .rows_affected();

        if rows_affected == 0 {
            tx.rollback()
                .await
                .map_err(|e| StorageError::Database(Box::new(e)))?;
            return Err(StorageError::LockMismatch(params.step_task_id.clone()));
        }

        tx.commit()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;
        Ok(())
    }

    async fn insert_completed_step(
        &self,
        run_id: &str,
        step_name: &str,
        step_kind: StepKind,
        result: serde_json::Value,
    ) -> Result<(), StorageError> {
        let step_id = format!("{run_id}:step:{step_name}");
        sqlx::query(
            r#"
            INSERT INTO zart_steps
                (step_id, run_id, step_name, step_kind, status, result, completed_at)
            VALUES
                ($1, $2, $3, $4, 'completed', $5, NOW())
            ON CONFLICT (step_id) DO NOTHING
            "#,
        )
        .bind(&step_id)
        .bind(run_id)
        .bind(step_name)
        .bind(step_kind)
        .bind(&result)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;
        Ok(())
    }

    async fn admin_retry_step(
        &self,
        run_id: &str,
        step_name: &str,
        _triggered_by: Option<&str>,
    ) -> Result<String, StorageError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;

        // 1. Find the dead step by run_id + step_name.
        let step_row: Option<(String, StepStatus, String)> = sqlx::query_as(
            r#"
            SELECT step_id, status, COALESCE(task_id, '')
            FROM zart_steps
            WHERE run_id = $1 AND step_name = $2
            "#,
        )
        .bind(run_id)
        .bind(step_name)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        let (step_id, current_status, old_task_id) = match step_row {
            None => {
                return Err(StorageError::StepNotFound(step_name.to_string()));
            }
            Some(row) => row,
        };

        if current_status != StepStatus::Dead {
            return Err(StorageError::StepStatusMismatch {
                step: step_name.to_string(),
                actual: format!("{current_status:?}"),
                expected: "Dead".to_string(),
            });
        }

        // 2. Extract metadata from the old task for reuse, but clear the
        //    step-level deadline. An admin retry gives the step a fresh chance,
        //    so the original deadline (which has already passed) must not be
        //    carried forward. The worker will apply a raw timeout duration
        //    when the step is executed.
        let task_metadata: serde_json::Value = if old_task_id.is_empty() {
            serde_json::json!({})
        } else {
            let meta_opt: Option<Option<serde_json::Value>> = sqlx::query_scalar(
                r#"
                SELECT metadata FROM zart_tasks WHERE task_id = $1
                "#,
            )
            .bind(&old_task_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;
            let mut meta = meta_opt.flatten().unwrap_or_else(|| serde_json::json!({}));
            // Clear the persisted deadline — the admin retry deserves a fresh
            // timeout, not a deadline that already expired.
            if let Some(obj) = meta.as_object_mut() {
                obj.remove("deadline");
            }
            meta
        };

        // 3. Create a new task for the retry.
        let new_task_id = format!(
            "{run_id}:step:retry:{step_name}:{}",
            Utc::now().timestamp_millis()
        );
        sqlx::query(
            r#"
            INSERT INTO zart_tasks (task_id, task_name, execution_time, data, metadata, status, attempt)
            SELECT $1, t.task_name, NOW(), t.data, $2, 'scheduled', 0
            FROM zart_tasks t
            WHERE t.task_id = $3
            "#,
        )
        .bind(&new_task_id)
        .bind(&task_metadata)
        .bind(&old_task_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        // 4. Update the step to scheduled with the new task.
        sqlx::query(
            r#"
            UPDATE zart_steps
            SET status = 'scheduled', task_id = $1, retry_attempt = 0, last_error = NULL, completed_at = NULL
            WHERE step_id = $2
            "#,
        )
        .bind(&new_task_id)
        .bind(&step_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        // 5. Set the run status back to running.
        sqlx::query(
            r#"
            UPDATE zart_execution_runs
            SET status = 'running', completed_at = NULL
            WHERE run_id = $1
            "#,
        )
        .bind(run_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        tx.commit()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;

        Ok(new_task_id)
    }

    async fn admin_restart_execution(
        &self,
        execution_id: &str,
        new_payload: Option<serde_json::Value>,
        triggered_by: Option<&str>,
    ) -> Result<String, StorageError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;

        // 1. Get the current run info.
        let current_run: Option<(String, ExecutionStatus, serde_json::Value)> = sqlx::query_as(
            r#"
            SELECT r.run_id, r.status, r.payload
            FROM zart_executions e
            JOIN zart_execution_runs r ON e.current_run_id = r.run_id
            WHERE e.execution_id = $1
            "#,
        )
        .bind(execution_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        let (current_run_id, current_status, current_payload) = match current_run {
            None => {
                // No run exists yet — this is a fresh execution.
                // Just create run 0 and schedule body.
                let run_id = format!("{execution_id}:run:0");
                let payload = new_payload.unwrap_or(serde_json::json!({}));

                sqlx::query(
                    r#"
                    INSERT INTO zart_executions (execution_id, task_name)
                    VALUES ($1, $2)
                    ON CONFLICT (execution_id) DO NOTHING
                    "#,
                )
                .bind(execution_id)
                .bind("") // task_name gets set below
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Database(Box::new(e)))?;

                // Get task_name
                let task_name: Option<String> = sqlx::query_scalar(
                    r#"
                    SELECT task_name FROM zart_executions WHERE execution_id = $1
                    "#,
                )
                .bind(execution_id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|e| StorageError::Database(Box::new(e)))?;

                let task_name = task_name.unwrap_or_default();

                sqlx::query(
                    r#"
                    INSERT INTO zart_execution_runs
                        (run_id, execution_id, run_index, payload, trigger, triggered_by)
                    VALUES ($1, $2, 0, $3, 'restart', $5)
                    "#,
                )
                .bind(&run_id)
                .bind(execution_id)
                .bind(&payload)
                .bind(triggered_by)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Database(Box::new(e)))?;

                sqlx::query(
                    r#"
                    UPDATE zart_executions
                    SET current_run_id = $1
                    WHERE execution_id = $2
                    "#,
                )
                .bind(&run_id)
                .bind(execution_id)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Database(Box::new(e)))?;

                // Schedule body
                let body_task_id = format!("{run_id}:body:start");
                let body_metadata = serde_json::json!({
                    "mode": "body",
                    "run_id": run_id,
                    "execution_id": execution_id,
                });
                sqlx::query(
                    r#"
                    INSERT INTO zart_tasks (task_id, task_name, execution_time, data, metadata)
                    VALUES ($1, $2, NOW(), $3, $4)
                    "#,
                )
                .bind(&body_task_id)
                .bind(&task_name)
                .bind(&payload)
                .bind(&body_metadata)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Database(Box::new(e)))?;

                tx.commit()
                    .await
                    .map_err(|e| StorageError::Database(Box::new(e)))?;
                return Ok(run_id);
            }
            Some(row) => row,
        };

        // Archive the current run — keep actual status so restarts of running
        // executions are distinguishable from natural completions.
        sqlx::query(
            r#"
            UPDATE zart_execution_runs
            SET status = $1, completed_at = COALESCE(completed_at, NOW())
            WHERE run_id = $2 AND completed_at IS NULL
            "#,
        )
        .bind(current_status)
        .bind(&current_run_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        // 2. Get the max run_index to compute the next one.
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

        // 3. Determine payload: use new_payload if provided, else keep existing.
        let payload = new_payload.unwrap_or(current_payload);

        // 4. Get task_name from execution record.
        let task_name: String = sqlx::query_scalar(
            r#"
            SELECT task_name FROM zart_executions WHERE execution_id = $1
            "#,
        )
        .bind(execution_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        // 5. Insert new run with trigger = 'restart'.
        sqlx::query(
            r#"
            INSERT INTO zart_execution_runs
                (run_id, execution_id, run_index, payload, trigger, triggered_by)
            VALUES ($1, $2, $3, $4, 'restart', $5)
            "#,
        )
        .bind(&new_run_id)
        .bind(execution_id)
        .bind(next_index)
        .bind(&payload)
        .bind(triggered_by)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        // 6. Update execution to point to the new run.
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

        // 7. Schedule a fresh body task at segment 0.
        let body_task_id = format!("{new_run_id}:body:start");
        let body_metadata = serde_json::json!({
            "mode": "body",
            "run_id": new_run_id,
            "execution_id": execution_id,
        });
        sqlx::query(
            r#"
            INSERT INTO zart_tasks (task_id, task_name, execution_time, data, metadata)
            VALUES ($1, $2, NOW(), $3, $4)
            "#,
        )
        .bind(&body_task_id)
        .bind(&task_name)
        .bind(&payload)
        .bind(&body_metadata)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        tx.commit()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;

        Ok(new_run_id)
    }

    async fn admin_rerun_steps(
        &self,
        execution_id: &str,
        force_rerun: &[String],
        preserve: &[String],
        triggered_by: Option<&str>,
    ) -> Result<(String, Vec<String>), StorageError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;

        // 1. Get the current run.
        let current_run: Option<(String, serde_json::Value, String)> = sqlx::query_as(
            r#"
            SELECT r.run_id, r.payload, e.task_name
            FROM zart_executions e
            JOIN zart_execution_runs r ON e.current_run_id = r.run_id
            WHERE e.execution_id = $1
            "#,
        )
        .bind(execution_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        let (current_run_id, payload, task_name) = match current_run {
            None => {
                return Err(StorageError::NotFound(format!(
                    "No run found for execution {execution_id}"
                )));
            }
            Some(row) => row,
        };

        // 2. Archive the current run.
        sqlx::query(
            r#"
            UPDATE zart_execution_runs
            SET status = COALESCE(NULLIF(status, 'running'), 'running'),
                completed_at = COALESCE(completed_at, NOW())
            WHERE run_id = $1 AND completed_at IS NULL
            "#,
        )
        .bind(&current_run_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        // 3. Fetch all steps in the current run with their status.
        let steps: Vec<(String, StepStatus)> = sqlx::query_as(
            r#"
            SELECT step_name, status
            FROM zart_steps
            WHERE run_id = $1
            ORDER BY scheduled_at ASC
            "#,
        )
        .bind(&current_run_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        // 4. Compute the effective rerun set:
        //    - All force_rerun steps (even if completed)
        //    - All dead steps (always rerun)
        let mut effective_rerun: HashSet<String> = force_rerun.iter().cloned().collect();
        for (name, status) in &steps {
            if matches!(status, StepStatus::Dead) {
                effective_rerun.insert(name.clone());
            }
        }

        // Remove preserved steps that are NOT failed/dead from the rerun set.
        let preserve_set: HashSet<&str> = preserve.iter().map(|s| s.as_str()).collect();
        let step_status_map: HashMap<&str, StepStatus> = steps
            .iter()
            .map(|(name, status)| (name.as_str(), status.clone()))
            .collect();
        for p in &preserve_set {
            if let Some(status) = step_status_map.get(p)
                && matches!(status, StepStatus::Completed)
            {
                effective_rerun.remove(*p);
            }
        }

        // 5. Compute the new run index.
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

        // 6. Insert new run.
        sqlx::query(
            r#"
            INSERT INTO zart_execution_runs
                (run_id, execution_id, run_index, payload, trigger, triggered_by)
            VALUES ($1, $2, $3, $4, 'selective_rerun', $5)
            "#,
        )
        .bind(&new_run_id)
        .bind(execution_id)
        .bind(next_index)
        .bind(&payload)
        .bind(triggered_by)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        // 7. Update execution pointer.
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

        // 8. Schedule fresh body task.
        let body_task_id = format!("{new_run_id}:body:start");
        let body_metadata = serde_json::json!({
            "mode": "body",
            "run_id": new_run_id,
            "execution_id": execution_id,
        });
        sqlx::query(
            r#"
            INSERT INTO zart_tasks (task_id, task_name, execution_time, data, metadata)
            VALUES ($1, $2, NOW(), $3, $4)
            "#,
        )
        .bind(&body_task_id)
        .bind(&task_name)
        .bind(&payload)
        .bind(&body_metadata)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        tx.commit()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;

        Ok((new_run_id, effective_rerun.into_iter().collect()))
    }
}
