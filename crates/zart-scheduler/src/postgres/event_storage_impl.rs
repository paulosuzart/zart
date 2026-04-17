//! Event delivery and admin operations for [`PostgresScheduler`].
//!
//! This module handles delivering external events to waiting executions,
//! retrying dead steps, and execution statistics reporting.

use chrono::Utc;

use super::PostgresScheduler;
use crate::{EventDeliveryResult, ExecutionStats, StepStatus, StorageError, TaskMetadata};

pub(crate) trait EventStorage: Sized {
    async fn deliver_event(
        &self,
        execution_id: &str,
        event_name: &str,
        payload: serde_json::Value,
    ) -> Result<EventDeliveryResult, StorageError>;

    async fn admin_retry_step(
        &self,
        run_id: &str,
        step_name: &str,
        triggered_by: Option<&str>,
    ) -> Result<String, StorageError>;

    async fn execution_stats(&self) -> Result<ExecutionStats, StorageError>;
}

impl EventStorage for PostgresScheduler {
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

        let exec_row: Option<(String, String, serde_json::Value)> = sqlx::query_as(&format!(
            r#"
            SELECT e.current_run_id, e.task_name, r.payload
            FROM {executions} e
            JOIN {execution_runs} r ON r.run_id = e.current_run_id
            WHERE e.execution_id = $1
            "#,
            executions = self.table_names.executions(),
            execution_runs = self.table_names.execution_runs(),
        ))
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

        let completed_row: Option<(String,)> = sqlx::query_as(&format!(
            r#"
            SELECT step_id
            FROM {steps}
            WHERE run_id = $1
              AND step_name = $2
              AND step_kind = 'wait_for_event'
              AND status = 'completed'
            "#,
            steps = self.table_names.steps(),
        ))
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

        let step_row: Option<(String,)> = sqlx::query_as(&format!(
            r#"
            SELECT step_id
            FROM {steps}
            WHERE run_id = $1
              AND step_name = $2
              AND step_kind = 'wait_for_event'
              AND status = 'scheduled'
            "#,
            steps = self.table_names.steps(),
        ))
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

        let updated = sqlx::query(&format!(
            r#"
            UPDATE {steps}
            SET status = 'completed',
                result = $1,
                completed_at = NOW()
            WHERE step_id = $2
              AND status = 'scheduled'
            "#,
            steps = self.table_names.steps(),
        ))
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
        let body_metadata = TaskMetadata::body(&run_id, execution_id).to_json_value();

        sqlx::query(&format!(
            r#"
            INSERT INTO {tasks} (task_id, task_name, execution_time, data, metadata)
            VALUES ($1, $2, NOW(), $3, $4)
            ON CONFLICT (task_id) DO NOTHING
            "#,
            tasks = self.table_names.tasks(),
        ))
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

        let step_row: Option<(String, StepStatus, String)> = sqlx::query_as(&format!(
            r#"
            SELECT step_id, status, COALESCE(task_id, '')
            FROM {steps}
            WHERE run_id = $1 AND step_name = $2
            "#,
            steps = self.table_names.steps(),
        ))
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

        let task_metadata: serde_json::Value = if old_task_id.is_empty() {
            serde_json::json!({})
        } else {
            let meta_opt: Option<Option<serde_json::Value>> = sqlx::query_scalar(&format!(
                r#"
                SELECT metadata FROM {tasks} WHERE task_id = $1
                "#,
                tasks = self.table_names.tasks(),
            ))
            .bind(&old_task_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;
            let mut meta = meta_opt.flatten().unwrap_or_else(|| serde_json::json!({}));
            if let Some(obj) = meta.as_object_mut() {
                obj.remove("deadline");
            }
            meta
        };

        let new_task_id = format!(
            "{run_id}:step:retry:{step_name}:{}",
            Utc::now().timestamp_millis()
        );
        sqlx::query(&format!(
            r#"
            INSERT INTO {tasks} (task_id, task_name, execution_time, data, metadata, status, attempt)
            SELECT $1, t.task_name, NOW(), t.data, $2, 'scheduled', 0
            FROM {tasks} t
            WHERE t.task_id = $3
            "#,
            tasks = self.table_names.tasks(),
        ))
        .bind(&new_task_id)
        .bind(&task_metadata)
        .bind(&old_task_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        sqlx::query(&format!(
            r#"
            UPDATE {steps}
            SET status = 'scheduled', task_id = $1, retry_attempt = 0, last_error = NULL, completed_at = NULL
            WHERE step_id = $2
            "#,
            steps = self.table_names.steps(),
        ))
        .bind(&new_task_id)
        .bind(&step_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        sqlx::query(&format!(
            r#"
            UPDATE {execution_runs}
            SET status = 'running', completed_at = NULL
            WHERE run_id = $1
            "#,
            execution_runs = self.table_names.execution_runs(),
        ))
        .bind(run_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        tx.commit()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;

        Ok(new_task_id)
    }

    async fn execution_stats(&self) -> Result<ExecutionStats, StorageError> {
        let row: (i64, i64, i64, i64, i64) = sqlx::query_as(&format!(
            r#"
            SELECT
                COALESCE(SUM(CASE WHEN r.status = 'scheduled' THEN 1 END), 0),
                COALESCE(SUM(CASE WHEN r.status = 'running' THEN 1 END), 0),
                COALESCE(SUM(CASE WHEN r.status = 'completed' THEN 1 END), 0),
                COALESCE(SUM(CASE WHEN r.status = 'failed' THEN 1 END), 0),
                COALESCE(SUM(CASE WHEN r.status = 'cancelled' THEN 1 END), 0)
            FROM {executions} e
            JOIN {execution_runs} r ON e.current_run_id = r.run_id
            "#,
            executions = self.table_names.executions(),
            execution_runs = self.table_names.execution_runs(),
        ))
        .fetch_one(&self.pool)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        Ok(ExecutionStats {
            scheduled: row.0,
            running: row.1,
            completed: row.2,
            failed: row.3,
            cancelled: row.4,
        })
    }
}
