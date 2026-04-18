//! PostgreSQL implementation of [`AdminRepository`] for [`PostgresScheduler`].
//!
//! Provides raw SQL primitives for admin intervention:
//! - `retry_dead_step` — atomically reset a dead step and schedule a retry task
//! - `restart_run` — archive the current run and start a fresh one
//! - `reset_execution` — create a new run for a terminal execution (no body task)
//!
//! Business logic (effective-rerun computation, step-status validation beyond
//! what is atomic with the transaction) lives in `ExecutionService` in the
//! `zart` crate.

use chrono::Utc;

use super::PostgresScheduler;
use crate::repository::AdminRepository;
use crate::{ExecutionStatus, StepStatus, StorageError, TaskMetadata};

impl AdminRepository for PostgresScheduler {
    async fn retry_dead_step(
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

    async fn restart_run(
        &self,
        execution_id: &str,
        new_payload: Option<serde_json::Value>,
        trigger: &str,
        triggered_by: Option<&str>,
    ) -> Result<String, StorageError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;

        let current_run: Option<(String, ExecutionStatus, serde_json::Value)> =
            sqlx::query_as(&format!(
                r#"
            SELECT r.run_id, r.status, r.payload
            FROM {executions} e
            JOIN {execution_runs} r ON e.current_run_id = r.run_id
            WHERE e.execution_id = $1
            "#,
                executions = self.table_names.executions(),
                execution_runs = self.table_names.execution_runs(),
            ))
            .bind(execution_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;

        let (current_run_id, current_status, current_payload) = match current_run {
            None => {
                // No existing run — bootstrap a first run (edge case for restart on fresh execution)
                let run_id = format!("{execution_id}:run:0");
                let payload = new_payload.unwrap_or(serde_json::json!({}));

                sqlx::query(&format!(
                    r#"
                    INSERT INTO {executions} (execution_id, task_name)
                    VALUES ($1, $2)
                    ON CONFLICT (execution_id) DO NOTHING
                    "#,
                    executions = self.table_names.executions(),
                ))
                .bind(execution_id)
                .bind("")
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Database(Box::new(e)))?;

                let task_name: Option<String> = sqlx::query_scalar(&format!(
                    r#"
                    SELECT task_name FROM {executions} WHERE execution_id = $1
                    "#,
                    executions = self.table_names.executions(),
                ))
                .bind(execution_id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|e| StorageError::Database(Box::new(e)))?;

                let task_name = task_name.unwrap_or_default();

                sqlx::query(&format!(
                    r#"
                    INSERT INTO {execution_runs}
                        (run_id, execution_id, run_index, payload, trigger, triggered_by)
                    VALUES ($1, $2, 0, $3, $4, $5)
                    "#,
                    execution_runs = self.table_names.execution_runs(),
                ))
                .bind(&run_id)
                .bind(execution_id)
                .bind(&payload)
                .bind(trigger)
                .bind(triggered_by)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Database(Box::new(e)))?;

                sqlx::query(&format!(
                    r#"
                    UPDATE {executions}
                    SET current_run_id = $1
                    WHERE execution_id = $2
                    "#,
                    executions = self.table_names.executions(),
                ))
                .bind(&run_id)
                .bind(execution_id)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Database(Box::new(e)))?;

                let body_task_id = format!("{run_id}:body:start");
                let body_metadata = TaskMetadata::body(&run_id, execution_id).to_json_value();
                sqlx::query(&format!(
                    r#"
                    INSERT INTO {tasks} (task_id, task_name, execution_time, data, metadata)
                    VALUES ($1, $2, NOW(), $3, $4)
                    "#,
                    tasks = self.table_names.tasks(),
                ))
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

        // Archive the current run (freeze its final status)
        sqlx::query(&format!(
            r#"
            UPDATE {execution_runs}
            SET status = $1, completed_at = COALESCE(completed_at, NOW())
            WHERE run_id = $2 AND completed_at IS NULL
            "#,
            execution_runs = self.table_names.execution_runs(),
        ))
        .bind(current_status)
        .bind(&current_run_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        let max_index: i32 = sqlx::query_scalar(&format!(
            r#"
            SELECT COALESCE(MAX(run_index), -1)
            FROM {execution_runs}
            WHERE execution_id = $1
            "#,
            execution_runs = self.table_names.execution_runs(),
        ))
        .bind(execution_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        let next_index = max_index + 1;
        let new_run_id = format!("{execution_id}:run:{next_index}");
        let payload = new_payload.unwrap_or(current_payload);

        let task_name: String = sqlx::query_scalar(&format!(
            r#"
            SELECT task_name FROM {executions} WHERE execution_id = $1
            "#,
            executions = self.table_names.executions(),
        ))
        .bind(execution_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        sqlx::query(&format!(
            r#"
            INSERT INTO {execution_runs}
                (run_id, execution_id, run_index, payload, trigger, triggered_by)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
            execution_runs = self.table_names.execution_runs(),
        ))
        .bind(&new_run_id)
        .bind(execution_id)
        .bind(next_index)
        .bind(&payload)
        .bind(trigger)
        .bind(triggered_by)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        sqlx::query(&format!(
            r#"
            UPDATE {executions}
            SET current_run_id = $1
            WHERE execution_id = $2
            "#,
            executions = self.table_names.executions(),
        ))
        .bind(&new_run_id)
        .bind(execution_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        let body_task_id = format!("{new_run_id}:body:start");
        let body_metadata = TaskMetadata::body(&new_run_id, execution_id).to_json_value();
        sqlx::query(&format!(
            r#"
            INSERT INTO {tasks} (task_id, task_name, execution_time, data, metadata)
            VALUES ($1, $2, NOW(), $3, $4)
            "#,
            tasks = self.table_names.tasks(),
        ))
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

        let max_index: i32 = sqlx::query_scalar(&format!(
            r#"
            SELECT COALESCE(MAX(run_index), -1)
            FROM {execution_runs}
            WHERE execution_id = $1
            "#,
            execution_runs = self.table_names.execution_runs(),
        ))
        .bind(execution_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        let next_index = max_index + 1;
        let new_run_id = format!("{execution_id}:run:{next_index}");

        sqlx::query(&format!(
            r#"
            INSERT INTO {execution_runs}
                (run_id, execution_id, run_index, payload, trigger)
            VALUES ($1, $2, $3, $4, 'restart')
            "#,
            execution_runs = self.table_names.execution_runs(),
        ))
        .bind(&new_run_id)
        .bind(execution_id)
        .bind(next_index)
        .bind(&payload)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        sqlx::query(&format!(
            r#"
            UPDATE {executions}
            SET current_run_id = $1
            WHERE execution_id = $2
            "#,
            executions = self.table_names.executions(),
        ))
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
}
