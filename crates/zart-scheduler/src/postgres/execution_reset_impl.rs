//! Execution reset and restart operations for [`PostgresScheduler`].
//!
//! This module handles resetting, restarting, and rerunning executions.
//! It includes admin operations for manual intervention.

use std::collections::HashSet;

use super::PostgresScheduler;
use crate::{
    ExecutionRunRecord, ExecutionStatus, ExecutionTrigger, StepStatus, StorageError, TaskMetadata,
};

/// Internal extension trait for execution reset/restart operations.
/// Not part of the public API — used to modularize the DurableStorage impl.
pub(crate) trait ExecutionReset: Sized {
    async fn reset_execution(
        &self,
        execution_id: &str,
        payload: serde_json::Value,
    ) -> Result<String, StorageError>;

    async fn get_current_run_id(&self, execution_id: &str) -> Result<Option<String>, StorageError>;

    async fn list_runs(&self, execution_id: &str) -> Result<Vec<ExecutionRunRecord>, StorageError>;

    async fn admin_restart_execution(
        &self,
        execution_id: &str,
        new_payload: Option<serde_json::Value>,
        triggered_by: Option<&str>,
    ) -> Result<String, StorageError>;

    async fn admin_rerun_steps(
        &self,
        execution_id: &str,
        force_rerun: &[String],
        preserve: &[String],
        triggered_by: Option<&str>,
    ) -> Result<(String, Vec<String>), StorageError>;
}

impl ExecutionReset for PostgresScheduler {
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

    async fn get_current_run_id(&self, execution_id: &str) -> Result<Option<String>, StorageError> {
        let run_id: Option<String> = sqlx::query_scalar(&format!(
            r#"
            SELECT current_run_id FROM {executions} WHERE execution_id = $1
            "#,
            executions = self.table_names.executions(),
        ))
        .bind(execution_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;
        Ok(run_id)
    }

    #[allow(clippy::type_complexity)]
    async fn list_runs(&self, execution_id: &str) -> Result<Vec<ExecutionRunRecord>, StorageError> {
        let rows: Vec<(
            String,
            String,
            i32,
            serde_json::Value,
            ExecutionStatus,
            Option<serde_json::Value>,
            chrono::DateTime<chrono::Utc>,
            Option<chrono::DateTime<chrono::Utc>>,
            ExecutionTrigger,
        )> = sqlx::query_as(&format!(
            r#"
            SELECT run_id, execution_id, run_index, payload, status,
                   result, started_at, completed_at, trigger
            FROM {execution_runs}
            WHERE execution_id = $1
            ORDER BY run_index ASC
            "#,
            execution_runs = self.table_names.execution_runs(),
        ))
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
                )| ExecutionRunRecord {
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
                    VALUES ($1, $2, 0, $3, 'restart', $5)
                    "#,
                    execution_runs = self.table_names.execution_runs(),
                ))
                .bind(&run_id)
                .bind(execution_id)
                .bind(&payload)
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
            VALUES ($1, $2, $3, $4, 'restart', $5)
            "#,
            execution_runs = self.table_names.execution_runs(),
        ))
        .bind(&new_run_id)
        .bind(execution_id)
        .bind(next_index)
        .bind(&payload)
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

        let current_run: Option<(String, serde_json::Value, String)> = sqlx::query_as(&format!(
            r#"
            SELECT r.run_id, r.payload, e.task_name
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

        let (current_run_id, payload, task_name) = match current_run {
            None => {
                return Err(StorageError::NotFound(format!(
                    "No run found for execution {execution_id}"
                )));
            }
            Some(row) => row,
        };

        sqlx::query(&format!(
            r#"
            UPDATE {execution_runs}
            SET status = COALESCE(NULLIF(status, 'running'), 'running'),
                completed_at = COALESCE(completed_at, NOW())
            WHERE run_id = $1 AND completed_at IS NULL
            "#,
            execution_runs = self.table_names.execution_runs(),
        ))
        .bind(&current_run_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        let steps: Vec<(String, StepStatus)> = sqlx::query_as(&format!(
            r#"
            SELECT step_name, status
            FROM {steps}
            WHERE run_id = $1
            ORDER BY scheduled_at ASC
            "#,
            steps = self.table_names.steps(),
        ))
        .bind(&current_run_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        let mut effective_rerun: HashSet<String> = force_rerun.iter().cloned().collect();
        for (name, status) in &steps {
            if matches!(status, StepStatus::Dead) {
                effective_rerun.insert(name.clone());
            }
        }

        let preserve_set: HashSet<&str> = preserve.iter().map(|s| s.as_str()).collect();
        use std::collections::HashMap;
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
                (run_id, execution_id, run_index, payload, trigger, triggered_by)
            VALUES ($1, $2, $3, $4, 'selective_rerun', $5)
            "#,
            execution_runs = self.table_names.execution_runs(),
        ))
        .bind(&new_run_id)
        .bind(execution_id)
        .bind(next_index)
        .bind(&payload)
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

        Ok((new_run_id, effective_rerun.into_iter().collect()))
    }
}
