//! PostgreSQL implementation of [`AdminRepository`] for [`PostgresScheduler`].
//!
//! Covers manual intervention operations: retrying dead steps, restarting
//! executions, selectively rerunning steps, and the `reset_execution` primitive
//! that underlies restart/rerun.
//!
//! Note (spec 0034 Phase 2): the business logic in `admin_retry_step`,
//! `admin_restart_execution`, and `admin_rerun_steps` will move to
//! `ExecutionService` in the `zart` crate. Only the SQL primitives will remain.

use chrono::Utc;
use std::collections::{HashMap, HashSet};

use super::PostgresScheduler;
use crate::repository::AdminRepository;
use crate::{ExecutionStatus, StepStatus, StorageError, TaskMetadata};

impl AdminRepository for PostgresScheduler {
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
