//! Admin SQL helpers — inherent methods on [`PostgresStorage`].
//!
//! These implement the orchestration currently in `ExecutionStore::retry_dead_step`,
//! `restart_run`, and `reset_execution`. They are called by the `ExecutionStore`
//! trait impl in `execution_storage_impl.rs`.
//!
//! In Phase 3, the orchestration logic will move to `ExecutionService` which
//! will call the fine-grained `create_run`/`set_current_run` primitives.

use chrono::Utc;
use zart_core::StorageError;
use zart_core::task_metadata::TaskMetadata;
use zart_core::types::{ExecutionStatus, ScheduleAtParams, StepStatus};

use super::PostgresStorage;
use super::sql_helpers::copy_steps_and_attempts_sql;

impl PostgresStorage {
    /// Atomically validate a step is `dead`, create a retry task, and reset the run.
    pub(super) async fn do_retry_dead_step(
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
            None => return Err(StorageError::StepNotFound(step_name.to_string())),
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

        // Fetch the data for the new task from the old task row.
        let retry_data: serde_json::Value = sqlx::query_scalar(&format!(
            r#"SELECT data FROM {tasks} WHERE task_id = $1"#,
            tasks = self.table_names.tasks(),
        ))
        .bind(&old_task_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?
        .unwrap_or(serde_json::json!({}));

        // Schedule the retry task via the task_scheduler delegate (no task-queue SQL here).
        self.task_scheduler
            .schedule_at_in_tx(
                &mut tx,
                ScheduleAtParams {
                    task_id: new_task_id.clone(),
                    task_name: crate::TASK_NAME.to_string(),
                    execution_time: Utc::now(),
                    data: retry_data,
                    recurrence: None,
                    metadata: task_metadata.clone(),
                },
            )
            .await?;

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

    /// Archive the current run and start a fresh one, scheduling a new body task.
    ///
    /// When `preserved_step_names` is non-empty, completed step rows (and their
    /// attempt history) are copied from the old run into the new run inside the
    /// same transaction, making the restart + copy atomic.
    pub(super) async fn do_restart_run(
        &self,
        execution_id: &str,
        new_payload: Option<serde_json::Value>,
        trigger: &str,
        triggered_by: Option<&str>,
        preserved_step_names: &[String],
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
                // Bootstrap a first run for fresh execution
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

                sqlx::query(&format!(
                    r#"
                    INSERT INTO {execution_runs}
                        (run_id, execution_id, run_index, payload, trigger, triggered_by)
                    VALUES ($1, $2, 0, $3, $4::execution_trigger, $5)
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
                self.task_scheduler
                    .schedule_at_in_tx(
                        &mut tx,
                        ScheduleAtParams {
                            task_id: body_task_id,
                            task_name: crate::TASK_NAME.to_string(),
                            execution_time: Utc::now(),
                            data: payload,
                            recurrence: None,
                            metadata: TaskMetadata::body(&run_id, execution_id).to_json_value(),
                        },
                    )
                    .await?;

                tx.commit()
                    .await
                    .map_err(|e| StorageError::Database(Box::new(e)))?;
                return Ok(run_id);
            }
            Some(row) => row,
        };

        // Archive the current run
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

        sqlx::query(&format!(
            r#"
            INSERT INTO {execution_runs}
                (run_id, execution_id, run_index, payload, trigger, triggered_by)
            VALUES ($1, $2, $3, $4, $5::execution_trigger, $6)
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

        if !preserved_step_names.is_empty() {
            copy_steps_and_attempts_sql(
                &mut tx,
                &current_run_id,
                &new_run_id,
                preserved_step_names,
                &self.table_names,
            )
            .await?;
        }

        let body_task_id = format!("{new_run_id}:body:start");
        self.task_scheduler
            .schedule_at_in_tx(
                &mut tx,
                ScheduleAtParams {
                    task_id: body_task_id,
                    task_name: crate::TASK_NAME.to_string(),
                    execution_time: Utc::now(),
                    data: payload,
                    recurrence: None,
                    metadata: TaskMetadata::body(&new_run_id, execution_id).to_json_value(),
                },
            )
            .await?;

        tx.commit()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;

        Ok(new_run_id)
    }
}
