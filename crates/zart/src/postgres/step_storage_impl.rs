//! PostgreSQL implementation of [`StepStore`] for [`PostgresStorage`].

use async_trait::async_trait;
use chrono::Utc;
use sqlx::PgConnection;
use zart_core::StorageError;
use zart_core::store::StepStore;
use zart_core::types::{
    CompleteStepAndScheduleBodyParams, CompleteStepNoResumeParams, RescheduleStepForRetryParams,
    ScheduleResult, ScheduleStepParams, StepAttemptRow, StepAttemptStatus, StepKind, StepLookup,
    StepResultKind, StepRow, StepStatus, TaskStatus,
};

use super::PostgresStorage;
use super::sql_helpers::complete_step_and_schedule_body_sql;

#[async_trait]
impl StepStore for PostgresStorage {
    async fn get_step_status(
        &self,
        run_id: &str,
        step_name: &str,
    ) -> Result<Option<StepLookup>, StorageError> {
        let task_id = format!("{run_id}:step:{step_name}");

        let row: Option<(
            String,
            StepStatus,
            Option<serde_json::Value>,
            Option<StepResultKind>,
        )> = sqlx::query_as(&format!(
            r#"
            SELECT step_id, status, result, result_kind
            FROM {steps}
            WHERE step_id = $1
            "#,
            steps = self.table_names.steps(),
        ))
        .bind(&task_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        match row {
            None => Ok(None),
            Some((step_id, step_status, result, result_kind)) => {
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

    #[allow(clippy::type_complexity)]
    async fn get_step(
        &self,
        run_id: &str,
        step_name: &str,
    ) -> Result<Option<StepRow>, StorageError> {
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
        )> = sqlx::query_as(&format!(
            r#"
            SELECT step_id, run_id, step_name, step_kind, task_id,
                   status, retry_attempt, retry_config, result, last_error,
                   wg_total, wg_remaining, wg_threshold, wg_first_failed,
                   scheduled_at, completed_at
            FROM {steps}
            WHERE run_id = $1 AND step_name = $2
            "#,
            steps = self.table_names.steps(),
        ))
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

    #[allow(clippy::type_complexity)]
    async fn list_steps(&self, run_id: &str) -> Result<Vec<StepRow>, StorageError> {
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
        )> = sqlx::query_as(&format!(
            r#"
            SELECT step_id, run_id, step_name, step_kind, task_id,
                   status, retry_attempt, retry_config, result, last_error,
                   wg_total, wg_remaining, wg_threshold, wg_first_failed,
                   scheduled_at, completed_at
            FROM {steps}
            WHERE run_id = $1
            ORDER BY scheduled_at ASC
            "#,
            steps = self.table_names.steps(),
        ))
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
    ) -> Result<ScheduleResult, StorageError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;

        sqlx::query(&format!(
            r#"
            INSERT INTO {tasks} (task_id, task_name, execution_time, data, metadata)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (task_id) DO NOTHING
            "#,
            tasks = self.table_names.tasks(),
        ))
        .bind(&params.task_id)
        .bind(&params.task_name)
        .bind(params.execution_time)
        .bind(&params.data)
        .bind(&params.metadata)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        let step_id = format!("{}:step:{}", params.run_id, params.step_name);
        sqlx::query(&format!(
            r#"
            INSERT INTO {steps} (step_id, run_id, step_name, step_kind, task_id, retry_config)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (step_id) DO NOTHING
            "#,
            steps = self.table_names.steps(),
        ))
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

        Ok(ScheduleResult {
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

        complete_step_and_schedule_body_sql(&mut tx, &params, &self.table_names).await?;

        tx.commit()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;
        Ok(())
    }

    async fn complete_step_and_schedule_body_in_tx(
        &self,
        conn: &mut PgConnection,
        params: CompleteStepAndScheduleBodyParams,
    ) -> Result<(), StorageError> {
        complete_step_and_schedule_body_sql(conn, &params, &self.table_names).await
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
        sqlx::query(&format!(
            r#"
            INSERT INTO {step_attempts} (attempt_id, step_id, attempt_number, status, completed_at, result, error)
            VALUES ($1, $2, $3, 'completed', NOW(), $4, NULL)
            ON CONFLICT (attempt_id) DO NOTHING
            "#,
            step_attempts = self.table_names.step_attempts(),
        ))
        .bind(&attempt_id)
        .bind(&params.step_id)
        .bind(params.attempt_number as i32)
        .bind(&params.result)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        sqlx::query(&format!(
            r#"
            UPDATE {steps} SET status = 'completed', result = $1, completed_at = $2 WHERE step_id = $3
            "#,
            steps = self.table_names.steps(),
        ))
        .bind(&params.result)
        .bind(Utc::now())
        .bind(&params.step_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        let rows_affected = sqlx::query(&format!(
            r#"
            UPDATE {tasks} SET status = 'completed', result = $1, completed_at = NOW(), updated_at = NOW(), locked_at = NULL, worker_id = NULL
            WHERE task_id = $2 AND worker_id = $3
            "#,
            tasks = self.table_names.tasks(),
        ))
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
        sqlx::query(&format!(
            r#"
            INSERT INTO {step_attempts} (attempt_id, step_id, attempt_number, status, completed_at, result, error)
            VALUES ($1, $2, $3, 'failed', NOW(), NULL, $4)
            ON CONFLICT (attempt_id) DO NOTHING
            "#,
            step_attempts = self.table_names.step_attempts(),
        ))
        .bind(&attempt_id)
        .bind(&params.step_task_id)
        .bind(params.attempt_number as i32)
        .bind(&params.error)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        sqlx::query(&format!(
            r#"
            UPDATE {steps} SET retry_attempt = $1, last_error = NULL WHERE step_id = $2
            "#,
            steps = self.table_names.steps(),
        ))
        .bind(params.attempt_number as i32 + 1)
        .bind(&params.step_task_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        let rows_affected = sqlx::query(&format!(
            r#"
            UPDATE {tasks} SET status = 'scheduled', last_error = $1, execution_time = $2, locked_at = NULL, worker_id = NULL, updated_at = NOW()
            WHERE task_id = $3 AND worker_id = $4
            "#,
            tasks = self.table_names.tasks(),
        ))
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
        sqlx::query(&format!(
            r#"
            INSERT INTO {steps}
                (step_id, run_id, step_name, step_kind, status, result, completed_at)
            VALUES
                ($1, $2, $3, $4, 'completed', $5, NOW())
            ON CONFLICT (step_id) DO NOTHING
            "#,
            steps = self.table_names.steps(),
        ))
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

    async fn check_wait_all_children(
        &self,
        wait_for_task_ids: &[String],
    ) -> Result<Vec<(String, serde_json::Value)>, StorageError> {
        if wait_for_task_ids.is_empty() {
            return Ok(vec![]);
        }

        let rows: Vec<(String, Option<serde_json::Value>)> = sqlx::query_as(&format!(
            r#"
            SELECT task_id, result
            FROM {tasks}
            WHERE task_id = ANY($1)
              AND status  = 'completed'
              AND result  IS NOT NULL
            "#,
            tasks = self.table_names.tasks(),
        ))
        .bind(wait_for_task_ids)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        Ok(rows
            .into_iter()
            .filter_map(|(id, r)| r.map(|v| (id, v)))
            .collect())
    }

    #[allow(clippy::type_complexity)]
    async fn list_step_attempts(&self, run_id: &str) -> Result<Vec<StepAttemptRow>, StorageError> {
        let rows: Vec<(
            String,
            String,
            i32,
            chrono::DateTime<chrono::Utc>,
            Option<chrono::DateTime<chrono::Utc>>,
            StepAttemptStatus,
            Option<serde_json::Value>,
            Option<String>,
        )> = sqlx::query_as(&format!(
            r#"
            SELECT a.attempt_id, a.step_id, a.attempt_number,
                   a.started_at, a.completed_at, a.status, a.result, a.error
            FROM {step_attempts} a
            JOIN {steps} s ON a.step_id = s.step_id
            WHERE s.run_id = $1
            ORDER BY s.scheduled_at ASC, a.attempt_number ASC
            "#,
            step_attempts = self.table_names.step_attempts(),
            steps = self.table_names.steps(),
        ))
        .bind(run_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        Ok(rows
            .into_iter()
            .map(
                |(
                    attempt_id,
                    step_id,
                    attempt_number,
                    started_at,
                    completed_at,
                    status,
                    result,
                    error,
                )| {
                    StepAttemptRow {
                        attempt_id,
                        step_id,
                        attempt_number,
                        started_at,
                        completed_at,
                        status,
                        result,
                        error,
                    }
                },
            )
            .collect())
    }
}
