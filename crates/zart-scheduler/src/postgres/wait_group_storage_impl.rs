//! PostgreSQL implementation of [`WaitGroupRepository`] and [`WaitGroupStore`] for [`PostgresScheduler`].
//!
//! Covers upserting wait-group steps, completing/failing children, and
//! recovering orphaned groups. Step-level operations (schedule, complete)
//! live in `step_storage_impl`.

use async_trait::async_trait;

use super::PostgresScheduler;
use crate::repository::WaitGroupRepository;
use crate::store::WaitGroupStore;
use crate::{
    CompleteWaitGroupChildParams, FailWaitGroupChildParams, StorageError, TaskMetadata,
    UpsertWaitGroupStepParams,
};

impl WaitGroupRepository for PostgresScheduler {
    async fn upsert_wait_group_step(
        &self,
        params: UpsertWaitGroupStepParams,
    ) -> Result<(), StorageError> {
        let step_id = format!("{}:step:{}", params.run_id, params.group_step_name);
        sqlx::query(&format!(
            r#"
            INSERT INTO {steps}
                (step_id, run_id, step_name, step_kind, status,
                 wg_total, wg_remaining, wg_threshold, wg_first_failed)
            VALUES
                ($1, $2, $3, 'wait_group', 'scheduled',
                 $4, $4, $5, FALSE)
            ON CONFLICT (step_id) DO NOTHING
            "#,
            steps = self.table_names.steps(),
        ))
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
        sqlx::query(&format!(
            r#"
            INSERT INTO {step_attempts} (attempt_id, step_id, attempt_number, status, completed_at, result, error)
            VALUES ($1, $2, $3, 'completed', NOW(), $4, NULL)
            ON CONFLICT (attempt_id) DO NOTHING
            "#,
            step_attempts = self.table_names.step_attempts(),
        ))
        .bind(&attempt_id)
        .bind(&params.child_step_id)
        .bind(params.attempt_number as i32)
        .bind(&params.child_result)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        sqlx::query(&format!(
            r#"
            UPDATE {steps}
            SET status = 'completed', result = $1, completed_at = NOW()
            WHERE step_id = $2
            "#,
            steps = self.table_names.steps(),
        ))
        .bind(&params.child_result)
        .bind(&params.child_step_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        let rows_affected = sqlx::query(&format!(
            r#"
            UPDATE {tasks}
            SET status = 'completed', result = $1, completed_at = NOW(), updated_at = NOW(), locked_at = NULL, worker_id = NULL
            WHERE task_id = $2 AND worker_id = $3
            "#,
            tasks = self.table_names.tasks(),
        ))
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

        let wg_row: Option<(i32, i32)> = sqlx::query_as(&format!(
            r#"
            UPDATE {steps}
            SET wg_remaining = wg_remaining - 1
            WHERE run_id = $1
              AND step_name = $2
              AND step_kind = 'wait_group'
              AND wg_remaining IS NOT NULL
              AND wg_threshold IS NOT NULL
            RETURNING wg_remaining, wg_threshold
            "#,
            steps = self.table_names.steps(),
        ))
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
            let body_metadata =
                TaskMetadata::body(&params.run_id, &params.execution_id).to_json_value();
            sqlx::query(&format!(
                r#"
                INSERT INTO {tasks} (task_id, task_name, execution_time, data, metadata)
                VALUES ($1, $2, NOW(), $3, $4)
                ON CONFLICT (task_id) DO NOTHING
                "#,
                tasks = self.table_names.tasks(),
            ))
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
        sqlx::query(&format!(
            r#"
            INSERT INTO {step_attempts} (attempt_id, step_id, attempt_number, status, completed_at, result, error)
            VALUES ($1, $2, $3, 'failed', NOW(), NULL, $4)
            ON CONFLICT (attempt_id) DO NOTHING
            "#,
            step_attempts = self.table_names.step_attempts(),
        ))
        .bind(&attempt_id)
        .bind(&params.child_step_id)
        .bind(params.attempt_number as i32)
        .bind(&params.error)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        sqlx::query(&format!(
            r#"
            UPDATE {steps}
            SET status = 'dead', last_error = $1, completed_at = NOW()
            WHERE step_id = $2
            "#,
            steps = self.table_names.steps(),
        ))
        .bind(&params.error)
        .bind(&params.child_step_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        let rows_affected = sqlx::query(&format!(
            r#"
            UPDATE {tasks}
            SET status = 'failed', last_error = $1, updated_at = NOW(), locked_at = NULL, worker_id = NULL
            WHERE task_id = $2 AND worker_id = $3
            "#,
            tasks = self.table_names.tasks(),
        ))
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

        let cas_row: Option<(bool,)> = sqlx::query_as(&format!(
            r#"
            UPDATE {steps}
            SET wg_first_failed = TRUE
            WHERE run_id = $1
              AND step_name = $2
              AND step_kind = 'wait_group'
              AND COALESCE(wg_first_failed, FALSE) = FALSE
            RETURNING wg_first_failed
            "#,
            steps = self.table_names.steps(),
        ))
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
        let rows: Vec<(String, String, String, String)> = sqlx::query_as(&format!(
            r#"
            SELECT
                s.run_id,
                s.step_name,
                e.task_name,
                r.execution_id
            FROM {steps} s
            JOIN {execution_runs} r ON r.run_id = s.run_id
            JOIN {executions} e ON e.execution_id = r.execution_id
            WHERE s.step_kind = 'wait_group'
              AND s.wg_remaining IS NOT NULL
              AND s.wg_threshold IS NOT NULL
              AND s.wg_remaining = s.wg_threshold
            "#,
            steps = self.table_names.steps(),
            execution_runs = self.table_names.execution_runs(),
            executions = self.table_names.executions(),
        ))
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        let mut recovered = 0usize;
        for (run_id, step_name, task_name, execution_id) in rows {
            let next_body_task_id = format!("{run_id}:body:after:{step_name}");
            let body_metadata = TaskMetadata::body(&run_id, &execution_id).to_json_value();

            let inserted = sqlx::query(&format!(
                r#"
                INSERT INTO {tasks} (task_id, task_name, execution_time, data, metadata)
                SELECT $1, $2, NOW(), r.payload, $3
                FROM {execution_runs} r
                WHERE r.run_id = $4
                ON CONFLICT (task_id) DO NOTHING
                "#,
                tasks = self.table_names.tasks(),
                execution_runs = self.table_names.execution_runs(),
            ))
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
}

// ── WaitGroupStore ────────────────────────────────────────────────────────────

#[async_trait]
impl WaitGroupStore for PostgresScheduler {
    async fn upsert_wait_group_step(
        &self,
        params: UpsertWaitGroupStepParams,
    ) -> Result<(), StorageError> {
        WaitGroupRepository::upsert_wait_group_step(self, params).await
    }

    async fn complete_wait_group_child(
        &self,
        params: CompleteWaitGroupChildParams,
    ) -> Result<bool, StorageError> {
        WaitGroupRepository::complete_wait_group_child(self, params).await
    }

    async fn fail_wait_group_child(
        &self,
        params: FailWaitGroupChildParams,
    ) -> Result<bool, StorageError> {
        WaitGroupRepository::fail_wait_group_child(self, params).await
    }

    async fn recover_wait_group_orphans(&self) -> Result<usize, StorageError> {
        WaitGroupRepository::recover_wait_group_orphans(self).await
    }
}
