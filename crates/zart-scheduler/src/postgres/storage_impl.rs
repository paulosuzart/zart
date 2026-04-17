//! Implementation of the [`DurableStorage`] trait for [`PostgresScheduler`].

use async_trait::async_trait;
use chrono::Utc;
use sqlx::PgConnection;

use super::PostgresScheduler;
use super::sql_helpers::{complete_step_and_schedule_body_sql, start_execution_sql};
use crate::{
    CompleteStepAndScheduleBodyParams, CompleteStepNoResumeParams, CompleteWaitGroupChildParams,
    DurableStorage, EventDeliveryResult, ExecutionRecord, ExecutionSortField, ExecutionStats,
    ExecutionStatus, FailWaitGroupChildParams, ListExecutionsParams, RescheduleStepForRetryParams,
    ScheduleStepParams, SortOrder, StepAttemptRow, StepAttemptStatus, StepKind, StepLookup,
    StepResultKind, StepStatus, StorageError, TaskMetadata, TaskStatus, UpsertWaitGroupStepParams,
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

        start_execution_sql(
            &mut tx,
            execution_id,
            task_name,
            &payload,
            &self.table_names,
        )
        .await?;

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
        start_execution_sql(conn, execution_id, task_name, &payload, &self.table_names).await
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

        sqlx::query(&format!(
            r#"
            UPDATE {execution_runs}
            SET status       = 'completed',
                result       = $1,
                completed_at = NOW()
            WHERE execution_id = $2
              AND run_id = (SELECT current_run_id FROM {executions} WHERE execution_id = $2)
            "#,
            execution_runs = self.table_names.execution_runs(),
            executions = self.table_names.executions(),
        ))
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

        sqlx::query(&format!(
            r#"
            UPDATE {execution_runs}
            SET status = 'failed'
            WHERE execution_id = $1
              AND run_id = (SELECT current_run_id FROM {executions} WHERE execution_id = $1)
            "#,
            execution_runs = self.table_names.execution_runs(),
            executions = self.table_names.executions(),
        ))
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
        let row: Option<(
            String,
            String,
            serde_json::Value,
            Option<serde_json::Value>,
            ExecutionStatus,
            chrono::DateTime<chrono::Utc>,
            Option<chrono::DateTime<chrono::Utc>>,
            i32,
        )> = sqlx::query_as(&format!(
            r#"
                SELECT r.run_id, e.task_name, r.payload, r.result, r.status,
                       r.started_at, r.completed_at, 1
                FROM {executions} e
                LEFT JOIN {execution_runs} r ON e.current_run_id = r.run_id
                WHERE e.execution_id = $1
                "#,
            executions = self.table_names.executions(),
            execution_runs = self.table_names.execution_runs(),
        ))
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
        let exec_rows = sqlx::query(&format!(
            r#"
            UPDATE {execution_runs}
            SET status = 'cancelled', completed_at = NOW()
            WHERE run_id = (SELECT current_run_id FROM {executions} WHERE execution_id = $1)
              AND status IN ('scheduled', 'running')
            "#,
            execution_runs = self.table_names.execution_runs(),
            executions = self.table_names.executions(),
        ))
        .bind(execution_id)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?
        .rows_affected();

        sqlx::query(&format!(
            r#"
            UPDATE {tasks}
            SET status = 'cancelled', updated_at = NOW()
            WHERE status = 'scheduled'
              AND (
                metadata->>'execution_id' = $1
                OR metadata->>'run_id' LIKE $1 || ':run:%'
              )
            "#,
            tasks = self.table_names.tasks(),
        ))
        .bind(execution_id)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        Ok(exec_rows > 0)
    }

    async fn list_executions(
        &self,
        params: ListExecutionsParams,
    ) -> Result<Vec<ExecutionRecord>, StorageError> {
        let order_clause = match (params.sort_by, &params.sort_order) {
            (ExecutionSortField::ScheduledAt, SortOrder::Desc) => "r.started_at DESC",
            (ExecutionSortField::ScheduledAt, SortOrder::Asc) => "r.started_at ASC",
            (ExecutionSortField::Status, SortOrder::Desc) => "r.status::TEXT DESC",
            (ExecutionSortField::Status, SortOrder::Asc) => "r.status::TEXT ASC",
            (ExecutionSortField::TaskName, SortOrder::Desc) => "e.task_name DESC",
            (ExecutionSortField::TaskName, SortOrder::Asc) => "e.task_name ASC",
        };

        let sql = format!(
            r#"
            SELECT e.execution_id, e.task_name, r.payload, r.result, r.status,
                   r.started_at, r.completed_at, 1
            FROM {executions} e
            JOIN {execution_runs} r ON e.current_run_id = r.run_id
            WHERE ($1::execution_status IS NULL OR r.status = $1)
              AND ($2::TEXT IS NULL OR e.task_name ILIKE '%' || $2 || '%')
              AND ($3::TIMESTAMPTZ IS NULL OR r.started_at >= $3)
              AND ($4::TIMESTAMPTZ IS NULL OR r.started_at <= $4)
              AND ($5::TEXT IS NULL OR e.execution_id LIKE $5 || '%')
            ORDER BY {order_clause}
            LIMIT $6 OFFSET $7
            "#,
            executions = self.table_names.executions(),
            execution_runs = self.table_names.execution_runs(),
        );

        let rows: Vec<(
            String,
            String,
            serde_json::Value,
            Option<serde_json::Value>,
            ExecutionStatus,
            chrono::DateTime<chrono::Utc>,
            Option<chrono::DateTime<chrono::Utc>>,
            i32,
        )> = sqlx::query_as(&sql)
            .bind(params.status)
            .bind(params.task_name.as_deref())
            .bind(params.from)
            .bind(params.to)
            .bind(params.search.as_deref())
            .bind(params.limit as i64)
            .bind(params.offset as i64)
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
    ) -> Result<crate::ScheduleResult, StorageError> {
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

        complete_step_and_schedule_body_sql(&mut tx, &params, &self.table_names).await?;

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
