//! PostgreSQL implementation of [`ExecutionStore`] for [`PostgresStorage`].
//!
//! Covers execution lifecycle (start, complete, fail, cancel) and run queries.
//! Admin operations (retry, restart, reset) delegate to `admin_storage_impl`.
//! Fine-grained primitives `create_run` and `set_current_run` are implemented here.

use async_trait::async_trait;
use sqlx::PgConnection;
use zart_core::StorageError;
use zart_core::store::ExecutionStore;
use zart_core::types::{
    ExecutionRecord, ExecutionRunRecord, ExecutionSortField, ExecutionStatus, ExecutionTrigger,
    ListExecutionsParams, SortOrder,
};

use super::PostgresStorage;
use super::sql_helpers::start_execution_sql;

#[async_trait]
impl ExecutionStore for PostgresStorage {
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
              AND status != 'cancelled'
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
              AND status != 'cancelled'
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

    #[allow(clippy::type_complexity)]
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

    #[allow(clippy::type_complexity)]
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

    async fn retry_dead_step(
        &self,
        run_id: &str,
        step_name: &str,
        triggered_by: Option<&str>,
    ) -> Result<String, StorageError> {
        self.do_retry_dead_step(run_id, step_name, triggered_by)
            .await
    }

    async fn restart_run(
        &self,
        execution_id: &str,
        new_payload: Option<serde_json::Value>,
        trigger: &str,
        triggered_by: Option<&str>,
    ) -> Result<String, StorageError> {
        self.do_restart_run(execution_id, new_payload, trigger, triggered_by)
            .await
    }

    async fn create_run(
        &self,
        execution_id: &str,
        payload: serde_json::Value,
        trigger: &str,
        triggered_by: Option<&str>,
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
        let run_id = format!("{execution_id}:run:{next_index}");

        sqlx::query(&format!(
            r#"
            INSERT INTO {execution_runs}
                (run_id, execution_id, run_index, payload, trigger, triggered_by)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
            execution_runs = self.table_names.execution_runs(),
        ))
        .bind(&run_id)
        .bind(execution_id)
        .bind(next_index)
        .bind(&payload)
        .bind(trigger)
        .bind(triggered_by)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        tx.commit()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;

        Ok(run_id)
    }

    async fn set_current_run(&self, execution_id: &str, run_id: &str) -> Result<(), StorageError> {
        sqlx::query(&format!(
            r#"
            UPDATE {executions}
            SET current_run_id = $1
            WHERE execution_id = $2
            "#,
            executions = self.table_names.executions(),
        ))
        .bind(run_id)
        .bind(execution_id)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;
        Ok(())
    }
}
