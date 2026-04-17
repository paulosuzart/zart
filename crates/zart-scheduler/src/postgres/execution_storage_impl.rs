//! Execution lifecycle operations for [`PostgresScheduler`].
//!
//! This module handles starting, completing, failing, canceling, and listing
//! durable executions. It does not handle step-level operations or wait-group
//! coordination — those live in `step_storage_impl` and `wait_group_storage_impl`.

use sqlx::PgConnection;

use super::PostgresScheduler;
use super::sql_helpers::start_execution_sql;
use crate::{
    ExecutionRecord, ExecutionSortField, ExecutionStatus, ListExecutionsParams, SortOrder,
    StorageError,
};

/// Internal extension trait for execution lifecycle operations.
/// Not part of the public API — used to modularize the DurableStorage impl.
pub(crate) trait ExecutionStorage: Sized {
    async fn start_execution(
        &self,
        execution_id: &str,
        task_name: &str,
        payload: serde_json::Value,
    ) -> Result<(), StorageError>;

    async fn start_execution_in_tx(
        &self,
        conn: &mut PgConnection,
        execution_id: &str,
        task_name: &str,
        payload: serde_json::Value,
    ) -> Result<(), StorageError>;

    async fn complete_execution(
        &self,
        execution_id: &str,
        result: serde_json::Value,
    ) -> Result<(), StorageError>;

    async fn fail_execution(&self, execution_id: &str) -> Result<(), StorageError>;

    async fn get_execution(
        &self,
        execution_id: &str,
    ) -> Result<Option<ExecutionRecord>, StorageError>;

    async fn cancel_execution(&self, execution_id: &str) -> Result<bool, StorageError>;

    async fn list_executions(
        &self,
        params: ListExecutionsParams,
    ) -> Result<Vec<ExecutionRecord>, StorageError>;
}

impl ExecutionStorage for PostgresScheduler {
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
}
