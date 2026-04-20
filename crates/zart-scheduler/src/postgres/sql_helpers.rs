//! Executor-agnostic SQL helpers for task-queue operations.
//!
//! Each function accepts a `&mut sqlx::PgConnection` so that it can be executed
//! either against a fresh transaction (for the standalone methods) or against a
//! caller-owned transaction (for the `_in_tx` variants).
//!
//! All functions also accept a `&TableNames` so that the generated SQL uses the
//! caller-configured table names rather than the hard-coded `zart_*` defaults.

use crate::{ScheduleAtParams, ScheduleResult, StorageError};
use sqlx::PgConnection;

use super::table_names::TableNames;

/// Mark a task as completed within a caller-owned connection/transaction.
pub async fn mark_completed_sql(
    conn: &mut PgConnection,
    task_id: &str,
    result: Option<serde_json::Value>,
    lock_token: &str,
    names: &TableNames,
) -> Result<(), StorageError> {
    let rows_affected = sqlx::query(&format!(
        r#"
        UPDATE {tasks}
        SET status       = 'completed',
            result       = $1,
            completed_at = NOW(),
            updated_at   = NOW(),
            locked_at    = NULL,
            worker_id    = NULL
        WHERE task_id  = $2
          AND worker_id = $3
        "#,
        tasks = names.tasks(),
    ))
    .bind(&result)
    .bind(task_id)
    .bind(lock_token)
    .execute(&mut *conn)
    .await
    .map_err(|e| StorageError::Database(Box::new(e)))?
    .rows_affected();

    if rows_affected == 0 {
        return Err(StorageError::LockMismatch(task_id.to_string()));
    }
    Ok(())
}

/// Insert a task row.
///
/// Used by both `schedule_at` and body task scheduling.
pub async fn schedule_at_sql(
    conn: &mut PgConnection,
    params: &ScheduleAtParams,
    names: &TableNames,
) -> Result<ScheduleResult, StorageError> {
    let recurrence_json = params
        .recurrence
        .as_ref()
        .map(serde_json::to_value)
        .transpose()
        .map_err(|e| StorageError::Database(Box::new(e)))?;

    sqlx::query(&format!(
        r#"
        INSERT INTO {tasks}
            (task_id, task_name, execution_time, data, recurrence, metadata)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (task_id) DO NOTHING
        "#,
        tasks = names.tasks(),
    ))
    .bind(&params.task_id)
    .bind(&params.task_name)
    .bind(params.execution_time)
    .bind(&params.data)
    .bind(&recurrence_json)
    .bind(&params.metadata)
    .execute(&mut *conn)
    .await
    .map_err(|e| StorageError::Database(Box::new(e)))?;

    Ok(ScheduleResult {
        task_id: params.task_id.clone(),
        execution_time: params.execution_time,
    })
}
