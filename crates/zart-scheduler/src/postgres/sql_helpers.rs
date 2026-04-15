//! Executor-agnostic SQL helpers for [`DurableStorage`] operations.
//!
//! Each function accepts a `&mut sqlx::PgConnection` so that it can be executed
//! either against a fresh transaction (for the standalone `DurableStorage`
//! methods) or against a caller-owned transaction (for the `_in_tx` variants).

use crate::{
    CompleteStepAndScheduleBodyParams, ScheduleAtParams, ScheduleResult, StorageError, TaskMetadata,
};
use chrono::Utc;
use serde_json::Value;
use sqlx::PgConnection;

/// Insert a new durable execution record into `zart_executions` and create the
/// initial run row at index 0.
///
/// This is idempotent: if the execution already exists, the INSERT is a no-op
/// and the existing run row is left unchanged.
pub async fn start_execution_sql(
    conn: &mut PgConnection,
    execution_id: &str,
    task_name: &str,
    payload: &Value,
) -> Result<(), StorageError> {
    sqlx::query(
        r#"
        INSERT INTO zart_executions (execution_id, task_name)
        VALUES ($1, $2)
        ON CONFLICT (execution_id) DO NOTHING
        "#,
    )
    .bind(execution_id)
    .bind(task_name)
    .execute(&mut *conn)
    .await
    .map_err(|e| StorageError::Database(Box::new(e)))?;

    // Check if a run already exists at index 0
    let run_exists: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS(
            SELECT 1 FROM zart_execution_runs
            WHERE execution_id = $1 AND run_index = 0
        )
        "#,
    )
    .bind(execution_id)
    .fetch_one(&mut *conn)
    .await
    .map_err(|e| StorageError::Database(Box::new(e)))?;

    // Create run row if it doesn't exist
    if !run_exists {
        let run_id = format!("{execution_id}:run:0");
        sqlx::query(
            r#"
            INSERT INTO zart_execution_runs
                (run_id, execution_id, run_index, payload, trigger)
            VALUES ($1, $2, 0, $3, 'initial')
            ON CONFLICT DO NOTHING
            "#,
        )
        .bind(&run_id)
        .bind(execution_id)
        .bind(payload)
        .execute(&mut *conn)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        // Set current_run_id pointer
        sqlx::query(
            r#"
            UPDATE zart_executions
            SET current_run_id = $1
            WHERE execution_id = $2 AND current_run_id IS NULL
            "#,
        )
        .bind(&run_id)
        .bind(execution_id)
        .execute(&mut *conn)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;
    }

    Ok(())
}

/// Insert a task row into `zart_tasks`.
///
/// Used by both `schedule_at` and body task scheduling.
pub async fn schedule_at_sql(
    conn: &mut PgConnection,
    params: &ScheduleAtParams,
) -> Result<ScheduleResult, StorageError> {
    let recurrence_json = params
        .recurrence
        .as_ref()
        .map(serde_json::to_value)
        .transpose()
        .map_err(|e| StorageError::Database(Box::new(e)))?;

    sqlx::query(
        r#"
        INSERT INTO zart_tasks
            (task_id, task_name, execution_time, data, recurrence, metadata)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (task_id) DO NOTHING
        "#,
    )
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

/// Atomically complete a step+task, record the attempt, and schedule the next body task.
///
/// This is the core atomic operation for step completion: all four SQL statements
/// (attempt insert, step update, task completion, body task insert) execute in a
/// single transaction.
pub async fn complete_step_and_schedule_body_sql(
    conn: &mut PgConnection,
    params: &CompleteStepAndScheduleBodyParams,
) -> Result<(), StorageError> {
    let attempt_id = format!("{}:attempt:{}", params.step_id, params.attempt_number);
    sqlx::query(
        r#"
        INSERT INTO zart_step_attempts (attempt_id, step_id, attempt_number, status, completed_at, result, error)
        VALUES ($1, $2, $3, 'completed', NOW(), $4, NULL)
        ON CONFLICT (attempt_id) DO NOTHING
        "#,
    )
    .bind(&attempt_id)
    .bind(&params.step_id)
    .bind(params.attempt_number as i32)
    .bind(&params.result)
    .execute(&mut *conn)
    .await
    .map_err(|e| StorageError::Database(Box::new(e)))?;

    sqlx::query(
        r#"
        UPDATE zart_steps SET status = 'completed', result = $1, result_kind = $2, completed_at = $3 WHERE step_id = $4
        "#,
    )
    .bind(&params.result)
    .bind(params.result_kind)
    .bind(Utc::now())
    .bind(&params.step_id)
    .execute(&mut *conn)
    .await
    .map_err(|e| StorageError::Database(Box::new(e)))?;

    let rows_affected = sqlx::query(
        r#"
        UPDATE zart_tasks SET status = 'completed', result = $1, completed_at = NOW(), updated_at = NOW(), locked_at = NULL, worker_id = NULL
        WHERE task_id = $2 AND worker_id = $3
        "#,
    )
    .bind(&params.result)
    .bind(&params.step_task_id)
    .bind(&params.lock_token)
    .execute(&mut *conn)
    .await
    .map_err(|e| StorageError::Database(Box::new(e)))?
    .rows_affected();

    if rows_affected == 0 {
        return Err(StorageError::LockMismatch(params.step_task_id.clone()));
    }

    let body_metadata = TaskMetadata::body(&params.run_id, &params.execution_id).to_json_value();
    sqlx::query(
        r#"
        INSERT INTO zart_tasks (task_id, task_name, execution_time, data, metadata)
        VALUES ($1, $2, NOW(), $3, $4)
        ON CONFLICT (task_id) DO NOTHING
        "#,
    )
    .bind(&params.next_body_task_id)
    .bind(&params.task_name)
    .bind(&params.data)
    .bind(&body_metadata)
    .execute(&mut *conn)
    .await
    .map_err(|e| StorageError::Database(Box::new(e)))?;

    Ok(())
}
