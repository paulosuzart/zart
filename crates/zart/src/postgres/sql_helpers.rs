//! Executor-agnostic SQL helpers for [`PostgresStorage`] operations.
//!
//! Each function accepts a `&mut sqlx::PgConnection` so that it can be executed
//! either against a fresh transaction (for the standalone methods) or against a
//! caller-owned transaction (for the `_in_tx` variants).

use chrono::Utc;
use serde_json::Value;
use sqlx::PgConnection;
use zart_core::StorageError;
use zart_core::task_metadata::TaskMetadata;
use zart_core::types::{CompleteStepAndScheduleBodyParams, ScheduleAtParams, ScheduleResult};

use super::table_names::TableNames;

/// Insert a new durable execution record and create the initial run row at index 0.
///
/// Idempotent: if the execution already exists, the INSERT is a no-op.
pub async fn start_execution_sql(
    conn: &mut PgConnection,
    execution_id: &str,
    task_name: &str,
    payload: &Value,
    names: &TableNames,
) -> Result<(), StorageError> {
    sqlx::query(&format!(
        r#"
        INSERT INTO {executions} (execution_id, task_name)
        VALUES ($1, $2)
        ON CONFLICT (execution_id) DO NOTHING
        "#,
        executions = names.executions(),
    ))
    .bind(execution_id)
    .bind(task_name)
    .execute(&mut *conn)
    .await
    .map_err(|e| StorageError::Database(Box::new(e)))?;

    let run_exists: bool = sqlx::query_scalar(&format!(
        r#"
        SELECT EXISTS(
            SELECT 1 FROM {execution_runs}
            WHERE execution_id = $1 AND run_index = 0
        )
        "#,
        execution_runs = names.execution_runs(),
    ))
    .bind(execution_id)
    .fetch_one(&mut *conn)
    .await
    .map_err(|e| StorageError::Database(Box::new(e)))?;

    if !run_exists {
        let run_id = format!("{execution_id}:run:0");
        sqlx::query(&format!(
            r#"
            INSERT INTO {execution_runs}
                (run_id, execution_id, run_index, payload, trigger)
            VALUES ($1, $2, 0, $3, 'initial')
            ON CONFLICT DO NOTHING
            "#,
            execution_runs = names.execution_runs(),
        ))
        .bind(&run_id)
        .bind(execution_id)
        .bind(payload)
        .execute(&mut *conn)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;

        sqlx::query(&format!(
            r#"
            UPDATE {executions}
            SET current_run_id = $1
            WHERE execution_id = $2 AND current_run_id IS NULL
            "#,
            executions = names.executions(),
        ))
        .bind(&run_id)
        .bind(execution_id)
        .execute(&mut *conn)
        .await
        .map_err(|e| StorageError::Database(Box::new(e)))?;
    }

    Ok(())
}

/// Insert a task row.
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

/// Atomically complete a step+task, record the attempt, and schedule the next body task.
pub async fn complete_step_and_schedule_body_sql(
    conn: &mut PgConnection,
    params: &CompleteStepAndScheduleBodyParams,
    names: &TableNames,
) -> Result<(), StorageError> {
    let attempt_id = format!("{}:attempt:{}", params.step_id, params.attempt_number);
    sqlx::query(&format!(
        r#"
        INSERT INTO {step_attempts} (attempt_id, step_id, attempt_number, status, completed_at, result, error)
        VALUES ($1, $2, $3, 'completed', NOW(), $4, NULL)
        ON CONFLICT (attempt_id) DO NOTHING
        "#,
        step_attempts = names.step_attempts(),
    ))
    .bind(&attempt_id)
    .bind(&params.step_id)
    .bind(params.attempt_number as i32)
    .bind(&params.result)
    .execute(&mut *conn)
    .await
    .map_err(|e| StorageError::Database(Box::new(e)))?;

    sqlx::query(&format!(
        r#"
        UPDATE {steps} SET status = 'completed', result = $1, result_kind = $2, completed_at = $3 WHERE step_id = $4
        "#,
        steps = names.steps(),
    ))
    .bind(&params.result)
    .bind(params.result_kind)
    .bind(Utc::now())
    .bind(&params.step_id)
    .execute(&mut *conn)
    .await
    .map_err(|e| StorageError::Database(Box::new(e)))?;

    let rows_affected = sqlx::query(&format!(
        r#"
        UPDATE {tasks} SET status = 'completed', result = $1, completed_at = NOW(), updated_at = NOW(), locked_at = NULL, worker_id = NULL
        WHERE task_id = $2 AND worker_id = $3
        "#,
        tasks = names.tasks(),
    ))
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
    sqlx::query(&format!(
        r#"
        INSERT INTO {tasks} (task_id, task_name, execution_time, data, metadata)
        VALUES ($1, $2, NOW(), $3, $4)
        ON CONFLICT (task_id) DO NOTHING
        "#,
        tasks = names.tasks(),
    ))
    .bind(&params.next_body_task_id)
    .bind(&params.task_name)
    .bind(&params.data)
    .bind(&body_metadata)
    .execute(&mut *conn)
    .await
    .map_err(|e| StorageError::Database(Box::new(e)))?;

    Ok(())
}
