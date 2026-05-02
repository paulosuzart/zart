//! Executor-agnostic SQL helpers for [`PostgresStorage`] operations.
//!
//! Each function accepts a `&mut sqlx::PgConnection` so that it can be executed
//! either against a fresh transaction (for the standalone methods) or against a
//! caller-owned transaction (for the `_in_tx` variants).
//!
//! Task-queue inserts (scheduling body tasks, step tasks) go through the
//! `task_scheduler` parameter so that no task-queue SQL lives in this crate.

use chrono::Utc;
use serde_json::Value;
use sqlx::PgConnection;
use zart_core::StorageError;
use zart_core::types::{ScheduleAtParams, ScheduleResult, WriteStepCompletionParams};
use zart_scheduler::TaskScheduler;

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

/// Write step SQL only (step_attempts insert, steps update) within a caller-owned transaction.
///
/// Does NOT schedule the next body task.
/// Does NOT commit — the caller owns the transaction lifecycle.
pub async fn write_step_completion_sql(
    conn: &mut PgConnection,
    params: &WriteStepCompletionParams,
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

    Ok(())
}

/// Copy completed step rows and their attempt history from `from_run_id` to `to_run_id`.
///
/// Only rows with `status = 'completed'` are copied. Step IDs, task IDs, and
/// attempt IDs are rewritten to use `to_run_id` as the prefix. Result and
/// result_kind are preserved verbatim. Idempotent — `ON CONFLICT DO NOTHING`.
pub async fn copy_steps_and_attempts_sql(
    conn: &mut PgConnection,
    from_run_id: &str,
    to_run_id: &str,
    step_names: &[String],
    names: &TableNames,
) -> Result<(), StorageError> {
    if step_names.is_empty() {
        return Ok(());
    }

    sqlx::query(&format!(
        r#"
        INSERT INTO {steps}
            (step_id, run_id, step_name, step_kind, task_id,
             status, result, result_kind, retry_config, completed_at)
        SELECT
            concat($2, substring(step_id from length($1) + 1)),
            $2,
            step_name,
            step_kind,
            concat($2, substring(task_id from length($1) + 1)),
            status,
            result,
            result_kind,
            retry_config,
            completed_at
        FROM {steps}
        WHERE run_id = $1
          AND step_name = ANY($3)
          AND status = 'completed'
        ON CONFLICT (step_id) DO NOTHING
        "#,
        steps = names.steps(),
    ))
    .bind(from_run_id)
    .bind(to_run_id)
    .bind(step_names)
    .execute(&mut *conn)
    .await
    .map_err(|e| StorageError::Database(Box::new(e)))?;

    sqlx::query(&format!(
        r#"
        INSERT INTO {step_attempts}
            (attempt_id, step_id, attempt_number, started_at, completed_at,
             status, result, error)
        SELECT
            concat($2, substring(a.attempt_id from length($1) + 1)),
            concat($2, substring(a.step_id from length($1) + 1)),
            a.attempt_number,
            a.started_at,
            a.completed_at,
            a.status,
            a.result,
            a.error
        FROM {step_attempts} a
        WHERE a.step_id IN (
            SELECT step_id FROM {steps}
            WHERE run_id = $1 AND step_name = ANY($3)
        )
        ON CONFLICT (attempt_id) DO NOTHING
        "#,
        step_attempts = names.step_attempts(),
        steps = names.steps(),
    ))
    .bind(from_run_id)
    .bind(to_run_id)
    .bind(step_names)
    .execute(&mut *conn)
    .await
    .map_err(|e| StorageError::Database(Box::new(e)))?;

    Ok(())
}

/// Insert a task row for a step (task_id + step row) within a caller-owned transaction.
///
/// Used by `StepStore::schedule_step`.
pub async fn schedule_step_sql(
    conn: &mut PgConnection,
    task_id: &str,
    _task_name: &str,
    execution_time: chrono::DateTime<chrono::Utc>,
    data: &Value,
    metadata: &Value,
    task_scheduler: &dyn TaskScheduler,
) -> Result<ScheduleResult, StorageError> {
    task_scheduler
        .schedule_at_in_tx(
            conn,
            ScheduleAtParams {
                task_id: task_id.to_string(),
                task_name: crate::TASK_NAME.to_string(),
                execution_time,
                data: data.clone(),
                recurrence: None,
                metadata: metadata.clone(),
            },
        )
        .await
}
