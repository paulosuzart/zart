//! PostgreSQL implementation of [`EventStore`] for [`PostgresStorage`].

use async_trait::async_trait;
use chrono::Utc;
use zart_core::StorageError;
use zart_core::store::EventStore;
use zart_core::task_metadata::TaskMetadata;
use zart_core::types::{EventDeliveryResult, ExecutionStats, ScheduleAtParams};

use super::PostgresStorage;

#[async_trait]
impl EventStore for PostgresStorage {
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
        let body_metadata = {
            let mut m = TaskMetadata::body(&run_id, execution_id).to_json_value();
            if let Some(obj) = m.as_object_mut() {
                obj.insert(
                    "handler".to_string(),
                    serde_json::Value::String(task_name.clone()),
                );
            }
            m
        };

        // Schedule the continuation body task via the task_scheduler delegate.
        self.task_scheduler
            .schedule_at_in_tx(
                &mut tx,
                ScheduleAtParams {
                    task_id: next_body_task_id,
                    task_name: crate::TASK_NAME.to_string(),
                    execution_time: Utc::now(),
                    data: run_payload,
                    recurrence: None,
                    metadata: body_metadata,
                },
            )
            .await?;

        tx.commit()
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;

        Ok(EventDeliveryResult::Delivered)
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
}
