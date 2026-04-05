//! Free functions that implement execution-model-specific scheduling.
//!
//! These compose the generic [`Scheduler`] primitives (`schedule_at`,
//! `complete_and_schedule`, `mark_completed`) to perform operations specific
//! to the per-row step execution model.
//!
//! Keeping this logic here means [`PostgresScheduler`] remains a clean,
//! generic storage backend with no execution-model knowledge.

use chrono::Utc;
use scheduler::{ScheduleResult, Scheduler, StorageError};

/// Insert a new step task row for a sequential (non-wait_all) step.
pub async fn schedule_step_task<S: Scheduler>(
    scheduler: &S,
    task_id: &str,
    task_name: &str,
    execution_id: &str,
    step_name: &str,
    next_body_segment: usize,
    data: serde_json::Value,
) -> Result<ScheduleResult, StorageError> {
    let metadata = serde_json::json!({
        "mode": "step",
        "step_type": "step",
        "execution_id": execution_id,
        "step_name": step_name,
        "segment": next_body_segment,
    });
    scheduler
        .schedule_at(task_id, task_name, Utc::now(), data, None, Some(execution_id), metadata)
        .await
}

/// Insert a wait_all child step task.
pub async fn schedule_wait_all_child<S: Scheduler>(
    scheduler: &S,
    task_id: &str,
    task_name: &str,
    execution_id: &str,
    step_name: &str,
    coordinator_id: &str,
    data: serde_json::Value,
) -> Result<ScheduleResult, StorageError> {
    let metadata = serde_json::json!({
        "mode": "step",
        "step_type": "step",
        "execution_id": execution_id,
        "step_name": step_name,
        "is_wait_all_child": true,
        "coordinator_id": coordinator_id,
    });
    scheduler
        .schedule_at(task_id, task_name, Utc::now(), data, None, Some(execution_id), metadata)
        .await
}

/// Atomically complete a step task and schedule the next body segment.
pub async fn complete_step_and_schedule_body<S: Scheduler>(
    scheduler: &S,
    step_task_id: &str,
    result: serde_json::Value,
    lock_token: &str,
    next_body_task_id: &str,
    task_name: &str,
    execution_id: &str,
    next_segment: usize,
    data: serde_json::Value,
) -> Result<(), StorageError> {
    let body_metadata = serde_json::json!({
        "mode": "body",
        "execution_id": execution_id,
        "segment": next_segment,
    });
    scheduler
        .complete_and_schedule(
            step_task_id,
            Some(result),
            lock_token,
            next_body_task_id,
            task_name,
            Utc::now(),
            data,
            Some(execution_id),
            body_metadata,
        )
        .await
}

/// Complete a wait_all child step without scheduling a body continuation.
///
/// The coordinator task polls children and schedules the body when all are done.
pub async fn complete_step_no_resume<S: Scheduler>(
    scheduler: &S,
    step_task_id: &str,
    result: serde_json::Value,
    lock_token: &str,
) -> Result<(), StorageError> {
    scheduler.mark_completed(step_task_id, Some(result), lock_token).await
}

/// Schedule a coordinator task that polls wait_all children.
pub async fn schedule_coordinator<S: Scheduler>(
    scheduler: &S,
    coordinator_task_id: &str,
    task_name: &str,
    execution_id: &str,
    next_segment: usize,
    wait_for: Vec<String>,
    data: serde_json::Value,
) -> Result<ScheduleResult, StorageError> {
    let metadata = serde_json::json!({
        "mode": "step",
        "step_type": "wait_all",
        "execution_id": execution_id,
        "segment": next_segment,
        "wait_for": wait_for,
    });
    scheduler
        .schedule_at(coordinator_task_id, task_name, Utc::now(), data, None, Some(execution_id), metadata)
        .await
}

/// Schedule a sleep continuation task.
pub async fn schedule_sleep_task<S: Scheduler>(
    scheduler: &S,
    sleep_task_id: &str,
    task_name: &str,
    execution_id: &str,
    next_segment: usize,
    wake_time: chrono::DateTime<chrono::Utc>,
    data: serde_json::Value,
) -> Result<ScheduleResult, StorageError> {
    let metadata = serde_json::json!({
        "mode": "step",
        "step_type": "sleep",
        "execution_id": execution_id,
        "segment": next_segment,
    });
    scheduler
        .schedule_at(sleep_task_id, task_name, wake_time, data, None, Some(execution_id), metadata)
        .await
}
