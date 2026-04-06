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
#[allow(clippy::too_many_arguments)]
pub async fn schedule_step_task<S: Scheduler + ?Sized>(
    scheduler: &S,
    task_id: &str,
    task_name: &str,
    execution_id: &str,
    step_name: &str,
    next_body_segment: usize,
    data: serde_json::Value,
    retry_config: Option<&crate::retry::RetryConfig>,
) -> Result<ScheduleResult, StorageError> {
    let mut metadata = serde_json::json!({
        "mode": "step",
        "step_type": "step",
        "execution_id": execution_id,
        "step_name": step_name,
        "segment": next_body_segment,
        "retry_attempt": 0,
    });
    if let Some(rc) = retry_config {
        metadata["retry_config"] = serde_json::to_value(rc).unwrap_or(serde_json::Value::Null);
    }
    scheduler
        .schedule_at(
            task_id,
            task_name,
            Utc::now(),
            data,
            None,
            Some(execution_id),
            metadata,
        )
        .await
}

/// Reschedule a failed step task for retry after a delay.
///
/// Marks the step task as failed with a future execution time so the worker
/// will pick it up again after the retry delay. The scheduler's built-in
/// `task.attempt` counter increments on each pickup and is used to track
/// the retry attempt number.
pub async fn reschedule_step_for_retry<S: Scheduler + ?Sized>(
    scheduler: &S,
    step_task_id: &str,
    error: &str,
    retry_time: chrono::DateTime<chrono::Utc>,
    lock_token: &str,
) -> Result<(), StorageError> {
    scheduler
        .mark_failed(step_task_id, error, Some(retry_time), lock_token)
        .await?;
    Ok(())
}

/// Insert a wait_all child step task.
pub async fn schedule_wait_all_child<S: Scheduler + ?Sized>(
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
        .schedule_at(
            task_id,
            task_name,
            Utc::now(),
            data,
            None,
            Some(execution_id),
            metadata,
        )
        .await
}

/// Atomically complete a step task and schedule the next body segment.
#[allow(clippy::too_many_arguments)]
pub async fn complete_step_and_schedule_body<S: Scheduler + ?Sized>(
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
pub async fn complete_step_no_resume<S: Scheduler + ?Sized>(
    scheduler: &S,
    step_task_id: &str,
    result: serde_json::Value,
    lock_token: &str,
) -> Result<(), StorageError> {
    scheduler
        .mark_completed(step_task_id, Some(result), lock_token)
        .await
}

/// Schedule a coordinator task that polls wait_all children.
pub async fn schedule_coordinator<S: Scheduler + ?Sized>(
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
        .schedule_at(
            coordinator_task_id,
            task_name,
            Utc::now(),
            data,
            None,
            Some(execution_id),
            metadata,
        )
        .await
}

/// Insert a wait_for_event step task row.
///
/// If `deadline` is `None`, the task is scheduled for `DateTime::MAX_UTC` (year
/// 262142) so it effectively never fires unless `offer_event` arrives first.
///
/// **Trade-off**: using a sentinel far-future timestamp is simpler than a dedicated
/// `parked` status or a nullable `execution_time`, but it means the scheduler's
/// `poll_due` query will never return this row under normal conditions. The row is
/// only completed via `complete_event_step_and_schedule_body`, which bypasses the
/// `execution_time` check entirely. Operators querying for "scheduled" tasks with
/// far-future times can identify these as pending event waits.
#[allow(clippy::too_many_arguments)]
pub async fn schedule_wait_for_event_task<S: Scheduler + ?Sized>(
    scheduler: &S,
    task_id: &str,
    task_name: &str,
    execution_id: &str,
    event_name: &str,
    next_body_segment: usize,
    data: serde_json::Value,
    deadline: Option<chrono::DateTime<chrono::Utc>>,
) -> Result<ScheduleResult, StorageError> {
    let execution_time = deadline.unwrap_or(chrono::DateTime::<chrono::Utc>::MAX_UTC);
    let metadata = serde_json::json!({
        "mode":         "step",
        "step_type":    "wait_for_event",
        "execution_id": execution_id,
        "step_name":    event_name,
        "segment":      next_body_segment,
    });
    scheduler
        .schedule_at(
            task_id,
            task_name,
            execution_time,
            data,
            None,
            Some(execution_id),
            metadata,
        )
        .await
}

/// Schedule a sleep continuation task.
pub async fn schedule_sleep_task<S: Scheduler + ?Sized>(
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
        .schedule_at(
            sleep_task_id,
            task_name,
            wake_time,
            data,
            None,
            Some(execution_id),
            metadata,
        )
        .await
}
