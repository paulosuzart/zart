//! Free functions that implement execution-model-specific scheduling.
//!
//! These compose the generic [`Scheduler`] primitives (`schedule_at`,
//! `complete_and_schedule`, `mark_completed`) to perform operations specific
//! to the per-row step execution model.
//!
//! Keeping this logic here means [`PostgresScheduler`] remains a clean,
//! generic storage backend with no execution-model knowledge.

use chrono::Utc;
use scheduler::{CompleteAndScheduleParams, ScheduleAtParams, ScheduleResult, Scheduler, StorageError};

/// Parameters for [`schedule_step_task`].
pub struct StepTaskSpec<'a> {
    pub task_id: &'a str,
    pub task_name: &'a str,
    pub execution_id: &'a str,
    pub step_name: &'a str,
    pub next_body_segment: usize,
    pub data: serde_json::Value,
    pub retry_config: Option<&'a crate::retry::RetryConfig>,
}

/// Parameters for [`complete_step_and_schedule_body`].
pub struct ResumeBodySpec<'a> {
    pub step_task_id: &'a str,
    pub result: serde_json::Value,
    pub lock_token: &'a str,
    pub next_body_task_id: &'a str,
    pub task_name: &'a str,
    pub execution_id: &'a str,
    pub next_segment: usize,
    pub data: serde_json::Value,
}

/// Parameters for [`schedule_wait_for_event_task`].
pub struct EventStepSpec<'a> {
    pub task_id: &'a str,
    pub task_name: &'a str,
    pub execution_id: &'a str,
    pub event_name: &'a str,
    pub next_body_segment: usize,
    pub data: serde_json::Value,
    pub deadline: Option<chrono::DateTime<chrono::Utc>>,
}

/// Insert a new step task row for a sequential (non-wait_all) step.
pub async fn schedule_step_task<S: Scheduler + ?Sized>(
    scheduler: &S,
    spec: StepTaskSpec<'_>,
) -> Result<ScheduleResult, StorageError> {
    let mut metadata = serde_json::json!({
        "mode": "step",
        "step_type": "step",
        "execution_id": spec.execution_id,
        "step_name": spec.step_name,
        "segment": spec.next_body_segment,
        "retry_attempt": 0,
    });
    if let Some(rc) = spec.retry_config {
        metadata["retry_config"] = serde_json::to_value(rc).unwrap_or(serde_json::Value::Null);
    }
    scheduler
        .schedule_at(ScheduleAtParams {
            task_id: spec.task_id.to_string(),
            task_name: spec.task_name.to_string(),
            execution_time: Utc::now(),
            data: spec.data,
            recurrence: None,
            execution_id: Some(spec.execution_id.to_string()),
            metadata,
        })
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
        .schedule_at(ScheduleAtParams {
            task_id: task_id.to_string(),
            task_name: task_name.to_string(),
            execution_time: Utc::now(),
            data,
            recurrence: None,
            execution_id: Some(execution_id.to_string()),
            metadata,
        })
        .await
}

/// Atomically complete a step task and schedule the next body segment.
pub async fn complete_step_and_schedule_body<S: Scheduler + ?Sized>(
    scheduler: &S,
    spec: ResumeBodySpec<'_>,
) -> Result<(), StorageError> {
    let body_metadata = serde_json::json!({
        "mode": "body",
        "execution_id": spec.execution_id,
        "segment": spec.next_segment,
    });
    scheduler
        .complete_and_schedule(CompleteAndScheduleParams {
            completed_task_id: spec.step_task_id.to_string(),
            result: Some(spec.result),
            lock_token: spec.lock_token.to_string(),
            new_task_id: spec.next_body_task_id.to_string(),
            new_task_name: spec.task_name.to_string(),
            new_execution_time: Utc::now(),
            new_data: spec.data,
            new_execution_id: Some(spec.execution_id.to_string()),
            new_metadata: body_metadata,
        })
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
        .schedule_at(ScheduleAtParams {
            task_id: coordinator_task_id.to_string(),
            task_name: task_name.to_string(),
            execution_time: Utc::now(),
            data,
            recurrence: None,
            execution_id: Some(execution_id.to_string()),
            metadata,
        })
        .await
}

/// Insert a wait_for_event step task row.
///
/// If `spec.deadline` is `None`, the task is scheduled for `DateTime::MAX_UTC`
/// (year 262142) so it effectively never fires unless `offer_event` arrives first.
///
/// **Trade-off**: using a sentinel far-future timestamp is simpler than a dedicated
/// `parked` status or a nullable `execution_time`, but it means the scheduler's
/// `poll_due` query will never return this row under normal conditions. The row is
/// only completed via `complete_event_step_and_schedule_body`, which bypasses the
/// `execution_time` check entirely. Operators querying for "scheduled" tasks with
/// far-future times can identify these as pending event waits.
pub async fn schedule_wait_for_event_task<S: Scheduler + ?Sized>(
    scheduler: &S,
    spec: EventStepSpec<'_>,
) -> Result<ScheduleResult, StorageError> {
    let execution_time = spec
        .deadline
        .unwrap_or(chrono::DateTime::<chrono::Utc>::MAX_UTC);
    let metadata = serde_json::json!({
        "mode":         "step",
        "step_type":    "wait_for_event",
        "execution_id": spec.execution_id,
        "step_name":    spec.event_name,
        "segment":      spec.next_body_segment,
    });
    scheduler
        .schedule_at(ScheduleAtParams {
            task_id: spec.task_id.to_string(),
            task_name: spec.task_name.to_string(),
            execution_time,
            data: spec.data,
            recurrence: None,
            execution_id: Some(spec.execution_id.to_string()),
            metadata,
        })
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
        .schedule_at(ScheduleAtParams {
            task_id: sleep_task_id.to_string(),
            task_name: task_name.to_string(),
            execution_time: wake_time,
            data,
            recurrence: None,
            execution_id: Some(execution_id.to_string()),
            metadata,
        })
        .await
}
