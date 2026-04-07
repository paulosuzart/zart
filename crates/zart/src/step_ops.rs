//! Free functions that implement execution-model-specific scheduling.
//!
//! These compose the generic [`Scheduler`] primitives (`schedule_at`,
//! `complete_and_schedule`, `mark_completed`) to perform operations specific
//! to the per-row step execution model.
//!
//! Keeping this logic here means [`PostgresScheduler`] remains a clean,
//! generic storage backend with no execution-model knowledge.

use scheduler::{ScheduleAtParams, ScheduleResult, StorageBackend, StorageError};

/// Parameters for [`schedule_step_task`].
pub struct StepTaskSpec<'a> {
    pub task_id: &'a str,
    pub task_name: &'a str,
    pub run_id: &'a str,
    pub step_name: &'a str,
    pub data: serde_json::Value,
    pub retry_config: Option<&'a crate::retry::RetryConfig>,
}

/// Parameters for [`complete_step_and_schedule_body`].
pub struct ResumeBodySpec<'a> {
    pub step_task_id: &'a str,
    pub step_id: &'a str,
    pub result: serde_json::Value,
    pub lock_token: &'a str,
    pub next_body_task_id: &'a str,
    pub task_name: &'a str,
    pub run_id: &'a str,
    pub data: serde_json::Value,
    /// 1-indexed attempt number for recording in `zart_step_attempts`.
    pub attempt_number: usize,
}

/// Parameters for [`schedule_wait_for_event_task`].
pub struct EventStepSpec<'a> {
    pub task_id: &'a str,
    pub task_name: &'a str,
    pub run_id: &'a str,
    pub event_name: &'a str,
    pub data: serde_json::Value,
    pub deadline: Option<chrono::DateTime<chrono::Utc>>,
}

/// Insert a new step task row for a sequential (non-wait_all) step.
///
/// This function is transactional: it inserts both the task row and the step row
/// atomically via the scheduler's transaction API.
pub async fn schedule_step_task(
    scheduler: &dyn StorageBackend,
    spec: StepTaskSpec<'_>,
) -> Result<ScheduleResult, StorageError> {
    let mut metadata = serde_json::json!({
        "mode": "step",
        "step_type": "step",
        "run_id": spec.run_id,
        "step_name": spec.step_name,
        "retry_attempt": 0,
    });
    if let Some(rc) = spec.retry_config {
        metadata["retry_config"] = serde_json::to_value(rc).unwrap_or(serde_json::Value::Null);
    }
    let retry_config_json = spec
        .retry_config
        .map(serde_json::to_value)
        .transpose()
        .map_err(|e| StorageError::Database(Box::new(e)))?;

    scheduler
        .schedule_step(
            spec.task_id,
            spec.task_name,
            spec.run_id,
            spec.step_name,
            "step",
            chrono::Utc::now(),
            spec.data,
            metadata,
            retry_config_json.as_ref(),
        )
        .await
}

/// Reschedule a failed step task for retry after a delay.
///
/// Records the failed attempt, updates the retry count on the step row, and
/// reschedules the task to be picked up after `retry_time`.
pub async fn reschedule_step_for_retry(
    scheduler: &dyn StorageBackend,
    step_task_id: &str,
    attempt_number: usize,
    error: &str,
    retry_time: chrono::DateTime<chrono::Utc>,
    lock_token: &str,
) -> Result<(), StorageError> {
    scheduler
        .reschedule_step_for_retry(step_task_id, attempt_number, error, retry_time, lock_token)
        .await
}

/// Insert a wait_all child step task.
pub async fn schedule_wait_all_child(
    scheduler: &dyn StorageBackend,
    task_id: &str,
    task_name: &str,
    run_id: &str,
    step_name: &str,
    coordinator_id: &str,
    data: serde_json::Value,
) -> Result<ScheduleResult, StorageError> {
    let metadata = serde_json::json!({
        "mode": "step",
        "step_type": "step",
        "run_id": run_id,
        "step_name": step_name,
        "is_wait_all_child": true,
        "coordinator_id": coordinator_id,
    });

    scheduler
        .schedule_step(
            task_id,
            task_name,
            run_id,
            step_name,
            "step",
            chrono::Utc::now(),
            data,
            metadata,
            None,
        )
        .await
}

/// Atomically complete a step task and schedule the next body segment.
pub async fn complete_step_and_schedule_body(
    scheduler: &dyn StorageBackend,
    spec: ResumeBodySpec<'_>,
) -> Result<(), StorageError> {
    scheduler
        .complete_step_and_schedule_body(
            spec.step_task_id,
            spec.step_id,
            spec.result,
            spec.lock_token,
            spec.attempt_number,
            spec.next_body_task_id,
            spec.task_name,
            spec.run_id,
            spec.data,
        )
        .await
}

/// Complete a wait_all child step without scheduling a body continuation.
///
/// The coordinator task polls children and schedules the body when all are done.
pub async fn complete_step_no_resume(
    scheduler: &dyn StorageBackend,
    step_task_id: &str,
    step_id: &str,
    result: serde_json::Value,
    lock_token: &str,
    attempt_number: usize,
) -> Result<(), StorageError> {
    scheduler
        .complete_step_no_resume(step_task_id, step_id, result, lock_token, attempt_number)
        .await
}

/// Schedule a coordinator task that polls wait_all children.
pub async fn schedule_coordinator(
    scheduler: &dyn StorageBackend,
    coordinator_task_id: &str,
    task_name: &str,
    run_id: &str,
    wait_for: Vec<String>,
    data: serde_json::Value,
) -> Result<ScheduleResult, StorageError> {
    let metadata = serde_json::json!({
        "mode": "step",
        "step_type": "wait_all",
        "run_id": run_id,
        "wait_for": wait_for,
    });

    // Coordinator is not a step row — just a task insert
    scheduler.schedule_at(scheduler::ScheduleAtParams {
        task_id: coordinator_task_id.to_string(),
        task_name: task_name.to_string(),
        execution_time: chrono::Utc::now(),
        data,
        recurrence: None,
        metadata,
    }).await
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
pub async fn schedule_wait_for_event_task(
    scheduler: &dyn StorageBackend,
    spec: EventStepSpec<'_>,
) -> Result<ScheduleResult, StorageError> {
    let execution_time = spec
        .deadline
        .unwrap_or(chrono::DateTime::<chrono::Utc>::MAX_UTC);
    let metadata = serde_json::json!({
        "mode":         "step",
        "step_type":    "wait_for_event",
        "run_id":       spec.run_id,
        "step_name":    spec.event_name,
    });

    scheduler
        .schedule_step(
            spec.task_id,
            spec.task_name,
            spec.run_id,
            spec.event_name,
            "wait_for_event",
            execution_time,
            spec.data,
            metadata,
            None,
        )
        .await
}

/// Schedule a sleep continuation task.
pub async fn schedule_sleep_task(
    scheduler: &dyn StorageBackend,
    sleep_task_id: &str,
    task_name: &str,
    run_id: &str,
    wake_time: chrono::DateTime<chrono::Utc>,
    data: serde_json::Value,
) -> Result<ScheduleResult, StorageError> {
    let metadata = serde_json::json!({
        "mode": "step",
        "step_type": "sleep",
        "run_id": run_id,
    });

    scheduler
        .schedule_step(
            sleep_task_id,
            task_name,
            run_id,
            "__sleep",
            "sleep",
            wake_time,
            data,
            metadata,
            None,
        )
        .await
}
