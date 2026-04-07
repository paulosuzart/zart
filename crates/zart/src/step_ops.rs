//! Free functions that implement execution-model-specific scheduling.
//!
//! These compose the generic [`Scheduler`] primitives (`schedule_at`,
//! `complete_and_schedule`, `mark_completed`) to perform operations specific
//! to the per-row step execution model.
//!
//! Keeping this logic here means [`PostgresScheduler`] remains a clean,
//! generic storage backend with no execution-model knowledge.

use chrono::Utc;
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

    let mut tx = scheduler.begin().await?;
    tx.insert_task(ScheduleAtParams {
        task_id: spec.task_id.to_string(),
        task_name: spec.task_name.to_string(),
        execution_time: Utc::now(),
        data: spec.data,
        recurrence: None,
        execution_id: None,
        metadata,
    })
    .await?;

    let step_kind = "step";
    let retry_config_json = spec
        .retry_config
        .map(serde_json::to_value)
        .transpose()
        .map_err(|e| StorageError::Database(Box::new(e)))?;

    tx.insert_step(
        spec.task_id,
        spec.run_id,
        spec.step_name,
        step_kind,
        spec.task_id,
        retry_config_json.as_ref(),
    )
    .await?;

    tx.commit().await?;

    Ok(ScheduleResult {
        task_id: spec.task_id.to_string(),
        execution_time: Utc::now(),
    })
}

/// Reschedule a failed step task for retry after a delay.
///
/// Marks the step task as failed with a future execution time so the worker
/// will pick it up again after the retry delay. The scheduler's built-in
/// `task.attempt` counter increments on each pickup and is used to track
/// the retry attempt number.
pub async fn reschedule_step_for_retry(
    scheduler: &dyn StorageBackend,
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

    let mut tx = scheduler.begin().await?;
    tx.insert_task(ScheduleAtParams {
        task_id: task_id.to_string(),
        task_name: task_name.to_string(),
        execution_time: Utc::now(),
        data,
        recurrence: None,
        execution_id: None,
        metadata,
    })
    .await?;

    tx.insert_step(task_id, run_id, step_name, "step", task_id, None)
        .await?;

    tx.commit().await?;

    Ok(ScheduleResult {
        task_id: task_id.to_string(),
        execution_time: Utc::now(),
    })
}

/// Atomically complete a step task and schedule the next body segment.
pub async fn complete_step_and_schedule_body(
    scheduler: &dyn StorageBackend,
    spec: ResumeBodySpec<'_>,
) -> Result<(), StorageError> {
    let body_metadata = serde_json::json!({
        "mode": "body",
        "run_id": spec.run_id,
    });

    let mut tx = scheduler.begin().await?;
    tx.complete_step(spec.step_id, spec.result.clone(), Utc::now())
        .await?;
    tx.mark_task_completed(spec.step_task_id, Some(spec.result), spec.lock_token)
        .await?;
    tx.insert_body_task(
        spec.next_body_task_id,
        spec.task_name,
        spec.run_id,
        Utc::now(),
        spec.data,
        body_metadata,
    )
    .await?;
    tx.commit().await
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
) -> Result<(), StorageError> {
    let mut tx = scheduler.begin().await?;
    tx.complete_step(step_id, result.clone(), Utc::now())
        .await?;
    tx.mark_task_completed(step_task_id, Some(result), lock_token)
        .await?;
    tx.commit().await
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

    let mut tx = scheduler.begin().await?;
    tx.insert_task(ScheduleAtParams {
        task_id: coordinator_task_id.to_string(),
        task_name: task_name.to_string(),
        execution_time: Utc::now(),
        data,
        recurrence: None,
        execution_id: None,
        metadata,
    })
    .await?;

    // Coordinator is not a step, so no insert_step call needed.
    tx.commit().await?;

    Ok(ScheduleResult {
        task_id: coordinator_task_id.to_string(),
        execution_time: Utc::now(),
    })
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

    let mut tx = scheduler.begin().await?;
    tx.insert_task(ScheduleAtParams {
        task_id: spec.task_id.to_string(),
        task_name: spec.task_name.to_string(),
        execution_time,
        data: spec.data,
        recurrence: None,
        execution_id: None,
        metadata,
    })
    .await?;

    tx.insert_step(
        spec.task_id,
        spec.run_id,
        spec.event_name,
        "wait_for_event",
        spec.task_id,
        None,
    )
    .await?;

    tx.commit().await?;

    Ok(ScheduleResult {
        task_id: spec.task_id.to_string(),
        execution_time,
    })
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

    let mut tx = scheduler.begin().await?;
    tx.insert_task(ScheduleAtParams {
        task_id: sleep_task_id.to_string(),
        task_name: task_name.to_string(),
        execution_time: wake_time,
        data,
        recurrence: None,
        execution_id: None,
        metadata,
    })
    .await?;

    tx.insert_step(
        sleep_task_id,
        run_id,
        "__sleep",
        "sleep",
        sleep_task_id,
        None,
    )
    .await?;

    tx.commit().await?;

    Ok(ScheduleResult {
        task_id: sleep_task_id.to_string(),
        execution_time: wake_time,
    })
}
