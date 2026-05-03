//! Core types used throughout the Zart storage layer.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::recurrence::Recurrence;
use crate::task_metadata::TaskMetadata;

// ── Task lifecycle ────────────────────────────────────────────────────────────

/// The lifecycle status of a task row in the database.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(sqlx::Type)]
#[sqlx(type_name = "task_status", rename_all = "snake_case")]
pub enum TaskStatus {
    /// Waiting to be picked up by a worker.
    Scheduled,
    /// Currently being executed by a worker (locked).
    PickedUp,
    /// Execution finished successfully.
    Completed,
    /// Execution failed; may be retried.
    Failed,
    /// All retry attempts exhausted.
    Dead,
    /// Cancelled before execution.
    Cancelled,
}

impl std::str::FromStr for TaskStatus {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "scheduled" => Ok(Self::Scheduled),
            "picked_up" => Ok(Self::PickedUp),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "dead" => Ok(Self::Dead),
            "cancelled" => Ok(Self::Cancelled),
            other => Err(format!("unknown task status: {other}")),
        }
    }
}

/// A task that has been fetched from the database and is ready for execution.
///
/// The `lock_token` must be passed back when completing or failing the task
/// to ensure only the worker that fetched the task can update it.
#[derive(Debug, Clone)]
pub struct FetchedTask {
    /// Unique task identifier.
    pub task_id: String,
    /// The registered handler name.
    pub task_name: String,
    /// Serialized input payload.
    pub data: serde_json::Value,
    /// Serialized step/execution state for durable flows.
    pub state: serde_json::Value,
    /// How many times this task has been attempted (including the current attempt).
    pub attempt: usize,
    /// Opaque token that identifies this particular lock acquisition.
    pub lock_token: String,
    /// Recurrence configuration, if this is a recurring task.
    pub recurrence: Option<Recurrence>,
    /// Execution model metadata (mode, run_id, step_name, step_type, etc.).
    /// Empty object `{}` for legacy tasks that predate the new execution model.
    pub metadata: serde_json::Value,
    /// The time this task was originally scheduled to execute.
    /// Used for computing next cron execution times.
    pub execution_time: DateTime<Utc>,
}

impl std::fmt::Display for FetchedTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let exec_id = TaskMetadata::from_json_value(self.metadata.clone())
            .ok()
            .map(|m| m.execution_id().to_string());
        let exec_id = exec_id.as_deref().unwrap_or("-");
        write!(
            f,
            "task={} exec={} attempt={}",
            self.task_name, exec_id, self.attempt,
        )
    }
}

/// The result of successfully scheduling a task.
#[derive(Debug, Clone)]
pub struct ScheduleResult {
    /// The task ID that was scheduled.
    pub task_id: String,
    /// The time at which the task is scheduled to run.
    pub execution_time: DateTime<Utc>,
}

/// Result of looking up a step task by execution_id + step_name.
#[derive(Debug, Clone)]
pub struct StepLookup {
    /// The task_id of the step row.
    pub task_id: String,
    /// Current lifecycle status of the step task.
    pub status: TaskStatus,
    /// JSON result stored when the step completed. `None` if not yet complete.
    pub result: Option<serde_json::Value>,
    /// Outcome discriminant. `None` for rows written before this column existed (treated as `Ok`).
    pub result_kind: Option<StepResultKind>,
}

/// Parameters for `StorageBackend::schedule_at`.
#[derive(Debug, Clone)]
pub struct ScheduleAtParams {
    /// Unique identifier for the new task row.
    pub task_id: String,
    /// Registered handler name.
    pub task_name: String,
    /// Earliest time the task may be picked up.
    pub execution_time: DateTime<Utc>,
    /// Serialized input payload.
    pub data: serde_json::Value,
    /// Optional recurrence rule for repeating tasks.
    pub recurrence: Option<Recurrence>,
    /// Execution-model metadata (mode, run_id, step_name, …).
    pub metadata: serde_json::Value,
}

/// Parameters for `StorageBackend::complete_and_schedule`.
#[derive(Debug)]
pub struct CompleteAndScheduleParams {
    /// Task ID of the task being completed.
    pub completed_task_id: String,
    /// Optional result payload for the completed task.
    pub result: Option<serde_json::Value>,
    /// Lock token that must match the worker's current hold.
    pub lock_token: String,
    /// Task ID for the newly inserted task.
    pub new_task_id: String,
    /// Handler name for the newly inserted task.
    pub new_task_name: String,
    /// Scheduled execution time for the new task.
    pub new_execution_time: DateTime<Utc>,
    /// Input payload for the new task.
    pub new_data: serde_json::Value,
    /// Execution-model metadata for the new task.
    pub new_metadata: serde_json::Value,
}

// ── Execution lifecycle ───────────────────────────────────────────────────────

/// The lifecycle status of a durable execution record in `zart_executions`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(sqlx::Type)]
#[sqlx(type_name = "execution_status", rename_all = "snake_case")]
pub enum ExecutionStatus {
    Scheduled,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl std::fmt::Display for ExecutionStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Scheduled => write!(f, "scheduled"),
            Self::Running => write!(f, "running"),
            Self::Completed => write!(f, "completed"),
            Self::Failed => write!(f, "failed"),
            Self::Cancelled => write!(f, "cancelled"),
        }
    }
}

impl std::str::FromStr for ExecutionStatus {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "scheduled" => Ok(Self::Scheduled),
            "running" => Ok(Self::Running),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            other => Err(format!("unknown execution status: {other}")),
        }
    }
}

/// What triggered a run of a durable execution (`zart_execution_runs.trigger`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(sqlx::Type)]
#[sqlx(type_name = "execution_trigger", rename_all = "snake_case")]
pub enum ExecutionTrigger {
    /// First ever run of this execution.
    Initial,
    /// A manual or automatic restart.
    Restart,
    /// A selective re-run of specific steps.
    SelectiveRerun,
}

/// A durable execution record fetched from `zart_executions`.
#[derive(Debug, Clone)]
pub struct ExecutionRecord {
    pub execution_id: String,
    pub task_name: String,
    pub payload: serde_json::Value,
    pub status: ExecutionStatus,
    pub result: Option<serde_json::Value>,
    pub scheduled_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub version: i32,
}

/// A single run of a durable execution.
///
/// Each run represents one invocation of an execution, starting from run_index = 0.
/// Runs are append-only: restarts create new rows, they don't mutate existing ones.
#[derive(Debug, Clone)]
pub struct ExecutionRunRecord {
    /// Unique run identifier.
    pub run_id: String,
    /// The execution this run belongs to.
    pub execution_id: String,
    /// Run index: 0 = first, 1 = first restart, …
    pub run_index: i32,
    /// Input payload for this run.
    pub payload: serde_json::Value,
    /// Current status of this run.
    pub status: ExecutionStatus,
    /// Result payload (set when completed).
    pub result: Option<serde_json::Value>,
    /// When this run started.
    pub started_at: DateTime<Utc>,
    /// When this run finished (None if still running).
    pub completed_at: Option<DateTime<Utc>>,
    /// What triggered this run.
    pub trigger: ExecutionTrigger,
}

/// Execution count statistics grouped by status.
#[derive(Debug, Clone, Default)]
pub struct ExecutionStats {
    pub scheduled: i64,
    pub running: i64,
    pub completed: i64,
    pub failed: i64,
    pub cancelled: i64,
}

/// Field to sort execution listings by.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum ExecutionSortField {
    #[default]
    ScheduledAt,
    Status,
    TaskName,
}

/// Sort direction for execution listings.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum SortOrder {
    #[default]
    Desc,
    Asc,
}

/// Parameters for filtered, paginated execution listing.
#[derive(Debug, Clone)]
pub struct ListExecutionsParams {
    pub status: Option<ExecutionStatus>,
    pub task_name: Option<String>,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    pub search: Option<String>,
    pub sort_by: ExecutionSortField,
    pub sort_order: SortOrder,
    pub limit: usize,
    pub offset: usize,
}

impl Default for ListExecutionsParams {
    fn default() -> Self {
        Self {
            status: None,
            task_name: None,
            from: None,
            to: None,
            search: None,
            sort_by: ExecutionSortField::default(),
            sort_order: SortOrder::default(),
            limit: 20,
            offset: 0,
        }
    }
}

// ── Step lifecycle ────────────────────────────────────────────────────────────

/// The kind of step stored in `zart_steps.step_kind`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(sqlx::Type)]
#[sqlx(type_name = "step_kind", rename_all = "snake_case")]
pub enum StepKind {
    /// A user-defined step with a lambda.
    Step,
    /// A timed pause (`zart::sleep`). No lambda — fires when `execution_time` arrives.
    Sleep,
    /// A fan-out wait: all children must complete (or threshold reached) before resuming.
    WaitAll,
    /// An external event wait. Parked until the event is delivered.
    WaitForEvent,
    /// A wait-group coordinator row. Tracks child completion state.
    WaitGroup,
    /// A capture step: synchronously persisted on first encounter, no task row.
    Capture,
}

/// The lifecycle status of a step row in `zart_steps`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(sqlx::Type)]
#[sqlx(type_name = "step_status", rename_all = "snake_case")]
pub enum StepStatus {
    /// Waiting to be picked up by a worker.
    Scheduled,
    /// Currently being executed by a worker.
    Running,
    /// Step finished (successfully or with a recorded error).
    Completed,
    /// All retry attempts exhausted; step will not be retried.
    Dead,
}

/// Outcome discriminant for a completed step, stored in `zart_steps.result_kind`.
///
/// The short abbreviations (`Rx`, `Dl`) match the pre-existing PostgreSQL
/// `step_result_kind` enum labels introduced in the initial schema migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(sqlx::Type)]
#[sqlx(type_name = "step_result_kind", rename_all = "snake_case")]
pub enum StepResultKind {
    /// Step succeeded — `result` holds the serialized output.
    Ok,
    /// Step returned a business error — `result` holds the serialized error.
    Err,
    /// All retries exhausted — `result` holds the last serialized error.
    Rx,
    /// Step timed out — `result` is NULL.
    Timeout,
    /// wait_for_event deadline exceeded — `result` is NULL.
    Dl,
}

/// A step record from the `zart_steps` table.
///
/// Represents the authoritative state of a single step within a specific run.
#[derive(Debug, Clone)]
pub struct StepRow {
    /// Step ID (same as the step task_id).
    pub step_id: String,
    /// The run this step belongs to.
    pub run_id: String,
    /// The step name (unique within a run).
    pub step_name: String,
    /// What kind of step this is.
    pub step_kind: StepKind,
    /// The task currently responsible for this step (None if completed or not yet scheduled).
    pub task_id: Option<String>,
    /// Current lifecycle status of this step.
    pub status: StepStatus,
    /// How many retries have been attempted (0 = no retries yet).
    pub retry_attempt: i32,
    /// Retry policy serialized as JSON.
    pub retry_config: Option<serde_json::Value>,
    /// Result payload (set when completed).
    pub result: Option<serde_json::Value>,
    /// Error message (set when failed).
    pub last_error: Option<String>,
    /// Wait-group total children count (NULL for non-wait-group steps).
    pub wg_total: Option<i32>,
    /// Wait-group remaining children count (NULL for non-wait-group steps).
    pub wg_remaining: Option<i32>,
    /// Wait-group trigger threshold (NULL for non-wait-group steps).
    pub wg_threshold: Option<i32>,
    /// Compare-and-set guard for first failing child in wait-groups (NULL for non-wait-group steps).
    pub wg_first_failed: Option<bool>,
    /// When this step was scheduled.
    pub scheduled_at: DateTime<Utc>,
    /// When this step completed (None if still in progress).
    pub completed_at: Option<DateTime<Utc>>,
}

/// Lifecycle status of a step attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(sqlx::Type)]
#[sqlx(type_name = "step_attempt_status", rename_all = "snake_case")]
pub enum StepAttemptStatus {
    Completed,
    Failed,
}

/// A single attempt record from the `zart_step_attempts` table.
#[derive(Debug, Clone)]
pub struct StepAttemptRow {
    pub attempt_id: String,
    pub step_id: String,
    pub attempt_number: i32,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub status: StepAttemptStatus,
    pub result: Option<serde_json::Value>,
    pub error: Option<String>,
}

// ── Step operation parameters ─────────────────────────────────────────────────

/// Parameters for durable storage step scheduling.
#[derive(Debug, Clone)]
pub struct ScheduleStepParams {
    pub task_id: String,
    pub task_name: String,
    pub run_id: String,
    pub step_name: String,
    pub step_kind: StepKind,
    pub execution_time: DateTime<Utc>,
    pub data: serde_json::Value,
    pub metadata: serde_json::Value,
    pub retry_config: Option<serde_json::Value>,
}

/// Parameters for writing step completion SQL (INSERT step_attempts, UPDATE steps).
///
/// Passed to `StepStore::write_step_completion_in_tx`. Does not schedule the next
/// body task — that responsibility belongs to the caller via `ops.complete_in_tx`.
#[derive(Debug, Clone)]
pub struct WriteStepCompletionParams {
    pub step_id: String,
    pub attempt_number: usize,
    pub result: serde_json::Value,
    /// Outcome discriminant stored in `zart_steps.result_kind`.
    pub result_kind: StepResultKind,
}

/// Parameters for step completion without body resume.
#[derive(Debug, Clone)]
pub struct CompleteStepNoResumeParams {
    pub step_task_id: String,
    pub step_id: String,
    pub result: serde_json::Value,
    pub lock_token: String,
    pub attempt_number: usize,
}

/// Parameters for retry rescheduling of a step task.
#[derive(Debug, Clone)]
pub struct RescheduleStepForRetryParams {
    pub step_task_id: String,
    pub attempt_number: usize,
    pub error: String,
    pub retry_time: DateTime<Utc>,
    pub lock_token: String,
}

/// Parameters for creating/upserting a wait-group step row.
#[derive(Debug, Clone)]
pub struct UpsertWaitGroupStepParams {
    pub run_id: String,
    pub group_step_name: String,
    pub total: i32,
    pub threshold: i32,
}

/// Parameters for completing a wait-group child.
#[derive(Debug, Clone)]
pub struct CompleteWaitGroupChildParams {
    pub run_id: String,
    pub execution_id: String,
    pub group_step_name: String,
    pub child_step_task_id: String,
    pub child_step_id: String,
    pub child_result: serde_json::Value,
    pub lock_token: String,
    pub attempt_number: usize,
    pub next_body_task_id: String,
    pub data: serde_json::Value,
}

/// Parameters for failing a wait-group child.
#[derive(Debug, Clone)]
pub struct FailWaitGroupChildParams {
    pub run_id: String,
    pub group_step_name: String,
    pub child_step_task_id: String,
    pub child_step_id: String,
    pub error: String,
    pub lock_token: String,
    pub attempt_number: usize,
}

/// Result of attempting to deliver an external event to a waiting execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventDeliveryResult {
    /// Event matched a scheduled wait_for_event step and resumed the body.
    Delivered,
    /// Event targeted a step that had already been completed by a previous delivery.
    AlreadyDelivered,
    /// No matching wait_for_event step was registered for this execution/event.
    NotRegistered,
}
