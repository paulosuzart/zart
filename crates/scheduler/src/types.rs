//! Core types used throughout the scheduler crate.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::Recurrence;

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
    /// The durable execution this task belongs to, if any.
    pub execution_id: Option<String>,
    /// Recurrence configuration, if this is a recurring task.
    pub recurrence: Option<Recurrence>,
    /// Execution model metadata (mode, segment, step_name, step_type, etc.).
    /// Empty object `{}` for legacy tasks that predate the new execution model.
    pub metadata: serde_json::Value,
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
}

impl std::fmt::Display for FetchedTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "task={} exec={} attempt={}",
            self.task_name,
            self.execution_id.as_deref().unwrap_or("-"),
            self.attempt,
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

/// The lifecycle status of a task row in the database.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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

/// The lifecycle status of a durable execution record in `zart_executions`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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
