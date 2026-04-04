//! Core types used throughout the scheduler crate.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

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
