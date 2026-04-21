//! Scheduled task trait and associated types.

use async_trait::async_trait;
use serde_json::Value;

use crate::error::StorageError;
use crate::ops::ExecutionOps;

/// Error returned by a [`ScheduledTask`] handler.
#[derive(Debug, thiserror::Error)]
pub enum SchedulerTaskError {
    /// The task failed with a message.
    #[error("{0}")]
    Failed(String),

    /// A storage operation inside the task failed.
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
}

/// Read-only view of the fetched task row passed to a handler.
pub struct TaskInstance {
    pub task_id: String,
    pub task_name: String,
    pub data: Value,
    pub metadata: Value,
    pub lock_token: String,
    pub attempt: u32,
}

/// A handler invoked by the scheduler worker for a specific task name.
///
/// Implement this trait to define task execution logic. The worker dispatches
/// to the handler registered under the task's name in a [`TaskRegistry`](crate::TaskRegistry).
///
/// All imperative operations (complete, reschedule, chain a new task) are
/// performed through the [`ExecutionOps`] handle, which writes through the
/// worker's open transaction. The worker commits after `execute` returns
/// `Ok(())`, or rolls back on `Err`.
///
/// # Default completion
///
/// If `execute` returns `Ok(())` without calling any `ops` method, the worker
/// automatically calls `ops.complete(None)` before committing.
#[async_trait]
pub trait ScheduledTask: Send + Sync {
    async fn execute(
        &self,
        instance: &TaskInstance,
        ops: &mut ExecutionOps<'_>,
    ) -> Result<(), SchedulerTaskError>;
}
