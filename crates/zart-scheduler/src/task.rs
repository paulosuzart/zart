//! Scheduled task trait and associated types.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::error::StorageError;
use crate::ops::ExecutionOps;
use crate::recurrence::Recurrence;

/// Error returned by a [`ScheduledTask`] handler.
#[derive(Debug, thiserror::Error)]
pub enum SchedulerTaskError {
    /// The task failed with a message.
    #[error("{0}")]
    Failed(String),

    /// A handler panicked or failed catastrophically.
    #[error("handler panic: {0}")]
    HandlerPanic(String),

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

/// Performs the bookkeeping SQL after a task handler returns `Ok`.
///
/// The worker constructs an [`ExecutionOps`] and calls
/// `completion.complete(ops, recurrence, execution_time)`. The handler uses
/// `ops` to call granular operations (`complete`, `reschedule`,
/// `complete_in_tx`, etc.) rather than accessing the scheduler directly.
///
/// `self: Box<Self>` is required so implementations that own a `Transaction`
/// (which is not `Copy` or `Clone`) can consume it during `complete()`.
#[async_trait]
pub trait CompletionHandler: Send + 'static {
    async fn complete(
        self: Box<Self>,
        ops: ExecutionOps,
        recurrence: Option<&Recurrence>,
        execution_time: DateTime<Utc>,
    ) -> Result<(), StorageError>;
}

/// A handler invoked by the scheduler worker for a specific task name.
///
/// Implement this trait to define task execution logic. The worker dispatches
/// to the handler registered under the task's name in a [`TaskRegistry`](crate::TaskRegistry).
///
/// On success, return a [`CompletionHandler`] that the worker will invoke to
/// persist the task outcome. On failure, return an error and the worker will
/// call `mark_failed`.
///
/// External implementations can return standard handlers from
/// [`crate::completion`](crate::completion): [`OnComplete`](crate::completion::OnComplete),
/// [`OnReschedule`](crate::completion::OnReschedule), or
/// [`WithTransaction`](crate::completion::WithTransaction).
#[async_trait]
pub trait ScheduledTask: Send + Sync {
    async fn execute(
        &self,
        instance: &TaskInstance,
    ) -> Result<Box<dyn CompletionHandler>, SchedulerTaskError>;
}
