//! Error types for the Zart durable execution framework.
//!
//! Errors serve a dual purpose in Zart:
//! - **Real errors** that signal failure (e.g., [`StepError::RetryExhausted`])
//! - **Control-flow signals** used internally to drive the execution engine
//!   (e.g., [`StepError::Scheduled`])

use scheduler::StorageError;
use thiserror::Error;

/// Top-level errors from the scheduler / durable execution API.
#[derive(Debug, Error)]
pub enum SchedulerError {
    /// An error from the underlying storage backend.
    #[error("Database error: {0}")]
    Database(#[from] StorageError),

    /// A task with the given name was not registered.
    #[error("Task '{0}' not found in registry")]
    TaskNotFound(String),

    /// The task is already in a terminal state.
    #[error("Task '{0}' is already completed")]
    TaskAlreadyCompleted(String),

    /// No durable execution with the given ID exists.
    #[error("Execution '{0}' not found")]
    ExecutionNotFound(String),

    /// Serialization or deserialization of task data failed.
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// `wait` / `wait_with_timeout` exceeded the maximum wait duration.
    #[error("Timed out waiting for execution '{0}'")]
    WaitTimedOut(String),
}

/// Errors that can occur during task execution.
///
/// A [`TaskError::StepFailed`] wraps a [`StepError`] that escaped from a step.
#[derive(Debug, Error)]
pub enum TaskError {
    /// A step returned an unrecoverable error.
    #[error("Step '{step}' failed: {source}")]
    StepFailed {
        step: String,
        #[source]
        source: StepError,
    },

    /// The task exhausted all retry attempts.
    #[error("Task exhausted retries (max: {max_retries})")]
    MaxRetriesExhausted { max_retries: usize },

    /// The task exceeded its configured timeout.
    #[error("Task timed out after {duration:?}")]
    Timeout { duration: std::time::Duration },

    /// The task was explicitly cancelled.
    #[error("Task was cancelled")]
    Cancelled,

    /// The task handler panicked.
    #[error("Handler panic: {0}")]
    HandlerPanic(String),
}

/// Errors from step execution. Some variants are **control-flow signals**
/// and are not real failures.
#[derive(Debug, Error)]
pub enum StepError {
    /// **Control-flow**: the step has been scheduled for the first time.
    ///
    /// The runtime catches this, persists state, and returns early from the handler.
    /// The task will be re-scheduled so that the step can execute on the next pick-up.
    #[error("Step '{step}' is being scheduled (control flow)")]
    Scheduled { step: String },

    /// The step lambda returned a user-visible failure.
    #[error("Step '{step}' failed: {reason}")]
    Failed { step: String, reason: String },

    /// The step exhausted all configured retry attempts.
    #[error("Step '{step}' retry exhausted after {attempts} attempts")]
    RetryExhausted { step: String, attempts: usize },

    /// The step exceeded its configured timeout.
    #[error("Step '{step}' timed out after {duration:?}")]
    Timeout {
        step: String,
        duration: std::time::Duration,
    },

    /// **Control-flow**: the step is waiting for an external event that hasn't arrived yet.
    #[error("Waiting for event '{event}' (control flow)")]
    WaitingForEvent { event: String },

    /// Any other error from user code.
    #[error(transparent)]
    Other(#[from] Box<dyn std::error::Error + Send + Sync>),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_error_display() {
        let err = TaskError::MaxRetriesExhausted { max_retries: 3 };
        assert!(err.to_string().contains("3"));
    }

    #[test]
    fn step_error_scheduled_is_control_flow() {
        let err = StepError::Scheduled {
            step: "send-email".to_string(),
        };
        assert!(err.to_string().contains("send-email"));
    }
}
