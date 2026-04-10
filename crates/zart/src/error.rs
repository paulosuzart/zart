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

    /// An execution with this ID already exists and is not in a terminal state.
    #[error("Execution '{0}' already exists (status: {1})")]
    ExecutionAlreadyExists(String, scheduler::ExecutionStatus),

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

// ── StepOutcome & ZartStepError ──────────────────────────────────────────────

/// The outcome of a completed step as seen by the handler body.
///
/// This distinguishes between business errors (the step's own `E` type) and
/// framework errors (retry exhausted, timeout, deadline exceeded).
///
/// Use with [`crate::step()`] for explicit error handling, or use
/// [`crate::require()`] for fail-fast semantics.
pub enum StepOutcome<T, E> {
    /// Step logic succeeded.
    Ok(T),
    /// Step logic returned `Err(E)` — the user's domain error.
    ///
    /// This variant carries the step's own error type, which is meaningful to
    /// the handler body. Match on its variants to handle specific failures.
    BusinessErr(E),
    /// The framework could not complete the step regardless of step logic.
    ///
    /// Covers retry budget exhausted, timeout exceeded, or deadline passed.
    /// Not a user type — it signals framework-level failure.
    ZartErr(ZartStepError),
}

impl<T, E> std::fmt::Debug for StepOutcome<T, E>
where
    T: std::fmt::Debug,
    E: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ok(v) => f.debug_tuple("Ok").field(v).finish(),
            Self::BusinessErr(e) => f.debug_tuple("BusinessErr").field(e).finish(),
            Self::ZartErr(e) => f.debug_tuple("ZartErr").field(e).finish(),
        }
    }
}

/// Framework-level step failure. Not a user type.
///
/// Returned via [`StepOutcome::ZartErr`] when the framework cannot complete a
/// step regardless of the step's own logic (retry exhausted, timeout, deadline).
#[derive(Debug)]
pub enum ZartStepError {
    /// The step failed on every attempt and the retry budget is exhausted.
    ///
    /// `last_error` carries the serialized final `S::Error` for inspection.
    RetryExhausted {
        step: String,
        attempts: usize,
        last_error: serde_json::Value,
    },
    /// The step exceeded its configured execution timeout.
    TimedOut {
        step: String,
        duration: std::time::Duration,
    },
    /// A wait_for_event deadline passed before the event arrived.
    DeadlineExceeded { step: String },
}

impl std::fmt::Display for ZartStepError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ZartStepError::RetryExhausted { step, attempts, .. } => {
                write!(
                    f,
                    "Step '{}' retry exhausted after {} attempts",
                    step, attempts
                )
            }
            ZartStepError::TimedOut { step, duration } => {
                write!(f, "Step '{}' timed out after {:?}", step, duration)
            }
            ZartStepError::DeadlineExceeded { step } => {
                write!(f, "Step '{}' deadline exceeded", step)
            }
        }
    }
}

impl std::error::Error for ZartStepError {}

impl ZartStepError {
    /// Attempt to deserialize the last business error from a `RetryExhausted` failure.
    ///
    /// Returns `None` for other variants.
    pub fn last_error<E: serde::de::DeserializeOwned>(
        &self,
    ) -> Option<Result<E, serde_json::Error>> {
        match self {
            ZartStepError::RetryExhausted { last_error, .. } => {
                Some(serde_json::from_value(last_error.clone()))
            }
            _ => None,
        }
    }
}

// ── ExecutionFailure ─────────────────────────────────────────────────────────

/// Describes why `on_failure` was invoked on a [`DurableExecution`].
///
/// Returned to the centralized failure handler when a step failure propagates
/// out of the body, or when an execution-level failure occurs.
pub enum ExecutionFailure {
    /// A step's failure propagated out of the body via `?`.
    ///
    /// Covers both business errors and `ZartStepError`s that were not handled inline.
    StepFailed {
        step: String,
        /// Serialized failure envelope — inspect or ignore.
        raw: serde_json::Value,
    },
    /// The execution's own deadline was exceeded before or during the body.
    ///
    /// **Not** fired for `wait_for_event` step-level deadlines. This variant is
    /// only reachable when the execution's own timer fires at the worker level,
    /// before or between body invocations.
    ExecutionDeadlineExceeded,
    /// The execution's own retry policy was exhausted.
    RetriesExhausted { attempts: usize },
}

impl std::fmt::Debug for ExecutionFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StepFailed { step, raw } => f
                .debug_struct("StepFailed")
                .field("step", step)
                .field("raw", raw)
                .finish(),
            Self::ExecutionDeadlineExceeded => f.debug_struct("ExecutionDeadlineExceeded").finish(),
            Self::RetriesExhausted { attempts } => f
                .debug_struct("RetriesExhausted")
                .field("attempts", attempts)
                .finish(),
        }
    }
}

impl std::fmt::Display for ExecutionFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecutionFailure::StepFailed { step, .. } => {
                write!(f, "Step '{}' failed", step)
            }
            ExecutionFailure::ExecutionDeadlineExceeded => {
                write!(f, "Execution deadline exceeded")
            }
            ExecutionFailure::RetriesExhausted { attempts } => {
                write!(f, "Execution retries exhausted after {} attempts", attempts)
            }
        }
    }
}

/// Errors from step execution. Some variants are **control-flow signals**
/// and are not real failures.
///
/// This type is `#[non_exhaustive]` — users propagate it via `?` only; they
/// never construct or match on it. For framework-level failures visible to the
/// handler body, use [`ZartStepError`] (returned via [`StepOutcome::ZartErr`]).
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum StepError {
    /// **Control-flow**: the step has been scheduled (first time) or a retry is pending.
    ///
    /// The runtime catches this, persists state, and returns early from the handler.
    /// The task will be re-scheduled at `next_execution` (or immediately if `None`).
    #[error("Step '{step}' is being scheduled (control flow)")]
    Scheduled {
        step: String,
        /// When the task should next execute. `None` means immediately.
        next_execution: Option<chrono::DateTime<chrono::Utc>>,
    },

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

    /// The wait_for_event deadline was exceeded before the event arrived.
    #[error("Step '{step}' deadline exceeded")]
    DeadlineExceeded { step: String },

    /// Any other error from user code.
    #[error(transparent)]
    Other(#[from] Box<dyn std::error::Error + Send + Sync>),

    /// **Control-flow** (new execution model): a step task executed its lambda and
    /// completed transactionally (step marked completed + next body scheduled in one
    /// DB transaction). The worker should do nothing further for this task.
    #[error("Step '{step}' executed in step mode (transactional completion done)")]
    StepExecuted { step: String },
}

/// Converts a [`StepError`] into a [`TaskError::StepFailed`].
///
/// This enables the `?` operator in task handlers that return
/// `Result<_, TaskError>` when calling `ctx.execute_step(...)`.
///
/// [`StepError::Scheduled`] is a control-flow signal, not a real failure —
/// the worker inspects the wrapped variant and handles it specially.
impl From<StepError> for TaskError {
    fn from(e: StepError) -> Self {
        let step = match &e {
            StepError::Scheduled { step, .. } => step.clone(),
            StepError::Failed { step, .. } => step.clone(),
            StepError::RetryExhausted { step, .. } => step.clone(),
            StepError::Timeout { step, .. } => step.clone(),
            StepError::DeadlineExceeded { step } => step.clone(),
            StepError::StepExecuted { step } => step.clone(),
            StepError::Other(_) => "unknown".to_string(),
        };
        TaskError::StepFailed { step, source: e }
    }
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
            next_execution: None,
        };
        assert!(err.to_string().contains("send-email"));
    }
}
