//! Read-only execution metadata passed to step closures.

/// Read-only execution metadata passed to step closures.
///
/// This struct provides access to execution metadata like the current retry
/// attempt, execution ID, and task name. It is deliberately separate from
/// [`TaskContext`](super::TaskContext) which provides scheduling methods
/// (`step`, `schedule_step`, etc.) that require `&mut self`.
///
/// Step closures receive a `StepContext` so they can inspect retry state
/// without needing mutable access to the full [`TaskContext`](super::TaskContext).
#[derive(Clone)]
pub struct StepContext {
    pub(crate) execution_id: String,
    pub(crate) task_name: String,
    pub(crate) current_attempt: usize,
    pub(crate) max_retries: Option<usize>,
}

impl StepContext {
    /// Returns the current retry attempt number (0-indexed).
    ///
    /// Returns `0` if this is the first attempt or if no retry is configured.
    /// Returns `1` for the first retry, `2` for the second retry, etc.
    pub fn current_attempt(&self) -> usize {
        self.current_attempt
    }

    /// Returns the maximum number of retry attempts configured for this step.
    ///
    /// Returns `None` if no retry policy is configured.
    pub fn max_retries(&self) -> Option<usize> {
        self.max_retries
    }

    /// Returns `true` if this is a retry attempt (i.e., not the first attempt).
    pub fn is_retry_attempt(&self) -> bool {
        self.current_attempt > 0
    }

    /// Returns the unique ID of the enclosing durable execution.
    pub fn execution_id(&self) -> &str {
        &self.execution_id
    }

    /// Returns the registered name of this task handler.
    pub fn task_name(&self) -> &str {
        &self.task_name
    }
}
