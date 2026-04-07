//! Internal state types: StepHandle, attempt history, step records, and ExecutionState.

use crate::retry::RetryConfig;
use crate::error::StepError;
use super::step_context::StepContext;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

// ── Internal type alias ───────────────────────────────────────────────────────

/// A boxed, one-shot async function that receives a [`StepContext`] and yields a
/// JSON-serialized step result. Used internally by [`StepHandle`] to store a
/// pending step lambda.
pub(crate) type PendingFn = Box<
    dyn FnOnce(
            StepContext,
        ) -> Pin<
            Box<dyn Future<Output = Result<serde_json::Value, StepError>> + Send + 'static>,
        > + Send
        + 'static,
>;

// ── StepHandle ────────────────────────────────────────────────────────────────

/// A handle to a step registered for parallel execution via [`TaskContext::schedule_step`](super::TaskContext::schedule_step).
///
/// Collect handles from multiple `schedule_step` calls and pass them to
/// [`TaskContext::wait_all`](super::TaskContext::wait_all) to execute them and collect results.
pub struct StepHandle<T> {
    pub(crate) step_name: String,
    /// The step lambda wrapped to produce a JSON value. `None` if the step is
    /// already completed (result is cached in state).
    pub(crate) pending: Option<PendingFn>,
    pub(crate) _marker: std::marker::PhantomData<fn() -> T>,
}

// ── Attempt history ──────────────────────────────────────────────────────────

/// The outcome of a single step execution attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptStatus {
    /// The attempt is still in progress (transient, not persisted).
    Running,
    /// The attempt completed successfully.
    Completed,
    /// The attempt failed.
    Failed,
}

/// A record of one execution attempt for a step.
///
/// Each retry produces a new `StepAttempt`, preserving the full history
/// for observability: "Attempt 1 failed with X; Attempt 2 succeeded."
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepAttempt {
    /// 1-indexed attempt number (1 = first try, 2 = first retry, …).
    pub attempt_number: usize,
    /// When this attempt started executing.
    pub started_at: chrono::DateTime<chrono::Utc>,
    /// When this attempt finished (None if still running).
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Outcome of this attempt.
    pub status: AttemptStatus,
    /// Error message if the attempt failed.
    pub error: Option<String>,
    /// JSON result if the attempt succeeded.
    pub result: Option<serde_json::Value>,
}

// ── Step record ──────────────────────────────────────────────────────────────

/// The status of an individual step within a durable execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepStatus {
    /// The step has been registered and scheduled but not yet run.
    Scheduled,
    /// The step completed successfully (result is stored in DB).
    Completed,
}

/// Persisted record for a single step within a durable execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepRecord {
    /// Current lifecycle status of the step.
    pub status: StepStatus,
    /// JSON-serialized return value from the step lambda (set when `Completed`).
    pub result: Option<serde_json::Value>,
    /// Task ID of the underlying scheduled task running this step.
    pub in_task_id: Option<String>,
    /// How many retries have been attempted (0 = no retries yet).
    pub retry_attempt: usize,
    /// The retry policy configured for this step (persisted for observability).
    pub retry_config: Option<RetryConfig>,
    /// Per-attempt history for observability.
    pub attempts: Vec<StepAttempt>,
}

// ── Execution state ──────────────────────────────────────────────────────────

/// The persistent state associated with a durable execution.
///
/// This struct is serialized to JSON and stored in the `state` column of `zart_tasks`.
/// It is re-loaded on every re-entry so that completed steps can be skipped.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ExecutionState {
    /// Map of `step_name → StepRecord` tracking each step's progress.
    pub steps: HashMap<String, StepRecord>,
    /// Arbitrary execution-level metadata, mutable across re-entries.
    pub data: serde_json::Value,
    /// How many times the entire durable execution has been retried.
    pub retry_count: usize,
}
