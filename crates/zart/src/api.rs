//! Public free functions for the zart API.
//!
//! All user-facing operations are exposed as free functions. There are no
//! `TaskContext` methods that users ever call — the type is a framework
//! implementation detail.

use crate::context::{StepHandle, ZartStep};
use crate::error::StepError;
use crate::execution_model::ExecutionMode;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

use crate::local::{Phase, ZART_CTX, ZART_PHASE, body_ctx};

// ── Free functions ─────────────────────────────────────────────────────────────

/// Execute a step and return its result.
///
/// This is the primary way to call a step from a durable handler body.
/// The step is executed with automatic retry and timeout handling.
///
/// # Panics
///
/// Panics if called from inside a step body (i.e. from `Phase::Step`).
/// Steps may not schedule other steps.
pub async fn step<S: ZartStep + Send>(s: S) -> Result<S::Output, StepError> {
    body_ctx().execute_step(s).await
}

/// Register a step for parallel execution without waiting for it to complete.
///
/// The returned handle can be passed to [`wait`] to collect results.
///
/// # Panics
///
/// Panics if called from inside a step body.
pub fn schedule<S: ZartStep + Send + 'static>(s: S) -> StepHandle<S::Output> {
    body_ctx().schedule_step(s)
}

/// Wait for all handles returned by [`schedule`] to complete.
///
/// Returns `Ok(results)` where each element corresponds to one handle in order.
/// An individual step failure appears as `Err(StepError)` inside the `Vec`;
/// the outer `Err` is reserved for control-flow or programming errors.
///
/// # Panics
///
/// Panics if called from inside a step body.
pub async fn wait<T>(handles: Vec<StepHandle<T>>) -> Result<Vec<Result<T, StepError>>, StepError>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    body_ctx().wait_all(handles).await
}

/// Suspend execution for `duration`, resuming at `now + duration`.
///
/// The `name` must be a stable, unique string within this execution body.
/// Treat it like a migration name — do not change it after the execution has started.
///
/// # Panics
///
/// Panics if called from inside a step body.
pub async fn sleep(name: &str, duration: Duration) -> Result<(), StepError> {
    body_ctx().sleep(name, duration).await
}

/// Suspend execution until `wake_time`.
///
/// # Panics
///
/// Panics if called from inside a step body.
pub async fn sleep_until(
    name: &str,
    wake_time: chrono::DateTime<chrono::Utc>,
) -> Result<(), StepError> {
    body_ctx().sleep_until(name, wake_time).await
}

/// Wait for an external event to be delivered to this execution.
///
/// # Panics
///
/// Panics if called from inside a step body.
pub async fn wait_for_event<T: DeserializeOwned>(
    name: &str,
    timeout: Option<Duration>,
) -> Result<T, StepError> {
    body_ctx().wait_for_event(name, timeout).await
}

/// Capture a synchronous, pure value durably.
///
/// On first body run: evaluates `f()`, writes the result as a completed step row,
/// returns the value — body walk continues without parking.
/// On replay: returns the cached DB value; `f` is never called.
///
/// # Panics
///
/// Panics if called from inside a step body.
pub async fn capture<T, F>(name: &str, f: F) -> Result<T, StepError>
where
    T: Serialize + for<'de> Deserialize<'de>,
    F: FnOnce() -> T,
{
    body_ctx().capture(name, f).await
}

/// Capture the current UTC time durably.
///
/// Shorthand for `capture(name, chrono::Utc::now)`.
///
/// # Panics
///
/// Panics if called from inside a step body.
pub async fn now(name: &str) -> Result<chrono::DateTime<chrono::Utc>, StepError> {
    body_ctx().now(name).await
}

// ── Read-only introspection ────────────────────────────────────────────────────

/// Read-only view of the current execution metadata.
///
/// Returned by [`context()`]. Usable from both handler body and step body.
pub struct ExecutionInfo {
    /// Unique identifier of the enclosing durable execution.
    pub execution_id: String,
    /// Registered name of the task handler.
    pub task_name: String,
    /// The original JSON payload (read-only view).
    pub data: serde_json::Value,
    /// Current retry attempt number (0-indexed). `0` on first attempt.
    pub current_attempt: usize,
    /// Maximum configured retries (`None` if no retry policy).
    pub max_retries: Option<usize>,
}

impl ExecutionInfo {
    /// Returns `true` if this is a retry attempt (i.e. not the first attempt).
    pub fn is_retry(&self) -> bool {
        self.current_attempt > 0
    }
}

/// Returns read-only information about the current execution.
///
/// Callable from **anywhere** inside an execution — handler body or step body.
///
/// # Panics
///
/// Panics if called outside a zart execution context (i.e. when the task-locals
/// have not been set by the worker).
pub fn context() -> ExecutionInfo {
    let ctx = ZART_CTX.with(Arc::clone);
    let (current_attempt, max_retries) = ZART_PHASE.with(|phase| match phase {
        Phase::Step(sc) => (sc.current_attempt(), sc.max_retries()),
        Phase::Body => match &ctx.execution_mode {
            ExecutionMode::Step {
                retry_attempt,
                retry_config,
                ..
            } => (
                *retry_attempt,
                retry_config.as_ref().map(|rc| rc.max_attempts),
            ),
            _ => (0, None),
        },
    });
    ExecutionInfo {
        execution_id: ctx.execution_id().to_string(),
        task_name: ctx.task_name().to_string(),
        data: ctx.data().clone(),
        current_attempt,
        max_retries,
    }
}
