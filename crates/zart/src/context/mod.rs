//! Task execution context — the interface through which durable step execution is managed.
//!
//! This module is split into several sub-modules for maintainability:
//!
//! - [`step_trait`] — `ZartStep` trait and `StepWithId` wrapper
//! - [`step_context`] — `StepContext` for read-only execution metadata
//! - [`state`] — `StepHandle`, attempt history, step records, `ExecutionState`
//! - [`task_context`] — `TaskContext` (the primary execution interface)

mod state;
mod step_context;
mod step_trait;
mod task_context;

#[cfg(test)]
mod tests;

// Re-export all public types to maintain the `context::*` namespace.
pub(crate) use state::PendingFn;
pub use state::{AttemptStatus, ExecutionState, StepAttempt, StepHandle, StepRecord, StepStatus};
pub use step_context::StepContext;
pub use step_trait::{StepWithId, ZartStep};
pub use task_context::TaskContext;
