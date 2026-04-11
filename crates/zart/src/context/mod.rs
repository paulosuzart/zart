//! Task execution context — the interface through which durable step execution is managed.
//!
//! This module is split into several sub-modules for maintainability:
//!
//! - `state` — `StepHandle`, attempt history, step records, `ExecutionState`
//! - `step_context` — `StepContext` for read-only execution metadata (internal)
//! - `step_trait` — `ZartStep` trait and `NamedStep` wrapper
//! - `task_context` — `TaskContext` (the primary execution interface)

mod state;
mod step_context;
mod step_trait;
mod task_context;

#[cfg(test)]
mod tests;

pub(crate) use state::PendingFn;
pub use state::{AttemptStatus, ExecutionState, StepAttempt, StepHandle, StepRecord, StepStatus};
pub(crate) use step_context::StepContext;
pub use step_trait::{NamedStep, ZartStep};
pub use task_context::TaskContext;
