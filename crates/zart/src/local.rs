//! Task-local storage for the zart execution context.
//!
//! Two task-locals live here (crate-private). They are set by the worker before
//! dispatching a handler and by `TaskContext::execute_step` when entering a step.
//! Users never touch these directly — they go through the `zart::*` free functions.

use std::sync::Arc;

use crate::context::StepContext;
use crate::context::TaskContext;

/// What kind of code is currently executing.
///
/// This is orthogonal to `ExecutionMode`. `ExecutionMode` (Body vs Step) drives
/// the replay logic at the database level. `Phase` controls what the user's code
/// is allowed to do right now:
///
/// | Phase | When active | `zart::` functions allowed |
/// |---|---|---|
/// | `Body` | Handler body code is running | All (`step`, `schedule`, `sleep`, `wait`, …) |
/// | `Step(StepContext)` | A step's user body is running | Only `zart::context()` |
#[derive(Clone)]
#[allow(dead_code)] // variants are constructed by the worker in Phase 4
pub(crate) enum Phase {
    Body,
    Step(StepContext),
}

tokio::task_local! {
    /// The task-local holding the current `TaskContext` (inside an `Arc`).
    ///
    /// After Phase 1, `TaskContext` has no interior mutability (no `Mutex`,
    /// no `RefCell`), so it is `Send + Sync` and safe inside `Arc`.
    pub(crate) static ZART_CTX: Arc<TaskContext>;

    /// What kind of user code is running right now.
    ///
    /// Set by the worker to `Phase::Body` before calling the handler.
    /// Set by `execute_step` to `Phase::Step(sc)` just before calling the step lambda.
    pub(crate) static ZART_PHASE: Phase;
}

/// Returns the `Arc<TaskContext>` for the current execution.
///
/// Panics if called from `Phase::Step` (i.e. from inside a step body),
/// because scheduling functions are not allowed there.
pub(crate) fn body_ctx() -> Arc<TaskContext> {
    ZART_PHASE.with(|phase| {
        if !matches!(phase, Phase::Body) {
            panic!(
                "zart scheduling functions (step, schedule, sleep, wait, …) \
                 cannot be called from within a step body. \
                 Steps are pure computations; dispatch work from the handler body."
            );
        }
    });
    ZART_CTX.with(Arc::clone)
}
