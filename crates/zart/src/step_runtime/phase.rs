//! Execution phase tracking and step-level metadata (internal).
//!
//! Kept separate from `local` (`ZART_CTX`) and `context` (`TaskContext`) so that
//! `trx_impl` and `context` can read/scope `ZART_PHASE` without creating a
//! module cycle with `TaskContext`.

/// Read-only execution metadata for step closures.
///
/// This struct is internal — users access execution metadata via `zart::context()`.
/// It lives on as `Phase::Step(StepContext)` in task-local storage and is populated
/// by `execute_step` when entering a step.
#[derive(Clone)]
pub(crate) struct StepContext {
    pub(crate) current_attempt: usize,
    pub(crate) max_retries: Option<usize>,
}

impl StepContext {
    pub(crate) fn current_attempt(&self) -> usize {
        self.current_attempt
    }

    pub(crate) fn max_retries(&self) -> Option<usize> {
        self.max_retries
    }
}

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
#[allow(dead_code)] // variants are constructed by the worker
pub(crate) enum Phase {
    Body,
    Step(StepContext),
}

tokio::task_local! {
    /// What kind of user code is running right now.
    ///
    /// Set by the worker to `Phase::Body` before calling the handler.
    /// Set by `execute_step` to `Phase::Step(sc)` just before calling the step lambda.
    pub(crate) static ZART_PHASE: Phase;
}

/// Returns `true` if the current phase is `Phase::Step`.
///
/// Used by `zart::trx` to guard against being called outside a step.
pub(crate) fn is_step_phase() -> bool {
    ZART_PHASE
        .try_with(|phase| matches!(phase, Phase::Step(_)))
        .unwrap_or(false)
}
