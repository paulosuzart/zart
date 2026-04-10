//! Read-only execution metadata for step closures (internal).

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
