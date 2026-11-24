//! Task-local storage for the zart execution context.
//!
//! `ZART_CTX` lives here (crate-private). It is set by the worker before
//! dispatching a handler. Users never touch it directly — they go through the
//! `zart::*` free functions.

use std::sync::Arc;

use crate::context::TaskContext;
use crate::phase::{Phase, ZART_PHASE};

tokio::task_local! {
    /// The task-local holding the current `TaskContext` (inside an `Arc`).
    ///
    /// `TaskContext` has no interior mutability (no `Mutex`,
    /// no `RefCell`), so it is `Send + Sync` and safe inside `Arc`.
    pub(crate) static ZART_CTX: Arc<TaskContext>;
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
