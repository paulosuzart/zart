//! Open task registry — maps task names to [`ScheduledTask`] handlers.

use std::collections::HashMap;
use std::sync::Arc;

use crate::task::ScheduledTask;

/// Maps task-name strings to their [`ScheduledTask`] handlers.
///
/// The registry is open: any number of tasks can be registered under any name.
/// This crate has no knowledge of what the handlers do — that is the concern of
/// the crate that registers them.
///
/// # Example
///
/// ```rust,ignore
/// use zart_scheduler::{TaskRegistry, ScheduledTask};
///
/// let mut registry = TaskRegistry::new();
/// registry.register("__zart__", ZartTask::new(storage, durable_registry));
/// registry.register("cleanup-job", CleanupJob);
/// ```
pub struct TaskRegistry {
    handlers: HashMap<String, Arc<dyn ScheduledTask>>,
}

impl TaskRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            handlers: HashMap::new(),
        }
    }

    /// Register a handler under `name`.
    ///
    /// The `name` must match the `task_name` stored in `zart_tasks` rows.
    pub fn register(&mut self, name: impl Into<String>, task: impl ScheduledTask + 'static) {
        self.handlers.insert(name.into(), Arc::new(task));
    }

    /// Look up a handler by task name.
    pub fn get(&self, name: &str) -> Option<Arc<dyn ScheduledTask>> {
        self.handlers.get(name).cloned()
    }

    /// Returns the names of all registered handlers (for diagnostics).
    pub fn handler_names(&self) -> Vec<&str> {
        self.handlers.keys().map(|s| s.as_str()).collect()
    }

    /// Returns the number of registered handlers.
    pub fn len(&self) -> usize {
        self.handlers.len()
    }

    /// Returns `true` if no handlers have been registered.
    pub fn is_empty(&self) -> bool {
        self.handlers.is_empty()
    }
}

impl Default for TaskRegistry {
    fn default() -> Self {
        Self::new()
    }
}
