//! Task handler registration — maps task names to their concrete handlers.

use crate::context::TaskContext;
use crate::error::TaskError;
use async_trait::async_trait;
use std::collections::HashMap;

/// A user-defined durable execution handler.
///
/// Implement this trait to define durable work. The framework calls [`run`](DurableExecution::run)
/// whenever a task is picked up from the scheduler. Steps inside `run` drive the
/// durable execution through the control-flow error model.
///
/// # Example
///
/// ```rust,ignore
/// use zart::prelude::*;
/// use async_trait::async_trait;
///
/// struct GreetTask;
///
/// #[async_trait]
/// impl DurableExecution for GreetTask {
///     type Data = String;
///     type Output = String;
///
///     async fn run(
///         &self,
///         _ctx: &mut TaskContext,
///         data: Self::Data,
///     ) -> Result<Self::Output, TaskError> {
///         Ok(format!("Hello, {}!", data))
///     }
/// }
/// ```
#[async_trait]
pub trait DurableExecution: Send + Sync + 'static {
    /// The deserialized input type this task expects.
    type Data: serde::de::DeserializeOwned + Send + Sync;
    /// The serialized output type this task produces.
    type Output: serde::Serialize + Send + Sync;

    /// Execute the task.
    ///
    /// The `ctx` provides the step API and access to the execution state.
    /// `data` is the deserialized input payload provided when the execution was started.
    async fn run(&self, ctx: &mut TaskContext, data: Self::Data)
    -> Result<Self::Output, TaskError>;

    /// Maximum number of times the entire task (not individual steps) is retried
    /// before being marked as `dead`. Defaults to `0` (no retries).
    fn max_retries(&self) -> usize {
        0
    }

    /// Optional wall-clock timeout for the entire task execution.
    fn timeout(&self) -> Option<std::time::Duration> {
        None
    }
}

/// Type-erased internal trait used by [`TaskRegistry`] to dispatch to concrete handlers.
#[async_trait]
pub trait RegisteredTask: Send + Sync {
    /// Execute the task with raw JSON data, returning a raw JSON result.
    async fn execute(
        &self,
        ctx: &mut TaskContext,
        raw_data: serde_json::Value,
    ) -> Result<serde_json::Value, TaskError>;

    fn max_retries(&self) -> usize;
    fn timeout(&self) -> Option<std::time::Duration>;
}

/// Adapts a concrete [`DurableExecution`] into a type-erased [`RegisteredTask`].
struct DurableExecutionAdapter<T: DurableExecution>(T);

#[async_trait]
impl<T: DurableExecution> RegisteredTask for DurableExecutionAdapter<T> {
    async fn execute(
        &self,
        ctx: &mut TaskContext,
        raw_data: serde_json::Value,
    ) -> Result<serde_json::Value, TaskError> {
        let data: T::Data = serde_json::from_value(raw_data).map_err(|e| {
            TaskError::HandlerPanic(format!("failed to deserialize task data: {e}"))
        })?;

        let output = self.0.run(ctx, data).await?;

        serde_json::to_value(output)
            .map_err(|e| TaskError::HandlerPanic(format!("failed to serialize task output: {e}")))
    }

    fn max_retries(&self) -> usize {
        self.0.max_retries()
    }

    fn timeout(&self) -> Option<std::time::Duration> {
        self.0.timeout()
    }
}

/// A registry that maps task names to their concrete handlers.
///
/// Built once at startup and shared (via [`Arc`]) across all workers.
pub struct TaskRegistry {
    handlers: HashMap<String, Box<dyn RegisteredTask>>,
}

impl TaskRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            handlers: HashMap::new(),
        }
    }

    /// Register a durable execution handler under the given `task_name`.
    ///
    /// The `task_name` must match the name used when scheduling the task.
    pub fn register<T: DurableExecution>(&mut self, task_name: &str, handler: T) {
        self.handlers.insert(
            task_name.to_string(),
            Box::new(DurableExecutionAdapter(handler)),
        );
    }

    /// Look up a registered handler by task name.
    pub fn get_handler(&self, task_name: &str) -> Option<&dyn RegisteredTask> {
        self.handlers.get(task_name).map(|h| h.as_ref())
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

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;

    struct EchoTask;

    #[async_trait]
    impl DurableExecution for EchoTask {
        type Data = serde_json::Value;
        type Output = serde_json::Value;

        async fn run(
            &self,
            _ctx: &mut TaskContext,
            data: Self::Data,
        ) -> Result<Self::Output, TaskError> {
            Ok(data)
        }
    }

    #[test]
    fn register_and_lookup() {
        let mut registry = TaskRegistry::new();
        registry.register("echo", EchoTask);
        assert!(registry.get_handler("echo").is_some());
        assert!(registry.get_handler("missing").is_none());
    }

    #[test]
    fn registry_len() {
        let mut registry = TaskRegistry::new();
        assert_eq!(registry.len(), 0);
        assert!(registry.is_empty());
        registry.register("echo", EchoTask);
        assert_eq!(registry.len(), 1);
        assert!(!registry.is_empty());
    }
}
