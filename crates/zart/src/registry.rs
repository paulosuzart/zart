//! Task handler registration — maps task names to their concrete handlers.

use crate::context::TaskContext;
use crate::error::TaskError;
use async_trait::async_trait;
use scheduler::Scheduler;
use std::collections::HashMap;

/// A user-defined task handler.
///
/// Implement this trait to define durable work. The framework calls [`run`](TaskHandler::run)
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
/// impl TaskHandler for GreetTask {
///     type Data = String;
///     type Output = String;
///
///     async fn run<S: Scheduler>(
///         &self,
///         _ctx: &mut TaskContext<S>,
///         data: Self::Data,
///     ) -> Result<Self::Output, TaskError> {
///         Ok(format!("Hello, {}!", data))
///     }
/// }
/// ```
#[async_trait]
pub trait TaskHandler: Send + Sync + 'static {
    /// The deserialized input type this task expects.
    type Data: serde::de::DeserializeOwned + Send + Sync;
    /// The serialized output type this task produces.
    type Output: serde::Serialize + Send + Sync;

    /// Execute the task.
    ///
    /// The `ctx` provides the step API and access to the execution state.
    /// `data` is the deserialized input payload provided when the execution was started.
    async fn run<S: Scheduler>(
        &self,
        ctx: &mut TaskContext<S>,
        data: Self::Data,
    ) -> Result<Self::Output, TaskError>;

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

/// Type-erased internal trait used by [`TaskRegistry<S>`] to dispatch to concrete handlers.
///
/// `S` is fixed at the registry level so that `execute` is a concrete method
/// (not a generic one) — which allows it to be used as a trait object.
#[async_trait]
pub trait RegisteredTask<S: Scheduler>: Send + Sync {
    /// Execute the task with raw JSON data, returning a raw JSON result.
    async fn execute(
        &self,
        ctx: &mut TaskContext<S>,
        raw_data: serde_json::Value,
    ) -> Result<serde_json::Value, TaskError>;

    fn max_retries(&self) -> usize;
    fn timeout(&self) -> Option<std::time::Duration>;
}

/// Adapts a concrete [`TaskHandler`] into a type-erased [`RegisteredTask<S>`].
struct TaskHandlerAdapter<T: TaskHandler>(T);

#[async_trait]
impl<T, S> RegisteredTask<S> for TaskHandlerAdapter<T>
where
    T: TaskHandler,
    S: Scheduler + Send + Sync + 'static,
{
    async fn execute(
        &self,
        ctx: &mut TaskContext<S>,
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
/// The registry is generic over the scheduler type `S` so that all handlers
/// share the same backend, enabling type-erased dispatch via `Box<dyn RegisteredTask<S>>`.
///
/// The registry is built once at startup and shared (via [`Arc`]) across all workers.
pub struct TaskRegistry<S: Scheduler> {
    handlers: HashMap<String, Box<dyn RegisteredTask<S>>>,
}

impl<S: Scheduler + Send + Sync + 'static> TaskRegistry<S> {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            handlers: HashMap::new(),
        }
    }

    /// Register a task handler under the given `task_name`.
    ///
    /// The `task_name` must match the name used when scheduling the task.
    pub fn register<T: TaskHandler>(&mut self, task_name: &str, handler: T) {
        self.handlers
            .insert(task_name.to_string(), Box::new(TaskHandlerAdapter(handler)));
    }

    /// Look up a registered handler by task name.
    pub fn get_handler(&self, task_name: &str) -> Option<&dyn RegisteredTask<S>> {
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

impl<S: Scheduler + Send + Sync + 'static> Default for TaskRegistry<S> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use scheduler::{FetchedTask, Recurrence, ScheduleResult, StorageError};

    /// Minimal no-op scheduler for unit tests.
    struct NoopScheduler;

    #[async_trait]
    impl Scheduler for NoopScheduler {
        async fn schedule_now(
            &self,
            task_id: &str,
            _task_name: &str,
            _data: serde_json::Value,
            _execution_id: Option<&str>,
        ) -> Result<ScheduleResult, StorageError> {
            Ok(ScheduleResult {
                task_id: task_id.to_string(),
                execution_time: chrono::Utc::now(),
            })
        }

        async fn schedule_at(
            &self,
            task_id: &str,
            _task_name: &str,
            execution_time: chrono::DateTime<chrono::Utc>,
            _data: serde_json::Value,
            _recurrence: Option<Recurrence>,
            _execution_id: Option<&str>,
        ) -> Result<ScheduleResult, StorageError> {
            Ok(ScheduleResult {
                task_id: task_id.to_string(),
                execution_time,
            })
        }

        async fn poll_due(
            &self,
            _now: chrono::DateTime<chrono::Utc>,
            _limit: usize,
        ) -> Result<Vec<FetchedTask>, StorageError> {
            Ok(vec![])
        }

        async fn update_task_state(
            &self,
            _task_id: &str,
            _state: serde_json::Value,
            _next_execution_time: chrono::DateTime<chrono::Utc>,
            _lock_token: &str,
        ) -> Result<(), StorageError> {
            Ok(())
        }

        async fn mark_completed(
            &self,
            _task_id: &str,
            _result: Option<serde_json::Value>,
            _lock_token: &str,
        ) -> Result<(), StorageError> {
            Ok(())
        }

        async fn mark_failed(
            &self,
            _task_id: &str,
            _error: &str,
            _next_execution_time: Option<chrono::DateTime<chrono::Utc>>,
            _lock_token: &str,
        ) -> Result<(), StorageError> {
            Ok(())
        }

        async fn cancel_task(&self, _task_id: &str) -> Result<bool, StorageError> {
            Ok(true)
        }

        async fn delete_task(&self, _task_id: &str) -> Result<(), StorageError> {
            Ok(())
        }

        async fn run_migrations(&self) -> Result<(), StorageError> {
            Ok(())
        }
    }

    struct EchoTask;

    #[async_trait]
    impl TaskHandler for EchoTask {
        type Data = serde_json::Value;
        type Output = serde_json::Value;

        async fn run<S: Scheduler>(
            &self,
            _ctx: &mut TaskContext<S>,
            data: Self::Data,
        ) -> Result<Self::Output, TaskError> {
            Ok(data)
        }
    }

    #[test]
    fn register_and_lookup() {
        let mut registry: TaskRegistry<NoopScheduler> = TaskRegistry::new();
        registry.register("echo", EchoTask);
        assert!(registry.get_handler("echo").is_some());
        assert!(registry.get_handler("missing").is_none());
    }

    #[test]
    fn registry_len() {
        let mut registry: TaskRegistry<NoopScheduler> = TaskRegistry::new();
        assert_eq!(registry.len(), 0);
        assert!(registry.is_empty());
        registry.register("echo", EchoTask);
        assert_eq!(registry.len(), 1);
        assert!(!registry.is_empty());
    }
}
