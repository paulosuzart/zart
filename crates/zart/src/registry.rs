//! Task handler registration — maps task names to their concrete handlers.

use crate::context::TaskContext;
use crate::error::{ExecutionFailure, TaskError};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;

/// The top-level workflow definition for a durable execution.
///
/// Implement this trait to define a multi-step workflow. The framework
/// persists every step result to the database. When a worker restarts,
/// `run()` is called again but completed steps short-circuit — their cached
/// output is returned instantly rather than re-executing the step body.
///
/// # Re-entry contract
///
/// `run()` **will be called more than once** for the same execution:
///
/// - after each step schedules the next one, the body task exits and a new
///   body task is enqueued;
/// - on worker restart, incomplete executions resume from the last committed
///   step.
///
/// As a result, code in `run()` that sits *outside* a `zart::require()` /
/// `zart::step()` call will execute on every re-entry. Keep such code
/// side-effect free or guard it with a step.
///
/// # Associated types
///
/// - [`Data`](DurableExecution::Data) — the input payload. Deserialized from
///   JSON when the execution is picked up; must be [`serde::Deserialize`].
/// - [`Output`](DurableExecution::Output) — the final result written to
///   `zart_executions.result` on success; must be [`serde::Serialize`].
///
/// # Failure handling
///
/// When `run()` returns `Err`, the default behaviour marks the execution as
/// failed. Override [`on_failure`](DurableExecution::on_failure) to intercept
/// failures and produce a synthetic success result (e.g. to emit a
/// compensating event before completing).
///
/// # Example
///
/// ```rust,ignore
/// use zart::prelude::*;
///
/// struct FulfillOrder;
///
/// #[async_trait::async_trait]
/// impl DurableExecution for FulfillOrder {
///     type Data   = OrderId;
///     type Output = TrackingNumber;
///
///     async fn run(&self, order_id: OrderId) -> Result<TrackingNumber, TaskError> {
///         // Each step is executed once and its result cached.
///         // On re-entry the cached value is returned without calling the step body.
///         let _reserved = zart::require(ReserveInventory { order_id }).await?;
///         let _charged   = zart::require(ChargePayment    { order_id }).await?;
///         let tracking   = zart::require(ShipOrder        { order_id }).await?;
///         Ok(tracking)
///     }
///
///     async fn on_failure(
///         &self,
///         order_id: OrderId,
///         failure: ExecutionFailure,
///     ) -> Result<TrackingNumber, TaskError> {
///         // Emit a compensating event before marking the execution failed.
///         notify_ops_team(order_id, &failure).await;
///         Err(TaskError::Cancelled)
///     }
/// }
/// ```
#[async_trait]
pub trait DurableExecution: Send + Sync + 'static {
    /// The deserialized input type this task expects.
    type Data: serde::Serialize + serde::de::DeserializeOwned + Send + Sync;
    /// The serialized output type this task produces.
    type Output: serde::Serialize + serde::de::DeserializeOwned + Send + Sync;

    /// Execute the task.
    ///
    /// `data` is the deserialized input payload provided when the execution was started.
    /// Task context is accessed via `zart::context()` and `zart::*` free functions.
    async fn run(&self, data: Self::Data) -> Result<Self::Output, TaskError>;

    /// Maximum number of times the entire task (not individual steps) is retried
    /// before being marked as `dead`. Defaults to `0` (no retries).
    fn max_retries(&self) -> usize {
        0
    }

    /// Optional wall-clock timeout for the entire task execution.
    fn timeout(&self) -> Option<std::time::Duration> {
        None
    }

    /// Called when `run` returns `Err`, or when an execution-level failure occurs
    /// (deadline exceeded, execution retries exhausted) before `run` is invoked.
    ///
    /// Return `Ok(output)` to complete the execution gracefully with a synthetic result.
    /// Return `Err(...)` to fail the execution (default).
    ///
    /// The default implementation returns `Err(TaskError::Cancelled)` — i.e., no recovery.
    ///
    /// # Note on available data
    ///
    /// `on_failure` receives `&self` (handler config) and `data: Self::Data` (the original
    /// execution input payload). It does not receive mid-execution state — local variables
    /// computed during `run()` are gone by the time `on_failure` is called. This is deliberate:
    /// `on_failure` is a recovery function operating on the execution's stable input, not a
    /// continuation of an interrupted computation. If recovery requires mid-execution state,
    /// it must be modelled as a step whose result is durable.
    async fn on_failure(
        &self,
        _data: Self::Data,
        _failure: ExecutionFailure,
    ) -> Result<Self::Output, TaskError> {
        Err(TaskError::Cancelled)
    }
}

/// Type-erased internal trait used by [`TaskRegistry`] to dispatch to concrete handlers.
#[async_trait]
#[allow(dead_code)]
pub(crate) trait RegisteredTask: Send + Sync {
    /// Execute the task with raw JSON data, returning a raw JSON result.
    async fn execute(
        &self,
        ctx: Arc<TaskContext>,
        raw_data: serde_json::Value,
    ) -> Result<serde_json::Value, TaskError>;

    /// Call the handler's `on_failure` with raw JSON data and failure info.
    async fn on_failure(
        &self,
        raw_data: serde_json::Value,
        failure: ExecutionFailure,
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
        ctx: Arc<TaskContext>,
        raw_data: serde_json::Value,
    ) -> Result<serde_json::Value, TaskError> {
        let data: T::Data = serde_json::from_value(raw_data.clone()).map_err(|e| {
            TaskError::HandlerPanic(format!("failed to deserialize task data: {e}"))
        })?;

        let output = crate::local::ZART_CTX
            .scope(ctx, async move {
                crate::local::ZART_PHASE
                    .scope(
                        crate::local::Phase::Body,
                        async move { self.0.run(data).await },
                    )
                    .await
            })
            .await?;

        serde_json::to_value(output)
            .map_err(|e| TaskError::HandlerPanic(format!("failed to serialize task output: {e}")))
    }

    async fn on_failure(
        &self,
        raw_data: serde_json::Value,
        failure: ExecutionFailure,
    ) -> Result<serde_json::Value, TaskError> {
        let data: T::Data = serde_json::from_value(raw_data).map_err(|e| {
            TaskError::HandlerPanic(format!("failed to deserialize task data: {e}"))
        })?;

        let output = self.0.on_failure(data, failure).await?;
        serde_json::to_value(output).map_err(|e| {
            TaskError::HandlerPanic(format!("failed to serialize on_failure output: {e}"))
        })
    }

    fn max_retries(&self) -> usize {
        self.0.max_retries()
    }

    fn timeout(&self) -> Option<std::time::Duration> {
        self.0.timeout()
    }
}

/// Maps task-name strings to their [`DurableExecution`] handlers.
///
/// The registry is built once at application startup, then wrapped in an
/// [`Arc`] and shared across all workers. It cannot be modified after that
/// point — there is no hot-reload mechanism.
///
/// The **task name** is the string key used both when registering a handler
/// here and when starting an execution via
/// [`DurableScheduler::start`](crate::durable::DurableScheduler::start) /
/// [`DurableScheduler::start_for`](crate::durable::DurableScheduler::start_for).
/// The names must match exactly.
///
/// # Example
///
/// ```rust,ignore
/// use std::sync::Arc;
/// use zart::{TaskRegistry, Worker, WorkerConfig};
///
/// let mut registry = TaskRegistry::new();
/// registry.register("fulfill-order",  FulfillOrder);
/// registry.register("onboard-user",   OnboardUser);
/// registry.register("send-invoice",   SendInvoice);
///
/// // Wrap once; clone the Arc for each worker.
/// let registry = Arc::new(registry);
///
/// let worker = Worker::new(scheduler.clone(), Arc::clone(&registry), WorkerConfig::default());
/// ```
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
    pub(crate) fn get_handler(&self, task_name: &str) -> Option<&dyn RegisteredTask> {
        self.handlers.get(task_name).map(|h| h.as_ref())
    }

    /// Returns the names of all registered handlers (for diagnostics).
    pub(crate) fn handler_names(&self) -> Vec<&str> {
        self.handlers.keys().map(|s| s.as_str()).collect()
    }

    /// Execute a registered handler with the given raw JSON data.
    ///
    /// This sets up task-local scoping and delegates to the internal adapter.
    /// Useful for testing without running a full worker.
    pub async fn execute_handler(
        &self,
        task_name: &str,
        ctx: Arc<TaskContext>,
        raw_data: serde_json::Value,
    ) -> Result<serde_json::Value, TaskError> {
        let handler = self
            .handlers
            .get(task_name)
            .ok_or_else(|| TaskError::HandlerPanic(format!("unknown task: {task_name}")))?;
        handler.execute(ctx, raw_data).await
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

        async fn run(&self, data: Self::Data) -> Result<Self::Output, TaskError> {
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
