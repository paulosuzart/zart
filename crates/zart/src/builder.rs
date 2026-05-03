use crate::TASK_NAME;
use crate::durable::DurableScheduler;
use crate::recurring::{OverlapPolicy, RecurringDurableTask};
use crate::registry::{DurableExecution, DurableRegistry};
use crate::store::{Backend, StorageBackend};
use crate::task::ZartTask;
use std::marker::PhantomData;
use std::sync::Arc;
use zart_scheduler::{
    Recurrence, ScheduleAtParams, ScheduleResult, ScheduledTask, TaskRegistry as SchedulerRegistry,
    TaskScheduler, Worker, WorkerConfig,
};

/// Builder for the Zart worker.
///
/// This builder handles the wiring of Zart's durable execution logic into
/// the generic `zart-scheduler` worker. It supports two registry types:
///
/// - **Durable registry** — maps task names to [`DurableExecution`] handlers.
///   Populated via [`durable_registry()`](Self::durable_registry) or the fluent
///   [`register_durable_task()`](Self::register_durable_task).
///
/// - **Scheduler registry** — maps task names to [`ScheduledTask`] handlers at
///   the scheduler level. Populated via [`scheduler_registry()`](Self::scheduler_registry)
///   or the fluent [`register_scheduler_task()`](Self::register_scheduler_task).
///
/// If a durable registry is provided, the builder automatically creates a
/// [`ZartTask`] and registers it as `"__zart__"` in the scheduler registry.
pub struct WorkerBuilder {
    storage: Arc<dyn StorageBackend>,
    scheduler: Arc<dyn TaskScheduler>,
    scheduler_registry: Option<SchedulerRegistry>,
    durable_registry: Option<DurableRegistry>,
    config: WorkerConfig,
}

impl WorkerBuilder {
    pub fn new(storage: Arc<dyn StorageBackend>, scheduler: Arc<dyn TaskScheduler>) -> Self {
        Self {
            storage,
            scheduler,
            scheduler_registry: None,
            durable_registry: None,
            config: WorkerConfig::default(),
        }
    }

    /// Create a `WorkerBuilder` from any [`Backend`] implementation.
    ///
    /// This is the recommended production path.
    ///
    /// ```text
    /// let pg = PgBackend::new(pool);
    /// let worker = WorkerBuilder::from_backend(&pg)
    ///     .register_durable_task("my-task", MyHandler)
    ///     .build();
    /// ```
    pub fn from_backend(backend: &impl Backend) -> Self {
        Self::new(backend.storage(), backend.scheduler())
    }

    pub fn durable_registry(mut self, registry: DurableRegistry) -> Self {
        self.durable_registry = Some(registry);
        self
    }

    pub fn scheduler_registry(mut self, registry: SchedulerRegistry) -> Self {
        self.scheduler_registry = Some(registry);
        self
    }

    pub fn register_scheduler_task(
        mut self,
        name: &str,
        task: impl ScheduledTask + 'static,
    ) -> Self {
        let registry = self
            .scheduler_registry
            .get_or_insert_with(SchedulerRegistry::new);
        registry.register(name, task);
        self
    }

    pub fn register_durable_task<T: DurableExecution>(mut self, name: &str, handler: T) -> Self {
        let registry = self
            .durable_registry
            .get_or_insert_with(DurableRegistry::new);
        registry.register(name, handler);
        self
    }

    pub fn config(mut self, config: WorkerConfig) -> Self {
        self.config = config;
        self
    }

    /// Register a recurring durable execution.
    ///
    /// On every occurrence the handler `H` is started as a new durable execution.
    /// The execution ID is built by replacing `{occurrence}` in `id_template` with
    /// the current occurrence counter (0-based). The counter is stored in the
    /// recurring task's `metadata["occurrence"]` field and incremented each fire.
    ///
    /// `initial_data` is the JSON payload forwarded to `H::run` on every start.
    ///
    /// The recurring task is registered under the internal name
    /// `"__zart_recurring__:{task_id}"` and scheduled idempotently on build.
    ///
    /// # Runtime requirement
    ///
    /// This method uses [`tokio::task::block_in_place`] internally to schedule
    /// the recurring task synchronously during builder construction. It therefore
    /// **requires a multi-threaded Tokio runtime** — calling it from a
    /// `current_thread` runtime will panic.
    ///
    /// # Panics
    ///
    /// - Panics if `id_template` does not contain the `{occurrence}` placeholder.
    /// - Panics if scheduling the initial task fails. Use
    ///   [`register_scheduler_task`](Self::register_scheduler_task) plus manual
    ///   scheduling if you need finer error control.
    pub fn register_recurring_durable<H: DurableExecution + 'static>(
        mut self,
        task_id: &str,
        id_template: &str,
        recurrence: Recurrence,
        overlap: OverlapPolicy,
        initial_data: serde_json::Value,
    ) -> Self {
        assert!(
            id_template.contains("{occurrence}"),
            "id_template must contain {{occurrence}} placeholder, got: {id_template:?}"
        );

        let durable_scheduler = Arc::new(DurableScheduler::new(
            self.storage.clone(),
            self.scheduler.clone(),
        ));

        let handler = RecurringDurableTask::<H> {
            handler_name: task_id.to_string(),
            id_template: id_template.to_string(),
            overlap,
            scheduler: durable_scheduler,
            _marker: PhantomData,
        };

        let internal_name = format!("__zart_recurring__:{task_id}");
        let registry = self
            .scheduler_registry
            .get_or_insert_with(SchedulerRegistry::new);
        registry.register(&internal_name, handler);

        // Schedule the recurring task idempotently (ON CONFLICT DO NOTHING).
        let scheduler = self.scheduler.clone();
        let params = ScheduleAtParams {
            task_id: internal_name.clone(),
            task_name: internal_name,
            execution_time: chrono::Utc::now(),
            data: initial_data,
            recurrence: Some(recurrence),
            metadata: serde_json::json!({ "occurrence": 0 }),
        };
        // Block on the async schedule call using a one-shot runtime.
        let _: ScheduleResult = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(async move { scheduler.schedule_at(params).await })
        })
        .expect("failed to schedule recurring durable task");

        self
    }

    pub fn build(self) -> Worker {
        let mut scheduler_registry = self.scheduler_registry.unwrap_or_default();

        if scheduler_registry.get(TASK_NAME).is_some() {
            panic!(
                "'{}' is reserved for Zart internal use and cannot be registered by users",
                TASK_NAME
            );
        }

        if let Some(durable_registry) = self.durable_registry {
            let durable_registry = Arc::new(durable_registry);
            let zart_task = ZartTask::new(
                self.storage.clone(),
                self.scheduler.clone(),
                durable_registry,
            );
            scheduler_registry.register(TASK_NAME, zart_task);
        }

        Worker::new(self.scheduler, Arc::new(scheduler_registry), self.config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::RecordingScheduler;
    use crate::{DurableExecution, TaskError};
    use async_trait::async_trait;
    use serde_json;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use zart_scheduler::completion::OnComplete;
    use zart_scheduler::task::{CompletionHandler, SchedulerTaskError, TaskInstance};

    struct EchoDurable;

    #[async_trait]
    impl DurableExecution for EchoDurable {
        type Data = serde_json::Value;
        type Output = serde_json::Value;

        async fn run(&self, data: Self::Data) -> Result<Self::Output, TaskError> {
            Ok(data)
        }
    }

    struct MockScheduledTask {
        execute_count: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl ScheduledTask for MockScheduledTask {
        async fn execute(
            &self,
            _instance: &TaskInstance,
        ) -> Result<Box<dyn CompletionHandler>, SchedulerTaskError> {
            self.execute_count.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(OnComplete {
                result: None,
                schedule_next: vec![],
            }))
        }
    }

    fn mock_backend() -> Arc<dyn StorageBackend> {
        let (scheduler, _) = RecordingScheduler::builder().build();
        scheduler
    }

    fn mock_task_scheduler() -> Arc<dyn TaskScheduler> {
        let (scheduler, _) = RecordingScheduler::builder().build();
        scheduler
    }

    #[test]
    #[should_panic(expected = "__zart__")]
    fn build_panics_if_zart_task_already_registered() {
        let mut scheduler_registry = SchedulerRegistry::new();
        let mock = MockScheduledTask {
            execute_count: Arc::new(AtomicUsize::new(0)),
        };
        scheduler_registry.register(TASK_NAME, mock);

        let _worker = WorkerBuilder::new(mock_backend(), mock_task_scheduler())
            .scheduler_registry(scheduler_registry)
            .build();
    }

    #[test]
    fn build_succeeds_with_no_registries() {
        let _worker = WorkerBuilder::new(mock_backend(), mock_task_scheduler()).build();
    }

    #[test]
    fn build_succeeds_with_pre_built_scheduler_registry() {
        let mut scheduler_registry = SchedulerRegistry::new();
        let mock = MockScheduledTask {
            execute_count: Arc::new(AtomicUsize::new(0)),
        };
        scheduler_registry.register("custom-task", mock);

        let _worker = WorkerBuilder::new(mock_backend(), mock_task_scheduler())
            .scheduler_registry(scheduler_registry)
            .build();
    }

    #[test]
    fn register_durable_task_creates_registry() {
        let _worker = WorkerBuilder::new(mock_backend(), mock_task_scheduler())
            .register_durable_task("echo", EchoDurable)
            .build();
    }

    #[test]
    fn register_scheduler_task_creates_registry() {
        let mock = MockScheduledTask {
            execute_count: Arc::new(AtomicUsize::new(0)),
        };
        let _worker = WorkerBuilder::new(mock_backend(), mock_task_scheduler())
            .register_scheduler_task("cleanup", mock)
            .build();
    }

    #[test]
    fn build_with_both_registries() {
        let mut scheduler_registry = SchedulerRegistry::new();
        let mock = MockScheduledTask {
            execute_count: Arc::new(AtomicUsize::new(0)),
        };
        scheduler_registry.register("custom-task", mock);

        let _worker = WorkerBuilder::new(mock_backend(), mock_task_scheduler())
            .scheduler_registry(scheduler_registry)
            .register_durable_task("echo", EchoDurable)
            .build();
    }

    #[test]
    fn build_without_durable_registry_no_zart_task() {
        let mock = MockScheduledTask {
            execute_count: Arc::new(AtomicUsize::new(0)),
        };
        let _worker = WorkerBuilder::new(mock_backend(), mock_task_scheduler())
            .register_scheduler_task("cleanup", mock)
            .build();
    }

    #[test]
    fn fluent_multiple_durable_tasks() {
        let _worker = WorkerBuilder::new(mock_backend(), mock_task_scheduler())
            .register_durable_task("echo-1", EchoDurable)
            .register_durable_task("echo-2", EchoDurable)
            .build();
    }

    #[test]
    fn fluent_mixed_registrations() {
        let mock = MockScheduledTask {
            execute_count: Arc::new(AtomicUsize::new(0)),
        };
        let _worker = WorkerBuilder::new(mock_backend(), mock_task_scheduler())
            .register_durable_task("echo", EchoDurable)
            .register_scheduler_task("cleanup", mock)
            .build();
    }

    #[test]
    fn config_method_works() {
        let config = WorkerConfig {
            poll_interval: std::time::Duration::from_millis(200),
            max_tasks_per_poll: 5,
            max_concurrent_tasks: 2,
            shutdown_timeout: std::time::Duration::from_secs(10),
            orphan_timeout: std::time::Duration::from_secs(30),
            ..Default::default()
        };
        let _worker = WorkerBuilder::new(mock_backend(), mock_task_scheduler())
            .register_durable_task("echo", EchoDurable)
            .config(config)
            .build();
    }

    #[test]
    fn pre_built_durable_registry() {
        let mut durable_registry = DurableRegistry::new();
        durable_registry.register("echo", EchoDurable);

        let _worker = WorkerBuilder::new(mock_backend(), mock_task_scheduler())
            .durable_registry(durable_registry)
            .build();
    }
}
