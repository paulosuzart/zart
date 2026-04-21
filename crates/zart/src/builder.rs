use crate::TASK_NAME;
use crate::registry::DurableRegistry;
use crate::store::StorageBackend;
use crate::task::ZartTask;
use std::sync::Arc;
use zart_scheduler::{TaskRegistry as SchedulerRegistry, TaskScheduler, Worker, WorkerConfig};

/// Builder for the Zart worker.
///
/// This builder handles the wiring of Zart's durable execution logic into
/// the generic `zart-scheduler` worker.
pub struct WorkerBuilder {
    storage: Arc<dyn StorageBackend>,
    scheduler: Arc<dyn TaskScheduler>,
    registry: Option<DurableRegistry>,
    config: WorkerConfig,
}

impl WorkerBuilder {
    pub fn new(storage: Arc<dyn StorageBackend>, scheduler: Arc<dyn TaskScheduler>) -> Self {
        Self {
            storage,
            scheduler,
            registry: None,
            config: WorkerConfig::default(),
        }
    }

    pub fn registry(mut self, registry: DurableRegistry) -> Self {
        self.registry = Some(registry);
        self
    }

    pub fn config(mut self, config: WorkerConfig) -> Self {
        self.config = config;
        self
    }

    pub fn build(self) -> Worker {
        let registry = self
            .registry
            .expect("DurableRegistry must be provided to WorkerBuilder");
        let durable_registry = Arc::new(registry);

        // Create the ZartTask handler that knows how to dispatch to durable executions
        let zart_task = ZartTask::new(self.storage.clone(), durable_registry);

        // Register ZartTask as the sole handler for the "__zart__" task name
        let mut scheduler_registry = SchedulerRegistry::new();
        scheduler_registry.register(TASK_NAME, zart_task);

        Worker::new(self.scheduler, Arc::new(scheduler_registry), self.config)
    }
}
