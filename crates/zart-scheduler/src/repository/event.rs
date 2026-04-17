//! Repository trait for event delivery and execution statistics.
//!
//! Event delivery touches `zart_steps` and `zart_tasks` atomically.
//! Execution statistics is a read-only aggregate over `zart_executions`
//! and `zart_execution_runs` — grouped here as a reporting/observability query.

use crate::{EventDeliveryResult, ExecutionStats, StorageError};

/// Internal repository for event delivery and execution statistics.
/// Not part of the public API — used to modularize the `DurableStorage` impl.
pub(crate) trait EventRepository: Sized {
    async fn deliver_event(
        &self,
        execution_id: &str,
        event_name: &str,
        payload: serde_json::Value,
    ) -> Result<EventDeliveryResult, StorageError>;

    async fn execution_stats(&self) -> Result<ExecutionStats, StorageError>;
}
