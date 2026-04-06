//! [`DurableApi`] — an object-safe trait that wraps [`DurableScheduler`].
//!
//! The HTTP layer (`zart-api`) and other consumers that need dynamic dispatch
//! hold `Arc<dyn DurableApi>` instead of the generic `DurableScheduler<S>`.
//! This removes the `S: Scheduler` type parameter from call-sites that don't
//! care about the storage backend.

use crate::durable::DurableScheduler;
use crate::error::SchedulerError;
use async_trait::async_trait;
use scheduler::{ExecutionRecord, ExecutionStatus, ScheduleResult};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;

/// Object-safe interface for durable execution management.
///
/// Implemented by [`DurableScheduler<S>`] for any `S: Scheduler`.
/// Consumers that don't know (or care about) the concrete scheduler backend
/// can depend on `Arc<dyn DurableApi>` instead.
#[async_trait]
pub trait DurableApi: Send + Sync {
    /// Start a new durable execution with a raw JSON payload.
    async fn start(
        &self,
        execution_id: &str,
        task_name: &str,
        data: Value,
    ) -> Result<ScheduleResult, SchedulerError>;

    /// Cancel a running or scheduled durable execution.
    async fn cancel(&self, execution_id: &str) -> Result<bool, SchedulerError>;

    /// Return the current status of a durable execution.
    async fn status(&self, execution_id: &str) -> Result<ExecutionRecord, SchedulerError>;

    /// Block until the execution reaches a terminal state.
    async fn wait(
        &self,
        execution_id: &str,
        timeout: Duration,
        poll_interval: Option<Duration>,
    ) -> Result<ExecutionRecord, SchedulerError>;

    /// Deliver an external event to a waiting execution.
    async fn offer_event(
        &self,
        execution_id: &str,
        event_name: &str,
        payload: Value,
    ) -> Result<(), SchedulerError>;

    /// List durable execution records with optional filters.
    async fn list_executions(
        &self,
        status: Option<ExecutionStatus>,
        task_name: Option<String>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<ExecutionRecord>, SchedulerError>;

    /// Check if the API is ready to serve requests.
    /// Default implementation always returns `true`.
    fn is_ready(&self) -> bool {
        true
    }
}

#[async_trait]
impl DurableApi for DurableScheduler {
    async fn start(
        &self,
        execution_id: &str,
        task_name: &str,
        data: Value,
    ) -> Result<ScheduleResult, SchedulerError> {
        DurableScheduler::start(self, execution_id, task_name, data).await
    }

    async fn cancel(&self, execution_id: &str) -> Result<bool, SchedulerError> {
        DurableScheduler::cancel(self, execution_id).await
    }

    async fn status(&self, execution_id: &str) -> Result<ExecutionRecord, SchedulerError> {
        DurableScheduler::status(self, execution_id).await
    }

    async fn wait(
        &self,
        execution_id: &str,
        timeout: Duration,
        poll_interval: Option<Duration>,
    ) -> Result<ExecutionRecord, SchedulerError> {
        DurableScheduler::wait(self, execution_id, timeout, poll_interval).await
    }

    async fn offer_event(
        &self,
        execution_id: &str,
        event_name: &str,
        payload: Value,
    ) -> Result<(), SchedulerError> {
        DurableScheduler::offer_event(self, execution_id, event_name, payload).await
    }

    async fn list_executions(
        &self,
        status: Option<ExecutionStatus>,
        task_name: Option<String>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<ExecutionRecord>, SchedulerError> {
        DurableScheduler::list_executions(self, status, task_name, limit, offset).await
    }
}

/// Convenience constructor — wrap a `DurableScheduler` into an `Arc<dyn DurableApi>`.
pub fn into_durable_api(scheduler: DurableScheduler) -> Arc<dyn DurableApi> {
    Arc::new(scheduler)
}
