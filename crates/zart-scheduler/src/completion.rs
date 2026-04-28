//! Standard [`CompletionHandler`](crate::CompletionHandler) implementations.
//!
//! These cover the common completion cases for external [`ScheduledTask`]
//! (crate::ScheduledTask) implementations. `ZartTask` uses its own private
//! handlers defined in the `zart` crate.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::Postgres;

use crate::ops::ExecutionOps;
use crate::recurrence::Recurrence;
use crate::{CompletionHandler, ScheduleAtParams};

/// Mark the task complete.
///
/// Opens a fresh transaction. Handles recurring tasks automatically:
/// if `recurrence` is set and no `result`/`schedule_next` is provided,
/// reschedules instead of completing.
pub struct OnComplete {
    pub result: Option<Value>,
    pub schedule_next: Vec<ScheduleAtParams>,
}

impl OnComplete {
    /// Convenience: complete with no result, no chaining.
    pub fn done() -> Box<dyn CompletionHandler> {
        Box::new(Self {
            result: None,
            schedule_next: vec![],
        })
    }
}

#[async_trait]
impl CompletionHandler for OnComplete {
    async fn complete(
        self: Box<Self>,
        ops: ExecutionOps,
        recurrence: Option<&Recurrence>,
        execution_time: DateTime<Utc>,
    ) -> Result<(), crate::error::StorageError> {
        // Automatic recurring behaviour: if recurrence is set and the caller
        // hasn't provided an explicit result or follow-up, reschedule.
        if recurrence.is_some()
            && self.result.is_none()
            && self.schedule_next.is_empty()
            && let Some(next_time) = recurrence
                .as_ref()
                .and_then(|r| r.next_after(execution_time))
        {
            return ops.reschedule(next_time).await;
        }
        ops.complete(self.result, self.schedule_next).await
    }
}

/// Reschedule the task to an explicit future time.
///
/// Opens a fresh transaction.
pub struct OnReschedule {
    pub at: DateTime<Utc>,
}

#[async_trait]
impl CompletionHandler for OnReschedule {
    async fn complete(
        self: Box<Self>,
        ops: ExecutionOps,
        _recurrence: Option<&Recurrence>,
        _execution_time: DateTime<Utc>,
    ) -> Result<(), crate::error::StorageError> {
        ops.reschedule(self.at).await
    }
}

/// Use an existing open transaction for all bookkeeping.
///
/// Accepts an open transaction, calls `ops.complete_in_tx(tx, ...)` which
/// appends `mark_completed_in_tx` and any `schedule_at_in_tx` entries then
/// commits.
pub struct WithTransaction {
    pub tx: sqlx::Transaction<'static, Postgres>,
    pub result: Option<Value>,
    pub schedule_next: Vec<ScheduleAtParams>,
}

#[async_trait]
impl CompletionHandler for WithTransaction {
    async fn complete(
        self: Box<Self>,
        ops: ExecutionOps,
        _recurrence: Option<&Recurrence>,
        _execution_time: DateTime<Utc>,
    ) -> Result<(), crate::error::StorageError> {
        ops.complete_in_tx(self.tx, self.result, self.schedule_next)
            .await
    }
}
