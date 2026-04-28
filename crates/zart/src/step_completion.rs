//! Private [`CompletionHandler`] implementations for [`crate::task::ZartTask`].

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use zart_scheduler::{CompletionHandler, ExecutionOps, Recurrence, ScheduleAtParams, StorageError};

/// Completion handler for Path 1 (regular step execution).
///
/// The step SQL is already written into `tx`; `complete` appends scheduler
/// bookkeeping (mark step task complete + schedule body continuation) and
/// commits, making everything atomic in one round-trip.
pub(crate) struct ZartStepCompletion {
    pub tx: sqlx::Transaction<'static, sqlx::Postgres>,
    pub next_body: ScheduleAtParams,
}

#[async_trait]
impl CompletionHandler for ZartStepCompletion {
    async fn complete(
        self: Box<Self>,
        ops: ExecutionOps,
        _recurrence: Option<&Recurrence>,
        _execution_time: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        ops.complete_in_tx(self.tx, None, vec![self.next_body])
            .await
    }
}
