//! Private [`CompletionHandler`] implementations for [`crate::task::ZartTask`].

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::sync::Arc;
use zart_core::types::{
    CompleteWaitGroupChildParams, FailWaitGroupChildParams, WriteStepCompletionParams,
};
use zart_scheduler::{CompletionHandler, ExecutionOps, Recurrence, ScheduleAtParams, StorageError};

use crate::store::StorageBackend;

pub(crate) struct ZartStepCompletion {
    pub storage: Arc<dyn StorageBackend>,
    pub tx: sqlx::Transaction<'static, sqlx::Postgres>,
    pub step_params: WriteStepCompletionParams,
    pub next_body: ScheduleAtParams,
}

#[async_trait]
impl CompletionHandler for ZartStepCompletion {
    async fn complete(
        mut self: Box<Self>,
        ops: ExecutionOps,
        _recurrence: Option<&Recurrence>,
        _execution_time: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        self.storage
            .write_step_completion_in_tx(&mut self.tx, self.step_params)
            .await?;
        ops.complete_in_tx(self.tx, None, vec![self.next_body])
            .await
    }
}

pub(crate) struct ZartWaitGroupChildCompletion {
    pub storage: Arc<dyn StorageBackend>,
    pub tx: sqlx::Transaction<'static, sqlx::Postgres>,
    pub step_params: WriteStepCompletionParams,
    pub params: CompleteWaitGroupChildParams,
}

#[async_trait]
impl CompletionHandler for ZartWaitGroupChildCompletion {
    async fn complete(
        mut self: Box<Self>,
        ops: ExecutionOps,
        _recurrence: Option<&Recurrence>,
        _execution_time: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        self.storage
            .write_step_completion_in_tx(&mut self.tx, self.step_params)
            .await?;
        self.storage
            .complete_wait_group_child_in_tx(&mut self.tx, self.params)
            .await?;
        ops.complete_in_tx(self.tx, None, vec![]).await
    }
}

pub(crate) struct ZartWaitGroupFailureCompletion {
    pub storage: Arc<dyn StorageBackend>,
    pub tx: sqlx::Transaction<'static, sqlx::Postgres>,
    pub step_params: WriteStepCompletionParams,
    pub params: FailWaitGroupChildParams,
    #[allow(dead_code)]
    pub execution_id: String,
}

#[async_trait]
impl CompletionHandler for ZartWaitGroupFailureCompletion {
    async fn complete(
        mut self: Box<Self>,
        ops: ExecutionOps,
        _recurrence: Option<&Recurrence>,
        _execution_time: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        self.storage
            .write_step_completion_in_tx(&mut self.tx, self.step_params)
            .await?;
        let _was_first = self
            .storage
            .fail_wait_group_child_in_tx(&mut self.tx, self.params)
            .await?;
        ops.complete_in_tx(self.tx, None, vec![]).await
    }
}
