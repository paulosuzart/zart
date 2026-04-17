//! Repository trait for wait-group coordination.
//!
//! Scoped to the wait-group columns of `zart_steps` and related task rows.
//! Raw data access only — no business logic.

use crate::{CompleteWaitGroupChildParams, FailWaitGroupChildParams, StorageError, UpsertWaitGroupStepParams};

/// Internal repository for wait-group step coordination.
/// Not part of the public API — used to modularize the `DurableStorage` impl.
pub(crate) trait WaitGroupRepository: Sized {
    async fn upsert_wait_group_step(
        &self,
        params: UpsertWaitGroupStepParams,
    ) -> Result<(), StorageError>;

    async fn complete_wait_group_child(
        &self,
        params: CompleteWaitGroupChildParams,
    ) -> Result<bool, StorageError>;

    async fn fail_wait_group_child(
        &self,
        params: FailWaitGroupChildParams,
    ) -> Result<bool, StorageError>;

    async fn recover_wait_group_orphans(&self) -> Result<usize, StorageError>;
}
