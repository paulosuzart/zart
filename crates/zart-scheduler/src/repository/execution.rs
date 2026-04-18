//! Repository trait for execution and run table access.
//!
//! Scoped to `zart_executions` and `zart_execution_runs`.
//! Raw data access only — no business logic.

use sqlx::PgConnection;

use crate::{ExecutionRecord, ExecutionRunRecord, ListExecutionsParams, StorageError};

/// Internal repository for durable execution and run row access.
/// Not part of the public API — used to modularize the `DurableStorage` impl.
pub(crate) trait ExecutionRepository: Sized {
    async fn start_execution(
        &self,
        execution_id: &str,
        task_name: &str,
        payload: serde_json::Value,
    ) -> Result<(), StorageError>;

    async fn start_execution_in_tx(
        &self,
        conn: &mut PgConnection,
        execution_id: &str,
        task_name: &str,
        payload: serde_json::Value,
    ) -> Result<(), StorageError>;

    async fn complete_execution(
        &self,
        execution_id: &str,
        result: serde_json::Value,
    ) -> Result<(), StorageError>;

    async fn fail_execution(&self, execution_id: &str) -> Result<(), StorageError>;

    async fn get_execution(
        &self,
        execution_id: &str,
    ) -> Result<Option<ExecutionRecord>, StorageError>;

    async fn cancel_execution(&self, execution_id: &str) -> Result<bool, StorageError>;

    async fn list_executions(
        &self,
        params: ListExecutionsParams,
    ) -> Result<Vec<ExecutionRecord>, StorageError>;

    async fn get_current_run_id(&self, execution_id: &str) -> Result<Option<String>, StorageError>;

    async fn list_runs(&self, execution_id: &str) -> Result<Vec<ExecutionRunRecord>, StorageError>;
}
