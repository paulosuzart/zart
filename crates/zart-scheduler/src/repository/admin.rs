//! Repository trait for admin and run-reset operations.
//!
//! Contains manual-intervention mutations (retry, restart, rerun) and the
//! `reset_execution` primitive that underlies them. Business logic for these
//! operations lives in the service layer (`zart` crate); this trait provides
//! only the SQL-level primitives.
//!
//! Temporary home: in Phase 2 of spec 0034, the business-logic methods
//! (`admin_retry_step`, `admin_restart_execution`, `admin_rerun_steps`) move
//! to `ExecutionService` in the `zart` crate, leaving only `reset_execution`
//! here (or merged into `ExecutionRepository`).

use crate::StorageError;

/// Internal repository for admin intervention and run-reset primitives.
/// Not part of the public API — used to modularize the `DurableStorage` impl.
pub(crate) trait AdminRepository: Sized {
    async fn admin_retry_step(
        &self,
        run_id: &str,
        step_name: &str,
        triggered_by: Option<&str>,
    ) -> Result<String, StorageError>;

    async fn admin_restart_execution(
        &self,
        execution_id: &str,
        new_payload: Option<serde_json::Value>,
        triggered_by: Option<&str>,
    ) -> Result<String, StorageError>;

    async fn admin_rerun_steps(
        &self,
        execution_id: &str,
        force_rerun: &[String],
        preserve: &[String],
        triggered_by: Option<&str>,
    ) -> Result<(String, Vec<String>), StorageError>;

    async fn reset_execution(
        &self,
        execution_id: &str,
        payload: serde_json::Value,
    ) -> Result<String, StorageError>;
}
