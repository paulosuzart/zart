//! Repository trait for admin and run-reset operations.
//!
//! Provides raw SQL primitives for manual-intervention mutations. Business
//! logic for these operations (step-status validation, effective-rerun
//! computation) lives in the service layer (`zart` crate); this trait covers
//! only the atomically-committed SQL mechanics.

use crate::StorageError;

/// Internal repository for admin intervention and run-reset primitives.
/// Not part of the public API — used to modularize the `DurableStorage` impl.
pub(crate) trait AdminRepository: Sized {
    /// Atomically validate a step is `dead`, create a retry task, and reset
    /// the run to `running`. Corresponds to `DurableStorage::retry_dead_step`.
    async fn retry_dead_step(
        &self,
        run_id: &str,
        step_name: &str,
        triggered_by: Option<&str>,
    ) -> Result<String, StorageError>;

    /// Archive the current run and start a fresh one with `trigger`.
    /// Corresponds to `DurableStorage::restart_run`.
    async fn restart_run(
        &self,
        execution_id: &str,
        new_payload: Option<serde_json::Value>,
        trigger: &str,
        triggered_by: Option<&str>,
    ) -> Result<String, StorageError>;

    /// Reset a terminal execution so it can be retried.
    ///
    /// Creates a new run at run_index+1, updates `current_run_id`, and returns
    /// the new `run_id`. Does NOT schedule a body task — the caller does that.
    async fn reset_execution(
        &self,
        execution_id: &str,
        payload: serde_json::Value,
    ) -> Result<String, StorageError>;
}
