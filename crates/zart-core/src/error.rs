//! Error types for Zart storage backends.

use thiserror::Error;

/// Errors originating from a storage backend.
#[derive(Debug, Error)]
pub enum StorageError {
    /// A database-level error (connection, query, constraint violation, etc.).
    #[error("Database error: {0}")]
    Database(Box<dyn std::error::Error + Send + Sync>),

    /// Failed to establish or maintain a database connection.
    #[error("Connection error: {0}")]
    Connection(String),

    /// A database migration failed to apply.
    #[error("Migration error: {0}")]
    Migration(String),

    /// A task with the given ID was not found.
    #[error("Task not found: {0}")]
    NotFound(String),

    /// The provided lock token did not match (optimistic lock failure).
    #[error("Lock token mismatch for task {0}")]
    LockMismatch(String),

    /// The operation is not implemented by this backend.
    #[error("Not implemented: {0}")]
    NotImplemented(&'static str),

    /// A step was found but was not in the expected status (e.g. not dead for retry).
    #[error("Step '{step}' has status '{actual}', expected '{expected}'")]
    StepStatusMismatch {
        step: String,
        actual: String,
        expected: String,
    },

    /// No step was found for the given name and run.
    #[error("Step '{0}' not found")]
    StepNotFound(String),
}
