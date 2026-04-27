//! PostgreSQL-backed storage for all Zart execution-side tables.
//!
//! [`PostgresStorage`] implements `StorageBackend`, covering all execution-side
//! tables (`zart_executions`, `zart_execution_runs`, `zart_steps`,
//! `zart_step_attempts`, `zart_wait_groups`, `zart_events`, `zart_pause_rules`).
//!
//! Task-queue operations (`zart_tasks`) are owned by [`zart_scheduler::PostgresTaskScheduler`]
//! and are accessed via the internal `task_scheduler` held by this struct.
//!
//! # Usage
//!
//! ```rust,no_run
//! # async fn example() {
//! use sqlx::PgPool;
//! use zart::postgres::PostgresStorage;
//!
//! let pool = PgPool::connect("postgres://localhost/mydb").await.unwrap();
//! let storage = PostgresStorage::new(pool);
//! storage.run_migrations().await.unwrap();
//! # }
//! ```

mod admin_storage_impl;
mod event_storage_impl;
mod execution_storage_impl;
mod pause_storage_impl;
mod sql_helpers;
mod step_storage_impl;
mod table_names;
mod wait_group_storage_impl;

use std::sync::Arc;

use sqlx::PgPool;
use zart_core::StorageError;
use zart_scheduler::PostgresTaskScheduler;
use zart_scheduler::TaskScheduler;

pub use table_names::{TableNames, TableNamesError};

/// A fully-capable execution-side storage backend backed by a PostgreSQL database.
///
/// Implements `StorageBackend`, composing `ExecutionStore`, `StepStore`,
/// `WaitGroupStore`, `EventStore`, and `PauseStorage`. Task-queue operations
/// are delegated to an internal [`PostgresTaskScheduler`].
///
/// Create one with [`PostgresStorage::new`], passing in an already-built
/// `sqlx::PgPool`. Call [`run_migrations`](Self::run_migrations) before first
/// use to ensure the schema is up to date.
pub struct PostgresStorage {
    pool: PgPool,
    table_names: TableNames,
    /// Task-queue delegate. All task-queue methods forward here.
    /// No task-queue SQL lives in this crate.
    pub(crate) task_scheduler: Arc<dyn TaskScheduler>,
}

impl PostgresStorage {
    /// Create a new storage using the default `zart_*` table names.
    ///
    /// An internal [`PostgresTaskScheduler`] is created from the same pool
    /// to handle task-queue operations via delegation.
    pub fn new(pool: PgPool) -> Self {
        let task_scheduler = Arc::new(PostgresTaskScheduler::new(pool.clone()));
        Self {
            pool,
            table_names: TableNames::default(),
            task_scheduler,
        }
    }

    /// Create a new storage with explicit table-name configuration.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # async fn example() {
    /// use sqlx::PgPool;
    /// use zart::postgres::{PostgresStorage, TableNames};
    ///
    /// let pool = PgPool::connect("postgres://...").await.unwrap();
    /// let config = zart_core::table_names::TableNameConfig::with_prefix("myapp_").unwrap();
    /// let storage = PostgresStorage::with_table_names(pool, TableNames::from_config(config));
    /// # }
    /// ```
    pub fn with_table_names(pool: PgPool, table_names: TableNames) -> Self {
        let task_scheduler = Arc::new(PostgresTaskScheduler::new(pool.clone()));
        Self {
            pool,
            table_names,
            task_scheduler,
        }
    }

    /// Inject a custom task-scheduler (useful in tests or advanced deployments).
    ///
    /// By default, [`new`](Self::new) creates an internal [`PostgresTaskScheduler`]
    /// from the same pool. Call this to replace it with a custom implementation or
    /// a mock for testing.
    pub fn with_task_scheduler(mut self, task_scheduler: Arc<dyn TaskScheduler>) -> Self {
        self.task_scheduler = task_scheduler;
        self
    }

    /// Returns a reference to the underlying connection pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Returns a clone of the internal task scheduler.
    pub fn task_scheduler(&self) -> Arc<dyn TaskScheduler> {
        self.task_scheduler.clone()
    }

    /// Run all database migrations required by this backend.
    ///
    /// This applies the embedded SQL from `zart-scheduler/migrations` (which
    /// covers both `zart_tasks` and all execution-side tables in one file).
    /// Idempotent — safe to call multiple times.
    pub async fn run_migrations(&self) -> Result<(), StorageError> {
        self.task_scheduler.run_migrations().await
    }
}
