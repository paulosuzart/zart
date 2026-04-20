//! PostgreSQL-backed storage for all Zart tables.
//!
//! [`PostgresStorage`] implements the full [`zart_scheduler::StorageBackend`]
//! trait, covering both the task queue (`zart_tasks`) and all execution-side
//! tables (`zart_executions`, `zart_execution_runs`, `zart_steps`,
//! `zart_step_attempts`, `zart_wait_groups`, `zart_events`, `zart_pause_rules`).
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
//! # }
//! ```

mod admin_storage_impl;
mod event_storage_impl;
mod execution_storage_impl;
mod pause_storage_impl;
mod scheduler_impl;
mod sql_helpers;
mod step_storage_impl;
mod table_names;
mod wait_group_storage_impl;

use sqlx::PgPool;

pub use table_names::{TableNames, TableNamesError};

/// A fully-capable storage backend backed by a PostgreSQL database.
///
/// Implements [`zart_scheduler::StorageBackend`] which composes
/// [`zart_scheduler::TaskScheduler`], [`zart_scheduler::ExecutionStore`],
/// [`zart_scheduler::StepStore`], [`zart_scheduler::WaitGroupStore`],
/// [`zart_scheduler::EventStore`], and [`zart_scheduler::PauseStorage`].
///
/// Create one with [`PostgresStorage::new`], passing in an already-built
/// `sqlx::PgPool`. Call `run_migrations` before first use to ensure the
/// schema is up to date.
pub struct PostgresStorage {
    pool: PgPool,
    table_names: TableNames,
}

impl PostgresStorage {
    /// Create a new storage using the default `zart_*` table names.
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            table_names: TableNames::default(),
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
    /// let names = TableNames::with_prefix("myapp_").unwrap();
    /// let storage = PostgresStorage::with_table_names(pool, names);
    /// # }
    /// ```
    pub fn with_table_names(pool: PgPool, table_names: TableNames) -> Self {
        Self { pool, table_names }
    }

    /// Returns a reference to the underlying connection pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}
