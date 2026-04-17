//! PostgreSQL-backed implementation of the `Scheduler` trait.
//!
//! Uses `sqlx` with a `PgPool` for connection pooling. Task locking is
//! implemented with `SELECT … FOR UPDATE SKIP LOCKED` so multiple workers
//! can poll concurrently without processing the same task twice.
//!
//! # Migrations
//!
//! Call `PostgresScheduler::run_migrations` (or `just migrate`) once before
//! starting workers. It applies the embedded SQL files under `migrations/`.

mod admin_storage_impl;
mod event_storage_impl;
mod execution_storage_impl;
mod pause_storage_impl;
mod scheduler_impl;
mod sql_helpers;
mod step_storage_impl;
mod storage_impl;
mod table_names;
mod wait_group_storage_impl;

pub(crate) use admin_storage_impl::AdminStorage;
pub(crate) use event_storage_impl::EventStorage;
pub(crate) use execution_storage_impl::ExecutionStorage;
pub(crate) use step_storage_impl::StepStorage;
pub(crate) use wait_group_storage_impl::WaitGroupStorage;

use sqlx::PgPool;

pub use table_names::{TableNames, TableNamesError};

/// A `Scheduler` backed by a PostgreSQL database.
///
/// Create one with [`PostgresScheduler::new`], passing in an already-built
/// `sqlx::PgPool`. Call `run_migrations` before first use to ensure the
/// schema is up to date.
///
/// To use custom table names (e.g. to avoid collisions or support multi-tenancy),
/// use [`PostgresScheduler::with_table_names`] together with [`TableNames`].
pub struct PostgresScheduler {
    pool: PgPool,
    table_names: TableNames,
}

impl PostgresScheduler {
    /// Create a new scheduler using the default `zart_*` table names.
    ///
    /// This is backward-compatible with existing code — no migration or
    /// configuration change is required.
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            table_names: TableNames::default(),
        }
    }

    /// Create a new scheduler with explicit table-name configuration.
    ///
    /// Use this when you need a custom prefix or schema qualifier.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # async fn example() {
    /// use sqlx::PgPool;
    /// use zart_scheduler::postgres::{PostgresScheduler, TableNames};
    ///
    /// let pool = PgPool::connect("postgres://...").await.unwrap();
    /// let names = TableNames::with_prefix("myapp_").unwrap();
    /// let scheduler = PostgresScheduler::with_table_names(pool, names);
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
