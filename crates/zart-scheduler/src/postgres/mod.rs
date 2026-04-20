//! PostgreSQL-backed task-queue implementation of [`TaskScheduler`].
//!
//! Uses `sqlx` with a `PgPool` for connection pooling. Task locking is
//! implemented with `SELECT … FOR UPDATE SKIP LOCKED` so multiple workers
//! can poll concurrently without processing the same task twice.
//!
//! # Migrations
//!
//! Call `PostgresTaskScheduler::run_migrations` (or `just migrate`) once before
//! starting workers. It applies the embedded SQL files under `migrations/`.

mod scheduler_impl;
mod sql_helpers;
mod table_names;

use sqlx::PgPool;

pub use table_names::{TableNames, TableNamesError};

/// A [`TaskScheduler`](crate::TaskScheduler) backed by a PostgreSQL database.
///
/// Manages only the `zart_tasks` table (task queue lifecycle: schedule, poll,
/// complete, fail, cancel). For execution, step, and event storage use
/// `zart::PostgresStorage`.
///
/// Create one with [`PostgresTaskScheduler::new`], passing in an already-built
/// `sqlx::PgPool`. Call `run_migrations` before first use to ensure the
/// schema is up to date.
///
/// To use custom table names (e.g. to avoid collisions or support multi-tenancy),
/// use [`PostgresTaskScheduler::with_table_names`] together with [`TableNames`].
pub struct PostgresTaskScheduler {
    pub(crate) pool: PgPool,
    pub(crate) table_names: TableNames,
}

impl PostgresTaskScheduler {
    /// Create a new task scheduler using the default `zart_*` table names.
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            table_names: TableNames::default(),
        }
    }

    /// Create a new task scheduler with explicit table-name configuration.
    pub fn with_table_names(pool: PgPool, table_names: TableNames) -> Self {
        Self { pool, table_names }
    }

    /// Returns a reference to the underlying connection pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

/// Deprecated: use [`zart::PostgresStorage`] for full storage or
/// [`PostgresTaskScheduler`] for task-queue-only use.
#[deprecated(
    since = "0.2.0",
    note = "Use zart::PostgresStorage for full StorageBackend or PostgresTaskScheduler for task-queue only."
)]
pub type PostgresScheduler = PostgresTaskScheduler;
