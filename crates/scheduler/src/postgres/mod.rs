//! PostgreSQL-backed implementation of the [`Scheduler`] trait.
//!
//! Uses `sqlx` with a `PgPool` for connection pooling. Task locking is
//! implemented with `SELECT … FOR UPDATE SKIP LOCKED` so multiple workers
//! can poll concurrently without processing the same task twice.
//!
//! # Migrations
//!
//! Call [`PostgresScheduler::run_migrations`] (or `just migrate`) once before
//! starting workers. It applies the embedded SQL files under `migrations/`.

mod scheduler_impl;
mod storage_impl;

use sqlx::PgPool;

/// A [`Scheduler`] backed by a PostgreSQL database.
///
/// Create one with [`PostgresScheduler::new`], passing in an already-built
/// `sqlx::PgPool`. Call [`run_migrations`][Self::run_migrations] before first
/// use to ensure the schema is up to date.
pub struct PostgresScheduler {
    pool: PgPool,
}

impl PostgresScheduler {
    /// Create a new scheduler wrapping the given connection pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}
