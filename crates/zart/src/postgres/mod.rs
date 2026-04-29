//! PostgreSQL-backed storage and entry point for Zart.
//!
//! The primary entry point is [`PgBackend`], which owns both execution-side
//! storage ([`PostgresStorage`]) and task-queue scheduling
//! ([`zart_scheduler::PostgresTaskScheduler`]) from a single connection pool.
//!
//! [`PostgresStorage`] implements `StorageBackend`, covering all execution-side
//! tables (`zart_executions`, `zart_execution_runs`, `zart_steps`,
//! `zart_step_attempts`, `zart_wait_groups`, `zart_events`, `zart_pause_rules`).
//!
//! # Usage
//!
//! ```rust,no_run
//! # async fn example() {
//! use sqlx::PgPool;
//! use zart::{DurableScheduler, WorkerBuilder, postgres::PgBackend};
//!
//! let pool = PgPool::connect("postgres://localhost/mydb").await.unwrap();
//! let pg = PgBackend::new(pool);
//! pg.run_migrations().await.unwrap();
//!
//! let durable = DurableScheduler::from_backend(&pg);
//! let worker = WorkerBuilder::from_backend(&pg).build();
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

use crate::store::{Backend, StorageBackend};

pub use table_names::{TableNames, TableNamesError};

/// A fully-capable execution-side storage backend backed by a PostgreSQL database.
///
/// Implements `StorageBackend`, composing `ExecutionStore`, `StepStore`,
/// `WaitGroupStore`, `EventStore`, and `PauseStorage`. Task-queue operations
/// are delegated to an internal [`PostgresTaskScheduler`].
///
/// For most users, [`PgBackend`] is the preferred entry point — it owns both
/// the storage and the scheduler in a single struct and exposes
/// [`PgBackend::run_migrations`]. Use [`PostgresStorage`] directly only when
/// you need fine-grained control over the scheduler instance.
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

    /// Returns a reference to the underlying connection pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Returns a clone of the internal task scheduler.
    #[deprecated(
        since = "0.1.0",
        note = "Use `PgBackend::scheduler()` instead. `PostgresStorage` will no longer own a scheduler in a future major version."
    )]
    pub fn task_scheduler(&self) -> Arc<dyn TaskScheduler> {
        self.task_scheduler.clone()
    }

    /// Inject a custom task-scheduler (useful in tests or advanced deployments).
    #[deprecated(
        since = "0.1.0",
        note = "Construct `PgBackend` directly or use `DurableScheduler::new` with explicit arguments instead."
    )]
    pub fn with_task_scheduler(mut self, task_scheduler: Arc<dyn TaskScheduler>) -> Self {
        self.task_scheduler = task_scheduler;
        self
    }

    /// Run all migrations for this crate's unified migration set.
    ///
    /// This covers both scheduler tables (mirrored from `zart-scheduler`) and execution
    /// tables in a single sequential set. See `migrations/0001_scheduler.sql` — that file
    /// is intentionally byte-identical to `zart-scheduler/migrations/0001_scheduler.sql`
    /// so their SQLx checksums agree when both migration paths share `_sqlx_migrations`.
    /// The test `migration_files_are_in_sync` enforces this invariant.
    pub(crate) async fn run_all_migrations(&self) -> Result<(), StorageError> {
        sqlx::migrate!("./migrations")
            .run(&self.pool)
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))
    }

    /// Run all database migrations required by this backend.
    #[deprecated(
        since = "0.1.0",
        note = "Use `PgBackend::run_migrations()` instead, which runs both scheduler and execution migrations."
    )]
    pub async fn run_migrations(&self) -> Result<(), StorageError> {
        self.task_scheduler.run_migrations().await
    }
}

// ── PgBackend ─────────────────────────────────────────────────────────────────

/// Single entry point for PostgreSQL-backed Zart.
///
/// Owns both execution-side storage ([`PostgresStorage`]) and task-queue
/// scheduling ([`PostgresTaskScheduler`]), created from the same connection pool.
///
/// # Usage
///
/// ```text
/// let pool = PgPool::connect("postgres://localhost/mydb").await.unwrap();
/// let pg = PgBackend::new(pool);
/// pg.run_migrations().await.unwrap();
///
/// let durable = DurableScheduler::from_backend(&pg);
/// let worker = WorkerBuilder::from_backend(&pg)
///     .register_durable_task("my-task", MyHandler)
///     .build();
/// ```
pub struct PgBackend {
    storage: Arc<PostgresStorage>,
    scheduler: Arc<PostgresTaskScheduler>,
}

impl PgBackend {
    /// Create a new `PgBackend` using the default `zart_*` table names.
    pub fn new(pool: PgPool) -> Self {
        let scheduler = Arc::new(PostgresTaskScheduler::new(pool.clone()));
        let storage = Arc::new(PostgresStorage {
            pool: pool.clone(),
            table_names: TableNames::default(),
            task_scheduler: scheduler.clone(),
        });
        Self { storage, scheduler }
    }

    /// Create a new `PgBackend` with explicit table-name configuration.
    pub fn with_table_names(pool: PgPool, names: TableNames) -> Self {
        let scheduler = Arc::new(PostgresTaskScheduler::new(pool.clone()));
        let storage = Arc::new(PostgresStorage {
            pool: pool.clone(),
            table_names: names,
            task_scheduler: scheduler.clone(),
        });
        Self { storage, scheduler }
    }

    /// Returns a reference to the underlying connection pool.
    pub fn pool(&self) -> &PgPool {
        &self.storage.pool
    }

    /// Run all database migrations against the connected database.
    ///
    /// Applies `zart/migrations/` as a single sequential set: `0001_scheduler.sql`
    /// (task-queue tables) followed by `0002_execution.sql` (durable-execution tables).
    /// Idempotent — safe to call multiple times.
    ///
    /// ## Migration file ownership
    ///
    /// `0001_scheduler.sql` here is a byte-identical mirror of
    /// `zart-scheduler/migrations/0001_scheduler.sql`. Both paths share the same
    /// `_sqlx_migrations` tracking table, so the checksums must agree.
    /// If you ever need to use `zart-scheduler` standalone first and later switch to
    /// `PgBackend`, the checksum match means the v1 migration is recognised as already
    /// applied and skipped. The test `migration_files_are_in_sync` enforces this.
    pub async fn run_migrations(&self) -> Result<(), StorageError> {
        self.storage.run_all_migrations().await
    }
}

impl Backend for PgBackend {
    fn storage(&self) -> Arc<dyn StorageBackend> {
        self.storage.clone()
    }

    fn scheduler(&self) -> Arc<dyn TaskScheduler> {
        self.scheduler.clone()
    }
}

#[cfg(test)]
mod tests {
    /// `zart/migrations/0001_scheduler.sql` must stay byte-identical to
    /// `zart-scheduler/migrations/0001_scheduler.sql` so that both migration paths
    /// (standalone scheduler and PgBackend) produce the same SQLx checksum for version 1
    /// and therefore share `_sqlx_migrations` without conflicts.
    ///
    /// If this test fails, copy the canonical file from `zart-scheduler` into `zart`.
    #[test]
    fn migration_files_are_in_sync() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap();
        let zart = root.join("crates/zart/migrations/0001_scheduler.sql");
        let scheduler = root.join("crates/zart-scheduler/migrations/0001_scheduler.sql");

        let zart_content =
            std::fs::read(&zart).unwrap_or_else(|e| panic!("cannot read {}: {e}", zart.display()));
        let scheduler_content = std::fs::read(&scheduler)
            .unwrap_or_else(|e| panic!("cannot read {}: {e}", scheduler.display()));

        assert_eq!(
            zart_content, scheduler_content,
            "migration files are out of sync — copy zart-scheduler/migrations/0001_scheduler.sql \
             into zart/migrations/0001_scheduler.sql"
        );
    }
}
