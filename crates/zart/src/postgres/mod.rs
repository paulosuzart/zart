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

use std::str::FromStr;
use std::sync::Arc;

use sqlx::PgPool;
use sqlx::postgres::PgConnectOptions;
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
    /// This covers both scheduler tables and execution tables in a single
    /// sequential set. Idempotent — safe to call multiple times.
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
        self.run_all_migrations().await
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

    /// Configure dedicated PostgreSQL connection options for `LISTEN/NOTIFY`.
    ///
    /// Use this when the main `PgPool` connects through a transaction-mode
    /// pooler (such as PgBouncer in transaction mode) that does not support
    /// session-level `LISTEN` commands.
    pub fn with_listener_options(self, options: PgConnectOptions) -> Self {
        let scheduler = Arc::new(
            self.scheduler
                .as_ref()
                .clone()
                .with_listener_options(options),
        );
        let storage = Arc::new(PostgresStorage {
            pool: self.storage.pool.clone(),
            table_names: self.storage.table_names.clone(),
            task_scheduler: scheduler.clone(),
        });
        Self { storage, scheduler }
    }

    /// Parse a PostgreSQL connection URL and configure it as the dedicated
    /// connection for `LISTEN/NOTIFY`.
    ///
    /// See [`PgBackend::with_listener_options`] for details.
    pub fn with_listener_url(self, url: &str) -> Result<Self, StorageError> {
        let options =
            PgConnectOptions::from_str(url).map_err(|e| StorageError::Database(Box::new(e)))?;
        Ok(self.with_listener_options(options))
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
    use super::*;

    #[tokio::test]
    async fn pg_backend_with_listener_options_wires_shared_scheduler() {
        let pool = PgPool::connect_lazy("postgres://postgres:postgres@pgbouncer:6432/zart")
            .expect("lazy pool creation should succeed");
        let opts = PgConnectOptions::new()
            .host("pg-direct")
            .port(5432)
            .database("zart");

        let backend = PgBackend::new(pool).with_listener_options(opts);
        let configured = backend
            .scheduler
            .listener_options()
            .expect("listener options should be wired onto scheduler");
        assert_eq!(configured.get_host(), "pg-direct");
        assert_eq!(configured.get_port(), 5432);
        assert_eq!(configured.get_database(), Some("zart"));
        // Both PgBackend::scheduler and PostgresStorage::task_scheduler share the same Arc.
        assert_eq!(Arc::strong_count(&backend.scheduler), 2);
    }

    #[tokio::test]
    async fn pg_backend_with_listener_url_parses_and_wires_or_errors() {
        let pool = PgPool::connect_lazy("postgres://postgres:postgres@pgbouncer:6432/zart")
            .expect("lazy pool creation should succeed");

        let backend = PgBackend::new(pool.clone())
            .with_listener_url("postgres://user:pass@pg-direct.internal:5435/zart_db")
            .expect("valid listener URL should parse");
        let configured = backend
            .scheduler
            .listener_options()
            .expect("listener options should be wired onto scheduler");
        assert_eq!(configured.get_host(), "pg-direct.internal");
        assert_eq!(configured.get_port(), 5435);
        assert_eq!(configured.get_database(), Some("zart_db"));
        assert_eq!(Arc::strong_count(&backend.scheduler), 2);

        let err = PgBackend::new(pool).with_listener_url("not a valid url");
        assert!(matches!(err, Err(StorageError::Database(_))));
    }
}
