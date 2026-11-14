//! PostgreSQL-backed task-queue implementation of `TaskScheduler`.
//!
//! Uses `sqlx` with a `PgPool` for connection pooling. Task locking is
//! implemented with `SELECT … FOR UPDATE SKIP LOCKED` so multiple workers
//! can poll concurrently without processing the same task twice.
//!
//! # Schema setup
//!
//! Run `PgBackend::run_migrations()` (from the `zart` crate) or apply
//! `sql/0001_scheduler.sql` manually before starting workers to create
//! the required tables.

mod scheduler_impl;
mod sql_helpers;
mod table_names;

use std::str::FromStr;
use std::sync::Arc;

use sqlx::PgPool;
use sqlx::postgres::PgConnectOptions;
use tokio::sync::Notify;

use crate::StorageError;

pub use table_names::{TableNames, TableNamesError};

/// A [`TaskScheduler`](crate::TaskScheduler) backed by a PostgreSQL database.
///
/// Manages only the `zart_tasks` table (task queue lifecycle: schedule, poll,
/// complete, fail, cancel). For execution, step, and event storage use
/// `zart::PostgresStorage`.
///
/// Create one with [`PostgresTaskScheduler::new`], passing in an already-built
/// `sqlx::PgPool`. Ensure the schema has been provisioned via
/// `PgBackend::run_migrations()` (from the `zart` crate) or by applying
/// `sql/0001_scheduler.sql` manually before first use.
///
/// To use custom table names (e.g. to avoid collisions or support multi-tenancy),
/// use [`PostgresTaskScheduler::with_table_names`] together with [`TableNames`].
#[derive(Clone)]
pub struct PostgresTaskScheduler {
    pub(crate) pool: PgPool,
    pub(crate) table_names: TableNames,
    pub(crate) local_wakeup: Arc<Notify>,
    pub(crate) listener_options: Option<PgConnectOptions>,
}

impl PostgresTaskScheduler {
    /// Create a new task scheduler using the default `zart_*` table names.
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            table_names: TableNames::default(),
            local_wakeup: Arc::new(Notify::new()),
            listener_options: None,
        }
    }

    /// Create a new task scheduler with explicit table-name configuration.
    pub fn with_table_names(pool: PgPool, table_names: TableNames) -> Self {
        Self {
            pool,
            table_names,
            local_wakeup: Arc::new(Notify::new()),
            listener_options: None,
        }
    }

    /// Configure dedicated PostgreSQL connection options for `LISTEN/NOTIFY`.
    ///
    /// Use this when `pool` connects through a transaction-mode pooler (such
    /// as PgBouncer in transaction mode) that does not support session-level
    /// `LISTEN` commands.
    pub fn with_listener_options(mut self, options: PgConnectOptions) -> Self {
        self.listener_options = Some(options);
        self
    }

    /// Parse a PostgreSQL connection URL and configure it as the dedicated
    /// connection for `LISTEN/NOTIFY`.
    ///
    /// See [`PostgresTaskScheduler::with_listener_options`] for details.
    pub fn with_listener_url(self, url: &str) -> Result<Self, StorageError> {
        let options =
            PgConnectOptions::from_str(url).map_err(|e| StorageError::Database(Box::new(e)))?;
        Ok(self.with_listener_options(options))
    }

    /// Returns a reference to the underlying connection pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Returns a reference to the in-process wakeup notifier.
    pub fn local_wakeup(&self) -> &Arc<Notify> {
        &self.local_wakeup
    }

    /// Returns the configured dedicated listener connection options, if any.
    pub fn listener_options(&self) -> Option<&PgConnectOptions> {
        self.listener_options.as_ref()
    }
}

/// Deprecated: use `zart::PostgresStorage` for full storage or
/// [`PostgresTaskScheduler`] for task-queue-only use.
#[deprecated(
    since = "0.2.0",
    note = "Use zart::PostgresStorage for full StorageBackend or PostgresTaskScheduler for task-queue only."
)]
pub type PostgresScheduler = PostgresTaskScheduler;
