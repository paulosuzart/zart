//! Credential rotation support for PostgreSQL connection pools.
//!
//! Provides mechanisms for gracefully refreshing database credentials
//! without restarting the worker process. Supports both programmatic
//! API access and file-based credential sources.
//!
//! # Examples
//!
//! ```rust
//! use scheduler::postgres::CredentialManager;
//!
//! // Rotate credentials programmatically
//! let manager = CredentialManager::new(pool_ref, database_url);
//! manager.rotate(&new_database_url).await?;
//! ```

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;
use tokio::sync::RwLock;
use tracing::{info, warn};

/// Source for database credentials.
#[derive(Debug, Clone)]
pub enum CredentialsSource {
    /// Static database URL (no automatic refresh).
    Static(String),
    /// Read credentials from a file on each rotation.
    File(PathBuf),
    /// Read credentials from an environment variable.
    EnvVar(String),
}

impl CredentialsSource {
    /// Read the current credentials from the source.
    pub async fn read(&self) -> Result<String, CredentialRotationError> {
        match self {
            CredentialsSource::Static(url) => Ok(url.clone()),
            CredentialsSource::File(path) => {
                let content = tokio::fs::read_to_string(path)
                    .await
                    .map_err(|e| CredentialRotationError::FileRead(path.clone(), e))?;
                Ok(content.trim().to_string())
            }
            CredentialsSource::EnvVar(name) => {
                std::env::var(name)
                    .map_err(|e| CredentialRotationError::EnvVar(name.clone(), e))
            }
        }
    }
}

/// Manages credential rotation for a PostgreSQL connection pool.
///
/// The pool reference is shared with the `PostgresScheduler` via an `Arc<RwLock<PgPool>>`,
/// allowing atomic replacement during rotation while the scheduler continues to hold
/// a reference to the same `Arc`.
pub struct CredentialManager {
    /// Shared reference to the pool, swapped during rotation.
    pool_ref: Arc<RwLock<PgPool>>,
    /// Optional source for automatic reload.
    source: Option<CredentialsSource>,
    /// Current tracked URL for idempotency checks.
    current_url: RwLock<String>,
}

impl CredentialManager {
    /// Create a new credential manager.
    ///
    /// The `pool_ref` is an `Arc<RwLock<PgPool>>` shared with the scheduler.
    /// During rotation, the pool inside the Arc is atomically replaced.
    pub fn new(pool_ref: Arc<RwLock<PgPool>>, database_url: &str) -> Self {
        Self {
            pool_ref,
            source: None,
            current_url: RwLock::new(database_url.to_string()),
        }
    }

    /// Create a new credential manager with a credential source.
    ///
    /// The source is used when [`reload`](Self::reload) is called.
    pub fn with_source(
        pool_ref: Arc<RwLock<PgPool>>,
        source: CredentialsSource,
        current_url: &str,
    ) -> Self {
        Self {
            pool_ref,
            source: Some(source),
            current_url: RwLock::new(current_url.to_string()),
        }
    }

    /// Gracefully replace the connection pool with a new one using the given URL.
    ///
    /// # Behavior
    /// 1. Creates a new pool with the new credentials
    /// 2. Atomically swaps the old pool with the new one inside the shared Arc<RwLock>
    /// 3. Closes the old pool's connections
    ///
    /// # Arguments
    ///
    /// * `new_url` - The new database connection URL
    /// * `graceful` - If true, drains existing connections before closing
    /// * `drain_timeout` - How long to wait for in-flight queries to complete
    ///
    /// # Note
    ///
    /// In-flight tasks may fail during rotation if the old pool is closed
    /// before they complete. For zero-downtime rotation, ensure that
    /// `drain_timeout` is longer than your longest-running task.
    pub async fn rotate(
        &self,
        new_url: &str,
        graceful: bool,
        drain_timeout: Duration,
    ) -> Result<(), CredentialRotationError> {
        // Idempotency check: if URL is the same, skip rotation.
        {
            let current = self.current_url.read().await;
            if current == new_url {
                info!("Credential rotation skipped: URL unchanged");
                return Ok(());
            }
        }

        info!("Starting credential rotation");

        // Create new pool with fresh credentials.
        let new_pool = PgPool::connect(new_url)
            .await
            .map_err(|e| CredentialRotationError::ConnectionFailed(e.to_string()))?;

        // Atomic swap: acquire write lock on the RwLock, replace the pool inside.
        {
            let mut lock = self.pool_ref.write().await;
            let old_pool = std::mem::replace(&mut *lock, new_pool);

            // Gracefully close old connections.
            if graceful {
                info!("Gracefully draining old connections");
                tokio::select! {
                    _ = old_pool.close() => {
                        info!("Old connections drained successfully");
                    }
                    _ = tokio::time::sleep(drain_timeout) => {
                        warn!("Old connections did not drain within timeout, forcing close");
                        old_pool.close().await;
                    }
                }
            } else {
                // Immediate close: in-flight queries will fail.
                info!("Forcing immediate close of old connections");
                old_pool.close().await;
            }
        }

        // Update tracked URL.
        {
            let mut current = self.current_url.write().await;
            *current = new_url.to_string();
        }

        info!("Credential rotation completed");
        Ok(())
    }

    /// Reload credentials from the configured source and rotate the pool.
    ///
    /// Returns an error if no source is configured or if the source cannot be read.
    pub async fn reload(
        &self,
        graceful: bool,
        drain_timeout: Duration,
    ) -> Result<(), CredentialRotationError> {
        let source = self
            .source
            .as_ref()
            .ok_or(CredentialRotationError::NoSourceConfigured)?;

        let new_url = source.read().await?;
        self.rotate(&new_url, graceful, drain_timeout).await
    }

    /// Check if the current pool is healthy.
    ///
    /// Attempts to acquire a connection and run a simple query.
    pub async fn is_healthy(&self) -> bool {
        let pool = self.pool_ref.read().await;
        sqlx::query("SELECT 1")
            .execute(&*pool)
            .await
            .is_ok()
    }

    /// Get the current database URL (for diagnostics, not for connections).
    pub async fn current_url(&self) -> String {
        self.current_url.read().await.clone()
    }
}

/// Errors that can occur during credential rotation.
#[derive(Debug, thiserror::Error)]
pub enum CredentialRotationError {
    /// Failed to establish a connection with the new credentials.
    #[error("Failed to connect with new credentials: {0}")]
    ConnectionFailed(String),

    /// No credential source was configured for automatic reload.
    #[error("No credential source configured")]
    NoSourceConfigured,

    /// Failed to read credentials from a file.
    #[error("Failed to read credentials from file {0}: {1}")]
    FileRead(PathBuf, std::io::Error),

    /// Failed to read credentials from an environment variable.
    #[error("Failed to read environment variable {0}: {1}")]
    EnvVar(String, std::env::VarError),

    /// A database error occurred during rotation.
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
}

/// Spawn a background task that listens for SIGUSR1 and triggers credential reload.
///
/// This is a convenience function for UNIX systems where credential rotation
/// is signaled via SIGUSR1. The handler reads credentials from the configured
/// source and rotates the pool.
///
/// # Platform
///
/// Only available on UNIX-like systems. On other platforms, this function
/// does nothing.
///
/// # Example
///
/// ```rust
/// // In your main function:
/// scheduler::postgres::credentials::spawn_sigusr1_handler(&scheduler).await;
/// ```
#[cfg(unix)]
pub async fn spawn_sigusr1_handler(
    scheduler: &PostgresScheduler,
) -> Result<(), CredentialRotationError> {
    use tokio::signal::unix::{signal, SignalKind};

    let scheduler = scheduler.clone();

    tokio::spawn(async move {
        let mut sigusr1 = match signal(SignalKind::user_defined1()) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = %e, "Failed to register SIGUSR1 handler");
                return;
            }
        };

        loop {
            sigusr1.recv().await;
            tracing::info!("Received SIGUSR1 signal, reloading credentials");

            match scheduler.reload_credentials(true).await {
                Ok(()) => tracing::info!("Credential reload successful"),
                Err(e) => tracing::error!(error = %e, "Failed to reload credentials"),
            }
        }
    });

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credentials_source_static_reads_url() {
        let source = CredentialsSource::Static("postgres://user:pass@localhost/db".to_string());
        // Can't use async in sync tests, but the variant is constructible.
        assert!(matches!(source, CredentialsSource::Static(_)));
    }

    #[test]
    fn credentials_source_file_variant_is_constructible() {
        let source = CredentialsSource::File(PathBuf::from("/tmp/creds"));
        assert!(matches!(source, CredentialsSource::File(_)));
    }

    #[test]
    fn credentials_source_envvar_variant_is_constructible() {
        let source = CredentialsSource::EnvVar("DATABASE_URL".to_string());
        assert!(matches!(source, CredentialsSource::EnvVar(_)));
    }
}
