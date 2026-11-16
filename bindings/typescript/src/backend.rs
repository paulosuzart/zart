use std::sync::Arc;

use napi::bindgen_prelude::*;
use napi_derive::napi;
use sqlx::PgPool;

use crate::errors::backend_err;
use crate::trx::ClientTrx;

/// PostgreSQL-backed storage and scheduler entry point.
#[napi]
#[derive(Clone)]
pub struct PgBackend {
    pub(crate) inner: Arc<zart::PgBackend>,
}

#[napi]
impl PgBackend {
    /// Connect to PostgreSQL. Optional `listenerUrl` is used for LISTEN/NOTIFY.
    #[napi(factory)]
    pub async fn connect(url: String, listener_url: Option<String>) -> Result<PgBackend> {
        let pool = PgPool::connect(&url)
            .await
            .map_err(|e| backend_err("Failed to connect to PostgreSQL", e))?;
        let mut backend = zart::PgBackend::new(pool);
        if let Some(l) = listener_url {
            backend = backend
                .with_listener_url(&l)
                .map_err(|e| backend_err("Invalid listener URL", e))?;
        }
        Ok(PgBackend {
            inner: Arc::new(backend),
        })
    }

    /// New backend sharing the same pool with a dedicated LISTEN/NOTIFY URL.
    #[napi]
    pub fn with_listener_url(&self, url: String) -> Result<PgBackend> {
        let backend = zart::PgBackend::new(self.inner.pool().clone())
            .with_listener_url(&url)
            .map_err(|e| backend_err("Invalid listener URL", e))?;
        Ok(PgBackend {
            inner: Arc::new(backend),
        })
    }

    #[napi]
    pub async fn run_migrations(&self) -> Result<()> {
        self.inner
            .run_migrations()
            .await
            .map_err(|e| backend_err("Migration failed", e))
    }

    /// Round-trip `SELECT 1`.
    #[napi]
    pub async fn ping(&self) -> Result<()> {
        sqlx::query("SELECT 1")
            .execute(self.inner.pool())
            .await
            .map(|_| ())
            .map_err(|e| backend_err("Ping failed", e))
    }

    #[napi]
    pub async fn close(&self) -> Result<()> {
        self.inner.pool().close().await;
        Ok(())
    }

    /// Begin a caller-owned transaction.
    #[napi]
    pub async fn begin(&self) -> Result<ClientTrx> {
        let tx = self
            .inner
            .pool()
            .begin()
            .await
            .map_err(|e| backend_err("Failed to begin transaction", e))?;
        Ok(ClientTrx::new(tx))
    }
}
