use std::sync::Arc;

use napi::bindgen_prelude::*;
use napi_derive::napi;
use sqlx::PgPool;
use zart_core::table_names::TableNameConfig;

use crate::errors::{backend_err, schema_err};
use crate::trx::ClientTrx;

fn build_table_name_config(
    table_prefix: Option<String>,
    schema: Option<String>,
) -> Result<Option<TableNameConfig>> {
    if table_prefix.is_none() && schema.is_none() {
        return Ok(None);
    }
    let mut config = match table_prefix {
        Some(prefix) => TableNameConfig::with_prefix(prefix).map_err(schema_err)?,
        None => TableNameConfig::default(),
    };
    if let Some(s) = schema {
        config = config.with_schema(s).map_err(schema_err)?;
    }
    Ok(Some(config))
}

fn assemble_backend(
    pool: PgPool,
    listener_url: Option<String>,
    table_config: Option<TableNameConfig>,
) -> Result<zart::PgBackend> {
    let mut backend = match table_config {
        Some(config) => {
            zart::PgBackend::with_table_names(pool, zart::postgres::TableNames::from_config(config))
        }
        None => zart::PgBackend::new(pool),
    };
    if let Some(l) = listener_url {
        backend = backend
            .with_listener_url(&l)
            .map_err(|e| backend_err("Invalid listener URL", e))?;
    }
    Ok(backend)
}

fn clone_with_listener_url(inner: &zart::PgBackend, url: &str) -> Result<zart::PgBackend> {
    inner
        .clone()
        .with_listener_url(url)
        .map_err(|e| backend_err("Invalid listener URL", e))
}

/// PostgreSQL-backed storage and scheduler entry point.
#[napi]
#[derive(Clone)]
pub struct PgBackend {
    pub(crate) inner: Arc<zart::PgBackend>,
}

#[napi]
impl PgBackend {
    /// Connect to PostgreSQL.
    ///
    /// Optional `listenerUrl` sets a dedicated connection URL for `LISTEN/NOTIFY`.
    /// Optional `tablePrefix` and `schema` customize Zart table names.
    #[napi(factory)]
    pub async fn connect(
        url: String,
        listener_url: Option<String>,
        table_prefix: Option<String>,
        schema: Option<String>,
    ) -> Result<PgBackend> {
        let table_config = build_table_name_config(table_prefix, schema)?;
        let pool = PgPool::connect(&url)
            .await
            .map_err(|e| backend_err("Failed to connect to PostgreSQL", e))?;
        let backend = assemble_backend(pool, listener_url, table_config)?;
        Ok(PgBackend {
            inner: Arc::new(backend),
        })
    }

    /// New backend sharing the same pool with a dedicated LISTEN/NOTIFY URL.
    #[napi]
    pub fn with_listener_url(&self, url: String) -> Result<PgBackend> {
        let backend = clone_with_listener_url(self.inner.as_ref(), &url)?;
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
        self.inner
            .ping()
            .await
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_table_name_config_handles_defaults_and_custom_values() {
        assert!(build_table_name_config(None, None).unwrap().is_none());

        let prefix_only = build_table_name_config(Some("app_".into()), None)
            .unwrap()
            .unwrap();
        assert_eq!(prefix_only.resolve("tasks"), "\"app_tasks\"");

        let schema_only = build_table_name_config(None, Some("tenant_a".into()))
            .unwrap()
            .unwrap();
        assert_eq!(schema_only.resolve("tasks"), "\"tenant_a\".\"zart_tasks\"");

        let both = build_table_name_config(Some("svc_".into()), Some("tenant_b".into()))
            .unwrap()
            .unwrap();
        assert_eq!(both.resolve("steps"), "\"tenant_b\".\"svc_steps\"");
    }

    #[test]
    fn build_table_name_config_rejects_invalid_identifiers_as_schema_error() {
        for (prefix, schema) in [
            (Some("bad-prefix"), None),
            (Some(""), None),
            (None, Some("1invalid")),
            (None, Some("bad-schema")),
        ] {
            let err = build_table_name_config(prefix.map(str::to_owned), schema.map(str::to_owned))
                .expect_err("invalid table identifier should fail");
            let v: serde_json::Value = serde_json::from_str(&err.reason).unwrap();
            assert_eq!(v["_tag"], "ZartSchemaError");
        }
    }

    #[tokio::test]
    async fn clone_with_listener_url_accepts_valid_and_rejects_invalid_url() {
        let pool = PgPool::connect_lazy("postgres://postgres:postgres@localhost:5432/zart")
            .expect("lazy pool creation should succeed");
        let cfg = build_table_name_config(Some("svc_".into()), Some("tenant_a".into()))
            .unwrap()
            .unwrap();
        let backend = assemble_backend(pool, None, Some(cfg)).unwrap();

        assert!(
            clone_with_listener_url(&backend, "postgres://user:pass@pg-direct:5432/zart").is_ok()
        );

        let err = match clone_with_listener_url(&backend, "not a url") {
            Ok(_) => panic!("invalid listener URL should fail"),
            Err(e) => e,
        };
        let v: serde_json::Value = serde_json::from_str(&err.reason).unwrap();
        assert_eq!(v["_tag"], "ZartBackendError");
    }

    #[tokio::test]
    async fn inner_ping_returns_error_when_pool_closed() {
        let pool = PgPool::connect_lazy("postgres://postgres:postgres@localhost:5432/zart")
            .expect("lazy pool creation should succeed");
        let backend = assemble_backend(pool.clone(), None, None).unwrap();
        pool.close().await;

        let err = backend
            .ping()
            .await
            .map_err(|e| backend_err("Ping failed", e))
            .expect_err("ping on closed pool should fail");
        let v: serde_json::Value = serde_json::from_str(&err.reason).unwrap();
        assert_eq!(v["_tag"], "ZartBackendError");
        assert!(v["message"].as_str().unwrap().starts_with("Ping failed:"));
    }
}
