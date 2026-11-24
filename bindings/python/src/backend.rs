use std::str::FromStr;
use std::sync::Arc;

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use sqlx::PgPool;
use sqlx::postgres::PgConnectOptions;
use zart_core::table_names::TableNameConfig;

use crate::trx::PyClientTrx;

/// Build and validate custom [`zart::postgres::TableNames`] when `table_prefix` or `schema` is set.
fn build_table_names(
    table_prefix: Option<String>,
    schema: Option<String>,
) -> PyResult<Option<zart::postgres::TableNames>> {
    if table_prefix.is_none() && schema.is_none() {
        return Ok(None);
    }
    let mut config = match table_prefix {
        Some(prefix) => TableNameConfig::with_prefix(prefix)
            .map_err(|e| PyValueError::new_err(e.to_string()))?,
        None => TableNameConfig::default(),
    };
    if let Some(s) = schema {
        config = config
            .with_schema(s)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
    }
    Ok(Some(zart::postgres::TableNames::from_config(config)))
}

/// Parse and validate an optional `LISTEN/NOTIFY` PostgreSQL URL before opening a pool.
fn parse_listener_options(listener_url: Option<&str>) -> PyResult<Option<PgConnectOptions>> {
    listener_url
        .map(|url| {
            PgConnectOptions::from_str(url)
                .map_err(|e| PyRuntimeError::new_err(format!("Invalid listener URL: {e}")))
        })
        .transpose()
}

/// Construct a [`PyPgBackend`] from `pool` with optional table isolation and listener options.
fn build_backend(
    pool: PgPool,
    listener_options: Option<PgConnectOptions>,
    custom_names: Option<zart::postgres::TableNames>,
) -> PyPgBackend {
    let mut backend = match custom_names {
        Some(names) => zart::PgBackend::with_table_names(pool, names),
        None => zart::PgBackend::new(pool),
    };
    if let Some(opts) = listener_options {
        backend = backend.with_listener_options(opts);
    }
    PyPgBackend {
        inner: Arc::new(backend),
    }
}

/// PostgreSQL-backed storage and scheduler entry point for Zart.
#[pyclass(name = "PgBackend", frozen)]
#[derive(Clone)]
pub struct PyPgBackend {
    pub(crate) inner: Arc<zart::PgBackend>,
}

#[pymethods]
impl PyPgBackend {
    /// Connect to PostgreSQL and create a new `PgBackend`.
    ///
    /// Optionally configures a dedicated `listener_url` for `LISTEN/NOTIFY`
    /// when the main pool connects through a transaction-mode pooler, as well
    /// as custom `table_prefix` and/or `schema` PostgreSQL identifiers.
    #[staticmethod]
    #[pyo3(signature = (url, listener_url = None, table_prefix = None, schema = None))]
    fn connect<'py>(
        py: Python<'py>,
        url: String,
        listener_url: Option<String>,
        table_prefix: Option<String>,
        schema: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let custom_names = build_table_names(table_prefix, schema)?;
        let listener_opts = parse_listener_options(listener_url.as_deref())?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let pool = PgPool::connect(&url).await.map_err(|e| {
                PyRuntimeError::new_err(format!("Failed to connect to PostgreSQL: {e}"))
            })?;
            Ok(build_backend(pool, listener_opts, custom_names))
        })
    }

    /// Create a lazy `PgBackend` without opening an immediate database connection (for unit tests).
    #[staticmethod]
    #[pyo3(signature = (url, listener_url = None, table_prefix = None, schema = None))]
    fn _connect_lazy(
        url: &str,
        listener_url: Option<&str>,
        table_prefix: Option<String>,
        schema: Option<String>,
    ) -> PyResult<Self> {
        let custom_names = build_table_names(table_prefix, schema)?;
        let listener_opts = parse_listener_options(listener_url)?;
        let _rt_guard = pyo3_async_runtimes::tokio::get_runtime().enter();
        let pool = PgPool::connect_lazy(url)
            .map_err(|e| PyValueError::new_err(format!("Invalid PostgreSQL URL: {e}")))?;
        Ok(build_backend(pool, listener_opts, custom_names))
    }

    /// Return a new `PgBackend` sharing the same pool and table names with a dedicated `LISTEN/NOTIFY` URL.
    fn with_listener_url(&self, url: &str) -> PyResult<Self> {
        let backend = self
            .inner
            .as_ref()
            .clone()
            .with_listener_url(url)
            .map_err(|e| PyRuntimeError::new_err(format!("Invalid listener URL: {e}")))?;
        Ok(Self {
            inner: Arc::new(backend),
        })
    }

    /// Verify database connectivity by executing `SELECT 1`.
    fn ping<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let backend = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            backend
                .ping()
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("Ping failed: {e}")))?;
            Ok(())
        })
    }

    /// Run all Zart database migrations against the connected PostgreSQL database.
    fn run_migrations<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let backend = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            backend
                .run_migrations()
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("Migration failed: {e}")))?;
            Ok(())
        })
    }

    /// Close the underlying PostgreSQL connection pool.
    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let backend = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            backend.pool().close().await;
            Ok(())
        })
    }

    /// Begin a caller-owned PostgreSQL transaction for atomic `start_in_tx` calls.
    fn begin<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.inner.pool().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let tx = pool.begin().await.map_err(|e| {
                PyRuntimeError::new_err(format!("Failed to begin transaction: {e}"))
            })?;
            Ok(PyClientTrx::new(tx))
        })
    }
}
