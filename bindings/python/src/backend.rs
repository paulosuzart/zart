use std::sync::Arc;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use sqlx::PgPool;

use crate::trx::PyClientTrx;

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
    /// when the main pool connects through a transaction-mode pooler.
    #[staticmethod]
    #[pyo3(signature = (url, listener_url = None))]
    fn connect<'py>(
        py: Python<'py>,
        url: String,
        listener_url: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let pool = PgPool::connect(&url)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to connect to PostgreSQL: {e}")))?;
            let mut backend = zart::PgBackend::new(pool);
            if let Some(l_url) = listener_url {
                backend = backend
                    .with_listener_url(&l_url)
                    .map_err(|e| PyRuntimeError::new_err(format!("Invalid listener URL: {e}")))?;
            }
            Ok(PyPgBackend {
                inner: Arc::new(backend),
            })
        })
    }

    /// Return a new `PgBackend` sharing the same pool with a dedicated `LISTEN/NOTIFY` URL.
    fn with_listener_url(&self, url: &str) -> PyResult<Self> {
        let pool = self.inner.pool().clone();
        let backend = zart::PgBackend::new(pool)
            .with_listener_url(url)
            .map_err(|e| PyRuntimeError::new_err(format!("Invalid listener URL: {e}")))?;
        Ok(Self {
            inner: Arc::new(backend),
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
            let tx = pool
                .begin()
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to begin transaction: {e}")))?;
            Ok(PyClientTrx::new(tx))
        })
    }
}
