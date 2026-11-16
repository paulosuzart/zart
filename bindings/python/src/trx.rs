use std::sync::Arc;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyTuple};
use sqlx::postgres::{PgArguments, PgRow};
use sqlx::{Arguments, Column, Executor, Row, TypeInfo};
use tokio::sync::Mutex;

use crate::serde_bridge::{json_to_py, py_to_json};

fn bind_py_args(args: &Bound<'_, PyTuple>) -> PyResult<PgArguments> {
    let mut pg_args = PgArguments::default();
    for item in args.iter() {
        bind_one_arg(&mut pg_args, &item)?;
    }
    Ok(pg_args)
}

fn bind_one_arg(pg_args: &mut PgArguments, item: &Bound<'_, PyAny>) -> PyResult<()> {
    let py = item.py();
    if item.is_none() {
        pg_args
            .add(Option::<String>::None)
            .map_err(|e| PyRuntimeError::new_err(format!("SQL bind error: {e}")))?;
        return Ok(());
    }
    if item.is_instance_of::<PyBool>() {
        let v: bool = item.extract()?;
        pg_args
            .add(v)
            .map_err(|e| PyRuntimeError::new_err(format!("SQL bind error: {e}")))?;
        return Ok(());
    }
    if let Ok(v) = item.extract::<i64>() {
        pg_args
            .add(v)
            .map_err(|e| PyRuntimeError::new_err(format!("SQL bind error: {e}")))?;
        return Ok(());
    }
    if let Ok(v) = item.extract::<f64>() {
        pg_args
            .add(v)
            .map_err(|e| PyRuntimeError::new_err(format!("SQL bind error: {e}")))?;
        return Ok(());
    }
    if let Ok(v) = item.extract::<String>() {
        pg_args
            .add(v)
            .map_err(|e| PyRuntimeError::new_err(format!("SQL bind error: {e}")))?;
        return Ok(());
    }
    if let Ok(v) = item.extract::<Vec<u8>>() {
        pg_args
            .add(v)
            .map_err(|e| PyRuntimeError::new_err(format!("SQL bind error: {e}")))?;
        return Ok(());
    }

    let uuid_cls = py.import("uuid")?.getattr("UUID")?;
    if item.is_instance(&uuid_cls)? {
        let s: String = item.str()?.extract()?;
        let u = sqlx::types::Uuid::parse_str(&s)
            .map_err(|e| PyRuntimeError::new_err(format!("Invalid UUID: {e}")))?;
        pg_args
            .add(u)
            .map_err(|e| PyRuntimeError::new_err(format!("SQL bind error: {e}")))?;
        return Ok(());
    }

    let dt_cls = py.import("datetime")?.getattr("datetime")?;
    if item.is_instance(&dt_cls)? {
        let iso: String = item.call_method0("isoformat")?.extract()?;
        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&iso) {
            pg_args
                .add(dt.with_timezone(&chrono::Utc))
                .map_err(|e| PyRuntimeError::new_err(format!("SQL bind error: {e}")))?;
        } else {
            let naive = chrono::NaiveDateTime::parse_from_str(&iso, "%Y-%m-%dT%H:%M:%S%.f")
                .or_else(|_| chrono::NaiveDateTime::parse_from_str(&iso, "%Y-%m-%dT%H:%M:%S"))
                .map_err(|e| PyRuntimeError::new_err(format!("Invalid datetime: {e}")))?;
            pg_args
                .add(naive.and_utc())
                .map_err(|e| PyRuntimeError::new_err(format!("SQL bind error: {e}")))?;
        }
        return Ok(());
    }

    let json_val = py_to_json(item)?;
    pg_args
        .add(sqlx::types::Json(json_val))
        .map_err(|e| PyRuntimeError::new_err(format!("SQL bind error: {e}")))?;
    Ok(())
}

fn row_to_py_dict(py: Python<'_>, row: &PgRow) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    for (idx, col) in row.columns().iter().enumerate() {
        let name = col.name();
        let type_name = col.type_info().name();
        let val: Py<PyAny> = match type_name {
            "BOOL" => match row.try_get::<Option<bool>, _>(idx) {
                Ok(Some(v)) => v.into_pyobject(py)?.to_owned().into_any().unbind(),
                _ => py.None(),
            },
            "INT2" => match row.try_get::<Option<i16>, _>(idx) {
                Ok(Some(v)) => v.into_pyobject(py)?.into_any().unbind(),
                _ => py.None(),
            },
            "INT4" => match row.try_get::<Option<i32>, _>(idx) {
                Ok(Some(v)) => v.into_pyobject(py)?.into_any().unbind(),
                _ => py.None(),
            },
            "INT8" => match row.try_get::<Option<i64>, _>(idx) {
                Ok(Some(v)) => v.into_pyobject(py)?.into_any().unbind(),
                _ => py.None(),
            },
            "FLOAT4" => match row.try_get::<Option<f32>, _>(idx) {
                Ok(Some(v)) => v.into_pyobject(py)?.into_any().unbind(),
                _ => py.None(),
            },
            "FLOAT8" => match row.try_get::<Option<f64>, _>(idx) {
                Ok(Some(v)) => v.into_pyobject(py)?.into_any().unbind(),
                _ => py.None(),
            },
            "UUID" => match row.try_get::<Option<sqlx::types::Uuid>, _>(idx) {
                Ok(Some(u)) => u.to_string().into_pyobject(py)?.into_any().unbind(),
                _ => py.None(),
            },
            "TIMESTAMPTZ" => match row.try_get::<Option<chrono::DateTime<chrono::Utc>>, _>(idx) {
                Ok(Some(dt)) => dt.to_rfc3339().into_pyobject(py)?.into_any().unbind(),
                _ => py.None(),
            },
            "TIMESTAMP" => match row.try_get::<Option<chrono::NaiveDateTime>, _>(idx) {
                Ok(Some(dt)) => dt.to_string().into_pyobject(py)?.into_any().unbind(),
                _ => py.None(),
            },
            "DATE" => match row.try_get::<Option<chrono::NaiveDate>, _>(idx) {
                Ok(Some(d)) => d.to_string().into_pyobject(py)?.into_any().unbind(),
                _ => py.None(),
            },
            "JSON" | "JSONB" => match row.try_get::<Option<serde_json::Value>, _>(idx) {
                Ok(Some(v)) => json_to_py(py, &v)?.unbind(),
                _ => py.None(),
            },
            "BYTEA" => match row.try_get::<Option<Vec<u8>>, _>(idx) {
                Ok(Some(b)) => PyBytes::new(py, &b).into_any().unbind(),
                _ => py.None(),
            },
            _ => {
                if let Ok(opt) = row.try_get::<Option<String>, _>(idx) {
                    match opt {
                        Some(s) => s.into_pyobject(py)?.into_any().unbind(),
                        None => py.None(),
                    }
                } else if let Ok(opt) = row.try_get::<Option<serde_json::Value>, _>(idx) {
                    match opt {
                        Some(v) => json_to_py(py, &v)?.unbind(),
                        None => py.None(),
                    }
                } else {
                    py.None()
                }
            }
        };
        dict.set_item(name, val)?;
    }
    Ok(dict.into_any().unbind())
}

async fn exec_on_conn(
    conn: &mut sqlx::PgConnection,
    sql: &str,
    args: PgArguments,
) -> PyResult<u64> {
    let res = conn
        .execute(sqlx::query_with::<sqlx::Postgres, _>(
            sqlx::AssertSqlSafe(sql.to_string()),
            args,
        ))
        .await
        .map_err(|e| PyRuntimeError::new_err(format!("SQL execute failed: {e}")))?;
    Ok(res.rows_affected())
}

async fn fetch_one_on_conn(
    conn: &mut sqlx::PgConnection,
    sql: &str,
    args: PgArguments,
) -> PyResult<Py<PyAny>> {
    let row = sqlx::query_with::<sqlx::Postgres, _>(
        sqlx::AssertSqlSafe(sql.to_string()),
        args,
    )
    .fetch_one(conn)
    .await
    .map_err(|e| PyRuntimeError::new_err(format!("SQL fetch_one failed: {e}")))?;
    Python::attach(|py| row_to_py_dict(py, &row))
}

async fn fetch_optional_on_conn(
    conn: &mut sqlx::PgConnection,
    sql: &str,
    args: PgArguments,
) -> PyResult<Option<Py<PyAny>>> {
    let row = sqlx::query_with::<sqlx::Postgres, _>(
        sqlx::AssertSqlSafe(sql.to_string()),
        args,
    )
    .fetch_optional(conn)
    .await
    .map_err(|e| PyRuntimeError::new_err(format!("SQL fetch_optional failed: {e}")))?;
    match row {
        Some(r) => Python::attach(|py| row_to_py_dict(py, &r).map(Some)),
        None => Ok(None),
    }
}

async fn fetch_all_on_conn(
    conn: &mut sqlx::PgConnection,
    sql: &str,
    args: PgArguments,
) -> PyResult<Vec<Py<PyAny>>> {
    let rows = sqlx::query_with::<sqlx::Postgres, _>(
        sqlx::AssertSqlSafe(sql.to_string()),
        args,
    )
    .fetch_all(conn)
    .await
    .map_err(|e| PyRuntimeError::new_err(format!("SQL fetch_all failed: {e}")))?;
    Python::attach(|py| rows.iter().map(|r| row_to_py_dict(py, r)).collect())
}

trait AsPgConn: Send + 'static {
    fn as_pg_conn(&mut self) -> &mut sqlx::PgConnection;
}

impl AsPgConn for zart::ZartTrx {
    fn as_pg_conn(&mut self) -> &mut sqlx::PgConnection {
        self
    }
}

impl AsPgConn for sqlx::Transaction<'static, sqlx::Postgres> {
    fn as_pg_conn(&mut self) -> &mut sqlx::PgConnection {
        self
    }
}

fn dispatch_trx_query<'py, C, F, R>(
    py: Python<'py>,
    inner: &Arc<Mutex<Option<C>>>,
    sql: String,
    args: &Bound<'py, PyTuple>,
    op: F,
) -> PyResult<Bound<'py, PyAny>>
where
    C: AsPgConn,
    F: for<'a> FnOnce(
            &'a mut sqlx::PgConnection,
            &'a str,
            PgArguments,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = PyResult<R>> + Send + 'a>>
        + Send
        + 'static,
    R: for<'a> pyo3::IntoPyObject<'a> + Send + 'static,
    for<'a> <R as pyo3::IntoPyObject<'a>>::Output: pyo3::IntoPyObject<'a>,
    for<'a> <R as pyo3::IntoPyObject<'a>>::Error: Into<PyErr>,
{
    let pg_args = bind_py_args(args)?;
    let inner = inner.clone();
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let mut guard = inner.lock().await;
        let conn = guard
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("Transaction is already closed"))?;
        op(conn.as_pg_conn(), &sql, pg_args).await
    })
}

/// Zart-managed step transaction handle (`zart::ZartTrx`).
///
/// Automatically commits atomically with Zart's step completion record when
/// the step succeeds, or rolls back if the step fails.
#[pyclass(name = "Trx", frozen)]
pub struct PyTrx {
    pub(crate) inner: Arc<Mutex<Option<zart::ZartTrx>>>,
}

impl PyTrx {
    pub(crate) fn new(trx: zart::ZartTrx) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Some(trx))),
        }
    }
}

#[pymethods]
impl PyTrx {
    #[pyo3(signature = (sql, *args))]
    fn execute<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        args: &Bound<'py, PyTuple>,
    ) -> PyResult<Bound<'py, PyAny>> {
        dispatch_trx_query(py, &self.inner, sql, args, |c, s, a| {
            Box::pin(exec_on_conn(c, s, a))
        })
    }

    #[pyo3(signature = (sql, *args))]
    fn fetch_one<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        args: &Bound<'py, PyTuple>,
    ) -> PyResult<Bound<'py, PyAny>> {
        dispatch_trx_query(py, &self.inner, sql, args, |c, s, a| {
            Box::pin(fetch_one_on_conn(c, s, a))
        })
    }

    #[pyo3(signature = (sql, *args))]
    fn fetch_optional<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        args: &Bound<'py, PyTuple>,
    ) -> PyResult<Bound<'py, PyAny>> {
        dispatch_trx_query(py, &self.inner, sql, args, |c, s, a| {
            Box::pin(fetch_optional_on_conn(c, s, a))
        })
    }

    #[pyo3(signature = (sql, *args))]
    fn fetch_all<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        args: &Bound<'py, PyTuple>,
    ) -> PyResult<Bound<'py, PyAny>> {
        dispatch_trx_query(py, &self.inner, sql, args, |c, s, a| {
            Box::pin(fetch_all_on_conn(c, s, a))
        })
    }
}

/// Caller-owned PostgreSQL transaction for atomic `start_in_tx` operations.
#[pyclass(name = "ClientTrx", frozen)]
pub struct PyClientTrx {
    pub(crate) inner: Arc<Mutex<Option<sqlx::Transaction<'static, sqlx::Postgres>>>>,
}

impl PyClientTrx {
    pub(crate) fn new(tx: sqlx::Transaction<'static, sqlx::Postgres>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Some(tx))),
        }
    }
}

#[pymethods]
impl PyClientTrx {
    #[pyo3(signature = (sql, *args))]
    fn execute<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        args: &Bound<'py, PyTuple>,
    ) -> PyResult<Bound<'py, PyAny>> {
        dispatch_trx_query(py, &self.inner, sql, args, |c, s, a| {
            Box::pin(exec_on_conn(c, s, a))
        })
    }

    #[pyo3(signature = (sql, *args))]
    fn fetch_one<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        args: &Bound<'py, PyTuple>,
    ) -> PyResult<Bound<'py, PyAny>> {
        dispatch_trx_query(py, &self.inner, sql, args, |c, s, a| {
            Box::pin(fetch_one_on_conn(c, s, a))
        })
    }

    #[pyo3(signature = (sql, *args))]
    fn fetch_optional<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        args: &Bound<'py, PyTuple>,
    ) -> PyResult<Bound<'py, PyAny>> {
        dispatch_trx_query(py, &self.inner, sql, args, |c, s, a| {
            Box::pin(fetch_optional_on_conn(c, s, a))
        })
    }

    #[pyo3(signature = (sql, *args))]
    fn fetch_all<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        args: &Bound<'py, PyTuple>,
    ) -> PyResult<Bound<'py, PyAny>> {
        dispatch_trx_query(py, &self.inner, sql, args, |c, s, a| {
            Box::pin(fetch_all_on_conn(c, s, a))
        })
    }

    /// Commit the transaction.
    fn commit<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let tx = inner
                .lock()
                .await
                .take()
                .ok_or_else(|| PyRuntimeError::new_err("Transaction is already closed"))?;
            tx.commit()
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("Transaction commit failed: {e}")))?;
            Ok(())
        })
    }

    /// Roll back the transaction.
    fn rollback<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let tx = inner
                .lock()
                .await
                .take()
                .ok_or_else(|| PyRuntimeError::new_err("Transaction is already closed"))?;
            tx.rollback()
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("Transaction rollback failed: {e}")))?;
            Ok(())
        })
    }

    fn __aenter__<'py>(slf: Py<Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, async move { Ok(slf) })
    }

    #[pyo3(signature = (exc_type = None, _exc_val = None, _exc_tb = None))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        exc_type: Option<Py<PyAny>>,
        _exc_val: Option<Py<PyAny>>,
        _exc_tb: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let had_exc = exc_type.is_some();
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Some(tx) = inner.lock().await.take() {
                if had_exc {
                    tx.rollback().await.map_err(|e| {
                        PyRuntimeError::new_err(format!("Transaction rollback failed: {e}"))
                    })?;
                } else {
                    tx.commit().await.map_err(|e| {
                        PyRuntimeError::new_err(format!("Transaction commit failed: {e}"))
                    })?;
                }
            }
            Ok(false)
        })
    }
}
