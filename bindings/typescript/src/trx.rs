//! SQL transaction handles (`NativeTrx` for Zart-managed step transactions and
//! `ClientTrx` for caller-owned transactions).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use napi::bindgen_prelude::*;
use napi::{JsBigInt, JsBuffer, JsDate, JsObject, JsTypedArray, JsUnknown, ValueType};
use napi_derive::napi;
use sqlx::postgres::{PgArguments, PgRow};
use sqlx::{Arguments, Column, Executor, Row, TypeInfo};
use tokio::sync::Mutex;

use crate::errors::{backend_err, schema_err};
use crate::serde_bridge::{bytes_to_json, datetime_to_json, i64_to_json};

const MAX_SAFE_INT: f64 = 9_007_199_254_740_991.0;

fn bind_err(e: impl std::fmt::Display) -> napi::Error {
    schema_err(format!("SQL bind error: {e}"))
}

fn bind_js_args(env: &Env, params: Option<Vec<JsUnknown>>) -> Result<PgArguments> {
    let mut pg_args = PgArguments::default();
    if let Some(items) = params {
        for item in items {
            bind_one_arg(env, &mut pg_args, item)?;
        }
    }
    Ok(pg_args)
}

fn bind_one_arg(env: &Env, pg_args: &mut PgArguments, item: JsUnknown) -> Result<()> {
    match item.get_type()? {
        ValueType::Null | ValueType::Undefined => {
            pg_args.add(Option::<String>::None).map_err(bind_err)?;
        }
        ValueType::Boolean => {
            let v = item.coerce_to_bool()?.get_value()?;
            pg_args.add(v).map_err(bind_err)?;
        }
        ValueType::Number => {
            let n = item.coerce_to_number()?.get_double()?;
            if n.is_finite() && n.fract() == 0.0 && (-MAX_SAFE_INT..=MAX_SAFE_INT).contains(&n) {
                pg_args.add(n as i64).map_err(bind_err)?;
            } else {
                pg_args.add(n).map_err(bind_err)?;
            }
        }
        ValueType::BigInt => {
            let bi = JsBigInt::from_unknown(item)?;
            let (v, lossless) = bi.get_i64()?;
            if !lossless {
                return Err(schema_err("BigInt value out of i64 range"));
            }
            pg_args.add(v).map_err(bind_err)?;
        }
        ValueType::String => {
            let s = item.coerce_to_string()?.into_utf8()?.into_owned()?;
            pg_args.add(s).map_err(bind_err)?;
        }
        ValueType::Object => {
            if item.is_buffer()? {
                let buf = JsBuffer::from_unknown(item)?.into_value()?;
                pg_args.add(buf.to_vec()).map_err(bind_err)?;
            } else if item.is_typedarray()? {
                let ta = JsTypedArray::from_unknown(item)?.into_value()?;
                pg_args
                    .add(AsRef::<[u8]>::as_ref(&ta).to_vec())
                    .map_err(bind_err)?;
            } else if item.is_date()? {
                let d = JsDate::from_unknown(item)?;
                let ms = d.value_of()?;
                let dt = chrono::DateTime::from_timestamp_millis(ms as i64)
                    .ok_or_else(|| schema_err(format!("Invalid Date timestamp: {ms}")))?;
                pg_args.add(dt).map_err(bind_err)?;
            } else {
                let json: serde_json::Value = env.from_js_value(item)?;
                pg_args.add(sqlx::types::Json(json)).map_err(bind_err)?;
            }
        }
        other => {
            return Err(schema_err(format!(
                "Unsupported SQL parameter type: {other:?}"
            )));
        }
    }
    Ok(())
}

fn row_to_json(row: &PgRow) -> serde_json::Value {
    let mut map = serde_json::Map::with_capacity(row.columns().len());
    for (idx, col) in row.columns().iter().enumerate() {
        let name = col.name().to_string();
        let type_name = col.type_info().name();
        let val = match type_name {
            "BOOL" => row
                .try_get::<Option<bool>, _>(idx)
                .ok()
                .flatten()
                .map_or(serde_json::Value::Null, serde_json::Value::Bool),
            "INT2" => row
                .try_get::<Option<i16>, _>(idx)
                .ok()
                .flatten()
                .map_or(serde_json::Value::Null, serde_json::Value::from),
            "INT4" => row
                .try_get::<Option<i32>, _>(idx)
                .ok()
                .flatten()
                .map_or(serde_json::Value::Null, serde_json::Value::from),
            "INT8" => row
                .try_get::<Option<i64>, _>(idx)
                .ok()
                .flatten()
                .map_or(serde_json::Value::Null, i64_to_json),
            "FLOAT4" => row
                .try_get::<Option<f32>, _>(idx)
                .ok()
                .flatten()
                .map_or(serde_json::Value::Null, serde_json::Value::from),
            "FLOAT8" => row
                .try_get::<Option<f64>, _>(idx)
                .ok()
                .flatten()
                .map_or(serde_json::Value::Null, serde_json::Value::from),
            "UUID" => row
                .try_get::<Option<sqlx::types::Uuid>, _>(idx)
                .ok()
                .flatten()
                .map_or(serde_json::Value::Null, |u| {
                    serde_json::Value::String(u.to_string())
                }),
            "TIMESTAMPTZ" => row
                .try_get::<Option<chrono::DateTime<chrono::Utc>>, _>(idx)
                .ok()
                .flatten()
                .map_or(serde_json::Value::Null, |dt| datetime_to_json(&dt)),
            "TIMESTAMP" => row
                .try_get::<Option<chrono::NaiveDateTime>, _>(idx)
                .ok()
                .flatten()
                .map_or(serde_json::Value::Null, |dt| {
                    serde_json::Value::String(dt.to_string())
                }),
            "DATE" => row
                .try_get::<Option<chrono::NaiveDate>, _>(idx)
                .ok()
                .flatten()
                .map_or(serde_json::Value::Null, |d| {
                    serde_json::Value::String(d.to_string())
                }),
            "JSON" | "JSONB" => row
                .try_get::<Option<serde_json::Value>, _>(idx)
                .ok()
                .flatten()
                .unwrap_or(serde_json::Value::Null),
            "BYTEA" => row
                .try_get::<Option<Vec<u8>>, _>(idx)
                .ok()
                .flatten()
                .map_or(serde_json::Value::Null, |b| bytes_to_json(&b)),
            _ => {
                if let Ok(opt) = row.try_get::<Option<String>, _>(idx) {
                    opt.map_or(serde_json::Value::Null, serde_json::Value::String)
                } else if let Ok(opt) = row.try_get::<Option<serde_json::Value>, _>(idx) {
                    opt.unwrap_or(serde_json::Value::Null)
                } else {
                    serde_json::Value::Null
                }
            }
        };
        map.insert(name, val);
    }
    serde_json::Value::Object(map)
}

async fn exec_on_conn(conn: &mut sqlx::PgConnection, sql: &str, args: PgArguments) -> Result<f64> {
    let res = conn
        .execute(sqlx::query_with::<sqlx::Postgres, _>(
            sqlx::AssertSqlSafe(sql.to_string()),
            args,
        ))
        .await
        .map_err(|e| backend_err("SQL execute failed", e))?;
    Ok(res.rows_affected() as f64)
}

async fn fetch_one_on_conn(
    conn: &mut sqlx::PgConnection,
    sql: &str,
    args: PgArguments,
) -> Result<serde_json::Value> {
    let row = sqlx::query_with::<sqlx::Postgres, _>(sqlx::AssertSqlSafe(sql.to_string()), args)
        .fetch_one(conn)
        .await
        .map_err(|e| backend_err("SQL fetchOne failed", e))?;
    Ok(row_to_json(&row))
}

async fn fetch_optional_on_conn(
    conn: &mut sqlx::PgConnection,
    sql: &str,
    args: PgArguments,
) -> Result<Option<serde_json::Value>> {
    let row = sqlx::query_with::<sqlx::Postgres, _>(sqlx::AssertSqlSafe(sql.to_string()), args)
        .fetch_optional(conn)
        .await
        .map_err(|e| backend_err("SQL fetchOptional failed", e))?;
    Ok(row.as_ref().map(row_to_json))
}

async fn fetch_all_on_conn(
    conn: &mut sqlx::PgConnection,
    sql: &str,
    args: PgArguments,
) -> Result<Vec<serde_json::Value>> {
    let rows = sqlx::query_with::<sqlx::Postgres, _>(sqlx::AssertSqlSafe(sql.to_string()), args)
        .fetch_all(conn)
        .await
        .map_err(|e| backend_err("SQL fetchAll failed", e))?;
    Ok(rows.iter().map(row_to_json).collect())
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

fn dispatch_query<C, F, R>(
    env: Env,
    inner: &Arc<Mutex<Option<C>>>,
    sql: String,
    params: Option<Vec<JsUnknown>>,
    op: F,
) -> Result<JsObject>
where
    C: AsPgConn,
    F: for<'a> FnOnce(
            &'a mut sqlx::PgConnection,
            &'a str,
            PgArguments,
        ) -> Pin<Box<dyn Future<Output = Result<R>> + Send + 'a>>
        + Send
        + 'static,
    R: ToNapiValue + Send + 'static,
{
    let pg_args = bind_js_args(&env, params)?;
    let inner = inner.clone();
    env.spawn_future(async move {
        let mut guard = inner.lock().await;
        let conn = guard
            .as_mut()
            .ok_or_else(|| backend_err("trx", "transaction is already closed"))?;
        op(conn.as_pg_conn(), &sql, pg_args).await
    })
}

/// Zart-managed step transaction handle (`zart::ZartTrx`).
#[napi(js_name = "NativeTrx")]
pub struct NativeTrx {
    pub(crate) inner: Arc<Mutex<Option<zart::ZartTrx>>>,
}

impl NativeTrx {
    pub(crate) fn from_arc(inner: Arc<Mutex<Option<zart::ZartTrx>>>) -> Self {
        Self { inner }
    }
}

#[napi]
impl NativeTrx {
    #[napi(ts_return_type = "Promise<number>")]
    pub fn execute(
        &self,
        env: Env,
        sql: String,
        params: Option<Vec<JsUnknown>>,
    ) -> Result<JsObject> {
        dispatch_query(env, &self.inner, sql, params, |c, s, a| {
            Box::pin(exec_on_conn(c, s, a))
        })
    }

    #[napi(ts_return_type = "Promise<Record<string, unknown>>")]
    pub fn fetch_one(
        &self,
        env: Env,
        sql: String,
        params: Option<Vec<JsUnknown>>,
    ) -> Result<JsObject> {
        dispatch_query(env, &self.inner, sql, params, |c, s, a| {
            Box::pin(fetch_one_on_conn(c, s, a))
        })
    }

    #[napi(ts_return_type = "Promise<Record<string, unknown> | null>")]
    pub fn fetch_optional(
        &self,
        env: Env,
        sql: String,
        params: Option<Vec<JsUnknown>>,
    ) -> Result<JsObject> {
        dispatch_query(env, &self.inner, sql, params, |c, s, a| {
            Box::pin(fetch_optional_on_conn(c, s, a))
        })
    }

    #[napi(ts_return_type = "Promise<Array<Record<string, unknown>>>")]
    pub fn fetch_all(
        &self,
        env: Env,
        sql: String,
        params: Option<Vec<JsUnknown>>,
    ) -> Result<JsObject> {
        dispatch_query(env, &self.inner, sql, params, |c, s, a| {
            Box::pin(fetch_all_on_conn(c, s, a))
        })
    }
}

/// Caller-owned PostgreSQL transaction handle.
#[napi]
pub struct ClientTrx {
    pub(crate) tx: Arc<Mutex<Option<sqlx::Transaction<'static, sqlx::Postgres>>>>,
}

impl ClientTrx {
    pub(crate) fn new(tx: sqlx::Transaction<'static, sqlx::Postgres>) -> Self {
        Self {
            tx: Arc::new(Mutex::new(Some(tx))),
        }
    }
}

#[napi]
impl ClientTrx {
    #[napi(ts_return_type = "Promise<number>")]
    pub fn execute(
        &self,
        env: Env,
        sql: String,
        params: Option<Vec<JsUnknown>>,
    ) -> Result<JsObject> {
        dispatch_query(env, &self.tx, sql, params, |c, s, a| {
            Box::pin(exec_on_conn(c, s, a))
        })
    }

    #[napi(ts_return_type = "Promise<Record<string, unknown>>")]
    pub fn fetch_one(
        &self,
        env: Env,
        sql: String,
        params: Option<Vec<JsUnknown>>,
    ) -> Result<JsObject> {
        dispatch_query(env, &self.tx, sql, params, |c, s, a| {
            Box::pin(fetch_one_on_conn(c, s, a))
        })
    }

    #[napi(ts_return_type = "Promise<Record<string, unknown> | null>")]
    pub fn fetch_optional(
        &self,
        env: Env,
        sql: String,
        params: Option<Vec<JsUnknown>>,
    ) -> Result<JsObject> {
        dispatch_query(env, &self.tx, sql, params, |c, s, a| {
            Box::pin(fetch_optional_on_conn(c, s, a))
        })
    }

    #[napi(ts_return_type = "Promise<Array<Record<string, unknown>>>")]
    pub fn fetch_all(
        &self,
        env: Env,
        sql: String,
        params: Option<Vec<JsUnknown>>,
    ) -> Result<JsObject> {
        dispatch_query(env, &self.tx, sql, params, |c, s, a| {
            Box::pin(fetch_all_on_conn(c, s, a))
        })
    }

    #[napi]
    pub async fn commit(&self) -> Result<()> {
        let tx = self
            .tx
            .lock()
            .await
            .take()
            .ok_or_else(|| backend_err("commit", "transaction already finished"))?;
        tx.commit()
            .await
            .map_err(|e| backend_err("Commit failed", e))
    }

    #[napi]
    pub async fn rollback(&self) -> Result<()> {
        let tx = self
            .tx
            .lock()
            .await
            .take()
            .ok_or_else(|| backend_err("rollback", "transaction already finished"))?;
        tx.rollback()
            .await
            .map_err(|e| backend_err("Rollback failed", e))
    }
}
