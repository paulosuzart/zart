use std::sync::Arc;
use std::time::Duration;

use napi::bindgen_prelude::*;
use napi_derive::napi;

use crate::backend::PgBackend;
use crate::errors::{backend_err, scheduler_err, schema_err};
use crate::trx::ClientTrx;

fn secs(v: Option<f64>, default: f64, name: &str) -> Result<Duration> {
    Duration::try_from_secs_f64(v.unwrap_or(default))
        .map_err(|e| schema_err(format!("invalid {name}: {e}")))
}

#[napi(object)]
pub struct ScheduleResult {
    pub task_id: String,
    /// RFC3339 timestamp.
    pub execution_time: String,
}

#[napi(object)]
pub struct ExecutionRecord {
    pub execution_id: String,
    pub task_name: String,
    pub payload: serde_json::Value,
    /// scheduled | running | completed | failed | cancelled
    pub status: String,
    pub result: Option<serde_json::Value>,
    pub scheduled_at: String,
    pub completed_at: Option<String>,
    pub version: i32,
}

impl From<zart::ExecutionRecord> for ExecutionRecord {
    fn from(r: zart::ExecutionRecord) -> Self {
        Self {
            execution_id: r.execution_id,
            task_name: r.task_name,
            payload: r.payload,
            status: r.status.to_string(),
            result: r.result,
            scheduled_at: r.scheduled_at.to_rfc3339(),
            completed_at: r.completed_at.map(|d| d.to_rfc3339()),
            version: r.version,
        }
    }
}

#[napi]
pub struct DurableScheduler {
    inner: Arc<zart::DurableScheduler>,
}

#[napi]
impl DurableScheduler {
    #[napi(factory)]
    pub fn from_backend(backend: &PgBackend) -> DurableScheduler {
        DurableScheduler {
            inner: Arc::new(zart::DurableScheduler::from_backend(&*backend.inner)),
        }
    }

    #[napi]
    pub async fn start(
        &self,
        execution_id: String,
        task_name: String,
        payload: serde_json::Value,
    ) -> Result<ScheduleResult> {
        let r = self
            .inner
            .start(&execution_id, &task_name, payload)
            .await
            .map_err(scheduler_err)?;
        Ok(ScheduleResult {
            task_id: r.task_id,
            execution_time: r.execution_time.to_rfc3339(),
        })
    }

    /// Start a new durable execution atomically inside a caller-owned `ClientTrx`.
    #[napi(ts_return_type = "Promise<ScheduleResult>")]
    pub fn start_in_tx(
        &self,
        env: Env,
        tx: &ClientTrx,
        execution_id: String,
        task_name: String,
        payload: serde_json::Value,
    ) -> Result<Object> {
        let inner = self.inner.clone();
        let tx_arc = tx.tx.clone();
        env.spawn_future(async move {
            let mut guard = tx_arc.lock().await;
            let sqlx_tx = guard
                .as_mut()
                .ok_or_else(|| backend_err("startInTx", "transaction is already closed"))?;
            let r = inner
                .start_in_tx(sqlx_tx, &execution_id, &task_name, payload)
                .await
                .map_err(scheduler_err)?;
            Ok(ScheduleResult {
                task_id: r.task_id,
                execution_time: r.execution_time.to_rfc3339(),
            })
        })
    }

    #[napi]
    pub async fn status(&self, execution_id: String) -> Result<ExecutionRecord> {
        self.inner
            .status(&execution_id)
            .await
            .map(Into::into)
            .map_err(scheduler_err)
    }

    #[napi]
    pub async fn wait(
        &self,
        execution_id: String,
        timeout_secs: Option<f64>,
        poll_interval_secs: Option<f64>,
    ) -> Result<ExecutionRecord> {
        let t = secs(timeout_secs, 30.0, "timeoutSecs")?;
        let p = poll_interval_secs
            .map(|s| secs(Some(s), 0.5, "pollIntervalSecs"))
            .transpose()?;
        self.inner
            .wait(&execution_id, t, p)
            .await
            .map(Into::into)
            .map_err(scheduler_err)
    }

    #[napi]
    pub async fn wait_completion(
        &self,
        execution_id: String,
        timeout_secs: Option<f64>,
        poll_interval_secs: Option<f64>,
    ) -> Result<serde_json::Value> {
        let t = secs(timeout_secs, 30.0, "timeoutSecs")?;
        let p = poll_interval_secs
            .map(|s| secs(Some(s), 0.5, "pollIntervalSecs"))
            .transpose()?;
        self.inner
            .wait_completion(&execution_id, t, p)
            .await
            .map_err(scheduler_err)
    }

    #[napi]
    pub async fn cancel(&self, execution_id: String) -> Result<bool> {
        self.inner
            .cancel(&execution_id)
            .await
            .map_err(scheduler_err)
    }

    #[napi]
    pub async fn offer_event(
        &self,
        execution_id: String,
        event_name: String,
        payload: serde_json::Value,
    ) -> Result<()> {
        self.inner
            .offer_event(&execution_id, &event_name, payload)
            .await
            .map_err(scheduler_err)
    }
}
