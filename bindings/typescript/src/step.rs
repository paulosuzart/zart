//! JS step specs bridged to `zart::ZartStep`.

use std::borrow::Cow;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use napi::bindgen_prelude::*;
use napi::threadsafe_function::{ErrorStrategy, ThreadsafeFunction};
use napi_derive::napi;
use tokio::sync::Mutex;

use crate::errors::{StepOutcome, TsStepError, reason_message, schema_err};
use crate::trx::NativeTrx;

/// JS callback invoked once per attempt; resolves to a `StepOutcome`.
pub type RunFn = ThreadsafeFunction<Option<NativeTrx>, ErrorStrategy::Fatal>;

/// Retry policy: `kind` is `none | fixed | linear | exponential`.
#[napi(object)]
#[derive(Clone)]
pub struct RetrySpec {
    pub kind: String,
    pub max_attempts: u32,
    pub delay_ms: f64,
    pub factor: Option<f64>,
    pub max_delay_ms: Option<f64>,
}

/// Step options. `timeoutScope` is `attempt | total` (also `per_attempt | global`).
#[napi(object)]
#[derive(Clone)]
pub struct StepSpec {
    pub name: String,
    pub retry: Option<RetrySpec>,
    pub timeout_ms: Option<f64>,
    pub timeout_scope: Option<String>,
    pub needs_trx: Option<bool>,
}

pub fn millis(ms: f64, name: &str) -> Result<Duration> {
    if !ms.is_finite() || ms < 0.0 {
        return Err(schema_err(format!("invalid {name}: {ms}")));
    }
    Duration::try_from_secs_f64(ms / 1000.0).map_err(|e| schema_err(format!("invalid {name}: {e}")))
}

fn retry_config(r: &RetrySpec) -> Result<zart::RetryConfig> {
    let delay = millis(r.delay_ms, "delayMs")?;
    let max_delay = r
        .max_delay_ms
        .map(|m| millis(m, "maxDelayMs"))
        .transpose()?;
    let attempts = r.max_attempts as usize;
    match r.kind.as_str() {
        "none" => Ok(zart::RetryConfig::none()),
        "fixed" => Ok(zart::RetryConfig::fixed(attempts, delay)),
        "linear" => Ok(zart::RetryConfig {
            max_attempts: attempts,
            initial_delay: delay,
            backoff_multiplier: 1.0,
            max_delay,
        }),
        "exponential" => {
            let factor = r.factor.unwrap_or(2.0);
            if !factor.is_finite() || factor < 1.0 {
                return Err(schema_err("factor must be a finite number >= 1.0"));
            }
            Ok(zart::RetryConfig {
                max_attempts: attempts,
                initial_delay: delay,
                backoff_multiplier: factor,
                max_delay,
            })
        }
        other => Err(schema_err(format!("invalid retry kind: '{other}'"))),
    }
}

fn timeout_scope(s: Option<&str>) -> Result<zart::TimeoutScope> {
    match s {
        None | Some("total") | Some("global") => Ok(zart::TimeoutScope::Global),
        Some("attempt") | Some("per_attempt") => Ok(zart::TimeoutScope::PerAttempt),
        Some(o) => Err(schema_err(format!("invalid timeoutScope: '{o}'"))),
    }
}

/// A step invocation: spec + JS `run` callback.
#[derive(Clone)]
pub struct TsStep {
    name: String,
    run: Arc<RunFn>,
    retry: Option<zart::RetryConfig>,
    timeout: Option<Duration>,
    scope: zart::TimeoutScope,
    needs_trx: bool,
    pool: sqlx::PgPool,
}

impl TsStep {
    pub fn new(spec: StepSpec, run: RunFn, pool: sqlx::PgPool) -> Result<Self> {
        Ok(Self {
            retry: spec.retry.as_ref().map(retry_config).transpose()?,
            timeout: spec
                .timeout_ms
                .map(|m| millis(m, "timeoutMs"))
                .transpose()?,
            scope: timeout_scope(spec.timeout_scope.as_deref())?,
            name: spec.name,
            run: Arc::new(run),
            needs_trx: spec.needs_trx.unwrap_or(false),
            pool,
        })
    }
}

fn js_err_to_step(e: napi::Error) -> TsStepError {
    TsStepError {
        error_type: "Error".into(),
        message: reason_message(&e.reason),
        data: None,
    }
}

impl zart::ZartStep for TsStep {
    type Output = serde_json::Value;
    type Error = TsStepError;

    fn step_name(&self) -> Cow<'static, str> {
        Cow::Owned(self.name.clone())
    }

    fn retry_config(&self) -> Option<zart::RetryConfig> {
        self.retry.clone()
    }

    fn timeout(&self) -> Option<Duration> {
        self.timeout
    }

    fn timeout_scope(&self) -> zart::TimeoutScope {
        self.scope
    }

    fn run(&self) -> impl Future<Output = std::result::Result<Self::Output, Self::Error>> + Send {
        struct TrxGuard(Option<Arc<Mutex<Option<zart::ZartTrx>>>>);
        impl TrxGuard {
            async fn release(&mut self) {
                if let Some(arc) = self.0.take() {
                    let _ = arc.lock().await.take();
                }
            }
        }
        impl Drop for TrxGuard {
            fn drop(&mut self) {
                if let Some(arc) = self.0.take()
                    && let Ok(mut g) = arc.try_lock()
                {
                    let _ = g.take();
                }
            }
        }

        let run = self.run.clone();
        let needs_trx = self.needs_trx;
        let pool = self.pool.clone();
        async move {
            let mut guard = TrxGuard(None);
            let native_trx = if needs_trx {
                let zart_trx = zart::trx(&pool).await.map_err(|e| TsStepError {
                    error_type: "ZartBackendError".into(),
                    message: e.to_string(),
                    data: None,
                })?;
                let arc = Arc::new(Mutex::new(Some(zart_trx)));
                guard.0 = Some(arc.clone());
                Some(NativeTrx::from_arc(arc))
            } else {
                None
            };

            let promise_res = run.call_async::<Promise<StepOutcome>>(native_trx).await;
            let promise = match promise_res {
                Ok(p) => p,
                Err(e) => {
                    guard.release().await;
                    return Err(js_err_to_step(e));
                }
            };
            let out_res = promise.await;
            guard.release().await;

            let out = out_res.map_err(js_err_to_step)?;
            if out.kind == "ok" {
                Ok(out.value.unwrap_or(serde_json::Value::Null))
            } else {
                Err(TsStepError {
                    error_type: out.error_type.unwrap_or_else(|| "Error".into()),
                    message: out.message.unwrap_or_else(|| "Step failed".into()),
                    data: out.data,
                })
            }
        }
    }
}
