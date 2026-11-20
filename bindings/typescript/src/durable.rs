//! JS durable handlers bridged to `zart::DurableExecution`.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use napi::bindgen_prelude::*;
use napi::threadsafe_function::{ErrorStrategy, ThreadsafeFunction};
use napi::{JsUnknown, NapiValue};
use napi_derive::napi;
use tokio::sync::mpsc;

use crate::api::{Command, Dispatcher, LoopState, handle_command};
use crate::errors::{is_control_flow_reason, reason_message};

/// Description of why `on_failure` was invoked on a `DurableTask`.
#[napi(object)]
#[derive(Clone, serde::Serialize)]
pub struct NativeExecutionFailure {
    pub kind: String,
    pub step: Option<String>,
    pub raw: Option<serde_json::Value>,
    pub attempts: Option<u32>,
}

impl From<zart::ExecutionFailure> for NativeExecutionFailure {
    fn from(f: zart::ExecutionFailure) -> Self {
        match f {
            zart::ExecutionFailure::StepFailed { step, raw } => Self {
                kind: "step_failed".to_string(),
                step: Some(step),
                raw: Some(raw),
                attempts: None,
            },
            zart::ExecutionFailure::ExecutionDeadlineExceeded => Self {
                kind: "execution_deadline_exceeded".to_string(),
                step: None,
                raw: None,
                attempts: None,
            },
            zart::ExecutionFailure::RetriesExhausted { attempts } => Self {
                kind: "retries_exhausted".to_string(),
                step: None,
                raw: None,
                attempts: Some(attempts as u32),
            },
        }
    }
}

/// Arguments passed to a registered `on_failure` handler invocation.
pub struct OnFailureCall {
    pub input: serde_json::Value,
    pub failure: NativeExecutionFailure,
}

/// Threadsafe callback for durable task failure recovery: `on_failure(input, failure)`.
pub type OnFailureFn = ThreadsafeFunction<OnFailureCall, ErrorStrategy::Fatal>;

/// Create the threadsafe `on_failure` handler: JS is invoked as `on_failure(input, failure)`.
pub fn make_on_failure_handler(env: &Env, func: &JsFunction) -> Result<OnFailureFn> {
    let mut tsfn: OnFailureFn = func.create_threadsafe_function(
        0,
        |ctx: napi::threadsafe_function::ThreadSafeCallContext<OnFailureCall>| {
            let input = ctx.env.to_js_value(&ctx.value.input)?;
            let failure = ctx.env.to_js_value(&ctx.value.failure)?;
            Ok(vec![input.into_unknown(), failure.into_unknown()])
        },
    )?;
    tsfn.unref(env)?;
    Ok(tsfn)
}

/// Arguments for one handler invocation.
pub struct DurableCall {
    pub dispatcher: Dispatcher,
    pub input: serde_json::Value,
}

pub type HandlerFn = ThreadsafeFunction<DurableCall, ErrorStrategy::Fatal>;

/// Create the threadsafe handler: JS is invoked as `handler(dispatcher, input)`.
pub fn make_handler(env: &Env, func: &JsFunction) -> Result<HandlerFn> {
    let mut tsfn: HandlerFn = func.create_threadsafe_function(
        0,
        |ctx: napi::threadsafe_function::ThreadSafeCallContext<DurableCall>| {
            let raw = unsafe {
                <Dispatcher as ToNapiValue>::to_napi_value(ctx.env.raw(), ctx.value.dispatcher)?
            };
            let disp = unsafe { JsUnknown::from_raw(ctx.env.raw(), raw)? };
            let input = ctx.env.to_js_value(&ctx.value.input)?;
            Ok(vec![disp, input.into_unknown()])
        },
    )?;
    // Do not keep the Node event loop alive just for the registered handler.
    tsfn.unref(env)?;
    Ok(tsfn)
}

#[derive(Clone)]
pub struct TsDurableTask {
    pub name: String,
    pub handler: Arc<HandlerFn>,
    pub timeout: Option<Duration>,
    pub max_retries: usize,
    pub pool: sqlx::PgPool,
    pub on_failure: Option<Arc<OnFailureFn>>,
}

fn reject_to_task_err(e: napi::Error, cf: &Mutex<Option<zart::StepError>>) -> zart::TaskError {
    if let Some(step_err) = cf.lock().ok().and_then(|mut g| g.take()) {
        return zart::TaskError::from(step_err);
    }
    if is_control_flow_reason(&e.reason) {
        return zart::TaskError::Cancelled;
    }
    zart::TaskError::StepFailed {
        step: "handler".into(),
        source: zart::StepError::Failed {
            step: "handler".into(),
            reason: reason_message(&e.reason),
        },
    }
}

#[async_trait]
impl zart::DurableExecution for TsDurableTask {
    type Data = serde_json::Value;
    type Output = serde_json::Value;

    fn max_retries(&self) -> usize {
        self.max_retries
    }

    fn timeout(&self) -> Option<Duration> {
        self.timeout
    }

    async fn run(&self, data: Self::Data) -> std::result::Result<Self::Output, zart::TaskError> {
        let info = zart::context().into();
        let (tx, mut rx) = mpsc::unbounded_channel::<Command>();
        let cf: Mutex<Option<zart::StepError>> = Mutex::new(None);
        let call = DurableCall {
            dispatcher: Dispatcher::new(tx, info, self.pool.clone()),
            input: data,
        };
        let handler = self.handler.clone();

        let fut = async move {
            let promise = handler
                .call_async::<Promise<serde_json::Value>>(call)
                .await?;
            promise.await
        };
        tokio::pin!(fut);

        let mut state = LoopState::default();
        let result = loop {
            tokio::select! {
                res = &mut fut => break res,
                Some(cmd) = rx.recv() => handle_command(cmd, &mut state, &cf).await,
            }
        };

        // A swallowed control-flow signal must still suspend the execution.
        if let Some(step_err) = cf.lock().ok().and_then(|mut g| g.take()) {
            return Err(zart::TaskError::from(step_err));
        }
        result.map_err(|e| reject_to_task_err(e, &cf))
    }

    async fn on_failure(
        &self,
        data: Self::Data,
        failure: zart::ExecutionFailure,
    ) -> std::result::Result<Self::Output, zart::TaskError> {
        let Some(ref cb) = self.on_failure else {
            return Err(zart::TaskError::Cancelled);
        };
        let call = OnFailureCall {
            input: data,
            failure: failure.into(),
        };
        let cb = cb.clone();
        let fut = async move {
            let promise = cb.call_async::<Promise<serde_json::Value>>(call).await?;
            promise.await
        };
        fut.await.map_err(|e| zart::TaskError::StepFailed {
            step: "on_failure".into(),
            source: zart::StepError::Failed {
                step: "on_failure".into(),
                reason: reason_message(&e.reason),
            },
        })
    }
}
