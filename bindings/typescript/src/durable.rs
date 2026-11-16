//! JS durable handlers bridged to `zart::DurableExecution`.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use napi::bindgen_prelude::*;
use napi::threadsafe_function::{ErrorStrategy, ThreadsafeFunction};
use napi::{JsUnknown, NapiValue};
use tokio::sync::mpsc;

use crate::api::{Command, Dispatcher, LoopState, handle_command};
use crate::errors::{is_control_flow_reason, reason_message};

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
}
