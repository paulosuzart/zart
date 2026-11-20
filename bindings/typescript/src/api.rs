//! `NativeDispatcher`: JS-facing handle for the Zart workflow primitives. Commands are sent
//! over an mpsc channel and executed by the durable task's run loop (so the `ZART_CTX` /
//! `ZART_PHASE` task-locals are in scope), replying via `oneshot`.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use napi::bindgen_prelude::*;
use napi::sys;
use napi::threadsafe_function::{ErrorStrategy, ThreadsafeFunction};
use napi_derive::napi;
use tokio::sync::{mpsc, oneshot};

use crate::errors::{StepOutcome, backend_err, schema_err, step_err_to_napi, task_err_to_napi};
use crate::step::{RunFn, StepSpec, TsStep, millis};

type Reply<T> = oneshot::Sender<Result<T>>;
type Json = serde_json::Value;

pub type CaptureFn = ThreadsafeFunction<(), ErrorStrategy::Fatal>;

pub enum Command {
    Require {
        step: TsStep,
        reply: Reply<Json>,
    },
    Step {
        step: TsStep,
        reply: Reply<StepOutcome>,
    },
    StepOr {
        step: TsStep,
        default: Json,
        reply: Reply<Json>,
    },
    Schedule {
        step: TsStep,
        reply: Reply<u32>,
    },
    WaitAll {
        handles: Vec<u32>,
        reply: Reply<Vec<StepOutcome>>,
    },
    Sleep {
        name: String,
        duration: Duration,
        reply: Reply<()>,
    },
    SleepUntil {
        name: String,
        wake_time: chrono::DateTime<chrono::Utc>,
        reply: Reply<()>,
    },
    WaitForEvent {
        name: String,
        timeout: Option<Duration>,
        reply: Reply<Json>,
    },
    Capture {
        name: String,
        run: CaptureFn,
        reply: Reply<Json>,
    },
    Now {
        name: String,
        reply: Reply<String>,
    },
}

/// Read-only view of the current execution.
#[napi(object)]
#[derive(Clone)]
pub struct ExecutionInfo {
    pub execution_id: String,
    pub task_name: String,
    pub data: Json,
    pub current_attempt: u32,
    pub max_retries: Option<u32>,
    pub is_retry: bool,
}

impl From<zart::ExecutionInfo> for ExecutionInfo {
    fn from(i: zart::ExecutionInfo) -> Self {
        Self {
            is_retry: i.is_retry(),
            execution_id: i.execution_id,
            task_name: i.task_name,
            data: i.data,
            current_attempt: i.current_attempt as u32,
            max_retries: i.max_retries.map(|m| m as u32),
        }
    }
}

/// Handle given to JS durable handlers.
#[napi(js_name = "NativeDispatcher")]
pub struct Dispatcher {
    tx: mpsc::UnboundedSender<Command>,
    info: ExecutionInfo,
    pool: sqlx::PgPool,
}

impl Dispatcher {
    pub fn new(
        tx: mpsc::UnboundedSender<Command>,
        info: ExecutionInfo,
        pool: sqlx::PgPool,
    ) -> Self {
        Self { tx, info, pool }
    }

    async fn ask<T>(&self, build: impl FnOnce(Reply<T>) -> Command) -> Result<T> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(build(reply))
            .map_err(|_| backend_err("dispatcher", "durable execution is no longer running"))?;
        rx.await
            .map_err(|_| backend_err("dispatcher", "dispatcher dropped reply"))?
    }
}

#[napi]
impl Dispatcher {
    /// Execution metadata (synchronous).
    #[napi]
    pub fn context(&self) -> ExecutionInfo {
        self.info.clone()
    }

    /// Run a step; failures are returned as `StepOutcome` (`business_err` / `zart_err`).
    #[napi]
    pub async fn step(&self, spec: StepSpec, run: RunFn) -> Result<StepOutcome> {
        let step = TsStep::new(spec, run, self.pool.clone())?;
        self.ask(|reply| Command::Step { step, reply }).await
    }

    /// Run a step and reject on any failure.
    #[napi]
    pub async fn require(&self, spec: StepSpec, run: RunFn) -> Result<Json> {
        let step = TsStep::new(spec, run, self.pool.clone())?;
        self.ask(|reply| Command::Require { step, reply }).await
    }

    /// Run a step, falling back to `default` on business error.
    #[napi]
    pub async fn step_or(&self, spec: StepSpec, run: RunFn, default: Json) -> Result<Json> {
        let step = TsStep::new(spec, run, self.pool.clone())?;
        self.ask(|reply| Command::StepOr {
            step,
            default,
            reply,
        })
        .await
    }

    /// Schedule a step for parallel execution; resolves to a handle id.
    #[napi]
    pub async fn schedule(&self, spec: StepSpec, run: RunFn) -> Result<u32> {
        let step = TsStep::new(spec, run, self.pool.clone())?;
        self.ask(|reply| Command::Schedule { step, reply }).await
    }

    /// Wait for one scheduled step.
    #[napi]
    pub async fn wait(&self, handle: u32) -> Result<StepOutcome> {
        let mut v = self
            .ask(|reply| Command::WaitAll {
                handles: vec![handle],
                reply,
            })
            .await?;
        v.pop().ok_or_else(|| backend_err("wait", "no outcome"))
    }

    /// Wait for several scheduled steps concurrently, preserving order.
    #[napi]
    pub async fn wait_all(&self, handles: Vec<u32>) -> Result<Vec<StepOutcome>> {
        self.ask(|reply| Command::WaitAll { handles, reply }).await
    }

    #[napi]
    pub async fn sleep(&self, name: String, ms: f64) -> Result<()> {
        let duration = millis(ms, "ms")?;
        self.ask(|reply| Command::Sleep {
            name,
            duration,
            reply,
        })
        .await
    }

    #[napi]
    pub async fn sleep_until(&self, name: String, wake_time: String) -> Result<()> {
        let wake_time = chrono::DateTime::parse_from_rfc3339(&wake_time)
            .map_err(|e| schema_err(format!("Invalid ISO-8601 timestamp: {e}")))?
            .with_timezone(&chrono::Utc);
        self.ask(|reply| Command::SleepUntil {
            name,
            wake_time,
            reply,
        })
        .await
    }

    #[napi]
    pub async fn wait_for_event(&self, name: String, timeout_ms: Option<f64>) -> Result<Json> {
        let timeout = timeout_ms.map(|m| millis(m, "timeoutMs")).transpose()?;
        self.ask(|reply| Command::WaitForEvent {
            name,
            timeout,
            reply,
        })
        .await
    }

    /// Capture a value durably. `run` may be synchronous or return a `Promise`, and is
    /// only invoked the first time (never on replay).
    #[napi]
    pub async fn capture(
        &self,
        name: String,
        run: ThreadsafeFunction<(), ErrorStrategy::Fatal>,
    ) -> Result<Json> {
        self.ask(|reply| Command::Capture { name, run, reply })
            .await
    }

    /// Durable timestamp (RFC3339).
    #[napi]
    pub async fn now(&self, name: String) -> Result<String> {
        self.ask(|reply| Command::Now { name, reply }).await
    }
}

/// Mutable state owned by the run loop.
#[derive(Default)]
pub struct LoopState {
    handles: HashMap<u32, zart::StepHandle<Json>>,
    next_id: u32,
}

/// Execute a command inside the Tokio task where `ZART_CTX` / `ZART_PHASE` are set.
pub async fn handle_command(
    cmd: Command,
    state: &mut LoopState,
    cf: &Mutex<Option<zart::StepError>>,
) {
    match cmd {
        Command::Require { step, reply } => {
            let r = zart::require(step)
                .await
                .map_err(|e| task_err_to_napi(e, cf));
            let _ = reply.send(r);
        }
        Command::Step { step, reply } => {
            let r = match zart::step(step).await {
                Ok(zart::StepOutcome::Ok(v)) => Ok(StepOutcome::ok(v)),
                Ok(zart::StepOutcome::BusinessErr(e)) => Ok(StepOutcome::business_err(e)),
                Ok(zart::StepOutcome::ZartErr(e)) => Ok(StepOutcome::from_zart_step_error(&e)),
                Err(e) => Err(step_err_to_napi(e, cf)),
            };
            let _ = reply.send(r);
        }
        Command::StepOr {
            step,
            default,
            reply,
        } => {
            let r = zart::step_or(step, default)
                .await
                .map_err(|e| step_err_to_napi(e, cf));
            let _ = reply.send(r);
        }
        Command::Schedule { step, reply } => {
            let handle = zart::schedule(step);
            let id = state.next_id;
            state.next_id += 1;
            state.handles.insert(id, handle);
            let _ = reply.send(Ok(id));
        }
        Command::WaitAll { handles, reply } => {
            let mut taken = Vec::with_capacity(handles.len());
            for id in &handles {
                match state.handles.remove(id) {
                    Some(h) => taken.push(h),
                    None => {
                        let _ = reply.send(Err(schema_err(format!(
                            "unknown or already-awaited step handle {id}"
                        ))));
                        return;
                    }
                }
            }
            let r = match zart::wait(taken).await {
                Ok(results) => Ok(results
                    .into_iter()
                    .map(|r| match r {
                        Ok(v) => StepOutcome::ok(v),
                        Err(e) => StepOutcome::from_step_error(&e),
                    })
                    .collect()),
                Err(e) => Err(step_err_to_napi(e, cf)),
            };
            let _ = reply.send(r);
        }
        Command::Sleep {
            name,
            duration,
            reply,
        } => {
            let r = zart::sleep(&name, duration)
                .await
                .map_err(|e| step_err_to_napi(e, cf));
            let _ = reply.send(r);
        }
        Command::SleepUntil {
            name,
            wake_time,
            reply,
        } => {
            let r = zart::sleep_until(&name, wake_time)
                .await
                .map_err(|e| step_err_to_napi(e, cf));
            let _ = reply.send(r);
        }
        Command::WaitForEvent {
            name,
            timeout,
            reply,
        } => {
            let r = zart::wait_for_event::<Json>(&name, timeout)
                .await
                .map_err(|e| step_err_to_napi(e, cf));
            let _ = reply.send(r);
        }
        Command::Capture { name, run, reply } => {
            enum CaptureOutput {
                Future(Promise<Json>),
                Value(Json),
            }
            impl FromNapiValue for CaptureOutput {
                unsafe fn from_napi_value(
                    env: sys::napi_env,
                    napi_val: sys::napi_value,
                ) -> Result<Self> {
                    let mut is_promise = false;
                    check_status!(
                        unsafe { sys::napi_is_promise(env, napi_val, &mut is_promise) },
                        "Failed to check promise"
                    )?;
                    if is_promise {
                        Ok(Self::Future(unsafe {
                            Promise::<Json>::from_napi_value(env, napi_val)?
                        }))
                    } else {
                        Ok(Self::Value(unsafe {
                            Json::from_napi_value(env, napi_val)?
                        }))
                    }
                }
            }

            let mut js_err: Option<napi::Error> = None;
            let r = zart::capture(&name, || {
                // Called at most once, on a tokio worker; JS runs on the Node main thread.
                let handle = tokio::runtime::Handle::current();
                let res = tokio::task::block_in_place(|| {
                    handle.block_on(async {
                        match run.call_async::<CaptureOutput>(()).await? {
                            CaptureOutput::Future(p) => p.await,
                            CaptureOutput::Value(v) => Ok(v),
                        }
                    })
                });
                match res {
                    Ok(v) => v,
                    Err(e) => {
                        js_err = Some(e);
                        Json::Null
                    }
                }
            })
            .await;
            let out = match (js_err, r) {
                (Some(e), _) => Err(e),
                (None, Ok(v)) => Ok(v),
                (None, Err(e)) => Err(step_err_to_napi(e, cf)),
            };
            let _ = reply.send(out);
        }
        Command::Now { name, reply } => {
            let r = zart::now(&name)
                .await
                .map(|d| d.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
                .map_err(|e| step_err_to_napi(e, cf));
            let _ = reply.send(r);
        }
    }
}
