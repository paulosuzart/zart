//! Integration tests for zart-macros.
//!
//! These tests verify that the procedural macros expand correctly and integrate
//! with the `zart` runtime. They do NOT require a running PostgreSQL instance —
//! all tests use an in-memory `MockScheduler`.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use scheduler::{
    DurableStorage, FetchedTask, ScheduleAtParams, ScheduleResult, Scheduler, StepLookup,
    StorageError,
};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use zart::context::TaskContext;
use zart::error::{StepError, TaskError};
use zart::registry::DurableExecution;
use zart::retry::RetryConfig;
use zart_macros::{z_durable_loop, z_wait_event, zart_durable};

// ── Mock scheduler (no-op) ────────────────────────────────────────────────────

struct MockScheduler {
    step_responses: HashMap<(String, String), Option<StepLookup>>,
}

impl MockScheduler {
    fn new() -> Self {
        Self {
            step_responses: HashMap::new(),
        }
    }
}

#[async_trait]
impl Scheduler for MockScheduler {
    async fn schedule_now(
        &self,
        task_id: &str,
        _task_name: &str,
        _data: serde_json::Value,
    ) -> Result<ScheduleResult, StorageError> {
        Ok(ScheduleResult {
            task_id: task_id.to_string(),
            execution_time: Utc::now(),
        })
    }

    async fn schedule_at(&self, params: ScheduleAtParams) -> Result<ScheduleResult, StorageError> {
        Ok(ScheduleResult {
            task_id: params.task_id,
            execution_time: params.execution_time,
        })
    }

    async fn poll_due(
        &self,
        _now: DateTime<Utc>,
        _limit: usize,
    ) -> Result<Vec<FetchedTask>, StorageError> {
        Ok(vec![])
    }

    async fn update_task_state(
        &self,
        _task_id: &str,
        _state: serde_json::Value,
        _next_execution_time: DateTime<Utc>,
        _lock_token: &str,
    ) -> Result<(), StorageError> {
        Ok(())
    }

    async fn mark_completed(
        &self,
        _task_id: &str,
        _result: Option<serde_json::Value>,
        _lock_token: &str,
    ) -> Result<(), StorageError> {
        Ok(())
    }

    async fn mark_failed(
        &self,
        _task_id: &str,
        _error: &str,
        _next_execution_time: Option<DateTime<Utc>>,
        _lock_token: &str,
    ) -> Result<(), StorageError> {
        Ok(())
    }

    async fn cancel_task(&self, _task_id: &str) -> Result<bool, StorageError> {
        Ok(true)
    }

    async fn delete_task(&self, _task_id: &str) -> Result<(), StorageError> {
        Ok(())
    }

    async fn run_migrations(&self) -> Result<(), StorageError> {
        Ok(())
    }
}

#[async_trait]
impl DurableStorage for MockScheduler {
    async fn get_step_status(
        &self,
        execution_id: &str,
        step_name: &str,
    ) -> Result<Option<StepLookup>, StorageError> {
        Ok(self
            .step_responses
            .get(&(execution_id.to_string(), step_name.to_string()))
            .cloned()
            .unwrap_or(None))
    }

    async fn complete_event_step_and_schedule_body(
        &self,
        _execution_id: &str,
        _event_name: &str,
        _payload: serde_json::Value,
    ) -> Result<bool, StorageError> {
        Ok(true)
    }

    async fn schedule_step(
        &self,
        task_id: &str,
        _task_name: &str,
        _run_id: &str,
        _step_name: &str,
        _step_kind: &str,
        execution_time: DateTime<Utc>,
        _data: serde_json::Value,
        _metadata: serde_json::Value,
        _retry_config: Option<&serde_json::Value>,
    ) -> Result<ScheduleResult, StorageError> {
        Ok(ScheduleResult {
            task_id: task_id.to_string(),
            execution_time,
        })
    }

    async fn complete_step_and_schedule_body(
        &self,
        _step_task_id: &str,
        _step_id: &str,
        _result: serde_json::Value,
        _lock_token: &str,
        _attempt_number: usize,
        _next_body_task_id: &str,
        _task_name: &str,
        _run_id: &str,
        _data: serde_json::Value,
    ) -> Result<(), StorageError> {
        Ok(())
    }

    async fn complete_step_no_resume(
        &self,
        _step_task_id: &str,
        _step_id: &str,
        _result: serde_json::Value,
        _lock_token: &str,
        _attempt_number: usize,
    ) -> Result<(), StorageError> {
        Ok(())
    }

    async fn reschedule_step_for_retry(
        &self,
        _step_task_id: &str,
        _attempt_number: usize,
        _error: &str,
        _retry_time: DateTime<Utc>,
        _lock_token: &str,
    ) -> Result<(), StorageError> {
        Ok(())
    }
}

/// Construct a fresh TaskContext backed by the MockScheduler.
fn make_ctx() -> TaskContext {
    TaskContext::new(
        Arc::new(MockScheduler::new()),
        "test-execution",
        "test-task",
        "lock-token",
        serde_json::Value::Null,
    )
}

// ── z_wait_event! tests ───────────────────────────────────────────────────────

/// `z_wait_event!` must expand to `ctx.wait_for_event(name, None)` when no
/// timeout is given. On first call (step row absent), returns `Scheduled`.
#[tokio::test]
async fn z_wait_event_no_timeout_returns_waiting() {
    let mut ctx = make_ctx();
    let result: Result<String, StepError> = z_wait_event!("approval").await;

    assert!(
        matches!(result, Err(StepError::Scheduled { ref step, .. }) if step == "approval"),
        "expected Scheduled (step task created), got: {result:?}"
    );
}

/// `z_wait_event!` with `timeout = "1h"` passes `Some(Duration::from_secs(3600))`
/// to `wait_for_event`. On first call, returns `Scheduled`.
#[tokio::test]
async fn z_wait_event_with_timeout_returns_waiting() {
    let mut ctx = make_ctx();
    let result: Result<serde_json::Value, StepError> =
        z_wait_event!("manager-approval", timeout = "1h").await;

    assert!(
        matches!(result, Err(StepError::Scheduled { .. })),
        "expected Scheduled (step task created), got: {result:?}"
    );
}

// ── z_durable_loop! tests ─────────────────────────────────────────────────────

/// `z_durable_loop!` expands to a plain `for` loop.
#[test]
fn z_durable_loop_iterates_all_items() {
    let items = vec![1u32, 2, 3, 4, 5];
    let mut sum = 0u32;
    z_durable_loop!(items, |n| {
        sum += n;
    });
    assert_eq!(sum, 15);
}

/// An empty collection results in zero iterations.
#[test]
fn z_durable_loop_empty_collection() {
    let items: Vec<u32> = vec![];
    let mut ran = false;
    z_durable_loop!(items, |_n| {
        ran = true;
    });
    assert!(!ran);
}

// ── #[zart_durable] tests ─────────────────────────────────────────────────────

/// A simple handler with no steps: the macro generates `EchoHandler` and the
/// `run` method executes the body correctly.
#[zart_durable("echo-task")]
async fn echo_handler(_ctx: &mut TaskContext, data: String) -> Result<String, TaskError> {
    Ok(format!("echo: {data}"))
}

#[tokio::test]
async fn zart_durable_generates_handler_struct() {
    let handler = EchoHandler;
    let mut ctx = make_ctx();
    let result = handler.run(&mut ctx, "hello".to_string()).await.unwrap();
    assert_eq!(result, "echo: hello");
}

/// The generated struct name follows `snake_case → PascalCase` convention.
#[zart_durable("multi-word-task")]
async fn multi_word_task_handler(_ctx: &mut TaskContext, data: u32) -> Result<u32, TaskError> {
    Ok(data * 2)
}

#[tokio::test]
async fn zart_durable_pascal_case_struct_name() {
    let handler = MultiWordTaskHandler;
    let mut ctx = make_ctx();
    let result = handler.run(&mut ctx, 21u32).await.unwrap();
    assert_eq!(result, 42);
}

/// The `timeout` attribute is reflected in `DurableExecution::timeout()`.
#[zart_durable("timed-task", timeout = "5m")]
async fn timed_handler(_ctx: &mut TaskContext, data: ()) -> Result<(), TaskError> {
    Ok(data)
}

#[test]
fn zart_durable_timeout_attribute() {
    let handler = TimedHandler;
    assert_eq!(handler.timeout(), Some(Duration::from_secs(300)));
}

#[zart_durable("hours-task", timeout = "2h")]
async fn hours_handler(_ctx: &mut TaskContext, _data: ()) -> Result<(), TaskError> {
    Ok(())
}

#[test]
fn zart_durable_timeout_hours() {
    let handler = HoursHandler;
    assert_eq!(handler.timeout(), Some(Duration::from_secs(7200)));
}

/// A handler with no timeout returns `None` from `timeout()`.
#[zart_durable("no-timeout-task")]
async fn no_timeout_handler(_ctx: &mut TaskContext, _data: ()) -> Result<(), TaskError> {
    Ok(())
}

#[test]
fn zart_durable_no_timeout_returns_none() {
    let handler = NoTimeoutHandler;
    assert_eq!(handler.timeout(), None);
}

/// A handler that uses `ctx.execute_step()` inside: on first call the step is scheduled,
/// causing the handler to return `Err(TaskError::StepFailed)`.
///
/// We define a simple ZartStep struct inline.
use zart::context::ZartStep;

struct ProcessStep {
    input: String,
}

#[async_trait]
impl ZartStep for ProcessStep {
    type Output = String;
    fn step_name(&self) -> Cow<'static, str> {
        Cow::Borrowed("process")
    }
    async fn run(&self, _ctx: zart::context::StepContext) -> Result<Self::Output, StepError> {
        Ok(self.input.to_uppercase())
    }
}

#[zart_durable("step-task")]
async fn step_using_handler(ctx: &mut TaskContext, data: String) -> Result<String, TaskError> {
    let processed = ctx.execute_step(ProcessStep { input: data }).await?;
    Ok(processed)
}

#[tokio::test]
async fn zart_durable_with_execute_step_first_call_schedules() {
    let handler = StepUsingHandler;
    let mut ctx = make_ctx();
    let result = handler.run(&mut ctx, "hello".to_string()).await;

    // The step is encountered for the first time → StepFailed(Scheduled) control-flow.
    assert!(
        matches!(result, Err(TaskError::StepFailed { ref step, .. }) if step == "process"),
        "expected StepFailed(Scheduled) for first step, got: {result:?}"
    );
}

/// A handler that uses `ctx.execute_step()` with a retry-configured step.
struct ComputeStep {
    input: u32,
}

#[async_trait]
impl ZartStep for ComputeStep {
    type Output = u32;
    fn step_name(&self) -> Cow<'static, str> {
        Cow::Borrowed("compute")
    }
    fn retry_config(&self) -> Option<RetryConfig> {
        Some(RetryConfig::fixed(3, Duration::from_millis(10)))
    }
    async fn run(&self, _ctx: zart::context::StepContext) -> Result<Self::Output, StepError> {
        Ok(self.input + 1)
    }
}

#[zart_durable("retry-task")]
async fn retry_step_handler(ctx: &mut TaskContext, data: u32) -> Result<u32, TaskError> {
    let result = ctx.execute_step(ComputeStep { input: data }).await?;
    Ok(result)
}

#[tokio::test]
async fn zart_durable_with_execute_step_retry_first_call_schedules() {
    let handler = RetryStepHandler;
    let mut ctx = make_ctx();
    let result = handler.run(&mut ctx, 5u32).await;
    assert!(matches!(result, Err(TaskError::StepFailed { .. })));
}

/// Struct data types (not just primitives) work as handler data.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderData {
    id: u64,
    amount: f64,
}

#[zart_durable("order-task")]
async fn order_handler(_ctx: &mut TaskContext, data: OrderData) -> Result<String, TaskError> {
    Ok(format!("order-{}-{:.2}", data.id, data.amount))
}

#[tokio::test]
async fn zart_durable_struct_data_type() {
    let handler = OrderHandler;
    let mut ctx = make_ctx();
    let result = handler
        .run(
            &mut ctx,
            OrderData {
                id: 42,
                amount: 9.99,
            },
        )
        .await
        .unwrap();
    assert_eq!(result, "order-42-9.99");
}

/// `z_durable_loop!` combined with `ctx.execute_step()` inside a `#[zart_durable]` handler.
/// Each iteration uses a unique step name via dynamic `Cow::Owned` — required because the
/// database tracks steps by name (`{execution_id}:step:{step_name}`) and the `task_id`
/// is PRIMARY KEY. Without unique names, all iterations would share the same cached result.
#[zart_durable("loop-task")]
async fn loop_handler(ctx: &mut TaskContext, data: Vec<u32>) -> Result<u32, TaskError> {
    let mut total = 0u32;
    for (index, item) in data.into_iter().enumerate() {
        struct LoopItemStep {
            index: usize,
            value: u32,
        }
        #[async_trait]
        impl ZartStep for LoopItemStep {
            type Output = u32;
            // Unique per iteration: "loop-item-0", "loop-item-1", etc.
            fn step_name(&self) -> Cow<'static, str> {
                Cow::Owned(format!("loop-item-{}", self.index))
            }
            async fn run(
                &self,
                _ctx: zart::context::StepContext,
            ) -> Result<Self::Output, StepError> {
                Ok(self.value * 2)
            }
        }
        let v = ctx
            .execute_step(LoopItemStep { index, value: item })
            .await?;
        total += v;
    }
    Ok(total)
}

#[tokio::test]
async fn zart_durable_loop_with_execute_step_schedules_first_item() {
    let handler = LoopHandler;
    let mut ctx = make_ctx();
    // The first step encountered is "item-placeholder"; it will be scheduled.
    let result = handler.run(&mut ctx, vec![1u32, 2, 3]).await;
    assert!(
        matches!(result, Err(TaskError::StepFailed { .. })),
        "expected step to be scheduled, got: {result:?}"
    );
}

// ── Dynamic step name tests ───────────────────────────────────────────────────

/// A step whose name encodes a loop index via the {field} template.
#[zart_macros::zart_step("process-item-{index}")]
async fn process_item(index: u32, ctx: zart::context::StepContext) -> Result<u32, StepError> {
    Ok(index * 10)
}

#[test]
fn zart_step_template_name_generates_dynamic_cow() {
    // step name must embed the field value at runtime
    let step = process_item(3);
    assert_eq!(step.step_name(), "process-item-3");

    let step = process_item(42);
    assert_eq!(step.step_name(), "process-item-42");
}

#[test]
fn with_id_overrides_static_step_name() {
    let step = ProcessStep {
        input: "hello".to_string(),
    };
    assert_eq!(step.step_name(), "process");

    let overridden = step.with_id("process-0");
    assert_eq!(overridden.step_name(), "process-0");
}
