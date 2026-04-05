//! Integration tests for zart-macros.
//!
//! These tests verify that the procedural macros expand correctly and integrate
//! with the `zart` runtime. They do NOT require a running PostgreSQL instance —
//! all tests use an in-memory `MockScheduler`.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use scheduler::{DurableStorage, FetchedTask, Recurrence, ScheduleResult, Scheduler, StorageError};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use zart::context::{ExecutionState, StepRecord, StepStatus, TaskContext};
use zart::error::{StepError, TaskError};
use zart::registry::TaskHandler;
use zart::retry::RetryConfig;
use zart_macros::{z_durable_loop, z_step, z_step_with_retry, z_wait_event, zart_durable};

// ── Mock scheduler (no-op) ────────────────────────────────────────────────────

struct MockScheduler;

#[async_trait]
impl Scheduler for MockScheduler {
    async fn schedule_now(
        &self,
        task_id: &str,
        _task_name: &str,
        _data: serde_json::Value,
        _execution_id: Option<&str>,
    ) -> Result<ScheduleResult, StorageError> {
        Ok(ScheduleResult {
            task_id: task_id.to_string(),
            execution_time: Utc::now(),
        })
    }

    async fn schedule_at(
        &self,
        task_id: &str,
        _task_name: &str,
        execution_time: DateTime<Utc>,
        _data: serde_json::Value,
        _recurrence: Option<Recurrence>,
        _execution_id: Option<&str>,
        _metadata: serde_json::Value,
    ) -> Result<ScheduleResult, StorageError> {
        Ok(ScheduleResult {
            task_id: task_id.to_string(),
            execution_time,
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

impl DurableStorage for MockScheduler {}

/// Construct a fresh TaskContext backed by the MockScheduler.
fn make_ctx() -> TaskContext<MockScheduler> {
    TaskContext::new(
        Arc::new(MockScheduler),
        "test-execution",
        "test-task",
        Default::default(),
        "lock-token",
        serde_json::Value::Null,
    )
}

// ── z_step! tests ─────────────────────────────────────────────────────────────

/// `z_step!` must expand to `ctx.step(name, closure)`.
/// On first call with a fresh context, the step is not yet registered, so the
/// framework returns `Err(StepError::Scheduled)` as a control-flow signal.
#[tokio::test]
async fn z_step_first_call_returns_scheduled() {
    let mut ctx = make_ctx();
    let result = z_step!("my-step", || async { Ok::<i32, StepError>(42) }).await;

    assert!(
        matches!(result, Err(StepError::Scheduled { ref step, .. }) if step == "my-step"),
        "expected StepError::Scheduled for first step encounter, got: {result:?}"
    );
}

/// After the step is manually marked Completed in state, `z_step!` returns the
/// cached result without running the closure again.
#[tokio::test]
async fn z_step_completed_step_returns_cached_result() {
    let mut state = ExecutionState::default();
    state.steps.insert(
        "cached-step".to_string(),
        StepRecord {
            status: StepStatus::Completed,
            result: Some(serde_json::json!(99)),
            in_task_id: None,
            retry_attempt: 0,
            retry_config: None,
            attempts: vec![],
            event_deadline: None,
        },
    );

    let mut ctx = TaskContext::new(
        Arc::new(MockScheduler),
        "exec",
        "task",
        state,
        "token",
        serde_json::Value::Null,
    );

    // The closure must NOT be called; the cached value (99) is returned.
    let result = z_step!("cached-step", || async {
        // If this runs, the test should panic to flag a bug.
        panic!("closure should not be called for a completed step");
        #[allow(unreachable_code)]
        Ok::<i32, StepError>(0)
    })
    .await;

    assert_eq!(result.unwrap(), 99);
}

// ── z_step_with_retry! tests ──────────────────────────────────────────────────

/// `z_step_with_retry!` must expand to `ctx.step_with_retry(name, config, closure)`.
/// On first call the step is scheduled — same control-flow as `z_step!`.
#[tokio::test]
async fn z_step_with_retry_first_call_returns_scheduled() {
    let mut ctx = make_ctx();
    let config = RetryConfig::fixed(3, Duration::from_millis(10));

    let result = z_step_with_retry!("retry-step", config, || async {
        Ok::<String, StepError>("ok".to_string())
    })
    .await;

    assert!(
        matches!(result, Err(StepError::Scheduled { ref step, .. }) if step == "retry-step"),
        "expected StepError::Scheduled, got: {result:?}"
    );
}

// ── z_wait_event! tests ───────────────────────────────────────────────────────

/// `z_wait_event!` must expand to `ctx.wait_for_event(name, None)` when no
/// timeout is given. Returns `WaitingForEvent` on first call.
#[tokio::test]
async fn z_wait_event_no_timeout_returns_waiting() {
    let mut ctx = make_ctx();
    let result: Result<String, StepError> = z_wait_event!("approval").await;

    assert!(
        matches!(result, Err(StepError::WaitingForEvent { ref event }) if event == "approval"),
        "expected WaitingForEvent, got: {result:?}"
    );
}

/// `z_wait_event!` with `timeout = "1h"` passes `Some(Duration::from_secs(3600))`
/// to `wait_for_event`. Behaviour is still `WaitingForEvent` on first call.
#[tokio::test]
async fn z_wait_event_with_timeout_returns_waiting() {
    let mut ctx = make_ctx();
    let result: Result<serde_json::Value, StepError> =
        z_wait_event!("manager-approval", timeout = "1h").await;

    assert!(
        matches!(result, Err(StepError::WaitingForEvent { .. })),
        "expected WaitingForEvent, got: {result:?}"
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
async fn echo_handler(
    _ctx: &mut TaskContext<impl Scheduler>,
    data: String,
) -> Result<String, TaskError> {
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
async fn multi_word_task_handler(
    _ctx: &mut TaskContext<impl Scheduler>,
    data: u32,
) -> Result<u32, TaskError> {
    Ok(data * 2)
}

#[tokio::test]
async fn zart_durable_pascal_case_struct_name() {
    let handler = MultiWordTaskHandler;
    let mut ctx = make_ctx();
    let result = handler.run(&mut ctx, 21u32).await.unwrap();
    assert_eq!(result, 42);
}

/// The `timeout` attribute is reflected in `TaskHandler::timeout()`.
#[zart_durable("timed-task", timeout = "5m")]
async fn timed_handler(_ctx: &mut TaskContext<impl Scheduler>, data: ()) -> Result<(), TaskError> {
    Ok(data)
}

#[test]
fn zart_durable_timeout_attribute() {
    let handler = TimedHandler;
    assert_eq!(handler.timeout(), Some(Duration::from_secs(300)));
}

#[zart_durable("hours-task", timeout = "2h")]
async fn hours_handler(_ctx: &mut TaskContext<impl Scheduler>, _data: ()) -> Result<(), TaskError> {
    Ok(())
}

#[test]
fn zart_durable_timeout_hours() {
    let handler = HoursHandler;
    assert_eq!(handler.timeout(), Some(Duration::from_secs(7200)));
}

/// A handler with no timeout returns `None` from `timeout()`.
#[zart_durable("no-timeout-task")]
async fn no_timeout_handler(
    _ctx: &mut TaskContext<impl Scheduler>,
    _data: (),
) -> Result<(), TaskError> {
    Ok(())
}

#[test]
fn zart_durable_no_timeout_returns_none() {
    let handler = NoTimeoutHandler;
    assert_eq!(handler.timeout(), None);
}

/// A handler that uses `z_step!` inside: on first call the step is scheduled,
/// causing the handler to return `Err(TaskError::StepFailed)`.
#[zart_durable("step-task")]
async fn step_using_handler(
    ctx: &mut TaskContext<impl Scheduler>,
    data: String,
) -> Result<String, TaskError> {
    let processed = z_step!("process", || async {
        Ok::<String, StepError>(data.to_uppercase())
    })
    .await?;
    Ok(processed)
}

#[tokio::test]
async fn zart_durable_with_z_step_first_call_schedules() {
    let handler = StepUsingHandler;
    let mut ctx = make_ctx();
    let result = handler.run(&mut ctx, "hello".to_string()).await;

    // The step is encountered for the first time → StepFailed(Scheduled) control-flow.
    assert!(
        matches!(result, Err(TaskError::StepFailed { ref step, .. }) if step == "process"),
        "expected StepFailed(Scheduled) for first step, got: {result:?}"
    );
}

/// A handler that uses `z_step_with_retry!` compiles and schedules on first call.
#[zart_durable("retry-task")]
async fn retry_step_handler(
    ctx: &mut TaskContext<impl Scheduler>,
    data: u32,
) -> Result<u32, TaskError> {
    let result = z_step_with_retry!(
        "compute",
        RetryConfig::fixed(3, Duration::from_millis(10)),
        || async { Ok::<u32, StepError>(data + 1) }
    )
    .await?;
    Ok(result)
}

#[tokio::test]
async fn zart_durable_with_z_step_with_retry_first_call_schedules() {
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
async fn order_handler(
    _ctx: &mut TaskContext<impl Scheduler>,
    data: OrderData,
) -> Result<String, TaskError> {
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

/// `z_durable_loop!` combined with `z_step!` inside a `#[zart_durable]` handler.
#[zart_durable("loop-task")]
async fn loop_handler(
    ctx: &mut TaskContext<impl Scheduler>,
    data: Vec<u32>,
) -> Result<u32, TaskError> {
    let mut total = 0u32;
    z_durable_loop!(data, |item| {
        let v = z_step!(&format!("item-{item}"), || async {
            Ok::<u32, StepError>(item * 2)
        })
        .await?;
        total += v;
    });
    Ok(total)
}

#[tokio::test]
async fn zart_durable_loop_with_z_step_schedules_first_item() {
    let handler = LoopHandler;
    let mut ctx = make_ctx();
    // The first step encountered is "item-1"; it will be scheduled.
    let result = handler.run(&mut ctx, vec![1u32, 2, 3]).await;
    assert!(
        matches!(result, Err(TaskError::StepFailed { .. })),
        "expected step to be scheduled, got: {result:?}"
    );
}
