//! Integration tests for zart-macros.
//!
//! These tests verify that the procedural macros expand correctly and integrate
//! with the `zart` runtime. They do NOT require a running PostgreSQL instance —
//! all tests use an in-memory `MockScheduler`.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use zart::context::TaskContext;
use zart::context::ZartStep;
use zart::error::TaskError;
use zart::registry::DurableExecution;
use zart::retry::RetryConfig;
use zart_core::StorageError;
use zart_core::store::pause_storage::PauseStorage;
use zart_core::store::{EventStore, ExecutionStore, StepStore, WaitGroupStore};
use zart_core::types::{
    CompleteStepAndScheduleBodyParams, CompleteStepNoResumeParams, CompleteWaitGroupChildParams,
    EventDeliveryResult, ExecutionRecord, ExecutionRunRecord, ExecutionStats,
    FailWaitGroupChildParams, FetchedTask, ListExecutionsParams, RescheduleStepForRetryParams,
    ScheduleAtParams, ScheduleResult, ScheduleStepParams, StepAttemptRow, StepKind, StepLookup,
    StepRow, UpsertWaitGroupStepParams,
};
use zart_macros::zart_durable;
use zart_scheduler::TaskScheduler;

// ── Local step error for test steps ───────────────────────────────────────

#[derive(Debug, serde::Serialize, serde::Deserialize)]
enum TestStepError {
    Failed { step: String, reason: String },
}

impl std::fmt::Display for TestStepError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TestStepError::Failed { step, reason } => {
                write!(f, "Step '{step}' failed: {reason}")
            }
        }
    }
}

impl std::error::Error for TestStepError {}

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
impl TaskScheduler for MockScheduler {
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
impl ExecutionStore for MockScheduler {
    async fn start_execution(
        &self,
        _: &str,
        _: &str,
        _: serde_json::Value,
    ) -> Result<(), StorageError> {
        Ok(())
    }
    async fn complete_execution(&self, _: &str, _: serde_json::Value) -> Result<(), StorageError> {
        Ok(())
    }
    async fn fail_execution(&self, _: &str) -> Result<(), StorageError> {
        Ok(())
    }
    async fn get_execution(&self, _: &str) -> Result<Option<ExecutionRecord>, StorageError> {
        Ok(None)
    }
    async fn cancel_execution(&self, _: &str) -> Result<bool, StorageError> {
        Ok(false)
    }
    async fn list_executions(
        &self,
        _: ListExecutionsParams,
    ) -> Result<Vec<ExecutionRecord>, StorageError> {
        Ok(vec![])
    }
    async fn get_current_run_id(&self, _: &str) -> Result<Option<String>, StorageError> {
        Ok(None)
    }
    async fn list_runs(&self, _: &str) -> Result<Vec<ExecutionRunRecord>, StorageError> {
        Ok(vec![])
    }
    async fn reset_execution(&self, _: &str, _: serde_json::Value) -> Result<String, StorageError> {
        Ok(String::new())
    }
    async fn retry_dead_step(
        &self,
        _: &str,
        _: &str,
        _: Option<&str>,
    ) -> Result<String, StorageError> {
        Ok(String::new())
    }
    async fn restart_run(
        &self,
        _: &str,
        _: Option<serde_json::Value>,
        _: &str,
        _: Option<&str>,
    ) -> Result<String, StorageError> {
        Ok(String::new())
    }
}

#[async_trait]
impl StepStore for MockScheduler {
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
    async fn get_step(&self, _: &str, _: &str) -> Result<Option<StepRow>, StorageError> {
        Ok(None)
    }
    async fn list_steps(&self, _: &str) -> Result<Vec<StepRow>, StorageError> {
        Ok(vec![])
    }
    async fn list_step_attempts(&self, _: &str) -> Result<Vec<StepAttemptRow>, StorageError> {
        Ok(vec![])
    }
    async fn schedule_step(
        &self,
        params: ScheduleStepParams,
    ) -> Result<ScheduleResult, StorageError> {
        Ok(ScheduleResult {
            task_id: params.task_id,
            execution_time: params.execution_time,
        })
    }
    async fn complete_step_and_schedule_body(
        &self,
        _: CompleteStepAndScheduleBodyParams,
    ) -> Result<(), StorageError> {
        Ok(())
    }
    async fn complete_step_no_resume(
        &self,
        _: CompleteStepNoResumeParams,
    ) -> Result<(), StorageError> {
        Ok(())
    }
    async fn reschedule_step_for_retry(
        &self,
        _: RescheduleStepForRetryParams,
    ) -> Result<(), StorageError> {
        Ok(())
    }
    async fn insert_completed_step(
        &self,
        _: &str,
        _: &str,
        _: StepKind,
        _: serde_json::Value,
    ) -> Result<(), StorageError> {
        Ok(())
    }
    async fn check_wait_all_children(
        &self,
        _: &[String],
    ) -> Result<Vec<(String, serde_json::Value)>, StorageError> {
        Ok(vec![])
    }
}

#[async_trait]
impl WaitGroupStore for MockScheduler {
    async fn upsert_wait_group_step(
        &self,
        _: UpsertWaitGroupStepParams,
    ) -> Result<(), StorageError> {
        Ok(())
    }
    async fn complete_wait_group_child(
        &self,
        _: CompleteWaitGroupChildParams,
    ) -> Result<bool, StorageError> {
        Ok(false)
    }
    async fn fail_wait_group_child(
        &self,
        _: FailWaitGroupChildParams,
    ) -> Result<bool, StorageError> {
        Ok(false)
    }
    async fn recover_wait_group_orphans(&self) -> Result<usize, StorageError> {
        Ok(0)
    }
}

#[async_trait]
impl EventStore for MockScheduler {
    async fn deliver_event(
        &self,
        _: &str,
        _: &str,
        _: serde_json::Value,
    ) -> Result<EventDeliveryResult, StorageError> {
        Ok(EventDeliveryResult::NotRegistered)
    }
    async fn complete_event_step_and_schedule_body(
        &self,
        _: &str,
        _: &str,
        _: serde_json::Value,
    ) -> Result<bool, StorageError> {
        Ok(true)
    }
    async fn execution_stats(&self) -> Result<ExecutionStats, StorageError> {
        Ok(ExecutionStats {
            scheduled: 0,
            running: 0,
            completed: 0,
            failed: 0,
            cancelled: 0,
        })
    }
}

impl PauseStorage for MockScheduler {}

fn make_ctx() -> TaskContext {
    let scheduler = Arc::new(MockScheduler::new());
    TaskContext::new(
        scheduler.clone() as Arc<dyn zart::store::StorageBackend>,
        scheduler as Arc<dyn zart_scheduler::TaskScheduler>,
        "test-execution",
        "test-task",
        "lock-token",
        serde_json::Value::Null,
    )
}

/// Run a handler via the registry path (which sets task-locals).
async fn run_handler<H: DurableExecution>(
    task_name: &str,
    handler: H,
    data: H::Data,
) -> Result<serde_json::Value, TaskError>
where
    H::Data: serde::Serialize,
{
    let mut registry = zart::TaskRegistry::new();
    registry.register(task_name, handler);

    let ctx = Arc::new(make_ctx());
    let raw_data = serde_json::to_value(data).unwrap();
    registry.execute_handler(task_name, ctx, raw_data).await
}

// ── #[zart_durable] tests ─────────────────────────────────────────────────────

#[zart_durable("echo-task")]
async fn echo_handler(data: String) -> Result<String, TaskError> {
    Ok(format!("echo: {data}"))
}

#[tokio::test]
async fn zart_durable_generates_handler_struct() {
    let result = run_handler("echo-task", EchoHandler, "hello".to_string())
        .await
        .unwrap();
    assert_eq!(result, serde_json::json!("echo: hello"));
}

#[zart_durable("multi-word-task")]
async fn multi_word_task_handler(data: u32) -> Result<u32, TaskError> {
    Ok(data * 2)
}

#[tokio::test]
async fn zart_durable_pascal_case_struct_name() {
    let result = run_handler("multi-word-task", MultiWordTaskHandler, 21u32)
        .await
        .unwrap();
    assert_eq!(result, serde_json::json!(42));
}

#[zart_durable("timed-task", timeout = "5m")]
async fn timed_handler(data: ()) -> Result<(), TaskError> {
    Ok(data)
}

#[test]
fn zart_durable_timeout_attribute() {
    let handler = TimedHandler;
    assert_eq!(handler.timeout(), Some(Duration::from_secs(300)));
}

#[zart_durable("hours-task", timeout = "2h")]
async fn hours_handler(_data: ()) -> Result<(), TaskError> {
    Ok(())
}

#[test]
fn zart_durable_timeout_hours() {
    let handler = HoursHandler;
    assert_eq!(handler.timeout(), Some(Duration::from_secs(7200)));
}

#[zart_durable("no-timeout-task")]
async fn no_timeout_handler(_data: ()) -> Result<(), TaskError> {
    Ok(())
}

#[test]
fn zart_durable_no_timeout_returns_none() {
    let handler = NoTimeoutHandler;
    assert_eq!(handler.timeout(), None);
}

struct ProcessStep {
    input: String,
}

#[async_trait]
impl ZartStep for ProcessStep {
    type Output = String;
    type Error = TestStepError;
    fn step_name(&self) -> Cow<'static, str> {
        Cow::Borrowed("process")
    }
    async fn run(&self) -> Result<Self::Output, Self::Error> {
        Ok(self.input.to_uppercase())
    }
}

#[zart_durable("step-task")]
async fn step_using_handler(data: String) -> Result<String, TaskError> {
    let processed = zart::require(ProcessStep { input: data }).await?;
    Ok(processed)
}

#[tokio::test]
async fn zart_durable_with_execute_step_first_call_schedules() {
    let result = run_handler("step-task", StepUsingHandler, "hello".to_string()).await;
    let err = result.unwrap_err();
    match &err {
        TaskError::StepFailed { step, .. } => assert_eq!(step, "process"),
        other => panic!("expected StepFailed, got: {other:?}"),
    }
}

struct ComputeStep {
    input: u32,
}

#[async_trait]
impl ZartStep for ComputeStep {
    type Output = u32;
    type Error = TestStepError;
    fn step_name(&self) -> Cow<'static, str> {
        Cow::Borrowed("compute")
    }
    fn retry_config(&self) -> Option<RetryConfig> {
        Some(RetryConfig::fixed(3, Duration::from_millis(10)))
    }
    async fn run(&self) -> Result<Self::Output, Self::Error> {
        Ok(self.input + 1)
    }
}

#[zart_durable("retry-task")]
async fn retry_step_handler(data: u32) -> Result<u32, TaskError> {
    let result = zart::require(ComputeStep { input: data }).await?;
    Ok(result)
}

#[tokio::test]
async fn zart_durable_with_execute_step_retry_first_call_schedules() {
    let result = run_handler("retry-task", RetryStepHandler, 5u32).await;
    assert!(
        matches!(result, Err(TaskError::StepFailed { .. })),
        "expected StepFailed, got: {result:?}"
    );
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderData {
    id: u64,
    amount: f64,
}

#[zart_durable("order-task")]
async fn order_handler(data: OrderData) -> Result<String, TaskError> {
    Ok(format!("order-{}-{:.2}", data.id, data.amount))
}

#[tokio::test]
async fn zart_durable_struct_data_type() {
    let result = run_handler(
        "order-task",
        OrderHandler,
        OrderData {
            id: 42,
            amount: 9.99,
        },
    )
    .await
    .unwrap();
    assert_eq!(result, serde_json::json!("order-42-9.99"));
}

#[zart_durable("loop-task")]
async fn loop_handler(data: Vec<u32>) -> Result<u32, TaskError> {
    let mut total = 0u32;
    for (index, item) in data.into_iter().enumerate() {
        struct LoopItemStep {
            index: usize,
            value: u32,
        }
        #[async_trait]
        impl ZartStep for LoopItemStep {
            type Output = u32;
            type Error = TestStepError;
            fn step_name(&self) -> Cow<'static, str> {
                Cow::Owned(format!("loop-item-{}", self.index))
            }
            async fn run(&self) -> Result<Self::Output, Self::Error> {
                Ok(self.value * 2)
            }
        }
        let v = zart::require(LoopItemStep { index, value: item }).await?;
        total += v;
    }
    Ok(total)
}

#[tokio::test]
async fn zart_durable_loop_with_execute_step_schedules_first_item() {
    let result = run_handler("loop-task", LoopHandler, vec![1u32, 2, 3]).await;
    assert!(
        matches!(result, Err(TaskError::StepFailed { .. })),
        "expected step to be scheduled, got: {result:?}"
    );
}

// ── Dynamic step name tests ───────────────────────────────────────────────────

#[zart_macros::zart_step("process-item-{index}")]
async fn process_item(index: u32) -> Result<u32, TestStepError> {
    Ok(index * 10)
}

#[test]
fn zart_step_template_name_generates_dynamic_cow() {
    let step = process_item(3);
    assert_eq!(step.step_name(), "process-item-3");

    let step = process_item(42);
    assert_eq!(step.step_name(), "process-item-42");
}

#[test]
fn named_overrides_static_step_name() {
    let step = ProcessStep {
        input: "hello".to_string(),
    };
    assert_eq!(step.step_name(), "process");

    let overridden = step.named("process-0");
    assert_eq!(overridden.step_name(), "process-0");
}
