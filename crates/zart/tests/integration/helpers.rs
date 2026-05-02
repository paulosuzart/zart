pub use serde::{Deserialize, Serialize};
pub use std::borrow::Cow;
pub use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
pub use std::time::Duration;
/// Shared helpers and test step definitions for integration tests.
pub use zart::postgres::PgBackend;
pub use zart::{
    Backend, DurableRegistry, RetryConfig, Worker, WorkerBuilder, WorkerConfig, context::ZartStep,
    error::TaskError, registry::DurableExecution,
};
pub use zart_core::store::{
    EventStore as _, ExecutionStore as _, StepStore as _, WaitGroupStore as _,
};
pub use zart_core::types::{EventDeliveryResult, ExecutionStatus, StepStatus};

// ── Local step error for test steps ───────────────────────────────────────

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum TestStepError {
    Failed { step: String, reason: String },
    Simple(String),
}

impl std::fmt::Display for TestStepError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TestStepError::Failed { step, reason } => {
                write!(f, "Step '{step}' failed: {reason}")
            }
            TestStepError::Simple(reason) => write!(f, "{reason}"),
        }
    }
}

impl std::error::Error for TestStepError {}

// ── Shared helpers ────────────────────────────────────────────────────────

pub fn pg_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://zart:zart@localhost:5432/zart".to_string())
}

pub async fn setup() -> Arc<PgBackend> {
    let pool = sqlx::PgPool::connect(&pg_url())
        .await
        .expect("failed to connect to PostgreSQL");
    let pg = Arc::new(PgBackend::new(pool));
    pg.run_migrations().await.expect("migrations failed");
    pg
}

pub fn spawn_worker(
    pg: Arc<PgBackend>,
    registry: DurableRegistry,
) -> (Arc<Worker>, tokio::task::JoinHandle<()>) {
    let config = WorkerConfig {
        poll_interval: Duration::from_millis(100),
        max_tasks_per_poll: 10,
        max_concurrent_tasks: 4,
        shutdown_timeout: Duration::from_secs(5),
        orphan_timeout: Duration::from_secs(30),
        ..Default::default()
    };
    let worker = Arc::new(
        WorkerBuilder::from_backend(pg.as_ref())
            .durable_registry(registry)
            .config(config)
            .build(),
    );
    let w = worker.clone();
    let handle = tokio::spawn(async move { w.run().await });
    (worker, handle)
}

// ── Handlers for sequential task tests ────────────────────────────────────

pub struct StepOne;

#[async_trait::async_trait]
impl ZartStep for StepOne {
    type Output = i32;
    type Error = TestStepError;
    fn step_name(&self) -> Cow<'static, str> {
        Cow::Borrowed("step-one")
    }
    async fn run(&self) -> Result<Self::Output, Self::Error> {
        println!("[step-one] Attempt {}", zart::context().current_attempt + 1);
        Ok(21i32)
    }
}

pub struct StepTwo {
    pub step1_result: i32,
}

#[async_trait::async_trait]
impl ZartStep for StepTwo {
    type Output = i32;
    type Error = TestStepError;
    fn step_name(&self) -> Cow<'static, str> {
        Cow::Borrowed("step-two")
    }
    async fn run(&self) -> Result<Self::Output, Self::Error> {
        println!("[step-two] running");
        Ok(self.step1_result * 2)
    }
}

pub struct SequentialTask;

#[async_trait::async_trait]
impl DurableExecution for SequentialTask {
    type Data = serde_json::Value;
    type Output = serde_json::Value;

    async fn run(&self, _data: Self::Data) -> Result<Self::Output, TaskError> {
        let step1: i32 = zart::require(StepOne).await?;
        let step2: i32 = zart::require(StepTwo {
            step1_result: step1,
        })
        .await?;
        Ok(serde_json::json!({ "answer": step2 }))
    }
}

// ── Simple types for testing start_for_in_tx ─────────────────────────────

#[derive(Serialize, Deserialize)]
pub struct TestInput {
    pub value: i32,
}

pub struct TestHandler;

#[async_trait::async_trait]
impl DurableExecution for TestHandler {
    type Data = TestInput;
    type Output = serde_json::Value;

    async fn run(&self, input: Self::Data) -> Result<Self::Output, TaskError> {
        Ok(serde_json::json!({ "echo": input.value }))
    }
}

// ── Handlers for failing task tests ──────────────────────────────────────

pub struct FailStep;

#[async_trait::async_trait]
impl ZartStep for FailStep {
    type Output = serde_json::Value;
    type Error = TestStepError;
    fn step_name(&self) -> Cow<'static, str> {
        Cow::Borrowed("fail-step")
    }
    async fn run(&self) -> Result<Self::Output, Self::Error> {
        println!("[fail-step] Failing intentionally");
        Err(TestStepError::Failed {
            step: "fail-step".to_string(),
            reason: "intentional failure".to_string(),
        })
    }
}

pub struct FailingTask;

#[async_trait::async_trait]
impl DurableExecution for FailingTask {
    type Data = serde_json::Value;
    type Output = serde_json::Value;

    async fn run(&self, _data: Self::Data) -> Result<Self::Output, TaskError> {
        zart::require(FailStep).await?;
        Ok(serde_json::json!({}))
    }
}

// ── Handlers for transient failure tests ─────────────────────────────────

pub struct TransientStep {
    pub attempts: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl ZartStep for TransientStep {
    type Output = String;
    type Error = TestStepError;
    fn step_name(&self) -> Cow<'static, str> {
        Cow::Borrowed("transient-step")
    }
    fn retry_config(&self) -> Option<RetryConfig> {
        Some(RetryConfig::fixed(3, Duration::from_millis(50)))
    }
    async fn run(&self) -> Result<Self::Output, Self::Error> {
        let count = self.attempts.fetch_add(1, Ordering::SeqCst);
        println!(
            "[transient-step] Attempt {} (0-indexed: {})",
            zart::context().current_attempt + 1,
            zart::context().current_attempt
        );
        if count < 2 {
            Err(TestStepError::Failed {
                step: "transient-step".to_string(),
                reason: format!("transient error #{}", count + 1),
            })
        } else {
            Ok("success".to_string())
        }
    }
}

pub struct TransientFailTask {
    pub attempts: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl DurableExecution for TransientFailTask {
    type Data = serde_json::Value;
    type Output = serde_json::Value;

    async fn run(&self, _data: Self::Data) -> Result<Self::Output, TaskError> {
        let result: String = zart::require(TransientStep {
            attempts: self.attempts.clone(),
        })
        .await?;
        Ok(serde_json::json!({ "result": result }))
    }
}

// ── Handlers for event-driven tests ──────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
pub struct ApprovalPayload {
    pub approved: bool,
}

pub struct WaitEventTask;

#[async_trait::async_trait]
impl DurableExecution for WaitEventTask {
    type Data = serde_json::Value;
    type Output = serde_json::Value;

    async fn run(&self, _data: Self::Data) -> Result<Self::Output, TaskError> {
        let approval: ApprovalPayload =
            zart::wait_for_event("approve", Some(Duration::from_secs(30))).await?;
        Ok(serde_json::json!({ "approved": approval.approved }))
    }
}

// ── Handlers for parallel steps tests ────────────────────────────────────

pub struct StepA;

#[async_trait::async_trait]
impl ZartStep for StepA {
    type Output = i32;
    type Error = TestStepError;
    fn step_name(&self) -> Cow<'static, str> {
        Cow::Borrowed("step-a")
    }
    async fn run(&self) -> Result<Self::Output, Self::Error> {
        println!("[step-a] running");
        Ok(1)
    }
}

pub struct StepB;

#[async_trait::async_trait]
impl ZartStep for StepB {
    type Output = i32;
    type Error = TestStepError;
    fn step_name(&self) -> Cow<'static, str> {
        Cow::Borrowed("step-b")
    }
    async fn run(&self) -> Result<Self::Output, Self::Error> {
        println!("[step-b] running");
        Ok(2)
    }
}

pub struct StepC;

#[async_trait::async_trait]
impl ZartStep for StepC {
    type Output = i32;
    type Error = TestStepError;
    fn step_name(&self) -> Cow<'static, str> {
        Cow::Borrowed("step-c")
    }
    async fn run(&self) -> Result<Self::Output, Self::Error> {
        println!("[step-c] running");
        Ok(3)
    }
}

pub struct ParallelTask;

#[async_trait::async_trait]
impl DurableExecution for ParallelTask {
    type Data = serde_json::Value;
    type Output = serde_json::Value;

    async fn run(&self, _data: Self::Data) -> Result<Self::Output, TaskError> {
        let h1 = zart::schedule(StepA);
        let h2 = zart::schedule(StepB);
        let h3 = zart::schedule(StepC);

        let results = zart::wait(vec![h1, h2, h3]).await?;
        let sum: i32 = results.into_iter().map(|r| r.unwrap()).sum();
        Ok(serde_json::json!({ "sum": sum }))
    }
}

// ── Handlers for cancellation tests ──────────────────────────────────────

pub struct GatedTask {
    pub started: Arc<tokio::sync::Notify>,
    pub gate: Arc<tokio::sync::Notify>,
}

#[async_trait::async_trait]
impl DurableExecution for GatedTask {
    type Data = serde_json::Value;
    type Output = serde_json::Value;

    async fn run(&self, _data: Self::Data) -> Result<Self::Output, TaskError> {
        self.started.notify_one();
        self.gate.notified().await;
        Ok(serde_json::json!({ "done": true }))
    }
}

pub struct GatedStep;

#[async_trait::async_trait]
impl ZartStep for GatedStep {
    type Output = i32;
    type Error = TestStepError;
    fn step_name(&self) -> Cow<'static, str> {
        Cow::Borrowed("gated-step")
    }
    async fn run(&self) -> Result<Self::Output, Self::Error> {
        println!("[gated-step] Scheduling step");
        Ok(1)
    }
}

pub struct GatedStepTask {
    pub started: Arc<tokio::sync::Notify>,
    pub gate: Arc<tokio::sync::Notify>,
}

#[async_trait::async_trait]
impl DurableExecution for GatedStepTask {
    type Data = serde_json::Value;
    type Output = serde_json::Value;

    async fn run(&self, _data: Self::Data) -> Result<Self::Output, TaskError> {
        self.started.notify_one();
        self.gate.notified().await;
        zart::require(GatedStep).await?;
        Ok(serde_json::json!({}))
    }
}

// ── Handlers for typed completion API tests ──────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypedInput {
    pub multiplier: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TypedOutput {
    pub result: i32,
}

pub struct TypedTask;

#[async_trait::async_trait]
impl DurableExecution for TypedTask {
    type Data = TypedInput;
    type Output = TypedOutput;

    async fn run(&self, data: Self::Data) -> Result<Self::Output, TaskError> {
        let val: i32 = zart::require(MultiplyStep {
            multiplier: data.multiplier,
        })
        .await?;
        Ok(TypedOutput { result: val })
    }
}

pub struct MultiplyStep {
    pub multiplier: i32,
}

#[async_trait::async_trait]
impl ZartStep for MultiplyStep {
    type Output = i32;
    type Error = TestStepError;
    fn step_name(&self) -> Cow<'static, str> {
        Cow::Borrowed("multiply")
    }
    async fn run(&self) -> Result<Self::Output, Self::Error> {
        Ok(self.multiplier * 2)
    }
}
