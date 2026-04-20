//! Zart UI demo — long-running backend for the Admin UI.
//!
//! Starts a PostgresScheduler, registers two task types, seeds a handful of
//! executions, then runs the `zart-api` HTTP server (port 3000) and a worker
//! until Ctrl+C.
//!
//! # Quick start
//!
//! ```bash
//! # 1. Start Postgres (uses the docker-compose.yml in the repo root)
//! docker compose up -d postgres
//!
//! # 2. Run this example
//! cargo run --bin example-ui-demo
//!
//! # 3. Open the UI (see docker-compose.yml ui service or npm run dev)
//! #    Set the API Server to http://localhost:3000 if running the UI elsewhere
//! ```

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use zart::PostgresStorage;
use zart::error::TaskError;
use zart::prelude::*;
use zart_api::{AppState, admin_router};

// ── Order Processing Task ─────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderInput {
    order_id: String,
    customer: String,
    amount_cents: u64,
    /// When true, charge-payment step will fail (simulates a declined card).
    simulate_payment_failure: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderOutput {
    order_id: String,
    confirmation_code: String,
    total_charged_cents: u64,
}

struct OrderProcessingTask;

#[async_trait::async_trait]
impl DurableExecution for OrderProcessingTask {
    type Data = OrderInput;
    type Output = OrderOutput;

    async fn run(&self, data: Self::Data) -> Result<Self::Output, TaskError> {
        // Step 1: validate
        let validated: bool = zart::require(ValidateOrder {
            order_id: data.order_id.clone(),
        })
        .await?;

        if !validated {
            return Err(TaskError::MaxRetriesExhausted { max_retries: 0 });
        }

        // Step 2: charge
        let charged_cents: u64 = zart::require(ChargePayment {
            amount_cents: data.amount_cents,
            fail: data.simulate_payment_failure,
        })
        .await?;

        // Step 3: confirm
        let confirmation_code: String = zart::require(SendConfirmation {
            customer: data.customer.clone(),
            order_id: data.order_id.clone(),
        })
        .await?;

        Ok(OrderOutput {
            order_id: data.order_id,
            confirmation_code,
            total_charged_cents: charged_cents,
        })
    }

    fn max_retries(&self) -> usize {
        0
    }
}

struct ValidateOrder {
    order_id: String,
}

#[async_trait::async_trait]
impl ZartStep for ValidateOrder {
    type Output = bool;
    type Error = DemoError;

    fn step_name(&self) -> std::borrow::Cow<'static, str> {
        "validate-order".into()
    }

    async fn run(&self) -> Result<Self::Output, Self::Error> {
        tracing::info!(order_id = %self.order_id, "[validate-order] checking order");
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(true)
    }
}

struct ChargePayment {
    amount_cents: u64,
    fail: bool,
}

#[async_trait::async_trait]
impl ZartStep for ChargePayment {
    type Output = u64;
    type Error = DemoError;

    fn step_name(&self) -> std::borrow::Cow<'static, str> {
        "charge-payment".into()
    }

    async fn run(&self) -> Result<Self::Output, Self::Error> {
        tokio::time::sleep(Duration::from_millis(80)).await;
        if self.fail {
            tracing::warn!("[charge-payment] card declined");
            return Err(DemoError("card declined".into()));
        }
        tracing::info!(amount = self.amount_cents, "[charge-payment] charged");
        Ok(self.amount_cents)
    }
}

struct SendConfirmation {
    customer: String,
    order_id: String,
}

#[async_trait::async_trait]
impl ZartStep for SendConfirmation {
    type Output = String;
    type Error = DemoError;

    fn step_name(&self) -> std::borrow::Cow<'static, str> {
        "send-confirmation".into()
    }

    async fn run(&self) -> Result<Self::Output, Self::Error> {
        tokio::time::sleep(Duration::from_millis(40)).await;
        let code = format!("CONF-{}", &Uuid::new_v4().to_string()[..8].to_uppercase());
        tracing::info!(customer = %self.customer, order = %self.order_id, code = %code, "[send-confirmation] sent");
        Ok(code)
    }
}

// ── Data Pipeline Task ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PipelineInput {
    source: String,
    destination: String,
    record_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PipelineOutput {
    records_processed: u64,
    destination: String,
}

struct DataPipelineTask;

#[async_trait::async_trait]
impl DurableExecution for DataPipelineTask {
    type Data = PipelineInput;
    type Output = PipelineOutput;

    async fn run(&self, data: Self::Data) -> Result<Self::Output, TaskError> {
        let extracted: u64 = zart::require(ExtractData {
            source: data.source.clone(),
            record_count: data.record_count,
        })
        .await?;

        let loaded: u64 = zart::require(TransformAndLoad {
            destination: data.destination.clone(),
            record_count: extracted,
        })
        .await?;

        Ok(PipelineOutput {
            records_processed: loaded,
            destination: data.destination,
        })
    }

    fn max_retries(&self) -> usize {
        1
    }
}

struct ExtractData {
    source: String,
    record_count: u64,
}

#[async_trait::async_trait]
impl ZartStep for ExtractData {
    type Output = u64;
    type Error = DemoError;

    fn step_name(&self) -> std::borrow::Cow<'static, str> {
        "extract-data".into()
    }

    async fn run(&self) -> Result<Self::Output, Self::Error> {
        tracing::info!(source = %self.source, "[extract-data] extracting {} records", self.record_count);
        tokio::time::sleep(Duration::from_millis(120)).await;
        Ok(self.record_count)
    }
}

struct TransformAndLoad {
    destination: String,
    record_count: u64,
}

#[async_trait::async_trait]
impl ZartStep for TransformAndLoad {
    type Output = u64;
    type Error = DemoError;

    fn step_name(&self) -> std::borrow::Cow<'static, str> {
        "transform-load".into()
    }

    async fn run(&self) -> Result<Self::Output, Self::Error> {
        tracing::info!(destination = %self.destination, "[transform-load] loading {} records", self.record_count);
        tokio::time::sleep(Duration::from_millis(90)).await;
        Ok(self.record_count)
    }
}

// ── Shared error type ─────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
struct DemoError(String);

impl std::fmt::Display for DemoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for DemoError {}

// ── Main ──────────────────────────────────────────────────────────────────────

const TASK_ORDER: &str = "ui_demo::OrderProcessingTask";
const TASK_PIPELINE: &str = "ui_demo::DataPipelineTask";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let db_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://zart:zart@localhost:5432/zart".to_string());

    let bind_addr = std::env::var("BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:3000".to_string());

    tracing::info!("Connecting to database…");
    let pool = sqlx::PgPool::connect(&db_url).await?;

    let sched = Arc::new(PostgresStorage::new(pool.clone()));
    tracing::info!("Migrations applied");

    let durable = Arc::new(DurableScheduler::with_pause(sched.clone(), sched.clone()));

    // ── Worker ────────────────────────────────────────────────────────────────

    let mut registry = TaskRegistry::new();
    registry.register(TASK_ORDER, OrderProcessingTask);
    registry.register(TASK_PIPELINE, DataPipelineTask);
    let registry = Arc::new(registry);

    let cancellation = CancellationToken::new();

    let worker = Arc::new(Worker::new(
        sched.clone(),
        registry,
        WorkerConfig {
            poll_interval: Duration::from_millis(200),
            max_tasks_per_poll: 10,
            max_concurrent_tasks: 8,
            shutdown_timeout: Duration::from_secs(5),
            orphan_timeout: Duration::from_secs(60),
            ..Default::default()
        },
    ));

    let worker_handle = {
        let w = worker.clone();
        tokio::spawn(async move { w.run().await })
    };

    // ── Seed executions ───────────────────────────────────────────────────────

    seed_executions(&durable).await;

    // ── HTTP server ───────────────────────────────────────────────────────────

    // Merge the public API router with the admin router under one server.
    let api_state = AppState::new(durable.clone() as Arc<dyn zart::DurableApi>);
    let router = zart_api::routes::api_router(api_state)
        .merge(admin_router(durable.clone()))
        .layer(tower_http::trace::TraceLayer::new_for_http())
        .layer(tower_http::cors::CorsLayer::permissive());

    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    tracing::info!("Zart API listening on http://{bind_addr}");

    let ct = cancellation.clone();
    tokio::select! {
        res = axum::serve(listener, router).with_graceful_shutdown(async move { ct.cancelled().await }) => {
            res?;
        }
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Ctrl-C received, shutting down…");
            cancellation.cancel();
        }
    }

    worker.stop();
    let _ = worker_handle.await;
    tracing::info!("Shutdown complete");
    Ok(())
}

/// Seed a variety of executions so there's something to explore in the UI.
/// Uses `try` variants so already-existing IDs don't cause a crash on restart.
async fn seed_executions(durable: &Arc<DurableScheduler>) {
    let seeds: &[(&str, &str, serde_json::Value)] = &[
        (
            "order-success-001",
            TASK_ORDER,
            serde_json::json!({
                "order_id": "ORD-001",
                "customer": "alice@example.com",
                "amount_cents": 4999,
                "simulate_payment_failure": false
            }),
        ),
        (
            "order-success-002",
            TASK_ORDER,
            serde_json::json!({
                "order_id": "ORD-002",
                "customer": "bob@example.com",
                "amount_cents": 12500,
                "simulate_payment_failure": false
            }),
        ),
        (
            "order-failed-001",
            TASK_ORDER,
            serde_json::json!({
                "order_id": "ORD-003",
                "customer": "carol@example.com",
                "amount_cents": 99900,
                "simulate_payment_failure": true
            }),
        ),
        (
            "pipeline-etl-001",
            TASK_PIPELINE,
            serde_json::json!({
                "source": "s3://raw-events/2026-04-14",
                "destination": "warehouse.events_daily",
                "record_count": 84231
            }),
        ),
        (
            "pipeline-etl-002",
            TASK_PIPELINE,
            serde_json::json!({
                "source": "s3://raw-events/2026-04-13",
                "destination": "warehouse.events_daily",
                "record_count": 61040
            }),
        ),
        (
            "order-success-003",
            TASK_ORDER,
            serde_json::json!({
                "order_id": "ORD-004",
                "customer": "dave@example.com",
                "amount_cents": 2199,
                "simulate_payment_failure": false
            }),
        ),
    ];

    for (id, task, payload) in seeds {
        match durable.start(id, task, payload.clone()).await {
            Ok(_) => tracing::info!(%id, "Seeded execution"),
            Err(e) => tracing::debug!(%id, "Skip seed ({})", e),
        }
    }
}
