//! CLI Interaction Demo — Long-running durable execution for CLI admin commands demonstration.
//!
//! This example starts a durable execution with multiple steps and sleeps to provide
//! enough time for interactive CLI admin command demonstrations.
//!
//! The execution flow:
//! 1. Step: "prepare-data" — completes quickly
//! 2. Sleep: 30 seconds (gives time for CLI commands)
//! 3. Step: "process-results" — completes after sleep
//! 4. Sleep: 30 seconds (more time for CLI commands)
//! 5. Step: "finalize" — final step
//!
//! Run this example, then in another terminal use the zart CLI to:
//! - `zart status <execution_id>` — check execution status
//! - `zart detail <execution_id>` — inspect runs, steps, and attempt history
//! - `zart pause --execution-id <id>` — pause the execution
//! - `zart resume --execution-id <id>` — resume it
//! - `zart pause-list` — list pause rules
//! - `zart restart <execution_id>` — restart from scratch
//! - `zart runs <execution_id>` — list all runs

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use zart::PostgresStorage;
use zart::prelude::*;

// ── Handler ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CliDemoInput {
    /// Optional: make a step fail for retry demonstration.
    fail_step: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CliDemoOutput {
    prepared: String,
    processed: String,
    finalized: String,
}

struct CliDemoTask;

#[async_trait::async_trait]
impl DurableExecution for CliDemoTask {
    type Data = CliDemoInput;
    type Output = CliDemoOutput;

    async fn run(&self, data: Self::Data) -> Result<Self::Output, TaskError> {
        tracing::info!("[cli-demo] === Execution started ===");

        // Step 1: Prepare data (completes quickly)
        tracing::info!("[cli-demo] Step 1: Preparing data...");
        let prepared = zart::require(PrepareData).await?;
        tracing::info!("[cli-demo] Step 1 completed: {}", prepared);

        // Sleep 1: 30 seconds — time for CLI commands
        tracing::info!("[cli-demo] Sleeping 30 seconds (try CLI commands now)...");
        zart::sleep("first-sleep", Duration::from_secs(30)).await?;
        tracing::info!("[cli-demo] First sleep completed");

        // Step 2: Process results
        tracing::info!("[cli-demo] Step 2: Processing results...");
        let processed = zart::require(ProcessResults {
            fail: data.fail_step,
        })
        .await?;
        tracing::info!("[cli-demo] Step 2 completed: {}", processed);

        // Sleep 2: 30 seconds — more time for CLI commands
        tracing::info!("[cli-demo] Sleeping 30 seconds (more CLI commands)...");
        zart::sleep("second-sleep", Duration::from_secs(30)).await?;
        tracing::info!("[cli-demo] Second sleep completed");

        // Step 3: Finalize
        tracing::info!("[cli-demo] Step 3: Finalizing...");
        let finalized = zart::require(Finalize).await?;
        tracing::info!("[cli-demo] Step 3 completed: {}", finalized);

        tracing::info!("[cli-demo] === Execution completed successfully ===");

        Ok(CliDemoOutput {
            prepared,
            processed,
            finalized,
        })
    }

    fn max_retries(&self) -> usize {
        0 // No retries — we want controlled failures for demo
    }
}

// ── Steps ────────────────────────────────────────────────────────────────────

struct PrepareData;

#[async_trait::async_trait]
impl ZartStep for PrepareData {
    type Output = String;
    type Error = CliDemoStepError;

    fn step_name(&self) -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("prepare-data")
    }

    async fn run(&self) -> Result<Self::Output, Self::Error> {
        tracing::info!("[prepare-data] Running preparation logic...");
        // Simulate some work
        tokio::time::sleep(Duration::from_secs(2)).await;
        Ok("data-prepared-successfully".to_string())
    }
}

struct ProcessResults {
    fail: bool,
}

#[async_trait::async_trait]
impl ZartStep for ProcessResults {
    type Output = String;
    type Error = CliDemoStepError;

    fn step_name(&self) -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("process-results")
    }

    async fn run(&self) -> Result<Self::Output, Self::Error> {
        if self.fail {
            tracing::warn!("[process-results] INTENTIONAL FAILURE for retry demo");
            return Err(CliDemoStepError("intentional failure for demo".into()));
        }
        tracing::info!("[process-results] Processing results...");
        tokio::time::sleep(Duration::from_secs(2)).await;
        Ok("results-processed-successfully".to_string())
    }
}

struct Finalize;

#[async_trait::async_trait]
impl ZartStep for Finalize {
    type Output = String;
    type Error = CliDemoStepError;

    fn step_name(&self) -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("finalize")
    }

    async fn run(&self) -> Result<Self::Output, Self::Error> {
        tracing::info!("[finalize] Running finalization...");
        tokio::time::sleep(Duration::from_secs(2)).await;
        Ok("execution-finalized-successfully".to_string())
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct CliDemoStepError(String);

impl std::fmt::Display for CliDemoStepError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for CliDemoStepError {}

// ── Main ─────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Simple output-focused logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_target(false)
        .with_thread_ids(false)
        .init();

    let db_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://zart:zart@localhost:5432/zart".to_string());

    let execution_id = std::env::var("EXECUTION_ID")
        .unwrap_or_else(|_| format!("cli-demo-{}", uuid::Uuid::new_v4()));

    let fail_step = std::env::var("FAIL_STEP")
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    println!("=== Zart CLI Demo — Long-Running Execution ===\n");
    println!("Execution ID: {}", execution_id);
    println!("Database: {}", db_url);
    println!("Fail step: {}", fail_step);
    println!();
    println!("This execution will run for ~2 minutes with sleeps between steps.");
    println!("During the sleeps, you can use the zart CLI to interact:");
    println!("  zart status {}", execution_id);
    println!("  zart detail {}", execution_id);
    println!("  zart pause --execution-id {}", execution_id);
    println!("  zart resume --execution-id {}", execution_id);
    println!("  zart pause-list");
    println!("  zart restart {}", execution_id);
    println!("  zart runs {}", execution_id);
    println!();

    let pool = sqlx::PgPool::connect(&db_url).await?;
    let sched = Arc::new(PostgresStorage::new(pool.clone()));

    let durable = Arc::new(DurableScheduler::with_pause(
        sched.clone(),
        sched.task_scheduler(),
        sched.clone(),
    ));

    // Start the durable execution
    durable
        .start_for::<CliDemoTask>(
            &execution_id,
            "zart::cli_demo::CliDemoTask",
            &CliDemoInput { fail_step },
        )
        .await?;

    println!("✓ Execution started with ID: {}", execution_id);
    println!("  Task: zart::cli_demo::CliDemoTask");
    println!();

    // Register and run the handler
    let mut registry = TaskRegistry::new();
    registry.register("zart::cli_demo::CliDemoTask", CliDemoTask);
    let registry = Arc::new(registry);

    let config = zart::WorkerConfig {
        poll_interval: Duration::from_millis(500),
        max_tasks_per_poll: 10,
        max_concurrent_tasks: 4,
        shutdown_timeout: Duration::from_secs(10),
        orphan_timeout: Duration::from_secs(30),
        ..Default::default()
    };

    let worker = zart::Worker::new(sched.task_scheduler(), sched.clone(), registry, config);

    println!("Worker starting... (press Ctrl+C to stop)");
    println!();

    // Run the worker — it will execute the full flow
    worker.run().await;

    println!();
    println!("=== Worker completed ===");

    // Show final status
    let record = durable.status(&execution_id).await?;
    println!("Final execution status: {}", record.status);
    if let Some(result) = &record.result {
        println!("Result: {}", result);
    }

    Ok(())
}
