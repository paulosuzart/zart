//! Retry Simulation Example
//!
//! Demonstrates how to simulate and observe retry behavior in Zart durable executions.
//! This example intentionally fails on the first attempt and succeeds on the retry,
//! showing how the framework handles transient failures automatically.
//!
//! Key concepts demonstrated:
//! - Using `ctx.current_attempt()`, `ctx.max_retries()`, `ctx.is_retry_attempt()` accessors
//! - Using `#[zart_step]` with retry configuration for automatic retry handling
//! - Observing the retry behavior in real-time with logging
//!
//! This pattern is useful for:
//! - Testing retry logic without relying on external failures
//! - Demonstrating resilient behavior in examples and documentation
//! - Implementing "fail fast, retry successfully" patterns in production

use scheduler::PostgresScheduler;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use uuid::Uuid;
use zart::error::{StepError, TaskError};
use zart::prelude::*;
use zart::zart_step;

// ── Input / Output types ──────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RetrySimulationInput {
    /// Name to include in the output (just for demonstration)
    name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RetrySimulationOutput {
    pub name: String,
    pub total_attempts: usize,
    pub message: String,
    pub attempts_log: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RetryStepResult {
    message: String,
    attempt_number: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NormalStepResult {
    message: String,
}

// ── Step definitions using #[zart_step] ───────────────────────────────────────

/// Step that intentionally fails on first attempt, succeeds on retry.
#[zart_step("intentional-failure", retry = "fixed(3, 1s)")]
async fn intentional_failure_step(
    name: String,
    attempt_counter: Arc<AtomicUsize>,
    ctx: StepContext,
) -> Result<RetryStepResult, StepError> {
    let current = ctx.current_attempt();
    let max = ctx.max_retries();
    let is_retry = ctx.is_retry_attempt();

    println!(
        "[intentional-failure] Attempt #{} (0-indexed) | is_retry={} | max_retries={:?}",
        current, is_retry, max
    );

    // Track attempts for logging
    attempt_counter.fetch_add(1, Ordering::SeqCst);

    if current == 0 {
        // First attempt: simulate a transient failure
        let msg = format!(
            "⚠️  Simulated transient failure for '{}' on attempt #{}",
            name, current
        );
        println!("{}", msg);
        return Err(StepError::Failed {
            step: "intentional-failure".to_string(),
            reason: format!(
                "Simulated transient error (attempt {}): connection timeout",
                current + 1
            ),
        });
    }

    // Retry attempts: succeed
    let msg = format!("✓  Succeeded for '{}' on retry attempt #{}", name, current);
    println!("{}", msg);

    Ok(RetryStepResult {
        message: msg.clone(),
        attempt_number: current,
    })
}

/// A simple step that always succeeds.
#[zart_step("normal-step")]
async fn normal_step(_name: String, ctx: StepContext) -> Result<NormalStepResult, StepError> {
    let _ = ctx.current_attempt();
    println!("\n[normal-step] Running (no retries needed)");
    Ok(NormalStepResult {
        message: "Normal step completed successfully".to_string(),
    })
}

// ── Durable Execution Implementation ─────────────────────────────────────────

/// Manual implementation to demonstrate direct TaskContext usage
struct RetrySimulationTask;

#[async_trait::async_trait]
impl DurableExecution for RetrySimulationTask {
    type Data = RetrySimulationInput;
    type Output = RetrySimulationOutput;

    async fn run(
        &self,
        ctx: &mut TaskContext,
        data: Self::Data,
    ) -> Result<Self::Output, TaskError> {
        let attempt_counter = Arc::new(AtomicUsize::new(0));

        // Demonstrate the StepContext accessors BEFORE the retry
        println!("\n=== StepContext Retry Accessors Demo ===\n");
        println!("Before retry (in body mode):");
        println!("  - StepContext is only available inside step closures");
        println!("  - Each step receives its own StepContext with retry metadata");

        // Step 1: This step intentionally fails on attempt 0, succeeds on attempt 1
        let result = ctx
            .execute_step(intentional_failure_step(
                data.name.clone(),
                attempt_counter.clone(),
            ))
            .await?;

        let total_attempts = attempt_counter.load(Ordering::SeqCst);
        let mut attempts_log = vec![format!(
            "intentional-failure: succeeded on attempt #{} ({} retries)",
            result.attempt_number, result.attempt_number
        )];

        println!("\nAfter retry completion:");
        println!("  - Total attempts made: {}", total_attempts);
        println!("  - Succeeded on attempt #{}", result.attempt_number);
        println!("  - Number of retries: {}", result.attempt_number);

        // Step 2: A normal step that always succeeds
        let normal_result = ctx.execute_step(normal_step(data.name.clone())).await?;
        attempts_log.push(format!("normal-step: {}", normal_result.message));

        Ok(RetrySimulationOutput {
            name: data.name,
            total_attempts,
            message: format!(
                "Completed after {} attempt(s), succeeded on retry #{}",
                total_attempts, result.attempt_number
            ),
            attempts_log,
        })
    }
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    println!("=== Zart Retry Simulation Example ===\n");

    let db_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://zart:zart@localhost:5432/zart".to_string());

    let pool = sqlx::PgPool::connect(&db_url).await?;
    let sched = Arc::new(PostgresScheduler::new(pool));
    sched.run_migrations().await?;

    let mut registry = TaskRegistry::new();
    registry.register("retry-simulation", RetrySimulationTask);
    let registry = Arc::new(registry);

    let execution_id = format!("retry-sim-{}", Uuid::new_v4());
    let durable = DurableScheduler::new(sched.clone());

    let input = RetrySimulationInput {
        name: "retry-demo".to_string(),
    };

    println!("Starting execution '{}'...", execution_id);
    durable
        .start_typed(&execution_id, "retry-simulation", &input)
        .await?;

    let config = zart::WorkerConfig {
        poll_interval: Duration::from_millis(200),
        max_tasks_per_poll: 10,
        max_concurrent_tasks: 4,
        shutdown_timeout: Duration::from_secs(10),
        orphan_timeout: Duration::from_secs(30),
        ..Default::default()
    };
    let worker = Arc::new(zart::Worker::new(sched.clone(), registry.clone(), config));
    let w = worker.clone();
    let _handle = tokio::spawn(async move { w.run().await });

    tokio::time::sleep(Duration::from_millis(500)).await;

    let initial_status = durable.status(&execution_id).await?;
    println!("\nInitial execution status: {:?}\n", initial_status.status);

    println!("Waiting for execution to complete (watch the retries)...\n");
    let record = durable
        .wait(&execution_id, Duration::from_secs(60), None)
        .await?;

    worker.stop();

    match record.status {
        scheduler::ExecutionStatus::Completed => {
            let output: RetrySimulationOutput = serde_json::from_value(record.result.unwrap())?;
            println!("\n=== Execution Completed ===");
            println!("  Name:            {}", output.name);
            println!("  Total attempts:  {}", output.total_attempts);
            println!("  Message:         {}", output.message);
            println!("\nAttempts log:");
            for (i, entry) in output.attempts_log.iter().enumerate() {
                println!("  {}. {}", i + 1, entry);
            }
        }
        _ => {
            eprintln!("Execution ended with status: {:?}", record.status);
            if let Some(result) = &record.result {
                eprintln!("Result: {}", result);
            }
        }
    }

    Ok(())
}
