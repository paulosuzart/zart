//! Retry Simulation Example
//!
//! Demonstrates how to simulate and observe retry behavior in Zart durable executions.
//! This example intentionally fails on the first attempt and succeeds on the retry,
//! showing how the framework handles transient failures automatically.
//!
//! Key concepts demonstrated:
//! - Using Arc<AtomicUsize> to track attempts across retries
//! - Using `ctx.current_attempt()`, `ctx.max_retries()`, `ctx.is_retry_attempt()` accessors
//! - Using `step_with_retry` with a `RetryConfig` for automatic retry handling
//! - Observing the retry behavior in real-time with logging
//!
//! This pattern is useful for:
//! - Testing retry logic without relying on external failures
//! - Demonstrating resilient behavior in examples and documentation
//! - Implementing "fail fast, retry successfully" patterns in production

use std::sync::Arc;
use std::time::Duration;

use scheduler::PostgresScheduler;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use zart::error::StepError;
use zart::prelude::*;
use zart::retry::RetryConfig;

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
        // Demonstrate the TaskContext accessors BEFORE the retry
        println!("\n=== TaskContext Retry Accessors Demo ===\n");
        println!("Before retry (in body mode):");
        println!(
            "  - ctx.current_attempt() = {} (0 = body mode default)",
            ctx.current_attempt()
        );
        println!(
            "  - ctx.max_retries() = {:?} (None = no retry in body mode)",
            ctx.max_retries()
        );
        println!(
            "  - ctx.is_retry_attempt() = {} (false = not a retry)",
            ctx.is_retry_attempt()
        );

        // This step intentionally fails on attempt 0, succeeds on attempt 1
        // The closure receives &TaskContext so we can access retry metadata directly
        let result = ctx
            .step_with_retry(
                "intentional-failure",
                RetryConfig::fixed(3, Duration::from_secs(1)), // 3 retries, 1s delay
                |ctx| {
                    let name = data.name.clone();
                    async move {
                        // Access execution metadata directly from context!
                        let current = ctx.current_attempt();
                        let max = ctx.max_retries();
                        let is_retry = ctx.is_retry_attempt();

                        println!(
                            "[intentional-failure] Attempt #{} (0-indexed) | is_retry={} | max_retries={:?}",
                            current, is_retry, max
                        );

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
                        let msg = format!(
                            "✓  Succeeded for '{}' on retry attempt #{}",
                            name, current
                        );
                        println!("{}", msg);

                        Ok(RetryStepResult {
                            message: msg.clone(),
                            attempt_number: current,
                        })
                    }
                },
            )
            .await?;

        let mut attempts_log = vec![format!(
            "intentional-failure: succeeded on attempt #{} ({} retries)",
            result.attempt_number, result.attempt_number
        )];

        // Demonstrate the accessors after retry completion
        println!("\nAfter retry completion (back in body mode):");
        println!(
            "  - ctx.current_attempt() = {} (0 = body mode)",
            ctx.current_attempt()
        );
        println!(
            "  - ctx.max_retries() = {:?} (None = no retry in body mode)",
            ctx.max_retries()
        );
        println!(
            "  - ctx.is_retry_attempt() = {} (false)",
            ctx.is_retry_attempt()
        );
        println!("\nDuring the retry, the context would have shown:");
        println!("  - ctx.current_attempt() = {}", result.attempt_number);
        println!("  - ctx.is_retry_attempt() = {}", result.attempt_number > 0);
        println!("  - ctx.max_retries() = Some(3)");

        // A second step that always succeeds - demonstrates normal behavior
        let normal_result = ctx
            .step("normal-step", |ctx| async move {
                println!(
                    "\n[normal-step] Running (no retries needed, attempt {})",
                    ctx.current_attempt() + 1
                );
                Ok(NormalStepResult {
                    message: "Normal step completed successfully".to_string(),
                })
            })
            .await?;

        attempts_log.push(format!("normal-step: {}", normal_result.message));

        Ok(RetrySimulationOutput {
            name: data.name,
            total_attempts: result.attempt_number + 1, // Convert 0-indexed to count
            message: format!(
                "Completed with {} total attempt(s) - retry simulation successful!",
                result.attempt_number + 1
            ),
            attempts_log,
        })
    }
}

// ── Step result types ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RetryStepResult {
    message: String,
    attempt_number: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NormalStepResult {
    message: String,
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
    println!("This example demonstrates intentional failure and automatic retry.");
    println!("The first attempt will fail, and the framework will retry automatically.\n");

    let db_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://zart:zart@localhost:5432/zart".to_string());

    // Connect and run migrations
    let pool = sqlx::PgPool::connect(&db_url).await?;
    let sched = Arc::new(PostgresScheduler::new(pool));
    sched.run_migrations().await?;

    // Register the handler
    let mut registry = TaskRegistry::new();
    registry.register("retry-simulation", RetrySimulationTask);
    let registry = Arc::new(registry);

    // Start durable execution
    let execution_id = format!("retry-sim-{}", Uuid::new_v4());
    let durable = DurableScheduler::new(sched.clone());

    let input = RetrySimulationInput {
        name: "Paulo".to_string(),
    };

    println!(
        "Starting execution '{}' with name '{}'...",
        execution_id, input.name
    );
    durable
        .start_typed(&execution_id, "retry-simulation", &input)
        .await?;

    // Run worker
    let config = zart::WorkerConfig {
        poll_interval: Duration::from_millis(200),
        max_tasks_per_poll: 10,
        max_concurrent_tasks: 4,
        shutdown_timeout: Duration::from_secs(5),
        orphan_timeout: Duration::from_secs(30),
        ..Default::default()
    };
    let worker = Arc::new(zart::Worker::new(sched.clone(), registry.clone(), config));
    let w = worker.clone();
    let _handle = tokio::spawn(async move { w.run().await });

    // Wait a moment for the worker to start polling
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Check initial status
    let initial_status = durable.status(&execution_id).await?;
    println!("\nInitial execution status: {:?}\n", initial_status.status);

    // Wait for completion (with generous timeout to allow retries)
    println!("Waiting for execution to complete (including retries)...\n");
    let record = durable
        .wait(&execution_id, Duration::from_secs(120), None)
        .await?;

    worker.stop();

    // Display results
    match record.status {
        scheduler::ExecutionStatus::Completed => {
            let output: RetrySimulationOutput = serde_json::from_value(record.result.unwrap())?;
            println!("\n{}", "=".repeat(60));
            println!("✓ Execution completed successfully!");
            println!("{}", "=".repeat(60));
            println!("  Name:            {}", output.name);
            println!("  Total attempts:  {}", output.total_attempts);
            println!("  Message:         {}", output.message);
            println!("\nAttempts Log:");
            for (i, log) in output.attempts_log.iter().enumerate() {
                println!("  {}. {}", i + 1, log);
            }
            println!("{}", "=".repeat(60));
        }
        _ => {
            eprintln!("\n❌ Execution ended with status: {:?}", record.status);
            if let Some(result) = &record.result {
                eprintln!("Result: {}", result);
            }
            std::process::exit(1);
        }
    }

    Ok(())
}
