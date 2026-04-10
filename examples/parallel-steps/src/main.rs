//! Parallel Steps Example
//!
//! Demonstrates parallel step execution using schedule + wait:
//! 1. Schedule 3 independent simulated health checks
//! 2. Aggregate results into a summary
//!
//! Features: zart::schedule, zart::wait, structured output, #[zart_step].

use async_trait::async_trait;
use scheduler::PostgresScheduler;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use zart::error::{StepError, TaskError};
use zart::prelude::*;
use zart::registry::DurableExecution;
use zart::zart_step;

// ── Input / Output types ──────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HealthCheckInput {
    services: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ServiceResult {
    name: String,
    status: String,
    response_ms: u64,
    issues: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HealthCheckOutput {
    services_checked: usize,
    total_issues: usize,
    results: Vec<ServiceResult>,
}

// ── Step definition using #[zart_step] ────────────────────────────────────────

/// A health check step that simulates checking a service.
#[zart_step("check-service")]
async fn check_service(service: String) -> Result<ServiceResult, StepError> {
    println!(
        "[check-{}] Attempt {}",
        service,
        zart::context().current_attempt + 1
    );
    let (status, response_ms, issues) = match service.as_str() {
        "auth-api" => ("healthy".to_string(), 42, vec![]),
        "payments" => (
            "degraded".to_string(),
            156,
            vec!["high latency detected".to_string()],
        ),
        "users-db" => ("healthy".to_string(), 28, vec![]),
        _ => (
            "unknown".to_string(),
            0,
            vec!["no check configured".to_string()],
        ),
    };
    Ok(ServiceResult {
        name: service.to_string(),
        status,
        response_ms,
        issues,
    })
}

// ── Task handler ──────────────────────────────────────────────────────────────

struct HealthCheckTask;

#[async_trait]
impl DurableExecution for HealthCheckTask {
    type Data = HealthCheckInput;
    type Output = HealthCheckOutput;

    async fn run(&self, data: Self::Data) -> Result<Self::Output, TaskError> {
        let handles: Vec<StepHandle<ServiceResult>> = data
            .services
            .iter()
            .map(|service| zart::schedule(check_service(service.clone())))
            .collect();

        let results = zart::wait(handles).await?;
        let mut service_results = vec![];
        for result in results {
            let svc = result.map_err(|e| TaskError::StepFailed {
                step: "parallel-health-check".to_string(),
                source: e,
            })?;
            service_results.push(svc);
        }

        let total_issues: usize = service_results.iter().map(|s| s.issues.len()).sum();

        Ok(HealthCheckOutput {
            services_checked: service_results.len(),
            total_issues,
            results: service_results,
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

    println!("=== Zart Parallel Steps Example ===\n");

    let db_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://zart:zart@localhost:5432/zart".to_string());

    let pool = sqlx::PgPool::connect(&db_url).await?;
    let sched = Arc::new(PostgresScheduler::new(pool));
    sched.run_migrations().await?;

    let mut registry = TaskRegistry::new();
    registry.register("health-check", HealthCheckTask);
    let registry = Arc::new(registry);

    let execution_id = format!("health-check-demo-{}", uuid::Uuid::new_v4());
    let durable = DurableScheduler::new(sched.clone());

    let input = HealthCheckInput {
        services: vec![
            "auth-api".to_string(),
            "payments".to_string(),
            "users-db".to_string(),
        ],
    };

    println!(
        "Starting execution '{}' for {} services...",
        execution_id,
        input.services.len()
    );
    durable
        .start_typed(&execution_id, "health-check", &input)
        .await?;

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

    tokio::time::sleep(Duration::from_millis(500)).await;

    let initial_status = durable.status(&execution_id).await?;
    println!("Initial execution status: {:?}\n", initial_status.status);

    println!("Waiting for execution to complete...\n");
    let record = durable
        .wait(&execution_id, Duration::from_secs(60), None)
        .await?;

    worker.stop();

    match record.status {
        scheduler::ExecutionStatus::Completed => {
            let output: HealthCheckOutput = serde_json::from_value(record.result.unwrap())?;
            println!("Execution completed!");
            println!("  Services checked: {}", output.services_checked);
            println!("  Total issues:     {}", output.total_issues);

            if !output.results.is_empty() {
                println!();
                for r in &output.results {
                    println!("  {} — {} ({}ms)", r.name, r.status, r.response_ms);
                    for issue in &r.issues {
                        println!("    ⚠️  {}", issue);
                    }
                }
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
