//! Durable Loops Example
//!
//! Demonstrates durable iteration over a collection with guaranteed step-name uniqueness.
//! No external dependencies — all data is fake/in-memory.
//!
//! Key concepts demonstrated:
//! - Fetching the item list inside a step (stable replay after a process restart)
//! - Unique step names per iteration via `{index}` template in `#[zart_step("process-report-{index}")]`
//! - Unique step names per iteration via `.with_id()` at the call site

use async_trait::async_trait;
use scheduler::PostgresScheduler;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;
use zart::context::TaskContext;
use zart::error::{StepError, TaskError};
use zart::prelude::*;
use zart::registry::DurableExecution;
use zart::zart_step;

// ── Data types ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Report {
    id: u32,
    title: String,
    value: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProcessedReport {
    id: u32,
    title: String,
    score: u64,
    flagged: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BatchInput {
    batch_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BatchOutput {
    batch_name: String,
    total: usize,
    flagged: usize,
}

// ── Step definitions ──────────────────────────────────────────────────────────

/// Fetches the report list for the given batch. Running this inside a step ensures
/// the same list is replayed after a crash — even if the underlying data changed.
#[zart_step("fetch-reports")]
async fn fetch_reports(batch_name: String, ctx: StepContext) -> Result<Vec<Report>, StepError> {
    let _ = ctx.current_attempt();
    println!(
        "  [fetch-reports] Loading reports for batch '{}'",
        batch_name
    );
    // In production this would query a database filtered by batch_name.
    // We return static fake data so the example runs without external dependencies.
    Ok(vec![
        Report {
            id: 1,
            title: "Q1 Sales".into(),
            value: 84.5,
        },
        Report {
            id: 2,
            title: "Q2 Sales".into(),
            value: 91.2,
        },
        Report {
            id: 3,
            title: "Q3 Sales".into(),
            value: 72.0,
        },
        Report {
            id: 4,
            title: "Q4 Sales".into(),
            value: 110.8,
        },
    ])
}

/// Processes one report.
///
/// The `{index}` placeholder in the step name expands at runtime, producing
/// unique names: "process-report-0", "process-report-1", etc. Without this,
/// all iterations would share the same DB key and silently return the first
/// result for every iteration.
#[zart_step("process-report-{index}")]
async fn process_report(
    index: usize,
    report: Report,
    ctx: StepContext,
) -> Result<ProcessedReport, StepError> {
    let _ = ctx.current_attempt();
    let score = (report.value * 10.0) as u64;
    let flagged = report.value < 80.0;
    println!(
        "  [process-report-{}] '{}': value={:.1}, score={}, flagged={}",
        index, report.title, report.value, score, flagged
    );
    Ok(ProcessedReport {
        id: report.id,
        title: report.title,
        score,
        flagged,
    })
}

/// Sends a notification alert. This step has a static name — callers must
/// supply `.with_id()` for uniqueness when calling it in a loop.
#[zart_step("notify-stakeholder")]
async fn notify_stakeholder(
    email: String,
    report_title: String,
    ctx: StepContext,
) -> Result<(), StepError> {
    let _ = ctx.current_attempt();
    println!("  [notify] Sent alert for '{}' to {}", report_title, email);
    Ok(())
}

// ── Durable handler ───────────────────────────────────────────────────────────

struct ReportBatchTask;

#[async_trait]
impl DurableExecution for ReportBatchTask {
    type Data = BatchInput;
    type Output = BatchOutput;

    async fn run(
        &self,
        ctx: &mut TaskContext,
        data: Self::Data,
    ) -> Result<Self::Output, TaskError> {
        // Step 1: fetch the list inside a step so the same list is used on replay.
        let reports = ctx
            .execute_step(fetch_reports(data.batch_name.clone()))
            .await?;
        println!("Fetched {} reports\n", reports.len());

        // Step 2: process each report.
        // The `{index}` template generates unique step names per iteration:
        // "process-report-0", "process-report-1", ...
        let mut processed = Vec::new();
        for (i, report) in reports.into_iter().enumerate() {
            let result = ctx.execute_step(process_report(i, report)).await?;
            processed.push(result);
        }

        // Step 3: notify stakeholders for flagged reports using .with_id().
        // `notify_stakeholder` has a static name, so we override it per call.
        // This produces unique keys: "notify-stakeholder-2", etc.
        for (i, p) in processed.iter().enumerate() {
            if p.flagged {
                ctx.execute_step(
                    notify_stakeholder("team@example.com".into(), p.title.clone())
                        .with_id(format!("notify-stakeholder-{i}")),
                )
                .await?;
            }
        }

        let flagged = processed.iter().filter(|p| p.flagged).count();

        Ok(BatchOutput {
            batch_name: data.batch_name,
            total: processed.len(),
            flagged,
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

    println!("=== Zart Durable Loops Example ===\n");

    let db_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://zart:zart@localhost:5432/zart".to_string());

    let pool = sqlx::PgPool::connect(&db_url).await?;
    let sched = Arc::new(PostgresScheduler::new(pool));
    sched.run_migrations().await?;

    let mut registry = TaskRegistry::new();
    registry.register("report-batch", ReportBatchTask);
    let registry = Arc::new(registry);

    let execution_id = format!("report-batch-{}", Uuid::new_v4());
    let durable = DurableScheduler::new(sched.clone());

    let input = BatchInput {
        batch_name: "2024-annual".into(),
    };

    println!("Starting execution '{}'...\n", execution_id);
    durable
        .start_typed(&execution_id, "report-batch", &input)
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

    println!("Processing reports:\n");
    let record = durable
        .wait(&execution_id, Duration::from_secs(60), None)
        .await?;

    worker.stop();

    match record.status {
        scheduler::ExecutionStatus::Completed => {
            let output: BatchOutput = serde_json::from_value(record.result.unwrap())?;
            println!("\n=== Batch Complete ===");
            println!("  Batch:   {}", output.batch_name);
            println!("  Total:   {}", output.total);
            println!("  Flagged: {}", output.flagged);
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
