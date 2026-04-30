use async_trait::async_trait;
use chrono::Utc;
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use zart_scheduler::{
    CompletionHandler, OnComplete, PostgresTaskScheduler, Recurrence, ScheduledTask,
    SchedulerTaskError, TaskInstance, TaskRegistry, TaskScheduler, Worker, WorkerConfig,
};

// ---------------------------------------------------------------------------
// Task: heartbeat-check (FixedDelay recurring task)
// ---------------------------------------------------------------------------

struct HeartbeatCheck;

#[async_trait]
impl ScheduledTask for HeartbeatCheck {
    async fn execute(
        &self,
        instance: &TaskInstance,
    ) -> Result<Box<dyn CompletionHandler>, SchedulerTaskError> {
        println!(
            "[heartbeat-check] Running at {} (task: {})",
            Utc::now().to_rfc3339(),
            instance.task_id
        );
        // Simulate work
        sleep(Duration::from_millis(100)).await;
        // No explicit complete/reschedule: OnComplete::done() auto-reschedules for recurring tasks
        Ok(OnComplete::done())
    }
}

// ---------------------------------------------------------------------------
// Task: daily-report (Cron recurring task)
// ---------------------------------------------------------------------------

struct DailyReport;

#[async_trait]
impl ScheduledTask for DailyReport {
    async fn execute(
        &self,
        instance: &TaskInstance,
    ) -> Result<Box<dyn CompletionHandler>, SchedulerTaskError> {
        println!(
            "[daily-report] Generating report at {} (task: {})",
            Utc::now().to_rfc3339(),
            instance.task_id
        );
        sleep(Duration::from_millis(100)).await;
        Ok(OnComplete::done())
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    println!("=== Zart Recurring Tasks Example ===\n");

    let db_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://zart:zart@localhost:5432/zart".to_string());
    let pool = sqlx::PgPool::connect(&db_url).await?;

    let scheduler = Arc::new(PostgresTaskScheduler::new(pool));

    // Register task handlers
    let mut registry = TaskRegistry::new();
    registry.register("heartbeat-check", HeartbeatCheck);
    registry.register("daily-report", DailyReport);

    // Start worker
    let config = WorkerConfig {
        poll_interval: Duration::from_millis(500),
        max_tasks_per_poll: 10,
        max_concurrent_tasks: 2,
        shutdown_timeout: Duration::from_secs(10),
        ..Default::default()
    };
    let worker = Arc::new(Worker::new(scheduler.clone(), Arc::new(registry), config));
    let w = worker.clone();
    let handle = tokio::spawn(async move { w.run().await });

    // --- Demo 1: FixedDelay recurring task (every 2 seconds) ---
    println!("--- Demo 1: FixedDelay Recurring Task (every 2s) ---");
    scheduler
        .schedule_recurring(
            "heartbeat-check-1",
            "heartbeat-check",
            Recurrence::FixedDelay { duration_ms: 2000 },
            json!({}),
        )
        .await?;
    println!("Scheduled heartbeat-check (FixedDelay: 2s)\n");

    // --- Demo 2: Cron recurring task (every minute at :30) ---
    println!("--- Demo 2: Cron Recurring Task (every minute at :30) ---");
    scheduler
        .schedule_recurring(
            "daily-report-1",
            "daily-report",
            Recurrence::Cron {
                expression: "30 * * * * *".to_string(),
                timezone: "UTC".to_string(),
            },
            json!({}),
        )
        .await?;
    println!("Scheduled daily-report (Cron: 30 * * * * * UTC)\n");

    // Run for 5 seconds to see recurring executions
    println!("Running for 5 seconds to observe recurring tasks...\n");
    sleep(Duration::from_secs(5)).await;

    // Stop worker
    println!("\n=== Stopping worker ===");
    worker.stop();
    let _ = handle.await;

    Ok(())
}
