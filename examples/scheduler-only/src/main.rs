use async_trait::async_trait;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use uuid::Uuid;
use zart_scheduler::{
    ExecutionOps, PostgresTaskScheduler, ScheduledTask, SchedulerTaskError, TaskInstance,
    TaskRegistry, TaskScheduler, Worker, WorkerConfig,
};

// ---------------------------------------------------------------------------
// Task 1: send-welcome-email
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WelcomeEmailInput {
    user_id: String,
    email: String,
}

struct SendWelcomeEmail;

#[async_trait]
impl ScheduledTask for SendWelcomeEmail {
    async fn execute(
        &self,
        instance: &TaskInstance,
        ops: &mut ExecutionOps<'_>,
    ) -> Result<(), SchedulerTaskError> {
        let input: WelcomeEmailInput =
            serde_json::from_value(instance.data.clone()).map_err(|e| {
                SchedulerTaskError::Failed(format!("failed to parse input: {e}"))
            })?;

        println!(
            "  [send-welcome-email] Sending welcome email to {} ({})",
            input.email, input.user_id
        );

        // Simulate email API call
        sleep(Duration::from_millis(300)).await;

        // Chain: after email is sent, schedule onboarding cleanup
        let cleanup_id = format!("cleanup-{}", Uuid::new_v4());
        let cleanup_data = json!({
            "user_id": input.user_id,
            "action": "remove-pending-flag"
        });
        ops.schedule(zart_scheduler::ScheduleAtParams {
            task_id: cleanup_id,
            task_name: "onboarding-cleanup".to_string(),
            execution_time: Utc::now(),
            data: cleanup_data,
            recurrence: None,
            metadata: Value::Null,
        })
        .await
        .map_err(SchedulerTaskError::Storage)?;

        println!(
            "  [send-welcome-email] Email sent, scheduled onboarding-cleanup"
        );

        // Complete with a result payload
        ops.complete(Some(json!({
            "status": "sent",
            "user_id": input.user_id,
            "email": input.email
        })))
        .await
        .map_err(SchedulerTaskError::Storage)
    }
}

// ---------------------------------------------------------------------------
// Task 2: onboarding-cleanup
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CleanupInput {
    user_id: String,
    action: String,
}

struct OnboardingCleanup;

#[async_trait]
impl ScheduledTask for OnboardingCleanup {
    async fn execute(
        &self,
        instance: &TaskInstance,
        ops: &mut ExecutionOps<'_>,
    ) -> Result<(), SchedulerTaskError> {
        let input: CleanupInput = serde_json::from_value(instance.data.clone()).map_err(|e| {
            SchedulerTaskError::Failed(format!("failed to parse input: {e}"))
        })?;

        println!(
            "  [onboarding-cleanup] Running cleanup action '{}' for user {}",
            input.action, input.user_id
        );

        sleep(Duration::from_millis(200)).await;

        // Chain: schedule a report generation task
        let report_id = format!("report-{}", Uuid::new_v4());
        let report_data = json!({
            "user_id": input.user_id,
            "report_type": "onboarding-complete"
        });
        ops.schedule(zart_scheduler::ScheduleAtParams {
            task_id: report_id,
            task_name: "generate-report".to_string(),
            execution_time: Utc::now(),
            data: report_data,
            recurrence: None,
            metadata: Value::Null,
        })
        .await
        .map_err(SchedulerTaskError::Storage)?;

        println!(
            "  [onboarding-cleanup] Cleanup done, scheduled generate-report"
        );

        ops.complete(Some(json!({
            "status": "cleaned",
            "user_id": input.user_id
        })))
        .await
        .map_err(SchedulerTaskError::Storage)
    }
}

// ---------------------------------------------------------------------------
// Task 3: generate-report
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReportInput {
    user_id: String,
    report_type: String,
}

struct GenerateReport;

#[async_trait]
impl ScheduledTask for GenerateReport {
    async fn execute(
        &self,
        instance: &TaskInstance,
        ops: &mut ExecutionOps<'_>,
    ) -> Result<(), SchedulerTaskError> {
        let input: ReportInput = serde_json::from_value(instance.data.clone()).map_err(|e| {
            SchedulerTaskError::Failed(format!("failed to parse input: {e}"))
        })?;

        println!(
            "  [generate-report] Generating '{}' report for user {}",
            input.report_type, input.user_id
        );

        sleep(Duration::from_millis(150)).await;

        println!("  [generate-report] Report generated");

        ops.complete(Some(json!({
            "status": "generated",
            "user_id": input.user_id,
            "report_type": input.report_type
        })))
        .await
        .map_err(SchedulerTaskError::Storage)
    }
}

// ---------------------------------------------------------------------------
// Task 4: scheduled-greeting (demonstrates schedule_at with future time)
// ---------------------------------------------------------------------------

struct ScheduledGreeting;

#[async_trait]
impl ScheduledTask for ScheduledGreeting {
    async fn execute(
        &self,
        instance: &TaskInstance,
        ops: &mut ExecutionOps<'_>,
    ) -> Result<(), SchedulerTaskError> {
        let name = instance
            .data
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("World");

        println!("  [scheduled-greeting] Hello, {}! (task {})", name, instance.task_id);

        ops.complete(None)
            .await
            .map_err(SchedulerTaskError::Storage)
    }
}

// ---------------------------------------------------------------------------
// Helper: poll the database until a task is completed
// ---------------------------------------------------------------------------

async fn wait_for_task_completion(
    scheduler: &PostgresTaskScheduler,
    task_id: &str,
    timeout: Duration,
) -> Result<Option<Value>, String> {
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > timeout {
            return Err(format!("timeout waiting for task {task_id}"));
        }

        let row: Option<(String, Option<Value>)> = sqlx::query_as(
            "SELECT status::text, result FROM zart_tasks WHERE task_id = $1",
        )
        .bind(task_id)
        .fetch_optional(scheduler.pool())
        .await
        .map_err(|e| e.to_string())?;

        if let Some((status, result)) = row {
            if status == "completed" {
                return Ok(result);
            }
            if status == "failed" {
                return Err(format!("task {task_id} failed"));
            }
        }

        sleep(Duration::from_millis(100)).await;
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

    println!("=== Zart Scheduler-Only Example ===\n");

    let db_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://zart:zart@localhost:5432/zart".to_string());
    let pool = sqlx::PgPool::connect(&db_url).await?;

    let scheduler = Arc::new(PostgresTaskScheduler::new(pool));
    scheduler.run_migrations().await?;

    // Register task handlers
    let mut registry = TaskRegistry::new();
    registry.register("send-welcome-email", SendWelcomeEmail);
    registry.register("onboarding-cleanup", OnboardingCleanup);
    registry.register("generate-report", GenerateReport);
    registry.register("scheduled-greeting", ScheduledGreeting);

    // Start the worker
    let config = WorkerConfig {
        poll_interval: Duration::from_millis(200),
        max_tasks_per_poll: 10,
        max_concurrent_tasks: 4,
        shutdown_timeout: Duration::from_secs(10),
        orphan_timeout: Duration::from_secs(60),
        ..Default::default()
    };
    let worker = Arc::new(Worker::new(scheduler.clone(), Arc::new(registry), config));
    let w = worker.clone();
    let handle = tokio::spawn(async move { w.run().await });

    // --- Demo 1: Task chaining (email -> cleanup -> report) ---

    println!("--- Demo 1: Task Chaining ---");
    let chain_id = format!("chain-{}", Uuid::new_v4());
    let welcome_data = json!({
        "user_id": "user-42",
        "email": "alice@example.com"
    });

    println!("Scheduling send-welcome-email (task_id={chain_id})...\n");
    scheduler
        .schedule_now(&chain_id, "send-welcome-email", welcome_data)
        .await?;

    // Wait for the chain to complete by finding the last task (generate-report)
    let timeout = Duration::from_secs(15);
    println!("Waiting for chain to complete...\n");
    let report_id = find_task_by_name(&scheduler, "generate-report", timeout).await?;
    let result = wait_for_task_completion(&scheduler, &report_id, timeout).await?;
    println!("\nChain result: {:?}", result);

    // --- Demo 2: Schedule a task for the future ---

    println!("\n--- Demo 2: Scheduled Future Task ---");
    let greeting_id = format!("greeting-{}", Uuid::new_v4());
    let future_time = Utc::now() + chrono::Duration::seconds(3);
    scheduler
        .schedule_at(zart_scheduler::ScheduleAtParams {
            task_id: greeting_id.clone(),
            task_name: "scheduled-greeting".to_string(),
            execution_time: future_time,
            data: json!({ "name": "Paulo" }),
            recurrence: None,
            metadata: Value::Null,
        })
        .await?;
    println!(
        "Scheduled greeting for {} (in 3 seconds)",
        future_time.to_rfc3339()
    );

    wait_for_task_completion(&scheduler, &greeting_id, timeout).await?;
    println!("Future task completed!");

    // --- Demo 3: Schedule multiple independent tasks ---

    println!("\n--- Demo 3: Independent Parallel Tasks ---");
    let ids: Vec<_> = (0..3)
        .map(|i| format!("parallel-{}", i))
        .collect();
    for id in &ids {
        scheduler
            .schedule_now(
                id,
                "scheduled-greeting",
                json!({ "name": format!("User-{id}") }),
            )
            .await?;
    }
    println!("Scheduled 3 parallel greeting tasks");

    for id in &ids {
        wait_for_task_completion(&scheduler, id, timeout).await?;
    }
    println!("All parallel tasks completed!");

    // Shutdown
    println!("\n=== All demos completed ===");
    worker.stop();
    let _ = handle.await;

    Ok(())
}

/// Find a task row by task_name, polling until it appears.
async fn find_task_by_name(
    scheduler: &PostgresTaskScheduler,
    name: &str,
    timeout: Duration,
) -> Result<String, String> {
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > timeout {
            return Err(format!("timeout waiting for task named '{name}'"));
        }

        let row: Option<String> =
            sqlx::query_scalar("SELECT task_id FROM zart_tasks WHERE task_name = $1 LIMIT 1")
                .bind(name)
                .fetch_optional(scheduler.pool())
                .await
                .map_err(|e| e.to_string())?;

        if let Some(id) = row {
            return Ok(id);
        }

        sleep(Duration::from_millis(100)).await;
    }
}
