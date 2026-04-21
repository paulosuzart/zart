#![allow(deprecated)]
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;
use zart::PostgresStorage;
use zart::error::TaskError;
use zart::prelude::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SleepInput {
    task_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SleepOutput {
    task_name: String,
    started_at: String,
    resumed_at: String,
}

struct SleepTask;

#[async_trait::async_trait]
impl DurableExecution for SleepTask {
    type Data = SleepInput;
    type Output = SleepOutput;

    async fn run(&self, data: Self::Data) -> Result<Self::Output, TaskError> {
        let started_at = zart::capture!("started-at", chrono::Utc::now());

        zart::sleep("initial-sleep", Duration::from_secs(5)).await?;

        let resumed_at = zart::capture!("resumed-at", chrono::Utc::now());

        Ok(SleepOutput {
            task_name: data.task_name,
            started_at: started_at.to_rfc3339(),
            resumed_at: resumed_at.to_rfc3339(),
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    println!("=== Zart Sleep Example ===\n");

    let db_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://zart:zart@localhost:5432/zart".to_string());

    let pool = sqlx::PgPool::connect(&db_url).await?;
    let sched = Arc::new(PostgresStorage::new(pool));

    let mut registry = TaskRegistry::new();
    registry.register("sleep-task", SleepTask);
    let registry = Arc::new(registry);

    let execution_id = format!("sleep-demo-{}", Uuid::new_v4());
    let durable = DurableScheduler::new(sched.clone(), sched.task_scheduler());

    let input = SleepInput {
        task_name: "demo".to_string(),
    };

    println!("Starting execution '{}'...\n", execution_id);
    durable
        .start_for::<SleepTask>(&execution_id, "sleep-task", &input)
        .await?;

    let config = zart::WorkerConfig {
        poll_interval: Duration::from_millis(200),
        max_tasks_per_poll: 10,
        max_concurrent_tasks: 4,
        shutdown_timeout: Duration::from_secs(30),
        orphan_timeout: Duration::from_secs(60),
        ..Default::default()
    };
    let worker = Arc::new(zart::Worker::new(
        sched.task_scheduler(),
        sched.clone(),
        registry.clone(),
        config,
    ));
    let w = worker.clone();
    let _handle = tokio::spawn(async move { w.run().await });

    let output: SleepOutput = durable
        .wait_completion(&execution_id, Duration::from_secs(30), None)
        .await?;

    worker.stop();

    println!("\n=== Execution Completed ===");
    println!("  Task:       {}", output.task_name);
    println!("  Started:    {}", output.started_at);
    println!("  Resumed:    {}", output.resumed_at);

    Ok(())
}
