//! Multi-Worker LISTEN/NOTIFY Example
//!
//! Demonstrates 3 independent workers sharing a PostgreSQL queue with:
//! - Separate `PgBackend` instances (no shared in-process `local_wakeup`)
//! - `poll_interval = 30s` and `enable_listen_notify = true` (waking via `LISTEN/NOTIFY`)
//! - `FOR UPDATE SKIP LOCKED` + `Semaphore` backpressure (`max_concurrent_tasks = 2`)
//! - Continuous Drain Mode across a burst of 12 durable executions
//!
//! Run with:
//!   DATABASE_URL=postgres://zart:zart@localhost:5432/zart cargo run -p zart-examples --bin example-multi-worker

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use zart::error::TaskError;
use zart::prelude::*;
use zart::registry::DurableExecution;
use zart::{DurableRegistry, DurableScheduler, PgBackend, WorkerBuilder, WorkerConfig, zart_step};

#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
#[error("work step failed: {0}")]
struct WorkError(String);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct JobInput {
    job_index: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct JobOutput {
    job_index: usize,
    worker_label: String,
}

#[zart_step("process-item")]
async fn process_item(job_index: usize) -> Result<usize, WorkError> {
    tokio::time::sleep(Duration::from_millis(25)).await;
    Ok(job_index * 2)
}

struct MultiWorkerJob {
    worker_label: String,
    counter: Arc<AtomicUsize>,
}

#[async_trait]
impl DurableExecution for MultiWorkerJob {
    type Data = JobInput;
    type Output = JobOutput;

    async fn run(&self, data: Self::Data) -> Result<Self::Output, TaskError> {
        let doubled = zart::step_or_else(process_item(data.job_index), |_| 0).await?;
        let count = self.counter.fetch_add(1, Ordering::SeqCst) + 1;
        println!(
            "[{}] completed job #{} (doubled = {}, worker total = {})",
            self.worker_label, data.job_index, doubled, count
        );
        Ok(JobOutput {
            job_index: data.job_index,
            worker_label: self.worker_label.clone(),
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://zart:zart@localhost:5432/zart".to_string());

    let pool = PgPoolOptions::new()
        .max_connections(20)
        .connect(&database_url)
        .await?;

    let worker_count = 3usize;
    let total_jobs = 12usize;
    let counters: Vec<Arc<AtomicUsize>> = (0..worker_count)
        .map(|_| Arc::new(AtomicUsize::new(0)))
        .collect();

    let worker_config = WorkerConfig {
        max_concurrent_tasks: 2,
        max_tasks_per_poll: 2,
        ..Default::default()
    };

    let mut workers = Vec::with_capacity(worker_count);
    let mut handles = Vec::with_capacity(worker_count);

    for (idx, counter) in counters.iter().enumerate() {
        let label = format!("worker-{}", idx + 1);
        // Each worker uses its own PgBackend so wakeups come strictly from Postgres LISTEN/NOTIFY.
        let backend = PgBackend::new(pool.clone());
        let mut registry = DurableRegistry::new();
        registry.register(
            "multi-worker-job",
            MultiWorkerJob {
                worker_label: label,
                counter: Arc::clone(counter),
            },
        );

        let worker = Arc::new(
            WorkerBuilder::from_backend(&backend)
                .durable_registry(registry)
                .config(worker_config.clone())
                .build(),
        );
        let w = Arc::clone(&worker);
        handles.push(tokio::spawn(async move { w.run().await }));
        workers.push(worker);
    }

    // Give each worker's PgListener a moment to subscribe before enqueueing.
    tokio::time::sleep(Duration::from_millis(250)).await;

    let producer_backend = PgBackend::new(pool.clone());
    let durable = DurableScheduler::from_backend(&producer_backend);
    let run_prefix = uuid::Uuid::new_v4();

    println!(
        "Dispatching {} jobs across {} workers (poll_interval = 30s, LISTEN/NOTIFY enabled)...",
        total_jobs, worker_count
    );
    let started = Instant::now();

    let mut execution_ids = Vec::with_capacity(total_jobs);
    for i in 0..total_jobs {
        let exec_id = format!("multi-worker-{run_prefix}-{i}");
        durable
            .start(
                &exec_id,
                "multi-worker-job",
                serde_json::to_value(JobInput { job_index: i })?,
            )
            .await?;
        execution_ids.push(exec_id);
    }

    for exec_id in &execution_ids {
        let record = durable
            .wait(
                exec_id,
                Duration::from_secs(15),
                Some(Duration::from_millis(20)),
            )
            .await?;
        assert_eq!(record.status, ExecutionStatus::Completed);
    }
    let elapsed = started.elapsed();

    for worker in &workers {
        worker.stop();
    }
    for handle in handles {
        let _ = handle.await;
    }

    let total_handled: usize = counters.iter().map(|c| c.load(Ordering::SeqCst)).sum();
    println!("\nSummary:");
    println!("  Total jobs completed : {total_jobs}");
    println!("  Total handler runs   : {total_handled}");
    for (idx, counter) in counters.iter().enumerate() {
        println!(
            "  worker-{} handled     : {}",
            idx + 1,
            counter.load(Ordering::SeqCst)
        );
    }
    println!("  Elapsed time         : {elapsed:?} (vs 30s fallback poll_interval)");

    Ok(())
}
