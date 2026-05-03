//! Recurring Durable Executions Example
//!
//! Demonstrates three overlap-policy scenarios using `FixedDelay` recurrence:
//!
//! - **Scenario A — `SkipIfRunning`**: slow inventory snapshot handler; second tick
//!   is skipped because the first is still running.
//! - **Scenario B — `CancelAndRestart`**: config refresh; stale run is cancelled
//!   and a fresh one starts on every tick.
//! - **Scenario C — `AlwaysStart`**: independent audit windows that always run in
//!   parallel regardless of overlap.

use async_trait::async_trait;
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use zart::postgres::PgBackend;
use zart::{
    DurableScheduler, OverlapPolicy, WorkerBuilder, WorkerConfig, error::TaskError,
    registry::DurableExecution,
};
use zart_scheduler::Recurrence;

// ── Scenario A: InventorySnapshot (SkipIfRunning) ────────────────────────────

/// Simulates a slow nightly inventory snapshot (sleeps 300 ms).
/// When the second tick fires at ~100 ms the first is still running, so the
/// second occurrence is skipped.
struct InventorySnapshot;

#[async_trait]
impl DurableExecution for InventorySnapshot {
    type Data = serde_json::Value;
    type Output = serde_json::Value;

    async fn run(&self, data: Self::Data) -> Result<Self::Output, TaskError> {
        let warehouse = data["warehouse"].as_str().unwrap_or("unknown");
        println!("[InventorySnapshot] Starting snapshot for warehouse: {warehouse}");
        // Simulate slow report generation
        sleep(Duration::from_millis(300)).await;
        println!("[InventorySnapshot] Snapshot done for warehouse: {warehouse}");
        Ok(json!({ "warehouse": warehouse, "items_counted": 1234 }))
    }
}

// ── Scenario B: ConfigRefresh (CancelAndRestart) ──────────────────────────────

/// Simulates a slow config refresh (sleeps 300 ms).
/// When the second tick fires the stale run is cancelled and a fresh one starts.
struct ConfigRefresh;

#[async_trait]
impl DurableExecution for ConfigRefresh {
    type Data = serde_json::Value;
    type Output = serde_json::Value;

    async fn run(&self, _data: Self::Data) -> Result<Self::Output, TaskError> {
        println!("[ConfigRefresh] Loading latest configuration…");
        sleep(Duration::from_millis(300)).await;
        println!("[ConfigRefresh] Configuration loaded.");
        Ok(json!({ "version": "v2.0", "feature_flags": { "new_ui": true } }))
    }
}

// ── Scenario C: AuditWindow (AlwaysStart) ────────────────────────────────────

/// Independent audit windows — each occurrence runs fully regardless of overlap.
struct AuditWindow;

#[async_trait]
impl DurableExecution for AuditWindow {
    type Data = serde_json::Value;
    type Output = serde_json::Value;

    async fn run(&self, _data: Self::Data) -> Result<Self::Output, TaskError> {
        println!("[AuditWindow] Opening audit window…");
        sleep(Duration::from_millis(200)).await;
        println!("[AuditWindow] Audit window closed.");
        Ok(json!({ "events_audited": 42 }))
    }
}

// ── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    let db_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://zart:zart@localhost:5432/zart".to_string());
    let pool = sqlx::PgPool::connect(&db_url).await?;
    let pg = Arc::new(PgBackend::new(pool));
    pg.run_migrations().await?;

    // ── Scenario A: SkipIfRunning ────────────────────────────────────────────
    println!("\n=== Scenario A: SkipIfRunning ===");
    println!("Handler sleeps 300ms; tick fires every 100ms → second tick skipped.\n");

    run_scenario_a(pg.clone()).await?;

    // ── Scenario B: CancelAndRestart ─────────────────────────────────────────
    println!("\n=== Scenario B: CancelAndRestart ===");
    println!("Handler sleeps 300ms; tick fires every 150ms → first run cancelled.\n");

    run_scenario_b(pg.clone()).await?;

    // ── Scenario C: AlwaysStart ───────────────────────────────────────────────
    println!("\n=== Scenario C: AlwaysStart ===");
    println!("Each occurrence starts independently; two runs overlap in parallel.\n");

    run_scenario_c(pg.clone()).await?;

    println!("\n=== All scenarios complete ===");
    Ok(())
}

async fn run_scenario_a(pg: Arc<PgBackend>) -> Result<(), Box<dyn std::error::Error>> {
    let task_id = format!("inventory-snapshot-{}", uuid::Uuid::new_v4().simple());

    let config = WorkerConfig {
        poll_interval: Duration::from_millis(50),
        max_tasks_per_poll: 10,
        max_concurrent_tasks: 4,
        shutdown_timeout: Duration::from_secs(2),
        ..Default::default()
    };

    let worker = Arc::new(
        WorkerBuilder::from_backend(pg.as_ref())
            .register_durable_task(&task_id, InventorySnapshot)
            .register_recurring_durable::<InventorySnapshot>(
                &task_id,
                &format!("snapshot-{{occurrence}}-{}", &task_id[..8]),
                // Cron alternative: Recurrence::Cron { expression: "0 2 * * *".into(), timezone: "UTC".into() }
                Recurrence::FixedDelay { duration_ms: 100 },
                OverlapPolicy::SkipIfRunning,
                json!({ "warehouse": "EU-1" }),
            )
            .config(config)
            .build(),
    );

    let w = worker.clone();
    tokio::spawn(async move { w.run().await });

    // Wait long enough for at least 2 ticks to fire (the second should be skipped)
    sleep(Duration::from_millis(600)).await;
    worker.stop();
    sleep(Duration::from_millis(200)).await;

    // Query executions
    let durable = DurableScheduler::from_backend(pg.as_ref());
    let executions = durable
        .list_executions(zart::ListExecutionsParams {
            limit: 20,
            ..Default::default()
        })
        .await?;

    let scenario_execs: Vec<_> = executions
        .iter()
        .filter(|e| e.execution_id.contains(&task_id[..8]))
        .collect();

    println!(
        "Scenario A: {} execution(s) created (expected 1 — second tick skipped):",
        scenario_execs.len()
    );
    for e in &scenario_execs {
        println!("  - {} [{:?}]", e.execution_id, e.status);
    }

    Ok(())
}

async fn run_scenario_b(pg: Arc<PgBackend>) -> Result<(), Box<dyn std::error::Error>> {
    let task_id = format!("config-refresh-{}", uuid::Uuid::new_v4().simple());

    let config = WorkerConfig {
        poll_interval: Duration::from_millis(50),
        max_tasks_per_poll: 10,
        max_concurrent_tasks: 4,
        shutdown_timeout: Duration::from_secs(2),
        ..Default::default()
    };

    let worker = Arc::new(
        WorkerBuilder::from_backend(pg.as_ref())
            .register_durable_task(&task_id, ConfigRefresh)
            .register_recurring_durable::<ConfigRefresh>(
                &task_id,
                &format!("config-{{occurrence}}-{}", &task_id[..6]),
                Recurrence::FixedDelay { duration_ms: 150 },
                OverlapPolicy::CancelAndRestart,
                json!({}),
            )
            .config(config)
            .build(),
    );

    let w = worker.clone();
    tokio::spawn(async move { w.run().await });

    // Wait for at least 2 ticks
    sleep(Duration::from_millis(700)).await;
    worker.stop();
    sleep(Duration::from_millis(200)).await;

    let durable = DurableScheduler::from_backend(pg.as_ref());
    let executions = durable
        .list_executions(zart::ListExecutionsParams {
            limit: 20,
            ..Default::default()
        })
        .await?;

    let scenario_execs: Vec<_> = executions
        .iter()
        .filter(|e| e.execution_id.contains(&task_id[..6]))
        .collect();

    println!(
        "Scenario B: {} execution(s) created (first cancelled, latest completes):",
        scenario_execs.len()
    );
    for e in &scenario_execs {
        println!("  - {} [{:?}]", e.execution_id, e.status);
    }

    Ok(())
}

async fn run_scenario_c(pg: Arc<PgBackend>) -> Result<(), Box<dyn std::error::Error>> {
    let task_id = format!("audit-window-{}", uuid::Uuid::new_v4().simple());

    let config = WorkerConfig {
        poll_interval: Duration::from_millis(50),
        max_tasks_per_poll: 10,
        max_concurrent_tasks: 4,
        shutdown_timeout: Duration::from_secs(2),
        ..Default::default()
    };

    let worker = Arc::new(
        WorkerBuilder::from_backend(pg.as_ref())
            .register_durable_task(&task_id, AuditWindow)
            .register_recurring_durable::<AuditWindow>(
                &task_id,
                &format!("audit-{{occurrence}}-{}", &task_id[..6]),
                Recurrence::FixedDelay { duration_ms: 50 },
                OverlapPolicy::AlwaysStart,
                json!({}),
            )
            .config(config)
            .build(),
    );

    let w = worker.clone();
    tokio::spawn(async move { w.run().await });

    // Wait for several ticks to fire in parallel
    sleep(Duration::from_millis(500)).await;
    worker.stop();
    sleep(Duration::from_millis(200)).await;

    let durable = DurableScheduler::from_backend(pg.as_ref());
    let executions = durable
        .list_executions(zart::ListExecutionsParams {
            limit: 20,
            ..Default::default()
        })
        .await?;

    let scenario_execs: Vec<_> = executions
        .iter()
        .filter(|e| e.execution_id.contains(&task_id[..6]))
        .collect();

    println!(
        "Scenario C: {} execution(s) created (multiple parallel runs expected):",
        scenario_execs.len()
    );
    for e in &scenario_execs {
        println!("  - {} [{:?}]", e.execution_id, e.status);
    }

    Ok(())
}
