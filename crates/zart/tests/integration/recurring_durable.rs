//! Integration tests for recurring durable executions (spec 0053).
//!
//! Tests marked `#[ignore]` require a running PostgreSQL instance.
//! Start it with `just up`, then run: `just test-integration`

use super::helpers::*;
use sqlx::Row as _;
use std::time::Duration;
use uuid::Uuid;
use zart::{DurableScheduler, ListExecutionsParams, OverlapPolicy};
use zart_core::types::ExecutionStatus;
use zart_scheduler::Recurrence;

// ── Handlers ──────────────────────────────────────────────────────────────────

struct FastHandler;

#[async_trait::async_trait]
impl DurableExecution for FastHandler {
    type Data = serde_json::Value;
    type Output = serde_json::Value;

    async fn run(&self, _data: Self::Data) -> Result<Self::Output, zart::error::TaskError> {
        Ok(serde_json::json!({ "done": true }))
    }
}

struct SlowHandlerShared;

#[async_trait::async_trait]
impl DurableExecution for SlowHandlerShared {
    type Data = serde_json::Value;
    type Output = serde_json::Value;

    async fn run(&self, _data: Self::Data) -> Result<Self::Output, zart::error::TaskError> {
        // Sleep long enough to outlast several tick intervals without polluting the runtime.
        tokio::time::sleep(Duration::from_secs(2)).await;
        Ok(serde_json::json!({ "done": true }))
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires PostgreSQL — run with: just test-integration"]
async fn basic_recurrence_fixed_delay_runs_multiple_occurrences() {
    let pg = setup().await;
    let uid = Uuid::new_v4().simple().to_string();
    let task_id = format!("rdt-basic-{uid}");
    let prefix = uid.clone();

    let config = WorkerConfig {
        poll_interval: Duration::from_millis(25),
        max_tasks_per_poll: 10,
        max_concurrent_tasks: 4,
        shutdown_timeout: Duration::from_secs(2),
        orphan_timeout: Duration::from_secs(30),
        ..Default::default()
    };

    let worker = Arc::new(
        WorkerBuilder::from_backend(pg.as_ref())
            .register_durable_task(&task_id, FastHandler)
            .register_recurring_durable::<FastHandler>(
                &task_id,
                &format!("job-{{occurrence}}-{prefix}"),
                Recurrence::FixedDelay { duration_ms: 50 },
                OverlapPolicy::AlwaysStart,
                serde_json::json!({}),
            )
            .config(config)
            .build(),
    );

    let w = worker.clone();
    tokio::spawn(async move { w.run().await });

    // Wait for at least 3 ticks to fire and complete (extra headroom for CI load).
    tokio::time::sleep(Duration::from_millis(1500)).await;
    worker.stop();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let durable = DurableScheduler::from_backend(pg.as_ref());
    let all = durable
        .list_executions(ListExecutionsParams {
            limit: 200,
            ..Default::default()
        })
        .await
        .expect("list failed");

    let matching: Vec<_> = all
        .iter()
        .filter(|e| e.execution_id.contains(&prefix))
        .collect();

    assert!(
        matching.len() >= 3,
        "expected at least 3 executions, got {}: {:?}",
        matching.len(),
        matching.iter().map(|e| &e.execution_id).collect::<Vec<_>>()
    );

    // Verify job-0, job-1, job-2 exist
    for i in 0..3u64 {
        let expected_id = format!("job-{i}-{prefix}");
        assert!(
            matching.iter().any(|e| e.execution_id == expected_id),
            "missing execution {expected_id}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires PostgreSQL — run with: just test-integration"]
async fn skip_if_running_skips_second_tick_when_first_still_running() {
    let pg = setup().await;
    let uid = Uuid::new_v4().simple().to_string();
    let task_id = format!("rdt-skip-{uid}");
    let prefix = uid.clone();

    let config = WorkerConfig {
        poll_interval: Duration::from_millis(25),
        max_tasks_per_poll: 10,
        max_concurrent_tasks: 4,
        shutdown_timeout: Duration::from_secs(2),
        orphan_timeout: Duration::from_secs(30),
        ..Default::default()
    };

    let worker = Arc::new(
        WorkerBuilder::from_backend(pg.as_ref())
            .register_durable_task(&task_id, SlowHandlerShared)
            .register_recurring_durable::<SlowHandlerShared>(
                &task_id,
                &format!("skip-{{occurrence}}-{prefix}"),
                // Tick fires every 80ms; handler sleeps 10s so all subsequent ticks skip.
                Recurrence::FixedDelay { duration_ms: 80 },
                OverlapPolicy::SkipIfRunning,
                serde_json::json!({}),
            )
            .config(config)
            .build(),
    );

    let w = worker.clone();
    tokio::spawn(async move { w.run().await });

    // Let several ticks fire while handler is sleeping (only tick 1 should start an execution).
    tokio::time::sleep(Duration::from_millis(400)).await;
    worker.stop();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let durable = DurableScheduler::from_backend(pg.as_ref());
    let all = durable
        .list_executions(ListExecutionsParams {
            limit: 50,
            ..Default::default()
        })
        .await
        .expect("list failed");

    let matching: Vec<_> = all
        .iter()
        .filter(|e| e.execution_id.contains(&prefix))
        .collect();

    assert_eq!(
        matching.len(),
        1,
        "SkipIfRunning: expected exactly 1 execution, got {}: {:?}",
        matching.len(),
        matching.iter().map(|e| &e.execution_id).collect::<Vec<_>>()
    );
    // The handler is still sleeping when the worker stops, so the execution is non-terminal.
    assert!(
        matches!(
            matching[0].status,
            ExecutionStatus::Scheduled | ExecutionStatus::Running
        ),
        "expected non-terminal status, got {:?}",
        matching[0].status
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires PostgreSQL — run with: just test-integration"]
async fn cancel_and_restart_cancels_first_and_starts_second() {
    let pg = setup().await;
    let uid = Uuid::new_v4().simple().to_string();
    let task_id = format!("rdt-cancel-{uid}");
    let prefix = uid.clone();

    let config = WorkerConfig {
        poll_interval: Duration::from_millis(25),
        max_tasks_per_poll: 10,
        max_concurrent_tasks: 4,
        shutdown_timeout: Duration::from_secs(2),
        orphan_timeout: Duration::from_secs(30),
        ..Default::default()
    };

    let worker = Arc::new(
        WorkerBuilder::from_backend(pg.as_ref())
            .register_durable_task(&task_id, SlowHandlerShared)
            .register_recurring_durable::<SlowHandlerShared>(
                &task_id,
                &format!("cancel-{{occurrence}}-{prefix}"),
                // Tick every 100ms; handler sleeps 10s so first run is still running when tick 2 fires.
                Recurrence::FixedDelay { duration_ms: 100 },
                OverlapPolicy::CancelAndRestart,
                serde_json::json!({}),
            )
            .config(config)
            .build(),
    );

    let w = worker.clone();
    tokio::spawn(async move { w.run().await });

    // Wait long enough for at least 2 ticks; second tick cancels first run and starts a new one.
    tokio::time::sleep(Duration::from_millis(400)).await;
    worker.stop();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let durable = DurableScheduler::from_backend(pg.as_ref());
    let all = durable
        .list_executions(ListExecutionsParams {
            limit: 50,
            ..Default::default()
        })
        .await
        .expect("list failed");

    let matching: Vec<_> = all
        .iter()
        .filter(|e| e.execution_id.contains(&prefix))
        .collect();

    assert!(
        !matching.is_empty(),
        "expected at least one execution, got none"
    );

    // At least one should be cancelled
    let cancelled = matching
        .iter()
        .filter(|e| e.status == ExecutionStatus::Cancelled)
        .count();
    assert!(
        cancelled >= 1,
        "CancelAndRestart: expected at least 1 cancelled execution, got 0. statuses: {:?}",
        matching.iter().map(|e| &e.status).collect::<Vec<_>>()
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires PostgreSQL — run with: just test-integration"]
async fn always_start_runs_multiple_executions_in_parallel() {
    let pg = setup().await;
    let uid = Uuid::new_v4().simple().to_string();
    let task_id = format!("rdt-always-{uid}");
    let prefix = uid.clone();

    let config = WorkerConfig {
        poll_interval: Duration::from_millis(25),
        max_tasks_per_poll: 10,
        max_concurrent_tasks: 8,
        shutdown_timeout: Duration::from_secs(2),
        orphan_timeout: Duration::from_secs(30),
        ..Default::default()
    };

    let worker = Arc::new(
        WorkerBuilder::from_backend(pg.as_ref())
            .register_durable_task(&task_id, FastHandler)
            .register_recurring_durable::<FastHandler>(
                &task_id,
                &format!("always-{{occurrence}}-{prefix}"),
                Recurrence::FixedDelay { duration_ms: 60 },
                OverlapPolicy::AlwaysStart,
                serde_json::json!({}),
            )
            .config(config)
            .build(),
    );

    let w = worker.clone();
    tokio::spawn(async move { w.run().await });

    // Wait for multiple ticks
    tokio::time::sleep(Duration::from_millis(500)).await;
    worker.stop();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let durable = DurableScheduler::from_backend(pg.as_ref());
    let all = durable
        .list_executions(ListExecutionsParams {
            limit: 50,
            ..Default::default()
        })
        .await
        .expect("list failed");

    let matching: Vec<_> = all
        .iter()
        .filter(|e| e.execution_id.contains(&prefix))
        .collect();

    assert!(
        matching.len() >= 2,
        "AlwaysStart: expected at least 2 executions with distinct IDs, got {}: {:?}",
        matching.len(),
        matching.iter().map(|e| &e.execution_id).collect::<Vec<_>>()
    );

    // Verify IDs are distinct
    let unique_ids: std::collections::HashSet<_> =
        matching.iter().map(|e| e.execution_id.as_str()).collect();
    assert_eq!(
        unique_ids.len(),
        matching.len(),
        "AlwaysStart: expected distinct execution IDs"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires PostgreSQL — run with: just test-integration"]
async fn metadata_occurrence_increments_across_ticks() {
    let pg = setup().await;
    let uid = Uuid::new_v4().simple().to_string();
    let task_id = format!("rdt-meta-{uid}");
    let prefix = uid.clone();

    let config = WorkerConfig {
        poll_interval: Duration::from_millis(25),
        max_tasks_per_poll: 10,
        max_concurrent_tasks: 4,
        shutdown_timeout: Duration::from_secs(2),
        orphan_timeout: Duration::from_secs(30),
        ..Default::default()
    };

    let worker = Arc::new(
        WorkerBuilder::from_backend(pg.as_ref())
            .register_durable_task(&task_id, FastHandler)
            .register_recurring_durable::<FastHandler>(
                &task_id,
                &format!("meta-{{occurrence}}-{prefix}"),
                Recurrence::FixedDelay { duration_ms: 50 },
                OverlapPolicy::AlwaysStart,
                serde_json::json!({}),
            )
            .config(config)
            .build(),
    );

    let w = worker.clone();
    tokio::spawn(async move { w.run().await });

    // Wait for several ticks
    tokio::time::sleep(Duration::from_millis(500)).await;
    worker.stop();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Check that executions with incrementing occurrence IDs exist
    let durable = DurableScheduler::from_backend(pg.as_ref());
    let all = durable
        .list_executions(ListExecutionsParams {
            limit: 50,
            ..Default::default()
        })
        .await
        .expect("list failed");

    let matching: Vec<_> = all
        .iter()
        .filter(|e| e.execution_id.contains(&prefix))
        .collect();

    assert!(
        matching.len() >= 2,
        "expected at least 2 occurrences, got {}",
        matching.len()
    );

    // Verify occurrence counter embedded in IDs: meta-0-..., meta-1-..., meta-2-...
    for i in 0..matching.len().min(3) {
        let expected = format!("meta-{i}-{prefix}");
        assert!(
            matching.iter().any(|e| e.execution_id == expected),
            "missing execution for occurrence {i}: expected {expected}"
        );
    }

    // Probe the zart_tasks metadata column directly to confirm the occurrence
    // counter was persisted and incremented.
    let scheduler_task_id = format!("__zart_recurring__:{task_id}");
    let pool = pg.pool().clone();
    let row = sqlx::query("SELECT metadata FROM zart_tasks WHERE task_id = $1")
        .bind(&scheduler_task_id)
        .fetch_one(&pool)
        .await
        .expect("should find scheduler task row");
    let metadata: serde_json::Value = row.try_get("metadata").expect("metadata column");
    let occurrence = metadata["occurrence"]
        .as_u64()
        .expect("occurrence must be u64");
    assert!(
        occurrence >= 2,
        "occurrence should have incremented, got {occurrence}"
    );
}
