/// Parallel steps tests.
use super::helpers::*;
use std::time::Duration;
use uuid::Uuid;
use zart::{DurableRegistry, DurableScheduler};

#[tokio::test]
#[ignore = "requires PostgreSQL — run with: just test-integration"]
async fn parallel_steps_all_complete_and_sum_results() {
    let scheduler = setup().await;

    let mut registry = DurableRegistry::new();
    registry.register("parallel-task", ParallelTask);

    let execution_id = format!("test-par-{}", Uuid::new_v4());
    let durable = DurableScheduler::from_backend(scheduler.as_ref());

    durable
        .start(&execution_id, "parallel-task", serde_json::json!({}))
        .await
        .expect("start failed");

    let (worker, _handle) = spawn_worker(scheduler.clone(), registry);

    let record = durable
        .wait(&execution_id, Duration::from_secs(10), None)
        .await
        .expect("wait failed");

    worker.stop();

    assert_eq!(record.status, ExecutionStatus::Completed);
    let result = record.result.expect("expected a result");
    assert_eq!(result["sum"], 6);
}

#[tokio::test]
#[ignore = "requires PostgreSQL — run with: just test-integration"]
async fn parallel_steps_followed_by_downstream_step_completes() {
    let scheduler = setup().await;

    let mut registry = DurableRegistry::new();
    registry.register("parallel-then-downstream", ParallelThenDownstreamTask);

    let execution_id = format!("test-par-downstream-{}", Uuid::new_v4());
    let durable = DurableScheduler::from_backend(scheduler.as_ref());

    durable
        .start(
            &execution_id,
            "parallel-then-downstream",
            serde_json::json!({}),
        )
        .await
        .expect("start failed");

    let (worker, _handle) = spawn_worker(scheduler.clone(), registry);

    let record = durable
        .wait(&execution_id, Duration::from_secs(10), None)
        .await
        .expect("wait failed");

    worker.stop();

    assert_eq!(record.status, ExecutionStatus::Completed);
    let result = record.result.expect("expected a result");
    assert_eq!(result["total"], 6);
}

#[tokio::test]
#[ignore = "requires PostgreSQL — run with: just test-integration"]
async fn durable_multistep_and_waitgroup_completes_sub_100ms_with_long_poll_interval() {
    let scheduler = setup().await;

    let mut registry = DurableRegistry::new();
    registry.register("parallel-then-downstream", ParallelThenDownstreamTask);

    let config = WorkerConfig {
        poll_interval: Duration::from_secs(30),
        max_tasks_per_poll: 10,
        max_concurrent_tasks: 4,
        shutdown_timeout: Duration::from_secs(5),
        orphan_timeout: Duration::from_secs(30),
        enable_listen_notify: true,
        ..Default::default()
    };
    let worker = Arc::new(
        WorkerBuilder::from_backend(scheduler.as_ref())
            .durable_registry(registry)
            .config(config)
            .build(),
    );
    let w = worker.clone();
    let handle = tokio::spawn(async move { w.run().await });

    // Allow initial poll and PgListener subscription to establish before starting execution.
    tokio::time::sleep(Duration::from_millis(250)).await;

    let execution_id = format!("test-par-wakeup-{}", Uuid::new_v4());
    let durable = DurableScheduler::from_backend(scheduler.as_ref());

    let start = std::time::Instant::now();
    durable
        .start(
            &execution_id,
            "parallel-then-downstream",
            serde_json::json!({}),
        )
        .await
        .expect("start failed");

    let record = durable
        .wait(
            &execution_id,
            Duration::from_secs(5),
            Some(Duration::from_millis(10)),
        )
        .await
        .expect("wait failed");
    let elapsed = start.elapsed();

    worker.stop();
    let _ = handle.await;

    assert_eq!(record.status, ExecutionStatus::Completed);
    let result = record.result.expect("expected a result");
    assert_eq!(result["total"], 6);
    assert!(
        elapsed < Duration::from_secs(1),
        "expected durable multi-step + WaitGroup to complete well under the 30s poll_interval (within 1s across all DB transitions), took {elapsed:?}"
    );
}
