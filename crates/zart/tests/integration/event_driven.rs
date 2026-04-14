//! Event-driven execution tests.

use super::helpers::*;
use std::time::Duration;
use uuid::Uuid;
use zart::{DurableScheduler, TaskRegistry};

#[tokio::test]
#[ignore = "requires PostgreSQL — run with: just test-integration"]
async fn wait_for_event_resumes_execution_after_offer_event() {
    let scheduler = setup().await;

    let mut registry = TaskRegistry::new();
    registry.register("wait-event-task", WaitEventTask);
    let registry = Arc::new(registry);

    let execution_id = format!("test-event-{}", Uuid::new_v4());
    let durable = DurableScheduler::new(scheduler.clone());

    durable
        .start(&execution_id, "wait-event-task", serde_json::json!({}))
        .await
        .expect("start failed");

    let (worker, _handle) = spawn_worker(scheduler.clone(), registry);

    tokio::time::sleep(Duration::from_millis(500)).await;

    durable
        .offer_event(
            &execution_id,
            "approve",
            serde_json::json!({ "approved": true }),
        )
        .await
        .expect("offer_event failed");

    let record = durable
        .wait(&execution_id, Duration::from_secs(10), None)
        .await
        .expect("wait failed");

    worker.stop();

    assert_eq!(record.status, ExecutionStatus::Completed);
    let result = record.result.expect("expected a result");
    assert_eq!(result["approved"], true);
}

#[tokio::test]
#[ignore = "requires PostgreSQL — run with: just test-integration"]
async fn cancel_stops_execution_before_completion() {
    let scheduler = setup().await;

    let mut registry = TaskRegistry::new();
    registry.register("wait-event-task", WaitEventTask);
    let registry = Arc::new(registry);

    let execution_id = format!("test-cancel-{}", Uuid::new_v4());
    let durable = DurableScheduler::new(scheduler.clone());

    durable
        .start(&execution_id, "wait-event-task", serde_json::json!({}))
        .await
        .expect("start failed");

    let (worker, _handle) = spawn_worker(scheduler.clone(), registry);
    tokio::time::sleep(Duration::from_millis(500)).await;

    let cancelled = durable.cancel(&execution_id).await.expect("cancel failed");
    assert!(cancelled, "expected execution to be cancelled");

    let record = durable.status(&execution_id).await.expect("status failed");
    assert_eq!(record.status, ExecutionStatus::Cancelled);

    worker.stop();
}

#[tokio::test]
#[ignore = "requires PostgreSQL — run with: just test-integration"]
async fn wait_with_timeout_returns_error_when_execution_does_not_complete() {
    let scheduler = setup().await;

    let mut registry = TaskRegistry::new();
    registry.register("wait-event-task", WaitEventTask);
    let registry = Arc::new(registry);

    let execution_id = format!("test-timeout-{}", Uuid::new_v4());
    let durable = DurableScheduler::new(scheduler.clone());

    durable
        .start(&execution_id, "wait-event-task", serde_json::json!({}))
        .await
        .expect("start failed");

    let (worker, _handle) = spawn_worker(scheduler.clone(), registry);

    let result = durable
        .wait_with_timeout(&execution_id, Duration::from_millis(300))
        .await;

    worker.stop();

    assert!(
        matches!(result, Err(zart::SchedulerError::WaitTimedOut(_))),
        "expected WaitTimedOut, got {result:?}"
    );
}
