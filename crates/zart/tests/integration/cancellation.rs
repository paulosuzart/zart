/// Cancellation-during-execution tests.
///
/// These tests cover the race condition where a durable execution is cancelled
/// while its task is already `picked_up` by a worker.
use super::helpers::*;
use std::time::Duration;
use uuid::Uuid;
use zart::{DurableScheduler, TaskRegistry};

#[tokio::test]
#[ignore = "requires PostgreSQL — run with: just test-integration"]
async fn cancelled_execution_not_overwritten_when_handler_succeeds() {
    let scheduler = setup().await;
    let started = Arc::new(tokio::sync::Notify::new());
    let gate = Arc::new(tokio::sync::Notify::new());

    let mut registry = TaskRegistry::new();
    registry.register(
        "gated-task",
        GatedTask {
            started: started.clone(),
            gate: gate.clone(),
        },
    );
    let registry = Arc::new(registry);

    let execution_id = format!("test-cancel-race-{}", Uuid::new_v4());
    let durable = DurableScheduler::new(scheduler.clone());
    durable
        .start(&execution_id, "gated-task", serde_json::json!({}))
        .await
        .expect("start failed");

    let (_worker, _handle) = spawn_worker(scheduler.clone(), registry);

    started.notified().await;

    durable.cancel(&execution_id).await.expect("cancel failed");

    gate.notify_one();

    tokio::time::sleep(Duration::from_millis(500)).await;

    let record = durable.status(&execution_id).await.expect("status failed");
    assert_eq!(
        record.status,
        ExecutionStatus::Cancelled,
        "expected Cancelled but got {:?}",
        record.status
    );
}

#[tokio::test]
#[ignore = "requires PostgreSQL — run with: just test-integration"]
async fn cancelled_execution_not_requeued_on_step_scheduled() {
    let scheduler = setup().await;
    let started = Arc::new(tokio::sync::Notify::new());
    let gate = Arc::new(tokio::sync::Notify::new());

    let mut registry = TaskRegistry::new();
    registry.register(
        "gated-step-task",
        GatedStepTask {
            started: started.clone(),
            gate: gate.clone(),
        },
    );
    let registry = Arc::new(registry);

    let execution_id = format!("test-cancel-step-race-{}", Uuid::new_v4());
    let durable = DurableScheduler::new(scheduler.clone());
    durable
        .start(&execution_id, "gated-step-task", serde_json::json!({}))
        .await
        .expect("start failed");

    let (_worker, _handle) = spawn_worker(scheduler.clone(), registry);

    started.notified().await;

    durable.cancel(&execution_id).await.expect("cancel failed");

    gate.notify_one();

    tokio::time::sleep(Duration::from_millis(500)).await;

    let record = durable.status(&execution_id).await.expect("status failed");
    assert_eq!(
        record.status,
        ExecutionStatus::Cancelled,
        "expected Cancelled but got {:?}",
        record.status
    );
}
