/// Typed completion API tests.
use super::helpers::*;
use std::time::Duration;
use uuid::Uuid;
use zart::{DurableRegistry, DurableScheduler};

#[tokio::test]
#[ignore]
async fn wait_completion_returns_typed_result() {
    let scheduler = setup().await;
    let mut registry = DurableRegistry::new();
    registry.register("zart::tests::integration::TypedTask", TypedTask);

    let (worker, handle) = spawn_worker(scheduler.clone(), registry);
    let durable = DurableScheduler::new(scheduler.clone(), scheduler.task_scheduler());

    let execution_id = format!("typed-wait-{}", Uuid::new_v4());
    let input = TypedInput { multiplier: 21 };
    durable
        .start_for::<TypedTask>(&execution_id, "zart::tests::integration::TypedTask", &input)
        .await
        .expect("start failed");

    let output: TypedOutput = durable
        .wait_completion(&execution_id, Duration::from_secs(10), None)
        .await
        .expect("wait_completion failed");

    assert_eq!(output.result, 42);

    worker.stop();
    let _ = handle.await;
}

#[tokio::test]
#[ignore]
async fn wait_completion_with_timeout_returns_typed_result() {
    let scheduler = setup().await;
    let mut registry = DurableRegistry::new();
    registry.register("zart::tests::integration::TypedTask", TypedTask);

    let (worker, handle) = spawn_worker(scheduler.clone(), registry);
    let durable = DurableScheduler::new(scheduler.clone(), scheduler.task_scheduler());

    let execution_id = format!("typed-wait-timeout-{}", Uuid::new_v4());
    let input = TypedInput { multiplier: 10 };
    durable
        .start_for::<TypedTask>(&execution_id, "zart::tests::integration::TypedTask", &input)
        .await
        .expect("start failed");

    let output: TypedOutput = durable
        .wait_completion_with_timeout(&execution_id, Duration::from_secs(10))
        .await
        .expect("wait_completion_with_timeout failed");

    assert_eq!(output.result, 20);

    worker.stop();
    let _ = handle.await;
}

#[tokio::test]
#[ignore]
async fn start_and_wait_for_returns_typed_result() {
    let scheduler = setup().await;
    let mut registry = DurableRegistry::new();
    registry.register("zart::tests::integration::TypedTask", TypedTask);

    let (worker, handle) = spawn_worker(scheduler.clone(), registry);
    let durable = DurableScheduler::new(scheduler.clone(), scheduler.task_scheduler());

    let execution_id = format!("typed-start-and-wait-{}", Uuid::new_v4());
    let input = TypedInput { multiplier: 7 };

    let output = durable
        .start_and_wait_for::<TypedTask>(
            &execution_id,
            "zart::tests::integration::TypedTask",
            &input,
            Duration::from_secs(10),
        )
        .await
        .expect("start_and_wait_for failed");

    assert_eq!(output.result, 14);

    worker.stop();
    let _ = handle.await;
}

#[tokio::test]
#[ignore]
async fn start_and_wait_for_infers_types_from_handler() {
    let scheduler = setup().await;
    let mut registry = DurableRegistry::new();
    registry.register("zart::tests::integration::TypedTask", TypedTask);

    let (worker, handle) = spawn_worker(scheduler.clone(), registry);
    let durable = DurableScheduler::new(scheduler.clone(), scheduler.task_scheduler());

    let execution_id = format!("typed-start-for-{}", Uuid::new_v4());
    let input = TypedInput { multiplier: 5 };

    let output = durable
        .start_and_wait_for::<TypedTask>(
            &execution_id,
            "zart::tests::integration::TypedTask",
            &input,
            Duration::from_secs(10),
        )
        .await
        .expect("start_and_wait_for failed");

    assert_eq!(output.result, 10);

    worker.stop();
    let _ = handle.await;
}

#[tokio::test]
#[ignore]
async fn wait_completion_fails_when_no_result() {
    let scheduler = setup().await;
    let durable = DurableScheduler::new(scheduler.clone(), scheduler.task_scheduler());

    let execution_id = format!("typed-wait-no-result-{}", Uuid::new_v4());

    scheduler
        .start_execution(&execution_id, "test-task", serde_json::json!({}))
        .await
        .expect("start_execution failed");

    scheduler
        .fail_execution(&execution_id)
        .await
        .expect("fail_execution failed");

    let result = durable
        .wait_completion::<TypedOutput>(&execution_id, Duration::from_secs(2), None)
        .await;

    assert!(
        result.is_err(),
        "expected error when execution has no result"
    );
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("no result"),
        "expected 'no result' error but got: {err}"
    );
}

#[tokio::test]
#[ignore]
async fn wait_completion_fails_on_type_mismatch() {
    let scheduler = setup().await;
    let mut registry = DurableRegistry::new();
    registry.register("zart::tests::integration::TypedTask", TypedTask);

    let (worker, handle) = spawn_worker(scheduler.clone(), registry);
    let durable = DurableScheduler::new(scheduler.clone(), scheduler.task_scheduler());

    let execution_id = format!("typed-wait-mismatch-{}", Uuid::new_v4());
    let input = TypedInput { multiplier: 21 };
    durable
        .start_for::<TypedTask>(&execution_id, "zart::tests::integration::TypedTask", &input)
        .await
        .expect("start failed");

    let result = durable
        .wait_completion::<String>(&execution_id, Duration::from_secs(10), None)
        .await;

    assert!(result.is_err(), "expected error when type doesn't match");
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("deserialize"),
        "expected deserialization error but got: {err}"
    );

    worker.stop();
    let _ = handle.await;
}
