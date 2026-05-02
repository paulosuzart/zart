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
