//! Basic execution tests: sequential steps, failures, retries, and recurring tasks.

use super::helpers::*;
use scheduler::Recurrence;
use std::sync::atomic::Ordering;
use std::time::Duration;
use uuid::Uuid;
use zart::{DurableScheduler, TaskRegistry};

#[tokio::test]
#[ignore = "requires PostgreSQL — run with: just test-integration"]
async fn durable_execution_runs_sequential_steps() {
    let scheduler = setup().await;

    let mut registry = TaskRegistry::new();
    registry.register("sequential-task", SequentialTask);
    let registry = Arc::new(registry);

    let execution_id = format!("test-seq-{}", Uuid::new_v4());
    let durable = DurableScheduler::new(scheduler.clone());

    durable
        .start(&execution_id, "sequential-task", serde_json::json!({}))
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
    assert_eq!(result["answer"], 42);
}

#[tokio::test]
#[ignore = "requires PostgreSQL — run with: just test-integration"]
async fn failed_step_causes_execution_to_fail() {
    let scheduler = setup().await;

    let mut registry = TaskRegistry::new();
    registry.register("failing-task", FailingTask);
    let registry = Arc::new(registry);

    let execution_id = format!("test-fail-{}", Uuid::new_v4());
    let durable = DurableScheduler::new(scheduler.clone());

    durable
        .start(&execution_id, "failing-task", serde_json::json!({}))
        .await
        .expect("start failed");

    let (worker, _handle) = spawn_worker(scheduler.clone(), registry);

    let record = durable
        .wait(&execution_id, Duration::from_secs(10), None)
        .await
        .expect("wait failed");

    worker.stop();

    assert_eq!(record.status, ExecutionStatus::Failed);
}

#[tokio::test]
#[ignore = "requires PostgreSQL — run with: just test-integration"]
async fn step_retries_on_transient_failure() {
    let scheduler = setup().await;
    let attempt_counter = Arc::new(AtomicUsize::new(0));

    let mut registry = TaskRegistry::new();
    registry.register(
        "transient-fail-task",
        TransientFailTask {
            attempts: attempt_counter.clone(),
        },
    );
    let registry = Arc::new(registry);

    let execution_id = format!("test-retry-{}", Uuid::new_v4());
    let durable = DurableScheduler::new(scheduler.clone());

    durable
        .start(&execution_id, "transient-fail-task", serde_json::json!({}))
        .await
        .expect("start failed");

    let (worker, _handle) = spawn_worker(scheduler.clone(), registry);

    let record = durable
        .wait(&execution_id, Duration::from_secs(15), None)
        .await
        .expect("wait failed");

    worker.stop();

    assert_eq!(record.status, ExecutionStatus::Completed);
    let result = record.result.expect("expected a result");
    assert_eq!(result["result"], "success");
    assert_eq!(attempt_counter.load(Ordering::SeqCst), 3);
}

#[tokio::test]
#[ignore = "requires PostgreSQL — run with: just test-integration"]
async fn list_executions_returns_started_executions() {
    let scheduler = setup().await;

    let durable = DurableScheduler::new(scheduler.clone());

    let base_id = Uuid::new_v4().to_string();
    let id_a = format!("test-list-a-{base_id}");
    let id_b = format!("test-list-b-{base_id}");

    durable
        .start(&id_a, "no-op-task", serde_json::json!({}))
        .await
        .expect("start a failed");
    durable
        .start(&id_b, "no-op-task", serde_json::json!({}))
        .await
        .expect("start b failed");

    let all = durable
        .list_executions(None, Some("no-op-task".to_string()), 100, 0)
        .await
        .expect("list failed");

    let ids: Vec<&str> = all.iter().map(|r| r.execution_id.as_str()).collect();
    assert!(ids.contains(&id_a.as_str()), "id_a not in list");
    assert!(ids.contains(&id_b.as_str()), "id_b not in list");
}

#[tokio::test]
#[ignore = "requires PostgreSQL — run with: just test-integration"]
async fn recurring_fixed_delay_task_runs_multiple_times() {
    struct CounterTask {
        count: Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl DurableExecution for CounterTask {
        type Data = serde_json::Value;
        type Output = serde_json::Value;

        async fn run(&self, _data: Self::Data) -> Result<Self::Output, TaskError> {
            self.count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(serde_json::json!({}))
        }
    }

    let scheduler = setup().await;
    let call_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let mut registry = TaskRegistry::new();
    registry.register(
        "counter-task",
        CounterTask {
            count: call_count.clone(),
        },
    );
    let registry = Arc::new(registry);

    let task_id = format!("recurring-{}", Uuid::new_v4());
    scheduler
        .schedule_at(scheduler::ScheduleAtParams {
            task_id: task_id.clone(),
            task_name: "counter-task".to_string(),
            execution_time: chrono::Utc::now(),
            data: serde_json::json!({}),
            recurrence: Some(Recurrence::FixedDelay { duration_ms: 200 }),
            metadata: serde_json::Value::Null,
        })
        .await
        .expect("schedule_at failed");

    let config = zart::WorkerConfig {
        poll_interval: Duration::from_millis(50),
        max_tasks_per_poll: 5,
        max_concurrent_tasks: 4,
        shutdown_timeout: Duration::from_secs(2),
        orphan_timeout: Duration::from_secs(30),
        ..Default::default()
    };
    let worker = Arc::new(Worker::new(scheduler.clone(), registry, config));
    let w = worker.clone();
    let _handle = tokio::spawn(async move { w.run().await });

    tokio::time::sleep(Duration::from_millis(900)).await;
    worker.stop();

    let runs = call_count.load(std::sync::atomic::Ordering::SeqCst);
    assert!(runs >= 3, "expected at least 3 runs, got {runs}");
}

#[tokio::test]
#[ignore = "requires PostgreSQL — run with: just test-integration"]
async fn step_exhausts_retries_and_fails_execution() {
    let scheduler = setup().await;

    struct AlwaysFailStep;

    #[async_trait::async_trait]
    impl ZartStep for AlwaysFailStep {
        type Output = String;
        type Error = TestStepError;
        fn step_name(&self) -> Cow<'static, str> {
            Cow::Borrowed("always-fail")
        }
        fn retry_config(&self) -> Option<RetryConfig> {
            Some(RetryConfig::fixed(1, Duration::from_millis(50)))
        }
        async fn run(&self) -> Result<Self::Output, Self::Error> {
            Err(TestStepError::Failed {
                step: "always-fail".to_string(),
                reason: "permanent error".to_string(),
            })
        }
    }

    struct AlwaysFailTask;

    #[async_trait::async_trait]
    impl DurableExecution for AlwaysFailTask {
        type Data = serde_json::Value;
        type Output = serde_json::Value;

        async fn run(&self, _data: Self::Data) -> Result<Self::Output, TaskError> {
            zart::require(AlwaysFailStep).await?;
            Ok(serde_json::json!({}))
        }
    }

    let mut registry = TaskRegistry::new();
    registry.register("always-fail-task", AlwaysFailTask);
    let registry = Arc::new(registry);

    let execution_id = format!("test-exhaust-{}", Uuid::new_v4());
    let durable = DurableScheduler::new(scheduler.clone());

    durable
        .start(&execution_id, "always-fail-task", serde_json::json!({}))
        .await
        .expect("start failed");

    let (worker, _handle) = spawn_worker(scheduler.clone(), registry);

    let record = durable
        .wait(&execution_id, Duration::from_secs(15), None)
        .await
        .expect("wait failed");

    worker.stop();

    assert_eq!(record.status, ExecutionStatus::Failed);
}

#[tokio::test]
#[ignore = "requires PostgreSQL — run with: just test-integration"]
async fn cancel_stops_execution_before_it_runs() {
    let scheduler = setup().await;

    let execution_id = format!("test-cancel-{}", Uuid::new_v4());
    let durable = DurableScheduler::new(scheduler.clone());

    durable
        .start(&execution_id, "sequential-task", serde_json::json!({}))
        .await
        .expect("start failed");

    durable.cancel(&execution_id).await.expect("cancel failed");

    let record = durable.status(&execution_id).await.expect("status failed");
    assert_eq!(record.status, ExecutionStatus::Cancelled);
}

#[tokio::test]
#[ignore = "requires PostgreSQL — run with: just test-integration"]
async fn start_uses_single_internal_transaction() {
    let scheduler = setup().await;
    let sched = DurableScheduler::new(scheduler.clone());

    let exec_id = format!("start-atomic-{}", Uuid::new_v4());
    let result = sched
        .start(&exec_id, "noop", serde_json::json!({}))
        .await
        .expect("start failed");

    let exec = scheduler
        .get_execution(&exec_id)
        .await
        .expect("get_execution failed");
    assert!(exec.is_some(), "execution should exist");

    let body_task_id = format!("{exec_id}:run:0:body:start");
    let task_exists: bool =
        sqlx::query_scalar(r#"SELECT EXISTS(SELECT 1 FROM zart_tasks WHERE task_id = $1)"#)
            .bind(&body_task_id)
            .fetch_one(scheduler.pool())
            .await
            .expect("query task failed");
    assert!(task_exists, "body task should exist");
    assert_eq!(result.task_id, body_task_id);
}
