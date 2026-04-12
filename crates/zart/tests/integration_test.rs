//! Integration tests for the `zart` crate.
//!
//! Tests marked `#[ignore]` require a running PostgreSQL instance.
//! Start it with `just up`, then run: `just test-integration`

#[cfg(test)]
mod integration {
    use scheduler::{
        CompleteWaitGroupChildParams, DurableStorage as _, EventDeliveryResult, ExecutionStatus,
        FailWaitGroupChildParams, PostgresScheduler, Scheduler as _, StepKind,
    };
    use serde::{Deserialize, Serialize};
    use std::borrow::Cow;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Duration;
    use uuid::Uuid;
    use zart::{
        DurableScheduler, RetryConfig, TaskRegistry, Worker, WorkerConfig,
        context::ZartStep,
        error::TaskError,
        registry::DurableExecution,
        step_types::{
            CompletionBehavior, CompletionOutcome, CompletionSpec, StepDefId, StepResult,
        },
    };

    // ── Local step error for test steps ───────────────────────────────────────

    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    enum TestStepError {
        Failed { step: String, reason: String },
    }

    impl std::fmt::Display for TestStepError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                TestStepError::Failed { step, reason } => {
                    write!(f, "Step '{step}' failed: {reason}")
                }
            }
        }
    }

    impl std::error::Error for TestStepError {}

    // ── Shared helpers ────────────────────────────────────────────────────────

    fn pg_url() -> String {
        std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://zart:zart@localhost:5432/zart".to_string())
    }

    async fn setup() -> Arc<PostgresScheduler> {
        let pool = sqlx::PgPool::connect(&pg_url())
            .await
            .expect("failed to connect to PostgreSQL");
        let scheduler = Arc::new(PostgresScheduler::new(pool));
        scheduler.run_migrations().await.expect("migrations failed");
        scheduler
    }

    fn spawn_worker(
        scheduler: Arc<PostgresScheduler>,
        registry: Arc<TaskRegistry>,
    ) -> (Arc<Worker>, tokio::task::JoinHandle<()>) {
        let config = WorkerConfig {
            poll_interval: Duration::from_millis(100),
            max_tasks_per_poll: 10,
            max_concurrent_tasks: 4,
            shutdown_timeout: Duration::from_secs(5),
            orphan_timeout: Duration::from_secs(30),
            ..Default::default()
        };
        let worker = Arc::new(Worker::new(scheduler, registry, config));
        let w = worker.clone();
        let handle = tokio::spawn(async move { w.run().await });
        (worker, handle)
    }

    // ── Handlers ──────────────────────────────────────────────────────────────

    /// Runs two sequential steps, each returning a value used by the next.
    /// Proves the full re-entry and caching path.
    struct StepOne;

    #[async_trait::async_trait]
    impl ZartStep for StepOne {
        type Output = i32;
        type Error = TestStepError;
        fn step_name(&self) -> Cow<'static, str> {
            Cow::Borrowed("step-one")
        }
        async fn run(&self) -> Result<Self::Output, Self::Error> {
            println!("[step-one] Attempt {}", zart::context().current_attempt + 1);
            Ok(21i32)
        }
    }

    struct StepTwo {
        step1_result: i32,
    }

    #[async_trait::async_trait]
    impl ZartStep for StepTwo {
        type Output = i32;
        type Error = TestStepError;
        fn step_name(&self) -> Cow<'static, str> {
            Cow::Borrowed("step-two")
        }
        async fn run(&self) -> Result<Self::Output, Self::Error> {
            println!("[step-two] running");
            Ok(self.step1_result * 2)
        }
    }

    struct SequentialTask;

    #[async_trait::async_trait]
    impl DurableExecution for SequentialTask {
        type Data = serde_json::Value;
        type Output = serde_json::Value;

        async fn run(&self, _data: Self::Data) -> Result<Self::Output, TaskError> {
            let step1: i32 = zart::require(StepOne).await?;
            let step2: i32 = zart::require(StepTwo {
                step1_result: step1,
            })
            .await?;
            Ok(serde_json::json!({ "answer": step2 }))
        }
    }

    /// A task whose first step always fails with a non-control-flow error.
    struct FailStep;

    #[async_trait::async_trait]
    impl ZartStep for FailStep {
        type Output = serde_json::Value;
        type Error = TestStepError;
        fn step_name(&self) -> Cow<'static, str> {
            Cow::Borrowed("fail-step")
        }
        async fn run(&self) -> Result<Self::Output, Self::Error> {
            println!("[fail-step] Failing intentionally");
            Err(TestStepError::Failed {
                step: "fail-step".to_string(),
                reason: "intentional failure".to_string(),
            })
        }
    }

    struct FailingTask;

    #[async_trait::async_trait]
    impl DurableExecution for FailingTask {
        type Data = serde_json::Value;
        type Output = serde_json::Value;

        async fn run(&self, _data: Self::Data) -> Result<Self::Output, TaskError> {
            zart::require(FailStep).await?;
            Ok(serde_json::json!({}))
        }
    }

    /// A task whose first step fails twice then succeeds on the third attempt.
    struct TransientStep {
        attempts: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl ZartStep for TransientStep {
        type Output = String;
        type Error = TestStepError;
        fn step_name(&self) -> Cow<'static, str> {
            Cow::Borrowed("transient-step")
        }
        fn retry_config(&self) -> Option<RetryConfig> {
            Some(RetryConfig::fixed(3, Duration::from_millis(50)))
        }
        async fn run(&self) -> Result<Self::Output, Self::Error> {
            let count = self.attempts.fetch_add(1, Ordering::SeqCst);
            println!(
                "[transient-step] Attempt {} (0-indexed: {})",
                zart::context().current_attempt + 1,
                zart::context().current_attempt
            );
            if count < 2 {
                Err(TestStepError::Failed {
                    step: "transient-step".to_string(),
                    reason: format!("transient error #{}", count + 1),
                })
            } else {
                Ok("success".to_string())
            }
        }
    }

    struct TransientFailTask {
        attempts: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl DurableExecution for TransientFailTask {
        type Data = serde_json::Value;
        type Output = serde_json::Value;

        async fn run(&self, _data: Self::Data) -> Result<Self::Output, TaskError> {
            let result: String = zart::require(TransientStep {
                attempts: self.attempts.clone(),
            })
            .await?;
            Ok(serde_json::json!({ "result": result }))
        }
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

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
        // Lambda invoked exactly 3 times: 2 failures + 1 success.
        assert_eq!(attempt_counter.load(Ordering::SeqCst), 3);
    }

    // ── Event-driven execution ────────────────────────────────────────────────

    /// A task that waits for an external "approve" event before continuing.
    struct WaitEventTask;

    #[derive(Debug, Serialize, Deserialize)]
    struct ApprovalPayload {
        approved: bool,
    }

    #[async_trait::async_trait]
    impl DurableExecution for WaitEventTask {
        type Data = serde_json::Value;
        type Output = serde_json::Value;

        async fn run(&self, _data: Self::Data) -> Result<Self::Output, TaskError> {
            let approval: ApprovalPayload =
                zart::wait_for_event("approve", Some(Duration::from_secs(30))).await?;
            Ok(serde_json::json!({ "approved": approval.approved }))
        }
    }

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

        // Give the worker time to pick up the body task. In the new model the body task
        // schedules a wait_for_event step task (execution_time = deadline) and completes
        // itself — it does NOT park/block. The step task sits in the DB until offer_event
        // atomically marks it completed and inserts the next body segment.
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Deliver the event: atomically completes the step task + schedules the next body
        // segment. The worker will then pick up the continuation immediately.
        durable
            .offer_event(
                &execution_id,
                "approve",
                serde_json::json!({ "approved": true }),
            )
            .await
            .expect("offer_event failed");

        // Now wait for the execution to complete.
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

        // Give the worker a moment to pick it up and park it.
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

        // Wait with a very short timeout — execution is blocked on an event.
        let result = durable
            .wait_with_timeout(&execution_id, Duration::from_millis(300))
            .await;

        worker.stop();

        assert!(
            matches!(result, Err(zart::SchedulerError::WaitTimedOut(_))),
            "expected WaitTimedOut, got {result:?}"
        );
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

    // ── Parallel steps ────────────────────────────────────────────────────────

    // ── Parallel steps task ───────────────────────────────────────────────────

    struct StepA;

    #[async_trait::async_trait]
    impl ZartStep for StepA {
        type Output = i32;
        type Error = TestStepError;
        fn step_name(&self) -> Cow<'static, str> {
            Cow::Borrowed("step-a")
        }
        async fn run(&self) -> Result<Self::Output, Self::Error> {
            println!("[step-a] running");
            Ok(1)
        }
    }

    struct StepB;

    #[async_trait::async_trait]
    impl ZartStep for StepB {
        type Output = i32;
        type Error = TestStepError;
        fn step_name(&self) -> Cow<'static, str> {
            Cow::Borrowed("step-b")
        }
        async fn run(&self) -> Result<Self::Output, Self::Error> {
            println!("[step-b] running");
            Ok(2)
        }
    }

    struct StepC;

    #[async_trait::async_trait]
    impl ZartStep for StepC {
        type Output = i32;
        type Error = TestStepError;
        fn step_name(&self) -> Cow<'static, str> {
            Cow::Borrowed("step-c")
        }
        async fn run(&self) -> Result<Self::Output, Self::Error> {
            println!("[step-c] running");
            Ok(3)
        }
    }

    struct ParallelTask;

    #[async_trait::async_trait]
    impl DurableExecution for ParallelTask {
        type Data = serde_json::Value;
        type Output = serde_json::Value;

        async fn run(&self, _data: Self::Data) -> Result<Self::Output, TaskError> {
            let h1 = zart::schedule(StepA);
            let h2 = zart::schedule(StepB);
            let h3 = zart::schedule(StepC);

            let results = zart::wait(vec![h1, h2, h3]).await?;
            let sum: i32 = results.into_iter().map(|r| r.unwrap()).sum();
            Ok(serde_json::json!({ "sum": sum }))
        }
    }
    #[tokio::test]
    #[ignore = "requires PostgreSQL — run with: just test-integration"]
    async fn parallel_steps_all_complete_and_sum_results() {
        let scheduler = setup().await;

        let mut registry = TaskRegistry::new();
        registry.register("parallel-task", ParallelTask);
        let registry = Arc::new(registry);

        let execution_id = format!("test-par-{}", Uuid::new_v4());
        let durable = DurableScheduler::new(scheduler.clone());

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

    // ── Phase 3: Declarative v3 dispatch / step_internal integration ─────────

    #[tokio::test]
    #[ignore = "requires PostgreSQL — run with: just test-integration"]
    async fn phase3_stepdefid_from_metadata_backward_compatible() {
        let wg_new = serde_json::json!({
            "mode": "step",
            "step_name": "child-a",
            "wg_step_name": "__wg__all__abc"
        });
        let wg_old = serde_json::json!({
            "mode": "step",
            "step_name": "child-b",
            "is_wait_all_child": true
        });
        let sleep = serde_json::json!({
            "mode": "step",
            "step_name": "__sleep",
            "step_type": "sleep"
        });
        let event = serde_json::json!({
            "mode": "step",
            "step_name": "approval",
            "step_type": "wait_for_event"
        });
        let regular = serde_json::json!({
            "mode": "step",
            "step_name": "step-one",
            "step_type": "step"
        });

        assert_eq!(StepDefId::from_metadata(&wg_new), StepDefId::WaitGroupChild);
        assert_eq!(StepDefId::from_metadata(&wg_old), StepDefId::WaitGroupChild);
        assert_eq!(StepDefId::from_metadata(&sleep), StepDefId::Sleep);
        assert_eq!(StepDefId::from_metadata(&event), StepDefId::WaitForEvent);
        assert_eq!(StepDefId::from_metadata(&regular), StepDefId::Step);
    }

    #[tokio::test]
    #[ignore = "requires PostgreSQL — run with: just test-integration"]
    async fn phase3_wait_group_complete_concurrent_schedules_body_once() {
        let scheduler = setup().await;

        let execution_id = format!("test-phase3-wg-concurrent-{}", Uuid::new_v4());
        let run_id = format!("{execution_id}:run:0");
        let task_name = "phase3-wg-task";

        scheduler
            .start_execution(&execution_id, task_name, serde_json::json!({}))
            .await
            .expect("start_execution failed");

        scheduler
            .upsert_wait_group_step(scheduler::UpsertWaitGroupStepParams {
                run_id: run_id.clone(),
                group_step_name: "__wg__all__concurrent".to_string(),
                total: 2,
                threshold: 0,
            })
            .await
            .expect("upsert_wait_group_step failed");

        let child1_task_id = format!("{run_id}:step:child-1");
        let child2_task_id = format!("{run_id}:step:child-2");

        scheduler
            .schedule_step(scheduler::ScheduleStepParams {
                task_id: child1_task_id.clone(),
                task_name: task_name.to_string(),
                run_id: run_id.clone(),
                step_name: "child-1".to_string(),
                step_kind: StepKind::Step,
                execution_time: chrono::Utc::now(),
                data: serde_json::json!({}),
                metadata: serde_json::json!({
                    "mode": "step",
                    "step_type": "step",
                    "run_id": run_id.clone(),
                    "execution_id": execution_id.clone(),
                    "step_name": "child-1",
                    "is_wait_all_child": true,
                    "wg_step_name": "__wg__all__concurrent"
                }),
                retry_config: None,
            })
            .await
            .expect("schedule child-1 failed");

        scheduler
            .schedule_step(scheduler::ScheduleStepParams {
                task_id: child2_task_id.clone(),
                task_name: task_name.to_string(),
                run_id: run_id.clone(),
                step_name: "child-2".to_string(),
                step_kind: StepKind::Step,
                execution_time: chrono::Utc::now(),
                data: serde_json::json!({}),
                metadata: serde_json::json!({
                    "mode": "step",
                    "step_type": "step",
                    "run_id": run_id.clone(),
                    "execution_id": execution_id.clone(),
                    "step_name": "child-2",
                    "is_wait_all_child": true,
                    "wg_step_name": "__wg__all__concurrent"
                }),
                retry_config: None,
            })
            .await
            .expect("schedule child-2 failed");

        let fetched = scheduler
            .poll_due(chrono::Utc::now(), 200)
            .await
            .expect("poll_due failed");

        let lock1 = fetched
            .iter()
            .find(|t| t.task_id == child1_task_id)
            .map(|t| t.lock_token.clone())
            .expect("child-1 task not fetched");
        let lock2 = fetched
            .iter()
            .find(|t| t.task_id == child2_task_id)
            .map(|t| t.lock_token.clone())
            .expect("child-2 task not fetched");

        let next_body_task_id = format!("{run_id}:body:after:__wg__all__concurrent");

        let s1 = scheduler.clone();
        let run_id_1 = run_id.clone();
        let child1_task_id_clone = child1_task_id.clone();
        let next_body_task_id_1 = next_body_task_id.clone();
        let child1 = tokio::spawn(async move {
            s1.complete_wait_group_child(CompleteWaitGroupChildParams {
                run_id: run_id_1,
                group_step_name: "__wg__all__concurrent".to_string(),
                child_step_task_id: child1_task_id_clone.clone(),
                child_step_id: child1_task_id_clone,
                child_result: serde_json::json!(1),
                lock_token: lock1,
                attempt_number: 1,
                next_body_task_id: next_body_task_id_1,
                task_name: task_name.to_string(),
                data: serde_json::json!({}),
            })
            .await
        });

        let s2 = scheduler.clone();
        let run_id_2 = run_id.clone();
        let child2_task_id_clone = child2_task_id.clone();
        let next_body_task_id_2 = next_body_task_id.clone();
        let child2 = tokio::spawn(async move {
            s2.complete_wait_group_child(CompleteWaitGroupChildParams {
                run_id: run_id_2,
                group_step_name: "__wg__all__concurrent".to_string(),
                child_step_task_id: child2_task_id_clone.clone(),
                child_step_id: child2_task_id_clone,
                child_result: serde_json::json!(2),
                lock_token: lock2,
                attempt_number: 1,
                next_body_task_id: next_body_task_id_2,
                task_name: task_name.to_string(),
                data: serde_json::json!({}),
            })
            .await
        });

        let r1: Result<bool, scheduler::StorageError> = child1.await.expect("join child1 failed");
        let r2: Result<bool, scheduler::StorageError> = child2.await.expect("join child2 failed");
        let t1 = r1.expect("complete_wait_group_child #1 failed");
        let t2 = r2.expect("complete_wait_group_child #2 failed");

        assert!(t1 ^ t2, "exactly one child should trigger body scheduling");

        let fetched = scheduler
            .poll_due(chrono::Utc::now(), 200)
            .await
            .expect("poll_due failed");
        let body_count = fetched
            .iter()
            .filter(|t| t.task_id == next_body_task_id)
            .count();
        assert_eq!(body_count, 1, "body must be scheduled exactly once");
    }

    #[tokio::test]
    #[ignore = "requires PostgreSQL — run with: just test-integration"]
    async fn phase3_wait_group_failure_first_only_fails_execution_once() {
        let scheduler = setup().await;

        let execution_id = format!("test-phase3-wg-fail-{}", Uuid::new_v4());
        let run_id = format!("{execution_id}:run:0");
        let task_name = "phase3-wg-fail-task";

        scheduler
            .start_execution(&execution_id, task_name, serde_json::json!({}))
            .await
            .expect("start_execution failed");

        scheduler
            .upsert_wait_group_step(scheduler::UpsertWaitGroupStepParams {
                run_id: run_id.clone(),
                group_step_name: "__wg__all__fail".to_string(),
                total: 3,
                threshold: 0,
            })
            .await
            .expect("upsert_wait_group_step failed");

        let fail1_task_id = format!("{run_id}:step:fail-1");
        let fail2_task_id = format!("{run_id}:step:fail-2");

        scheduler
            .schedule_step(scheduler::ScheduleStepParams {
                task_id: fail1_task_id.clone(),
                task_name: task_name.to_string(),
                run_id: run_id.clone(),
                step_name: "fail-1".to_string(),
                step_kind: StepKind::Step,
                execution_time: chrono::Utc::now(),
                data: serde_json::json!({}),
                metadata: serde_json::json!({
                    "mode": "step",
                    "step_type": "step",
                    "run_id": run_id.clone(),
                    "execution_id": execution_id.clone(),
                    "step_name": "fail-1",
                    "is_wait_all_child": true,
                    "wg_step_name": "__wg__all__fail"
                }),
                retry_config: None,
            })
            .await
            .expect("schedule fail-1 failed");

        scheduler
            .schedule_step(scheduler::ScheduleStepParams {
                task_id: fail2_task_id.clone(),
                task_name: task_name.to_string(),
                run_id: run_id.clone(),
                step_name: "fail-2".to_string(),
                step_kind: StepKind::Step,
                execution_time: chrono::Utc::now(),
                data: serde_json::json!({}),
                metadata: serde_json::json!({
                    "mode": "step",
                    "step_type": "step",
                    "run_id": run_id.clone(),
                    "execution_id": execution_id.clone(),
                    "step_name": "fail-2",
                    "is_wait_all_child": true,
                    "wg_step_name": "__wg__all__fail"
                }),
                retry_config: None,
            })
            .await
            .expect("schedule fail-2 failed");

        let fetched = scheduler
            .poll_due(chrono::Utc::now(), 200)
            .await
            .expect("poll_due failed");

        let fail1_lock = fetched
            .iter()
            .find(|t| t.task_id == fail1_task_id)
            .map(|t| t.lock_token.clone())
            .expect("fail-1 task not fetched");
        let fail2_lock = fetched
            .iter()
            .find(|t| t.task_id == fail2_task_id)
            .map(|t| t.lock_token.clone())
            .expect("fail-2 task not fetched");

        let first = scheduler
            .fail_wait_group_child(FailWaitGroupChildParams {
                run_id: run_id.clone(),
                group_step_name: "__wg__all__fail".to_string(),
                child_step_task_id: fail1_task_id.clone(),
                child_step_id: fail1_task_id,
                error: "boom-1".to_string(),
                lock_token: fail1_lock,
                attempt_number: 1,
            })
            .await
            .expect("fail_wait_group_child first failed");

        let second = scheduler
            .fail_wait_group_child(FailWaitGroupChildParams {
                run_id: run_id.clone(),
                group_step_name: "__wg__all__fail".to_string(),
                child_step_task_id: fail2_task_id.clone(),
                child_step_id: fail2_task_id,
                error: "boom-2".to_string(),
                lock_token: fail2_lock,
                attempt_number: 1,
            })
            .await
            .expect("fail_wait_group_child second failed");

        assert!(first, "first failure must win CAS");
        assert!(!second, "second failure must not win CAS");

        if first {
            scheduler
                .fail_execution(&execution_id)
                .await
                .expect("fail_execution failed");
        }
        let exec = scheduler
            .get_execution(&execution_id)
            .await
            .expect("get_execution failed")
            .expect("execution not found");
        assert_eq!(exec.status, ExecutionStatus::Failed);
    }

    #[tokio::test]
    #[ignore = "requires PostgreSQL — run with: just test-integration"]
    async fn phase3_deliver_event_happy_path_and_idempotency() {
        let scheduler = setup().await;
        let durable = DurableScheduler::new(scheduler.clone());

        let execution_id = format!("test-phase3-deliver-event-{}", Uuid::new_v4());
        durable
            .start(&execution_id, "wait-event-task", serde_json::json!({}))
            .await
            .expect("start failed");

        let mut registry = TaskRegistry::new();
        registry.register("wait-event-task", WaitEventTask);
        let registry = Arc::new(registry);
        let (worker, _handle) = spawn_worker(scheduler.clone(), registry);

        tokio::time::sleep(Duration::from_millis(600)).await;

        let r1 = scheduler
            .deliver_event(
                &execution_id,
                "approve",
                serde_json::json!({ "approved": true }),
            )
            .await
            .expect("deliver_event #1 failed");
        let r2 = scheduler
            .deliver_event(
                &execution_id,
                "approve",
                serde_json::json!({ "approved": true }),
            )
            .await
            .expect("deliver_event #2 failed");

        assert_eq!(r1, EventDeliveryResult::Delivered);
        assert_eq!(r2, EventDeliveryResult::AlreadyDelivered);

        let record = durable
            .wait(&execution_id, Duration::from_secs(10), None)
            .await
            .expect("wait failed");

        worker.stop();

        assert_eq!(record.status, ExecutionStatus::Completed);
        assert_eq!(record.result.expect("result missing")["approved"], true);
    }

    #[tokio::test]
    #[ignore = "requires PostgreSQL — run with: just test-integration"]
    async fn phase3_completion_behaviors_execute_with_real_backend() {
        let scheduler = setup().await;

        let execution_id = format!("test-phase3-completion-{}", Uuid::new_v4());
        let run_id = format!("{execution_id}:run:0");
        let task_name = "phase3-completion-task";

        scheduler
            .start_execution(&execution_id, task_name, serde_json::json!({}))
            .await
            .expect("start_execution failed");

        let schedule = scheduler
            .schedule_step(scheduler::ScheduleStepParams {
                task_id: format!("{execution_id}:step:phase3-step"),
                task_name: task_name.to_string(),
                run_id: run_id.clone(),
                step_name: "phase3-step".to_string(),
                step_kind: StepKind::Step,
                execution_time: chrono::Utc::now(),
                data: serde_json::json!({}),
                metadata: serde_json::json!({
                    "mode": "step",
                    "step_type": "step",
                    "run_id": run_id,
                    "execution_id": execution_id,
                    "step_name": "phase3-step"
                }),
                retry_config: None,
            })
            .await
            .expect("schedule_step failed");

        let fetched = scheduler
            .poll_due(chrono::Utc::now(), 200)
            .await
            .expect("poll_due failed");

        let step_lock = fetched
            .iter()
            .find(|t| t.task_id == schedule.task_id)
            .map(|t| t.lock_token.clone())
            .expect("scheduled step task not fetched");

        let spec = CompletionSpec {
            step_task_id: schedule.task_id.clone(),
            step_id: schedule.task_id.clone(),
            step_name: "phase3-step".to_string(),
            worker_id: step_lock,
            task_name: task_name.to_string(),
            run_id: format!("{execution_id}:run:0"),
            execution_id: execution_id.clone(),
            data: serde_json::json!({}),
            attempt_number: 1,
            result: StepResult::Executed(serde_json::json!({"ok": true})),
            wait_group_step_name: None,
            outcome: CompletionOutcome::Success,
        };

        let behavior = zart::step_types::completion::ScheduleNextBody;
        behavior
            .complete(&*scheduler, spec)
            .await
            .expect("ScheduleNextBody::complete failed");

        let due = scheduler
            .poll_due(chrono::Utc::now(), 200)
            .await
            .expect("poll_due failed");

        let body_scheduled = due.iter().any(|t| {
            t.metadata.get("mode").and_then(|v| v.as_str()) == Some("body")
                && t.task_id.contains(":body:after:phase3-step")
        });
        assert!(body_scheduled, "expected body continuation task");
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

        // Cancel before any worker picks it up.
        durable.cancel(&execution_id).await.expect("cancel failed");

        let record = durable.status(&execution_id).await.expect("status failed");
        assert_eq!(record.status, ExecutionStatus::Cancelled);
    }

    #[tokio::test]
    #[ignore = "requires PostgreSQL — run with: just test-integration"]
    async fn recurring_fixed_delay_task_runs_multiple_times() {
        use scheduler::Recurrence;

        // A simple handler that records how many times it has run via an atomic counter.
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

        // Schedule a recurring task with a 200 ms fixed delay.
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

        // Wait long enough for at least 3 executions (3 × 200 ms = 600 ms + polling slack).
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

    // ── Cancellation-during-execution tests ───────────────────────────────────
    //
    // These tests cover the race condition where a durable execution is cancelled
    // while its task is already `picked_up` by a worker.  cancel_execution only
    // cancels `scheduled` tasks; the in-flight task must detect the cancellation
    // after the handler returns and NOT overwrite the execution's `cancelled` state.

    /// A task that signals when it has started, then waits for an external permit
    /// before returning.  This lets the test cancel the execution while the handler
    /// is still "running" before it finishes.
    struct GatedTask {
        /// Notified by the handler once it begins executing.
        started: Arc<tokio::sync::Notify>,
        /// The handler waits on this before returning a result.
        gate: Arc<tokio::sync::Notify>,
    }

    #[async_trait::async_trait]
    impl DurableExecution for GatedTask {
        type Data = serde_json::Value;
        type Output = serde_json::Value;

        async fn run(&self, _data: Self::Data) -> Result<Self::Output, TaskError> {
            self.started.notify_one();
            self.gate.notified().await;
            Ok(serde_json::json!({ "done": true }))
        }
    }

    /// A task whose FIRST step triggers the StepError::Scheduled control-flow path
    /// and then signals the test before re-queuing.
    struct GatedStep;

    #[async_trait::async_trait]
    impl ZartStep for GatedStep {
        type Output = i32;
        type Error = TestStepError;
        fn step_name(&self) -> Cow<'static, str> {
            Cow::Borrowed("gated-step")
        }
        async fn run(&self) -> Result<Self::Output, Self::Error> {
            println!("[gated-step] Scheduling step");
            Ok(1)
        }
    }

    struct GatedStepTask {
        started: Arc<tokio::sync::Notify>,
        gate: Arc<tokio::sync::Notify>,
    }

    #[async_trait::async_trait]
    impl DurableExecution for GatedStepTask {
        type Data = serde_json::Value;
        type Output = serde_json::Value;

        async fn run(&self, _data: Self::Data) -> Result<Self::Output, TaskError> {
            // Signal that we entered the handler (before the step call).
            self.started.notify_one();
            // Wait for the test to cancel the execution.
            self.gate.notified().await;

            // This is the first call: returns StepError::Scheduled, causing
            // the worker to call update_task_state and re-queue the task.
            zart::require(GatedStep).await?;

            Ok(serde_json::json!({}))
        }
    }

    /// Handler finishes successfully but the execution was already cancelled while
    /// it was running.  The worker must NOT overwrite the `cancelled` status.
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

        // Wait for the handler to start (task is now `picked_up`).
        started.notified().await;

        // Cancel the execution while the handler is still paused inside `gate`.
        durable.cancel(&execution_id).await.expect("cancel failed");

        // Release the handler so it returns Ok(…).
        gate.notify_one();

        // Give the worker time to process the result.
        tokio::time::sleep(Duration::from_millis(500)).await;

        // The execution must still be `cancelled` — not `completed`.
        let record = durable.status(&execution_id).await.expect("status failed");
        assert_eq!(
            record.status,
            ExecutionStatus::Cancelled,
            "expected Cancelled but got {:?}",
            record.status
        );
    }

    /// Handler triggers StepError::Scheduled (first step call) while the execution
    /// is already cancelled.  The worker must NOT re-queue the task via
    /// update_task_state, which would set it back to `scheduled` and allow it to
    /// be picked up again.
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

        // Wait for the handler body to start (before the step call).
        started.notified().await;

        // Cancel the execution while the handler is still paused.
        durable.cancel(&execution_id).await.expect("cancel failed");

        // Release the handler; it will now call zart::step(...) for the first time,
        // which returns StepError::Scheduled.
        gate.notify_one();

        // Give the worker time to process the StepError::Scheduled result.
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Execution must still be `cancelled`, not re-queued back to `scheduled`/`running`.
        let record = durable.status(&execution_id).await.expect("status failed");
        assert_eq!(
            record.status,
            ExecutionStatus::Cancelled,
            "expected Cancelled but got {:?}",
            record.status
        );
    }

    // ── Typed completion API tests ────────────────────────────────────────────

    /// Typed input/output for testing the typed completion API.
    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TypedInput {
        pub multiplier: i32,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct TypedOutput {
        pub result: i32,
    }

    struct TypedTask;

    #[async_trait::async_trait]
    impl DurableExecution for TypedTask {
        type Data = TypedInput;
        type Output = TypedOutput;

        async fn run(&self, data: Self::Data) -> Result<Self::Output, TaskError> {
            let val: i32 = zart::require(MultiplyStep {
                multiplier: data.multiplier,
            })
            .await?;
            Ok(TypedOutput { result: val })
        }
    }

    struct MultiplyStep {
        multiplier: i32,
    }

    #[async_trait::async_trait]
    impl ZartStep for MultiplyStep {
        type Output = i32;
        type Error = TestStepError;
        fn step_name(&self) -> Cow<'static, str> {
            Cow::Borrowed("multiply")
        }
        async fn run(&self) -> Result<Self::Output, Self::Error> {
            Ok(self.multiplier * 2)
        }
    }

    /// Tests `wait_completion` — typed deserialization of execution result.
    #[tokio::test]
    #[ignore]
    async fn wait_completion_returns_typed_result() {
        let scheduler = setup().await;
        let mut registry = TaskRegistry::new();
        registry.register("zart::tests::integration::TypedTask", TypedTask);
        let registry = Arc::new(registry);

        let (worker, handle) = spawn_worker(scheduler.clone(), registry);
        let durable = DurableScheduler::new(scheduler.clone());

        let execution_id = format!("typed-wait-{}", Uuid::new_v4());
        let input = TypedInput { multiplier: 21 };
        durable
            .start_typed(&execution_id, "zart::tests::integration::TypedTask", &input)
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

    /// Tests `wait_completion_with_timeout` — caps at 30 seconds.
    #[tokio::test]
    #[ignore]
    async fn wait_completion_with_timeout_returns_typed_result() {
        let scheduler = setup().await;
        let mut registry = TaskRegistry::new();
        registry.register("zart::tests::integration::TypedTask", TypedTask);
        let registry = Arc::new(registry);

        let (worker, handle) = spawn_worker(scheduler.clone(), registry);
        let durable = DurableScheduler::new(scheduler.clone());

        let execution_id = format!("typed-wait-timeout-{}", Uuid::new_v4());
        let input = TypedInput { multiplier: 10 };
        durable
            .start_typed(&execution_id, "zart::tests::integration::TypedTask", &input)
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

    /// Tests `start_and_wait` — explicit type parameters.
    #[tokio::test]
    #[ignore]
    async fn start_and_wait_returns_typed_result() {
        let scheduler = setup().await;
        let mut registry = TaskRegistry::new();
        registry.register("zart::tests::integration::TypedTask", TypedTask);
        let registry = Arc::new(registry);

        let (worker, handle) = spawn_worker(scheduler.clone(), registry);
        let durable = DurableScheduler::new(scheduler.clone());

        let execution_id = format!("typed-start-and-wait-{}", Uuid::new_v4());
        let input = TypedInput { multiplier: 7 };

        let output: TypedOutput = durable
            .start_and_wait(
                &execution_id,
                "zart::tests::integration::TypedTask",
                &input,
                Duration::from_secs(10),
            )
            .await
            .expect("start_and_wait failed");

        assert_eq!(output.result, 14);

        worker.stop();
        let _ = handle.await;
    }

    /// Tests `start_and_wait_for` — handler type inference for input/output,
    /// while the task name is provided explicitly.
    #[tokio::test]
    #[ignore]
    async fn start_and_wait_for_infers_types_from_handler() {
        let scheduler = setup().await;
        let mut registry = TaskRegistry::new();
        registry.register("zart::tests::integration::TypedTask", TypedTask);
        let registry = Arc::new(registry);

        let (worker, handle) = spawn_worker(scheduler.clone(), registry);
        let durable = DurableScheduler::new(scheduler.clone());

        let execution_id = format!("typed-start-for-{}", Uuid::new_v4());
        let input = TypedInput { multiplier: 5 };

        // start_and_wait_for uses H::Data and H::Output but task_name is explicit
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

    /// Tests `wait_completion` fails with Deserialization error when result is missing.
    #[tokio::test]
    #[ignore]
    async fn wait_completion_fails_when_no_result() {
        let scheduler = setup().await;
        let durable = DurableScheduler::new(scheduler.clone());

        let execution_id = format!("typed-wait-no-result-{}", Uuid::new_v4());

        // Manually create an execution and immediately fail it (no result stored)
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

    /// Tests `wait_completion` fails with Deserialization error when type doesn't match.
    #[tokio::test]
    #[ignore]
    async fn wait_completion_fails_on_type_mismatch() {
        let scheduler = setup().await;
        let mut registry = TaskRegistry::new();
        registry.register("zart::tests::integration::TypedTask", TypedTask);
        let registry = Arc::new(registry);

        let (worker, handle) = spawn_worker(scheduler.clone(), registry);
        let durable = DurableScheduler::new(scheduler.clone());

        let execution_id = format!("typed-wait-mismatch-{}", Uuid::new_v4());
        let input = TypedInput { multiplier: 21 };
        durable
            .start_typed(&execution_id, "zart::tests::integration::TypedTask", &input)
            .await
            .expect("start failed");

        // Try to deserialize to wrong type
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

    /// Tests that `admin_retry_step` clears the persisted step deadline so the
    /// retried step can actually execute (instead of immediately timing out).
    ///
    /// Scenario:
    /// 1. A step with a very short timeout (1 ms) times out and becomes Dead.
    /// 2. The step row carries `metadata["deadline"]` (a past RFC3339 timestamp).
    /// 3. `admin_retry_step` copies the old task metadata but removes `"deadline"`.
    /// 4. The retried step picks up with no deadline → runs normally.
    #[tokio::test]
    #[ignore]
    async fn admin_retry_step_clears_deadline_so_retried_step_can_run() {
        let scheduler = setup().await;

        // Insert a step that has already timed out (simulating what happens when
        // a Global-scope step exceeds its deadline). We insert the step row and
        // its task with an already-expired deadline in metadata.
        let run_id = format!("admin-deadline-retry-{}", Uuid::new_v4());
        let execution_id = run_id.split(":run:").next().unwrap();

        // Create the execution record.
        scheduler
            .start_execution(execution_id, "test-task", serde_json::json!({}))
            .await
            .expect("start_execution failed");

        // Insert a run row so admin_retry_step can find it.
        let pool = sqlx::PgPool::connect(&pg_url())
            .await
            .expect("failed to connect to PostgreSQL");
        sqlx::query(
            r#"
            INSERT INTO zart_execution_runs (run_id, execution_id, run_index, payload, trigger, status)
            VALUES ($1, $2, 0, $3, 'initial', 'running')
            "#,
        )
        .bind(&run_id)
        .bind(execution_id)
        .bind(serde_json::json!({}))
        .execute(&pool)
        .await
        .expect("insert run failed");

        // Insert a step row in Dead status (as if it timed out).
        let step_task_id = format!("{run_id}:step:slow-step");
        let past_deadline = chrono::Utc::now() - chrono::Duration::seconds(10);
        let step_metadata = serde_json::json!({
            "mode": "step",
            "step_type": "step",
            "run_id": run_id,
            "execution_id": execution_id,
            "step_name": "slow-step",
            "retry_attempt": 0,
            "deadline": past_deadline.to_rfc3339(),
        });

        sqlx::query(
            r#"
            INSERT INTO zart_tasks (task_id, task_name, execution_time, data, metadata, status, attempt)
            VALUES ($1, 'test-task', NOW(), $2, $3, 'completed', 1)
            "#,
        )
        .bind(&step_task_id)
        .bind(serde_json::json!({}))
        .bind(&step_metadata)
        .execute(&pool)
        .await
        .expect("insert task failed");

        sqlx::query(
            r#"
            INSERT INTO zart_steps (step_id, run_id, step_name, task_id, status, step_kind, retry_attempt)
            VALUES ($1, $2, $3, $4, 'dead', 'step', 3)
            "#,
        )
        .bind(format!("{run_id}:step:slow-step"))
        .bind(&run_id)
        .bind("slow-step")
        .bind(&step_task_id)
        .execute(&pool)
        .await
        .expect("insert step failed");

        // Now call admin_retry_step. This should copy metadata but REMOVE
        // the "deadline" key so the retried step isn't immediately timed out.
        let new_task_id = scheduler
            .admin_retry_step(&run_id, "slow-step", Some("test"))
            .await
            .expect("admin_retry_step failed");

        // Verify the new task's metadata does NOT contain a deadline.
        let new_metadata: Option<serde_json::Value> =
            sqlx::query_scalar(r#"SELECT metadata FROM zart_tasks WHERE task_id = $1"#)
                .bind(&new_task_id)
                .fetch_one(&pool)
                .await
                .expect("query new task metadata failed");

        let meta = new_metadata.expect("new task should have metadata");
        assert!(
            meta.get("deadline").is_none(),
            "admin_retry_step should have removed the 'deadline' key, but got: {meta}"
        );

        // The step should now be in 'scheduled' status (ready for worker pickup).
        let step_status: Option<String> = sqlx::query_scalar(
            r#"SELECT status FROM zart_steps WHERE step_name = $1 AND run_id = $2"#,
        )
        .bind("slow-step")
        .bind(&run_id)
        .fetch_one(&pool)
        .await
        .expect("query step status failed");

        assert_eq!(
            step_status,
            Some("scheduled".to_string()),
            "step should be scheduled after admin retry"
        );
    }
}
