//! Integration tests for the `zart` crate.
//!
//! Tests marked `#[ignore]` require a running PostgreSQL instance.
//! Start it with `just up`, then run: `just test-integration`

#[cfg(test)]
mod integration {
    use scheduler::{ExecutionStatus, PostgresScheduler, Scheduler as _};
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
        context::{TaskContext, ZartStep},
        error::{StepError, TaskError},
        registry::DurableExecution,
    };

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
        fn step_name(&self) -> Cow<'static, str> {
            Cow::Borrowed("step-one")
        }
        async fn run(&self, ctx: zart::context::StepContext) -> Result<Self::Output, StepError> {
            println!("[step-one] Attempt {}", ctx.current_attempt() + 1);
            Ok(21i32)
        }
    }

    struct StepTwo {
        step1_result: i32,
    }

    #[async_trait::async_trait]
    impl ZartStep for StepTwo {
        type Output = i32;
        fn step_name(&self) -> Cow<'static, str> {
            Cow::Borrowed("step-two")
        }
        async fn run(&self, _ctx: zart::context::StepContext) -> Result<Self::Output, StepError> {
            println!("[step-two] running");
            Ok(self.step1_result * 2)
        }
    }

    struct SequentialTask;

    #[async_trait::async_trait]
    impl DurableExecution for SequentialTask {
        type Data = serde_json::Value;
        type Output = serde_json::Value;

        async fn run(
            &self,
            ctx: &mut TaskContext,
            _data: Self::Data,
        ) -> Result<Self::Output, TaskError> {
            let step1: i32 = ctx.execute_step(StepOne).await?;
            let step2: i32 = ctx
                .execute_step(StepTwo {
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
        fn step_name(&self) -> Cow<'static, str> {
            Cow::Borrowed("fail-step")
        }
        async fn run(&self, _ctx: zart::context::StepContext) -> Result<Self::Output, StepError> {
            println!("[fail-step] Failing intentionally");
            Err(StepError::Failed {
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

        async fn run(
            &self,
            ctx: &mut TaskContext,
            _data: Self::Data,
        ) -> Result<Self::Output, TaskError> {
            ctx.execute_step(FailStep).await?;
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
        fn step_name(&self) -> Cow<'static, str> {
            Cow::Borrowed("transient-step")
        }
        fn retry_config(&self) -> Option<RetryConfig> {
            Some(RetryConfig::fixed(3, Duration::from_millis(50)))
        }
        async fn run(&self, ctx: zart::context::StepContext) -> Result<Self::Output, StepError> {
            let count = self.attempts.fetch_add(1, Ordering::SeqCst);
            println!(
                "[transient-step] Attempt {} (0-indexed: {})",
                ctx.current_attempt() + 1,
                ctx.current_attempt()
            );
            if count < 2 {
                Err(StepError::Failed {
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

        async fn run(
            &self,
            ctx: &mut TaskContext,
            _data: Self::Data,
        ) -> Result<Self::Output, TaskError> {
            let result: String = ctx
                .execute_step(TransientStep {
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

        async fn run(
            &self,
            ctx: &mut TaskContext,
            _data: Self::Data,
        ) -> Result<Self::Output, TaskError> {
            let approval: ApprovalPayload = ctx
                .wait_for_event("approve", Some(Duration::from_secs(30)))
                .await?;
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
        fn step_name(&self) -> Cow<'static, str> {
            Cow::Borrowed("step-a")
        }
        async fn run(&self, _ctx: zart::context::StepContext) -> Result<Self::Output, StepError> {
            println!("[step-a] running");
            Ok(1)
        }
    }

    struct StepB;

    #[async_trait::async_trait]
    impl ZartStep for StepB {
        type Output = i32;
        fn step_name(&self) -> Cow<'static, str> {
            Cow::Borrowed("step-b")
        }
        async fn run(&self, _ctx: zart::context::StepContext) -> Result<Self::Output, StepError> {
            println!("[step-b] running");
            Ok(2)
        }
    }

    struct StepC;

    #[async_trait::async_trait]
    impl ZartStep for StepC {
        type Output = i32;
        fn step_name(&self) -> Cow<'static, str> {
            Cow::Borrowed("step-c")
        }
        async fn run(&self, _ctx: zart::context::StepContext) -> Result<Self::Output, StepError> {
            println!("[step-c] running");
            Ok(3)
        }
    }

    struct ParallelTask;

    #[async_trait::async_trait]
    impl DurableExecution for ParallelTask {
        type Data = serde_json::Value;
        type Output = serde_json::Value;

        async fn run(
            &self,
            ctx: &mut TaskContext,
            _data: Self::Data,
        ) -> Result<Self::Output, TaskError> {
            let h1 = ctx.schedule_step(StepA);
            let h2 = ctx.schedule_step(StepB);
            let h3 = ctx.schedule_step(StepC);

            let results = ctx.wait_all(vec![h1, h2, h3]).await?;
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

            async fn run(
                &self,
                _ctx: &mut TaskContext,
                _data: Self::Data,
            ) -> Result<Self::Output, TaskError> {
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
                execution_id: None,
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
            fn step_name(&self) -> Cow<'static, str> {
                Cow::Borrowed("always-fail")
            }
            fn retry_config(&self) -> Option<RetryConfig> {
                Some(RetryConfig::fixed(1, Duration::from_millis(50)))
            }
            async fn run(
                &self,
                _ctx: zart::context::StepContext,
            ) -> Result<Self::Output, StepError> {
                Err(StepError::Failed {
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

            async fn run(
                &self,
                ctx: &mut TaskContext,
                _data: Self::Data,
            ) -> Result<Self::Output, TaskError> {
                ctx.execute_step(AlwaysFailStep).await?;
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

        async fn run(
            &self,
            _ctx: &mut TaskContext,
            _data: Self::Data,
        ) -> Result<Self::Output, TaskError> {
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
        fn step_name(&self) -> Cow<'static, str> {
            Cow::Borrowed("gated-step")
        }
        async fn run(&self, _ctx: zart::context::StepContext) -> Result<Self::Output, StepError> {
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

        async fn run(
            &self,
            ctx: &mut TaskContext,
            _data: Self::Data,
        ) -> Result<Self::Output, TaskError> {
            // Signal that we entered the handler (before the step call).
            self.started.notify_one();
            // Wait for the test to cancel the execution.
            self.gate.notified().await;

            // This is the first call: returns StepError::Scheduled, causing
            // the worker to call update_task_state and re-queue the task.
            ctx.execute_step(GatedStep).await?;

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

        // Release the handler; it will now call ctx.execute_step(...) for the first time,
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
}
