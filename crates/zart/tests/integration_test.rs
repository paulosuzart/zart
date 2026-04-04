//! Integration tests for the `zart` crate.
//!
//! Tests marked `#[ignore]` require a running PostgreSQL instance.
//! Start it with `just up`, then run: `just test-integration`

#[cfg(test)]
mod integration {
    use scheduler::{ExecutionStatus, PostgresScheduler, Scheduler as _};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Duration;
    use uuid::Uuid;
    use zart::{
        RetryConfig,
        context::TaskContext,
        error::{StepError, TaskError},
        registry::TaskHandler,
        DurableScheduler, TaskRegistry, Worker, WorkerConfig,
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
        registry: Arc<TaskRegistry<PostgresScheduler>>,
    ) -> (Arc<Worker<PostgresScheduler>>, tokio::task::JoinHandle<()>) {
        let config = WorkerConfig {
            poll_interval: Duration::from_millis(100),
            max_tasks_per_poll: 10,
            max_concurrent_tasks: 4,
            shutdown_timeout: Duration::from_secs(5),
        };
        let worker = Arc::new(Worker::new(scheduler, registry, config));
        let w = worker.clone();
        let handle = tokio::spawn(async move { w.run().await });
        (worker, handle)
    }

    // ── Handlers ──────────────────────────────────────────────────────────────

    /// Runs two sequential steps, each returning a value used by the next.
    /// Proves the full re-entry and caching path.
    struct SequentialTask;

    #[async_trait::async_trait]
    impl TaskHandler for SequentialTask {
        type Data = serde_json::Value;
        type Output = serde_json::Value;

        async fn run<S: scheduler::Scheduler>(
            &self,
            ctx: &mut TaskContext<S>,
            _data: Self::Data,
        ) -> Result<Self::Output, TaskError> {
            let step1: i32 = ctx
                .step("step-one", || async { Ok(21i32) })
                .await?;

            let step2: i32 = ctx
                .step("step-two", || async { Ok(step1 * 2) })
                .await?;

            Ok(serde_json::json!({ "answer": step2 }))
        }
    }

    /// A task whose first step always fails with a non-control-flow error.
    struct FailingTask;

    #[async_trait::async_trait]
    impl TaskHandler for FailingTask {
        type Data = serde_json::Value;
        type Output = serde_json::Value;

        async fn run<S: scheduler::Scheduler>(
            &self,
            ctx: &mut TaskContext<S>,
            _data: Self::Data,
        ) -> Result<Self::Output, TaskError> {
            ctx.step("fail-step", || async {
                Err::<serde_json::Value, _>(StepError::Failed {
                    step: "fail-step".to_string(),
                    reason: "intentional failure".to_string(),
                })
            })
            .await?;

            Ok(serde_json::json!({}))
        }
    }

    /// A task whose first step fails twice then succeeds on the third attempt.
    struct TransientFailTask {
        attempts: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl TaskHandler for TransientFailTask {
        type Data = serde_json::Value;
        type Output = serde_json::Value;

        async fn run<S: scheduler::Scheduler>(
            &self,
            ctx: &mut TaskContext<S>,
            _data: Self::Data,
        ) -> Result<Self::Output, TaskError> {
            let attempts = self.attempts.clone();
            let result: String = ctx
                .step_with_retry(
                    "transient-step",
                    RetryConfig::fixed(3, Duration::from_millis(50)),
                    move || {
                        let count = attempts.fetch_add(1, Ordering::SeqCst);
                        async move {
                            if count < 2 {
                                Err(StepError::Failed {
                                    step: "transient-step".to_string(),
                                    reason: format!("transient error #{}", count + 1),
                                })
                            } else {
                                Ok("success".to_string())
                            }
                        }
                    },
                )
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
        let durable = DurableScheduler::new(scheduler.clone(), registry.clone());

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
        let durable = DurableScheduler::new(scheduler.clone(), registry.clone());

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
            TransientFailTask { attempts: attempt_counter.clone() },
        );
        let registry = Arc::new(registry);

        let execution_id = format!("test-retry-{}", Uuid::new_v4());
        let durable = DurableScheduler::new(scheduler.clone(), registry.clone());

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

    #[tokio::test]
    #[ignore = "requires PostgreSQL — run with: just test-integration"]
    async fn step_exhausts_retries_and_fails_execution() {
        let scheduler = setup().await;

        struct AlwaysFailTask;

        #[async_trait::async_trait]
        impl TaskHandler for AlwaysFailTask {
            type Data = serde_json::Value;
            type Output = serde_json::Value;

            async fn run<S: scheduler::Scheduler>(
                &self,
                ctx: &mut TaskContext<S>,
                _data: Self::Data,
            ) -> Result<Self::Output, TaskError> {
                ctx.step_with_retry(
                    "always-fail",
                    RetryConfig::fixed(1, Duration::from_millis(50)),
                    || async {
                        Err::<String, _>(StepError::Failed {
                            step: "always-fail".to_string(),
                            reason: "permanent error".to_string(),
                        })
                    },
                )
                .await?;
                Ok(serde_json::json!({}))
            }
        }

        let mut registry = TaskRegistry::new();
        registry.register("always-fail-task", AlwaysFailTask);
        let registry = Arc::new(registry);

        let execution_id = format!("test-exhaust-{}", Uuid::new_v4());
        let durable = DurableScheduler::new(scheduler.clone(), registry.clone());

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
}
