//! Integration tests for the `zart` crate.
//!
//! Tests marked `#[ignore]` require a running PostgreSQL instance.
//! Start it with `just up`, then run: `just test-integration`

#[cfg(test)]
mod m2 {
    use scheduler::{ExecutionStatus, PostgresScheduler, Scheduler as _};
    use std::sync::Arc;
    use std::time::Duration;
    use uuid::Uuid;
    use zart::{
        context::TaskContext, error::TaskError, registry::TaskHandler, DurableScheduler,
        TaskRegistry, Worker, WorkerConfig,
    };

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

    // ── Handlers ──────────────────────────────────────────────────────────────

    /// A task that runs two sequential steps, each returning a value used by
    /// the next one. Proves the full re-entry and caching path.
    struct SequentialTask;

    impl TaskHandler for SequentialTask {
        type Data = serde_json::Value;
        type Output = serde_json::Value;

        fn run<'life0, 'life1, 'async_trait, S: scheduler::Scheduler>(
            &'life0 self,
            ctx: &'life1 mut TaskContext<S>,
            _data: Self::Data,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = Result<Self::Output, TaskError>>
                    + Send
                    + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(async move {
                let step1: i32 = ctx
                    .step("step-one", || async { Ok(21i32) })
                    .await?;

                let step2: i32 = ctx
                    .step("step-two", || async { Ok(step1 * 2) })
                    .await?;

                Ok(serde_json::json!({ "answer": step2 }))
            })
        }
    }

    /// A task whose first step always fails with a real (non-control-flow) error.
    struct FailingTask;

    impl TaskHandler for FailingTask {
        type Data = serde_json::Value;
        type Output = serde_json::Value;

        fn run<'life0, 'life1, 'async_trait, S: scheduler::Scheduler>(
            &'life0 self,
            ctx: &'life1 mut TaskContext<S>,
            _data: Self::Data,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = Result<Self::Output, TaskError>>
                    + Send
                    + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(async move {
                ctx.step("fail-step", || async {
                    Err::<serde_json::Value, _>(zart::StepError::Failed {
                        step: "fail-step".to_string(),
                        reason: "intentional failure".to_string(),
                    })
                })
                .await?;

                Ok(serde_json::json!({}))
            })
        }
    }

    // ── Helper: run a worker briefly in background ────────────────────────────

    /// Spawns a worker and returns its stop handle.
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
    #[ignore = "requires PostgreSQL — implement in M3"]
    async fn step_retries_on_transient_failure() {
        // Implemented in M3 with RetryConfig.
    }
}
