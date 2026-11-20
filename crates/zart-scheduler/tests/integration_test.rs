//! Integration tests for the scheduler crate.
//!
//! Tests marked with `#[ignore]` require a running PostgreSQL instance.
//! Run them with: `cargo test -- --include-ignored`
//! or via: `just test-integration`

mod postgres_tests {
    use chrono::{DateTime, Utc};
    use sqlx::PgPool;
    use uuid::Uuid;
    use zart_scheduler::{PostgresTaskScheduler, TaskScheduler};

    /// Returns a PostgreSQL connection string from the environment.
    /// Defaults to the local Docker Compose instance.
    fn pg_url() -> String {
        std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://zart:zart@localhost:5432/zart".to_string())
    }

    /// Build a pool, create a scheduler, and return both.
    /// Schema is expected to be provisioned via `PgBackend::run_migrations()`
    /// or by applying the SQL file before running tests.
    async fn setup() -> (PgPool, PostgresTaskScheduler) {
        let pool = PgPool::connect(&pg_url())
            .await
            .expect("failed to connect to PostgreSQL");

        let scheduler = PostgresTaskScheduler::new(pool.clone());

        (pool, scheduler)
    }

    /// Clean up tasks created during a test.
    async fn cleanup(pool: &PgPool, task_ids: &[&str]) {
        for id in task_ids {
            let _ = sqlx::query("DELETE FROM zart_tasks WHERE task_id = $1")
                .bind(id)
                .execute(pool)
                .await;
        }
    }

    #[tokio::test]
    #[ignore = "requires PostgreSQL — run with: just test-integration"]
    async fn postgres_schedule_now_and_poll() {
        let (pool, scheduler) = setup().await;
        // Unique ID per run prevents interference between parallel tests.
        let task_id = format!("test-schedule-now-{}", Uuid::new_v4());

        let result = scheduler
            .schedule_now(&task_id, "test-task", serde_json::json!({"key": "value"}))
            .await
            .expect("schedule_now failed");

        assert_eq!(result.task_id, task_id);

        // Poll — should find the task we just scheduled.
        let tasks = scheduler
            .poll_due(chrono::Utc::now(), 100)
            .await
            .expect("poll_due failed");

        let fetched = tasks.iter().find(|t| t.task_id == task_id);
        assert!(fetched.is_some(), "scheduled task not returned by poll_due");

        let fetched = fetched.unwrap();
        assert_eq!(fetched.task_name, "test-task");
        assert_eq!(fetched.data, serde_json::json!({"key": "value"}));
        assert_eq!(fetched.attempt, 1);

        scheduler
            .mark_completed(
                &task_id,
                Some(serde_json::json!("done")),
                &fetched.lock_token,
            )
            .await
            .expect("mark_completed failed");

        cleanup(&pool, &[task_id.as_str()]).await;
    }

    #[tokio::test]
    #[ignore = "requires PostgreSQL — run with: just test-integration"]
    async fn postgres_skip_lock_prevents_duplicate_pickup() {
        let (pool, scheduler) = setup().await;
        let task_id = format!("test-skip-lock-{}", Uuid::new_v4());

        scheduler
            .schedule_now(&task_id, "test-task", serde_json::json!({}))
            .await
            .expect("schedule_now failed");

        let now = chrono::Utc::now();

        // Poll twice concurrently — only one should get the task.
        let (poll_a, poll_b) =
            tokio::join!(scheduler.poll_due(now, 5), scheduler.poll_due(now, 5),);

        let tasks_a = poll_a.expect("poll A failed");
        let tasks_b = poll_b.expect("poll B failed");

        let got_a = tasks_a.iter().any(|t| t.task_id == task_id);
        let got_b = tasks_b.iter().any(|t| t.task_id == task_id);

        // Exactly one of the two pollers should have acquired the task.
        assert!(
            got_a ^ got_b,
            "SKIP LOCKED violated: task picked up by both or neither \
             (got_a={got_a}, got_b={got_b})"
        );

        cleanup(&pool, &[task_id.as_str()]).await;
    }

    #[tokio::test]
    #[ignore = "requires PostgreSQL — run with: just test-integration"]
    async fn postgres_mark_failed_and_reschedule() {
        let (pool, scheduler) = setup().await;
        let task_id = format!("test-mark-failed-{}", Uuid::new_v4());

        scheduler
            .schedule_now(&task_id, "test-task", serde_json::json!({}))
            .await
            .expect("schedule_now failed");

        let tasks = scheduler
            .poll_due(chrono::Utc::now(), 100)
            .await
            .expect("poll_due failed");

        let fetched = tasks.iter().find(|t| t.task_id == task_id).unwrap();

        // Mark failed without rescheduling.
        scheduler
            .mark_failed(&task_id, "something went wrong", None, &fetched.lock_token)
            .await
            .expect("mark_failed failed");

        // Task must not appear in poll_due with status='failed'.
        let tasks_after = scheduler
            .poll_due(chrono::Utc::now(), 100)
            .await
            .expect("second poll_due failed");

        assert!(
            !tasks_after.iter().any(|t| t.task_id == task_id),
            "failed task should not be returned by poll_due"
        );

        cleanup(&pool, &[task_id.as_str()]).await;
    }

    #[tokio::test]
    #[ignore = "requires PostgreSQL — run with: just test-integration"]
    async fn postgres_cancel_scheduled_task() {
        let (pool, scheduler) = setup().await;
        let task_id = format!("test-cancel-{}", Uuid::new_v4());

        scheduler
            .schedule_now(&task_id, "test-task", serde_json::json!({}))
            .await
            .expect("schedule_now failed");

        let cancelled = scheduler
            .cancel_task(&task_id)
            .await
            .expect("cancel_task failed");

        assert!(cancelled, "expected cancel_task to return true");

        let tasks = scheduler
            .poll_due(chrono::Utc::now(), 100)
            .await
            .expect("poll_due failed");

        assert!(
            !tasks.iter().any(|t| t.task_id == task_id),
            "cancelled task should not appear in poll_due"
        );

        cleanup(&pool, &[task_id.as_str()]).await;
    }

    #[tokio::test]
    #[ignore = "requires PostgreSQL — run with: just test-integration"]
    async fn postgres_schedule_at_future_not_polled_yet() {
        let (pool, scheduler) = setup().await;
        let task_id = format!("test-future-{}", Uuid::new_v4());
        let far_future = chrono::Utc::now() + chrono::Duration::hours(24);

        scheduler
            .schedule_at(zart_scheduler::ScheduleAtParams {
                task_id: task_id.clone(),
                task_name: "test-task".to_string(),
                execution_time: far_future,
                data: serde_json::json!({}),
                recurrence: None,
                metadata: serde_json::Value::Null,
            })
            .await
            .expect("schedule_at failed");

        // Polling now must not return a task due 24 hours from now.
        let tasks = scheduler
            .poll_due(chrono::Utc::now(), 100)
            .await
            .expect("poll_due failed");

        assert!(
            !tasks.iter().any(|t| t.task_id == task_id),
            "future-scheduled task should not be returned by poll_due"
        );

        cleanup(&pool, &[task_id.as_str()]).await;
    }

    #[tokio::test]
    #[ignore = "requires PostgreSQL — run with: just test-integration"]
    async fn postgres_recurring_cron_task_reschedules() {
        // Schema-level smoke test: validates a recurrence value is accepted.
        // Full recurring task logic is implemented in M4.
        let (pool, scheduler) = setup().await;
        let task_id = format!("test-recurring-{}", Uuid::new_v4());
        let recurrence = zart_scheduler::Recurrence::FixedDelay {
            duration_ms: 60_000,
        };

        scheduler
            .schedule_at(zart_scheduler::ScheduleAtParams {
                task_id: task_id.clone(),
                task_name: "recurring-task".to_string(),
                execution_time: chrono::Utc::now(),
                data: serde_json::json!({}),
                recurrence: Some(recurrence),
                metadata: serde_json::Value::Null,
            })
            .await
            .expect("schedule_at with recurrence failed");

        cleanup(&pool, &[task_id.as_str()]).await;
    }

    /// Create custom-prefixed tables by copying the structure of the default ones.
    ///
    /// Always drops then recreates so stale tables from a previous failed run
    /// can't leave behind an incorrect schema (e.g. missing PK constraints).
    async fn setup_custom_tables(pool: &PgPool, prefix: &str) {
        // Drop in FK-safe reverse order first.
        drop_custom_tables(pool, prefix).await;

        // Create in FK-safe forward order.
        // Only the scheduler crate's own table — execution tables live in the zart crate.
        let tables = ["tasks"];
        for base in &tables {
            let custom = format!("{prefix}{base}");
            let zart = format!("zart_{base}");
            // INCLUDING ALL copies column defaults, check constraints, indexes
            // (including PK/unique), and storage parameters — but NOT FK constraints,
            // which is fine since our queries don't rely on cross-table FKs at test time.
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "CREATE TABLE {custom} (LIKE {zart} INCLUDING ALL)"
            )))
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("failed to create table {custom}: {e}"));
        }
    }

    async fn drop_custom_tables(pool: &PgPool, prefix: &str) {
        // Drop in reverse FK order.
        let tables = ["tasks"];
        for base in &tables {
            let custom = format!("{prefix}{base}");
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "DROP TABLE IF EXISTS {custom} CASCADE"
            )))
            .execute(pool)
            .await
            .ok();
        }
    }

    #[tokio::test]
    #[ignore = "requires PostgreSQL — run with: just test-integration"]
    async fn postgres_custom_prefix_schedule_and_poll() {
        let pool = PgPool::connect(&pg_url())
            .await
            .expect("failed to connect to PostgreSQL");

        // Schema is expected to be provisioned beforehand,
        // ensuring zart_* tables and ENUMs exist.
        let prefix = "custtest_";
        setup_custom_tables(&pool, prefix).await;

        let config = zart_core::table_names::TableNameConfig::with_prefix(prefix)
            .expect("with_prefix failed");
        let names = zart_scheduler::TableNames::from_config(config);
        let scheduler = PostgresTaskScheduler::with_table_names(pool.clone(), names);

        let task_id = format!("custom-prefix-{}", Uuid::new_v4());

        // Schedule using the custom-prefixed scheduler.
        let result = scheduler
            .schedule_now(&task_id, "custom-test-task", serde_json::json!({"x": 1}))
            .await
            .expect("schedule_now failed");

        assert_eq!(result.task_id, task_id);

        // Verify the task is NOT in the default zart_tasks table.
        let in_default: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM zart_tasks WHERE task_id = $1")
                .bind(&task_id)
                .fetch_one(&pool)
                .await
                .expect("query failed");
        assert_eq!(in_default, 0, "task should NOT be in zart_tasks");

        // Verify the task IS in the custom table.
        let in_custom: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT COUNT(*) FROM {prefix}tasks WHERE task_id = $1"
        )))
        .bind(&task_id)
        .fetch_one(&pool)
        .await
        .expect("query failed");
        assert_eq!(in_custom, 1, "task should be in {prefix}tasks");

        // Poll and complete.
        let tasks = scheduler
            .poll_due(chrono::Utc::now(), 100)
            .await
            .expect("poll_due failed");

        let fetched = tasks
            .iter()
            .find(|t| t.task_id == task_id)
            .expect("scheduled task not returned by poll_due");

        scheduler
            .mark_completed(
                &task_id,
                Some(serde_json::json!("done")),
                &fetched.lock_token,
            )
            .await
            .expect("mark_completed failed");

        drop_custom_tables(&pool, prefix).await;
    }

    #[tokio::test]
    #[ignore = "requires PostgreSQL — run with: just test-integration"]
    async fn postgres_recurring_task_reschedules() {
        let (pool, scheduler) = setup().await;
        let task_id = format!("test-recurring-{}", Uuid::new_v4());

        // Schedule a recurring task with FixedDelay of 1 second
        let result = scheduler
            .schedule_recurring(
                &task_id,
                "recurring-test-task",
                zart_scheduler::Recurrence::FixedDelay { duration_ms: 1000 },
                serde_json::json!({}),
            )
            .await
            .expect("schedule_recurring failed");

        assert_eq!(result.task_id, task_id);

        // Poll for the initial execution
        let tasks = scheduler
            .poll_due(chrono::Utc::now(), 100)
            .await
            .expect("poll_due failed");
        let fetched = tasks
            .iter()
            .find(|t| t.task_id == task_id)
            .expect("recurring task not found");

        // Simulate worker completing the task (no explicit reschedule)
        scheduler
            .mark_completed(&task_id, None, &fetched.lock_token)
            .await
            .expect("mark_completed failed");

        // Check that the task was rescheduled: execution_time should be in the future
        let (next_exec,): (Option<DateTime<Utc>>,) =
            sqlx::query_as("SELECT execution_time FROM zart_tasks WHERE task_id = $1")
                .bind(&task_id)
                .fetch_one(&pool)
                .await
                .expect("query failed");

        assert!(
            next_exec.is_some(),
            "recurring task should have a next execution time after completion"
        );
        let next_exec = next_exec.unwrap();
        assert!(
            next_exec > chrono::Utc::now(),
            "next execution time should be in the future"
        );

        // Cleanup
        cleanup(&pool, &[task_id.as_str()]).await;
    }
}

mod wakeup_and_backpressure_tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use async_trait::async_trait;
    use sqlx::PgPool;
    use tokio::sync::mpsc;
    use uuid::Uuid;
    use zart_scheduler::{
        CompletionHandler, OnComplete, PostgresTaskScheduler, ScheduledTask, SchedulerTaskError,
        TaskInstance, TaskRegistry, TaskScheduler, Worker, WorkerConfig,
    };

    fn pg_url() -> String {
        std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://zart:zart@localhost:5432/zart".to_string())
    }

    async fn setup_pool() -> PgPool {
        PgPool::connect(&pg_url())
            .await
            .expect("failed to connect to PostgreSQL")
    }

    async fn cleanup(pool: &PgPool, task_ids: &[String]) {
        if task_ids.is_empty() {
            return;
        }
        let _ = sqlx::query("DELETE FROM zart_tasks WHERE task_id = ANY($1)")
            .bind(task_ids)
            .execute(pool)
            .await;
    }

    struct ConcurrencyGuard {
        active: Arc<AtomicUsize>,
    }

    impl Drop for ConcurrencyGuard {
        fn drop(&mut self) {
            self.active.fetch_sub(1, Ordering::SeqCst);
        }
    }

    struct RecordingTask {
        active: Arc<AtomicUsize>,
        peak: Arc<AtomicUsize>,
        completed: Arc<AtomicUsize>,
        work_delay: Duration,
        done_tx: mpsc::UnboundedSender<String>,
    }

    #[async_trait]
    impl ScheduledTask for RecordingTask {
        async fn execute(
            &self,
            instance: &TaskInstance,
        ) -> Result<Box<dyn CompletionHandler>, SchedulerTaskError> {
            let current = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(current, Ordering::SeqCst);
            let _guard = ConcurrencyGuard {
                active: Arc::clone(&self.active),
            };

            if !self.work_delay.is_zero() {
                tokio::time::sleep(self.work_delay).await;
            }

            self.completed.fetch_add(1, Ordering::SeqCst);
            let _ = self.done_tx.send(instance.task_id.clone());
            Ok(OnComplete::done())
        }
    }

    fn make_recording_task(
        work_delay: Duration,
    ) -> (
        RecordingTask,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
        mpsc::UnboundedReceiver<String>,
    ) {
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let completed = Arc::new(AtomicUsize::new(0));
        let (done_tx, done_rx) = mpsc::unbounded_channel();
        let task = RecordingTask {
            active,
            peak: Arc::clone(&peak),
            completed: Arc::clone(&completed),
            work_delay,
            done_tx,
        };
        (task, peak, completed, done_rx)
    }

    #[tokio::test]
    #[ignore = "requires PostgreSQL — run with: just test-integration"]
    async fn postgres_worker_sub_100ms_wakeup_via_listen_notify() {
        let pool = setup_pool().await;
        let worker_scheduler = Arc::new(PostgresTaskScheduler::new(pool.clone()));
        // Separate scheduler instance has its own local_wakeup, so the worker
        // can only wake sub-100ms via PostgreSQL LISTEN / NOTIFY.
        let producer_scheduler = PostgresTaskScheduler::new(pool.clone());

        let (task, _peak, completed, mut done_rx) = make_recording_task(Duration::ZERO);
        let mut registry = TaskRegistry::new();
        registry.register("wakeup-notify-task", task);

        let worker = Arc::new(Worker::new(
            worker_scheduler,
            Arc::new(registry),
            WorkerConfig {
                poll_interval: Duration::from_secs(30),
                enable_listen_notify: true,
                shutdown_timeout: Duration::from_secs(5),
                ..Default::default()
            },
            vec![],
        ));
        let w = Arc::clone(&worker);
        let handle = tokio::spawn(async move { w.run().await });

        // Allow initial poll and PgListener subscription to establish.
        tokio::time::sleep(Duration::from_millis(250)).await;

        let task_id = format!("test-notify-wakeup-{}", Uuid::new_v4());
        let start = Instant::now();
        producer_scheduler
            .schedule_now(&task_id, "wakeup-notify-task", serde_json::json!({}))
            .await
            .expect("schedule_now failed");

        let finished_id = tokio::time::timeout(Duration::from_secs(1), done_rx.recv())
            .await
            .expect("worker did not wake up within 1s via LISTEN/NOTIFY")
            .expect("channel closed unexpectedly");
        let elapsed = start.elapsed();

        assert_eq!(finished_id, task_id);
        assert_eq!(completed.load(Ordering::SeqCst), 1);
        assert!(
            elapsed < Duration::from_secs(1),
            "expected < 1s wakeup (well below 30s poll_interval), took {elapsed:?}"
        );

        worker.stop();
        let _ = handle.await;
        cleanup(&pool, &[task_id]).await;
    }

    #[tokio::test]
    #[ignore = "requires PostgreSQL — run with: just test-integration"]
    async fn postgres_worker_sub_100ms_wakeup_via_local_wakeup() {
        let pool = setup_pool().await;
        let scheduler = Arc::new(PostgresTaskScheduler::new(pool.clone()));

        let (task, _peak, completed, mut done_rx) = make_recording_task(Duration::ZERO);
        let mut registry = TaskRegistry::new();
        registry.register("wakeup-local-task", task);

        let worker = Arc::new(Worker::new(
            Arc::clone(&scheduler) as Arc<dyn TaskScheduler>,
            Arc::new(registry),
            WorkerConfig {
                poll_interval: Duration::from_secs(30),
                enable_listen_notify: false,
                shutdown_timeout: Duration::from_secs(5),
                ..Default::default()
            },
            vec![],
        ));
        let w = Arc::clone(&worker);
        let handle = tokio::spawn(async move { w.run().await });

        // Allow initial poll to complete so the worker parks on wait_for_wakeup.
        tokio::time::sleep(Duration::from_millis(200)).await;

        let task_id = format!("test-local-wakeup-{}", Uuid::new_v4());
        let start = Instant::now();
        scheduler
            .schedule_now(&task_id, "wakeup-local-task", serde_json::json!({}))
            .await
            .expect("schedule_now failed");

        let finished_id = tokio::time::timeout(Duration::from_secs(1), done_rx.recv())
            .await
            .expect("worker did not wake up within 1s via local_wakeup")
            .expect("channel closed unexpectedly");
        let elapsed = start.elapsed();

        assert_eq!(finished_id, task_id);
        assert_eq!(completed.load(Ordering::SeqCst), 1);
        assert!(
            elapsed < Duration::from_secs(1),
            "expected < 1s local_wakeup (well below 30s poll_interval), took {elapsed:?}"
        );

        worker.stop();
        let _ = handle.await;
        cleanup(&pool, &[task_id]).await;
    }

    #[tokio::test]
    #[ignore = "requires PostgreSQL — run with: just test-integration"]
    async fn postgres_worker_backpressure_and_drain_under_burst() {
        const TOTAL_TASKS: usize = 50;
        const MAX_CONCURRENT: usize = 4;

        let pool = setup_pool().await;
        let scheduler = Arc::new(PostgresTaskScheduler::new(pool.clone()));

        let mut task_ids = Vec::with_capacity(TOTAL_TASKS);
        for i in 0..TOTAL_TASKS {
            let task_id = format!("test-burst-{i}-{}", Uuid::new_v4());
            scheduler
                .schedule_now(&task_id, "burst-task", serde_json::json!({ "idx": i }))
                .await
                .expect("schedule_now failed");
            task_ids.push(task_id);
        }

        let (task, peak, completed, mut done_rx) = make_recording_task(Duration::from_millis(40));
        let mut registry = TaskRegistry::new();
        registry.register("burst-task", task);

        let worker = Arc::new(Worker::new(
            scheduler,
            Arc::new(registry),
            WorkerConfig {
                poll_interval: Duration::from_secs(30),
                max_concurrent_tasks: MAX_CONCURRENT,
                max_tasks_per_poll: 10,
                enable_listen_notify: true,
                shutdown_timeout: Duration::from_secs(5),
                ..Default::default()
            },
            vec![],
        ));

        let start = Instant::now();
        let w = Arc::clone(&worker);
        let handle = tokio::spawn(async move { w.run().await });

        let mut received = 0usize;
        tokio::time::timeout(Duration::from_secs(10), async {
            while received < TOTAL_TASKS {
                done_rx.recv().await.expect("channel closed unexpectedly");
                received += 1;
            }
        })
        .await
        .expect("50 burst tasks did not drain within 10s");

        let elapsed = start.elapsed();

        // Wait for Postgres `mark_completed` commits before stopping the worker,
        // since `done_rx` fires inside the task handler prior to DB completion.
        let completed_in_db = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let count: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM zart_tasks WHERE task_id = ANY($1) AND status = 'completed'",
                )
                .bind(&task_ids)
                .fetch_one(&pool)
                .await
                .expect("DB count query failed");
                if count == TOTAL_TASKS as i64 {
                    return count;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("DB status did not reach TOTAL_TASKS completed within 5s");

        worker.stop();
        let _ = handle.await;

        let observed_peak = peak.load(Ordering::SeqCst);
        assert!(
            observed_peak <= MAX_CONCURRENT,
            "concurrency peak {observed_peak} exceeded max_concurrent_tasks {MAX_CONCURRENT}"
        );
        assert!(
            observed_peak >= 2,
            "expected concurrent execution, but peak was {observed_peak}"
        );
        assert_eq!(completed.load(Ordering::SeqCst), TOTAL_TASKS);
        assert!(
            elapsed < Duration::from_secs(5),
            "burst should drain continuously without 30s sleeps, took {elapsed:?}"
        );
        assert_eq!(completed_in_db, TOTAL_TASKS as i64);

        cleanup(&pool, &task_ids).await;
    }

    #[tokio::test]
    #[ignore = "requires PostgreSQL — run with: just test-integration"]
    async fn postgres_worker_fallback_poll_executes_external_insert() {
        let pool = setup_pool().await;
        let scheduler = Arc::new(PostgresTaskScheduler::new(pool.clone()));

        let (task, _peak, completed, mut done_rx) = make_recording_task(Duration::ZERO);
        let mut registry = TaskRegistry::new();
        registry.register("fallback-poll-task", task);

        let worker = Arc::new(Worker::new(
            scheduler,
            Arc::new(registry),
            WorkerConfig {
                poll_interval: Duration::from_millis(200),
                enable_listen_notify: false,
                shutdown_timeout: Duration::from_secs(5),
                ..Default::default()
            },
            vec![],
        ));
        let w = Arc::clone(&worker);
        let handle = tokio::spawn(async move { w.run().await });

        // Let initial poll finish so the worker enters the 200ms fallback sleep.
        tokio::time::sleep(Duration::from_millis(50)).await;

        let task_id = format!("test-fallback-{}", Uuid::new_v4());
        let start = Instant::now();
        // External raw SQL insert: neither NOTIFY nor local_wakeup fires.
        sqlx::query(
            r#"
            INSERT INTO zart_tasks (task_id, task_name, execution_time, data, recurrence, metadata)
            VALUES ($1, $2, NOW(), $3, NULL, 'null'::jsonb)
            "#,
        )
        .bind(&task_id)
        .bind("fallback-poll-task")
        .bind(serde_json::json!({ "source": "external" }))
        .execute(&pool)
        .await
        .expect("external SQL insert failed");

        let finished_id = tokio::time::timeout(Duration::from_secs(2), done_rx.recv())
            .await
            .expect("worker did not pick up external insert within fallback poll window")
            .expect("channel closed unexpectedly");
        let elapsed = start.elapsed();

        assert_eq!(finished_id, task_id);
        assert_eq!(completed.load(Ordering::SeqCst), 1);
        assert!(
            elapsed < Duration::from_secs(1),
            "expected fallback poll pickup within 1s, took {elapsed:?}"
        );

        worker.stop();
        let _ = handle.await;
        cleanup(&pool, &[task_id]).await;
    }
}
