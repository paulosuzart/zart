//! Integration tests for the scheduler crate.
//!
//! Tests marked with `#[ignore]` require a running PostgreSQL instance.
//! Run them with: `cargo test -- --include-ignored`
//! or via: `just test-integration`

mod postgres_tests {
    use sqlx::PgPool;
    use uuid::Uuid;
    use zart_scheduler::{PostgresTaskScheduler, TaskScheduler};

    /// Returns a PostgreSQL connection string from the environment.
    /// Defaults to the local Docker Compose instance.
    fn pg_url() -> String {
        std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://zart:zart@localhost:5432/zart".to_string())
    }

    /// Build a pool, run migrations, and return both.
    async fn setup() -> (PgPool, PostgresTaskScheduler) {
        let pool = PgPool::connect(&pg_url())
            .await
            .expect("failed to connect to PostgreSQL");

        let scheduler = PostgresTaskScheduler::new(pool.clone());
        scheduler.run_migrations().await.expect("migrations failed");

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
        let tables = [
            "tasks",
            "executions",
            "execution_runs",
            "steps",
            "step_attempts",
        ];
        for base in &tables {
            let custom = format!("{prefix}{base}");
            let zart = format!("zart_{base}");
            // INCLUDING ALL copies column defaults, check constraints, indexes
            // (including PK/unique), and storage parameters — but NOT FK constraints,
            // which is fine since our queries don't rely on cross-table FKs at test time.
            sqlx::query(&format!(
                "CREATE TABLE {custom} (LIKE {zart} INCLUDING ALL)"
            ))
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("failed to create table {custom}: {e}"));
        }
    }

    async fn drop_custom_tables(pool: &PgPool, prefix: &str) {
        // Drop in reverse FK order.
        let tables = [
            "step_attempts",
            "steps",
            "execution_runs",
            "executions",
            "tasks",
        ];
        for base in &tables {
            let custom = format!("{prefix}{base}");
            sqlx::query(&format!("DROP TABLE IF EXISTS {custom} CASCADE"))
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

        // Run default migrations to ensure zart_* tables and ENUMs exist.
        let default_scheduler = PostgresTaskScheduler::new(pool.clone());
        default_scheduler
            .run_migrations()
            .await
            .expect("migrations failed");

        // Create custom-prefixed tables for this test.
        let prefix = "custtest_";
        setup_custom_tables(&pool, prefix).await;

        let names = zart_scheduler::TableNames::with_prefix(prefix).expect("with_prefix failed");
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
        let in_custom: i64 = sqlx::query_scalar(&format!(
            "SELECT COUNT(*) FROM {prefix}tasks WHERE task_id = $1"
        ))
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
}
