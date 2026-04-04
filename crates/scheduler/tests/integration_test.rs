//! Integration tests for the scheduler crate.
//!
//! Tests marked with `#[ignore]` require a running PostgreSQL instance.
//! Run them with: `cargo test -- --include-ignored`
//! or via: `just test-integration`

/// Returns a PostgreSQL connection string from the environment.
/// Defaults to a local Docker Compose instance.
fn pg_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://zart:zart@localhost:5432/zart".to_string())
}

/// Placeholder: real PostgreSQL integration tests will be added in M1
/// when `PostgresScheduler` is implemented.
#[tokio::test]
#[ignore = "requires PostgreSQL — run with: cargo test -- --include-ignored"]
async fn postgres_schedule_and_poll() {
    let _url = pg_url();
    // TODO(M1): instantiate PostgresScheduler, schedule a task, poll it back.
    todo!("Implement in M1 with PostgresScheduler")
}

#[tokio::test]
#[ignore = "requires PostgreSQL — run with: cargo test -- --include-ignored"]
async fn postgres_skip_lock_prevents_duplicate_pickup() {
    let _url = pg_url();
    // TODO(M1): spawn two concurrent pollers, verify the same task is only picked up once.
    todo!("Implement in M1 with PostgresScheduler")
}

#[tokio::test]
#[ignore = "requires PostgreSQL — run with: cargo test -- --include-ignored"]
async fn postgres_recurring_cron_task_reschedules() {
    let _url = pg_url();
    // TODO(M4): verify that completing a cron task inserts the next occurrence.
    todo!("Implement in M4 with recurring task support")
}
