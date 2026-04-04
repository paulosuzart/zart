//! Integration tests for the `zart` crate.
//!
//! Tests marked `#[ignore]` require a running PostgreSQL instance.
//! Run them with: `cargo test -- --include-ignored`

/// Placeholder: full end-to-end durable execution tests come in M2.
#[tokio::test]
#[ignore = "requires PostgreSQL — implement in M2"]
async fn durable_execution_runs_sequential_steps() {
    todo!("Implement in M2 with PostgresScheduler")
}

#[tokio::test]
#[ignore = "requires PostgreSQL — implement in M2"]
async fn failed_step_causes_execution_to_fail() {
    todo!("Implement in M2")
}

#[tokio::test]
#[ignore = "requires PostgreSQL — implement in M3"]
async fn step_retries_on_transient_failure() {
    todo!("Implement in M3 with RetryConfig")
}
