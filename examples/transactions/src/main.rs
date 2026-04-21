#![allow(deprecated)]
//! Demonstrates Zart's transaction participation features.
//!
//! **Scenario 1** — `start_in_tx`: atomically create a user record and start a
//! durable onboarding execution in the same database transaction.
//!
//! **Scenario 2** — `zart::trx`: deduct account balance atomically with step
//! completion so a crash after the step returns cannot cause a double-charge.
//!
//! Run with: `just example-transactions`

use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;
use zart::PostgresStorage;
use zart::context::ZartStep;
use zart::error::TaskError;
use zart::prelude::*;
use zart::trx;

// ── Schema (created on first run) ────────────────────────────────────────────

async fn ensure_schema(pool: &sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS demo_users (
            id        UUID PRIMARY KEY,
            email     TEXT NOT NULL,
            balance   BIGINT NOT NULL DEFAULT 0,
            onboarded BOOLEAN NOT NULL DEFAULT FALSE
        )
        "#,
    )
    .execute(pool)
    .await?;
    Ok(())
}

// ── Scenario 1: Transactional Scheduling ─────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OnboardInput {
    user_id: Uuid,
    email: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OnboardOutput {
    user_id: Uuid,
    email: String,
    balance: i64,
}

struct OnboardUser;

#[async_trait::async_trait]
impl DurableExecution for OnboardUser {
    type Data = OnboardInput;
    type Output = OnboardOutput;

    async fn run(&self, data: Self::Data) -> Result<Self::Output, TaskError> {
        // Deduct welcome bonus from balance (uses zart::trx internally).
        let balance = zart::require(DeductBalanceStep {
            user_id: data.user_id,
            amount: 100,
        })
        .await?;

        // Mark user as onboarded.
        zart::require(MarkOnboardedStep {
            user_id: data.user_id,
        })
        .await?;

        Ok(OnboardOutput {
            user_id: data.user_id,
            email: data.email,
            balance,
        })
    }
}

/// Step that deducts from user balance using `zart::trx` for atomic
/// completion — the DB write and step-completion metadata commit together.
struct DeductBalanceStep {
    user_id: Uuid,
    amount: i64,
}

#[async_trait::async_trait]
impl ZartStep for DeductBalanceStep {
    type Output = i64;
    type Error = TransferError;

    fn step_name(&self) -> Cow<'static, str> {
        Cow::Borrowed("deduct-balance")
    }

    async fn run(&self) -> Result<Self::Output, Self::Error> {
        let pool = get_pool();
        let mut tx = trx(pool).await.map_err(|e| TransferError::Framework {
            reason: e.to_string(),
        })?;

        let row: (i64,) = sqlx::query_as(
            r#"
            UPDATE demo_users
            SET balance = balance - $1
            WHERE id = $2
            RETURNING balance
            "#,
        )
        .bind(self.amount)
        .bind(self.user_id)
        .fetch_one(&mut **tx)
        .await
        .map_err(|e| TransferError::Db {
            reason: e.to_string(),
        })?;

        Ok(row.0)
    }
}

struct MarkOnboardedStep {
    user_id: Uuid,
}

#[async_trait::async_trait]
impl ZartStep for MarkOnboardedStep {
    type Output = ();
    type Error = TransferError;

    fn step_name(&self) -> Cow<'static, str> {
        Cow::Borrowed("mark-onboarded")
    }

    async fn run(&self) -> Result<Self::Output, Self::Error> {
        let pool = get_pool();
        sqlx::query(r#"UPDATE demo_users SET onboarded = TRUE WHERE id = $1"#)
            .bind(self.user_id)
            .execute(pool)
            .await
            .map_err(|e| TransferError::Db {
                reason: e.to_string(),
            })?;
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum TransferError {
    Db { reason: String },
    Framework { reason: String },
}

impl std::fmt::Display for TransferError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransferError::Db { reason } => write!(f, "database error: {reason}"),
            TransferError::Framework { reason } => write!(f, "framework error: {reason}"),
        }
    }
}

impl std::error::Error for TransferError {}

// ── Shared pool (set by main) ────────────────────────────────────────────────

static POOL: std::sync::OnceLock<sqlx::PgPool> = std::sync::OnceLock::new();

fn get_pool() -> &'static sqlx::PgPool {
    POOL.get().expect("pool not initialized")
}

// ── Main ─────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    println!("=== Zart Transaction Example ===\n");

    let db_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://zart:zart@localhost:5432/zart".to_string());

    let pool = sqlx::PgPool::connect(&db_url).await?;
    POOL.set(pool.clone()).ok();

    let sched = Arc::new(PostgresStorage::new(pool.clone()));
    ensure_schema(&pool).await?;

    let user_id = Uuid::new_v4();
    let email = format!("user-{user_id}@example.com");

    // ── Scenario 1: Transactional Scheduling ─────────────────────────────────────────
    println!("--- Scenario 1: Transactional Scheduling ---");
    println!("Creating user and starting onboarding in a single transaction...\n");

    let durable = DurableScheduler::new(sched.clone(), sched.task_scheduler());

    let mut tx = pool.begin().await?;

    // Insert the user row.
    sqlx::query(
        r#"
        INSERT INTO demo_users (id, email, balance)
        VALUES ($1, $2, 1000)
        "#,
    )
    .bind(user_id)
    .bind(&email)
    .execute(&mut *tx)
    .await?;

    // Start the durable execution — same transaction.
    durable
        .start_in_tx(
            &mut tx,
            &format!("onboard-{user_id}"),
            "onboard-user",
            serde_json::to_value(OnboardInput {
                user_id,
                email: email.clone(),
            })?,
        )
        .await?;

    // Both commit atomically — or neither exists.
    tx.commit().await?;

    println!("  User created:     {email}");
    println!("  User ID:          {user_id}");
    println!("  Initial balance:  1000");
    println!("  Execution started atomically ✓\n");

    // ── Run the worker ───────────────────────────────────────────────────
    println!("--- Running Worker (Scenario 2: zart::trx in deduct-balance step) ---\n");

    let mut registry = DurableRegistry::new();
    registry.register("onboard-user", OnboardUser);

    let config = zart::WorkerConfig {
        poll_interval: Duration::from_millis(200),
        max_tasks_per_poll: 10,
        max_concurrent_tasks: 4,
        shutdown_timeout: Duration::from_secs(30),
        orphan_timeout: Duration::from_secs(60),
        ..Default::default()
    };
    let worker = Arc::new(
        zart::WorkerBuilder::new(sched.clone(), sched.task_scheduler())
            .registry(registry)
            .config(config)
            .build(),
    );
    let w = worker.clone();
    let _handle = tokio::spawn(async move { w.run().await });

    let execution_id = format!("onboard-{user_id}");
    let output: OnboardOutput = durable
        .wait_completion(&execution_id, Duration::from_secs(30), None)
        .await?;

    worker.stop();

    // ── Verify results ───────────────────────────────────────────────────
    println!("--- Results ---\n");
    println!("  Execution completed ✓");
    println!("  User:           {}", output.email);
    println!("  Final balance:  {}", output.balance);
    println!("  (1000 initial - 100 bonus deduction = 900)\n");

    assert_eq!(output.balance, 900, "balance should be 900 after deduction");
    println!("=== All checks passed ===");

    Ok(())
}
