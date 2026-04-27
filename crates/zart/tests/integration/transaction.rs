/// Phase 6: Spec 0023 — Transaction participation tests.
use super::helpers::*;
use std::borrow::Cow;
use std::time::Duration;
use uuid::Uuid;
use zart::{
    DurableRegistry, DurableScheduler, context::ZartStep, error::TaskError,
    registry::DurableExecution,
};

// ── Scenario 1: Transactional scheduling ───────────────────────────────

#[tokio::test]
#[ignore]
async fn start_in_tx_rollback_leaves_no_execution() {
    let scheduler = setup().await;
    let sched = DurableScheduler::new(scheduler.clone(), scheduler.task_scheduler());
    let pool = scheduler.pool().clone();

    let exec_id = format!("trx-rollback-{}", Uuid::new_v4());

    let mut tx = pool.begin().await.expect("begin tx failed");
    sched
        .start_in_tx(&mut tx, &exec_id, "noop", serde_json::json!({}))
        .await
        .expect("start_in_tx failed");

    tx.rollback().await.expect("rollback failed");

    let exec = scheduler
        .get_execution(&exec_id)
        .await
        .expect("get_execution failed");
    assert!(
        exec.is_none(),
        "execution should not exist after rollback, got: {exec:?}"
    );
}

#[tokio::test]
#[ignore]
async fn start_in_tx_commit_creates_execution_and_task() {
    let scheduler = setup().await;
    let sched = DurableScheduler::new(scheduler.clone(), scheduler.task_scheduler());
    let pool = scheduler.pool().clone();

    let exec_id = format!("trx-commit-{}", Uuid::new_v4());

    let mut tx = pool.begin().await.expect("begin tx failed");
    let result = sched
        .start_in_tx(&mut tx, &exec_id, "noop", serde_json::json!({}))
        .await
        .expect("start_in_tx failed");
    tx.commit().await.expect("commit failed");

    let exec = scheduler
        .get_execution(&exec_id)
        .await
        .expect("get_execution failed");
    assert!(exec.is_some(), "execution should exist after commit");

    let body_task_id = format!("{exec_id}:run:0:body:start");
    let task_exists: bool =
        sqlx::query_scalar(r#"SELECT EXISTS(SELECT 1 FROM zart_tasks WHERE task_id = $1)"#)
            .bind(&body_task_id)
            .fetch_one(&pool)
            .await
            .expect("query task failed");
    assert!(task_exists, "body task '{body_task_id}' should exist");

    assert_eq!(result.task_id, body_task_id);
}

#[tokio::test]
#[ignore]
async fn start_for_in_tx_commit_creates_execution_with_payload() {
    let scheduler = setup().await;
    let sched = DurableScheduler::new(scheduler.clone(), scheduler.task_scheduler());
    let pool = scheduler.pool().clone();

    let exec_id = format!("trx-typed-{}", Uuid::new_v4());

    let mut tx = pool.begin().await.expect("begin tx failed");
    sched
        .start_for_in_tx::<TestHandler>(&mut tx, &exec_id, "test_handler", &TestInput { value: 42 })
        .await
        .expect("start_for_in_tx failed");
    tx.commit().await.expect("commit failed");

    let exec = scheduler
        .get_execution(&exec_id)
        .await
        .expect("get_execution failed");
    assert!(exec.is_some(), "execution should exist after commit");
    let exec = exec.unwrap();
    assert_eq!(
        exec.payload.get("value"),
        Some(&serde_json::json!(42)),
        "payload should contain input value"
    );
}

#[tokio::test]
#[ignore]
async fn start_in_tx_duplicate_returns_not_supported() {
    let scheduler = setup().await;
    let sched = DurableScheduler::new(scheduler.clone(), scheduler.task_scheduler());
    let pool = scheduler.pool().clone();

    let exec_id = format!("trx-dup-{}", Uuid::new_v4());

    let mut tx = pool.begin().await.expect("begin tx failed");
    sched
        .start_in_tx(&mut tx, &exec_id, "noop", serde_json::json!({}))
        .await
        .expect("first start_in_tx failed");
    tx.commit().await.expect("first commit failed");

    let mut tx = pool.begin().await.expect("begin tx failed");
    let err = sched
        .start_in_tx(&mut tx, &exec_id, "noop", serde_json::json!({}))
        .await
        .expect_err("second start_in_tx should have failed");
    tx.rollback().await.expect("rollback failed");

    assert!(
        matches!(err, zart::error::SchedulerError::NotSupported(_)),
        "expected NotSupported error, got: {err:?}"
    );
}

// ── Scenario 2: Transactional step completion ─────────────────────────

#[tokio::test]
#[ignore]
async fn trx_called_outside_step_returns_error() {
    let pool = sqlx::PgPool::connect(&pg_url())
        .await
        .expect("failed to connect to PostgreSQL");

    let result = zart::trx(&pool).await;
    assert!(
        result.is_err(),
        "trx() should return Err when called outside a step"
    );
}

#[tokio::test]
#[ignore]
async fn trx_double_call_returns_error() {
    let scheduler = setup().await;
    let pool = scheduler.pool().clone();
    let mut registry = DurableRegistry::new();

    registry.register("double-trx", DoubleTrxHandler { pool });

    let sched = DurableScheduler::new(scheduler.clone(), scheduler.task_scheduler());
    let exec_id = format!("double-trx-{}", Uuid::new_v4());
    sched
        .start(&exec_id, "double-trx", serde_json::json!({}))
        .await
        .expect("start failed");

    let (worker, _handle) = spawn_worker(scheduler.clone(), registry);
    let result = sched
        .wait_completion::<serde_json::Value>(&exec_id, Duration::from_secs(10), None)
        .await;
    worker.stop();

    assert!(result.is_ok(), "execution should complete: {result:?}");
}

struct DoubleTrxStep {
    pool: sqlx::PgPool,
}

#[async_trait::async_trait]
impl ZartStep for DoubleTrxStep {
    type Output = serde_json::Value;
    type Error = TestStepError;

    fn step_name(&self) -> Cow<'static, str> {
        Cow::Borrowed("double-trx-step")
    }

    async fn run(&self) -> Result<Self::Output, Self::Error> {
        let _trx1 = zart::trx(&self.pool)
            .await
            .map_err(|e| TestStepError::Simple(e.to_string()))?;

        let result = zart::trx(&self.pool).await;
        assert!(result.is_err(), "second trx() should return Err");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("already called"),
            "error should mention double call: {err_msg}"
        );

        Ok(serde_json::json!({ "double_trx_guarded": true }))
    }
}

struct DoubleTrxHandler {
    pool: sqlx::PgPool,
}

#[async_trait::async_trait]
impl DurableExecution for DoubleTrxHandler {
    type Data = serde_json::Value;
    type Output = serde_json::Value;

    async fn run(&self, _data: Self::Data) -> Result<Self::Output, TaskError> {
        let outcome = zart::require(DoubleTrxStep {
            pool: self.pool.clone(),
        })
        .await?;
        Ok(outcome)
    }
}

#[tokio::test]
#[ignore]
async fn step_without_trx_completes_normally() {
    let scheduler = setup().await;
    let mut registry = DurableRegistry::new();

    registry.register("no-trx-step", NoTrxStepHandler);

    let sched = DurableScheduler::new(scheduler.clone(), scheduler.task_scheduler());
    let exec_id = format!("no-trx-{}", Uuid::new_v4());
    sched
        .start(&exec_id, "no-trx-step", serde_json::json!({}))
        .await
        .expect("start failed");

    let (worker, _handle) = spawn_worker(scheduler.clone(), registry);
    let result = sched
        .wait_completion::<serde_json::Value>(&exec_id, Duration::from_secs(10), None)
        .await;
    worker.stop();

    assert!(
        result.is_ok(),
        "step without trx should complete normally: {result:?}"
    );
    assert_eq!(
        result.unwrap().get("no_trx"),
        Some(&serde_json::json!(true)),
        "result should contain step output"
    );
}

struct NoTrxStepHandler;

#[async_trait::async_trait]
impl DurableExecution for NoTrxStepHandler {
    type Data = serde_json::Value;
    type Output = serde_json::Value;

    async fn run(&self, _data: Self::Data) -> Result<Self::Output, TaskError> {
        Ok(serde_json::json!({"no_trx": true}))
    }
}

#[tokio::test]
#[ignore]
async fn trx_atomic_step_write_and_completion_commit_together() {
    let scheduler = setup().await;
    let pool = scheduler.pool().clone();

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS zart_test_ledger \
         (key TEXT PRIMARY KEY, value BIGINT NOT NULL DEFAULT 0)",
    )
    .execute(&pool)
    .await
    .expect("create ledger table");

    let ledger_key = format!("atomic-ok-{}", Uuid::new_v4());

    struct AtomicWriteStep {
        pool: sqlx::PgPool,
        ledger_key: String,
    }

    #[async_trait::async_trait]
    impl ZartStep for AtomicWriteStep {
        type Output = serde_json::Value;
        type Error = TestStepError;

        fn step_name(&self) -> Cow<'static, str> {
            Cow::Borrowed("atomic-write")
        }

        async fn run(&self) -> Result<Self::Output, Self::Error> {
            let mut tx = zart::trx(&self.pool)
                .await
                .map_err(|e| TestStepError::Simple(e.to_string()))?;

            sqlx::query("INSERT INTO zart_test_ledger (key, value) VALUES ($1, 1)")
                .bind(&self.ledger_key)
                .execute(&mut **tx)
                .await
                .map_err(|e| TestStepError::Simple(e.to_string()))?;

            Ok(serde_json::json!({ "written": true }))
        }
    }

    struct AtomicHandler {
        pool: sqlx::PgPool,
        ledger_key: String,
    }

    #[async_trait::async_trait]
    impl DurableExecution for AtomicHandler {
        type Data = serde_json::Value;
        type Output = serde_json::Value;

        async fn run(&self, _data: Self::Data) -> Result<Self::Output, TaskError> {
            let out = zart::require(AtomicWriteStep {
                pool: self.pool.clone(),
                ledger_key: self.ledger_key.clone(),
            })
            .await?;
            Ok(out)
        }
    }

    let ledger_key_clone = ledger_key.clone();
    let pool_clone = pool.clone();
    let mut registry = DurableRegistry::new();
    registry.register(
        "atomic-write-handler",
        AtomicHandler {
            pool: pool_clone,
            ledger_key: ledger_key_clone,
        },
    );

    let sched = DurableScheduler::new(scheduler.clone(), scheduler.task_scheduler());
    let exec_id = format!("atomic-write-{}", Uuid::new_v4());
    sched
        .start(&exec_id, "atomic-write-handler", serde_json::json!({}))
        .await
        .expect("start failed");

    let (worker, _handle) = spawn_worker(scheduler.clone(), registry);
    let result = sched
        .wait_completion::<serde_json::Value>(&exec_id, Duration::from_secs(10), None)
        .await;
    worker.stop();

    assert!(
        result.is_ok(),
        "execution should complete successfully: {result:?}"
    );

    let val: Option<i64> = sqlx::query_scalar("SELECT value FROM zart_test_ledger WHERE key = $1")
        .bind(&ledger_key)
        .fetch_optional(&pool)
        .await
        .expect("ledger query failed");

    assert_eq!(
        val,
        Some(1),
        "ledger write should be committed after step success"
    );
}

#[tokio::test]
#[ignore]
async fn trx_atomic_step_write_rolls_back_on_step_error() {
    let scheduler = setup().await;
    let pool = scheduler.pool().clone();

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS zart_test_ledger \
         (key TEXT PRIMARY KEY, value BIGINT NOT NULL DEFAULT 0)",
    )
    .execute(&pool)
    .await
    .expect("create ledger table");

    let ledger_key = format!("atomic-err-{}", Uuid::new_v4());

    struct FailingWriteStep {
        pool: sqlx::PgPool,
        ledger_key: String,
    }

    #[async_trait::async_trait]
    impl ZartStep for FailingWriteStep {
        type Output = serde_json::Value;
        type Error = TestStepError;

        fn step_name(&self) -> Cow<'static, str> {
            Cow::Borrowed("failing-write")
        }

        async fn run(&self) -> Result<Self::Output, Self::Error> {
            let mut tx = zart::trx(&self.pool)
                .await
                .map_err(|e| TestStepError::Simple(e.to_string()))?;

            sqlx::query("INSERT INTO zart_test_ledger (key, value) VALUES ($1, 99)")
                .bind(&self.ledger_key)
                .execute(&mut **tx)
                .await
                .map_err(|e| TestStepError::Simple(e.to_string()))?;

            Err(TestStepError::Simple(
                "intentional step failure".to_string(),
            ))
        }
    }

    struct FailingHandler {
        pool: sqlx::PgPool,
        ledger_key: String,
    }

    #[async_trait::async_trait]
    impl DurableExecution for FailingHandler {
        type Data = serde_json::Value;
        type Output = serde_json::Value;

        async fn run(&self, _data: Self::Data) -> Result<Self::Output, TaskError> {
            zart::require(FailingWriteStep {
                pool: self.pool.clone(),
                ledger_key: self.ledger_key.clone(),
            })
            .await?;
            Ok(serde_json::json!({}))
        }
    }

    let ledger_key_clone = ledger_key.clone();
    let pool_clone = pool.clone();
    let mut registry = DurableRegistry::new();
    registry.register(
        "failing-write-handler",
        FailingHandler {
            pool: pool_clone,
            ledger_key: ledger_key_clone,
        },
    );

    let sched = DurableScheduler::new(scheduler.clone(), scheduler.task_scheduler());
    let exec_id = format!("atomic-err-{}", Uuid::new_v4());
    sched
        .start(&exec_id, "failing-write-handler", serde_json::json!({}))
        .await
        .expect("start failed");

    let (worker, _handle) = spawn_worker(scheduler.clone(), registry);
    let _ = sched.wait(&exec_id, Duration::from_secs(10), None).await;
    worker.stop();

    let val: Option<i64> = sqlx::query_scalar("SELECT value FROM zart_test_ledger WHERE key = $1")
        .bind(&ledger_key)
        .fetch_optional(&pool)
        .await
        .expect("ledger query failed");

    assert!(
        val.is_none(),
        "ledger write should be rolled back after step error; got: {val:?}"
    );
}
