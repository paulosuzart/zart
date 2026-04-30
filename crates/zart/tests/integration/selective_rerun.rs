/// Selective rerun tests: verify that preserved step results are copied to the new run.
use super::helpers::*;
use std::sync::atomic::Ordering;
use std::time::Duration;
use uuid::Uuid;
use zart::{DurableRegistry, DurableScheduler, admin::RerunSpec};

// ── Three-step task ───────────────────────────────────────────────────────────

pub struct StepA;

#[async_trait::async_trait]
impl ZartStep for StepA {
    type Output = String;
    type Error = TestStepError;
    fn step_name(&self) -> Cow<'static, str> {
        Cow::Borrowed("step-a")
    }
    async fn run(&self) -> Result<Self::Output, Self::Error> {
        Ok("result-a".to_string())
    }
}

pub struct StepB {
    pub counter: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl ZartStep for StepB {
    type Output = String;
    type Error = TestStepError;
    fn step_name(&self) -> Cow<'static, str> {
        Cow::Borrowed("step-b")
    }
    async fn run(&self) -> Result<Self::Output, Self::Error> {
        self.counter.fetch_add(1, Ordering::SeqCst);
        Ok("result-b".to_string())
    }
}

pub struct StepC;

#[async_trait::async_trait]
impl ZartStep for StepC {
    type Output = String;
    type Error = TestStepError;
    fn step_name(&self) -> Cow<'static, str> {
        Cow::Borrowed("step-c")
    }
    async fn run(&self) -> Result<Self::Output, Self::Error> {
        Ok("result-c".to_string())
    }
}

pub struct ThreeStepTask {
    pub step_b_counter: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl zart::registry::DurableExecution for ThreeStepTask {
    type Data = serde_json::Value;
    type Output = serde_json::Value;

    async fn run(&self, _data: Self::Data) -> Result<Self::Output, zart::error::TaskError> {
        let a: String = zart::require(StepA).await?;
        let b: String = zart::require(StepB {
            counter: self.step_b_counter.clone(),
        })
        .await?;
        let c: String = zart::require(StepC).await?;
        Ok(serde_json::json!({ "a": a, "b": b, "c": c }))
    }
}

// ── Test ──────────────────────────────────────────────────────────────────────

/// Tests that selective rerun copies preserved step results into the new run so
/// the body does not re-execute them.
///
/// Scenario:
/// 1. Run a three-step execution to completion (step-a, step-b, step-c).
/// 2. Call `rerun_steps` with `force_rerun: ["step-b"]`.
/// 3. Assert step-a and step-c are in the new run with status=completed and
///    the same result values.
/// 4. Assert step-b is NOT in the new run (it will be re-scheduled on replay).
/// 5. Run the new run to completion and assert final status is completed.
/// 6. Assert step-b was executed exactly twice (once per run).
#[tokio::test]
#[ignore = "requires PostgreSQL — run with: just test-integration"]
async fn selective_rerun_copies_preserved_steps_to_new_run() {
    let pg = setup().await;
    let step_b_counter = Arc::new(AtomicUsize::new(0));

    let make_registry = |counter: Arc<AtomicUsize>| {
        let mut registry = DurableRegistry::new();
        registry.register(
            "three-step-task",
            ThreeStepTask {
                step_b_counter: counter,
            },
        );
        registry
    };

    let execution_id = format!("selective-rerun-{}", Uuid::new_v4());
    let durable = DurableScheduler::from_backend(pg.as_ref());

    // ── Phase 1: run to completion ────────────────────────────────────────────

    durable
        .start(&execution_id, "three-step-task", serde_json::json!({}))
        .await
        .expect("start failed");

    let (worker, _handle) = spawn_worker(pg.clone(), make_registry(step_b_counter.clone()));

    let record = durable
        .wait(&execution_id, Duration::from_secs(15), None)
        .await
        .expect("wait after first run failed");

    worker.stop();

    assert_eq!(record.status, ExecutionStatus::Completed);
    assert_eq!(
        step_b_counter.load(Ordering::SeqCst),
        1,
        "step-b should run once in first run"
    );

    // ── Phase 2: selective rerun of step-b ────────────────────────────────────

    let rerun_result = durable
        .rerun_steps(
            &execution_id,
            RerunSpec {
                force_rerun: vec!["step-b".to_string()],
                preserve: vec![],
                triggered_by: Some("test".to_string()),
            },
        )
        .await
        .expect("rerun_steps failed");

    assert!(
        rerun_result.effective_rerun.contains(&"step-b".to_string()),
        "effective_rerun must include step-b"
    );

    // ── Phase 3: verify step rows in the new run ──────────────────────────────

    let new_run_id = format!("{execution_id}:run:{}", rerun_result.new_run_number);

    // Query the new run's steps directly to verify the copy.
    let rows: Vec<(String, StepStatus, Option<serde_json::Value>)> =
        sqlx::query_as("SELECT step_name, status, result FROM zart_steps WHERE run_id = $1")
            .bind(&new_run_id)
            .fetch_all(pg.pool())
            .await
            .expect("query new run steps failed");

    let find_step = |name: &str| rows.iter().find(|(n, _, _)| n == name).cloned();

    let (_, status_a, result_a) = find_step("step-a").expect("step-a should be in new run");
    assert_eq!(
        status_a,
        StepStatus::Completed,
        "step-a should be completed"
    );
    assert_eq!(
        result_a.as_ref().and_then(|v| v.as_str()),
        Some("result-a"),
        "step-a result should be preserved"
    );

    let (_, status_c, result_c) = find_step("step-c").expect("step-c should be in new run");
    assert_eq!(
        status_c,
        StepStatus::Completed,
        "step-c should be completed"
    );
    assert_eq!(
        result_c.as_ref().and_then(|v| v.as_str()),
        Some("result-c"),
        "step-c result should be preserved"
    );

    assert!(
        find_step("step-b").is_none(),
        "step-b should NOT be in new run before replay"
    );

    // ── Phase 4: run the new run to completion ────────────────────────────────

    let (worker2, _handle2) = spawn_worker(pg.clone(), make_registry(step_b_counter.clone()));

    let record2 = durable
        .wait(&execution_id, Duration::from_secs(15), None)
        .await
        .expect("wait after second run failed");

    worker2.stop();

    assert_eq!(record2.status, ExecutionStatus::Completed);
    assert_eq!(
        step_b_counter.load(Ordering::SeqCst),
        2,
        "step-b should run exactly once more in the rerun"
    );
}
