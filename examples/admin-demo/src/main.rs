//! Admin API demonstration example.
//!
//! Demonstrates:
//! 1. `wait_completion<T>` — typed result deserialization
//! 2. `start_and_wait_for` — start + wait in one call with handler type inference
//! 3. `restart` — full restart with history preservation
//! 4. `retry_step` — retry a dead step
//! 5. `rerun_steps` — selective rerun with dependency warnings
//! 6. `pause` / `resume` / `list_pause_rules` — pause lifecycle

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;
use zart::ExecutionStatus;
use zart::PgBackend;
use zart::admin::{PauseScope, RerunSpec};
use zart::error::{SchedulerError, TaskError};
use zart::prelude::*;

// ── Handler ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AdminDemoInput {
    /// Force a step failure for retry demonstration.
    fail_step: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AdminDemoOutput {
    step_one: i32,
    step_two: i32,
    final_result: String,
}

struct AdminDemoTask;

#[async_trait::async_trait]
impl DurableExecution for AdminDemoTask {
    type Data = AdminDemoInput;
    type Output = AdminDemoOutput;

    async fn run(&self, data: Self::Data) -> Result<Self::Output, TaskError> {
        let step_one: i32 = zart::require(StepOne).await?;
        let step_two: i32 = zart::require(StepTwo {
            fail: data.fail_step,
        })
        .await?;

        Ok(AdminDemoOutput {
            step_one,
            step_two,
            final_result: format!("step_one={step_one}, step_two={step_two}"),
        })
    }

    fn max_retries(&self) -> usize {
        0 // No execution-level retries — we want controlled failures.
    }
}

struct StepOne;

#[async_trait::async_trait]
impl ZartStep for StepOne {
    type Output = i32;
    type Error = DemoStepError;

    fn step_name(&self) -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("step-one")
    }

    async fn run(&self) -> Result<Self::Output, Self::Error> {
        tracing::info!("[step-one] computing");
        Ok(42)
    }
}

struct StepTwo {
    fail: bool,
}

#[async_trait::async_trait]
impl ZartStep for StepTwo {
    type Output = i32;
    type Error = DemoStepError;

    fn step_name(&self) -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("step-two")
    }

    async fn run(&self) -> Result<Self::Output, Self::Error> {
        if self.fail {
            tracing::warn!("[step-two] intentionally failing");
            return Err(DemoStepError("intentional failure".into()));
        }
        tracing::info!("[step-two] computing");
        Ok(100)
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct DemoStepError(String);

impl std::fmt::Display for DemoStepError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for DemoStepError {}

// ── Main ─────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let db_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://zart:zart@localhost:5432/zart".to_string());

    let pool = sqlx::PgPool::connect(&db_url).await?;
    let pg = PgBackend::new(pool);
    let durable = Arc::new(DurableScheduler::from_backend(&pg));

    // ── 1. Typed wait_completion ──────────────────────────────────────────────

    println!("=== 1. start_for + wait_completion ===\n");
    {
        let execution_id = format!("admin-typed-wait-{}", Uuid::new_v4());

        // Start execution normally
        durable
            .start_for::<AdminDemoTask>(
                &execution_id,
                "zart::admin_demo::AdminDemoTask",
                &AdminDemoInput { fail_step: false },
            )
            .await?;

        // Run worker
        let worker = spawn_worker(&pg);

        // Typed wait — no manual serde_json::from_value needed!
        let output: AdminDemoOutput = durable
            .wait_completion(&execution_id, Duration::from_secs(15), None)
            .await?;

        worker.stop();

        println!("  step_one:  {}", output.step_one);
        println!("  step_two:  {}", output.step_two);
        println!("  result:    {}\n", output.final_result);
    }

    // ── 2. start_and_wait_for ─────────────────────────────────────────────────

    println!("=== 2. start_and_wait_for ===\n");
    {
        let execution_id = format!("admin-start-and-wait-{}", Uuid::new_v4());
        let worker = spawn_worker(&pg);

        // Start + wait in one call — types inferred from handler
        let output = durable
            .start_and_wait_for::<AdminDemoTask>(
                &execution_id,
                "zart::admin_demo::AdminDemoTask",
                &AdminDemoInput { fail_step: false },
                Duration::from_secs(15),
            )
            .await?;

        worker.stop();
        println!("  result: {}\n", output.final_result);
    }

    // ── 3. Restart ────────────────────────────────────────────────────────────

    println!("=== 3. restart (full restart) ===\n");
    {
        let execution_id = format!("admin-restart-{}", Uuid::new_v4());
        let worker = spawn_worker(&pg);

        // Run to completion first
        durable
            .start_for::<AdminDemoTask>(
                &execution_id,
                "zart::admin_demo::AdminDemoTask",
                &AdminDemoInput { fail_step: false },
            )
            .await?;

        let _ = durable
            .wait(&execution_id, Duration::from_secs(15), None)
            .await?;
        worker.stop();

        // Now restart with a new payload
        let new_run_id = durable
            .restart(
                &execution_id,
                Some(serde_json::json!({ "fail_step": false })),
                Some("demo-restart"),
            )
            .await?;

        println!("  Restarted: new run = {new_run_id}");

        // Run worker again for the new run
        let worker = spawn_worker(&pg);
        let _ = durable
            .wait(&execution_id, Duration::from_secs(15), None)
            .await?;
        worker.stop();

        // List all runs
        let runs = durable.list_runs(&execution_id).await?;
        println!("  Total runs: {}", runs.len());
        for r in &runs {
            println!(
                "    run:{}  status:{}  trigger:{}",
                r.run_index,
                r.status,
                format!("{:?}", r.trigger).to_lowercase()
            );
        }
        println!();
    }

    // ── 4. retry_step ────────────────────────────────────────────────────────

    println!("=== 4. retry_step ===\n");
    {
        let execution_id = format!("admin-retry-{}", Uuid::new_v4());
        let worker = spawn_worker(&pg);

        // Start with a failing step — it will go dead (no retries)
        durable
            .start_for::<AdminDemoTask>(
                &execution_id,
                "zart::admin_demo::AdminDemoTask",
                &AdminDemoInput { fail_step: true },
            )
            .await?;

        let record = durable
            .wait(&execution_id, Duration::from_secs(15), None)
            .await?;
        worker.stop();

        if record.status == ExecutionStatus::Failed {
            println!("  Execution failed as expected.");
            let run_id = durable.get_current_run_id(&execution_id).await?.unwrap();

            // Retry the dead step
            match durable
                .retry_step(&run_id, "step-two", Some("demo-retry"))
                .await
            {
                Ok(new_task_id) => {
                    println!("  Step retried: new task = {new_task_id}");

                    // Re-run worker to pick up the retried step
                    let worker = spawn_worker(&pg);
                    let record = durable
                        .wait(&execution_id, Duration::from_secs(15), None)
                        .await?;
                    worker.stop();
                    println!("  After retry: status = {}", record.status);
                }
                Err(SchedulerError::Database(_)) => {
                    println!("  (Step retry not available — no dead step found)");
                }
                Err(e) => eprintln!("  Retry error: {e}"),
            }
        }
        println!();
    }

    // ── 5. rerun_steps ───────────────────────────────────────────────────────

    println!("=== 5. rerun_steps (selective rerun) ===\n");
    {
        let execution_id = format!("admin-rerun-{}", Uuid::new_v4());
        let worker = spawn_worker(&pg);

        // Run to completion
        durable
            .start_for::<AdminDemoTask>(
                &execution_id,
                "zart::admin_demo::AdminDemoTask",
                &AdminDemoInput { fail_step: false },
            )
            .await?;
        let _ = durable
            .wait(&execution_id, Duration::from_secs(15), None)
            .await?;
        worker.stop();

        // Selective rerun: force rerun step-one, preserve step-two
        let result = durable
            .rerun_steps(
                &execution_id,
                RerunSpec {
                    force_rerun: vec!["step-one".into()],
                    preserve: vec!["step-two".into()],
                    triggered_by: Some("demo-rerun".to_string()),
                },
            )
            .await?;

        println!("  New run number: {}", result.new_run_number);
        println!("  Effective rerun: {}", result.effective_rerun.join(", "));
        if !result.potentially_stale.is_empty() {
            println!("  Potentially stale preserved steps:");
            for dep in &result.potentially_stale {
                println!(
                    "    • '{}' may depend on: {}",
                    dep.preserved_step,
                    dep.possibly_depends_on.join(", ")
                );
            }
        }

        // Run worker for the rerun
        let worker = spawn_worker(&pg);
        let record = durable
            .wait(&execution_id, Duration::from_secs(15), None)
            .await?;
        worker.stop();
        println!("  After rerun: status = {}\n", record.status);
    }

    // ── 6. pause / resume / list_pause_rules ─────────────────────────────────

    println!("=== 6. pause / resume / list_pause_rules ===\n");
    {
        // Create a pause rule for this task name
        let rule = durable
            .pause(PauseScope {
                task_name: Some("zart::admin_demo::AdminDemoTask".into()),
                step_pattern: Some("step-two".into()),
                triggered_by: Some("demo-pause".to_string()),
                ..Default::default()
            })
            .await?;

        println!("  Created pause rule: {}", rule.rule_id);

        // List rules
        let rules = durable.list_pause_rules(None).await?;
        println!("  Active pause rules: {}", rules.len());
        for r in &rules {
            println!("    {}  scope: {:?}", r.rule_id, format!("{:?}", r.scope));
        }

        // Resume (soft-delete the rule)
        let result = durable.resume(PauseScope::default()).await?;
        println!("  Resumed: {} rules deleted\n", result.rules_deleted);
    }

    println!("=== Admin demo complete ===");
    Ok(())
}

fn spawn_worker(pg: &PgBackend) -> Arc<zart::Worker> {
    let config = zart::WorkerConfig {
        poll_interval: Duration::from_millis(100),
        max_tasks_per_poll: 10,
        max_concurrent_tasks: 4,
        shutdown_timeout: Duration::from_secs(10),
        orphan_timeout: Duration::from_secs(30),
        ..Default::default()
    };
    let worker = Arc::new(
        zart::WorkerBuilder::from_backend(pg)
            .register_durable_task("zart::admin_demo::AdminDemoTask", AdminDemoTask)
            .config(config)
            .build(),
    );
    let w = worker.clone();
    tokio::spawn(async move { w.run().await });
    worker
}
