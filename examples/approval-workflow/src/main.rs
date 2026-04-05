//! Approval Workflow Example
//!
//! Demonstrates a human-in-the-loop durable execution:
//! 1. Validate the approval request (fake step)
//! 2. Wait for manager approval (via wait_for_event)
//! 3. On approval: process the request and return a result
//! 4. On rejection: return a rejection notice
//!
//! Features: wait_for_event, offer_event, sequential steps.

use async_trait::async_trait;
use scheduler::PostgresScheduler;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use zart::prelude::*;
use zart::registry::TaskHandler;
use zart::context::TaskContext;
use zart::error::{StepError, TaskError};

// ── Input / Output types ──────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ApprovalRequest {
    requester_name: String,
    resource: String,
    reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ApprovalDecision {
    approved: bool,
    reviewer: String,
    comment: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ApprovalOutput {
    decision: String,
    requester: String,
    resource: String,
    reviewer: String,
    comment: String,
}

// ── Task handler ──────────────────────────────────────────────────────────────

struct ApprovalTask;

#[async_trait]
impl TaskHandler for ApprovalTask {
    type Data = ApprovalRequest;
    type Output = ApprovalOutput;

    async fn run<S: scheduler::Scheduler>(
        &self,
        ctx: &mut TaskContext<S>,
        data: Self::Data,
    ) -> Result<Self::Output, TaskError> {
        // Step 1: Validate the request (fake step)
        let _validated = ctx
            .step("validate-request", || {
                let request = data.clone();
                async move {
                    if request.requester_name.is_empty() {
                        return Err(StepError::Failed {
                            step: "validate-request".to_string(),
                            reason: "requester name is required".to_string(),
                        });
                    }
                    Ok(format!(
                        "Validated request from {} for {}",
                        request.requester_name, request.resource
                    ))
                }
            })
            .await?;

        // Step 2: Wait for manager approval
        let decision: ApprovalDecision = ctx
            .wait_for_event("manager-approval", Some(Duration::from_secs(120)))
            .await?;

        // Step 3: Act on the decision
        if decision.approved {
            ctx.step("process-approved", || {
                let resource = data.resource.clone();
                let requester = data.requester_name.clone();
                async move {
                    // In a real system, this would provision the resource
                    Ok(format!(
                        "Provisioned {} for {}",
                        resource, requester
                    ))
                }
            })
            .await?;
        }

        Ok(ApprovalOutput {
            decision: if decision.approved {
                "approved".to_string()
            } else {
                "rejected".to_string()
            },
            requester: data.requester_name,
            resource: data.resource,
            reviewer: decision.reviewer,
            comment: decision.comment,
        })
    }
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    println!("=== Zart Approval Workflow Example ===\n");

    let db_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://zart:zart@localhost:5432/zart".to_string());

    let pool = sqlx::PgPool::connect(&db_url).await?;
    let sched = Arc::new(PostgresScheduler::new(pool));
    sched.run_migrations().await?;

    let mut registry = TaskRegistry::new();
    registry.register("approval-task", ApprovalTask);
    let registry = Arc::new(registry);

    let execution_id = format!("approval-demo-{}", uuid::Uuid::new_v4());
    let durable = DurableScheduler::new(sched.clone(), registry.clone());

    let request = ApprovalRequest {
        requester_name: "Bob".to_string(),
        resource: "staging-environment".to_string(),
        reason: "Need to test the new deployment pipeline".to_string(),
    };

    println!("Starting execution '{execution_id}'...");
    println!("  Requester: {}", request.requester_name);
    println!("  Resource:  {}", request.resource);
    println!("  Reason:    {}", request.reason);
    durable
        .start_typed(&execution_id, "approval-task", &request)
        .await?;

    // Start worker
    let config = zart::WorkerConfig {
        poll_interval: Duration::from_millis(200),
        max_tasks_per_poll: 10,
        max_concurrent_tasks: 4,
        shutdown_timeout: Duration::from_secs(5),
        orphan_timeout: Duration::from_secs(30),
    };
    let worker = Arc::new(zart::Worker::new(sched.clone(), registry.clone(), config));
    let w = worker.clone();
    let _handle = tokio::spawn(async move { w.run().await });

    // Wait a bit for the execution to park itself waiting for the event
    tokio::time::sleep(Duration::from_secs(2)).await;
    println!("\nExecution is waiting for manager approval...");
    println!("(Simulating manager delivering the event after 2 seconds)\n");

    // Deliver the approval event (simulates a manager clicking "Approve" in a UI)
    let decision = ApprovalDecision {
        approved: true,
        reviewer: "Manager Carol".to_string(),
        comment: "Looks good — proceed!".to_string(),
    };

    println!("Delivering approval event...");
    durable
        .offer_event(
            &execution_id,
            "manager-approval",
            serde_json::to_value(&decision)?,
        )
        .await?;

    // Now wait for the execution to complete
    println!("Approval received! Processing approved request...\n");
    let record = durable
        .wait(&execution_id, Duration::from_secs(60), None)
        .await?;

    worker.stop();

    match record.status {
        scheduler::ExecutionStatus::Completed => {
            let output: ApprovalOutput = serde_json::from_value(record.result.unwrap())?;
            println!("Execution completed!");
            println!("  Decision:  {}", output.decision);
            println!("  Requester: {}", output.requester);
            println!("  Resource:  {}", output.resource);
            println!("  Reviewer:  {}", output.reviewer);
            println!("  Comment:   {}", output.comment);
        }
        _ => {
            eprintln!("Execution ended with status: {:?}", record.status);
            if let Some(result) = &record.result {
                eprintln!("Result: {}", result);
            }
        }
    }

    Ok(())
}
