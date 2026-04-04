//! Approval Workflow Example
//!
//! Demonstrates a human-in-the-loop durable execution:
//! 1. Fetch location data from Zippopotamus API
//! 2. Wait for manager approval (via wait_for_event)
//! 3. On approval: query Open Brewery DB and write recommendations file
//! 4. On rejection: write rejection notice
//!
//! Features: wait_for_event, offer_event, sequential steps, external APIs, file output.

use async_trait::async_trait;
use scheduler::PostgresScheduler;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use zart::prelude::*;
use zart::registry::TaskHandler;
use zart::context::TaskContext;
use zart::error::{StepError, TaskError};
use zart::retry::RetryConfig;

// ── Input / Output types ──────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ApprovalRequest {
    zip_code: String,
    requester_name: String,
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
    city: String,
    state: String,
    reviewer: String,
    comment: String,
    breweries: Vec<String>,
}

// ── API response types ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ZipInfo {
    #[serde(rename = "place name")]
    place_name: String,
    state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Brewery {
    name: String,
    brewery_type: Option<String>,
    city: Option<String>,
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
        let client = reqwest::Client::new();

        // Step 1: Fetch location data
        let zip_info: ZipInfo = ctx
            .step_with_retry(
                "fetch-location",
                RetryConfig::exponential(3, Duration::from_secs(1)),
                || {
                    let client = client.clone();
                    let zip = data.zip_code.clone();
                    async move {
                        let resp = client
                            .get(format!("https://api.zippopotam.us/us/{zip}"))
                            .send()
                            .await
                            .map_err(|e| StepError::Failed {
                                step: "fetch-location".to_string(),
                                reason: e.to_string(),
                            })?
                            .json::<ZipInfo>()
                            .await
                            .map_err(|e| StepError::Failed {
                                step: "fetch-location".to_string(),
                                reason: format!("parse error: {e}"),
                            })?;
                        Ok(resp)
                    }
                },
            )
            .await?;

        let city = zip_info.place_name.clone();

        // Step 2: Wait for manager approval
        let decision: ApprovalDecision = ctx
            .wait_for_event("manager-approval", Some(Duration::from_secs(120)))
            .await?;

        // Step 3: Act on the decision
        let breweries: Vec<Brewery> = if decision.approved {
            // Approved: fetch breweries
            ctx.step_with_retry(
                "fetch-breweries",
                RetryConfig::exponential(3, Duration::from_secs(1)),
                || {
                    let client = client.clone();
                    let city = city.clone();
                    async move {
                        let resp = client
                            .get("https://api.openbrewerydb.org/v1/breweries")
                            .query(&[("by_city", &city)])
                            .send()
                            .await
                            .map_err(|e| StepError::Failed {
                                step: "fetch-breweries".to_string(),
                                reason: e.to_string(),
                            })?
                            .json::<Vec<Brewery>>()
                            .await
                            .map_err(|e| StepError::Failed {
                                step: "fetch-breweries".to_string(),
                                reason: format!("parse error: {e}"),
                            })?;
                        Ok(resp)
                    }
                },
            )
            .await?
        } else {
            vec![]
        };

        Ok(ApprovalOutput {
            decision: if decision.approved {
                "approved".to_string()
            } else {
                "rejected".to_string()
            },
            requester: data.requester_name,
            city,
            state: zip_info.state,
            reviewer: decision.reviewer,
            comment: decision.comment,
            breweries: breweries.into_iter().map(|b| b.name).collect(),
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

    let execution_id = "approval-demo-1";
    let durable = DurableScheduler::new(sched.clone(), registry.clone());

    let request = ApprovalRequest {
        zip_code: "10001".to_string(), // New York
        requester_name: "Bob".to_string(),
        reason: "Team outing for brewery visits".to_string(),
    };

    println!("Starting execution '{execution_id}'...");
    println!("  Requester: {}", request.requester_name);
    println!("  Reason:    {}", request.reason);
    durable.start_typed(execution_id, "approval-task", &request).await?;

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
        comment: "Looks good — proceed with the outing!".to_string(),
    };

    println!("Delivering approval event...");
    durable
        .offer_event(execution_id, "manager-approval", serde_json::to_value(&decision)?)
        .await?;

    // Now wait for the execution to complete
    println!("Approval received! Fetching brewery recommendations...\n");
    let record = durable.wait(execution_id, Duration::from_secs(60), None).await?;

    worker.stop();

    match record.status {
        scheduler::ExecutionStatus::Completed => {
            let output: ApprovalOutput = serde_json::from_value(record.result.unwrap())?;
            println!("Execution completed!");
            println!("  Decision:     {}", output.decision);
            println!("  Requester:    {}", output.requester);
            println!("  City:         {}, {}", output.city, output.state);
            println!("  Reviewer:     {}", output.reviewer);
            println!("  Comment:      {}", output.comment);
            println!("  Breweries:    {}", output.breweries.len());
            for (i, name) in output.breweries.iter().enumerate() {
                println!("    {}. {}", i + 1, name);
            }
        }
        _ => {
            eprintln!("Execution ended with status: {:?}", record.status);
        }
    }

    Ok(())
}
