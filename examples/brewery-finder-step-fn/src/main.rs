//! Brewery Finder Example — Using #[zart_step] macro
//!
//! This example demonstrates the new step function model where steps are defined
//! as standalone async functions and then composed in the durable body.
//!
//! Usage:
//!   DATABASE_URL=postgres://zart:zart@localhost:5432/zart cargo run --bin example-brewery-finder-step-fn

use chrono::Utc;
use scheduler::PostgresScheduler;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;
use zart::error::StepError;
use zart::prelude::*;
use zart::zart_step;

// ── Input / Output types ──────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FinderInput {
    zip_code: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BreweryInfo {
    name: String,
    brewery_type: String,
    city: String,
    state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FinderOutput {
    zip_code: String,
    city: String,
    state: String,
    breweries: Vec<BreweryInfo>,
    found_at: String,
}

// ── External API response types ───────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
struct PlaceInfo {
    #[serde(rename = "place name")]
    place_name: String,
    state: String,
}

#[derive(Debug, Clone, Deserialize)]
struct ZipResponse {
    places: Vec<PlaceInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BreweryRaw {
    name: String,
    #[serde(default)]
    brewery_type: Option<String>,
    city: Option<String>,
    #[serde(default)]
    state: Option<String>,
}

// ── Step definitions using #[zart_step] ───────────────────────────────────────

/// Step 1: Look up city/state from a US ZIP code via Zippopotamus API.
/// Retries up to 3 times with exponential backoff.
#[zart_step("lookup-zip", retry = "exponential(3, 1s)")]
async fn lookup_zip(
    client: &reqwest::Client,
    zip_code: &str,
    ctx: StepContext,
) -> Result<(String, String), StepError> {
    println!("[lookup-zip] Attempt {}", ctx.current_attempt() + 1);

    let resp = client
        .get(format!("https://api.zippopotam.us/us/{zip_code}"))
        .send()
        .await
        .map_err(|e| StepError::Failed {
            step: "lookup-zip".to_string(),
            reason: e.to_string(),
        })?;

    let zip_resp: ZipResponse = resp.json().await.map_err(|e| StepError::Failed {
        step: "lookup-zip".to_string(),
        reason: format!("failed to parse response: {e}"),
    })?;

    let place = zip_resp.places.first().ok_or_else(|| StepError::Failed {
        step: "lookup-zip".to_string(),
        reason: format!("no place found for ZIP {zip_code}"),
    })?;

    Ok((place.place_name.clone(), place.state.clone()))
}

/// Step 2: Find breweries in a city via Open Brewery DB API.
/// Retries up to 3 times with exponential backoff.
#[zart_step("find-breweries", retry = "exponential(3, 1s)")]
async fn find_breweries(
    client: &reqwest::Client,
    city: &str,
    ctx: StepContext,
) -> Result<Vec<BreweryRaw>, StepError> {
    println!("[find-breweries] Attempt {}", ctx.current_attempt() + 1);

    let resp = client
        .get("https://api.openbrewerydb.org/v1/breweries")
        .query(&[("by_city", city)])
        .send()
        .await
        .map_err(|e| StepError::Failed {
            step: "find-breweries".to_string(),
            reason: e.to_string(),
        })?;

    resp.json().await.map_err(|e| StepError::Failed {
        step: "find-breweries".to_string(),
        reason: format!("failed to parse response: {e}"),
    })
}

/// Step 3: Transform raw API data into structured output.
/// No retry needed — this is a pure computation step.
#[zart_step("transform-results")]
async fn transform_results(
    raw: &[BreweryRaw],
    city: &str,
    state: &str,
    ctx: StepContext,
) -> Result<Vec<BreweryInfo>, StepError> {
    let _ = ctx.current_attempt(); // Suppress unused warning
    Ok(raw
        .iter()
        .map(|b| BreweryInfo {
            name: b.name.clone(),
            brewery_type: b.brewery_type.clone().unwrap_or_else(|| "unknown".to_string()),
            city: b.city.clone().unwrap_or_else(|| city.to_string()),
            state: b.state.clone().unwrap_or_else(|| state.to_string()),
        })
        .collect())
}

// ── Durable execution body: pure orchestration flow ──────────────────────────

#[zart::zart_durable("brewery-finder-step-fn", timeout = "5m")]
async fn brewery_finder(
    ctx: &mut TaskContext,
    data: FinderInput,
) -> Result<FinderOutput, TaskError> {
    let client = reqwest::Client::new();

    // Step 1: Look up ZIP code → (city, state)
    let (city, state) = lookup_zip(&client, &data.zip_code).execute(ctx).await?;

    // Step 2: Find breweries in the city
    let raw_breweries = find_breweries(&client, &city).execute(ctx).await?;

    // Step 3: Transform into structured output
    let breweries = transform_results(&raw_breweries, &city, &state).execute(ctx).await?;

    Ok(FinderOutput {
        zip_code: data.zip_code,
        city,
        state,
        breweries,
        found_at: Utc::now().to_rfc3339(),
    })
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

    println!("=== Zart Brewery Finder Example (Step Functions) ===\n");

    let db_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://zart:zart@localhost:5432/zart".to_string());

    // Connect and run migrations
    let pool = sqlx::PgPool::connect(&db_url).await?;
    let sched = Arc::new(PostgresScheduler::new(pool));
    sched.run_migrations().await?;

    // Register the handler
    let mut registry = TaskRegistry::new();
    registry.register("brewery-finder-step-fn", BreweryFinder);
    let registry = Arc::new(registry);

    // Start durable execution
    let execution_id = format!("brewery-finder-step-fn-{}", Uuid::new_v4());
    let durable = DurableScheduler::new(sched.clone());

    let input = FinderInput {
        zip_code: "97209".to_string(), // Portland, OR — lots of breweries
    };

    println!(
        "Starting execution '{execution_id}' for ZIP {}...",
        input.zip_code
    );
    durable
        .start_typed(&execution_id, "brewery-finder-step-fn", &input)
        .await?;

    // Run worker
    let config = zart::WorkerConfig {
        poll_interval: Duration::from_millis(200),
        max_tasks_per_poll: 10,
        max_concurrent_tasks: 4,
        shutdown_timeout: Duration::from_secs(5),
        orphan_timeout: Duration::from_secs(30),
        ..Default::default()
    };
    let worker = Arc::new(zart::Worker::new(sched.clone(), registry.clone(), config));
    let w = worker.clone();
    let _handle = tokio::spawn(async move { w.run().await });

    // Wait a moment for the worker to start polling
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Check initial status
    let initial_status = durable.status(&execution_id).await?;
    println!("Initial execution status: {:?}\n", initial_status.status);

    // Wait for completion
    println!("Waiting for execution to complete...\n");
    let record = durable
        .wait(&execution_id, Duration::from_secs(60), None)
        .await?;

    worker.stop();

    match record.status {
        scheduler::ExecutionStatus::Completed => {
            let output: FinderOutput = serde_json::from_value(record.result.unwrap())?;
            println!("Execution completed!");
            println!("  ZIP:         {}", output.zip_code);
            println!("  City:        {}", output.city);
            println!("  State:       {}", output.state);
            println!("  Breweries:   {}", output.breweries.len());

            if !output.breweries.is_empty() {
                println!();
                for (i, b) in output.breweries.iter().enumerate() {
                    println!(
                        "  {}. {} ({}) — {}, {}",
                        i + 1,
                        b.name,
                        b.brewery_type,
                        b.city,
                        b.state,
                    );
                }
            }
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
