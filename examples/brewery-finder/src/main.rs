//! Brewery Finder Example
//!
//! Demonstrates a sequential multi-step durable execution using macros:
//! 1. Look up city/state from a US ZIP code via Zippopotamus API
//! 2. Find breweries in that city via Open Brewery DB API
//! 3. Return a structured result with all brewery data
//!
//! Features: #[zart_durable], z_step!, z_step_with_retry!, external API calls,
//! structured output (no file I/O).

use chrono::Utc;
use scheduler::PostgresScheduler;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;
use zart::error::StepError;
use zart::prelude::*;
use zart::retry::RetryConfig;
use zart_macros::{z_step, z_step_with_retry, zart_durable};

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

// ── API response types ────────────────────────────────────────────────────────

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

// ── Durable handler (macro-generated TaskHandler) ─────────────────────────────

#[zart_durable("brewery-finder", timeout = "5m")]
async fn brewery_finder(
    ctx: &mut TaskContext<impl scheduler::Scheduler + scheduler::DurableStorage>,
    data: FinderInput,
) -> Result<FinderOutput, TaskError> {
    let client = reqwest::Client::new();

    // Step 1: Look up ZIP code via Zippopotamus API (with retries)
    let (city, state) = z_step_with_retry!(
        "lookup-zip",
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
                        step: "lookup-zip".to_string(),
                        reason: e.to_string(),
                    })?;
                let zip_resp: ZipResponse = resp
                    .json()
                    .await
                    .map_err(|e| StepError::Failed {
                        step: "lookup-zip".to_string(),
                        reason: format!("failed to parse response: {e}"),
                    })?;
                let place = zip_resp.places.first().ok_or_else(|| StepError::Failed {
                    step: "lookup-zip".to_string(),
                    reason: format!("no place found for ZIP {zip}"),
                })?;
                Ok((place.place_name.clone(), place.state.clone()))
            }
        }
    )
    .await?;

    // Step 2: Find breweries in the city via Open Brewery DB (with retries)
    let raw_breweries = z_step_with_retry!(
        "find-breweries",
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
                        step: "find-breweries".to_string(),
                        reason: e.to_string(),
                    })?
                    .json::<Vec<BreweryRaw>>()
                    .await
                    .map_err(|e| StepError::Failed {
                        step: "find-breweries".to_string(),
                        reason: format!("failed to parse response: {e}"),
                    })?;
                Ok(resp)
            }
        }
    )
    .await?;

    // Step 3: Transform raw data into structured output (no retries needed)
    let breweries: Vec<BreweryInfo> = z_step!("transform-results", || {
        let raw = raw_breweries.clone();
        let city = city.clone();
        let state = state.clone();
        async move {
            Ok(raw
                .into_iter()
                .map(|b| BreweryInfo {
                    name: b.name,
                    brewery_type: b.brewery_type.unwrap_or_else(|| "unknown".to_string()),
                    city: b.city.unwrap_or_else(|| city.clone()),
                    state: b.state.unwrap_or_else(|| state.clone()),
                })
                .collect())
        }
    })
    .await?;

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

    println!("=== Zart Brewery Finder Example ===\n");

    let db_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://zart:zart@localhost:5432/zart".to_string());

    // Connect and run migrations
    let pool = sqlx::PgPool::connect(&db_url).await?;
    let sched = Arc::new(PostgresScheduler::new(pool));
    sched.run_migrations().await?;

    // Register the macro-generated handler
    let mut registry = TaskRegistry::new();
    registry.register("brewery-finder", BreweryFinder);
    let registry = Arc::new(registry);

    // Start durable execution
    let execution_id = format!("brewery-finder-demo-{}", Uuid::new_v4());
    let durable = DurableScheduler::new(sched.clone(), registry.clone());

    let input = FinderInput {
        zip_code: "97209".to_string(), // Portland, OR — lots of breweries
    };

    println!(
        "Starting execution '{execution_id}' for ZIP {}...",
        input.zip_code
    );
    durable
        .start_typed(&execution_id, "brewery-finder", &input)
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
