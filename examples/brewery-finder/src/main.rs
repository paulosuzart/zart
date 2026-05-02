#![allow(deprecated)]
//! Brewery Finder Example
//!
//! Demonstrates a sequential multi-step durable execution using the new `#[zart_step]` macro:
//! 1. Look up city/state from a US ZIP code via Zippopotamus API
//! 2. Find breweries in that city via Open Brewery DB API
//! 3. Return a structured result with all brewery data
//!
//! Features: `#[zart_durable]`, `#[zart_step]`, external API calls,
//! structured output (no file I/O).

use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;
use zart::PgBackend;
use zart::prelude::*;

// ── Local serializable step error ─────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
pub enum StepError {
    #[error("Step '{step}' failed: {reason}")]
    Failed { step: String, reason: String },
}

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
#[zart::zart_step("lookup-zip", retry = "exponential(3, 1s)")]
async fn lookup_zip(
    client: &reqwest::Client,
    zip_code: &str,
) -> Result<(String, String), StepError> {
    println!(
        "[lookup-zip] Attempt {}",
        zart::context().current_attempt + 1
    );

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
#[zart::zart_step("find-breweries", retry = "exponential(3, 1s)")]
async fn find_breweries(
    client: &reqwest::Client,
    city: &str,
) -> Result<Vec<BreweryRaw>, StepError> {
    println!(
        "[find-breweries] Attempt {}",
        zart::context().current_attempt + 1
    );

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

/// Step 3: Transform raw data into structured output.
#[zart::zart_step("transform-results")]
async fn transform_results(
    raw: &[BreweryRaw],
    city: &str,
    state: &str,
) -> Result<Vec<BreweryInfo>, StepError> {
    let _ = zart::context().current_attempt;
    Ok(raw
        .iter()
        .map(|b| BreweryInfo {
            name: b.name.clone(),
            brewery_type: b
                .brewery_type
                .clone()
                .unwrap_or_else(|| "unknown".to_string()),
            city: b.city.clone().unwrap_or_else(|| city.to_string()),
            state: b.state.clone().unwrap_or_else(|| state.to_string()),
        })
        .collect())
}

// ── Durable handler ──────────────────────────────────────────────────────────

#[zart::zart_durable("brewery-finder", timeout = "5m")]
async fn brewery_finder(data: FinderInput) -> Result<FinderOutput, zart::error::TaskError> {
    let client = reqwest::Client::new();

    let (city, state) = lookup_zip(&client, &data.zip_code).await?;

    let raw_breweries = find_breweries(&client, &city).await?;

    let breweries = transform_results(&raw_breweries, &city, &state).await?;

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

    let pool = sqlx::PgPool::connect(&db_url).await?;
    let pg = PgBackend::new(pool);

    let execution_id = format!("brewery-finder-demo-{}", Uuid::new_v4());
    let durable = DurableScheduler::from_backend(&pg);

    let input = FinderInput {
        zip_code: "97209".to_string(),
    };

    println!(
        "Starting execution '{execution_id}' for ZIP {}...",
        input.zip_code
    );
    durable
        .start_for::<BreweryFinder>(&execution_id, "brewery-finder", &input)
        .await?;

    let config = zart::WorkerConfig {
        poll_interval: Duration::from_millis(200),
        max_tasks_per_poll: 10,
        max_concurrent_tasks: 4,
        shutdown_timeout: Duration::from_secs(5),
        orphan_timeout: Duration::from_secs(30),
        ..Default::default()
    };
    let worker = Arc::new(
        zart::WorkerBuilder::from_backend(&pg)
            .register_durable_task("brewery-finder", BreweryFinder)
            .config(config)
            .build(),
    );
    let w = worker.clone();
    let _handle = tokio::spawn(async move { w.run().await });

    tokio::time::sleep(Duration::from_millis(500)).await;

    let initial_status = durable.status(&execution_id).await?;
    println!("Initial execution status: {:?}\n", initial_status.status);

    println!("Waiting for execution to complete...\n");
    let output: FinderOutput = durable
        .wait_completion(&execution_id, Duration::from_secs(60), None)
        .await?;

    worker.stop();

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

    Ok(())
}
