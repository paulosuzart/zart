//! Brewery Finder Example — Raw Trait Implementation (ZERO macros)
//!
//! This example demonstrates the raw `DurableExecution` and `ZartStep` traits
//! with NO macros: no `#[zart_durable]`, no `#[zart_step]`.
//! It shows what the macros generate under the hood.
//!
//! **What you'll see:**
//! - Manual `struct` definitions for each step
//! - Manual `impl ZartStep` with `step_name()`, `retry_config()`, and `run()`
//! - Manual `impl DurableExecution` with `run()` method
//! - Clean orchestration using `.execute(ctx)` on step structs
//!
//! **Compare with other examples:**
//! | Example | Macros Used | Step Style | Handler Style |
//! |---|---|---|---|
//! | `brewery-finder` | `#[zart_durable]` + `z_step!` | Inline closures | Macro-generated trait |
//! | `brewery-finder-step-fn` | `#[zart_durable]` + `#[zart_step]` | Step functions | Macro-generated trait |
//! | `brewery-finder-trait` | `#[zart_step]` only | Step functions | Manual trait impl |
//! | **`brewery-finder-raw`** | **NONE** | **Manual ZartStep structs** | **Manual trait impl** |
//!
//! Usage:
//!   just example-brewery-finder-raw

use async_trait::async_trait;
use chrono::Utc;
use scheduler::PostgresScheduler;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;
use zart::context::{StepContext, TaskContext};
use zart::error::{StepError, TaskError};
use zart::prelude::*;
use zart::registry::DurableExecution;
use zart::retry::RetryConfig;

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

// ── Step structs implementing ZartStep (what #[zart_step] generates) ──────────

/// Step 1: Look up city/state from a US ZIP code via Zippopotamus API.
struct LookupZipStep<'a> {
    client: &'a reqwest::Client,
    zip_code: &'a str,
}

#[async_trait]
impl ZartStep for LookupZipStep<'_> {
    type Output = (String, String);

    fn step_name(&self) -> &'static str {
        "lookup-zip"
    }

    fn retry_config(&self) -> Option<RetryConfig> {
        Some(RetryConfig::exponential(3, Duration::from_secs(1)))
    }

    async fn run(&self, ctx: StepContext) -> Result<Self::Output, StepError> {
        println!("[lookup-zip] Attempt {}", ctx.current_attempt() + 1);

        let resp = self
            .client
            .get(format!("https://api.zippopotam.us/us/{}", self.zip_code))
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
            reason: format!("no place found for ZIP {}", self.zip_code),
        })?;

        Ok((place.place_name.clone(), place.state.clone()))
    }
}

/// Step 2: Find breweries in a city via Open Brewery DB API.
struct FindBreweriesStep<'a> {
    client: &'a reqwest::Client,
    city: &'a str,
}

#[async_trait]
impl ZartStep for FindBreweriesStep<'_> {
    type Output = Vec<BreweryRaw>;

    fn step_name(&self) -> &'static str {
        "find-breweries"
    }

    fn retry_config(&self) -> Option<RetryConfig> {
        Some(RetryConfig::exponential(3, Duration::from_secs(1)))
    }

    async fn run(&self, ctx: StepContext) -> Result<Self::Output, StepError> {
        println!("[find-breweries] Attempt {}", ctx.current_attempt() + 1);

        let resp = self
            .client
            .get("https://api.openbrewerydb.org/v1/breweries")
            .query(&[("by_city", self.city)])
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
}

/// Step 3: Transform raw API data into structured output.
struct TransformResultsStep<'a> {
    raw: &'a [BreweryRaw],
    city: &'a str,
    state: &'a str,
}

#[async_trait]
impl ZartStep for TransformResultsStep<'_> {
    type Output = Vec<BreweryInfo>;

    fn step_name(&self) -> &'static str {
        "transform-results"
    }

    // No retry — pure computation step
    fn retry_config(&self) -> Option<RetryConfig> {
        None
    }

    async fn run(&self, _ctx: StepContext) -> Result<Self::Output, StepError> {
        Ok(self
            .raw
            .iter()
            .map(|b| BreweryInfo {
                name: b.name.clone(),
                brewery_type: b.brewery_type.clone().unwrap_or_else(|| "unknown".to_string()),
                city: b.city.clone().unwrap_or_else(|| self.city.to_string()),
                state: b.state.clone().unwrap_or_else(|| self.state.to_string()),
            })
            .collect())
    }
}

// ── RAW DurableExecution IMPLEMENTATION (no #[zart_durable] macro) ────────────

/// BreweryFinderRaw — implements DurableExecution manually.
///
/// This is what `#[zart_durable]` generates under the hood.
/// Note how clean the orchestration is when using `ZartStep` structs!
struct BreweryFinderRaw;

#[async_trait]
impl DurableExecution for BreweryFinderRaw {
    type Data = FinderInput;
    type Output = FinderOutput;

    /// Execute the task — pure orchestration flow using ZartStep structs.
    ///
    /// Compare this with:
    /// - **Closure style** (old): ~80 lines with manual `.clone()` and nested indentation
    /// - **This** (ZartStep): ~15 lines of clean orchestration
    async fn run(
        &self,
        ctx: &mut TaskContext,
        data: Self::Data,
    ) -> Result<Self::Output, TaskError> {
        let client = reqwest::Client::new();

        // Step 1: Look up ZIP code → (city, state)
        let (city, state) =
            LookupZipStep {
                client: &client,
                zip_code: &data.zip_code,
            }
            .execute(ctx)
            .await?;

        // Step 2: Find breweries in the city
        let raw_breweries: Vec<BreweryRaw> =
            FindBreweriesStep {
                client: &client,
                city: &city,
            }
            .execute(ctx)
            .await?;

        // Step 3: Transform into structured output
        let breweries =
            TransformResultsStep {
                raw: &raw_breweries,
                city: &city,
                state: &state,
            }
            .execute(ctx)
            .await?;

        Ok(FinderOutput {
            zip_code: data.zip_code,
            city,
            state,
            breweries,
            found_at: Utc::now().to_rfc3339(),
        })
    }

    /// Optional: set a timeout for the entire execution.
    fn timeout(&self) -> Option<Duration> {
        Some(Duration::from_secs(300)) // 5 minutes
    }

    /// Optional: set max retries for the entire execution.
    fn max_retries(&self) -> usize {
        2
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

    println!("=== Zart Brewery Finder Example (Raw Traits — NO macros) ===\n");

    let db_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://zart:zart@localhost:5432/zart".to_string());

    // Connect and run migrations
    let pool = sqlx::PgPool::connect(&db_url).await?;
    let sched = Arc::new(PostgresScheduler::new(pool));
    sched.run_migrations().await?;

    // Register the handler MANUALLY — no macro magic
    let mut registry = TaskRegistry::new();
    registry.register("brewery-finder-raw", BreweryFinderRaw);
    let registry = Arc::new(registry);

    // Start durable execution
    let execution_id = format!("brewery-finder-raw-{}", Uuid::new_v4());
    let durable = DurableScheduler::new(sched.clone());

    let input = FinderInput {
        zip_code: "97209".to_string(), // Portland, OR — lots of breweries
    };

    println!(
        "Starting execution '{execution_id}' for ZIP {}...",
        input.zip_code
    );
    durable
        .start_typed(&execution_id, "brewery-finder-raw", &input)
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
