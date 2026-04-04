//! Parallel Steps Example
//!
//! Demonstrates parallel step execution using schedule_step + wait_all:
//! 1. Look up 3 ZIP codes in parallel via Zippopotamus API
//! 2. Search breweries for each city in parallel via Open Brewery DB
//! 3. Aggregate all results into a consolidated report file
//!
//! Features: schedule_step, wait_all, external APIs, aggregated file output.

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
struct ParallelInput {
    zip_codes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CityBreweries {
    city: String,
    state: String,
    breweries: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ParallelOutput {
    cities_processed: usize,
    total_breweries: usize,
    city_details: Vec<CityBreweries>,
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
}

// ── Task handler ──────────────────────────────────────────────────────────────

struct ParallelTask;

#[async_trait]
impl TaskHandler for ParallelTask {
    type Data = ParallelInput;
    type Output = ParallelOutput;

    async fn run<S: scheduler::Scheduler>(
        &self,
        ctx: &mut TaskContext<S>,
        data: Self::Data,
    ) -> Result<Self::Output, TaskError> {
        let client = reqwest::Client::new();

        // ── Phase 1: Look up all ZIP codes in parallel ────────────────────────
        // Pre-build the list of (index, zip) pairs so we don't borrow from `data`.
        let zip_pairs: Vec<(usize, String)> = data
            .zip_codes
            .iter()
            .enumerate()
            .map(|(i, z)| (i, z.clone()))
            .collect();

        let mut zip_handles = vec![];
        for (i, zip) in zip_pairs {
            let handle = ctx.schedule_step(&format!("lookup-zip-{i}"), {
                let client = client.clone();
                move || {
                    let client = client.clone();
                    let zip = zip.clone();
                    async move {
                        let resp = client
                            .get(format!("https://api.zippopotam.us/us/{zip}"))
                            .send()
                            .await
                            .map_err(|e| StepError::Failed {
                                step: format!("lookup-zip-{zip}"),
                                reason: e.to_string(),
                            })?
                            .json::<ZipInfo>()
                            .await
                            .map_err(|e| StepError::Failed {
                                step: format!("lookup-zip-{zip}"),
                                reason: format!("parse error: {e}"),
                            })?;
                        Ok((zip, resp.place_name, resp.state))
                    }
                }
            });
            zip_handles.push(handle);
        }

        let zip_results = ctx.wait_all(zip_handles).await?;
        let mut cities = vec![];
        for result in zip_results {
            let (zip, city, state) = result.map_err(|e| TaskError::StepFailed {
                step: "parallel-zip-lookup".to_string(),
                source: e,
            })?;
            println!("  ZIP {zip} → {city}, {state}");
            cities.push((zip, city, state));
        }

        // ── Phase 2: Fetch breweries for each city in parallel ────────────────
        // Pre-build the list of (index, city) pairs.
        let city_pairs: Vec<(usize, String, String)> = cities
            .iter()
            .enumerate()
            .map(|(i, (_zip, city, state))| (i, city.clone(), state.clone()))
            .collect();

        let mut brewery_handles = vec![];
        let cities_for_closure = cities.clone();
        for (i, _city, _state) in city_pairs {
            let handle = ctx.schedule_step(&format!("fetch-breweries-{i}"), {
                let client = client.clone();
                let cities = cities_for_closure.clone();
                move || {
                    let client = client.clone();
                    let city = cities[i].1.clone();
                    async move {
                        let resp = client
                            .get("https://api.openbrewerydb.org/v1/breweries")
                            .query(&[("by_city", &city)])
                            .send()
                            .await
                            .map_err(|e| StepError::Failed {
                                step: format!("fetch-breweries-{city}"),
                                reason: e.to_string(),
                            })?
                            .json::<Vec<Brewery>>()
                            .await
                            .map_err(|e| StepError::Failed {
                                step: format!("fetch-breweries-{city}"),
                                reason: format!("parse error: {e}"),
                            })?;
                        let brewery_names: Vec<String> =
                            resp.into_iter().map(|b| b.name).collect();
                        Ok(brewery_names)
                    }
                }
            });
            brewery_handles.push(handle);
        }

        let brewery_results = ctx.wait_all(brewery_handles).await?;
        let mut city_details = vec![];
        for (j, result) in brewery_results.into_iter().enumerate() {
            let breweries = result.map_err(|e| TaskError::StepFailed {
                step: "parallel-brewery-fetch".to_string(),
                source: e,
            })?;
            let (_zip, city, state) = &cities[j];
            println!("  {city}, {state}: {} breweries", breweries.len());
            city_details.push(CityBreweries {
                city: city.clone(),
                state: state.clone(),
                breweries,
            });
        }

        let total_breweries: usize = city_details.iter().map(|cd| cd.breweries.len()).sum();

        Ok(ParallelOutput {
            cities_processed: city_details.len(),
            total_breweries,
            city_details,
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

    println!("=== Zart Parallel Steps Example ===\n");

    let db_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://zart:zart@localhost:5432/zart".to_string());

    let pool = sqlx::PgPool::connect(&db_url).await?;
    let sched = Arc::new(PostgresScheduler::new(pool));
    sched.run_migrations().await?;

    let mut registry = TaskRegistry::new();
    registry.register("parallel-task", ParallelTask);
    let registry = Arc::new(registry);

    let execution_id = "parallel-demo-1";
    let durable = DurableScheduler::new(sched.clone(), registry.clone());

    let input = ParallelInput {
        zip_codes: vec![
            "90210".to_string(), // Beverly Hills
            "10001".to_string(), // New York
            "60601".to_string(), // Chicago
        ],
    };

    println!("Starting execution '{execution_id}'...");
    println!("  ZIP codes: {:?}", input.zip_codes);
    durable.start_typed(execution_id, "parallel-task", &input).await?;

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

    println!("\nWorker started. Steps executing...\n");
    let record = durable.wait(execution_id, Duration::from_secs(60), None).await?;

    worker.stop();

    match record.status {
        scheduler::ExecutionStatus::Completed => {
            let output: ParallelOutput = serde_json::from_value(record.result.unwrap())?;
            println!("\nExecution completed!");
            println!("  Cities processed: {}", output.cities_processed);
            println!("  Total breweries:  {}", output.total_breweries);
            for cd in &output.city_details {
                println!(
                    "  {} ({}) — {} breweries",
                    cd.city, cd.state, cd.breweries.len(),
                );
                for (i, name) in cd.breweries.iter().take(5).enumerate() {
                    println!("    {}. {}", i + 1, name);
                }
                if cd.breweries.len() > 5 {
                    println!("    ... and {} more", cd.breweries.len() - 5);
                }
            }
        }
        _ => {
            eprintln!("Execution ended with status: {:?}", record.status);
        }
    }

    Ok(())
}
