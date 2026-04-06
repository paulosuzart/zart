//! Radkit Agent Example
//!
//! Demonstrates integrating radkit AI agents with durable execution:
//! 1. Use an LLM to extract location from natural language input
//! 2. Find breweries in that city via Open Brewery DB API
//! 3. Use an LLM to generate a friendly summary of results
//!
//! Features: manual TaskHandler trait with struct fields, z_step!,
//! z_step_with_retry!, radkit LLM integration, dependency injection,
//! AI-powered extraction and summarization, structured output.

use chrono::Utc;
use radkit::agent::LlmFunction;
use radkit::models::BaseLlm;
use radkit::models::providers::OpenAILlm;
use scheduler::PostgresScheduler;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;
use zart::context::TaskContext;
use zart::error::{StepError, TaskError};
use zart::prelude::*;
use zart::registry::TaskHandler;
use zart::retry::RetryConfig;

// ── Input / Output types ──────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AgentInput {
    /// Natural language query, e.g. "Find breweries in Portland, Oregon"
    query: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ExtractedLocation {
    city: String,
    state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BreweryInfo {
    name: String,
    brewery_type: String,
    city: String,
    state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AgentOutput {
    query: String,
    location: ExtractedLocation,
    breweries: Vec<BreweryInfo>,
    summary: String,
    completed_at: String,
}

// ── API response types ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BreweryRaw {
    name: String,
    #[serde(default)]
    brewery_type: Option<String>,
    city: Option<String>,
    #[serde(default)]
    state: Option<String>,
}

// ── LLM extraction types ─────────────────────────────────────────────────────

/// Schema for the LLM to extract location from natural language
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema, radkit::macros::LLMOutput)]
struct LocationExtraction {
    /// The city name extracted from the query
    city: String,
    /// The state abbreviation or full name
    state: String,
}

/// Schema for the LLM to generate a summary
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema, radkit::macros::LLMOutput)]
struct BrewerySummary {
    /// A friendly, conversational summary of the results
    text: String,
}

// ── Task handler struct (with injected LLM dependency) ────────────────────────

struct RadkitAgent {
    llm: Arc<dyn BaseLlm>,
}

impl RadkitAgent {
    fn new(llm: OpenAILlm) -> Self {
        Self { llm: Arc::new(llm) }
    }
}

#[async_trait::async_trait]
impl TaskHandler for RadkitAgent {
    type Data = AgentInput;
    type Output = AgentOutput;

    async fn run(
        &self,
        ctx: &mut TaskContext,
        data: Self::Data,
    ) -> Result<Self::Output, TaskError> {
        // Step 1: Use radkit LLM to extract location from natural language query
        let location: ExtractedLocation = ctx
            .step_with_retry(
                "extract-location",
                RetryConfig::exponential(3, Duration::from_secs(2)),
                || {
                    let llm = self.llm.clone();
                    let query = data.query.clone();
                    async move {
                        // Build extraction prompt
                        let prompt = format!(
                            r#"Extract the city and state from this query. Return valid JSON.

Query: "{query}"

Respond with only a JSON object with "city" and "state" fields."#
                        );

                        // Use radkit's structured output via LlmFunction
                        let function =
                            LlmFunction::<LocationExtraction>::new_with_system_instructions(
                                llm,
                                "You are a location extraction assistant. \
                                 Always return valid JSON with city and state fields.",
                            );

                        let result =
                            function.run(&prompt).await.map_err(|e| StepError::Failed {
                                step: "extract-location".to_string(),
                                reason: format!("LLM extraction failed: {e}"),
                            })?;

                        Ok(ExtractedLocation {
                            city: result.city,
                            state: result.state,
                        })
                    }
                },
            )
            .await?;

        println!(
            "  Extracted location: {}, {}",
            location.city, location.state
        );

        // Step 2: Find breweries in the extracted city via Open Brewery DB (with retries)
        let raw_breweries = ctx
            .step_with_retry(
                "find-breweries",
                RetryConfig::exponential(3, Duration::from_secs(1)),
                || {
                    let city = location.city.clone();
                    async move {
                        let client = reqwest::Client::new();
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
                },
            )
            .await?;

        println!("  Found {} raw brewery results", raw_breweries.len());

        // Step 3: Transform raw data into structured output
        let breweries: Vec<BreweryInfo> = ctx
            .step("transform-results", || {
                let raw = raw_breweries.clone();
                let city = location.city.clone();
                let state = location.state.clone();
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

        // Step 4: Use radkit LLM to generate a friendly summary
        let summary = ctx
            .step_with_retry(
                "generate-summary",
                RetryConfig::exponential(3, Duration::from_secs(2)),
                || {
                    let llm = self.llm.clone();
                    let query = data.query.clone();
                    let location = location.clone();
                    let breweries = breweries.clone();
                    async move {
                        let brewery_list = breweries
                            .iter()
                            .take(5)
                            .map(|b| format!("- {} ({})", b.name, b.brewery_type))
                            .collect::<Vec<_>>()
                            .join("\n");

                        let more_text = if breweries.len() > 5 {
                            format!("\n...and {} more", breweries.len() - 5)
                        } else {
                            String::new()
                        };

                        let prompt = format!(
                            r#"You're a friendly beer enthusiast. Write a short, conversational summary (2-3 sentences) about these brewery results.

User asked: "{query}"
Found {} breweries in {}, {}.

Top results:
{brewery_list}{more_text}

Keep it casual and enthusiastic."#,
                            breweries.len(),
                            location.city,
                            location.state
                        );

                        let function =
                            LlmFunction::<BrewerySummary>::new_with_system_instructions(
                                llm,
                                "You are a friendly beer enthusiast writing casual summaries.",
                            );

                        let result =
                            function.run(&prompt).await.map_err(|e| StepError::Failed {
                                step: "generate-summary".to_string(),
                                reason: format!("LLM summary generation failed: {e}"),
                            })?;

                        Ok(result.text)
                    }
                },
            )
            .await?;

        Ok(AgentOutput {
            query: data.query,
            location,
            breweries,
            summary,
            completed_at: Utc::now().to_rfc3339(),
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

    // Fail fast if OpenAI API key is not set
    let openai_key = std::env::var("OPENAI_API_KEY").expect(
        "OPENAI_API_KEY must be set. Export your OpenAI API key before running this example.",
    );

    println!("=== Zart Radkit Agent Example ===\n");
    println!("This example combines durable execution with AI agents using radkit.");
    println!("It extracts locations from natural language and generates summaries.\n");

    let db_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://zart:zart@localhost:5432/zart".to_string());

    // Connect and run migrations
    let pool = sqlx::PgPool::connect(&db_url).await?;
    let sched = Arc::new(PostgresScheduler::new(pool));
    sched.run_migrations().await?;

    // Initialize the LLM provider
    let llm = OpenAILlm::new("gpt-4o", &openai_key);

    // Register the handler with injected dependency
    let mut registry = TaskRegistry::new();
    registry.register("radkit-agent", RadkitAgent::new(llm));
    let registry = Arc::new(registry);

    // Start durable execution
    let execution_id = format!("radkit-agent-demo-{}", Uuid::new_v4());
    let durable = DurableScheduler::new(sched.clone(), registry.clone());

    let input = AgentInput {
        query: "Find me breweries in Portland, Oregon".to_string(),
    };

    println!("Input query: \"{}\"\n", input.query);
    println!("Starting execution '{execution_id}'...");
    durable
        .start_typed(&execution_id, "radkit-agent", &input)
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
        .wait(&execution_id, Duration::from_secs(180), None)
        .await?;

    worker.stop();

    match record.status {
        scheduler::ExecutionStatus::Completed => {
            let output: AgentOutput = serde_json::from_value(record.result.unwrap())?;
            println!("═══════════════════════════════════════════");
            println!("Execution completed!");
            println!("═══════════════════════════════════════════");
            println!();
            println!("Query:     {}", output.query);
            println!(
                "Location:  {}, {}",
                output.location.city, output.location.state
            );
            println!("Breweries: {}", output.breweries.len());
            println!();

            if !output.breweries.is_empty() {
                println!("── Top Results ──────────────────────────");
                for (i, b) in output.breweries.iter().take(10).enumerate() {
                    println!(
                        "  {}. {} ({}) — {}, {}",
                        i + 1,
                        b.name,
                        b.brewery_type,
                        b.city,
                        b.state,
                    );
                }
                if output.breweries.len() > 10 {
                    println!("  ... and {} more", output.breweries.len() - 10);
                }
                println!();
            }

            println!("── AI Summary ───────────────────────────");
            println!("  {}", output.summary);
            println!();
            println!("Completed at: {}", output.completed_at);
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
