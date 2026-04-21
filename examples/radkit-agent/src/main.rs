//! Radkit Agent Example
//!
//! Demonstrates integrating radkit AI agents with durable execution:
//! 1. Use an LLM to extract location from natural language input
//! 2. Find breweries in that city via Open Brewery DB API
//! 3. Use an LLM to generate a friendly summary of results
//!
//! Features: manual DurableExecution trait, #[zart_step],
//! radkit LLM integration, dependency injection,
//! AI-powered extraction and summarization, structured output.

use async_trait::async_trait;
use chrono::Utc;
use radkit::agent::LlmFunction;
use radkit::macros::LLMOutput;
use radkit::models::providers::OpenAILlm;
use radkit::models::{BaseLlm, Thread};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;
use zart::PostgresStorage;
use zart::error::TaskError;
use zart::prelude::*;
use zart::registry::DurableExecution;
use zart::zart_step;

// ── Local serializable step error ─────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
pub enum StepError {
    #[error("Step '{step}' failed: {reason}")]
    Failed { step: String, reason: String },
}

// ── Input / Output types ──────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AgentInput {
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
    generated_at: String,
}

// ── Radkit LLM types ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, LLMOutput, schemars::JsonSchema)]
struct LocationExtraction {
    city: String,
    state: String,
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

// ── Step definitions ─────────────────────────────────────────────────────────

#[zart_step("extract-location", retry = "exponential(3, 2s)")]
async fn extract_location(
    llm: Arc<dyn BaseLlm>,
    query: &str,
) -> Result<ExtractedLocation, StepError> {
    println!(
        "[extract-location] Attempt {}",
        zart::context().current_attempt + 1
    );

    let prompt = format!(
        r#"Extract the city and state from this query. Return valid JSON.

Query: "{query}"

Respond with only a JSON object with "city" and "state" fields."#
    );

    let function = LlmFunction::<LocationExtraction>::new_with_system_instructions(
        llm,
        "You are a location extraction assistant. \
         Always return valid JSON with city and state fields.",
    );

    let result = function
        .run(Thread::from_user(&prompt))
        .await
        .map_err(|e| StepError::Failed {
            step: "extract-location".to_string(),
            reason: format!("LLM extraction failed: {e}"),
        })?;

    Ok(ExtractedLocation {
        city: result.city,
        state: result.state,
    })
}

#[zart_step("find-breweries", retry = "exponential(3, 1s)")]
async fn find_breweries(city: &str) -> Result<Vec<BreweryRaw>, StepError> {
    println!(
        "[find-breweries] Attempt {}",
        zart::context().current_attempt + 1
    );

    let client = reqwest::Client::new();
    let resp = client
        .get("https://api.openbrewerydb.org/v1/breweries")
        .query(&[("by_city", city)])
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

#[zart_step("transform-results")]
async fn transform_results(
    raw: Vec<BreweryRaw>,
    city: &str,
    state: &str,
) -> Result<Vec<BreweryInfo>, StepError> {
    let _ = zart::context().current_attempt;
    Ok(raw
        .into_iter()
        .map(|b| BreweryInfo {
            name: b.name,
            brewery_type: b.brewery_type.unwrap_or_else(|| "unknown".to_string()),
            city: b.city.unwrap_or_else(|| city.to_string()),
            state: b.state.unwrap_or_else(|| state.to_string()),
        })
        .collect())
}

#[zart_step("generate-summary", retry = "exponential(3, 2s)")]
async fn generate_summary(
    llm: Arc<dyn BaseLlm>,
    query: &str,
    location: &ExtractedLocation,
    breweries: &[BreweryInfo],
) -> Result<String, StepError> {
    println!(
        "[generate-summary] Attempt {}",
        zart::context().current_attempt + 1
    );

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

{}
{}

Make it friendly and helpful."#,
        breweries.len(),
        location.city,
        location.state,
        brewery_list,
        more_text,
    );

    let response = llm
        .generate_content(Thread::from_user(&prompt), None)
        .await
        .map_err(|e| StepError::Failed {
            step: "generate-summary".to_string(),
            reason: format!("LLM summary generation failed: {e}"),
        })?;

    Ok(response.into_content().joined_texts().unwrap_or_default())
}

// ── Task handler ──────────────────────────────────────────────────────────────

struct RadkitAgent {
    llm: Arc<dyn BaseLlm>,
}

#[async_trait]
impl DurableExecution for RadkitAgent {
    type Data = AgentInput;
    type Output = AgentOutput;

    async fn run(&self, data: Self::Data) -> Result<Self::Output, TaskError> {
        let location = extract_location(self.llm.clone(), &data.query).await?;

        println!(
            "  Extracted location: {}, {}",
            location.city, location.state
        );

        let raw_breweries: Vec<BreweryRaw> = find_breweries(&location.city).await?;

        println!("  Found {} raw brewery results", raw_breweries.len());

        let breweries: Vec<BreweryInfo> =
            transform_results(raw_breweries, &location.city, &location.state).await?;

        let summary =
            generate_summary(self.llm.clone(), &data.query, &location, &breweries).await?;

        Ok(AgentOutput {
            query: data.query,
            location,
            breweries,
            summary,
            generated_at: Utc::now().to_rfc3339(),
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

    println!("=== Zart Radkit Agent Example ===\n");

    let db_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://zart:zart@localhost:5432/zart".to_string());

    let pool = sqlx::PgPool::connect(&db_url).await?;
    let sched = Arc::new(PostgresStorage::new(pool));

    let api_key = std::env::var("OPENAI_API_KEY").expect("OPENAI_API_KEY must be set");
    let llm = Arc::new(OpenAILlm::new("gpt-4o", &api_key));

    let agent = RadkitAgent { llm };

    let mut registry = TaskRegistry::new();
    registry.register("radkit-agent", agent);
    let registry = Arc::new(registry);

    let execution_id = format!("radkit-demo-{}", Uuid::new_v4());
    let durable = DurableScheduler::new(sched.clone(), sched.task_scheduler());

    let input = AgentInput {
        query: "Find breweries in Portland, Oregon".to_string(),
    };

    println!("Starting execution '{}'...", execution_id);
    println!("  Query: {}\n", input.query);
    durable
        .start_for::<RadkitAgent>(&execution_id, "radkit-agent", &input)
        .await?;

    let config = zart::WorkerConfig {
        poll_interval: Duration::from_millis(200),
        max_tasks_per_poll: 10,
        max_concurrent_tasks: 4,
        shutdown_timeout: Duration::from_secs(5),
        orphan_timeout: Duration::from_secs(30),
        ..Default::default()
    };
    let worker = Arc::new(zart::Worker::new(
        sched.task_scheduler(),
        sched.clone(),
        registry.clone(),
        config,
    ));
    let w = worker.clone();
    let _handle = tokio::spawn(async move { w.run().await });

    tokio::time::sleep(Duration::from_millis(500)).await;

    let initial_status = durable.status(&execution_id).await?;
    println!("Initial execution status: {:?}\n", initial_status.status);

    println!("Waiting for execution to complete...\n");
    let output: AgentOutput = durable
        .wait_completion(&execution_id, Duration::from_secs(120), None)
        .await?;

    worker.stop();

    println!("Execution completed!");
    println!("  Query:       {}", output.query);
    println!(
        "  Location:    {}, {}",
        output.location.city, output.location.state
    );
    println!("  Breweries:   {}", output.breweries.len());
    println!("\n  Summary:");
    println!("    {}", output.summary);

    if !output.breweries.is_empty() {
        println!("\n  Breweries found:");
        for (i, b) in output.breweries.iter().enumerate() {
            println!(
                "    {}. {} ({}) — {}, {}",
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
