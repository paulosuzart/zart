//! Durable Multi-Tool ReAct Agent Example (AutoAgents + Zart)
//!
//! Demonstrates intra-loop durability for an AutoAgents `ReActAgent` using
//! [`DurableReactTask`]:
//!   1. `list_recent_incidents` -> discovers incident IDs and affected customer
//!   2. `get_incident_logs`     -> called once per incident ID
//!   3. `lookup_customer_sla`   -> checks customer tier & SLA threshold
//!   4. `emit_client_action`    -> prepares a client-side notification payload
//!
//! Every LLM turn (`llm-call-0`, `llm-call-1`, ...) and every tool invocation
//! (`tool-<name>-0`, `tool-<name>-1`, ...) is persisted as a Zart step in PostgreSQL.

mod gemini;

use async_trait::async_trait;
use autoagents::core::tool::ToolCallError;
use autoagents::llm::LLMProvider;
use autoagents::llm::backends::openai::{OpenAI, OpenAIApiMode};
use autoagents::llm::builder::LLMBuilder;
use autoagents::prelude::{AgentHooks, ToolInput, ToolRuntime, ToolT, agent, tool};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;
use zart::PgBackend;
use zart::prelude::*;
use zart_autoagents::DurableReactTask;

// ── Tools ─────────────────────────────────────────────────────────────────────

#[derive(Serialize, Deserialize, ToolInput)]
struct ListIncidentsArgs {
    #[input(description = "Service name to list recent incidents for")]
    service: String,
}

#[tool(
    name = "list_recent_incidents",
    description = "List recent incident IDs and affected customer_id for a backend service.",
    input = ListIncidentsArgs,
)]
#[derive(Default)]
struct ListIncidentsTool {}

#[async_trait]
impl ToolRuntime for ListIncidentsTool {
    async fn execute(&self, args: Value) -> Result<Value, ToolCallError> {
        let parsed: ListIncidentsArgs = serde_json::from_value(args)?;
        println!(
            "  [tool:list_recent_incidents] service='{}'",
            parsed.service
        );
        Ok(serde_json::json!({
            "service": parsed.service,
            "affected_customer_id": "cust-42",
            "incidents": [
                { "id": "INC-101", "severity": "high" },
                { "id": "INC-102", "severity": "medium" }
            ]
        }))
    }
}

#[derive(Serialize, Deserialize, ToolInput)]
struct IncidentLogsArgs {
    #[input(description = "Incident ID (e.g. INC-101) to inspect logs for")]
    incident_id: String,
}

#[tool(
    name = "get_incident_logs",
    description = "Fetch root-cause log summary and latency spike for a specific incident ID.",
    input = IncidentLogsArgs,
)]
#[derive(Default)]
struct GetIncidentLogsTool {}

#[async_trait]
impl ToolRuntime for GetIncidentLogsTool {
    async fn execute(&self, args: Value) -> Result<Value, ToolCallError> {
        let parsed: IncidentLogsArgs = serde_json::from_value(args)?;
        println!(
            "  [tool:get_incident_logs] incident_id='{}'",
            parsed.incident_id
        );
        let details = match parsed.incident_id.as_str() {
            "INC-101" => serde_json::json!({
                "incident_id": "INC-101",
                "root_cause": "Postgres connection pool exhaustion",
                "p95_latency_ms": 480
            }),
            "INC-102" => serde_json::json!({
                "incident_id": "INC-102",
                "root_cause": "Upstream payment gateway timeout",
                "p95_latency_ms": 230
            }),
            _ => serde_json::json!({
                "incident_id": parsed.incident_id,
                "root_cause": "Unknown",
                "p95_latency_ms": 0
            }),
        };
        Ok(details)
    }
}

#[derive(Serialize, Deserialize, ToolInput)]
struct CustomerSlaArgs {
    #[input(description = "Customer ID to look up SLA threshold for")]
    customer_id: String,
}

#[tool(
    name = "lookup_customer_sla",
    description = "Look up customer tier and maximum allowed p95 latency SLA in milliseconds.",
    input = CustomerSlaArgs,
)]
#[derive(Default)]
struct LookupCustomerSlaTool {}

#[async_trait]
impl ToolRuntime for LookupCustomerSlaTool {
    async fn execute(&self, args: Value) -> Result<Value, ToolCallError> {
        let parsed: CustomerSlaArgs = serde_json::from_value(args)?;
        println!(
            "  [tool:lookup_customer_sla] customer_id='{}'",
            parsed.customer_id
        );
        Ok(serde_json::json!({
            "customer_id": parsed.customer_id,
            "tier": "enterprise",
            "max_p95_latency_ms": 150
        }))
    }
}

#[derive(Serialize, Deserialize, ToolInput)]
struct ClientActionArgs {
    #[input(description = "Customer ID to notify")]
    customer_id: String,
    #[input(description = "Whether the SLA was breached")]
    sla_breached: bool,
    #[input(description = "Short incident summary for the client UI")]
    summary: String,
}

#[tool(
    name = "emit_client_action",
    description = "Prepare a client-side notification action once all incidents and SLA status have been gathered.",
    input = ClientActionArgs,
)]
#[derive(Default)]
struct EmitClientActionTool {}

#[async_trait]
impl ToolRuntime for EmitClientActionTool {
    async fn execute(&self, args: Value) -> Result<Value, ToolCallError> {
        let parsed: ClientActionArgs = serde_json::from_value(args)?;
        println!(
            "  [tool:emit_client_action] customer_id='{}', sla_breached={}",
            parsed.customer_id, parsed.sla_breached
        );
        Ok(serde_json::json!({
            "action": "open_incident_banner",
            "customer_id": parsed.customer_id,
            "sla_breached": parsed.sla_breached,
            "summary": parsed.summary
        }))
    }
}

// ── AutoAgents ReAct Agent ────────────────────────────────────────────────────

#[agent(
    name = "durable_incident_agent",
    description = "You are an SRE incident investigator. Follow this exact process using tools: 1) list_recent_incidents for the service, 2) call get_incident_logs for EVERY incident returned, 3) call lookup_customer_sla for the affected customer, 4) call emit_client_action with your findings, then return a concise final report.",
    tools = [ListIncidentsTool, GetIncidentLogsTool, LookupCustomerSlaTool, EmitClientActionTool],
)]
#[derive(Default, Clone, AgentHooks)]
struct IncidentAgent {}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    println!("=== Zart Durable AutoAgents (Multi-Tool ReAct) Example ===\n");

    let db_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://zart:zart@localhost:5432/zart".to_string());

    let pool = sqlx::PgPool::connect(&db_url).await?;
    let pg = PgBackend::new(pool);

    let llm: Arc<dyn LLMProvider> = if let Ok(key) = std::env::var("OPENAI_API_KEY") {
        let base_url = std::env::var("OPENAI_BASE_URL")
            .unwrap_or_else(|_| "https://api.openai.com/v1".to_string());
        let model = std::env::var("OPENAI_MODEL").unwrap_or_else(|_| "gpt-4o-mini".to_string());
        LLMBuilder::<OpenAI>::new()
            .api_key(key)
            .base_url(base_url)
            .api_mode(OpenAIApiMode::ChatCompletions)
            .model(model)
            .build()?
    } else {
        let key = std::env::var("GEMINI_API_KEY")
            .or_else(|_| std::env::var("GOOGLE_API_KEY"))
            .expect("GEMINI_API_KEY, GOOGLE_API_KEY, or OPENAI_API_KEY must be set");
        let model =
            std::env::var("GEMINI_MODEL").unwrap_or_else(|_| "gemini-3.8-flash".to_string());
        Arc::new(gemini::Gemini3Provider::new(key, model))
    };

    let execution_id = format!("autoagents-durable-{}", Uuid::new_v4());
    let durable = DurableScheduler::from_backend(&pg);

    let prompt = "Investigate recent incidents on 'checkout-api'. Inspect logs for every incident, check the affected customer's SLA, emit the client action, and summarize the report.".to_string();

    println!("Starting execution '{}'...", execution_id);
    println!("  Prompt: {}\n", prompt);
    durable
        .start_for::<DurableReactTask<IncidentAgent>>(
            &execution_id,
            "durable-incident-agent",
            &prompt,
        )
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
            .register_durable_task(
                "durable-incident-agent",
                DurableReactTask::new(IncidentAgent {}, llm),
            )
            .config(config)
            .build(),
    );
    let w = worker.clone();
    let _handle = tokio::spawn(async move { w.run().await });

    let answer: String = durable
        .wait_completion(&execution_id, Duration::from_secs(120), None)
        .await?;

    worker.stop();

    println!("\nExecution completed!");
    if answer.trim().is_empty() {
        println!("\n  Final Report: <empty>");
    } else {
        println!("\n  Final Report:\n{}", answer);
    }

    Ok(())
}
