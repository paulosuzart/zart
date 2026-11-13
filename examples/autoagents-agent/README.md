# AutoAgents Agent Example

Demonstrates **AI-powered durable execution** using [AutoAgents](https://github.com/liquidos-ai/AutoAgents) for natural language processing and summarization within a Zart workflow.

## Features Used

- **Manual `DurableExecution` trait** — struct with fields for dependency injection (LLM provider)
- **`#[zart_step]` macro** — turns async functions into step structs with `ZartStep` implementation
- **AutoAgents LLM integration** — structured output extraction via `#[agent]` + `#[derive(AgentOutput)]` + `BasicAgent` and free-form summarization via `LLMProvider::chat`
- **Mixed workflow** — combines AI steps (LLM extraction, summarization) with traditional API calls

## Flow

1. **Extract location** — uses an AutoAgents `BasicAgent` with `LocationExtraction` (`#[derive(AgentOutput)]`) to parse city/state from a natural language query
2. **Find breweries** — calls the [Open Brewery DB API](https://www.openbrewerydb.org/) to find breweries in the extracted city
3. **Transform results** — maps raw API data to structured output (no retries needed — deterministic)
4. **Generate summary** — uses `LLMProvider::chat` to create a friendly, conversational summary

## Prerequisites

Requires a Google Gemini API key (`GEMINI_API_KEY` or `GOOGLE_API_KEY`):

```bash
export GEMINI_API_KEY="your-key-here"
```

Optionally override the model (defaults to `gemini-2.5-flash`):

```bash
export GEMINI_MODEL="gemini-2.5-flash"
```

The example exits immediately with a clear error message if the key is not set.

You can also swap to any other AutoAgents provider (OpenAI, Anthropic, Ollama, etc.) by changing the `LLMBuilder` call in `main()` — the `AutoAgentsAgent` struct stores `Arc<dyn LLMProvider>`.

## Running

```bash
# Ensure PostgreSQL is running
just up

# Set your Gemini API key
export GEMINI_API_KEY="your-key-here"

# Run the example
just example-autoagents-agent
```

Or directly:

```bash
GEMINI_API_KEY=your-key DATABASE_URL=postgres://zart:zart@localhost:5432/zart \
  cargo run -p zart-examples --bin example-autoagents-agent
```

## What You'll See

```
=== Zart AutoAgents Agent Example ===

Starting execution 'autoagents-demo-...'...
  Query: Find breweries in Portland, Oregon

Initial execution status: Pending

Waiting for execution to complete...

[extract-location] Attempt 1
  Extracted location: Portland, Oregon
[find-breweries] Attempt 1
  Found 20 raw brewery results
[generate-summary] Attempt 1

Execution completed!
  Query:       Find breweries in Portland, Oregon
  Location:    Portland, Oregon
  Breweries:   20

  Summary:
    Portland is a craft beer paradise with 20 fantastic breweries to explore! ...

  Breweries found:
    1. Breakside Brewery (micro) — Portland, Oregon
    2. Cascade Brewing (brewpub) — Portland, Oregon
    ...
```

## Why This Matters

**Durable AI workflows** are powerful because:

- **LLM calls can fail** — API timeouts, rate limits, transient errors. Durable execution retries safely without losing context.
- **Multi-step reasoning** — break complex AI tasks into auditable, resumable steps.
- **Mixed workloads** — seamlessly combine AI steps with traditional API calls and business logic.
- **State preservation** — if the process crashes after extracting the location but before fetching breweries, completed steps are not repeated.

## Key Code Patterns

### Structured output with `#[agent]` and `#[derive(AgentOutput)]`

`LocationExtraction` derives `AgentOutput` (with `#[output(description = "...")]` field hints), and `LocationExtractor` uses `#[agent(..., output = LocationExtraction)]`:

```rust
#[derive(Debug, Default, Clone, Serialize, Deserialize, AgentOutput)]
struct LocationExtraction {
    #[output(description = "City name extracted from the query")]
    city: String,
    #[output(description = "State or region name extracted from the query")]
    state: String,
}

#[agent(
    name = "location_extractor",
    description = "You are a location extraction assistant. Always return valid JSON with city and state fields.",
    output = LocationExtraction,
)]
#[derive(Default, Clone, AgentHooks)]
struct LocationExtractor {}

impl From<BasicAgentOutput> for LocationExtraction {
    fn from(output: BasicAgentOutput) -> Self {
        output.try_parse().unwrap_or_default()
    }
}

// In the step:
let agent_handle = AgentBuilder::<_, DirectAgent>::new(BasicAgent::new(LocationExtractor {}))
    .llm(llm)
    .build()
    .await?;
let result = agent_handle.agent.run(Task::new(&prompt)).await?;
```

### Free-form text generation

For summaries that return a plain `String`, call `LLMProvider::chat` directly:

```rust
let messages = vec![ChatMessage::user().content(&prompt).build()];
let response = llm.chat(&messages, None).await?;
let text = response.text().unwrap_or_default();
```

### Dependency injection via struct fields

```rust
struct AutoAgentsAgent {
    llm: Arc<dyn LLMProvider>,
}

// Registration:
let llm: Arc<dyn LLMProvider> = LLMBuilder::<Google>::new()
    .api_key(api_key)
    .model("gemini-2.5-flash")
    .build()?;
worker_builder.register_durable_task("autoagents-agent", AutoAgentsAgent { llm });
```

The `self.llm.clone()` in `run()` clones the `Arc` cheaply — a single provider instance is shared across all step calls.

## Changing the LLM Provider

```rust
use autoagents::llm::backends::openai::OpenAI;
use autoagents::llm::backends::anthropic::Anthropic;

// OpenAI
let llm: Arc<dyn LLMProvider> = LLMBuilder::<OpenAI>::new()
    .api_key(std::env::var("OPENAI_API_KEY")?)
    .model("gpt-4o")
    .build()?;

// Anthropic
let llm: Arc<dyn LLMProvider> = LLMBuilder::<Anthropic>::new()
    .api_key(std::env::var("ANTHROPIC_API_KEY")?)
    .model("claude-sonnet-4-5-20250929")
    .build()?;
```
