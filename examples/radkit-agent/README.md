# Radkit Agent Example

Demonstrates **AI-powered durable execution** using [radkit](https://github.com/agents-sh/radkit) for natural language processing and summarization within a Zart workflow.

## Features Used

- **Manual `DurableExecution` trait** — struct with fields for dependency injection (LLM provider)
- **`#[zart_step]` macro** — turns async functions into step structs with `ZartStep` implementation
- **`ctx.execute_step()`** — executes steps with automatic retry/timeout handling
- **radkit LLM integration** — structured output extraction (`LlmFunction<T>`) and free-form summarization
- **Mixed workflow** — combines AI steps (LLM extraction, summarization) with traditional API calls

## Flow

1. **Extract location** — uses radkit's `LlmFunction<LocationExtraction>` to parse city/state from a natural language query
2. **Find breweries** — calls the [Open Brewery DB API](https://www.openbrewerydb.org/) to find breweries in the extracted city
3. **Transform results** — maps raw API data to structured output (no retries needed — deterministic)
4. **Generate summary** — uses `BaseLlm::generate_content` to create a friendly, conversational summary

## Prerequisites

Requires an OpenAI API key:

```bash
export OPENAI_API_KEY="your-key-here"
```

The example exits immediately with a clear error message if the key is not set.

You can also swap to any other radkit provider (Anthropic, Gemini, OpenRouter, etc.) by changing the `llm` initialization in `main()` — the `RadkitAgent` struct stores `Arc<dyn BaseLlm>`.

## Running

```bash
# Ensure PostgreSQL is running
just up

# Set your OpenAI API key
export OPENAI_API_KEY="your-key-here"

# Run the example
just example-radkit-agent
```

Or directly:

```bash
OPENAI_API_KEY=your-key DATABASE_URL=postgres://zart:zart@localhost:5432/zart \
  cargo run -p zart-examples --bin example-radkit-agent
```

## What You'll See

```
=== Zart Radkit Agent Example ===

Starting execution 'radkit-demo-...'...
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

### Structured output with `LlmFunction<T>`

`LocationExtraction` must derive both `LLMOutput` (from `radkit::macros`) and `schemars::JsonSchema`:

```rust
#[derive(Debug, Clone, Serialize, Deserialize, LLMOutput, schemars::JsonSchema)]
struct LocationExtraction {
    city: String,
    state: String,
}

// In the step:
let function = LlmFunction::<LocationExtraction>::new_with_system_instructions(
    llm,
    "You are a location extraction assistant.",
);
let result = function.run(Thread::from_user(&prompt)).await?;
```

### Free-form text generation

For summaries that return a plain `String`, use `generate_content` directly:

```rust
let response = llm.generate_content(Thread::from_user(&prompt), None).await?;
let text = response.into_content().joined_texts().unwrap_or_default();
```

### Dependency injection via struct fields

```rust
struct RadkitAgent {
    llm: Arc<dyn BaseLlm>,
}

// Registration:
let llm = Arc::new(OpenAILlm::new("gpt-4o", &api_key));
registry.register("radkit-agent", RadkitAgent { llm });
```

The `self.llm.clone()` in `run()` clones the `Arc` cheaply — a single provider instance is shared across all step calls.

## Changing the LLM Provider

```rust
// Anthropic
let llm = Arc::new(AnthropicLlm::from_env("claude-sonnet-4-5-20250929")?);
registry.register("radkit-agent", RadkitAgent { llm });

// OpenRouter
let llm = Arc::new(OpenRouterLlm::from_env("openai/gpt-4o")?);
registry.register("radkit-agent", RadkitAgent { llm });
```
