# Radkit Agent Example

Demonstrates **AI-powered durable execution** using [radkit](https://github.com/agents-sh/radkit) for natural language processing and summarization within a Zart workflow.

## Features Used

- **Manual `DurableExecution` trait** — struct with fields for dependency injection (LLM provider)
- **`#[zart_step]` macro** — turns async functions into step structs with `ZartStep` implementation
- **`ctx.execute_step()`** — executes steps with automatic retry/timeout handling
- **radkit LLM integration** — structured output extraction and conversational summarization
- **Mixed workflow** — combines AI steps (LLM extraction, summarization) with traditional API calls

## Flow

1. **Extract location** — uses radkit's LLM with structured output to parse city/state from a natural language query
2. **Find breweries** — calls the [Open Brewery DB API](https://www.openbrewerydb.org/) to find breweries in the extracted city
3. **Transform results** — maps raw API data to structured output (no retries needed)
4. **Generate summary** — uses radkit's LLM to create a friendly, conversational summary

## Prerequisites

This example requires an OpenAI API key:

```bash
export OPENAI_API_KEY="your-key-here"
```

The example will exit immediately with a clear error message if the key is not set.

You can also modify the code to use other providers supported by radkit (Anthropic, Gemini, OpenRouter, etc.).

## Running

```bash
# Ensure PostgreSQL is running
just up

# Run migrations
just migrate

# Set your OpenAI API key
export OPENAI_API_KEY="your-key-here"

# Build and run the example
just example-radkit-agent
```

Or equivalently:

```bash
cargo run -p zart-examples --bin example-radkit-agent
```

The example will:
1. Fail fast if `OPENAI_API_KEY` is not set
2. Start a durable execution with a unique execution ID
3. Run a worker that processes the execution
4. Use the LLM to extract location from the query
5. Fetch brewery data from the API
6. Generate an AI summary of the results
7. Wait for completion and print the formatted output

## What You'll See

```
=== Zart Radkit Agent Example ===

This example combines durable execution with AI agents using radkit.
It extracts locations from natural language and generates summaries.

Input query: "Find me breweries in Portland, Oregon"

Starting execution 'radkit-agent-demo-...'...
Initial execution status: Pending

Waiting for execution to complete...

  Extracted location: Portland, Oregon
  Found 20 raw brewery results

═══════════════════════════════════════════
Execution completed!
═══════════════════════════════════════════

Query:     Find me breweries in Portland, Oregon
Location:  Portland, Oregon
Breweries: 20

── Top Results ──────────────────────────
  1. Breakside Brewery (micro) — Portland, Oregon
  2. Cascade Brewing (brewpub) — Portland, Oregon
  ...

── AI Summary ───────────────────────────
  I found 20 great breweries in Portland, Oregon for you! From the legendary
  Breakside Brewery to the sour beer specialists at Cascade Brewing, there's
  an incredible variety waiting to be explored. Enjoy your tasting adventure!

Completed at: 2026-04-05T...
```

## Why This Matters

**Durable AI workflows** are powerful because:

- **LLM calls can fail** — API timeouts, rate limits, transient errors. Durable execution retries safely without losing context.
- **Multi-step reasoning** — break complex AI tasks into auditable, resumable steps.
- **Mixed workloads** — seamlessly combine AI steps with traditional API calls, database queries, and business logic.
- **State preservation** — if the process crashes after extracting the location but before fetching breweries, completed work is not repeated.

## Step Functions with `#[zart_step]`

Each step is a standalone async function that captures its dependencies:

```rust
#[zart_step("extract-location", retry = "exponential(3, 2s)")]
async fn extract_location(
    llm: Arc<dyn BaseLlm>,
    query: String,
    ctx: StepContext,
) -> Result<ExtractedLocation, StepError> {
    let function = LlmFunction::<LocationExtraction>::new_with_system_instructions(
        llm.clone(),
        "You are a location extraction assistant...",
    );
    function.run(&prompt).await
}
```

The durable handler composes them cleanly:

```rust
async fn run(
    &self,
    ctx: &mut TaskContext,
    data: Self::Data,
) -> Result<Self::Output, TaskError> {
    // self.llm is available in every step
    let location = ctx.execute_step(extract_location(self.llm.clone(), data.query.clone())).await?;
    // ...
}
```

Registration at startup:

```rust
let llm = OpenAILlm::new("gpt-4o", &openai_key);
let mut registry = TaskRegistry::new();
registry.register("radkit-agent", RadkitAgent::new(llm));
```

## Changing the LLM Provider

Because the struct stores `Arc<dyn BaseLlm>`, swapping providers only requires changing the initialization — the struct definition stays the same:

```rust
// Anthropic
let llm = AnthropicLlm::from_env("claude-sonnet-4-5-20250929")?;
registry.register("radkit-agent", RadkitAgent::new(llm));

// Gemini
let llm = GeminiLlm::from_env("gemini-2.0-flash")?;
registry.register("radkit-agent", RadkitAgent::new(llm));

// OpenRouter
let llm = OpenRouterLlm::from_env("openai/gpt-4o")?;
registry.register("radkit-agent", RadkitAgent::new(llm));
```

Each provider works identically with `LlmFunction<T>` for structured outputs.
