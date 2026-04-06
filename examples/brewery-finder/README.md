# Brewery Finder Example

Demonstrates a **sequential multi-step durable execution** that calls external APIs and returns structured results.

## Features Used

- **`#[zart_durable]` macro** — generates a `DurableExecution` impl from a plain async function
- **`#[zart_step]` macro** — turns async functions into step structs with automatic `ZartStep` implementation
- **`ctx.execute_step()`** — executes steps with automatic retry/timeout handling
- **Sequential steps** — each step returns a value used by the next step
- **Structured output** — results returned as the durable execution output (no file I/O)

## Flow

1. **Lookup ZIP code** — calls the [Zippopotamus API](https://www.zippopotam.us/) to get city/state from a US ZIP code
2. **Find breweries** — calls the [Open Brewery DB API](https://www.openbrewerydb.org/) to find breweries in that city
3. **Transform results** — converts raw API data into a structured output
4. **Return result** — the execution completes with all brewery data

## Running

```bash
# Ensure PostgreSQL is running
just up

# Run migrations
just migrate

# Build and run the example
just example-brewery-finder
```

The example will:
1. Start a durable execution with a unique execution ID
2. Run a worker that processes the execution
3. Wait for completion and print the result

## What You'll See

```
=== Zart Brewery Finder Example ===

Starting execution 'brewery-finder-demo-...' for ZIP 97209...
Initial execution status: Pending

Waiting for execution to complete...

Execution completed!
  ZIP:         97209
  City:        Portland
  State:       Oregon
  Breweries:   20

  1. Breakside Brewery (micro) — Portland, Oregon
  2. Cascade Brewing (brewpub) — Portland, Oregon
  ...
```

## Step Definitions

Steps are defined as standalone async functions using `#[zart_step]`:

```rust
#[zart_step("lookup-zip", retry = "exponential(3, 1s)")]
async fn lookup_zip(
    client: &reqwest::Client,
    zip_code: &str,
    ctx: StepContext,
) -> Result<(String, String), StepError> {
    // ... step logic
}
```

The durable handler composes them cleanly:

```rust
#[zart_durable("brewery-finder", timeout = "5m")]
async fn brewery_finder(
    ctx: &mut TaskContext,
    data: FinderInput,
) -> Result<FinderOutput, TaskError> {
    let client = reqwest::Client::new();

    let (city, state) = ctx.execute_step(lookup_zip(&client, &data.zip_code)).await?;
    let raw_breweries = ctx.execute_step(find_breweries(&client, &city)).await?;
    let breweries = ctx.execute_step(transform_results(&raw_breweries, &city, &state)).await?;

    Ok(FinderOutput { zip_code: data.zip_code, city, state, breweries, found_at: Utc::now().to_rfc3339() })
}
```

The macro generates:
- A step struct capturing all parameters (except `ctx`)
- `impl ZartStep` with `step_name()`, `retry_config()`, and `run()`
- The original function is rewritten to return the step struct (builder pattern)
