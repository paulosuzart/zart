# Brewery Finder Example

Demonstrates a **sequential multi-step durable execution** that calls external APIs and returns structured results.

## Features Used

- **`#[zart_durable]` macro** — generates a `TaskHandler` impl from a plain async function
- **`z_step!` macro** — ergonomic step definition with automatic `ctx.step()` expansion
- **`z_step_with_retry!` macro** — step with retry configuration
- **Sequential steps** — each step returns a value used by the next step
- **Structured output** — results returned as the durable execution output (no file I/O)

## Flow

1. **Lookup ZIP code** — calls the [Zippopotamus API](https://www.zippopotam.us/) to get city/state from a US ZIP code
2. **Find breweries** — calls the [Open Brewery DB API](https://www.openbrewerydb.org/) to find breweries in that city
3. **Return result** — the execution completes with a structured output containing all brewery data

## Running

```bash
# Ensure PostgreSQL is running
just up

# Run migrations
just migrate

# Build and run the example
cargo run -p zart-examples --bin example-brewery-finder
```

The example will:
1. Start a durable execution with execution ID `brewery-finder-demo`
2. Run a worker that processes the execution
3. Wait for completion and print the result

## What You'll See

```
=== Zart Brewery Finder Example ===

Starting execution 'brewery-finder-demo' for ZIP 90210...
Worker started, waiting for execution to complete...
Execution completed!
  City:        Beverly Hills
  State:       CA
  Breweries:   3

  1. Local City Brewing
  2. Another Brewery
  ...
```

## Macro Style

This example uses the macro API that will be featured in the documentation site. Instead of manually implementing `TaskHandler`:

```rust
// Without macros (verbose):
struct MyTask;
#[async_trait]
impl TaskHandler for MyTask { ... }
```

You write a plain async function with an attribute:

```rust
#[zart_durable("brewery-finder", timeout = "5m")]
async fn brewery_finder(
    ctx: &mut TaskContext<impl Scheduler>,
    data: FinderInput,
) -> Result<FinderOutput, TaskError> {
    let city = z_step!("lookup-zip", || async { ... }).await?;
    let breweries = z_step_with_retry!("find-breweries", RetryConfig::exponential(3, Duration::from_secs(1)), || async { ... }).await?;
    Ok(FinderOutput { ... })
}
```

The macro generates:
- A unit struct `BreweryFinder` implementing `TaskHandler`
- Proper type extraction from the function signature
- The `run()` method wrapping your function body
