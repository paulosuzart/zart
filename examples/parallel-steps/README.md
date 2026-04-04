# Parallel Steps Example

Demonstrates **parallel step execution** using `schedule_step` + `wait_all`, where multiple independent API calls run concurrently and their results are aggregated.

## Features Used

- **`schedule_step`** — registers steps for parallel execution without waiting
- **`wait_all`** — collects results from all scheduled steps
- **Sequential steps after parallel** — uses parallel results in a subsequent sequential step
- **External API calls** — Zippopotamus API for multiple ZIP codes, Open Brewery DB for city lookups
- **Aggregated file output** — writes a consolidated report from all parallel results

## Flow

1. **Parallel ZIP lookups** — schedules 3 simultaneous calls to the Zippopotamus API for different ZIP codes
2. **Parallel brewery searches** — schedules 3 simultaneous calls to Open Brewery DB for each city found
3. **Aggregate report** — sequential step that combines all results into a single report file

## Running

```bash
# Ensure PostgreSQL is running
just up

# Run migrations
just migrate

# Build and run the example
cargo run -p zart-examples --bin example-parallel-steps
```

## What You'll See

```
=== Zart Parallel Steps Example ===

Starting execution 'parallel-demo-1'...
Worker started. Steps will execute in parallel...
Execution completed!
  ZIP codes processed: 3
  Total breweries found: 15
  Report written to: /tmp/zart-parallel-XXXXXX.txt
```

## Key Concept: `schedule_step` + `wait_all`

Instead of waiting for each step sequentially:

```rust
// Sequential (slow):
let a = ctx.step("step-a", || async { ... }).await?;
let b = ctx.step("step-b", || async { ... }).await?;
let c = ctx.step("step-c", || async { ... }).await?;
```

You can schedule them all at once and wait for all:

```rust
// Parallel (fast):
let h1 = ctx.schedule_step("step-a", || async { ... });
let h2 = ctx.schedule_step("step-b", || async { ... });
let h3 = ctx.schedule_step("step-c", || async { ... });
let results = ctx.wait_all(vec![h1, h2, h3]).await?;
```

Each scheduled step becomes its own task in the scheduler. They execute sequentially within the same worker dispatch (since `wait_all` runs them in order), but the pattern cleanly separates independent work that _could_ be parallelized by future scheduler improvements.
