# Parallel Steps Example

Demonstrates **parallel step execution** using `schedule_step` + `wait_all`, where multiple independent steps are scheduled concurrently and their results are aggregated.

## Features Used

- **`#[zart_step]` macro** — defines each parallel step as a standalone function
- **`ctx.schedule_step()`** — registers steps for parallel execution without waiting
- **`ctx.wait_all()`** — collects results from all scheduled steps
- **Sequential steps after parallel** — uses parallel results in a subsequent sequential step

## Flow

1. **Parallel data fetches** — schedules 3 independent simulated health checks
2. **Aggregate** — combines all results into a summary

## Running

```bash
# Ensure PostgreSQL is running
just up

# Run migrations
just migrate

# Build and run the example
just example-parallel-steps
```

## What You'll See

```
=== Zart Parallel Steps Example ===

Starting execution 'parallel-demo-...'...
Worker started. Steps executing...

Execution completed!
  Services checked: 3
  Total issues:     1

  Service: auth-api — status: healthy (42ms)
  Service: payments   — status: degraded (156ms)
    Issue: high latency detected
  Service: users-db   — status: healthy (28ms)
```

## Key Concept: `schedule_step` + `wait_all`

Instead of waiting for each step sequentially:

```rust
// Sequential (slow):
let a = ctx.execute_step(StepA).await?;
let b = ctx.execute_step(StepB).await?;
let c = ctx.execute_step(StepC).await?;
```

You can schedule them all at once and wait for all:

```rust
// Parallel (fast):
let h1 = ctx.schedule_step(StepA { param: "a" });
let h2 = ctx.schedule_step(StepB { param: "b" });
let h3 = ctx.schedule_step(StepC { param: "c" });
let results = ctx.wait_all(vec![h1, h2, h3]).await?;
```

Each scheduled step becomes its own task in the scheduler. The pattern cleanly separates independent work that _could_ be parallelized by the scheduler.
