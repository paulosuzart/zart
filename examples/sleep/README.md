# Sleep Example

Demonstrates **durable sleep** using `ctx.sleep()` — the execution pauses for a fixed duration and resumes automatically, surviving process restarts.

## Features Used

- **`ctx.sleep(duration)`** — suspends the durable execution for the given duration
- **Steps before and after sleep** — proves the body replays correctly across the sleep boundary

## Flow

1. **Log start time** — records when execution begins
2. **Sleep 5 seconds** — `ctx.sleep()` parks the execution; a step task is scheduled with `wake_time = now + 5s`
3. **Log resume time** — after the sleep fires, the worker replays the body and continues

## Running

```bash
# Ensure PostgreSQL is running
just up

# Run migrations
just migrate

# Build and run the example
just example-sleep
```

## What You'll See

```
=== Zart Sleep Example ===

Starting execution 'sleep-demo-3045af58-464f-481b-88b7-f43e4d7529cb'...


=== Execution Completed ===
  Task:       demo
  Started:    2026-04-08T20:31:32.875417+00:00
  Resumed:    2026-04-08T20:31:38.651535+00:00
```

## Key Concept: Durable Sleep

`ctx.sleep()` is not `tokio::time::sleep()`. It is a **durable pause** — the execution state is persisted in the database. If the worker process crashes during the sleep, another worker picks up the sleep step task when `wake_time` arrives and resumes the execution exactly where it left off.

Under the hood:
- `ctx.sleep()` inserts a step row with `step_kind = 'sleep'` and `execution_time = wake_time`
- The body task completes (the body walk encountered an unresolved node and parked)
- When `wake_time` arrives, the worker picks up the sleep step task, replays the body, and the walk continues past the sleep
