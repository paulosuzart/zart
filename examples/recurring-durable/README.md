# Recurring Durable Executions Example

Demonstrates `WorkerBuilder::register_recurring_durable` with all three overlap policies.

## Prerequisites

```bash
just up        # start Postgres via Docker Compose
```

## Run

```bash
DATABASE_URL=postgres://zart:zart@localhost:5432/zart \
  cargo run --bin example-recurring-durable
```

## Scenarios

### A — `SkipIfRunning` (default, recommended)

The handler sleeps 300 ms. The tick fires every 100 ms. The second tick is silently
skipped because the first execution is still running.

**Use when**: idempotent batch jobs where running the old instance to completion is
preferable to overlapping runs (e.g. nightly reports, ETL pipelines).

Cron equivalent:
```rust
Recurrence::Cron { expression: "0 2 * * *".into(), timezone: "UTC".into() }
```

### B — `CancelAndRestart`

The handler sleeps 300 ms. The tick fires every 150 ms. The stale run is cancelled
and a fresh execution starts on every tick.

**Use when**: you always want the latest config or state, and a partial previous run
is undesirable (e.g. configuration refresh, cache warming).

### C — `AlwaysStart`

Each occurrence starts its own durable execution regardless of how many others are
currently running. Multiple executions run in parallel.

**Use when**: occurrences are independent and must all complete (e.g. per-minute
audit windows, independent data ingestion slots).

## Overlap Policy Summary

| Policy           | Running execution | New occurrence |
|------------------|-------------------|----------------|
| `SkipIfRunning`  | Continues         | Skipped        |
| `CancelAndRestart` | Cancelled       | Started        |
| `AlwaysStart`    | Continues         | Also started   |
