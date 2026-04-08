# Durable Loops Example

Demonstrates **durable iteration over a collection** with guaranteed step-name uniqueness across loop iterations.

## Features Used

- **`#[zart_step]` with `{index}` template** — generates unique step names per iteration (`process-report-0`, `process-report-1`, ...)
- **`.with_id()` override** — supplies unique names for steps with static names when called in a loop
- **Step-scoped data fetching** — fetching the item list inside a step ensures the same list is used on replay, even if the underlying data changed
- **Sequential steps** — each step returns a value used by the next step

## Flow

1. **Fetch reports** — loads a batch of reports inside a step (stable across replays)
2. **Process each report** — iterates with `{index}`-templated step names for uniqueness
3. **Notify stakeholders** — conditionally sends alerts for flagged items using `.with_id()`

## Running

```bash
# Ensure PostgreSQL is running
just up

# Run migrations
just migrate

# Build and run the example
just example-durable-loops
```

## What You'll See

```
=== Zart Durable Loops Example ===

Starting execution 'report-batch-...'...
Processing reports:

  [fetch-reports] Loading reports for batch '2024-annual'
Fetched 4 reports

  [process-report-0] 'Q1 Sales': value=84.5, score=845, flagged=false
  [process-report-1] 'Q2 Sales': value=91.2, score=912, flagged=false
  [process-report-2] 'Q3 Sales': value=72.0, score=720, flagged=true
  [process-report-3] 'Q4 Sales': value=110.8, score=1108, flagged=false
  [notify] Sent alert for 'Q3 Sales' to team@example.com

=== Batch Complete ===
  Batch:   2024-annual
  Total:   4
  Flagged: 1
```

## Key Concept: Unique Step Names in Loops

In a durable execution, each step is identified by its name and stored as a database row. If you call the same step name inside a loop, every iteration would hit the same row — returning the first iteration's cached result for all subsequent iterations.

Two solutions:

**1. `{index}` template in the step name:**

```rust
#[zart_step("process-report-{index}")]
async fn process_report(index: usize, report: Report) -> Result<ProcessedReport, StepError> {
    // ...
}
```

The `{index}` placeholder is replaced at runtime, producing `"process-report-0"`, `"process-report-1"`, etc.

**2. `.with_id()` at the call site:**

```rust
for (i, report) in flagged.iter().enumerate() {
    ctx.execute_step(
        notify_stakeholder("team@example.com".into(), report.title.clone())
            .with_id(format!("notify-stakeholder-{i}"))
    ).await?;
}
```

Overrides the step name per call — useful when the step struct has a static name but is called in a loop.
