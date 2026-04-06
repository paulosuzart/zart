# Retry Simulation Example

This example demonstrates how to simulate and observe retry behavior in Zart durable executions.

## What It Demonstrates

- **Intentional Failure Pattern**: The example fails on the first attempt and succeeds on the retry
- **StepContext Metadata**: Using read-only accessors to query retry state:
  - `ctx.current_attempt()` — which attempt is running (0-indexed)
  - `ctx.max_retries()` — maximum retries configured
  - `ctx.is_retry_attempt()` — whether this is a retry (not the first attempt)
- **Automatic Retry Handling**: Using `#[zart_step]` with `retry = "fixed(3, 1s)"`
- **Real-time Observation**: Logging shows the retry behavior as it happens

## How It Works

The example implements a durable execution with two steps:

1. **`intentional-failure`**: A step that:
   - Checks `ctx.current_attempt()`
   - Fails on attempt 0 with a simulated transient error
   - Succeeds on attempt 1+ (the retries)
   - Uses `retry = "fixed(3, 1s)"` for up to 3 retries with 1s delay

2. **`normal-step`**: A step that always succeeds (demonstrates normal behavior)

## Running the Example

```bash
# Ensure PostgreSQL is running (Docker Compose)
docker-compose up -d

# Run the example
just example-retry-simulation
```

## Expected Output

```
=== Zart Retry Simulation Example ===

Starting execution 'retry-sim-...' with name 'retry-demo'...

[intentional-failure] Attempt #0 (0-indexed) | is_retry=false | max_retries=Some(3)
⚠️  Simulated transient failure for 'retry-demo' on attempt #0

[intentional-failure] Attempt #1 (0-indexed) | is_retry=true | max_retries=Some(3)
✓  Succeeded for 'retry-demo' on retry attempt #1

[normal-step] Running (no retries needed)

=== Execution Completed ===
  Name:            retry-demo
  Total attempts:  2
  Message:         Completed after 2 attempt(s), succeeded on retry #1

Attempts Log:
  1. intentional-failure: succeeded on attempt #1 (1 retries)
  2. normal-step: Normal step completed successfully
```

## Key Code Pattern

```rust
#[zart_step("intentional-failure", retry = "fixed(3, 1s)")]
async fn intentional_failure_step(
    name: String,
    ctx: StepContext,
) -> Result<RetryStepResult, StepError> {
    if ctx.current_attempt() == 0 {
        // Simulate transient failure on first attempt
        return Err(StepError::Failed {
            step: "intentional-failure".to_string(),
            reason: "Simulated transient error".to_string(),
        });
    }
    // Succeed on retry
    Ok(RetryStepResult { message: "Succeeded on retry!" })
}

// Usage in durable handler:
let result = ctx.execute_step(intentional_failure_step(name)).await?;
```

## Use Cases

- **Testing**: Verify retry logic without relying on external failures
- **Documentation**: Show resilient behavior in examples
- **Production**: Implement "fail fast, retry successfully" patterns
- **Education**: Understand how Zart's retry mechanism works
