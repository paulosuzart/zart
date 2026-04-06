# Retry Simulation Example

This example demonstrates how to simulate and observe retry behavior in Zart durable executions.

## What It Demonstrates

- **Intentional Failure Pattern**: The example fails on the first attempt and succeeds on the retry
- **Execution Metadata Access**: Using read-only accessors to query retry state:
  - `ctx.current_attempt()` - Which attempt is running (0-indexed)
  - `ctx.max_retries()` - Maximum retries configured
  - `ctx.is_retry_attempt()` - Whether this is a retry (not the first attempt)
- **Automatic Retry Handling**: Using `step_with_retry` with `RetryConfig`
- **Real-time Observation**: Logging shows the retry behavior as it happens

## How It Works

The example implements a durable execution with two steps:

1. **`intentional-failure`**: A step that:
   - Checks `ctx.current_attempt()` 
   - Fails on attempt 0 with a simulated transient error
   - Succeeds on attempt 1+ (the retries)
   - Uses `RetryConfig::fixed(3, Duration::from_secs(1))` for up to 3 retries with 1s delay

2. **`normal-step`**: A step that always succeeds (demonstrates normal behavior)

## Running the Example

```bash
# Ensure PostgreSQL is running (Docker Compose)
docker-compose up -d

# Run the example
cargo run --bin example-retry-simulation
```

## Expected Output

```
=== Zart Retry Simulation Example ===

This example demonstrates intentional failure and automatic retry.
The first attempt will fail, and the framework will retry automatically.

Starting execution 'retry-sim-...' with name 'Paulo'...

[intentional-failure] Attempt #0 (0-indexed) | is_retry=false | max_retries=Some(3)
⚠️  Simulated transient failure for 'Paulo' on attempt #0

[intentional-failure] Attempt #1 (0-indexed) | is_retry=true | max_retries=Some(3)
✓  Succeeded for 'Paulo' on retry attempt #1

[normal-step] Running (no retries needed)

============================================================
✓ Execution completed successfully!
============================================================
  Name:            Paulo
  Total attempts:  2
  Message:         Completed with 2 total attempt(s) - retry simulation successful!

Attempts Log:
  1. intentional-failure: succeeded on attempt #1 (1 retries)
  2. normal-step: Normal step completed successfully
============================================================
```

## Key Code Pattern

```rust
ctx.step_with_retry(
    "risky-operation",
    RetryConfig::fixed(3, Duration::from_secs(1)),
    || {
        async move {
            if ctx.current_attempt() == 0 {
                // Simulate transient failure on first attempt
                return Err(StepError::Failed {
                    step: "risky-operation".to_string(),
                    reason: "Simulated transient error".to_string(),
                });
            }
            // Succeed on retry
            Ok(SuccessResult { message: "Succeeded on retry!" })
        }
    },
).await?
```

## Use Cases

- **Testing**: Verify retry logic without relying on external failures
- **Documentation**: Show resilient behavior in examples
- **Production**: Implement "fail fast, retry successfully" patterns
- **Education**: Understand how Zart's retry mechanism works
