# zart

Durable multi-step workflows for Rust that survive process restarts.

Zart persists every step result to PostgreSQL. When a worker crashes and restarts, execution resumes from the last completed step — no work is repeated, no step runs twice. Concurrency is handled automatically via skip-locked polling.

## At a glance

- **Durable execution** — steps are checkpointed; replay is transparent
- **Retries** — configurable per-step retry policies with backoff
- **Wait groups** — fan-out/fan-in across parallel sub-tasks
- **Event-driven steps** — pause execution until an external event arrives
- **Timeouts** — per-handler and per-step deadlines
- **Observability** — structured tracing and optional Prometheus metrics

## Quick example

```rust
use zart::prelude::*;

#[zart_durable("onboard-user")]
async fn onboard(data: UserId) -> Result<(), MyError> {
    zart::require(SendWelcomeEmail { id: data }).await?;
    zart::require(ProvisionAccount { id: data }).await?;
    Ok(())
}
```

Each `require` call is a durable step. On the first run it executes and persists the result; on replay it returns the cached value instantly.

## Learn more

- Website: <https://www.zart.run/>
- Repository: <https://github.com/paulosuzart/zart>
- Crates: [`zart-scheduler`](https://crates.io/crates/zart-scheduler) · [`zart-macros`](https://crates.io/crates/zart-macros) · [`zart-api`](https://crates.io/crates/zart-api) · [`zart-cli`](https://crates.io/crates/zart-cli)
