<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="webpage/src/assets/logo-dark.svg">
    <img alt="Zart logo" src="webpage/src/assets/logo-light.svg" width="120">
  </picture>
</p>

# Zart

> **Durable execution for Rust.** Write workflows that survive crashes, restarts, and redeployments — without losing progress or repeating work.

**⚠️ Alpha — Work in Progress.** Zart is actively developed and not yet production-ready. Expect API changes and missing features.

---

## Why Zart Exists

Across payment processing, content generation, user onboarding, and compliance workflows, the same pattern repeats: teams build unreliable systems not because they don't care, but because the industry normalized a false choice. Ship fast and patch reliability later, or adopt heavy orchestration infrastructure from day one.

The tools that solve this — Temporal, Cadence, and similar platforms — are powerful. But they introduce concepts, infrastructure, and learning curves that most teams aren't ready for on day one. So reliability becomes a "later" problem. Until "later" means revenue loss, customer churn, or regulatory risk.

**Zart exists so you don't have to learn that lesson.**

It's a Rust library designed for **high ergonomics and zero orchestration overhead**. You get durable execution using your existing PostgreSQL database — no new infrastructure, no distributed systems primitives, no paradigm shift.

---

## What is Durable Execution?

Durable execution lets you write long-running workflows as ordinary async Rust code. Each step is checkpointed to the database. If your process crashes, times out, or gets redeployed, Zart resumes from the last successful step — no work is lost, no step is repeated.

Think of it like GitHub Actions: your workflow has multiple steps, and if the infrastructure fails mid-run, it resumes where it left off. No one configures durability as a non-functional requirement — it's just how the platform works. Zart brings that same default-reliable experience to your Rust backend.

## Complete Example

An order fulfillment workflow that validates an address, processes payment, then waits for an external shipment event:

```rust
use zart::prelude::*;
use zart::{zart_durable, zart_step};
use zart::error::TaskError;
use serde::{Deserialize, Serialize};
use std::time::Duration;

// ── Input / Output / Events ───────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderInput {
    order_id: String,
    amount: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Receipt {
    order_id: String,
    transaction_id: String,
    tracking_number: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ShipmentEvent {
    tracking_number: String,
}

// ── Step error types ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
enum PaymentError {
    #[error("card declined: {reason}")]
    CardDeclined { reason: String },
}

// ── Step 1: Validate address ──────────────────────────────────────────────────

#[zart_step("validate-address")]
async fn validate_address(order_id: String) -> Result<String, PaymentError> {
    // ... call address validation service ...
    Ok(format!("addr-{order_id}"))
}

// ── Step 2: Process payment ───────────────────────────────────────────────────
// Retries up to 3 times with exponential backoff on transient failures.

#[zart_step("process-payment", retry = "exponential(3, 2s)")]
async fn process_payment(order_id: String, amount: f64) -> Result<String, PaymentError> {
    // ... call payment service ...
    Ok(format!("txn-{order_id}"))
}

// ── Step 3: Reserve inventory ─────────────────────────────────────────────────

#[zart_step("reserve-inventory")]
async fn reserve_inventory(order_id: String) -> Result<(), PaymentError> {
    // ... call inventory service ...
    Ok(())
}

// ── Durable workflow ──────────────────────────────────────────────────────────

#[zart_durable("order-fulfillment", timeout = "30m")]
async fn order_fulfillment(data: OrderInput) -> Result<Receipt, TaskError> {
    validate_address(data.order_id.clone()).await?;
    let txn_id = process_payment(data.order_id.clone(), data.amount).await?;
    reserve_inventory(data.order_id.clone()).await?;

    // Durable sleep — survives restarts, no threads blocked
    zart::sleep("warehouse-settle", Duration::from_secs(60)).await?;

    // Wait up to 1 hour for an external "shipment_ready" event
    let event: ShipmentEvent = zart::wait_for_event(
        "shipment_ready",
        Some(Duration::from_secs(3600)),
    ).await?;

    Ok(Receipt {
        order_id: data.order_id,
        transaction_id: txn_id,
        tracking_number: event.tracking_number,
    })
}
```

If the process crashes after `process_payment` completes, a restart skips that step and resumes from `reserve_inventory`. No double-charges.

---

## What You Get

| Feature | What it does |
|---|---|
| **Step checkpointing** | Every step result persists to PostgreSQL. Crashes resume, not restart. |
| **Retry policies** | Per-step fixed or exponential backoff. Configurable attempts and delay. |
| **Durable sleep** | Pause workflows for minutes, hours, or days. Zero threads blocked. |
| **Event-driven waits** | Suspend until an external signal arrives — human approval, webhook, callback. |
| **Parallel steps** | Fan out independent work, wait for all to complete. Survives restarts mid-wait. |
| **Durable loops** | Iterate over collections with per-iteration checkpoints. Restarts skip done items. |
| **No new infrastructure** | Uses the PostgreSQL database you already have. Workers poll via `SKIP LOCKED`. |

## Philosophy

- **Reliability from day one** — Every step persists its result. Failures resume, not restart.
- **No new infrastructure** — Uses the PostgreSQL database you already have.
- **Progressive complexity** — Start simple. Add retries, parallel steps, and events only when you need them.
- **Rust-native** — Built on `tokio`, `serde`, and standard Rust patterns. No alien mental models.
- **Zero vendor lock-in** — It's a library, not a platform. Your workflows are just Rust code.

## Who Zart Is For

- **Teams shipping workflows** — payment flows, onboarding pipelines, content generation, data processing — that can't afford to lose progress.
- **Engineers who want reliability** without adopting a full orchestration platform or learning a new paradigm.
- **Startups and scale-ups** that need to move fast but can't afford to rebuild workflows after every incident.

## Who Zart Is Not For

- **Massive workflow orchestration** — If you're coordinating thousands of workers across multiple regions, Temporal or a similar platform may be a better fit.
- **Teams that need a managed service** — Zart is self-hosted on your infrastructure. There's no Zart Cloud (yet).

---

## Getting Started

Full documentation, examples, and API reference live at **[zart.run](http://zart.run/)**.

- **[Getting Started](http://zart.run/getting-started/)** — installation and your first workflow
- **[Features](http://zart.run/features/)** — steps, retries, sleep, events, and parallel execution
- **[Examples](http://zart.run/examples/)** — real-world patterns (brewery finder, approval workflows, parallel steps)
- **[Rust API](http://zart.run/rust-api/overview/)** — `#[zart_step]`, `#[zart_durable]`, macros, and loops

### In Your Project

```toml
[dependencies]
zart = "0.1"
zart-macros = "0.1"
```

```bash
# Start PostgreSQL
docker compose up -d

# Run migrations
just migrate

# Run the sleep example
just example-sleep
```

## Project Structure

| Crate | Description |
|---|---|
| `zart` | Core durable execution library |
| `scheduler` | PostgreSQL-backed task scheduler & worker |
| `zart-cli` | Command-line tools |
| `zart-api` | HTTP API for external event delivery |
| `zart-macros` | Procedural macros (`#[zart_step]`, `#[zart_durable]`) |

## Architecture

Zart uses a pull-based model: workers poll PostgreSQL for due tasks and execute them. No coordinator service is required beyond your existing worker processes.

Steps are decomposed into:
1. **Body mode** — your workflow function schedules/looks up steps
2. **Step mode** — individual step lambdas execute
3. **Completion** — results are atomically persisted and the next body task is scheduled

All state lives in PostgreSQL tables (`zart_tasks`, `zart_steps`, `zart_step_attempts`, `zart_execution_runs`).

## Development

> **Prerequisites:** Install [prek](https://github.com/shenxiangzhuang/prek) (`brew install prek` on macOS), then run `prek install` once after cloning to activate pre-commit hooks for format, lint, and doc checks.

```bash
# Install pre-commit hooks (one-time setup)
prek install

# Start PostgreSQL
docker compose up -d

# Run tests
just test

# Lint & format
just lint
just fmt
```

## License

MIT

## Links

- **[Website](http://zart.run/)** — full documentation and examples
- **[GitHub](https://github.com/paulosuzart/zart)** — source code and issues
- **[About](http://zart.run/about/)** — the story and philosophy behind Zart
