# zart-scheduler

Core scheduling primitives for the [Zart](https://www.zart.run/) durable execution framework.

This crate sits at the base of the Zart stack. It defines the storage-backend traits and provides the PostgreSQL implementation built on [sqlx](https://crates.io/crates/sqlx).

## At a glance

- **`Scheduler` trait** — schedule, poll, complete, fail, and cancel tasks via skip-locked polling (`SELECT … FOR UPDATE SKIP LOCKED`)
- **`DurableStorage` trait** — step-level operations for durable executions: start, complete, list, event delivery, wait-groups, retries
- **`StorageBackend` trait** — blanket combination of both traits; use `Arc<dyn StorageBackend>` for a type-erased backend
- **`PostgresScheduler`** — production-ready PostgreSQL backend
- **`Recurrence`** — cron and fixed-delay scheduling expressions

## Usage

`zart-scheduler` is a low-level building block. Most applications should depend on [`zart`](https://crates.io/crates/zart) directly, which re-exports the types you need.

```toml
[dependencies]
zart-scheduler = "0.1"
```

## Recurring Tasks

`zart-scheduler` supports recurring tasks via the `Recurrence` enum:

```rust
pub enum Recurrence {
    Cron { expression: String, timezone: String },
    FixedDelay { duration_ms: u64 },
}
```

### Scheduling Recurring Tasks

Use the `schedule_recurring` helper to schedule a task that automatically reschedules based on the recurrence rule:

```rust
scheduler
    .schedule_recurring(
        "heartbeat-check",
        "heartbeat-check",
        Recurrence::FixedDelay { duration_ms: 30_000 }, // every 30s
        serde_json::json!({}),
    )
    .await?;
```

### Worker Auto-Rescheduling

When a recurring task's handler returns `Ok(())` without explicitly calling `ops.complete()` or `ops.reschedule()`, the worker automatically computes the next execution time using `Recurrence::next_after()` and reschedules the task.

Cron tasks use the **original execution time** as the base for computing next runs, enabling catch-up behavior if a task runs past the next trigger.

### Examples

See the [recurring-tasks example](../../examples/recurring-tasks) for a working demo.

## Learn more

- Website: <https://www.zart.run/>
- Repository: <https://github.com/paulosuzart/zart>
