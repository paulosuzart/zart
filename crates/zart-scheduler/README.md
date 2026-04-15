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

## Learn more

- Website: <https://www.zart.run/>
- Repository: <https://github.com/paulosuzart/zart>
