# zart-core

Shared core types, traits, and error primitives for the [Zart](https://www.zart.run/) durable execution framework.

This crate holds the types, traits, and error definitions shared between `zart` and `zart-scheduler`, avoiding circular dependencies. It is an internal building block; most applications should depend on [`zart`](https://crates.io/crates/zart) directly.

## At a glance

- **`error::StorageError`** — unified error type for all storage backends
- **`types`** — shared execution, step, and scheduling types
- **`recurrence`** — recurrence configuration for repeating tasks (cron, fixed-delay)
- **`task_metadata`** — typed metadata carried on task rows (`StepMetaType`, `TaskMetadata`)
- **`store`** — focused storage trait definitions (`ExecutionStore`, `StepStore`, etc.)
- **`table_names`** — shared database table name constants

## Re-exports

For convenience, the following types are re-exported at the crate root:

```rust
pub use error::StorageError;
pub use recurrence::Recurrence;
pub use task_metadata::{StepMetaType, TaskMetadata};
```

## Learn more

- Website: <https://www.zart.run/>
- Repository: <https://github.com/paulosuzart/zart>
- Related crates: [`zart`](https://crates.io/crates/zart) · [`zart-scheduler`](https://crates.io/crates/zart-scheduler) · [`zart-macros`](https://crates.io/crates/zart-macros)
