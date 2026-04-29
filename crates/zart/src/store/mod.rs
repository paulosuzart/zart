//! Storage trait definitions for the Zart execution layer.
//!
//! [`StorageBackend`] is composed here from the domain traits defined in
//! `zart-core` (`ExecutionStore`, `StepStore`, `WaitGroupStore`, `EventStore`,
//! `PauseStorage`). The `TaskScheduler` trait lives in the `zart-scheduler`
//! crate and is **not** part of `StorageBackend` — it is held separately by
//! types that need both execution-side and task-queue operations.

pub use zart_core::store::pause_storage;
pub use zart_core::store::{
    EventStore, ExecutionStore, PauseRule, PauseRuleFilter, PauseSnapshot, PauseStorage,
    PauseStore, StepStore, WaitGroupStore,
};

// ── StorageBackend ────────────────────────────────────────────────────────────

/// Combined execution-side backend trait — the single type-erased handle for
/// all non-task-queue storage operations.
///
/// Use `Arc<dyn StorageBackend>` wherever a fully-capable execution backend is
/// needed. `PostgresStorage` satisfies this bound automatically via blanket impl.
///
/// Composed from:
/// - [`ExecutionStore`] — execution records and run primitives
/// - [`StepStore`] — step scheduling, completion, and query
/// - [`WaitGroupStore`] — wait-group coordination
/// - [`EventStore`] — event delivery and statistics
/// - [`pause_storage::PauseStorage`] — pause rules
pub trait StorageBackend:
    ExecutionStore + StepStore + WaitGroupStore + EventStore + pause_storage::PauseStorage + Send + Sync
{
}

impl<
    T: ExecutionStore
        + StepStore
        + WaitGroupStore
        + EventStore
        + pause_storage::PauseStorage
        + Send
        + Sync,
> StorageBackend for T
{
}

// ── Backend ───────────────────────────────────────────────────────────────────

/// Unified backend abstraction combining execution-side storage and
/// task-queue scheduling.
///
/// Pause/resume support comes for free: `StorageBackend` includes `PauseStorage`
/// as a supertrait, so any `Backend` provides pause through `storage()`.
///
/// Implemented by [`crate::postgres::PgBackend`] for production
/// and by test doubles like `RecordingScheduler` for unit tests.
///
/// Use [`crate::DurableScheduler::from_backend`] and [`crate::WorkerBuilder::from_backend`]
/// to construct fully-wired components from any `Backend`.
pub trait Backend: Send + Sync {
    fn storage(&self) -> std::sync::Arc<dyn StorageBackend>;
    fn scheduler(&self) -> std::sync::Arc<dyn zart_scheduler::TaskScheduler>;
}
