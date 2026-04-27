//! Durable multi-step workflows that survive process restarts.
//!
//! Zart persists every step result to a database. When a worker crashes and
//! restarts, execution resumes from the last completed step — no work is
//! repeated. Concurrency is handled automatically via skip-locked polling.
//!
//! # Example
//!
//! A handler is any type that implements [`DurableExecution`]. Steps inside
//! `run()` are individually persisted: on replay their cached result is
//! returned without re-executing.
//!
//! ```rust,ignore
//! use zart::prelude::*;
//!
//! // ── Steps ────────────────────────────────────────────────────────────────
//!
//! struct SendWelcomeEmail { user_id: Uuid }
//!
//! #[async_trait::async_trait]
//! impl ZartStep for SendWelcomeEmail {
//!     type Output = ();
//!     type Error  = MyError;
//!     fn step_name(&self) -> std::borrow::Cow<'static, str> { "send-welcome".into() }
//!
//!     async fn run(&self) -> Result<(), MyError> {
//!         email::send_welcome(self.user_id).await
//!     }
//! }
//!
//! struct CreateWorkspace { user_id: Uuid }
//!
//! #[async_trait::async_trait]
//! impl ZartStep for CreateWorkspace {
//!     type Output = Uuid;
//!     type Error  = MyError;
//!     fn step_name(&self) -> std::borrow::Cow<'static, str> { "create-workspace".into() }
//!
//!     async fn run(&self) -> Result<Uuid, MyError> {
//!         workspace::create(self.user_id).await
//!     }
//! }
//!
//! // ── Handler ───────────────────────────────────────────────────────────────
//!
//! struct OnboardUser;
//!
//! #[async_trait::async_trait]
//! impl DurableExecution for OnboardUser {
//!     type Data   = Uuid;          // passed in on start()
//!     type Output = Uuid;          // returned when the execution completes
//!
//!     async fn run(&self, user_id: Uuid) -> Result<Uuid, TaskError> {
//!         zart::require(SendWelcomeEmail { user_id }).await?;
//!         let workspace_id = zart::require(CreateWorkspace { user_id }).await?;
//!         Ok(workspace_id)
//!     }
//! }
//! ```
//!
//! Register the handler, start a worker, and fire an execution:
//!
//! ```text
//! let mut registry = DurableRegistry::new();
//! registry.register("onboard-user", OnboardUser);
//!
//! let scheduler = /* connect to postgres */;
//! let sched     = DurableScheduler::new(scheduler.clone(), scheduler.task_scheduler());
//! let worker    = WorkerBuilder::new(scheduler.clone(), scheduler.task_scheduler())
//!                     .registry(registry)
//!                     .build();
//!
//! // Start the execution from anywhere in your application.
//! sched.start_for::<OnboardUser>("onboard-alice", "onboard-user", &user_id).await?;
//!
//! // Optionally wait for the result.
//! let workspace_id = sched.wait_for::<OnboardUser>("onboard-alice", timeout, None).await?;
//! ```
//!
//! # Core concepts
//!
//! | Concept | Description |
//! |---|---|
//! | [`DurableExecution`] | Trait for workflow handlers — implement `run()`. |
//! | [`ZartStep`] | Trait for individual steps; results are cached across retries. |
//! | [`DurableScheduler`] | Starts executions, queries status, and waits for results. |
//! | [`Worker`] | Polls the database and dispatches handlers on a configurable thread pool. |
//! | [`DurableRegistry`] | Maps task-name strings to handler instances. |
//!
//! # Step helpers
//!
//! Inside `DurableExecution::run()` the following free functions are available:
//!
//! | Function | Description |
//! |---|---|
//! | [`require`] | Execute a step; re-entry returns the cached result. |
//! | [`step`] / [`step_or`] | Variants of `require` with different error-handling shapes. |
//! | [`schedule`] | Fire-and-forget: schedule a child execution without waiting. |
//! | [`sleep`] / [`sleep_until`] | Durable sleep — survives restarts. |
//! | [`wait`] / [`wait_for_event`] | Suspend until another execution completes or an event arrives. |
//!
//! # Optional: atomic transaction participation
//!
//! Both features below are opt-in; all existing APIs work without them.
//!
//! **Start inside a caller transaction** — use [`DurableScheduler::start_in_tx`]
//! so your business write and the execution record commit atomically:
//!
//! ```rust,ignore
//! let mut tx = pool.begin().await?;
//! sqlx::query("INSERT INTO users …").execute(&mut *tx).await?;
//! sched.start_in_tx(&mut tx, "onboard-alice", "onboard-user", payload).await?;
//! tx.commit().await?;
//! ```
//!
//! **Step-level atomicity** — call [`trx`] inside `ZartStep::run()` to make
//! your DB write and the framework's step-completion record commit together:
//!
//! ```rust,ignore
//! async fn run(&self) -> Result<(), MyError> {
//!     let mut tx = zart::trx(&self.pool).await?;
//!     sqlx::query("UPDATE accounts …").execute(&mut **tx).await?;
//!     Ok(()) // framework commits tx; rolls back automatically on Err
//! }
//! ```

pub mod admin;
pub mod api;
pub mod api_trait;
pub mod builder;
pub mod context;
pub mod durable;
pub mod error;
pub mod execution_model;
pub(crate) mod local;
pub mod logging;
pub mod metrics;
pub mod postgres;
pub mod registry;
pub mod retry;
pub mod service;
pub mod step_ops;
pub mod step_types;
pub mod store;
pub mod task;
pub mod task_metadata;
pub mod timeout;
mod trx_impl;
pub mod types;

pub const TASK_NAME: &str = "__zart__";

#[cfg(test)]
pub(crate) mod test_helpers;

pub use admin::{
    AdminOperation, AdminOperationContext, PauseRule, PauseScope, RerunResult, RerunSpec,
    ResumeResult,
};
pub use api::{
    ExecutionInfo, capture, context, now, require, schedule, sleep, sleep_until, step, step_or,
    step_or_else, wait, wait_for_event,
};
pub use api_trait::{DurableApi, into_durable_api};
pub use builder::WorkerBuilder;
pub use context::{StepHandle, TaskContext, ZartStep};
pub use durable::DurableScheduler;
pub use error::{
    ExecutionFailure, SchedulerError, StepError, StepOutcome, TaskError, ZartStepError,
};
pub use logging::{TracingConfig, init_tracing, init_tracing_with_config};
pub use postgres::PostgresStorage;
pub use registry::{DurableExecution, DurableRegistry};
pub use zart_scheduler::{Worker, WorkerConfig};
// Re-export execution-side types so callers don't need zart-core directly.
pub use retry::RetryConfig;
pub use service::ExecutionService;
pub use store::StorageBackend;
pub use timeout::TimeoutScope;
pub use trx_impl::{ZartTrx, trx};
// Worker is now provided via WorkerBuilder
pub use zart_core::store::pause_storage::PauseRuleFilter;
pub use zart_core::types::{
    ExecutionRecord, ExecutionRunRecord, ExecutionSortField, ExecutionStats, ExecutionStatus,
    ListExecutionsParams, SortOrder,
};

// Re-export proc macros from zart-macros
pub use zart_macros::{capture, z_wait_event, zart_durable, zart_step};

/// Commonly used types re-exported for ergonomic imports.
///
/// Add `use zart::prelude::*;` to get access to all core types.
pub mod prelude {
    pub use crate::{
        AdminOperation, AdminOperationContext, ExecutionInfo, PauseRule, PauseScope, RerunResult,
        RerunSpec, ResumeResult, WorkerConfig, ZartTrx,
        api_trait::DurableApi,
        builder::WorkerBuilder,
        capture, context,
        context::{StepHandle, ZartStep},
        durable::DurableScheduler,
        error::{
            ExecutionFailure, SchedulerError, StepError, StepOutcome, TaskError, ZartStepError,
        },
        now,
        registry::{DurableExecution, DurableRegistry},
        require,
        retry::RetryConfig,
        schedule, sleep, sleep_until, step, step_or, step_or_else, trx, wait, wait_for_event,
    };
    pub use crate::{ExecutionRecord, ExecutionStatus, StorageBackend};
}
