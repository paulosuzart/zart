//! Zart — Durable Execution Framework
//!
//! A high-level framework for durable execution built on top of a
//! database-backed scheduler. Tasks are persisted, polled with skip-lock
//! for concurrent workers, and support multi-step workflows with automatic
//! state recovery across process restarts and failures.
//!
//! # Architecture
//!
//! Zart operates on three layers:
//!
//! - **Layer 1 — Scheduler**: individual tasks persisted in a database
//!   (provided by the [`scheduler`] crate).
//! - **Layer 2 — Durable Execution**: multi-step workflows composed of
//!   individually scheduled tasks, with result persistence and re-entry.
//! - **Layer 3 — Attempts**: retry history for both executions and steps.
//!
//! # Quick Start
//!
//! ```rust,no_run
//! use zart::prelude::*;
//! use scheduler::Scheduler;
//!
//! struct MyTask;
//!
//! impl TaskHandler for MyTask {
//!     type Data = serde_json::Value;
//!     type Output = serde_json::Value;
//!
//!     fn run<'life0, 'life1, 'async_trait, S: Scheduler>(
//!         &'life0 self,
//!         ctx: &'life1 mut TaskContext<S>,
//!         data: Self::Data,
//!     ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Self::Output, TaskError>> + Send + 'async_trait>>
//!     where
//!         'life0: 'async_trait,
//!         'life1: 'async_trait,
//!         Self: 'async_trait,
//!     {
//!         Box::pin(async move { Ok(data) })
//!     }
//! }
//! ```

pub mod context;
pub mod error;
pub mod registry;
pub mod retry;
pub mod worker;

pub use context::TaskContext;
pub use error::{SchedulerError, StepError, TaskError};
pub use registry::{TaskHandler, TaskRegistry};
pub use retry::RetryConfig;
pub use worker::{Worker, WorkerConfig};

/// Commonly used types re-exported for ergonomic imports.
///
/// Add `use zart::prelude::*;` to get access to all core types.
pub mod prelude {
    pub use crate::{
        context::TaskContext,
        error::{SchedulerError, StepError, TaskError},
        registry::{TaskHandler, TaskRegistry},
        retry::RetryConfig,
        worker::{Worker, WorkerConfig},
    };
    pub use scheduler::Scheduler;
}
