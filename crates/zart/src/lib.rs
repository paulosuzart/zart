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
//! use async_trait::async_trait;
//! use scheduler::Scheduler;
//!
//! struct MyTask;
//!
//! #[async_trait]
//! impl TaskHandler for MyTask {
//!     type Data = serde_json::Value;
//!     type Output = serde_json::Value;
//!
//!     async fn run<S: Scheduler>(
//!         &self,
//!         _ctx: &mut TaskContext<S>,
//!         data: Self::Data,
//!     ) -> Result<Self::Output, TaskError> {
//!         Ok(data)
//!     }
//! }
//! ```

pub mod api_trait;
pub mod context;
pub mod durable;
pub mod error;
pub mod registry;
pub mod retry;
pub mod worker;

pub use api_trait::{DurableApi, into_durable_api};
pub use context::{StepHandle, TaskContext};
pub use durable::DurableScheduler;
pub use error::{SchedulerError, StepError, TaskError};
pub use registry::{TaskHandler, TaskRegistry};
pub use retry::RetryConfig;
pub use worker::{Worker, WorkerConfig};

/// Commonly used types re-exported for ergonomic imports.
///
/// Add `use zart::prelude::*;` to get access to all core types.
pub mod prelude {
    pub use crate::{
        api_trait::DurableApi,
        context::{StepHandle, TaskContext},
        durable::DurableScheduler,
        error::{SchedulerError, StepError, TaskError},
        registry::{TaskHandler, TaskRegistry},
        retry::RetryConfig,
        worker::{Worker, WorkerConfig},
    };
    pub use scheduler::{ExecutionRecord, ExecutionStatus, Scheduler};
}
