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
//!
//! struct MyTask;
//!
//! #[async_trait]
//! impl DurableExecution for MyTask {
//!     type Data = serde_json::Value;
//!     type Output = serde_json::Value;
//!
//!     async fn run(
//!         &self,
//!         _ctx: &mut TaskContext,
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
pub mod execution_model;
pub mod logging;
pub mod metrics;
pub mod registry;
pub mod retry;
pub mod step_ops;
pub mod worker;

#[cfg(test)]
pub(crate) mod test_helpers;

pub use api_trait::{DurableApi, into_durable_api};
pub use context::{StepContext, StepHandle, TaskContext};
pub use durable::DurableScheduler;
pub use error::{SchedulerError, StepError, TaskError};
pub use logging::{TracingConfig, init_tracing, init_tracing_with_config};
pub use registry::{DurableExecution, TaskRegistry};
pub use retry::RetryConfig;
pub use worker::{Worker, WorkerConfig};

/// Commonly used types re-exported for ergonomic imports.
///
/// Add `use zart::prelude::*;` to get access to all core types.
pub mod prelude {
    pub use crate::{
        api_trait::DurableApi,
        context::{StepContext, StepHandle, TaskContext},
        durable::DurableScheduler,
        error::{SchedulerError, StepError, TaskError},
        registry::{DurableExecution, TaskRegistry},
        retry::RetryConfig,
        worker::{Worker, WorkerConfig},
    };
    pub use scheduler::{
        DurableStorage, ExecutionRecord, ExecutionStatus, Scheduler, StorageBackend,
    };
}
