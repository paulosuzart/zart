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
//!     async fn run(&self, data: Self::Data) -> Result<Self::Output, TaskError> {
//!         Ok(data)
//!     }
//! }
//! ```

pub mod api;
pub mod api_trait;
pub mod context;
pub mod durable;
pub mod error;
pub mod execution_model;
pub(crate) mod local;
pub mod logging;
pub mod metrics;
pub mod registry;
pub mod retry;
pub mod step_ops;
pub mod step_types;
pub mod worker;

#[cfg(test)]
pub(crate) mod test_helpers;

pub use api::{
    ExecutionInfo, capture, context, now, require, schedule, sleep, sleep_until, step, step_or,
    step_or_else, wait, wait_for_event,
};
pub use api_trait::{DurableApi, into_durable_api};
pub use context::{StepHandle, TaskContext, ZartStep};
pub use durable::DurableScheduler;
pub use error::{
    ExecutionFailure, SchedulerError, StepError, StepOutcome, TaskError, ZartStepError,
};
pub use logging::{TracingConfig, init_tracing, init_tracing_with_config};
pub use registry::{DurableExecution, TaskRegistry};
pub use retry::RetryConfig;
pub use worker::{Worker, WorkerConfig};

// Re-export proc macros from zart-macros
pub use zart_macros::{capture, z_wait_event, zart_durable, zart_step};

/// Commonly used types re-exported for ergonomic imports.
///
/// Add `use zart::prelude::*;` to get access to all core types.
pub mod prelude {
    pub use crate::{
        ExecutionInfo,
        api_trait::DurableApi,
        capture, context,
        context::{StepHandle, ZartStep},
        durable::DurableScheduler,
        error::{
            ExecutionFailure, SchedulerError, StepError, StepOutcome, TaskError, ZartStepError,
        },
        now,
        registry::{DurableExecution, TaskRegistry},
        require,
        retry::RetryConfig,
        schedule, sleep, sleep_until, step, step_or, step_or_else, wait, wait_for_event,
        worker::{Worker, WorkerConfig},
    };
    pub use scheduler::{
        DurableStorage, ExecutionRecord, ExecutionStatus, Scheduler, StorageBackend,
    };
}
