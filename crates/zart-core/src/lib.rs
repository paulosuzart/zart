//! Shared core primitives for Zart crates.
//!
//! This crate holds the types, traits, and error definitions that are shared
//! between `zart` and `zart-scheduler`, avoiding circular dependencies.
//!
//! # Contents
//!
//! - [`error::StorageError`] — unified error type for all storage backends
//! - [`types`] — shared execution, step, and scheduling types
//! - [`recurrence`] — recurrence configuration for repeating tasks
//! - [`task_metadata`] — typed metadata carried on task rows
//! - [`store`] — focused storage trait definitions

pub mod error;
pub mod recurrence;
pub mod store;
pub mod table_names;
pub mod task_metadata;
pub mod types;

pub use error::StorageError;
pub use recurrence::Recurrence;
pub use task_metadata::{StepMetaType, TaskMetadata};
