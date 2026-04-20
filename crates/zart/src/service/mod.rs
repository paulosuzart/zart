//! Service layer for durable-execution business operations.
//!
//! Services sit between [`crate::durable::DurableScheduler`] (the public API)
//! and the storage layer ([`zart_scheduler::DurableStorage`]). They own
//! business logic that should not live in storage primitives.

pub mod execution_service;
pub mod pause_service;

pub use execution_service::ExecutionService;
pub use pause_service::PauseService;
