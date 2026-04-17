//! Internal repository traits for raw table access.
//!
//! Each trait is scoped to one table or one pair of tightly-coupled tables
//! (e.g. `zart_executions` + `zart_execution_runs`). Methods map to individual
//! SQL statements or logically atomic single-table operations. No cross-table
//! transactions. No business logic.
//!
//! These traits are `pub(crate)` — not part of the public API.
//! `PostgresScheduler` implements all of them in the `postgres` module.

pub(crate) mod admin;
pub(crate) mod event;
pub(crate) mod execution;
pub(crate) mod step;
pub(crate) mod wait_group;

pub(crate) use admin::AdminRepository;
pub(crate) use event::EventRepository;
pub(crate) use execution::ExecutionRepository;
pub(crate) use step::StepRepository;
pub(crate) use wait_group::WaitGroupRepository;
