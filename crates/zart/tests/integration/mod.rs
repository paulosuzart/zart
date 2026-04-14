//! Integration tests for the `zart` crate.
//!
//! Tests marked `#[ignore]` require a running PostgreSQL instance.
//! Start it with `just up`, then run: `just test-integration`

mod admin_retry;
mod basic_execution;
mod cancellation;
mod dispatch;
mod event_driven;
#[cfg(test)]
mod helpers;
mod parallel_steps;
mod transaction;
mod typed_completion;
