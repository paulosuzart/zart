//! Integration tests for the `zart` crate.
//!
//! Tests marked `#[ignore]` require a running PostgreSQL instance.
//! Start it with `just up`, then run: `just test-integration`

#[cfg(test)]
mod helpers;
mod admin_retry;
mod basic_execution;
mod cancellation;
mod event_driven;
mod parallel_steps;
mod dispatch;
mod transaction;
mod typed_completion;
