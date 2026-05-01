//! Zart HTTP API — optional Axum server for external interaction with durable executions.
//!
//! Route prefixes are configurable at startup (default: `/api/v1` and `/zart/admin/v1`).
//! Set `ZART_API_PREFIX` / `ZART_ADMIN_PREFIX` env vars or use the builder methods
//! [`ApiServer::with_api_prefix`] / [`ApiServer::with_admin_prefix`].
//!
//! # Endpoints (default prefixes)
//!
//! ```text
//! GET    /api/v1/executions                        — List executions
//! POST   /api/v1/executions                        — Start a new execution
//! GET    /api/v1/executions/:execution_id          — Get execution status
//! POST   /api/v1/executions/:execution_id/cancel   — Cancel an execution
//! GET    /api/v1/executions/:execution_id/wait     — Long-poll until completion
//! GET    /api/v1/stats                             — Aggregate execution counts by status
//! POST   /api/v1/events/:execution_id/:event_name  — Deliver an event
//! GET    /healthz                                  — Liveness probe (always at root)
//! GET    /readyz                                   — Readiness probe (always at root)
//! GET    /metrics                                  — Prometheus metrics (always at root)
//!
//! GET    /zart/admin/v1/executions/:id/detail           — Full execution detail with steps & attempts
//! POST   /zart/admin/v1/executions/:id/retry-step       — Retry a dead step
//! POST   /zart/admin/v1/executions/:id/restart          — Restart an execution
//! POST   /zart/admin/v1/executions/:id/rerun            — Selective step rerun
//! GET    /zart/admin/v1/executions/:id/runs             — List runs for an execution
//! POST   /zart/admin/v1/pause                           — Create a pause rule
//! GET    /zart/admin/v1/pause                           — List pause rules
//! POST   /zart/admin/v1/pause/:rule_id                  — Resume (soft-delete) a pause rule
//! DELETE /zart/admin/v1/pause/:rule_id                  — Delete a pause rule
//! ```
//!
//! # Usage
//!
//! ```rust,no_run
//! use zart_api::ApiServer;
//! use zart::{DurableRegistry, DurableScheduler, into_durable_api};
//! use std::sync::Arc;
//!
//! # async fn example() {
//! // let scheduler = Arc::new(/* PostgresScheduler */);
//! // let durable = into_durable_api(DurableScheduler::new(scheduler));
//! // ApiServer::new("0.0.0.0:8080", durable).serve().await.unwrap();
//! # }
//! ```

pub mod admin_routes;
pub mod models;
#[cfg(feature = "openapi")]
pub mod openapi;
pub mod routes;
pub mod server;
pub mod state;

pub use admin_routes::admin_router;
pub use server::ApiServer;
pub use state::{AdminState, AppState};

#[cfg(feature = "openapi")]
pub use openapi::{ZartApiDoc, build_openapi};
