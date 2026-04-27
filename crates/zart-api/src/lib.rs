//! Zart HTTP API — optional Axum server for external interaction with durable executions.
//!
//! # Endpoints
//!
//! ```text
//! GET    /api/v1/executions                        — List executions
//! POST   /api/v1/executions                        — Start a new execution
//! GET    /api/v1/executions/:execution_id          — Get execution status
//! POST   /api/v1/executions/:execution_id/cancel   — Cancel an execution
//! GET    /api/v1/executions/:execution_id/wait     — Long-poll until completion
//! GET    /api/v1/stats                             — Aggregate execution counts by status
//! POST   /api/v1/events/:execution_id/:event_name  — Deliver an event
//! GET    /healthz                                  — Liveness probe
//! GET    /readyz                                   — Readiness probe
//! GET    /metrics                                  — Prometheus metrics
//!
//! GET    /admin/v1/executions/:id/detail           — Full execution detail with steps & attempts
//! POST   /admin/v1/executions/:id/retry-step       — Retry a dead step
//! POST   /admin/v1/executions/:id/restart          — Restart an execution
//! POST   /admin/v1/executions/:id/rerun            — Selective step rerun
//! GET    /admin/v1/executions/:id/runs             — List runs for an execution
//! POST   /admin/v1/pause                           — Create a pause rule
//! GET    /admin/v1/pause                           — List pause rules
//! POST   /admin/v1/pause/:rule_id                  — Resume (soft-delete) a pause rule
//! DELETE /admin/v1/pause/:rule_id                  — Delete a pause rule
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
pub mod routes;
pub mod server;
pub mod state;

pub use admin_routes::admin_router;
pub use server::ApiServer;
pub use state::{AdminState, AppState};
