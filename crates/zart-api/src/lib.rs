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
//! POST   /api/v1/events/:execution_id/:event_name  — Deliver an event
//! GET    /healthz                                  — Liveness probe
//! GET    /readyz                                   — Readiness probe
//! GET    /metrics                                  — Prometheus metrics
//! ```
//!
//! # Usage
//!
//! ```rust,no_run
//! use zart_api::ApiServer;
//! use zart::{DurableScheduler, TaskRegistry, into_durable_api};
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
